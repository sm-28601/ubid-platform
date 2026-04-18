"""
Activity Inference Engine for UBID Platform.

Classifies each UBID as Active, Dormant, or Closed based on
transaction and activity events from department systems.

Rules:
- Active:  ≥ 1 activity event in last 6 months
- Dormant: Last activity between 6–18 months ago
- Closed:  No activity for 18+ months, OR explicit closure event

Every classification is explainable — stores which signals
drove the verdict and over what time window.
"""

import json
from datetime import datetime, timedelta
from collections import defaultdict
from database.schema import get_connection
from engine.normalizer import normalize_pan, normalize_gstin


# ── Configuration ──
REFERENCE_DATE = datetime(2025, 4, 1)  # "now" for synthetic data
ACTIVE_WINDOW_DAYS = 180       # 6 months
DORMANT_WINDOW_DAYS = 540      # 18 months

# ── Event signal strengths ──
EVENT_SIGNAL_STRENGTH = {
    "inspection_conducted": "strong",
    "license_renewed": "strong",
    "consent_renewed": "strong",
    "tax_filing_submitted": "moderate",
    "electricity_consumption": "moderate",
    "water_consumption": "moderate",
    "employee_report_filed": "moderate",
    "pollution_test_passed": "moderate",
    "pollution_test_failed": "weak",
    "compliance_notice_issued": "weak",
    "closure_application": "definitive_close",
}


def match_events_to_ubids():
    """
    Match activity events to UBIDs using available identifiers.
    Events can be matched via:
    1. PAN (extracted from raw_identifier)
    2. GSTIN (extracted from raw_identifier)
    3. Source system + source ID pattern
    4. Business name fuzzy match (last resort)

    Unmatched events are flagged for review.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Load all events
    cursor.execute("SELECT * FROM activity_events WHERE matched_ubid IS NULL")
    events = [dict(row) for row in cursor.fetchall()]

    # Build lookup indexes from UBID linkages
    # PAN → UBID
    pan_to_ubid = {}
    cursor.execute("""
        SELECT um.ubid, sr.pan, sr.gstin
        FROM ubid_master um
        JOIN ubid_linkages ul ON um.ubid = ul.ubid
        JOIN source_records sr ON ul.source_system = sr.source_system AND ul.source_id = sr.source_id
        WHERE ul.is_active = 1
    """)
    for row in cursor.fetchall():
        if row["pan"]:
            pan_to_ubid[row["pan"].upper()] = row["ubid"]
        if row["gstin"]:
            gstin_norm, pan_from_gstin = normalize_gstin(row["gstin"])
            if gstin_norm:
                pan_to_ubid[gstin_norm] = row["ubid"]
            if pan_from_gstin:
                pan_to_ubid[pan_from_gstin] = row["ubid"]

    # Source system:name → UBID (for name-based matching)
    name_to_ubid = {}
    cursor.execute("""
        SELECT um.ubid, sr.source_system, sr.raw_name, sr.normalized_name
        FROM ubid_master um
        JOIN ubid_linkages ul ON um.ubid = ul.ubid
        JOIN source_records sr ON ul.source_system = sr.source_system AND ul.source_id = sr.source_id
        WHERE ul.is_active = 1
    """)
    for row in cursor.fetchall():
        key = f"{row['source_system']}:{row['raw_name']}"
        name_to_ubid[key] = row["ubid"]
        if row["normalized_name"]:
            key2 = f"{row['source_system']}:{row['normalized_name']}"
            name_to_ubid[key2] = row["ubid"]

    matched_count = 0
    unmatched_count = 0

    for event in events:
        raw_id = (event.get("raw_identifier") or "").strip()
        matched_ubid = None
        match_conf = 0.0

        # Try PAN match
        pan = normalize_pan(raw_id)
        if pan and pan in pan_to_ubid:
            matched_ubid = pan_to_ubid[pan]
            match_conf = 0.95

        # Try GSTIN match
        if not matched_ubid:
            gstin_norm, pan_from_gstin = normalize_gstin(raw_id)
            if gstin_norm and gstin_norm in pan_to_ubid:
                matched_ubid = pan_to_ubid[gstin_norm]
                match_conf = 0.95
            elif pan_from_gstin and pan_from_gstin in pan_to_ubid:
                matched_ubid = pan_to_ubid[pan_from_gstin]
                match_conf = 0.90

        # Try source:name match
        if not matched_ubid and raw_id in name_to_ubid:
            matched_ubid = name_to_ubid[raw_id]
            match_conf = 0.70

        # Try partial name match (system:name format)
        if not matched_ubid and ":" in raw_id:
            parts = raw_id.split(":", 1)
            if len(parts) == 2:
                key = f"{parts[0]}:{parts[1]}"
                if key in name_to_ubid:
                    matched_ubid = name_to_ubid[key]
                    match_conf = 0.65

        # Update event
        if matched_ubid:
            cursor.execute("""
                UPDATE activity_events
                SET matched_ubid = ?, match_confidence = ?
                WHERE id = ?
            """, (matched_ubid, match_conf, event["id"]))
            matched_count += 1
        else:
            unmatched_count += 1

    conn.commit()
    conn.close()

    print(f"[ACTIVITY] Matched {matched_count} events to UBIDs, {unmatched_count} unmatched")
    return matched_count, unmatched_count


def classify_business_activity():
    """
    Classify each UBID as Active, Dormant, or Closed.

    Returns summary of classifications.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get all UBIDs
    cursor.execute("SELECT ubid FROM ubid_master WHERE activity_status != 'Merged'")
    ubids = [row["ubid"] for row in cursor.fetchall()]

    stats = {"Active": 0, "Dormant": 0, "Closed": 0, "Unknown": 0}

    for ubid in ubids:
        # Get all events for this UBID
        cursor.execute("""
            SELECT * FROM activity_events
            WHERE matched_ubid = ?
            ORDER BY event_date DESC
        """, (ubid,))
        events = [dict(row) for row in cursor.fetchall()]

        status, evidence = _classify_single(events)

        # Update UBID
        cursor.execute("""
            UPDATE ubid_master
            SET activity_status = ?,
                status_updated_at = datetime('now'),
                status_evidence = ?
            WHERE ubid = ?
        """, (status, json.dumps(evidence), ubid))

        stats[status] += 1

    conn.commit()
    conn.close()

    print(f"[ACTIVITY] Classification complete: {stats}")
    return stats


def _classify_single(events):
    """
    Classify a single business based on its events.

    Returns (status, evidence_dict)
    """
    if not events:
        return "Unknown", {
            "rule": "no_events",
            "explanation": "No activity events found for this business.",
            "event_count": 0,
            "time_window": f"Analyzed till {REFERENCE_DATE.strftime('%Y-%m-%d')}",
        }

    # Check for explicit closure
    for evt in events:
        if evt["event_type"] == "closure_application":
            return "Closed", {
                "rule": "explicit_closure",
                "explanation": f"Closure application filed on {evt['event_date']}.",
                "decisive_event": evt["event_type"],
                "decisive_date": evt["event_date"],
                "event_count": len(events),
                "time_window": f"{events[-1]['event_date']} to {events[0]['event_date']}",
                "events_summary": _summarize_events(events),
            }

    # Find most recent event
    most_recent = events[0]  # already sorted DESC
    try:
        most_recent_date = datetime.strptime(most_recent["event_date"], "%Y-%m-%d")
    except (ValueError, TypeError):
        return "Unknown", {
            "rule": "parse_error",
            "explanation": "Could not parse event dates.",
        }

    days_since_last = (REFERENCE_DATE - most_recent_date).days

    if days_since_last <= ACTIVE_WINDOW_DAYS:
        status = "Active"
        rule = "recent_activity"
        explanation = (
            f"Last activity ({most_recent['event_type']}) was {days_since_last} days ago "
            f"on {most_recent['event_date']}, within the {ACTIVE_WINDOW_DAYS}-day active window."
        )
    elif days_since_last <= DORMANT_WINDOW_DAYS:
        status = "Dormant"
        rule = "stale_activity"
        explanation = (
            f"Last activity ({most_recent['event_type']}) was {days_since_last} days ago "
            f"on {most_recent['event_date']}, between {ACTIVE_WINDOW_DAYS} and "
            f"{DORMANT_WINDOW_DAYS} days — classified as Dormant."
        )
    else:
        status = "Closed"
        rule = "no_recent_activity"
        explanation = (
            f"No activity for {days_since_last} days "
            f"(last event on {most_recent['event_date']}). "
            f"Exceeds {DORMANT_WINDOW_DAYS}-day threshold — classified as Closed."
        )

    evidence = {
        "rule": rule,
        "explanation": explanation,
        "days_since_last_activity": days_since_last,
        "most_recent_event": most_recent["event_type"],
        "most_recent_date": most_recent["event_date"],
        "event_count": len(events),
        "time_window": f"{events[-1]['event_date']} to {events[0]['event_date']}",
        "signal_strengths": _count_signal_strengths(events),
        "events_summary": _summarize_events(events),
    }

    return status, evidence


def _count_signal_strengths(events):
    """Count events by signal strength."""
    counts = {"strong": 0, "moderate": 0, "weak": 0}
    for evt in events:
        strength = EVENT_SIGNAL_STRENGTH.get(evt["event_type"], "weak")
        if strength in counts:
            counts[strength] += 1
    return counts


def _summarize_events(events):
    """Create a summary of event types and counts."""
    type_counts = defaultdict(int)
    for evt in events:
        type_counts[evt["event_type"]] += 1
    return dict(type_counts)


def run_activity_inference():
    """
    Full activity inference pipeline:
    1. Match events to UBIDs
    2. Classify each UBID's activity status
    """
    print("\n[ACTIVITY] Starting activity inference pipeline...")

    matched, unmatched = match_events_to_ubids()
    stats = classify_business_activity()

    return {
        "events_matched": matched,
        "events_unmatched": unmatched,
        "classifications": stats,
    }


if __name__ == "__main__":
    run_activity_inference()
