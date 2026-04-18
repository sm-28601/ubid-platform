"""
Activity Inference Engine for UBID Platform.
"""

import json
from datetime import datetime, timedelta
from collections import defaultdict
from database.schema import get_session
from database.models import SourceRecord, UbidMaster, UbidLinkage, ActivityEvent
from activity_config import ActivityConfigManager
from engine.normalizer import normalize_pan, normalize_gstin


REFERENCE_DATE = datetime(2025, 4, 1)  
ACTIVE_WINDOW_DAYS = 180       
DORMANT_WINDOW_DAYS = 540      

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
    "compliance_notice_issued": "compliance_flag",
    "closure_application": "definitive_close",
}

def match_events_to_ubids():
    session = get_session()
    try:
        events = session.query(ActivityEvent).filter(ActivityEvent.matched_ubid == None).all()
        linkages = session.query(UbidLinkage, SourceRecord).join(
            SourceRecord, 
            (UbidLinkage.source_system == SourceRecord.source_system) & 
            (UbidLinkage.source_id == SourceRecord.source_id)
        ).filter(UbidLinkage.is_active == True).all()

        pan_to_ubid = {}
        name_to_ubid = {}

        for link, rec in linkages:
            ubid = link.ubid
            if rec.pan:
                pan_to_ubid[rec.pan.upper()] = ubid
            if rec.gstin:
                gstin_norm, pan_from_gstin = normalize_gstin(rec.gstin)
                if gstin_norm:
                    pan_to_ubid[gstin_norm] = ubid
                if pan_from_gstin:
                    pan_to_ubid[pan_from_gstin] = ubid
            
            key1 = f"{rec.source_system}:{rec.raw_name}"
            name_to_ubid[key1] = ubid
            if rec.normalized_name:
                key2 = f"{rec.source_system}:{rec.normalized_name}"
                name_to_ubid[key2] = ubid

        matched_count = 0
        unmatched_count = 0

        for event in events:
            raw_id = (event.raw_identifier or "").strip()
            matched_ubid = None
            match_conf = 0.0

            pan = normalize_pan(raw_id)
            if pan and pan in pan_to_ubid:
                matched_ubid = pan_to_ubid[pan]
                match_conf = 0.95

            if not matched_ubid:
                gstin_norm, pan_from_gstin = normalize_gstin(raw_id)
                if gstin_norm and gstin_norm in pan_to_ubid:
                    matched_ubid = pan_to_ubid[gstin_norm]
                    match_conf = 0.95
                elif pan_from_gstin and pan_from_gstin in pan_to_ubid:
                    matched_ubid = pan_to_ubid[pan_from_gstin]
                    match_conf = 0.90

            if not matched_ubid and raw_id in name_to_ubid:
                matched_ubid = name_to_ubid[raw_id]
                match_conf = 0.70

            if not matched_ubid and ":" in raw_id:
                parts = raw_id.split(":", 1)
                if len(parts) == 2:
                    key = f"{parts[0]}:{parts[1]}"
                    if key in name_to_ubid:
                        matched_ubid = name_to_ubid[key]
                        match_conf = 0.65

            if matched_ubid:
                event.matched_ubid = matched_ubid
                event.match_confidence = match_conf
                matched_count += 1
            else:
                unmatched_count += 1

        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    print(f"[ACTIVITY] Matched {matched_count} events to UBIDs, {unmatched_count} unmatched")
    return matched_count, unmatched_count

def classify_business_activity():
    session = get_session()
    try:
        config_manager = ActivityConfigManager(session)
        ubids = session.query(UbidMaster.ubid).filter(UbidMaster.activity_status != 'Merged').all()
        ubids = [row[0] for row in ubids]

        stats = {"Active": 0, "Dormant": 0, "Closed": 0, "Compliance Risk": 0, "Unknown": 0}

        for ubid in ubids:
            events = session.query(ActivityEvent).filter_by(matched_ubid=ubid).order_by(ActivityEvent.event_date.desc()).all()
            events_data = [{"event_type": e.event_type, "event_date": e.event_date} for e in events]
            primary_system = _get_primary_system_for_ubid(session, ubid)
            rule = config_manager.get_rule(primary_system, "all")
            active_window_days = int((rule.active_window_months or 12) * 30)
            dormant_window_days = int((rule.dormant_window_months or 24) * 30)
            
            status, evidence = _classify_single(
                events_data,
                active_window_days=active_window_days,
                dormant_window_days=dormant_window_days,
            )
            evidence["rule_used"] = {
                "department": rule.department,
                "business_type": rule.business_type,
                "active_window_months": rule.active_window_months,
                "dormant_window_months": rule.dormant_window_months,
            }

            master = session.query(UbidMaster).get(ubid)
            if master:
                master.activity_status = status
                master.status_updated_at = datetime.utcnow()
                master.status_evidence = json.dumps(evidence)

            stats[status] += 1

        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    print(f"[ACTIVITY] Classification complete: {stats}")
    return stats


def _get_primary_system_for_ubid(session, ubid):
    systems = (
        session.query(UbidLinkage.source_system)
        .filter(UbidLinkage.ubid == ubid, UbidLinkage.is_active == True)
        .all()
    )
    if not systems:
        return "default"

    counts = defaultdict(int)
    for (source_system,) in systems:
        counts[source_system] += 1

    return max(counts.items(), key=lambda x: x[1])[0]

def _is_seasonal(events):
    if len(events) < 4:
        return False
    months = defaultdict(int)
    years = set()
    for evt in events:
        try:
            d = datetime.strptime(evt["event_date"], "%Y-%m-%d")
            months[d.month] += 1
            years.add(d.year)
        except:
            pass
    if len(years) >= 2 and len(months) <= 3:
        return True
    return False

def _has_compliance_risk(events):
    if len(events) < 3:
        return False
    consecutive_notices = 0
    for evt in events:
        strength = EVENT_SIGNAL_STRENGTH.get(evt["event_type"])
        if strength == "compliance_flag" or strength == "weak":
            consecutive_notices += 1
        else:
            break
            
    return consecutive_notices >= 3

def _classify_single(events, active_window_days=ACTIVE_WINDOW_DAYS, dormant_window_days=DORMANT_WINDOW_DAYS):
    if not events:
        return "Unknown", {
            "rule": "no_events",
            "explanation": "No activity events found for this business.",
            "event_count": 0,
        }

    for evt in events:
        if evt["event_type"] == "closure_application":
            return "Closed", {
                "rule": "explicit_closure",
                "explanation": f"Closure application filed on {evt['event_date']}.",
                "decisive_event": evt["event_type"],
            }

    if _has_compliance_risk(events):
        return "Compliance Risk", {
            "rule": "consecutive_compliance_failures",
            "explanation": "Multiple consecutive compliance notices or failed tests without any positive response."
        }

    most_recent = events[0]
    try:
        most_recent_date = datetime.strptime(most_recent["event_date"], "%Y-%m-%d")
    except (ValueError, TypeError):
        return "Unknown", {"rule": "parse_error"}

    days_since_last = (REFERENCE_DATE - most_recent_date).days

    if days_since_last <= active_window_days:
        status = "Active"
        rule = "recent_activity"
        explanation = f"Activity within {active_window_days} days."
    elif days_since_last <= dormant_window_days:
        is_seasonal = _is_seasonal(events)
        if is_seasonal:
            status = "Active"
            rule = "seasonal_pattern_detected"
            explanation = "Exceeded dormant threshold but historical seasonal clustering was detected."
        else:
            status = "Dormant"
            rule = "stale_activity"
            explanation = f"No activity for {days_since_last} days (between {active_window_days}-{dormant_window_days})."
    else:
        status = "Closed"
        rule = "no_recent_activity"
        explanation = f"No activity for {days_since_last} days. Classified as Closed."

    evidence = {
        "rule": rule,
        "explanation": explanation,
        "days_since_last_activity": days_since_last,
        "most_recent_event": most_recent["event_type"],
    }
    return status, evidence

def run_activity_inference():
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
