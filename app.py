"""
UBID Platform — Flask API Backend

REST API for the Karnataka Unified Business Identifier
and Active Business Intelligence platform.
"""

import json
import os
import sys
from datetime import datetime
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.schema import init_db, get_connection
from engine.resolver import run_resolution
from engine.activity_engine import run_activity_inference
from engine.ubid_manager import merge_ubids, split_ubid
from data.generate_synthetic import generate_all_data

app = Flask(__name__, static_folder="static", static_url_path="")
CORS(app)


# ────────────────────────────────────────────────────────────
# Serve frontend
# ────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


# ────────────────────────────────────────────────────────────
# Dashboard Stats
# ────────────────────────────────────────────────────────────

@app.route("/api/dashboard/stats")
def dashboard_stats():
    conn = get_connection()
    c = conn.cursor()

    stats = {}

    # Total source records
    c.execute("SELECT COUNT(*) as cnt FROM source_records")
    stats["total_source_records"] = c.fetchone()["cnt"]

    # By department
    c.execute("SELECT source_system, COUNT(*) as cnt FROM source_records GROUP BY source_system")
    stats["records_by_system"] = {row["source_system"]: row["cnt"] for row in c.fetchall()}

    # Total UBIDs (exclude Merged)
    c.execute("SELECT COUNT(*) as cnt FROM ubid_master WHERE activity_status != 'Merged'")
    stats["total_ubids"] = c.fetchone()["cnt"]

    # Anchored vs unanchored
    c.execute("SELECT COUNT(*) as cnt FROM ubid_master WHERE anchor_type IS NOT NULL AND activity_status != 'Merged'")
    stats["anchored_ubids"] = c.fetchone()["cnt"]
    stats["unanchored_ubids"] = stats["total_ubids"] - stats["anchored_ubids"]

    # Activity status breakdown
    c.execute("""
        SELECT activity_status, COUNT(*) as cnt
        FROM ubid_master WHERE activity_status != 'Merged'
        GROUP BY activity_status
    """)
    stats["status_breakdown"] = {row["activity_status"]: row["cnt"] for row in c.fetchall()}

    # Total linkages
    c.execute("SELECT COUNT(*) as cnt FROM ubid_linkages WHERE is_active = 1")
    stats["total_linkages"] = c.fetchone()["cnt"]

    # Pending reviews
    c.execute("SELECT COUNT(*) as cnt FROM match_candidates WHERE status = 'pending'")
    stats["pending_reviews"] = c.fetchone()["cnt"]

    # Total events
    c.execute("SELECT COUNT(*) as cnt FROM activity_events")
    stats["total_events"] = c.fetchone()["cnt"]

    # Matched vs unmatched events
    c.execute("SELECT COUNT(*) as cnt FROM activity_events WHERE matched_ubid IS NOT NULL")
    stats["matched_events"] = c.fetchone()["cnt"]
    stats["unmatched_events"] = stats["total_events"] - stats["matched_events"]

    # Resolution ratio
    if stats["total_source_records"] > 0:
        stats["dedup_ratio"] = round(
            (1 - stats["total_ubids"] / stats["total_source_records"]) * 100, 1
        )
    else:
        stats["dedup_ratio"] = 0

    conn.close()
    return jsonify(stats)


# ────────────────────────────────────────────────────────────
# UBID Search & Lookup
# ────────────────────────────────────────────────────────────

@app.route("/api/ubid/search")
def ubid_search():
    q = request.args.get("q", "").strip()
    search_type = request.args.get("type", "auto")  # auto, pan, gstin, name, pincode, source_id

    if not q:
        return jsonify({"results": [], "query": q})

    conn = get_connection()
    c = conn.cursor()
    results = []

    if search_type == "auto":
        # Auto-detect type
        q_upper = q.upper()
        if q_upper.startswith("UBID-"):
            search_type = "ubid"
        elif len(q_upper) == 15 and q_upper[:2].isdigit():
            search_type = "gstin"
        elif len(q_upper) == 10 and q_upper[:5].isalpha():
            search_type = "pan"
        elif q.isdigit() and len(q) == 6:
            search_type = "pincode"
        elif q.startswith("SE-") or q.startswith("FACT-") or q.startswith("LAB-") or q.startswith("KSPCB-"):
            search_type = "source_id"
        else:
            search_type = "name"

    if search_type == "ubid":
        c.execute("SELECT * FROM ubid_master WHERE ubid = ?", (q,))
        row = c.fetchone()
        if row:
            results.append(_ubid_to_dict(dict(row), c))

    elif search_type == "pan":
        c.execute("""
            SELECT DISTINCT um.* FROM ubid_master um
            JOIN ubid_linkages ul ON um.ubid = ul.ubid
            JOIN source_records sr ON ul.source_system = sr.source_system AND ul.source_id = sr.source_id
            WHERE sr.pan = ? AND um.activity_status != 'Merged'
        """, (q.upper(),))
        for row in c.fetchall():
            results.append(_ubid_to_dict(dict(row), c))

        # Also check anchor
        if not results:
            c.execute("SELECT * FROM ubid_master WHERE anchor_value = ? AND activity_status != 'Merged'", (q.upper(),))
            for row in c.fetchall():
                results.append(_ubid_to_dict(dict(row), c))

    elif search_type == "gstin":
        c.execute("""
            SELECT DISTINCT um.* FROM ubid_master um
            JOIN ubid_linkages ul ON um.ubid = ul.ubid
            JOIN source_records sr ON ul.source_system = sr.source_system AND ul.source_id = sr.source_id
            WHERE sr.gstin = ? AND um.activity_status != 'Merged'
        """, (q.upper(),))
        for row in c.fetchall():
            results.append(_ubid_to_dict(dict(row), c))

    elif search_type == "pincode":
        c.execute("""
            SELECT * FROM ubid_master WHERE pincode = ? AND activity_status != 'Merged'
            LIMIT 50
        """, (q,))
        for row in c.fetchall():
            results.append(_ubid_to_dict(dict(row), c))

    elif search_type == "source_id":
        c.execute("""
            SELECT DISTINCT um.* FROM ubid_master um
            JOIN ubid_linkages ul ON um.ubid = ul.ubid
            WHERE ul.source_id = ? AND um.activity_status != 'Merged'
        """, (q,))
        for row in c.fetchall():
            results.append(_ubid_to_dict(dict(row), c))

    elif search_type == "name":
        c.execute("""
            SELECT * FROM ubid_master
            WHERE (canonical_name LIKE ? OR canonical_name LIKE ?)
            AND activity_status != 'Merged'
            LIMIT 50
        """, (f"%{q}%", f"%{q.upper()}%"))
        for row in c.fetchall():
            results.append(_ubid_to_dict(dict(row), c))

    conn.close()
    return jsonify({"results": results, "query": q, "type": search_type})


def _ubid_to_dict(ubid_row, cursor):
    """Convert a UBID row to a rich dict with linked records."""
    ubid = ubid_row["ubid"]

    # Get linked source records
    cursor.execute("""
        SELECT ul.*, sr.raw_name, sr.raw_address, sr.pincode as sr_pincode,
               sr.pan as sr_pan, sr.gstin as sr_gstin, sr.owner_name,
               sr.category, sr.registration_date
        FROM ubid_linkages ul
        JOIN source_records sr ON ul.source_system = sr.source_system AND ul.source_id = sr.source_id
        WHERE ul.ubid = ? AND ul.is_active = 1
    """, (ubid,))
    linked_records = [dict(row) for row in cursor.fetchall()]

    # Get recent events count
    cursor.execute("""
        SELECT COUNT(*) as cnt FROM activity_events WHERE matched_ubid = ?
    """, (ubid,))
    event_count = cursor.fetchone()["cnt"]

    ubid_row["linked_records"] = linked_records
    ubid_row["event_count"] = event_count
    ubid_row["systems_present"] = list(set(r["source_system"] for r in linked_records))

    return ubid_row


@app.route("/api/ubid/<ubid>")
def ubid_detail(ubid):
    conn = get_connection()
    c = conn.cursor()

    c.execute("SELECT * FROM ubid_master WHERE ubid = ?", (ubid,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "UBID not found"}), 404

    result = _ubid_to_dict(dict(row), c)

    # Parse status evidence
    if result.get("status_evidence"):
        try:
            result["status_evidence"] = json.loads(result["status_evidence"])
        except json.JSONDecodeError:
            pass

    # Parse match evidence in linked records
    for rec in result.get("linked_records", []):
        if rec.get("match_evidence"):
            try:
                rec["match_evidence"] = json.loads(rec["match_evidence"])
            except json.JSONDecodeError:
                pass

    conn.close()
    return jsonify(result)


# ────────────────────────────────────────────────────────────
# Activity Timeline
# ────────────────────────────────────────────────────────────

@app.route("/api/ubid/<ubid>/timeline")
def ubid_timeline(ubid):
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
        SELECT * FROM activity_events
        WHERE matched_ubid = ?
        ORDER BY event_date DESC
    """, (ubid,))
    events = []
    for row in c.fetchall():
        evt = dict(row)
        if evt.get("event_details"):
            try:
                evt["event_details"] = json.loads(evt["event_details"])
            except json.JSONDecodeError:
                pass
        events.append(evt)

    # Get UBID status info
    c.execute("SELECT activity_status, status_evidence FROM ubid_master WHERE ubid = ?", (ubid,))
    ubid_row = c.fetchone()
    status_info = {}
    if ubid_row:
        status_info["status"] = ubid_row["activity_status"]
        if ubid_row["status_evidence"]:
            try:
                status_info["evidence"] = json.loads(ubid_row["status_evidence"])
            except json.JSONDecodeError:
                status_info["evidence"] = ubid_row["status_evidence"]

    conn.close()
    return jsonify({"ubid": ubid, "events": events, "status_info": status_info})


# ────────────────────────────────────────────────────────────
# Reviewer Workflow
# ────────────────────────────────────────────────────────────

@app.route("/api/review/pending")
def review_pending():
    conn = get_connection()
    c = conn.cursor()

    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 20))
    offset = (page - 1) * per_page

    # Get total count
    c.execute("SELECT COUNT(*) as cnt FROM match_candidates WHERE status = 'pending'")
    total = c.fetchone()["cnt"]

    # Get pending matches with record details
    c.execute("""
        SELECT mc.*,
               a.raw_name as name_a, a.raw_address as addr_a, a.pincode as pin_a,
               a.pan as pan_a, a.gstin as gstin_a, a.owner_name as owner_a,
               a.source_system as sys_a, a.source_id as sid_a, a.category as cat_a,
               b.raw_name as name_b, b.raw_address as addr_b, b.pincode as pin_b,
               b.pan as pan_b, b.gstin as gstin_b, b.owner_name as owner_b,
               b.source_system as sys_b, b.source_id as sid_b, b.category as cat_b
        FROM match_candidates mc
        JOIN source_records a ON mc.record_a_id = a.id
        JOIN source_records b ON mc.record_b_id = b.id
        WHERE mc.status = 'pending'
        ORDER BY mc.similarity_score DESC
        LIMIT ? OFFSET ?
    """, (per_page, offset))

    matches = []
    for row in c.fetchall():
        match = dict(row)
        if match.get("match_evidence"):
            try:
                match["match_evidence"] = json.loads(match["match_evidence"])
            except json.JSONDecodeError:
                pass
        matches.append(match)

    conn.close()
    return jsonify({
        "matches": matches,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": (total + per_page - 1) // per_page if per_page > 0 else 0,
    })


@app.route("/api/review/<int:match_id>/decide", methods=["POST"])
def review_decide(match_id):
    data = request.get_json()
    decision = data.get("decision")  # "merge", "reject", "defer"
    notes = data.get("notes", "")
    reviewer = data.get("reviewer", "reviewer")

    if decision not in ("merge", "reject", "defer"):
        return jsonify({"error": "Decision must be 'merge', 'reject', or 'defer'"}), 400

    conn = get_connection()
    c = conn.cursor()

    # Get match details
    c.execute("SELECT * FROM match_candidates WHERE id = ?", (match_id,))
    match = c.fetchone()
    if not match:
        conn.close()
        return jsonify({"error": "Match not found"}), 404

    # Update match status
    c.execute("""
        UPDATE match_candidates
        SET status = ?, reviewed_by = ?, reviewed_at = datetime('now'), reviewer_notes = ?
        WHERE id = ?
    """, (decision, reviewer, notes, match_id))

    # Record feedback (for model improvement)
    c.execute("""
        INSERT INTO reviewer_feedback (match_id, decision, confidence_at_decision, reviewer_notes)
        VALUES (?, ?, ?, ?)
    """, (match_id, decision, match["similarity_score"], notes))

    # If merge, handle UBID linkage
    if decision == "merge":
        rec_a_id = match["record_a_id"]
        rec_b_id = match["record_b_id"]

        # Get source records
        c.execute("SELECT * FROM source_records WHERE id = ?", (rec_a_id,))
        rec_a = dict(c.fetchone())
        c.execute("SELECT * FROM source_records WHERE id = ?", (rec_b_id,))
        rec_b = dict(c.fetchone())

        # Find existing UBIDs for both records
        c.execute("""
            SELECT ubid FROM ubid_linkages
            WHERE source_system = ? AND source_id = ? AND is_active = 1
        """, (rec_a["source_system"], rec_a["source_id"]))
        ubid_a_row = c.fetchone()

        c.execute("""
            SELECT ubid FROM ubid_linkages
            WHERE source_system = ? AND source_id = ? AND is_active = 1
        """, (rec_b["source_system"], rec_b["source_id"]))
        ubid_b_row = c.fetchone()

        if ubid_a_row and ubid_b_row and ubid_a_row["ubid"] != ubid_b_row["ubid"]:
            # Merge the two UBIDs
            merge_ubids(ubid_a_row["ubid"], ubid_b_row["ubid"],
                       merged_by=reviewer, reason=notes)

    # Audit log
    c.execute("""
        INSERT INTO audit_log (action_type, details, performed_by)
        VALUES (?, ?, ?)
    """, ("review_decision", json.dumps({
        "match_id": match_id,
        "decision": decision,
        "notes": notes,
    }), reviewer))

    conn.commit()
    conn.close()

    return jsonify({"status": "ok", "decision": decision, "match_id": match_id})


# ────────────────────────────────────────────────────────────
# Unmatched Events
# ────────────────────────────────────────────────────────────

@app.route("/api/events/unmatched")
def unmatched_events():
    conn = get_connection()
    c = conn.cursor()

    c.execute("""
        SELECT * FROM activity_events
        WHERE matched_ubid IS NULL
        ORDER BY event_date DESC
        LIMIT 100
    """)
    events = []
    for row in c.fetchall():
        evt = dict(row)
        if evt.get("event_details"):
            try:
                evt["event_details"] = json.loads(evt["event_details"])
            except json.JSONDecodeError:
                pass
        events.append(evt)

    conn.close()
    return jsonify({"events": events, "total": len(events)})


# ────────────────────────────────────────────────────────────
# Analytics & Query Builder
# ────────────────────────────────────────────────────────────

@app.route("/api/query/active-no-inspection")
def query_active_no_inspection():
    """Active factories in a pincode with no inspection in N months."""
    pincode = request.args.get("pincode", "560058")
    months = int(request.args.get("months", 18))

    conn = get_connection()
    c = conn.cursor()

    # Find active UBIDs in the pincode that are linked to factories
    c.execute("""
        SELECT DISTINCT um.ubid, um.canonical_name, um.canonical_address,
               um.pincode, um.activity_status, um.anchor_value
        FROM ubid_master um
        JOIN ubid_linkages ul ON um.ubid = ul.ubid
        WHERE um.pincode = ?
        AND um.activity_status = 'Active'
        AND ul.source_system = 'factories'
        AND ul.is_active = 1
    """, (pincode,))

    candidates = [dict(row) for row in c.fetchall()]

    # Filter: no inspection in last N months
    from datetime import timedelta
    cutoff = datetime(2025, 4, 1) - timedelta(days=months * 30)
    cutoff_str = cutoff.strftime("%Y-%m-%d")

    results = []
    for biz in candidates:
        c.execute("""
            SELECT COUNT(*) as cnt FROM activity_events
            WHERE matched_ubid = ?
            AND event_type = 'inspection_conducted'
            AND event_date >= ?
        """, (biz["ubid"], cutoff_str))

        inspection_count = c.fetchone()["cnt"]

        if inspection_count == 0:
            # Get last inspection date
            c.execute("""
                SELECT event_date FROM activity_events
                WHERE matched_ubid = ? AND event_type = 'inspection_conducted'
                ORDER BY event_date DESC LIMIT 1
            """, (biz["ubid"],))
            last_insp = c.fetchone()
            biz["last_inspection"] = last_insp["event_date"] if last_insp else "Never"
            biz["months_without_inspection"] = months
            results.append(biz)

    conn.close()
    return jsonify({
        "query": f"Active factories in {pincode} with no inspection in {months} months",
        "results": results,
        "total": len(results),
        "cutoff_date": cutoff_str,
    })


@app.route("/api/query/custom")
def query_custom():
    """Custom query builder."""
    status = request.args.get("status")
    pincode = request.args.get("pincode")
    department = request.args.get("department")
    category = request.args.get("category")

    conn = get_connection()
    c = conn.cursor()

    query = """
        SELECT DISTINCT um.ubid, um.canonical_name, um.canonical_address,
               um.pincode, um.activity_status, um.anchor_type, um.anchor_value
        FROM ubid_master um
        JOIN ubid_linkages ul ON um.ubid = ul.ubid
        JOIN source_records sr ON ul.source_system = sr.source_system AND ul.source_id = sr.source_id
        WHERE um.activity_status != 'Merged'
    """
    params = []

    if status:
        query += " AND um.activity_status = ?"
        params.append(status)
    if pincode:
        query += " AND um.pincode = ?"
        params.append(pincode)
    if department:
        query += " AND ul.source_system = ?"
        params.append(department)
    if category:
        query += " AND sr.category LIKE ?"
        params.append(f"%{category}%")

    query += " LIMIT 100"

    c.execute(query, params)
    results = [dict(row) for row in c.fetchall()]

    conn.close()
    return jsonify({"results": results, "total": len(results)})


# ────────────────────────────────────────────────────────────
# Pipeline Control
# ────────────────────────────────────────────────────────────

@app.route("/api/pipeline/generate", methods=["POST"])
def pipeline_generate():
    """Generate synthetic data."""
    num_records, num_events = generate_all_data()
    return jsonify({
        "status": "ok",
        "records_generated": num_records,
        "events_generated": num_events,
    })


@app.route("/api/pipeline/resolve", methods=["POST"])
def pipeline_resolve():
    """Run entity resolution."""
    summary = run_resolution()
    return jsonify({"status": "ok", "summary": summary})


@app.route("/api/pipeline/activity", methods=["POST"])
def pipeline_activity():
    """Run activity inference."""
    result = run_activity_inference()
    return jsonify({"status": "ok", "result": result})


@app.route("/api/pipeline/full", methods=["POST"])
def pipeline_full():
    """Run full pipeline: generate → resolve → activity."""
    gen_records, gen_events = generate_all_data()
    res_summary = run_resolution()
    act_result = run_activity_inference()
    return jsonify({
        "status": "ok",
        "generation": {"records": gen_records, "events": gen_events},
        "resolution": res_summary,
        "activity": act_result,
    })


@app.route("/api/audit/log")
def audit_log():
    """Get recent audit log entries."""
    limit = int(request.args.get("limit", 50))
    conn = get_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM audit_log ORDER BY performed_at DESC LIMIT ?", (limit,))
    entries = []
    for row in c.fetchall():
        entry = dict(row)
        if entry.get("details"):
            try:
                entry["details"] = json.loads(entry["details"])
            except json.JSONDecodeError:
                pass
        entries.append(entry)
    conn.close()
    return jsonify({"entries": entries})


# ────────────────────────────────────────────────────────────
# Resolution Stats
# ────────────────────────────────────────────────────────────

@app.route("/api/resolution/stats")
def resolution_stats():
    conn = get_connection()
    c = conn.cursor()

    # Confidence distribution
    c.execute("""
        SELECT
            CASE
                WHEN similarity_score >= 0.85 THEN 'auto_linked'
                WHEN similarity_score >= 0.55 THEN 'review'
                ELSE 'separate'
            END as category,
            COUNT(*) as cnt,
            AVG(similarity_score) as avg_score
        FROM match_candidates
        GROUP BY category
    """)
    confidence_dist = {row["category"]: {"count": row["cnt"], "avg_score": round(row["avg_score"], 4)}
                      for row in c.fetchall()}

    # Reviewer decisions
    c.execute("""
        SELECT decision, COUNT(*) as cnt
        FROM reviewer_feedback
        GROUP BY decision
    """)
    reviewer_stats = {row["decision"]: row["cnt"] for row in c.fetchall()}

    # Multi-system linkages
    c.execute("""
        SELECT ubid, COUNT(DISTINCT source_system) as sys_count
        FROM ubid_linkages WHERE is_active = 1
        GROUP BY ubid
        HAVING sys_count > 1
    """)
    multi_system = len(c.fetchall())

    conn.close()
    return jsonify({
        "confidence_distribution": confidence_dist,
        "reviewer_decisions": reviewer_stats,
        "multi_system_linkages": multi_system,
    })


if __name__ == "__main__":
    init_db()
    app.run(debug=True, port=5000, host="0.0.0.0")
