"""
UBID Platform — Flask API Backend
"""

import json
import csv
import io
import os
import re
import sys
from collections import defaultdict
from difflib import SequenceMatcher
from datetime import datetime, timedelta
from functools import wraps
from urllib import request as urlrequest
from flask import Flask, request, jsonify, send_from_directory, Response
from flask_cors import CORS
from sqlalchemy import func, or_, desc, text
from sqlalchemy.orm import aliased

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.schema import init_db, get_session
from database.models import (
    SourceRecord,
    UbidMaster,
    UbidLinkage,
    MatchCandidate,
    ActivityEvent,
    AuditLog,
    ReviewerFeedback,
    CalibrationLog,
    ActivityRule,
    FeedbackProcessingLog,
    GoldenRecord,
    ReviewEscalation,
    WatchlistAlert,
    WebhookSubscription,
    ReportSchedule,
)
from engine.resolver import run_resolution
from engine.activity_engine import run_activity_inference
from engine.ubid_manager import merge_ubids, split_ubid
from engine.matcher import get_calibrated_thresholds
from data.generate_synthetic import generate_all_data

app = Flask(__name__, static_folder="static", static_url_path="")
CORS(app)


# ────────────────────────────────────────────────────────────
# Serve frontend
# ────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/api/dashboard/stats")
def dashboard_stats():
    session = get_session()
    stats = {}

    try:
        stats["total_source_records"] = session.query(SourceRecord).count()

        dept_counts = session.query(SourceRecord.source_system, func.count(SourceRecord.id)).group_by(SourceRecord.source_system).all()
        stats["records_by_system"] = {sys: cnt for sys, cnt in dept_counts}

        active_ubids_query = session.query(UbidMaster).filter(UbidMaster.activity_status != 'Merged')
        stats["total_ubids"] = active_ubids_query.count()

        anchored_ubids = active_ubids_query.filter(UbidMaster.anchor_type != None).count()
        stats["anchored_ubids"] = anchored_ubids
        stats["unanchored_ubids"] = stats["total_ubids"] - anchored_ubids

        status_counts = session.query(UbidMaster.activity_status, func.count(UbidMaster.ubid)).filter(UbidMaster.activity_status != 'Merged').group_by(UbidMaster.activity_status).all()
        stats["status_breakdown"] = {st: cnt for st, cnt in status_counts}

        stats["total_linkages"] = session.query(UbidLinkage).filter(UbidLinkage.is_active == True).count()

        stats["pending_reviews"] = session.query(MatchCandidate).filter(MatchCandidate.status == 'pending').count()

        stats["total_events"] = session.query(ActivityEvent).count()

        stats["matched_events"] = session.query(ActivityEvent).filter(ActivityEvent.matched_ubid != None).count()
        stats["unmatched_events"] = stats["total_events"] - stats["matched_events"]

        if stats["total_source_records"] > 0:
            stats["dedup_ratio"] = round((1 - stats["total_ubids"] / stats["total_source_records"]) * 100, 1)
        else:
            stats["dedup_ratio"] = 0
            
    finally:
        session.close()

    return jsonify(stats)

@app.route("/api/ubid/search")
def ubid_search():
    q = request.args.get("q", "").strip()
    search_type = request.args.get("type", "auto")

    if not q:
        return jsonify({"results": [], "query": q})

    session = get_session()
    results = []

    try:
        if search_type == "auto":
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
            row = session.query(UbidMaster).filter_by(ubid=q).first()
            if row:
                results.append(_ubid_to_dict(row, session))

        elif search_type == "pan":
            q_upper = q.upper()
            linked_m = session.query(UbidMaster).join(
                UbidLinkage, UbidMaster.ubid == UbidLinkage.ubid
            ).join(
                SourceRecord, (UbidLinkage.source_system == SourceRecord.source_system) & (UbidLinkage.source_id == SourceRecord.source_id)
            ).filter(SourceRecord.pan == q_upper, UbidMaster.activity_status != 'Merged').all()
            
            for rm in set(linked_m):
                results.append(_ubid_to_dict(rm, session))

            if not results:
                anchored = session.query(UbidMaster).filter(UbidMaster.anchor_value == q_upper, UbidMaster.activity_status != 'Merged').all()
                for rm in anchored:
                    results.append(_ubid_to_dict(rm, session))

        elif search_type == "gstin":
            linked_m = session.query(UbidMaster).join(
                UbidLinkage, UbidMaster.ubid == UbidLinkage.ubid
            ).join(
                SourceRecord, (UbidLinkage.source_system == SourceRecord.source_system) & (UbidLinkage.source_id == SourceRecord.source_id)
            ).filter(SourceRecord.gstin == q.upper(), UbidMaster.activity_status != 'Merged').all()
            for rm in set(linked_m):
                results.append(_ubid_to_dict(rm, session))

        elif search_type == "pincode":
            rms = session.query(UbidMaster).filter(UbidMaster.pincode == q, UbidMaster.activity_status != 'Merged').limit(50).all()
            for rm in rms:
                results.append(_ubid_to_dict(rm, session))

        elif search_type == "source_id":
            linked_m = session.query(UbidMaster).join(
                UbidLinkage, UbidMaster.ubid == UbidLinkage.ubid
            ).filter(UbidLinkage.source_id == q, UbidMaster.activity_status != 'Merged').all()
            for rm in set(linked_m):
                results.append(_ubid_to_dict(rm, session))

        elif search_type == "name":
            rms = session.query(UbidMaster).filter(
                or_(
                    UbidMaster.canonical_name.ilike(f"%{q}%"),
                    UbidMaster.canonical_name.ilike(f"%{q.upper()}%")
                ),
                UbidMaster.activity_status != 'Merged'
            ).limit(50).all()
            for rm in rms:
                results.append(_ubid_to_dict(rm, session))
                
    finally:
        session.close()

    return jsonify({"results": results, "query": q, "type": search_type})


def _ubid_to_dict(ubid_row, session):
    ubid = ubid_row.ubid
    row_dict = {
        "ubid": ubid,
        "canonical_name": ubid_row.canonical_name,
        "canonical_address": ubid_row.canonical_address,
        "pincode": ubid_row.pincode,
        "activity_status": ubid_row.activity_status,
        "anchor_type": ubid_row.anchor_type,
        "anchor_value": ubid_row.anchor_value,
        "status_updated_at": ubid_row.status_updated_at.isoformat() if ubid_row.status_updated_at else None,
        "status_evidence": ubid_row.status_evidence,
        "created_at": ubid_row.created_at.isoformat() if ubid_row.created_at else None
    }

    links_query = session.query(UbidLinkage, SourceRecord).join(
        SourceRecord, (UbidLinkage.source_system == SourceRecord.source_system) & (UbidLinkage.source_id == SourceRecord.source_id)
    ).filter(UbidLinkage.ubid == ubid, UbidLinkage.is_active == True).all()

    linked_records = []
    for link, rec in links_query:
        linked_records.append({
            "source_system": link.source_system,
            "source_id": link.source_id,
            "confidence_score": link.confidence_score,
            "match_evidence": link.match_evidence,
            "linked_by": link.linked_by,
            "raw_name": rec.raw_name,
            "raw_address": rec.raw_address,
            "sr_pincode": rec.pincode,
            "sr_pan": rec.pan,
            "sr_gstin": rec.gstin,
            "owner_name": rec.owner_name,
            "category": rec.category,
            "registration_date": rec.registration_date
        })

    event_count = session.query(ActivityEvent).filter_by(matched_ubid=ubid).count()

    row_dict["linked_records"] = linked_records
    row_dict["event_count"] = event_count
    row_dict["systems_present"] = list(set(r["source_system"] for r in linked_records))

    return row_dict

@app.route("/api/ubid/<ubid>")
def ubid_detail(ubid):
    session = get_session()
    try:
        row = session.query(UbidMaster).filter_by(ubid=ubid).first()
        if not row:
            return jsonify({"error": "UBID not found"}), 404

        result = _ubid_to_dict(row, session)

        if result.get("status_evidence"):
            try:
                result["status_evidence"] = json.loads(result["status_evidence"])
            except json.JSONDecodeError:
                pass

        for rec in result.get("linked_records", []):
            if rec.get("match_evidence"):
                try:
                    rec["match_evidence"] = json.loads(rec["match_evidence"])
                except json.JSONDecodeError:
                    pass
                    
        return jsonify(result)
    finally:
        session.close()

@app.route("/api/ubid/<ubid>/timeline")
def ubid_timeline(ubid):
    session = get_session()
    try:
        evts = session.query(ActivityEvent).filter_by(matched_ubid=ubid).order_by(desc(ActivityEvent.event_date)).all()
        events = []
        for e in evts:
            evt = {
                "source_system": e.source_system,
                "event_type": e.event_type,
                "event_date": e.event_date,
                "raw_identifier": e.raw_identifier,
                "match_confidence": e.match_confidence
            }
            if e.event_details:
                try:
                    evt["event_details"] = json.loads(e.event_details)
                except json.JSONDecodeError:
                    evt["event_details"] = e.event_details
            events.append(evt)

        ubid_row = session.query(UbidMaster).filter_by(ubid=ubid).first()
        status_info = {}
        if ubid_row:
            status_info["status"] = ubid_row.activity_status
            if ubid_row.status_evidence:
                try:
                    status_info["evidence"] = json.loads(ubid_row.status_evidence)
                except json.JSONDecodeError:
                    status_info["evidence"] = ubid_row.status_evidence

        return jsonify({"ubid": ubid, "events": events, "status_info": status_info})
    finally:
        session.close()

@app.route("/api/review/pending")
def review_pending():
    session = get_session()
    
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 20))
    offset = (page - 1) * per_page

    try:
        total = session.query(MatchCandidate).filter_by(status='pending').count()

        SrcA = aliased(SourceRecord)
        SrcB = aliased(SourceRecord)

        query = session.query(MatchCandidate, SrcA, SrcB).join(
            SrcA, MatchCandidate.record_a_id == SrcA.id
        ).join(
            SrcB, MatchCandidate.record_b_id == SrcB.id
        ).filter(
            MatchCandidate.status == 'pending'
        ).order_by(
            desc(MatchCandidate.similarity_score)
        ).offset(offset).limit(per_page)

        orm_results = query.all()
        
        matches = []
        for mc, a, b in orm_results:
            match = {
                "id": mc.id,
                "record_a_id": mc.record_a_id,
                "record_b_id": mc.record_b_id,
                "similarity_score": mc.similarity_score,
                "match_evidence": mc.match_evidence,
                "name_a": a.raw_name, "addr_a": a.raw_address, "pin_a": a.pincode,
                "pan_a": a.pan, "gstin_a": a.gstin, "owner_a": a.owner_name,
                "sys_a": a.source_system, "sid_a": a.source_id, "cat_a": a.category,
                "name_b": b.raw_name, "addr_b": b.raw_address, "pin_b": b.pincode,
                "pan_b": b.pan, "gstin_b": b.gstin, "owner_b": b.owner_name,
                "sys_b": b.source_system, "sid_b": b.source_id, "cat_b": b.category,
            }
            if match.get("match_evidence"):
                try:
                    match["match_evidence"] = json.loads(match["match_evidence"])
                except json.JSONDecodeError:
                    pass
            matches.append(match)

        return jsonify({
            "matches": matches,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": (total + per_page - 1) // per_page if per_page > 0 else 0,
        })
    finally:
        session.close()

@app.route("/api/review/<int:match_id>/decide", methods=["POST"])
def review_decide(match_id):
    data = request.get_json() or {}
    raw_decision = data.get("decision")
    decision_aliases = {
        "merge_link": "merge",
        "merge_and_link": "merge",
        "keep_separate": "reject",
        "separate": "reject",
    }
    decision = decision_aliases.get(raw_decision, raw_decision)
    notes = data.get("notes", "")
    reviewer = data.get("reviewer", "reviewer")

    if decision not in ("merge", "reject", "defer"):
        return jsonify({"error": "Decision must be 'merge', 'reject', or 'defer'"}), 400

    session = get_session()
    try:
        match = session.query(MatchCandidate).get(match_id)
        if not match:
            return jsonify({"error": "Match not found"}), 404

        match.status = decision
        match.reviewed_by = reviewer
        match.reviewed_at = datetime.utcnow()
        match.reviewer_notes = notes

        feedback = ReviewerFeedback(
            match_id=match_id,
            decision=decision,
            confidence_at_decision=match.similarity_score,
            reviewer_notes=notes
        )
        session.add(feedback)

        if decision == "merge":
            rec_a = session.query(SourceRecord).get(match.record_a_id)
            rec_b = session.query(SourceRecord).get(match.record_b_id)

            link_a = session.query(UbidLinkage).filter_by(source_system=rec_a.source_system, source_id=rec_a.source_id, is_active=True).first()
            link_b = session.query(UbidLinkage).filter_by(source_system=rec_b.source_system, source_id=rec_b.source_id, is_active=True).first()

            if link_a and link_b and link_a.ubid != link_b.ubid:
                merge_ubids(link_a.ubid, link_b.ubid, merged_by=reviewer, reason=notes, session=session)
            elif link_a and not link_b:
                from engine.ubid_manager import link_record_to_ubid
                link_record_to_ubid(
                    link_a.ubid,
                    rec_b.source_system,
                    rec_b.source_id,
                    1.0,
                    {"method": "human_reviewed"},
                    linked_by=f"human:{reviewer}",
                    session=session,
                )
            elif link_b and not link_a:
                from engine.ubid_manager import link_record_to_ubid
                link_record_to_ubid(
                    link_b.ubid,
                    rec_a.source_system,
                    rec_a.source_id,
                    1.0,
                    {"method": "human_reviewed"},
                    linked_by=f"human:{reviewer}",
                    session=session,
                )
            else:
                from engine.ubid_manager import generate_ubid, create_ubid_record, link_record_to_ubid
                result = generate_ubid(rec_a.pincode, rec_a.pan, rec_a.gstin, session=session)
                if isinstance(result, tuple):
                    new_ubid, anchor_type, anchor_value = result
                else:
                    new_ubid = result
                    anchor_type = anchor_value = None
                
                create_ubid_record(
                    new_ubid,
                    anchor_type,
                    anchor_value,
                    rec_a.normalized_name or rec_a.raw_name,
                    rec_a.normalized_address or rec_a.raw_address,
                    rec_a.pincode,
                    session=session,
                )
                
                link_record_to_ubid(
                    new_ubid,
                    rec_a.source_system,
                    rec_a.source_id,
                    1.0,
                    {"method": "human_reviewed"},
                    linked_by=f"human:{reviewer}",
                    session=session,
                )
                link_record_to_ubid(
                    new_ubid,
                    rec_b.source_system,
                    rec_b.source_id,
                    1.0,
                    {"method": "human_reviewed"},
                    linked_by=f"human:{reviewer}",
                    session=session,
                )

        audit = AuditLog(
            action_type="review_decision",
            details=json.dumps({
                "match_id": match_id,
                "decision": decision,
                "notes": notes,
            }),
            performed_by=reviewer
        )
        session.add(audit)
        session.commit()

        pending_reviews = session.query(MatchCandidate).filter(MatchCandidate.status == 'pending').count()
        return jsonify({
            "status": "ok",
            "decision": decision,
            "match_id": match_id,
            "pending_reviews": pending_reviews,
        })
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

@app.route("/api/events/unmatched")
def unmatched_events():
    session = get_session()
    try:
        evts = session.query(ActivityEvent).filter_by(matched_ubid=None).order_by(desc(ActivityEvent.event_date)).limit(100).all()
        events = []
        for e in evts:
            evt = {
                "source_system": e.source_system,
                "event_type": e.event_type,
                "event_date": e.event_date,
                "raw_identifier": e.raw_identifier
            }
            if e.event_details:
                try:
                    evt["event_details"] = json.loads(e.event_details)
                except json.JSONDecodeError:
                    evt["event_details"] = e.event_details
            events.append(evt)

        return jsonify({"events": events, "total": len(events)})
    finally:
        session.close()

@app.route("/api/query/active-no-inspection")
def query_active_no_inspection():
    pincode = request.args.get("pincode", "560058")
    months = int(request.args.get("months", 18))

    session = get_session()
    try:
        um_query = session.query(UbidMaster).join(
            UbidLinkage, UbidMaster.ubid == UbidLinkage.ubid
        ).filter(
            UbidMaster.pincode == pincode,
            UbidMaster.activity_status == 'Active',
            UbidLinkage.source_system == 'factories',
            UbidLinkage.is_active == True
        ).all()
        
        candidates = list(set(um_query))

        from datetime import timedelta
        cutoff = datetime(2025, 4, 1) - timedelta(days=months * 30)
        cutoff_str = cutoff.strftime("%Y-%m-%d")

        results = []
        for c in candidates:
            insp_count = session.query(ActivityEvent).filter(
                ActivityEvent.matched_ubid == c.ubid,
                ActivityEvent.event_type == 'inspection_conducted',
                ActivityEvent.event_date >= cutoff_str
            ).count()

            if insp_count == 0:
                last_insp = session.query(ActivityEvent).filter(
                    ActivityEvent.matched_ubid == c.ubid,
                    ActivityEvent.event_type == 'inspection_conducted'
                ).order_by(desc(ActivityEvent.event_date)).first()

                results.append({
                    "ubid": c.ubid,
                    "canonical_name": c.canonical_name,
                    "canonical_address": c.canonical_address,
                    "pincode": c.pincode,
                    "activity_status": c.activity_status,
                    "anchor_value": c.anchor_value,
                    "last_inspection": last_insp.event_date if last_insp else "Never",
                    "months_without_inspection": months
                })

        return jsonify({
            "query": f"Active factories in {pincode} with no inspection in {months} months",
            "results": results,
            "total": len(results),
            "cutoff_date": cutoff_str,
        })
    finally:
        session.close()

@app.route("/api/query/custom")
def query_custom():
    status = request.args.get("status")
    pincode = request.args.get("pincode")
    department = request.args.get("department")
    category = request.args.get("category")

    session = get_session()
    try:
        q = session.query(UbidMaster).join(
            UbidLinkage, UbidMaster.ubid == UbidLinkage.ubid
        ).join(
            SourceRecord, (UbidLinkage.source_system == SourceRecord.source_system) & (UbidLinkage.source_id == SourceRecord.source_id)
        ).filter(UbidMaster.activity_status != 'Merged')

        if status:
            q = q.filter(UbidMaster.activity_status == status)
        if pincode:
            q = q.filter(UbidMaster.pincode == pincode)
        if department:
            q = q.filter(UbidLinkage.source_system == department)
        if category:
            q = q.filter(SourceRecord.category.ilike(f"%{category}%"))

        q = q.limit(100)
        items = list(set(q.all()))

        results = [{
            "ubid": m.ubid,
            "canonical_name": m.canonical_name,
            "canonical_address": m.canonical_address,
            "pincode": m.pincode,
            "activity_status": m.activity_status,
            "anchor_type": m.anchor_type,
            "anchor_value": m.anchor_value
        } for m in items]

        return jsonify({"results": results, "total": len(results)})
    finally:
        session.close()

@app.route("/api/pipeline/generate", methods=["POST"])
def pipeline_generate():
    num_records, num_events = generate_all_data()
    return jsonify({
        "status": "ok",
        "records_generated": num_records,
        "events_generated": num_events,
    })

@app.route("/api/pipeline/resolve", methods=["POST"])
def pipeline_resolve():
    summary = run_resolution()
    return jsonify({"status": "ok", "summary": summary})

@app.route("/api/pipeline/activity", methods=["POST"])
def pipeline_activity():
    result = run_activity_inference()
    return jsonify({"status": "ok", "result": result})

@app.route("/api/pipeline/full", methods=["POST"])
def pipeline_full():
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
    limit = int(request.args.get("limit", 50))
    session = get_session()
    try:
        entries = session.query(AuditLog).order_by(desc(AuditLog.performed_at)).limit(limit).all()
        result = []
        for e in entries:
            entry = {
                "id": e.id,
                "action_type": e.action_type,
                "ubid": e.ubid,
                "details": e.details,
                "performed_by": e.performed_by,
                "performed_at": e.performed_at.isoformat() if e.performed_at else None
            }
            if entry["details"]:
                try:
                    entry["details"] = json.loads(entry["details"])
                except json.JSONDecodeError:
                    pass
            result.append(entry)
        return jsonify({"entries": result})
    finally:
        session.close()

@app.route("/api/resolution/stats")
def resolution_stats():
    session = get_session()
    try:
        thresholds = get_calibrated_thresholds()
        matches = session.query(MatchCandidate.similarity_score).all()
        confidence_dist = {
            "auto_linked": {"count": 0, "avg_score": 0.0},
            "review": {"count": 0, "avg_score": 0.0},
            "separate": {"count": 0, "avg_score": 0.0}
        }
        sums = {"auto_linked": 0, "review": 0, "separate": 0}
        
        for (score,) in matches:
            if score >= thresholds["auto_link"]:
                cat = "auto_linked"
            elif score >= thresholds["review_lower"]:
                cat = "review"
            else:
                cat = "separate"
            confidence_dist[cat]["count"] += 1
            sums[cat] += score
            
        for cat in confidence_dist:
            if confidence_dist[cat]["count"] > 0:
                confidence_dist[cat]["avg_score"] = round(sums[cat] / confidence_dist[cat]["count"], 4)

        r_stats = session.query(ReviewerFeedback.decision, func.count(ReviewerFeedback.id)).group_by(ReviewerFeedback.decision).all()
        reviewer_stats = {d: cnt for d, cnt in r_stats}

        multi_system = session.query(UbidLinkage.ubid).filter_by(is_active=True).group_by(UbidLinkage.ubid).having(func.count(func.distinct(UbidLinkage.source_system)) > 1).count()

        return jsonify({
            "confidence_distribution": confidence_dist,
            "reviewer_decisions": reviewer_stats,
            "multi_system_linkages": multi_system,
        })
    finally:
        session.close()


def _serialize_activity_rule(rule):
    return {
        "id": rule.id,
        "department": rule.department,
        "business_type": rule.business_type,
        "active_window_months": rule.active_window_months,
        "dormant_window_months": rule.dormant_window_months,
        "min_consumption_kwh": rule.min_consumption_kwh,
        "min_consumption_liters": rule.min_consumption_liters,
        "renewal_weight": rule.renewal_weight,
        "inspection_weight": rule.inspection_weight,
        "compliance_filing_weight": rule.compliance_filing_weight,
        "consumption_weight": rule.consumption_weight,
        "notice_weight": rule.notice_weight,
        "updated_at": rule.updated_at.isoformat() if rule.updated_at else None,
    }


@app.route("/api/admin/calibrate", methods=["GET"])
def get_calibration_status():
    from calibration import ConfidenceCalibrator

    session = get_session()
    try:
        calibrator = ConfidenceCalibrator(session)
        thresholds = calibrator.estimate_thresholds_from_feedback()
        suggestions = calibrator.suggest_threshold_adjustments()

        return jsonify({
            "current_thresholds": suggestions["current"],
            "recommended_thresholds": thresholds,
            "suggestions": suggestions,
            "feedback_available": session.query(ReviewerFeedback).count(),
        })
    finally:
        session.close()


@app.route("/api/admin/apply-calibration", methods=["POST"])
def apply_calibration():
    from calibration import ConfidenceCalibrator

    data = request.get_json() or {}
    applied_by = data.get("applied_by", "admin")

    session = get_session()
    try:
        calibrator = ConfidenceCalibrator(session)
        recommended = calibrator.apply_recommended_thresholds(applied_by=applied_by)

        return jsonify({
            "status": "applied",
            "new_thresholds": recommended,
            "message": "Thresholds updated. New matches will use these values.",
        })
    finally:
        session.close()


@app.route("/api/admin/feedback-summary", methods=["GET"])
def get_feedback_summary():
    from feedback_learner import FeedbackLearner

    session = get_session()
    try:
        total_feedback = session.query(ReviewerFeedback).count()
        applied_feedback = session.query(FeedbackProcessingLog).count()
        pending_feedback = max(total_feedback - applied_feedback, 0)

        learner = FeedbackLearner(session)
        weights = learner.get_weight_summary()

        recent_matches = (
            session.query(ReviewerFeedback)
            .filter(
                ReviewerFeedback.decision == "merge",
                ReviewerFeedback.decided_at >= datetime.utcnow() - timedelta(days=7),
            )
            .count()
        )

        return jsonify({
            "total_feedback": total_feedback,
            "applied_feedback": applied_feedback,
            "pending_feedback": pending_feedback,
            "current_weights": weights,
            "weekly_match_rate": recent_matches,
            "learning_active": pending_feedback > 0,
        })
    finally:
        session.close()


@app.route("/api/admin/process-feedback", methods=["POST"])
def process_pending_feedback():
    from feedback_learner import FeedbackLearner

    session = get_session()
    try:
        learner = FeedbackLearner(session)
        results = learner.process_unapplied_feedback()
        return jsonify({
            "status": "processed",
            "results_count": len(results),
            "details": results[:10],
        })
    finally:
        session.close()


@app.route("/api/admin/activity-rules", methods=["GET"])
def get_activity_rules():
    from activity_config import ActivityConfigManager

    session = get_session()
    try:
        manager = ActivityConfigManager(session)
        rules = manager.get_all_rules()
        return jsonify([_serialize_activity_rule(r) for r in rules])
    finally:
        session.close()


@app.route("/api/admin/activity-rules/<int:rule_id>", methods=["PUT"])
def update_activity_rule(rule_id):
    from activity_config import ActivityConfigManager

    updates = request.get_json() or {}
    session = get_session()
    try:
        manager = ActivityConfigManager(session)
        rule = manager.update_rule(rule_id, updates)
        if not rule:
            return jsonify({"error": "Rule not found"}), 404
        return jsonify({"status": "updated", "rule": _serialize_activity_rule(rule)})
    finally:
        session.close()


@app.route("/api/admin/activity-rules", methods=["POST"])
def create_activity_rule():
    from activity_config import ActivityConfigManager

    rule_data = request.get_json() or {}
    session = get_session()
    try:
        manager = ActivityConfigManager(session)
        rule = manager.create_rule(rule_data)
        return jsonify({"status": "created", "rule": _serialize_activity_rule(rule)})
    finally:
        session.close()


@app.route("/api/explain/link", methods=["POST"])
def explain_link():
    from explainability import ExplanationEngine

    payload = request.get_json() or {}
    record_a_id = payload.get("record_a_id")
    record_b_id = payload.get("record_b_id")

    if not record_a_id or not record_b_id:
        return jsonify({"error": "record_a_id and record_b_id are required"}), 400

    session = get_session()
    try:
        engine = ExplanationEngine(session)
        result = engine.explain_link(record_a_id, record_b_id)
        if result.get("error"):
            return jsonify(result), 404
        return jsonify(result)
    finally:
        session.close()


@app.route("/api/explain/activity/<ubid>", methods=["GET"])
def explain_activity(ubid):
    from explainability import ExplanationEngine

    session = get_session()
    try:
        engine = ExplanationEngine(session)
        result = engine.explain_activity(ubid)
        if result.get("error"):
            return jsonify(result), 404
        return jsonify(result)
    finally:
        session.close()


@app.route("/api/ubid/<ubid>/split", methods=["POST"])
def split_ubid_endpoint(ubid):
    payload = request.get_json() or {}
    source_record_ids = payload.get("source_record_ids") or []
    split_by = payload.get("split_by", "reviewer")
    reason = payload.get("reason", "")

    if not isinstance(source_record_ids, list) or not source_record_ids:
        return jsonify({"error": "source_record_ids must be a non-empty list"}), 400

    new_ubid = split_ubid(
        ubid=ubid,
        source_records_to_split=source_record_ids,
        split_by=split_by,
        reason=reason,
    )
    if not new_ubid:
        return jsonify({"error": "Split failed; verify UBID and record IDs"}), 400

    return jsonify({
        "status": "ok",
        "original_ubid": ubid,
        "new_ubid": new_ubid,
        "moved_record_ids": source_record_ids,
    })


PINCODE_COORDS = {
    "560058": {"lat": 12.9721, "lng": 77.5304},
    "560034": {"lat": 12.9352, "lng": 77.6245},
    "560001": {"lat": 12.9763, "lng": 77.5929},
    "560002": {"lat": 12.9609, "lng": 77.5778},
}


def _looks_like_gstin(v):
    if not v:
        return False
    return bool(re.match(r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][0-9A-Z]{3}$", v.upper()))


def _looks_like_pan(v):
    if not v:
        return False
    return bool(re.match(r"^[A-Z]{5}[0-9]{4}[A-Z]$", v.upper()))


def _serialize_match_cockpit(mc, a, b):
    age_hours = 0
    if mc.created_at:
        age_hours = max(0, int((datetime.utcnow() - mc.created_at).total_seconds() // 3600))

    reason = "normal"
    if mc.similarity_score >= 0.85:
        reason = "high_confidence"
    elif age_hours >= 72:
        reason = "sla_breach"

    priority_score = round((mc.similarity_score * 100) + min(age_hours, 200) * 0.2, 2)

    return {
        "id": mc.id,
        "record_a_id": mc.record_a_id,
        "record_b_id": mc.record_b_id,
        "similarity_score": mc.similarity_score,
        "priority_score": priority_score,
        "priority_reason": reason,
        "sla_age_hours": age_hours,
        "status": mc.status,
        "created_at": mc.created_at.isoformat() if mc.created_at else None,
        "left": {
            "source_system": a.source_system,
            "source_id": a.source_id,
            "name": a.raw_name,
            "address": a.raw_address,
            "pan": a.pan,
            "gstin": a.gstin,
            "owner_name": a.owner_name,
            "pincode": a.pincode,
        },
        "right": {
            "source_system": b.source_system,
            "source_id": b.source_id,
            "name": b.raw_name,
            "address": b.raw_address,
            "pan": b.pan,
            "gstin": b.gstin,
            "owner_name": b.owner_name,
            "pincode": b.pincode,
        },
    }


@app.route('/api/search/universal')
def universal_search():
    q = (request.args.get('q') or '').strip()
    if not q:
        return jsonify({"results": [], "query": q})

    q_lower = q.lower()
    q_upper = q.upper()
    is_pincode = q.isdigit() and len(q) == 6

    session = get_session()
    try:
        ubid_rows = session.query(UbidMaster).filter(UbidMaster.activity_status != 'Merged').limit(300).all()
        src_rows = session.query(SourceRecord).limit(500).all()

        results = []

        for u in ubid_rows:
            score = 0.0
            tags = []

            if q_upper == (u.ubid or '').upper():
                score = 1.0
                tags.append('ubid_exact')
            if _looks_like_pan(q_upper) and q_upper == (u.anchor_value or '').upper():
                score = max(score, 0.99)
                tags.append('pan_anchor')
            if q_lower in (u.canonical_name or '').lower():
                score = max(score, 0.85)
                tags.append('name_contains')
            if q_lower in (u.canonical_address or '').lower():
                score = max(score, 0.78)
                tags.append('address_contains')
            if is_pincode and q == (u.pincode or ''):
                score = max(score, 0.8)
                tags.append('pincode')
            if score == 0.0 and u.canonical_name:
                fuzzy = SequenceMatcher(None, q_lower, u.canonical_name.lower()).ratio()
                if fuzzy >= 0.6:
                    score = max(score, round(fuzzy * 0.8, 3))
                    tags.append('name_fuzzy')

            if score > 0:
                coords = PINCODE_COORDS.get(u.pincode)
                results.append({
                    "entity_type": "ubid",
                    "ubid": u.ubid,
                    "name": u.canonical_name,
                    "address": u.canonical_address,
                    "pincode": u.pincode,
                    "activity_status": u.activity_status,
                    "map": coords,
                    "score": round(score, 3),
                    "matched_on": tags,
                })

        for s in src_rows:
            score = 0.0
            tags = []
            raw_json = (s.raw_json or '').lower()

            if q_upper == (s.source_id or '').upper():
                score = 0.99
                tags.append('source_id_exact')
            if _looks_like_pan(q_upper) and q_upper == (s.pan or '').upper():
                score = max(score, 0.98)
                tags.append('pan_exact')
            if _looks_like_gstin(q_upper) and q_upper == (s.gstin or '').upper():
                score = max(score, 0.98)
                tags.append('gstin_exact')
            if q_lower in (s.owner_name or '').lower():
                score = max(score, 0.85)
                tags.append('owner_name')
            if q_lower in (s.raw_name or '').lower():
                score = max(score, 0.82)
                tags.append('business_name')
            if q_lower in (s.raw_address or '').lower():
                score = max(score, 0.78)
                tags.append('address')
            if is_pincode and q == (s.pincode or ''):
                score = max(score, 0.8)
                tags.append('pincode')
            if q_lower in raw_json and (len(q) >= 8 and any(ch.isdigit() for ch in q)):
                score = max(score, 0.76)
                tags.append('mobile_or_raw_json')

            if score == 0.0 and s.raw_name:
                fuzzy = SequenceMatcher(None, q_lower, s.raw_name.lower()).ratio()
                if fuzzy >= 0.62:
                    score = max(score, round(fuzzy * 0.75, 3))
                    tags.append('name_fuzzy')

            if score > 0:
                coords = PINCODE_COORDS.get(s.pincode)
                results.append({
                    "entity_type": "source_record",
                    "record_id": s.id,
                    "source_system": s.source_system,
                    "source_id": s.source_id,
                    "name": s.raw_name,
                    "address": s.raw_address,
                    "pincode": s.pincode,
                    "pan": s.pan,
                    "gstin": s.gstin,
                    "owner_name": s.owner_name,
                    "map": coords,
                    "score": round(score, 3),
                    "matched_on": tags,
                })

        results.sort(key=lambda x: x.get('score', 0), reverse=True)
        return jsonify({
            "query": q,
            "total": len(results),
            "results": results[:150],
        })
    finally:
        session.close()


@app.route('/api/ubid/<ubid>/evidence')
def ubid_evidence(ubid):
    session = get_session()
    try:
        row = session.query(UbidMaster).filter_by(ubid=ubid).first()
        if not row:
            return jsonify({"error": "UBID not found"}), 404

        links = (
            session.query(UbidLinkage, SourceRecord)
            .join(SourceRecord, (UbidLinkage.source_system == SourceRecord.source_system) & (UbidLinkage.source_id == SourceRecord.source_id))
            .filter(UbidLinkage.ubid == ubid, UbidLinkage.is_active == True)
            .all()
        )

        source_records = []
        matched_fields = []
        for link, src in links:
            ev = {}
            if link.match_evidence:
                try:
                    ev = json.loads(link.match_evidence)
                except json.JSONDecodeError:
                    ev = {"raw": link.match_evidence}

            local_fields = []
            for key in ["pan_match", "gstin_match", "name_similarity", "address_similarity", "pincode_match", "owner_similarity"]:
                score = ev.get(key, {}).get("score") if isinstance(ev.get(key), dict) else None
                if isinstance(score, (int, float)) and score >= 0.7:
                    local_fields.append({"field": key, "score": round(score, 4)})

            if local_fields:
                matched_fields.append({
                    "source_system": link.source_system,
                    "source_id": link.source_id,
                    "fields": local_fields,
                })

            source_records.append({
                "source_system": link.source_system,
                "source_id": link.source_id,
                "confidence_score": link.confidence_score,
                "linked_by": link.linked_by,
                "raw_name": src.raw_name,
                "raw_address": src.raw_address,
                "pincode": src.pincode,
                "pan": src.pan,
                "gstin": src.gstin,
                "owner_name": src.owner_name,
                "match_evidence": ev,
            })

        evts = session.query(ActivityEvent).filter_by(matched_ubid=ubid).order_by(desc(ActivityEvent.event_date)).limit(200).all()
        timeline = [{
            "event_type": e.event_type,
            "event_date": e.event_date,
            "source_system": e.source_system,
            "match_confidence": e.match_confidence,
            "raw_identifier": e.raw_identifier,
        } for e in evts]

        return jsonify({
            "ubid": ubid,
            "golden_profile": {
                "canonical_name": row.canonical_name,
                "canonical_address": row.canonical_address,
                "pincode": row.pincode,
                "activity_status": row.activity_status,
                "anchor_type": row.anchor_type,
                "anchor_value": row.anchor_value,
            },
            "source_records": source_records,
            "matched_fields": matched_fields,
            "activity_timeline": timeline,
            "explanation": "Records are linked when identity and contextual signals cross calibrated confidence thresholds."
        })
    finally:
        session.close()


@app.route('/api/review/cockpit')
def review_cockpit():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    status = request.args.get('status', 'pending')

    session = get_session()
    try:
        SrcA = aliased(SourceRecord)
        SrcB = aliased(SourceRecord)

        q = (
            session.query(MatchCandidate, SrcA, SrcB)
            .join(SrcA, MatchCandidate.record_a_id == SrcA.id)
            .join(SrcB, MatchCandidate.record_b_id == SrcB.id)
        )
        if status:
            q = q.filter(MatchCandidate.status == status)

        rows = q.all()
        cockpit = [_serialize_match_cockpit(mc, a, b) for mc, a, b in rows]
        cockpit.sort(key=lambda x: (x['priority_score'], x['sla_age_hours']), reverse=True)

        total = len(cockpit)
        start = (page - 1) * per_page
        end = start + per_page
        return jsonify({
            "total": total,
            "page": page,
            "per_page": per_page,
            "items": cockpit[start:end],
        })
    finally:
        session.close()


@app.route('/api/review/bulk-decide', methods=['POST'])
def review_bulk_decide():
    payload = request.get_json() or {}
    ids = payload.get('match_ids') or []
    decision = payload.get('decision')
    reviewer = payload.get('reviewer', 'reviewer')
    notes = payload.get('notes', '')
    reason_code = payload.get('reason_code', 'bulk_action')

    if decision not in ('merge', 'reject', 'defer'):
        return jsonify({"error": "decision must be merge, reject, or defer"}), 400
    if not ids or not isinstance(ids, list):
        return jsonify({"error": "match_ids must be a non-empty list"}), 400

    session = get_session()
    try:
        updated = 0
        skipped = 0
        for match_id in ids:
            mc = session.query(MatchCandidate).get(match_id)
            if not mc or mc.status != 'pending':
                skipped += 1
                continue

            mc.status = decision
            mc.reviewed_by = reviewer
            mc.reviewed_at = datetime.utcnow()
            mc.reviewer_notes = notes
            session.add(ReviewerFeedback(
                match_id=mc.id,
                decision=decision,
                confidence_at_decision=mc.similarity_score,
                reviewer_notes=f"{notes} | reason_code={reason_code}",
            ))
            updated += 1

        session.add(AuditLog(
            action_type='bulk_review_decision',
            performed_by=reviewer,
            details=json.dumps({
                'match_ids': ids,
                'decision': decision,
                'reason_code': reason_code,
                'notes': notes,
                'updated': updated,
                'skipped': skipped,
            })
        ))
        session.commit()

        return jsonify({
            "status": "ok",
            "updated": updated,
            "skipped": skipped,
        })
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@app.route('/api/review/<int:match_id>/escalate', methods=['POST'])
def escalate_review(match_id):
    payload = request.get_json() or {}
    reason_code = payload.get('reason_code', 'complex_case')
    escalated_to = payload.get('escalated_to', 'supervisor')
    escalated_by = payload.get('escalated_by', 'reviewer')
    notes = payload.get('notes', '')

    session = get_session()
    try:
        mc = session.query(MatchCandidate).get(match_id)
        if not mc:
            return jsonify({"error": "match not found"}), 404

        esc = ReviewEscalation(
            match_id=match_id,
            reason_code=reason_code,
            escalated_to=escalated_to,
            escalated_by=escalated_by,
            notes=notes,
            status='open',
        )
        session.add(esc)
        session.add(AuditLog(
            action_type='review_escalated',
            performed_by=escalated_by,
            details=json.dumps({'match_id': match_id, 'reason_code': reason_code, 'to': escalated_to, 'notes': notes})
        ))
        session.commit()
        return jsonify({"status": "ok", "escalation_id": esc.id})
    finally:
        session.close()


@app.route('/api/review/escalations')
def list_escalations():
    session = get_session()
    try:
        rows = session.query(ReviewEscalation).order_by(desc(ReviewEscalation.created_at)).limit(200).all()
        return jsonify({
            "items": [{
                "id": r.id,
                "match_id": r.match_id,
                "reason_code": r.reason_code,
                "escalated_to": r.escalated_to,
                "escalated_by": r.escalated_by,
                "notes": r.notes,
                "status": r.status,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            } for r in rows]
        })
    finally:
        session.close()


def _build_watchlist_alerts(session):
    alerts = []
    
    addr_counts = (
        session.query(SourceRecord.raw_address, func.count(SourceRecord.id))
        .filter(SourceRecord.gstin != None)
        .group_by(SourceRecord.raw_address)
        .having(func.count(SourceRecord.id) >= 50)
        .all()
    )
    for addr, cnt in addr_counts:
        alerts.append({
            'alert_type': 'gstin_cluster_same_address',
            'severity': 'high',
            'entity_ref': addr,
            'title': 'GSTIN-like records concentrated at one address',
            'details': f'{cnt} records with GSTIN present share this address',
        })

    cutoff = (datetime(2025, 4, 1) - timedelta(days=18 * 30)).strftime('%Y-%m-%d')
    factories = (
        session.query(UbidMaster)
        .join(UbidLinkage, UbidMaster.ubid == UbidLinkage.ubid)
        .filter(UbidMaster.activity_status == 'Active', UbidLinkage.source_system == 'factories', UbidLinkage.is_active == True)
        .all()
    )
    seen_ubids = set()
    for ub in factories:
        if ub.ubid in seen_ubids:
            continue
        seen_ubids.add(ub.ubid)
        has_recent = (
            session.query(ActivityEvent)
            .filter(ActivityEvent.matched_ubid == ub.ubid, ActivityEvent.event_type == 'inspection_conducted', ActivityEvent.event_date >= cutoff)
            .count()
        )
        if has_recent == 0:
            alerts.append({
                'alert_type': 'active_factory_no_recent_inspection',
                'severity': 'high',
                'entity_ref': ub.ubid,
                'title': 'Active factory has no inspection in 18 months',
                'details': f'{ub.canonical_name} ({ub.ubid}) missing recent inspection evidence',
            })

    closed_with_power = (
        session.query(UbidMaster.ubid)
        .join(ActivityEvent, UbidMaster.ubid == ActivityEvent.matched_ubid)
        .filter(UbidMaster.activity_status == 'Closed', ActivityEvent.event_type == 'electricity_consumption')
        .distinct()
        .all()
    )
    for (ubid,) in closed_with_power:
        alerts.append({
            'alert_type': 'closed_business_power_consumption',
            'severity': 'critical',
            'entity_ref': ubid,
            'title': 'Closed business still consuming power',
            'details': 'Electricity consumption events exist after closure classification',
        })

    renewals = (
        session.query(ActivityEvent.matched_ubid, func.max(ActivityEvent.event_date))
        .filter(ActivityEvent.event_type == 'license_renewed', ActivityEvent.matched_ubid != None)
        .group_by(ActivityEvent.matched_ubid)
        .all()
    )
    for ubid, renewal_date in renewals:
        if not renewal_date:
            continue
        if renewal_date < (datetime(2025, 4, 1) - timedelta(days=365)).strftime('%Y-%m-%d'):
            filings = session.query(ActivityEvent).filter(
                ActivityEvent.matched_ubid == ubid,
                ActivityEvent.event_type.in_(['tax_filing_submitted', 'employee_report_filed']),
                ActivityEvent.event_date >= (datetime(2025, 4, 1) - timedelta(days=180)).strftime('%Y-%m-%d')
            ).count()
            if filings > 0:
                alerts.append({
                    'alert_type': 'renewal_expired_filings_continue',
                    'severity': 'medium',
                    'entity_ref': ubid,
                    'title': 'Renewal expired but compliance filings continue',
                    'details': f'{filings} compliance filings found after renewal expiry window',
                })

    return alerts


@app.route('/api/watchlists/alerts')
def watchlist_alerts():
    persist = request.args.get('persist', 'false').lower() == 'true'
    session = get_session()
    try:
        alerts = _build_watchlist_alerts(session)
        if persist:
            session.query(WatchlistAlert).delete()
            for a in alerts:
                session.add(WatchlistAlert(**a, status='open'))
            session.commit()

        return jsonify({"total": len(alerts), "alerts": alerts})
    finally:
        session.close()


@app.route('/api/watchlists/scan', methods=['POST'])
def watchlist_scan():
    payload = request.get_json(silent=True) or {}
    persist = bool(payload.get('persist', True))
    session = get_session()
    try:
        alerts = _build_watchlist_alerts(session)
        if persist:
            session.query(WatchlistAlert).delete()
            for a in alerts:
                session.add(WatchlistAlert(**a, status='open'))
            session.commit()

        return jsonify({
            "status": "ok",
            "persisted": persist,
            "total": len(alerts),
            "alerts": alerts,
        })
    finally:
        session.close()


def _compute_golden(session, ubid, survivorship_rule):
    links = (
        session.query(UbidLinkage, SourceRecord)
        .join(SourceRecord, (UbidLinkage.source_system == SourceRecord.source_system) & (UbidLinkage.source_id == SourceRecord.source_id))
        .filter(UbidLinkage.ubid == ubid, UbidLinkage.is_active == True)
        .all()
    )
    if not links:
        return None

    rows = [src for _, src in links]

    gst_rows = [r for r in rows if r.gstin]
    preferred = gst_rows if gst_rows else rows

    def _pick_name(items):
        return max(items, key=lambda r: len(r.raw_name or '')).raw_name if items else None

    def _pick_address(items):
        if survivorship_rule == 'latest_verified_address_wins':
            return max(items, key=lambda r: r.ingested_at or datetime.min).raw_address if items else None
        return max(items, key=lambda r: len(r.raw_address or '')).raw_address if items else None

    pan = next((r.pan for r in preferred if r.pan), None)
    gstin = next((r.gstin for r in preferred if r.gstin), None)
    owner = next((r.owner_name for r in preferred if r.owner_name), None)
    pincode = next((r.pincode for r in preferred if r.pincode), None)

    return {
        'golden_name': _pick_name(preferred),
        'golden_address': _pick_address(preferred),
        'golden_pan': pan,
        'golden_gstin': gstin,
        'golden_owner': owner,
        'golden_pincode': pincode,
        'evidence': json.dumps({
            'sources': [{
                'source_system': r.source_system,
                'source_id': r.source_id,
                'has_gstin': bool(r.gstin),
                'has_pan': bool(r.pan),
                'ingested_at': r.ingested_at.isoformat() if r.ingested_at else None,
            } for r in rows]
        })
    }


@app.route('/api/golden/<ubid>', methods=['GET'])
def get_golden_record(ubid):
    session = get_session()
    try:
        gr = session.query(GoldenRecord).filter_by(ubid=ubid).first()
        if not gr:
            return jsonify({"error": "Golden record not found; run recompute first"}), 404
        return jsonify({
            'ubid': ubid,
            'golden_name': gr.golden_name,
            'golden_address': gr.golden_address,
            'golden_pan': gr.golden_pan,
            'golden_gstin': gr.golden_gstin,
            'golden_owner': gr.golden_owner,
            'golden_pincode': gr.golden_pincode,
            'survivorship_rule': gr.survivorship_rule,
            'evidence': json.loads(gr.evidence) if gr.evidence else None,
            'updated_by': gr.updated_by,
            'updated_at': gr.updated_at.isoformat() if gr.updated_at else None,
        })
    finally:
        session.close()


@app.route('/api/golden/<ubid>/recompute', methods=['POST'])
def recompute_golden_record(ubid):
    payload = request.get_json() or {}
    survivorship_rule = payload.get('survivorship_rule', 'gst_name_preferred')
    updated_by = payload.get('updated_by', 'admin')

    session = get_session()
    try:
        computed = _compute_golden(session, ubid, survivorship_rule)
        if not computed:
            return jsonify({"error": "UBID has no linked source records"}), 404

        gr = session.query(GoldenRecord).filter_by(ubid=ubid).first()
        if not gr:
            gr = GoldenRecord(ubid=ubid)
            session.add(gr)

        gr.golden_name = computed['golden_name']
        gr.golden_address = computed['golden_address']
        gr.golden_pan = computed['golden_pan']
        gr.golden_gstin = computed['golden_gstin']
        gr.golden_owner = computed['golden_owner']
        gr.golden_pincode = computed['golden_pincode']
        gr.survivorship_rule = survivorship_rule
        gr.evidence = computed['evidence']
        gr.updated_by = updated_by

        session.add(AuditLog(
            action_type='golden_record_recomputed',
            ubid=ubid,
            performed_by=updated_by,
            details=json.dumps({'survivorship_rule': survivorship_rule}),
        ))
        session.commit()
        return jsonify({'status': 'ok', 'ubid': ubid, 'survivorship_rule': survivorship_rule})
    finally:
        session.close()


@app.route('/api/golden/<ubid>/build', methods=['POST'])
def build_golden_record(ubid):
    return recompute_golden_record(ubid)


@app.route('/api/data-quality/summary')
def data_quality_summary():
    session = get_session()
    try:
        total = session.query(SourceRecord).count()
        missing_pan = session.query(SourceRecord).filter(or_(SourceRecord.pan == None, SourceRecord.pan == '')).count()
        missing_gstin = session.query(SourceRecord).filter(or_(SourceRecord.gstin == None, SourceRecord.gstin == '')).count()
        incomplete_address = session.query(SourceRecord).filter(
            or_(SourceRecord.raw_address == None, func.length(SourceRecord.raw_address) < 15)
        ).count()

        dup_rows = (
            session.query(SourceRecord.source_system, SourceRecord.raw_name, SourceRecord.raw_address, SourceRecord.pincode, func.count(SourceRecord.id))
            .group_by(SourceRecord.source_system, SourceRecord.raw_name, SourceRecord.raw_address, SourceRecord.pincode)
            .having(func.count(SourceRecord.id) > 1)
            .all()
        )

        feed_quality = []
        by_sys = session.query(SourceRecord.source_system).distinct().all()
        for (sys_name,) in by_sys:
            count_sys = session.query(SourceRecord).filter(SourceRecord.source_system == sys_name).count()
            if count_sys == 0:
                continue
            missing_pan_sys = session.query(SourceRecord).filter(SourceRecord.source_system == sys_name, or_(SourceRecord.pan == None, SourceRecord.pan == '')).count()
            missing_addr_sys = session.query(SourceRecord).filter(SourceRecord.source_system == sys_name, or_(SourceRecord.raw_address == None, func.length(SourceRecord.raw_address) < 15)).count()
            quality = round(100 - ((missing_pan_sys + missing_addr_sys) / (count_sys * 2)) * 100, 2)
            feed_quality.append({
                'source_system': sys_name,
                'records': count_sys,
                'missing_pan': missing_pan_sys,
                'incomplete_address': missing_addr_sys,
                'quality_score': quality,
            })

        return jsonify({
            'total_records': total,
            'missing_pan': missing_pan,
            'missing_gstin': missing_gstin,
            'incomplete_address': incomplete_address,
            'duplicate_groups_within_department': len(dup_rows),
            'source_feed_quality': feed_quality,
        })
    finally:
        session.close()


def require_api_key(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        expected = os.environ.get('UBID_API_KEY', 'dev-api-key')
        key = request.headers.get('X-API-Key')
        if key != expected:
            return jsonify({'error': 'Unauthorized'}), 401
        return fn(*args, **kwargs)
    return wrapper


@app.route('/api/secure/departments/search')
@require_api_key
def secure_department_search():
    return universal_search()


@app.route('/api/secure/export/ubids.csv')
@require_api_key
def export_ubids_csv():
    session = get_session()
    try:
        rows = session.query(UbidMaster).filter(UbidMaster.activity_status != 'Merged').all()
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['ubid', 'canonical_name', 'canonical_address', 'pincode', 'activity_status', 'anchor_type', 'anchor_value'])
        for r in rows:
            writer.writerow([r.ubid, r.canonical_name, r.canonical_address, r.pincode, r.activity_status, r.anchor_type, r.anchor_value])

        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': 'attachment; filename=ubids_export.csv'}
        )
    finally:
        session.close()


@app.route('/api/secure/report-schedules', methods=['GET', 'POST'])
@require_api_key
def report_schedules():
    session = get_session()
    try:
        if request.method == 'POST':
            payload = request.get_json() or {}
            row = ReportSchedule(
                name=payload.get('name', 'Unnamed Report'),
                report_type=payload.get('report_type', 'dashboard_summary'),
                cron_expression=payload.get('cron_expression', '0 8 * * 1'),
                destination=payload.get('destination', 'email://ops@example.com'),
                is_active=payload.get('is_active', True),
            )
            session.add(row)
            session.commit()
            return jsonify({'status': 'created', 'id': row.id})

        rows = session.query(ReportSchedule).order_by(desc(ReportSchedule.created_at)).all()
        return jsonify({'items': [{
            'id': r.id,
            'name': r.name,
            'report_type': r.report_type,
            'cron_expression': r.cron_expression,
            'destination': r.destination,
            'is_active': r.is_active,
            'last_run_at': r.last_run_at.isoformat() if r.last_run_at else None,
        } for r in rows]})
    finally:
        session.close()


@app.route('/api/secure/webhooks/register', methods=['POST'])
@require_api_key
def register_webhook():
    payload = request.get_json() or {}
    session = get_session()
    try:
        row = WebhookSubscription(
            name=payload.get('name', 'default-webhook'),
            target_url=payload.get('target_url'),
            event_type=payload.get('event_type', 'ubid.updated'),
            secret=payload.get('secret'),
            is_active=True,
        )
        if not row.target_url:
            return jsonify({'error': 'target_url is required'}), 400
        session.add(row)
        session.commit()
        return jsonify({'status': 'created', 'id': row.id})
    finally:
        session.close()


@app.route('/api/secure/webhooks/test/<int:webhook_id>', methods=['POST'])
@require_api_key
def test_webhook(webhook_id):
    session = get_session()
    try:
        row = session.query(WebhookSubscription).get(webhook_id)
        if not row:
            return jsonify({'error': 'Webhook not found'}), 404

        payload = json.dumps({
            'event_type': row.event_type,
            'timestamp': datetime.utcnow().isoformat(),
            'data': request.get_json() or {'message': 'test event'},
        }).encode('utf-8')

        req = urlrequest.Request(row.target_url, data=payload, headers={'Content-Type': 'application/json'})
        try:
            with urlrequest.urlopen(req, timeout=6) as resp:
                status = resp.status
            return jsonify({'status': 'sent', 'http_status': status})
        except Exception as ex:
            return jsonify({'status': 'failed', 'error': str(ex)}), 502
    finally:
        session.close()


@app.route('/api/geo/analytics')
def geo_analytics():
    session = get_session()
    try:
        rows = (
            session.query(UbidMaster.pincode, UbidMaster.activity_status, func.count(UbidMaster.ubid))
            .filter(UbidMaster.activity_status != 'Merged')
            .group_by(UbidMaster.pincode, UbidMaster.activity_status)
            .all()
        )

        clusters = {}
        for pincode, status, count in rows:
            key = pincode or 'unknown'
            if key not in clusters:
                clusters[key] = {
                    'pincode': key,
                    'map': PINCODE_COORDS.get(key),
                    'total': 0,
                    'status_breakdown': {},
                }
            clusters[key]['total'] += count
            clusters[key]['status_breakdown'][status] = count

        return jsonify({
            'clusters': list(clusters.values()),
            'dimensions': ['sector', 'pincode', 'compliance_risk', 'inspection_gap', 'consumption_anomaly'],
        })
    finally:
        session.close()


@app.route('/api/policy/simulate/dormant-threshold')
def policy_simulation_dormant_threshold():
    from_months = int(request.args.get('from_months', 9))
    to_months = int(request.args.get('to_months', 12))

    from_days = from_months * 30
    to_days = to_months * 30

    session = get_session()
    try:
        ubids = session.query(UbidMaster).filter(UbidMaster.activity_status != 'Merged').all()
        moved = []

        for ub in ubids:
            evidence = {}
            if ub.status_evidence:
                try:
                    evidence = json.loads(ub.status_evidence)
                except json.JSONDecodeError:
                    evidence = {}

            days_since = evidence.get('days_since_last_activity')
            if days_since is None:
                continue

            old_status = 'Active' if days_since <= from_days else ('Dormant' if days_since <= 540 else 'Closed')
            new_status = 'Active' if days_since <= to_days else ('Dormant' if days_since <= 540 else 'Closed')

            if old_status != new_status:
                moved.append({
                    'ubid': ub.ubid,
                    'days_since_last_activity': days_since,
                    'from_status': old_status,
                    'to_status': new_status,
                })

        return jsonify({
            'from_months': from_months,
            'to_months': to_months,
            'moved_count': len(moved),
            'moved_entities': moved[:200],
        })
    finally:
        session.close()


def _normalize_phone(value):
    if not value:
        return None
    digits = ''.join(ch for ch in str(value) if ch.isdigit())
    if len(digits) < 10:
        return None
    return digits[-10:]


def _extract_phone_from_raw(raw_json):
    if not raw_json:
        return None
    try:
        obj = json.loads(raw_json)
        if isinstance(obj, dict):
            for key in ['phone', 'mobile', 'contact_phone', 'owner_phone', 'phone_number', 'contact_no']:
                if key in obj:
                    normalized = _normalize_phone(obj.get(key))
                    if normalized:
                        return normalized
    except (ValueError, TypeError, json.JSONDecodeError):
        pass
    return None


def _extract_license_ref(source_record):
    if source_record.source_system in ('factories', 'kspcb', 'shop_establishment', 'labour'):
        return source_record.source_id
    return None


@app.route('/api/graph/network')
def network_graph():
    min_cluster = int(request.args.get('min_cluster', 3))
    max_edges = int(request.args.get('max_edges', 2500))

    session = get_session()
    try:
        links = (
            session.query(UbidLinkage, SourceRecord)
            .join(
                SourceRecord,
                (UbidLinkage.source_system == SourceRecord.source_system)
                & (UbidLinkage.source_id == SourceRecord.source_id)
            )
            .filter(UbidLinkage.is_active == True)
            .all()
        )

        nodes = {}
        edges = []
        entity_to_businesses = defaultdict(set)
        business_to_entities = defaultdict(set)

        def add_node(node_id, node_type, label):
            if node_id not in nodes:
                nodes[node_id] = {
                    'id': node_id,
                    'type': node_type,
                    'label': label,
                }

        for link, src in links:
            biz_id = f'business:{link.ubid}'
            add_node(biz_id, 'business', link.ubid)

            proprietor = (src.owner_name or '').strip().lower()
            if proprietor:
                node_id = f'proprietor:{proprietor}'
                add_node(node_id, 'proprietor', src.owner_name)
                entity_to_businesses[node_id].add(biz_id)
                business_to_entities[biz_id].add(node_id)
                if len(edges) < max_edges:
                    edges.append({'source': biz_id, 'target': node_id, 'relation': 'owned_by'})

            address = (src.normalized_address or src.raw_address or '').strip().lower()
            if address:
                addr_label = (src.raw_address or src.normalized_address or '')[:110]
                node_id = f'address:{address[:120]}'
                add_node(node_id, 'address', addr_label)
                entity_to_businesses[node_id].add(biz_id)
                business_to_entities[biz_id].add(node_id)
                if len(edges) < max_edges:
                    edges.append({'source': biz_id, 'target': node_id, 'relation': 'located_at'})

            phone = _extract_phone_from_raw(src.raw_json)
            if phone:
                node_id = f'phone:{phone}'
                add_node(node_id, 'phone', phone)
                entity_to_businesses[node_id].add(biz_id)
                business_to_entities[biz_id].add(node_id)
                if len(edges) < max_edges:
                    edges.append({'source': biz_id, 'target': node_id, 'relation': 'contacted_via'})

            license_ref = _extract_license_ref(src)
            if license_ref:
                node_id = f'license:{license_ref}'
                add_node(node_id, 'license', license_ref)
                entity_to_businesses[node_id].add(biz_id)
                business_to_entities[biz_id].add(node_id)
                if len(edges) < max_edges:
                    edges.append({'source': biz_id, 'target': node_id, 'relation': 'licensed_under'})

        suspicious_clusters = []
        for entity_id, businesses in entity_to_businesses.items():
            degree = len(businesses)
            if degree >= min_cluster:
                entity_type = entity_id.split(':', 1)[0]
                severity = 'medium'
                if entity_type in ('phone', 'address') and degree >= 5:
                    severity = 'high'
                if entity_type == 'license' and degree >= 3:
                    severity = 'high'

                suspicious_clusters.append({
                    'pattern': f'shared_{entity_type}',
                    'entity_id': entity_id,
                    'linked_businesses': sorted(list(businesses))[:100],
                    'degree': degree,
                    'severity': severity,
                })

        shell_patterns = []
        biz_ids = list(business_to_entities.keys())
        for i in range(len(biz_ids)):
            a = biz_ids[i]
            entities_a = business_to_entities[a]
            for j in range(i + 1, len(biz_ids)):
                b = biz_ids[j]
                overlap = entities_a.intersection(business_to_entities[b])
                if len(overlap) >= 2:
                    shell_patterns.append({
                        'business_a': a,
                        'business_b': b,
                        'shared_links': sorted(list(overlap))[:6],
                        'shared_count': len(overlap),
                    })
                    if len(shell_patterns) >= 150:
                        break
            if len(shell_patterns) >= 150:
                break

        suspicious_clusters.sort(key=lambda x: x['degree'], reverse=True)

        return jsonify({
            'summary': {
                'business_nodes': len([n for n in nodes.values() if n['type'] == 'business']),
                'entity_nodes': len(nodes) - len([n for n in nodes.values() if n['type'] == 'business']),
                'edges': len(edges),
                'suspicious_cluster_count': len(suspicious_clusters),
                'shell_pattern_count': len(shell_patterns),
            },
            'nodes': list(nodes.values())[:1500],
            'edges': edges,
            'suspicious_clusters': suspicious_clusters[:200],
            'shell_patterns': shell_patterns,
        })
    finally:
        session.close()


@app.route('/api/learning/status')
def learning_status():
    from feedback_learner import FeedbackLearner

    session = get_session()
    try:
        total_feedback = session.query(ReviewerFeedback).count()
        applied_feedback = session.query(FeedbackProcessingLog).count()
        pending_feedback = max(total_feedback - applied_feedback, 0)
        learner = FeedbackLearner(session)
        latest = session.query(FeedbackProcessingLog).order_by(desc(FeedbackProcessingLog.processed_at)).first()

        return jsonify({
            'total_feedback': total_feedback,
            'processed_feedback': applied_feedback,
            'pending_feedback': pending_feedback,
            'weights': learner.get_weight_summary(),
            'human_control_mode': True,
            'auto_override_reviews': False,
            'last_processed_at': latest.processed_at.isoformat() if latest else None,
            'note': 'Feedback can improve future ranking only after explicit processing call.',
        })
    finally:
        session.close()


@app.route('/api/learning/process', methods=['POST'])
def learning_process():
    from feedback_learner import FeedbackLearner

    payload = request.get_json() or {}
    apply_changes = bool(payload.get('apply', False))

    session = get_session()
    try:
        total_feedback = session.query(ReviewerFeedback).count()
        applied_feedback = session.query(FeedbackProcessingLog).count()
        pending_feedback = max(total_feedback - applied_feedback, 0)

        if not apply_changes:
            return jsonify({
                'status': 'preview',
                'pending_feedback': pending_feedback,
                'message': 'No model weights updated. Re-run with apply=true to process feedback.',
            })

        learner = FeedbackLearner(session)
        results = learner.process_unapplied_feedback()
        return jsonify({
            'status': 'processed',
            'processed_items': len(results),
            'details': results[:25],
            'human_control_mode': True,
            'note': 'Reviewer decisions remain authoritative; only ranking weights were updated.',
        })
    finally:
        session.close()


@app.route('/api/department/scorecards')
def department_scorecards():
    session = get_session()
    try:
        systems = [s for (s,) in session.query(SourceRecord.source_system).distinct().all()]
        cards = []

        SrcA = aliased(SourceRecord)
        SrcB = aliased(SourceRecord)

        for system in systems:
            total_records = session.query(SourceRecord).filter(SourceRecord.source_system == system).count()
            latest_ingested = session.query(func.max(SourceRecord.ingested_at)).filter(SourceRecord.source_system == system).scalar()
            freshness_days = None
            if latest_ingested:
                freshness_days = max(0, (datetime.utcnow() - latest_ingested).days)

            unresolved_backlog = (
                session.query(func.count(func.distinct(MatchCandidate.id)))
                .join(SrcA, MatchCandidate.record_a_id == SrcA.id)
                .join(SrcB, MatchCandidate.record_b_id == SrcB.id)
                .filter(
                    MatchCandidate.status == 'pending',
                    or_(SrcA.source_system == system, SrcB.source_system == system)
                )
                .scalar()
            ) or 0

            missing_pan = session.query(SourceRecord).filter(SourceRecord.source_system == system, or_(SourceRecord.pan == None, SourceRecord.pan == '')).count()
            missing_gstin = session.query(SourceRecord).filter(SourceRecord.source_system == system, or_(SourceRecord.gstin == None, SourceRecord.gstin == '')).count()
            missing_addr = session.query(SourceRecord).filter(SourceRecord.source_system == system, or_(SourceRecord.raw_address == None, func.length(SourceRecord.raw_address) < 15)).count()

            denom = max(total_records, 1)
            issues = [
                {'issue': 'missing_pan', 'count': missing_pan, 'percent': round((missing_pan / denom) * 100, 2)},
                {'issue': 'missing_gstin', 'count': missing_gstin, 'percent': round((missing_gstin / denom) * 100, 2)},
                {'issue': 'incomplete_address', 'count': missing_addr, 'percent': round((missing_addr / denom) * 100, 2)},
            ]
            issues.sort(key=lambda x: x['percent'], reverse=True)
            top_issues = issues[:3]

            freshness_penalty = min(25.0, (freshness_days or 0) * 0.8)
            backlog_penalty = min(35.0, (unresolved_backlog / denom) * 120.0)
            dq_penalty = min(35.0, sum(i['percent'] for i in top_issues) / 3.0 * 0.6)
            score = round(max(0.0, 100.0 - freshness_penalty - backlog_penalty - dq_penalty), 2)

            cards.append({
                'source_system': system,
                'overall_score': score,
                'records': total_records,
                'feed_freshness_days': freshness_days,
                'latest_ingested_at': latest_ingested.isoformat() if latest_ingested else None,
                'unresolved_backlog': unresolved_backlog,
                'top_data_quality_issues': top_issues,
            })

        cards.sort(key=lambda x: x['overall_score'])
        return jsonify({'scorecards': cards})
    finally:
        session.close()


@app.route('/api/executive/dashboard')
def executive_dashboard():
    session = get_session()
    try:
        sector_rows = session.query(SourceRecord.category, SourceRecord.registration_date).filter(SourceRecord.category != None).all()
        sector_year_counts = defaultdict(lambda: defaultdict(int))
        all_years = set()

        for category, reg_date in sector_rows:
            if not reg_date:
                continue
            year = None
            reg_str = str(reg_date)
            if len(reg_str) >= 4 and reg_str[:4].isdigit():
                year = int(reg_str[:4])
            if not year:
                continue
            all_years.add(year)
            sector = category or 'Unknown'
            sector_year_counts[sector][year] += 1

        current_year = max(all_years) if all_years else datetime.utcnow().year
        prev_year = current_year - 1
        sector_growth = []
        for sector, year_map in sector_year_counts.items():
            current = year_map.get(current_year, 0)
            previous = year_map.get(prev_year, 0)
            growth_pct = 100.0 if previous == 0 and current > 0 else (0.0 if previous == 0 else ((current - previous) / previous) * 100.0)
            sector_growth.append({
                'sector': sector,
                'current_year_count': current,
                'previous_year_count': previous,
                'growth_percent': round(growth_pct, 2),
            })
        sector_growth.sort(key=lambda x: x['growth_percent'], reverse=True)

        active_rows = (
            session.query(UbidMaster.pincode, func.count(UbidMaster.ubid))
            .filter(UbidMaster.activity_status == 'Active')
            .group_by(UbidMaster.pincode)
            .all()
        )
        total_rows = (
            session.query(UbidMaster.pincode, func.count(UbidMaster.ubid))
            .filter(UbidMaster.activity_status != 'Merged')
            .group_by(UbidMaster.pincode)
            .all()
        )
        totals_by_pin = {pin or 'unknown': cnt for pin, cnt in total_rows}

        active_density = []
        for pin, active_count in active_rows:
            key = pin or 'unknown'
            total_count = totals_by_pin.get(key, active_count)
            density = 0.0 if total_count == 0 else (active_count / total_count) * 100.0
            active_density.append({
                'pincode': key,
                'active_businesses': active_count,
                'total_businesses': total_count,
                'active_density_percent': round(density, 2),
            })
        active_density.sort(key=lambda x: x['active_density_percent'], reverse=True)

        alerts = _build_watchlist_alerts(session)
        alerts_by_pin = defaultdict(int)
        for alert in alerts:
            ref = str(alert.get('entity_ref') or '')
            maybe_pin = ref.strip()[:6]
            if maybe_pin.isdigit():
                alerts_by_pin[maybe_pin] += 1

        closed_dormant_rows = (
            session.query(UbidMaster.pincode, UbidMaster.activity_status, func.count(UbidMaster.ubid))
            .filter(UbidMaster.activity_status.in_(['Dormant', 'Closed', 'Compliance Risk']))
            .group_by(UbidMaster.pincode, UbidMaster.activity_status)
            .all()
        )
        gap_by_pin = defaultdict(lambda: {'Dormant': 0, 'Closed': 0, 'Compliance Risk': 0})
        for pin, status, count in closed_dormant_rows:
            key = pin or 'unknown'
            gap_by_pin[key][status] = count

        compliance_gap_heatmap = []
        for pin, status_counts in gap_by_pin.items():
            total_issues = status_counts.get('Dormant', 0) + status_counts.get('Closed', 0) + status_counts.get('Compliance Risk', 0)
            compliance_gap_heatmap.append({
                'pincode': pin,
                'issues': total_issues,
                'status_breakdown': status_counts,
                'watchlist_alerts': alerts_by_pin.get(pin, 0),
                'map': PINCODE_COORDS.get(pin),
            })
        compliance_gap_heatmap.sort(key=lambda x: x['issues'], reverse=True)

        return jsonify({
            'summary': {
                'active_business_count': session.query(UbidMaster).filter(UbidMaster.activity_status == 'Active').count(),
                'total_business_count': session.query(UbidMaster).filter(UbidMaster.activity_status != 'Merged').count(),
                'watchlist_alert_count': len(alerts),
            },
            'sector_growth': sector_growth[:20],
            'active_business_density': active_density[:50],
            'compliance_gap_heatmap': compliance_gap_heatmap[:50],
            'key_questions': [
                'Which sectors are expanding fastest year-over-year?',
                'Where is active business density strongest by pincode?',
                'Which geographies show the largest compliance gaps?',
            ],
        })
    finally:
        session.close()


def _build_entity_maps_for_ubids(session):
    links = (
        session.query(UbidLinkage, SourceRecord)
        .join(
            SourceRecord,
            (UbidLinkage.source_system == SourceRecord.source_system)
            & (UbidLinkage.source_id == SourceRecord.source_id)
        )
        .filter(UbidLinkage.is_active == True)
        .all()
    )

    entity_to_ubids = defaultdict(set)
    for link, src in links:
        ubid = link.ubid

        owner = (src.owner_name or '').strip().lower()
        if owner:
            entity_to_ubids[f'owner:{owner}'].add(ubid)

        address = (src.normalized_address or src.raw_address or '').strip().lower()
        if address:
            entity_to_ubids[f'address:{address[:140]}'].add(ubid)

        phone = _extract_phone_from_raw(src.raw_json)
        if phone:
            entity_to_ubids[f'phone:{phone}'].add(ubid)

        license_ref = _extract_license_ref(src)
        if license_ref:
            entity_to_ubids[f'license:{license_ref}'].add(ubid)

    return entity_to_ubids


def _compute_trust_score(session, ubid):
    master = session.query(UbidMaster).filter_by(ubid=ubid).first()
    if not master:
        return None

    links = (
        session.query(UbidLinkage, SourceRecord)
        .join(
            SourceRecord,
            (UbidLinkage.source_system == SourceRecord.source_system)
            & (UbidLinkage.source_id == SourceRecord.source_id)
        )
        .filter(UbidLinkage.ubid == ubid, UbidLinkage.is_active == True)
        .all()
    )

    if not links:
        return {
            'ubid': ubid,
            'trust_score': 0.0,
            'components': {
                'identity_confidence': 0.0,
                'activity_health': 0.0,
                'compliance_health': 0.0,
                'network_risk': 100.0,
            },
            'explanation': ['No active linked records found for this UBID.'],
            'status': master.activity_status,
        }

    confidences = [float(l.confidence_score or 0.0) for l, _ in links]
    avg_conf = sum(confidences) / max(len(confidences), 1)
    has_anchor = 1.0 if master.anchor_type and master.anchor_value else 0.0
    identity_sources = len({src.source_system for _, src in links})
    identity_score = min(100.0, (avg_conf * 72.0) + (has_anchor * 18.0) + min(identity_sources, 3) * 3.5)

    evidence = {}
    if master.status_evidence:
        try:
            evidence = json.loads(master.status_evidence)
        except json.JSONDecodeError:
            evidence = {}

    days_since = evidence.get('days_since_last_activity')
    if master.activity_status == 'Active':
        activity_score = 88.0 if days_since is None else max(55.0, 100.0 - min(float(days_since), 240.0) * 0.18)
    elif master.activity_status == 'Dormant':
        activity_score = 52.0 if days_since is None else max(25.0, 70.0 - min(float(days_since), 540.0) * 0.07)
    elif master.activity_status == 'Closed':
        activity_score = 22.0
    elif master.activity_status == 'Compliance Risk':
        activity_score = 30.0
    else:
        activity_score = 45.0

    recent_180 = (datetime(2025, 4, 1) - timedelta(days=180)).strftime('%Y-%m-%d')
    filings = session.query(ActivityEvent).filter(
        ActivityEvent.matched_ubid == ubid,
        ActivityEvent.event_type.in_(['tax_filing_submitted', 'employee_report_filed', 'license_renewed', 'inspection_conducted']),
        ActivityEvent.event_date >= recent_180,
    ).count()
    notices = session.query(ActivityEvent).filter(
        ActivityEvent.matched_ubid == ubid,
        ActivityEvent.event_type.in_(['compliance_notice_issued', 'pollution_test_failed']),
        ActivityEvent.event_date >= recent_180,
    ).count()
    compliance_score = max(5.0, min(100.0, 58.0 + filings * 8.0 - notices * 12.0))

    entity_to_ubids = _build_entity_maps_for_ubids(session)
    linked_entity_overlap = 0
    high_risk_entities = []
    for key, ubids in entity_to_ubids.items():
        if ubid in ubids and len(ubids) > 1:
            linked_entity_overlap += (len(ubids) - 1)
            if len(high_risk_entities) < 6:
                high_risk_entities.append({'entity': key, 'linked_business_count': len(ubids)})

    watchlist_hits = len([a for a in _build_watchlist_alerts(session) if str(a.get('entity_ref') or '') == ubid])
    network_risk = min(100.0, linked_entity_overlap * 8.0 + watchlist_hits * 28.0)

    trust = (
        identity_score * 0.36
        + activity_score * 0.26
        + compliance_score * 0.24
        + (100.0 - network_risk) * 0.14
    )
    trust = round(max(0.0, min(100.0, trust)), 2)

    explanation = [
        f'Identity confidence uses average linkage confidence ({round(avg_conf * 100, 1)}%) and anchor reliability.',
        f'Activity health reflects status {master.activity_status} with days since activity {days_since if days_since is not None else "unknown"}.',
        f'Compliance health accounts for {filings} recent positive filings/events and {notices} negative notices/tests.',
        f'Network risk is driven by {linked_entity_overlap} shared-entity overlaps and {watchlist_hits} watchlist hits.',
    ]

    return {
        'ubid': ubid,
        'trust_score': trust,
        'components': {
            'identity_confidence': round(identity_score, 2),
            'activity_health': round(activity_score, 2),
            'compliance_health': round(compliance_score, 2),
            'network_risk': round(network_risk, 2),
        },
        'status': master.activity_status,
        'explanation': explanation,
        'shared_entities': high_risk_entities,
    }


def _build_shell_pairs(session, limit=80):
    entity_to_ubids = _build_entity_maps_for_ubids(session)
    pair_counter = defaultdict(lambda: {'count': 0, 'entities': []})

    for entity, ubids in entity_to_ubids.items():
        ubid_list = sorted(list(ubids))
        if len(ubid_list) < 2:
            continue

        capped = ubid_list[:40]
        for i in range(len(capped)):
            for j in range(i + 1, len(capped)):
                a = capped[i]
                b = capped[j]
                key = (a, b)
                pair_counter[key]['count'] += 1
                if len(pair_counter[key]['entities']) < 6:
                    pair_counter[key]['entities'].append(entity)

    pairs = []
    for (a, b), payload in pair_counter.items():
        if payload['count'] >= 2:
            pairs.append({
                'business_a': a,
                'business_b': b,
                'shared_count': payload['count'],
                'shared_entities': payload['entities'],
            })

    pairs.sort(key=lambda x: x['shared_count'], reverse=True)
    return pairs[:limit]


@app.route('/api/trust-score/<ubid>')
def get_trust_score(ubid):
    session = get_session()
    try:
        trust = _compute_trust_score(session, ubid)
        if not trust:
            return jsonify({'error': 'UBID not found'}), 404
        return jsonify(trust)
    finally:
        session.close()


@app.route('/api/trust-score/top')
def top_trust_scores():
    limit = int(request.args.get('limit', 30))
    session = get_session()
    try:
        ubids = [u for (u,) in session.query(UbidMaster.ubid).filter(UbidMaster.activity_status != 'Merged').all()]
        scored = []
        for ubid in ubids:
            trust = _compute_trust_score(session, ubid)
            if trust:
                scored.append(trust)
        scored.sort(key=lambda x: x['trust_score'], reverse=True)
        return jsonify({'items': scored[:limit], 'total_scored': len(scored)})
    finally:
        session.close()


@app.route('/api/workflows/inspection-priority')
def workflow_inspection_priority():
    limit = int(request.args.get('limit', 60))
    months = int(request.args.get('months_without_inspection', 18))
    cutoff = (datetime(2025, 4, 1) - timedelta(days=months * 30)).strftime('%Y-%m-%d')

    session = get_session()
    try:
        ubids = (
            session.query(UbidMaster)
            .join(UbidLinkage, UbidMaster.ubid == UbidLinkage.ubid)
            .filter(
                UbidMaster.activity_status == 'Active',
                UbidLinkage.source_system == 'factories',
                UbidLinkage.is_active == True,
            )
            .all()
        )

        seen = set()
        queue = []
        for ub in ubids:
            if ub.ubid in seen:
                continue
            seen.add(ub.ubid)

            last_inspection = session.query(func.max(ActivityEvent.event_date)).filter(
                ActivityEvent.matched_ubid == ub.ubid,
                ActivityEvent.event_type == 'inspection_conducted'
            ).scalar()

            if last_inspection and last_inspection >= cutoff:
                continue

            trust = _compute_trust_score(session, ub.ubid)
            trust_score = trust['trust_score'] if trust else 0.0

            overdue_days = 0
            if last_inspection:
                try:
                    last_dt = datetime.strptime(last_inspection, '%Y-%m-%d')
                    cutoff_dt = datetime.strptime(cutoff, '%Y-%m-%d')
                    overdue_days = max(0, (cutoff_dt - last_dt).days)
                except ValueError:
                    overdue_days = 0
            else:
                overdue_days = months * 30

            priority = round(min(100.0, (100.0 - trust_score) * 0.6 + min(overdue_days, 900) * 0.07 + 18.0), 2)
            queue.append({
                'ubid': ub.ubid,
                'business_name': ub.canonical_name,
                'pincode': ub.pincode,
                'trust_score': trust_score,
                'last_inspection_date': last_inspection,
                'days_overdue_vs_policy': overdue_days,
                'priority_score': priority,
                'reason': 'Active factory with overdue inspection window.',
            })

        queue.sort(key=lambda x: x['priority_score'], reverse=True)
        return jsonify({'workflow': 'inspection_priority', 'policy_months': months, 'items': queue[:limit], 'total': len(queue)})
    finally:
        session.close()


@app.route('/api/workflows/renewal-risk')
def workflow_renewal_risk():
    limit = int(request.args.get('limit', 60))
    expiry_days = int(request.args.get('expiry_days', 365))
    recent_days = int(request.args.get('recent_signal_days', 180))

    expiry_cutoff = (datetime(2025, 4, 1) - timedelta(days=expiry_days)).strftime('%Y-%m-%d')
    signal_cutoff = (datetime(2025, 4, 1) - timedelta(days=recent_days)).strftime('%Y-%m-%d')

    session = get_session()
    try:
        renewals = (
            session.query(ActivityEvent.matched_ubid, func.max(ActivityEvent.event_date))
            .filter(ActivityEvent.event_type == 'license_renewed', ActivityEvent.matched_ubid != None)
            .group_by(ActivityEvent.matched_ubid)
            .all()
        )

        queue = []
        for ubid, last_renewal in renewals:
            if not last_renewal or last_renewal >= expiry_cutoff:
                continue

            ongoing_activity = session.query(ActivityEvent).filter(
                ActivityEvent.matched_ubid == ubid,
                ActivityEvent.event_type.in_(['tax_filing_submitted', 'employee_report_filed', 'electricity_consumption', 'water_consumption']),
                ActivityEvent.event_date >= signal_cutoff,
            ).count()

            if ongoing_activity == 0:
                continue

            trust = _compute_trust_score(session, ubid)
            trust_score = trust['trust_score'] if trust else 0.0
            risk_score = round(min(100.0, (100.0 - trust_score) * 0.55 + ongoing_activity * 6.0 + 22.0), 2)

            master = session.query(UbidMaster).filter_by(ubid=ubid).first()
            queue.append({
                'ubid': ubid,
                'business_name': master.canonical_name if master else None,
                'pincode': master.pincode if master else None,
                'last_renewal_date': last_renewal,
                'recent_signal_count': ongoing_activity,
                'trust_score': trust_score,
                'priority_score': risk_score,
                'reason': 'License renewal appears stale but recent operational/compliance signals continue.',
            })

        queue.sort(key=lambda x: x['priority_score'], reverse=True)
        return jsonify({'workflow': 'renewal_risk', 'items': queue[:limit], 'total': len(queue)})
    finally:
        session.close()


@app.route('/api/workflows/shell-review-bundles')
def workflow_shell_review_bundles():
    limit = int(request.args.get('limit', 40))
    session = get_session()
    try:
        pairs = _build_shell_pairs(session, limit=limit * 2)
        bundles = []

        for pair in pairs[:limit]:
            trust_a = _compute_trust_score(session, pair['business_a'])
            trust_b = _compute_trust_score(session, pair['business_b'])
            score_a = trust_a['trust_score'] if trust_a else 0.0
            score_b = trust_b['trust_score'] if trust_b else 0.0

            suspicion = round(min(100.0, pair['shared_count'] * 18.0 + ((100.0 - score_a) + (100.0 - score_b)) * 0.22), 2)
            bundles.append({
                'bundle_id': f"SHELL-{pair['business_a'].split(':')[-1][-6:]}-{pair['business_b'].split(':')[-1][-6:]}",
                'business_a': pair['business_a'],
                'business_b': pair['business_b'],
                'shared_entities': pair['shared_entities'],
                'shared_count': pair['shared_count'],
                'trust_score_a': score_a,
                'trust_score_b': score_b,
                'suspicion_score': suspicion,
                'recommended_action': 'Supervisor shell-pattern review',
            })

        bundles.sort(key=lambda x: x['suspicion_score'], reverse=True)
        return jsonify({'workflow': 'shell_review_bundles', 'items': bundles, 'total': len(bundles)})
    finally:
        session.close()


@app.route('/api/workflows/<workflow_name>/ack', methods=['POST'])
def workflow_ack(workflow_name):
    payload = request.get_json() or {}
    performed_by = payload.get('performed_by', 'workflow_operator')
    item_ref = payload.get('item_ref')
    notes = payload.get('notes', '')

    session = get_session()
    try:
        session.add(AuditLog(
            action_type='workflow_acknowledged',
            performed_by=performed_by,
            details=json.dumps({
                'workflow': workflow_name,
                'item_ref': item_ref,
                'notes': notes,
                'timestamp': datetime.utcnow().isoformat(),
            })
        ))
        session.commit()
        return jsonify({'status': 'ok', 'workflow': workflow_name, 'item_ref': item_ref})
    finally:
        session.close()


@app.route('/api/i18n/<lang>')
def i18n_pack(lang):
    packs = {
        'en': {
            'dashboard': 'Dashboard',
            'search': 'Search',
            'review_queue': 'Review Queue',
            'activity': 'Activity',
            'analytics': 'Analytics',
            'pipeline': 'Pipeline',
            'operations': 'Operations',
            'merge_link': 'Merge & Link',
            'keep_separate': 'Keep Separate',
        },
        'kn': {
            'dashboard': 'ಡ್ಯಾಶ್‌ಬೋರ್ಡ್',
            'search': 'ಹುಡುಕಿ',
            'review_queue': 'ಪರಿಶೀಲನೆ ಕ್ಯೂ',
            'activity': 'ಚಟುವಟಿಕೆ',
            'analytics': 'ವಿಶ್ಲೇಷಣೆ',
            'pipeline': 'ಪೈಪ್‌ಲೈನ್',
            'operations': 'ಕಾರ್ಯಾಚರಣೆ',
            'merge_link': 'ಮರ್ಜ್ ಮತ್ತು ಲಿಂಕ್',
            'keep_separate': 'ಪ್ರತ್ಯೇಕವಾಗಿ ಇಡಿ',
        }
    }
    return jsonify({'lang': lang, 'strings': packs.get(lang, packs['en'])})

if __name__ == "__main__":
    init_db()
    app.run(debug=True, port=5000, host="0.0.0.0")
