"""
Human-readable explanations for match and activity decisions.
"""

import json
from datetime import datetime

from database.models import ActivityEvent, SourceRecord, UbidMaster
from engine.matcher import compute_similarity
from engine.normalizer import normalize_address, normalize_business_name


class ExplanationEngine:
    def __init__(self, db_session):
        self.session = db_session

    def explain_link(self, record_id_1, record_id_2):
        record1 = self.session.query(SourceRecord).get(record_id_1)
        record2 = self.session.query(SourceRecord).get(record_id_2)

        if not record1 or not record2:
            return {"error": "Record not found"}

        rec_a = self._record_as_dict(record1)
        rec_b = self._record_as_dict(record2)

        score, evidence, classification = compute_similarity(rec_a, rec_b)

        strong_signals = []
        reasons = []

        pan_data = evidence.get("pan_match", {})
        if pan_data.get("score", 0) == 1.0:
            strong_signals.append("PAN matches exactly")
            reasons.append(f"PAN {pan_data.get('pan_a')} is identical across both records.")

        gst_data = evidence.get("gstin_match", {})
        if gst_data.get("score", 0) == 1.0:
            strong_signals.append("GSTIN matches exactly")
            reasons.append(f"GSTIN {gst_data.get('gstin_a')} is identical across both records.")

        name_data = evidence.get("name_similarity", {})
        name_score = name_data.get("score", 0)
        if name_score >= 0.8:
            strong_signals.append("Business names are highly similar")
        reasons.append(
            f"Name similarity score is {round(name_score * 100, 1)}%."
        )

        addr_data = evidence.get("address_similarity", {})
        reasons.append(
            f"Address similarity score is {round(addr_data.get('score', 0) * 100, 1)}%."
        )

        pin_data = evidence.get("pincode_match", {})
        if pin_data.get("score", 0) == 1.0:
            reasons.append(f"Both records share pincode {pin_data.get('pincode_a')}.")

        if not strong_signals:
            strong_signals.append("No hard identifier (PAN/GSTIN) exact match")

        return {
            "record1": self._record_payload(record1),
            "record2": self._record_payload(record2),
            "classification": classification,
            "confidence": score,
            "feature_evidence": evidence,
            "explanation": {
                "summary": f"Pair classified as {classification} with {round(score * 100, 1)}% confidence.",
                "strong_signals": strong_signals,
                "reasons": reasons,
                "recommendation": self._recommendation(classification),
            },
        }

    def explain_activity(self, ubid):
        ubid_record = self.session.query(UbidMaster).get(ubid)
        if not ubid_record:
            return {"error": "UBID not found"}

        events = (
            self.session.query(ActivityEvent)
            .filter_by(matched_ubid=ubid)
            .order_by(ActivityEvent.event_date.desc())
            .all()
        )

        parsed_evidence = None
        if ubid_record.status_evidence:
            try:
                parsed_evidence = json.loads(ubid_record.status_evidence)
            except json.JSONDecodeError:
                parsed_evidence = {"raw": ubid_record.status_evidence}

        if not events:
            return {
                "ubid": ubid,
                "status": ubid_record.activity_status,
                "explanation": "No activity events mapped to this UBID yet.",
                "rule_evidence": parsed_evidence,
                "events": [],
            }

        recent = events[0]
        days_since = None
        try:
            recent_date = datetime.strptime(recent.event_date, "%Y-%m-%d")
            days_since = (datetime(2025, 4, 1) - recent_date).days
        except (ValueError, TypeError):
            pass

        return {
            "ubid": ubid,
            "status": ubid_record.activity_status,
            "last_event": {
                "event_type": recent.event_type,
                "event_date": recent.event_date,
                "source_system": recent.source_system,
                "days_since_reference": days_since,
            },
            "event_count": len(events),
            "rule_evidence": parsed_evidence,
            "events": [
                {
                    "event_type": e.event_type,
                    "event_date": e.event_date,
                    "source_system": e.source_system,
                    "raw_identifier": e.raw_identifier,
                }
                for e in events[:25]
            ],
        }

    def _record_as_dict(self, rec):
        return {
            "id": rec.id,
            "source_system": rec.source_system,
            "source_id": rec.source_id,
            "raw_name": rec.raw_name,
            "raw_address": rec.raw_address,
            "normalized_name": rec.normalized_name or normalize_business_name(rec.raw_name or ""),
            "normalized_address": rec.normalized_address or normalize_address(rec.raw_address or ""),
            "pincode": rec.pincode,
            "pan": rec.pan,
            "gstin": rec.gstin,
            "owner_name": rec.owner_name,
        }

    def _record_payload(self, rec):
        return {
            "id": rec.id,
            "source_system": rec.source_system,
            "source_id": rec.source_id,
            "raw_name": rec.raw_name,
            "raw_address": rec.raw_address,
            "pincode": rec.pincode,
            "pan": rec.pan,
            "gstin": rec.gstin,
            "owner_name": rec.owner_name,
        }

    def _recommendation(self, classification):
        if classification == "auto_link":
            return "AUTO-LINK: high confidence from combined signals."
        if classification == "review":
            return "REVIEW: moderate confidence, human validation advised."
        return "KEEP SEPARATE: low confidence for safe deduplication."
