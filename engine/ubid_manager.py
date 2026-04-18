"""
UBID Manager — Unique Business Identifier lifecycle management.
"""

import json
from datetime import datetime
from sqlalchemy import func
from database.schema import get_session
from database.models import SourceRecord, UbidMaster, UbidLinkage, ActivityEvent, AuditLog
from engine.normalizer import normalize_pan, normalize_gstin

_global_seq = None

def _next_sequence(session=None):
    global _global_seq
    if _global_seq is None:
        own_session = session is None
        if own_session:
            session = get_session()
        try:
            count = session.query(UbidMaster).filter(UbidMaster.anchor_type == None).count()
            _global_seq = count
        finally:
            if own_session:
                session.close()
            
    _global_seq += 1
    return _global_seq

def generate_ubid(pincode=None, pan=None, gstin=None, session=None):
    effective_pan = normalize_pan(pan)
    if not effective_pan and gstin:
        _, effective_pan = normalize_gstin(gstin)

    if effective_pan:
        own_session = session is None
        if own_session:
            session = get_session()
        try:
            existing = session.query(UbidMaster).filter_by(
                anchor_type='PAN', 
                anchor_value=effective_pan
            ).first()
            if existing:
                return existing.ubid, 'PAN', effective_pan
        finally:
            if own_session:
                session.close()

        ubid = f"UBID-KA-PAN-{effective_pan}"
        anchor_type = "PAN"
        anchor_value = effective_pan
    else:
        seq = _next_sequence(session=session)
        ubid = f"UBID-KA-{seq:07d}"
        anchor_type = None
        anchor_value = None

    return ubid, anchor_type, anchor_value

def create_ubid_record(ubid, anchor_type, anchor_value, canonical_name, canonical_address, pincode, session=None):
    own_session = session is None
    if own_session:
        session = get_session()
    try:
        existing = session.query(UbidMaster).filter_by(ubid=ubid).first()
        if existing:
            return ubid 
            
        new_ubid = UbidMaster(
            ubid=ubid,
            anchor_type=anchor_type,
            anchor_value=anchor_value,
            canonical_name=canonical_name,
            canonical_address=canonical_address,
            pincode=pincode
        )
        session.add(new_ubid)
        
        audit = AuditLog(
            action_type="ubid_created",
            ubid=ubid,
            details=json.dumps({
                "anchor_type": anchor_type,
                "anchor_value": anchor_value,
                "canonical_name": canonical_name,
            })
        )
        session.add(audit)
        if own_session:
            session.commit()
    except Exception:
        if own_session:
            session.rollback()
        raise
    finally:
        if own_session:
            session.close()
        
    return ubid

def link_record_to_ubid(ubid, source_system, source_id, confidence, evidence, linked_by="system", session=None):
    own_session = session is None
    if own_session:
        session = get_session()
    try:
        linkage = UbidLinkage(
            ubid=ubid,
            source_system=source_system,
            source_id=source_id,
            confidence_score=confidence,
            match_evidence=json.dumps(evidence),
            linked_by=linked_by
        )
        session.add(linkage)
        
        audit = AuditLog(
            action_type="record_linked",
            ubid=ubid,
            details=json.dumps({
                "source_system": source_system,
                "source_id": source_id,
                "confidence": confidence,
                "linked_by": linked_by,
            })
        )
        session.add(audit)
        if own_session:
            session.commit()
    except Exception:
        if own_session:
            session.rollback()
        raise
    finally:
        if own_session:
            session.close()

def merge_ubids(ubid_keep, ubid_remove, merged_by="system", reason="", session=None):
    own_session = session is None
    if own_session:
        session = get_session()
    try:
        linkages = session.query(UbidLinkage).filter_by(ubid=ubid_remove, is_active=True).all()
        for link in linkages:
            link.ubid = ubid_keep
            link.linked_by = f"merge:{merged_by}"
            
        events = session.query(ActivityEvent).filter_by(matched_ubid=ubid_remove).all()
        for evt in events:
            evt.matched_ubid = ubid_keep
            
        old_master = session.query(UbidMaster).filter_by(ubid=ubid_remove).first()
        if old_master:
            old_master.activity_status = 'Merged'
            old_master.updated_at = datetime.utcnow()
            
        audit = AuditLog(
            action_type="ubid_merged",
            ubid=ubid_keep,
            performed_by=merged_by,
            details=json.dumps({
                "merged_ubid": ubid_remove,
                "reason": reason,
            })
        )
        session.add(audit)
        if own_session:
            session.commit()
    except Exception:
        if own_session:
            session.rollback()
        raise
    finally:
        if own_session:
            session.close()

def split_ubid(ubid, source_records_to_split, split_by="reviewer", reason=""):
    session = get_session()
    try:
        first_rec_id = source_records_to_split[0] if source_records_to_split else None
        if not first_rec_id:
            return None
            
        first_rec = session.query(SourceRecord).get(first_rec_id)
        if not first_rec:
            return None
            
        result = generate_ubid(
            first_rec.pincode,
            first_rec.pan,
            first_rec.gstin,
            session=session,
        )
        if isinstance(result, tuple):
            new_ubid, anchor_type, anchor_value = result
        else:
            new_ubid = result
            anchor_type = anchor_value = None
            
        create_ubid_record(
            new_ubid, anchor_type, anchor_value,
            first_rec.normalized_name or first_rec.raw_name,
            first_rec.normalized_address or first_rec.raw_address,
            first_rec.pincode,
            session=session,
        )
        
        for rec_id in source_records_to_split:
            rec = session.query(SourceRecord).get(rec_id)
            if rec:
                link = session.query(UbidLinkage).filter_by(
                    ubid=ubid, 
                    source_system=rec.source_system, 
                    source_id=rec.source_id,
                    is_active=True
                ).first()
                if link:
                    link.ubid = new_ubid
                    link.linked_by = f"split:{split_by}"
                    
        audit = AuditLog(
            action_type="ubid_split",
            ubid=ubid,
            performed_by=split_by,
            details=json.dumps({
                "new_ubid": new_ubid,
                "records_moved": source_records_to_split,
                "reason": reason,
            })
        )
        session.add(audit)
        session.commit()
        return new_ubid
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

def choose_canonical(records):
    if not records:
        return "", ""

    best_name = ""
    best_addr = ""
    best_name_score = -1
    best_addr_score = -1

    for rec in records:
        if isinstance(rec, dict):
            name = rec.get("raw_name") or rec.get("normalized_name") or ""
            addr = rec.get("raw_address") or rec.get("normalized_address") or ""
        else:
            name = rec.raw_name or rec.normalized_name or ""
            addr = rec.raw_address or rec.normalized_address or ""

        name_score = len(name)
        if any(s in name.lower() for s in ["pvt", "ltd", "private", "limited"]):
            name_score += 50
        if any(s in name.lower() for s in ["enterprises", "industries", "manufacturing"]):
            name_score += 20

        addr_score = len(addr)
        if "bengaluru" in addr.lower() or "bangalore" in addr.lower():
            addr_score += 20

        if name_score > best_name_score:
            best_name_score = name_score
            best_name = name
        if addr_score > best_addr_score:
            best_addr_score = addr_score
            best_addr = addr

    return best_name, best_addr
