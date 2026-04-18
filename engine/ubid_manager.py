"""
UBID Manager — Unique Business Identifier lifecycle management.

Responsibilities:
- Generate new UBIDs
- Anchor UBIDs to PAN/GSTIN
- Link/unlink source records to UBIDs
- Merge two UBIDs (with full audit trail)
- Split a UBID (undo a wrong merge)
- Choose canonical name/address for a UBID
"""

import json
from datetime import datetime
from database.schema import get_connection
from engine.normalizer import normalize_pan, normalize_gstin


# ── Counters per pincode (maintained in memory, seeded from DB) ──
_counters = {}


def _next_sequence(pincode):
    """Get next sequence number for a pincode."""
    global _counters
    conn = get_connection()
    if pincode not in _counters:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM ubid_master WHERE pincode = ?",
            (pincode,)
        )
        _counters[pincode] = cursor.fetchone()[0]
    conn.close()
    _counters[pincode] += 1
    return _counters[pincode]


def generate_ubid(pincode, pan=None, gstin=None):
    """
    Generate a new UBID.

    If PAN/GSTIN is available, anchor the UBID to it.
    Format:
    - Anchored: UBID-KA-PAN-{PAN_VALUE}
    - Unanchored: UBID-KA-{PINCODE}-{SEQUENCE}
    """
    # Try to derive PAN from GSTIN if not directly available
    effective_pan = normalize_pan(pan)
    if not effective_pan and gstin:
        _, effective_pan = normalize_gstin(gstin)

    if effective_pan:
        # Check if this PAN already has a UBID
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT ubid FROM ubid_master WHERE anchor_type = 'PAN' AND anchor_value = ?",
            (effective_pan,)
        )
        row = cursor.fetchone()
        conn.close()
        if row:
            return row["ubid"]  # Return existing UBID

        ubid = f"UBID-KA-PAN-{effective_pan}"
        anchor_type = "PAN"
        anchor_value = effective_pan
    else:
        seq = _next_sequence(pincode or "000000")
        ubid = f"UBID-KA-{pincode or '000000'}-{seq:05d}"
        anchor_type = None
        anchor_value = None

    return ubid, anchor_type, anchor_value


def create_ubid_record(ubid, anchor_type, anchor_value, canonical_name,
                       canonical_address, pincode):
    """Insert a new UBID into the master table."""
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO ubid_master
            (ubid, anchor_type, anchor_value, canonical_name, canonical_address, pincode)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (ubid, anchor_type, anchor_value, canonical_name, canonical_address, pincode))

        # Audit log
        cursor.execute("""
            INSERT INTO audit_log (action_type, ubid, details)
            VALUES (?, ?, ?)
        """, ("ubid_created", ubid, json.dumps({
            "anchor_type": anchor_type,
            "anchor_value": anchor_value,
            "canonical_name": canonical_name,
        })))

        conn.commit()
    except Exception as e:
        conn.rollback()
        # UBID might already exist (race condition) — that's OK
        if "UNIQUE constraint" in str(e):
            pass
        else:
            raise
    finally:
        conn.close()

    return ubid


def link_record_to_ubid(ubid, source_system, source_id, confidence, evidence, linked_by="system"):
    """Create a linkage between a source record and a UBID."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO ubid_linkages
        (ubid, source_system, source_id, confidence_score, match_evidence, linked_by)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (ubid, source_system, source_id, confidence, json.dumps(evidence), linked_by))

    cursor.execute("""
        INSERT INTO audit_log (action_type, ubid, details)
        VALUES (?, ?, ?)
    """, ("record_linked", ubid, json.dumps({
        "source_system": source_system,
        "source_id": source_id,
        "confidence": confidence,
        "linked_by": linked_by,
    })))

    conn.commit()
    conn.close()


def merge_ubids(ubid_keep, ubid_remove, merged_by="system", reason=""):
    """
    Merge two UBIDs — move all linkages from ubid_remove to ubid_keep.
    The removed UBID is deactivated, not deleted (for reversibility).
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Move linkages
    cursor.execute("""
        UPDATE ubid_linkages SET ubid = ?, linked_by = ?
        WHERE ubid = ? AND is_active = 1
    """, (ubid_keep, f"merge:{merged_by}", ubid_remove))

    # Move events
    cursor.execute("""
        UPDATE activity_events SET matched_ubid = ?
        WHERE matched_ubid = ?
    """, (ubid_keep, ubid_remove))

    # Deactivate old UBID (mark as merged)
    cursor.execute("""
        UPDATE ubid_master SET activity_status = 'Merged',
        updated_at = datetime('now')
        WHERE ubid = ?
    """, (ubid_remove,))

    # Audit
    cursor.execute("""
        INSERT INTO audit_log (action_type, ubid, details, performed_by)
        VALUES (?, ?, ?, ?)
    """, ("ubid_merged", ubid_keep, json.dumps({
        "merged_ubid": ubid_remove,
        "reason": reason,
    }), merged_by))

    conn.commit()
    conn.close()


def split_ubid(ubid, source_records_to_split, split_by="reviewer", reason=""):
    """
    Split records out of a UBID into a new UBID.
    Used to undo a wrong merge.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get first record to split for naming
    first_rec_id = source_records_to_split[0] if source_records_to_split else None
    if first_rec_id:
        cursor.execute("SELECT * FROM source_records WHERE id = ?", (first_rec_id,))
        first_rec = cursor.fetchone()
    else:
        conn.close()
        return None

    # Generate new UBID
    result = generate_ubid(
        first_rec["pincode"] if first_rec else "000000",
        first_rec["pan"] if first_rec else None,
        first_rec["gstin"] if first_rec else None,
    )
    if isinstance(result, tuple):
        new_ubid, anchor_type, anchor_value = result
    else:
        new_ubid = result
        anchor_type = anchor_value = None

    # Create new UBID record
    create_ubid_record(
        new_ubid, anchor_type, anchor_value,
        first_rec["normalized_name"] or first_rec["raw_name"],
        first_rec["normalized_address"] or first_rec["raw_address"],
        first_rec["pincode"]
    )

    # Move specified linkages
    for rec_id in source_records_to_split:
        cursor.execute("SELECT * FROM source_records WHERE id = ?", (rec_id,))
        rec = cursor.fetchone()
        if rec:
            cursor.execute("""
                UPDATE ubid_linkages SET ubid = ?, linked_by = ?
                WHERE ubid = ? AND source_system = ? AND source_id = ? AND is_active = 1
            """, (new_ubid, f"split:{split_by}", ubid, rec["source_system"], rec["source_id"]))

    # Audit
    cursor.execute("""
        INSERT INTO audit_log (action_type, ubid, details, performed_by)
        VALUES (?, ?, ?, ?)
    """, ("ubid_split", ubid, json.dumps({
        "new_ubid": new_ubid,
        "records_moved": source_records_to_split,
        "reason": reason,
    }), split_by))

    conn.commit()
    conn.close()
    return new_ubid


def choose_canonical(records):
    """
    Choose the best canonical name and address from a cluster of records.
    Prefers: longest name (usually most complete), most complete address.
    """
    if not records:
        return "", ""

    # Score each record
    best_name = ""
    best_addr = ""
    best_name_score = -1
    best_addr_score = -1

    for rec in records:
        name = rec.get("raw_name") or rec.get("normalized_name") or ""
        addr = rec.get("raw_address") or rec.get("normalized_address") or ""

        # Prefer formal names (those with legal suffixes)
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
