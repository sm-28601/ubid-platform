"""
Blocking Strategy for UBID Entity Resolution.

Reduces O(n²) comparisons by grouping records into blocks
that share a common attribute. Only records within the same
block are compared against each other.

Blocking keys:
1. Exact PAN
2. Exact GSTIN (or PAN extracted from GSTIN)
3. Pincode
4. Name-phonetic (first 3 chars of normalised name + Soundex of first word)
"""

from collections import defaultdict
from engine.normalizer import normalize_pan, normalize_gstin, soundex


def build_blocks(records):
    """
    Build blocking groups from a list of source records.

    Each record is a dict with at least:
        id, normalized_name, normalized_address, pincode, pan, gstin

    Returns a dict of { block_key: set_of_record_ids }
    """
    blocks = defaultdict(set)

    for rec in records:
        rec_id = rec["id"]
        name = rec.get("normalized_name", "") or ""
        pincode = rec.get("pincode", "") or ""
        pan = normalize_pan(rec.get("pan"))
        gstin_val = rec.get("gstin")
        gstin_norm, pan_from_gstin = normalize_gstin(gstin_val)

        # Effective PAN (direct or from GSTIN)
        effective_pan = pan or pan_from_gstin

        # ── Block 1: Exact PAN ──
        if effective_pan:
            blocks[f"PAN:{effective_pan}"].add(rec_id)

        # ── Block 2: Exact GSTIN ──
        if gstin_norm:
            blocks[f"GSTIN:{gstin_norm}"].add(rec_id)

        # ── Block 3: Pincode ──
        if pincode:
            blocks[f"PIN:{pincode}"].add(rec_id)

        # ── Block 4: Name-phonetic ──
        if name and len(name) >= 3:
            prefix = name[:3].lower()
            first_word = name.split()[0] if name.split() else ""
            sdx = soundex(first_word)
            blocks[f"NAMEPHON:{prefix}:{sdx}"].add(rec_id)

        # ── Block 5: First significant word of name + pincode ──
        if name and pincode:
            words = [w for w in name.split() if len(w) > 2]
            if words:
                blocks[f"WORD1PIN:{words[0]}:{pincode}"].add(rec_id)

    # Filter out singleton blocks (no pairs to compare)
    blocks = {k: v for k, v in blocks.items() if len(v) > 1}

    return blocks


def generate_candidate_pairs(blocks):
    """
    Generate unique (record_a_id, record_b_id) pairs from blocks.

    Returns a dict: { (id_a, id_b): set_of_block_keys_they_share }
    Pairs are always stored with the smaller id first.
    """
    pair_blocks = defaultdict(set)

    for block_key, record_ids in blocks.items():
        id_list = sorted(record_ids)
        for i in range(len(id_list)):
            for j in range(i + 1, len(id_list)):
                pair = (id_list[i], id_list[j])
                pair_blocks[pair].add(block_key)

    return pair_blocks


def get_block_stats(blocks):
    """Return statistics about blocking results."""
    total_blocks = len(blocks)
    total_records_in_blocks = len(set().union(*blocks.values())) if blocks else 0
    block_sizes = [len(v) for v in blocks.values()]
    avg_block_size = sum(block_sizes) / len(block_sizes) if block_sizes else 0
    max_block_size = max(block_sizes) if block_sizes else 0

    by_type = defaultdict(int)
    for key in blocks:
        btype = key.split(":")[0]
        by_type[btype] += 1

    return {
        "total_blocks": total_blocks,
        "total_records_in_blocks": total_records_in_blocks,
        "avg_block_size": round(avg_block_size, 2),
        "max_block_size": max_block_size,
        "blocks_by_type": dict(by_type),
    }
