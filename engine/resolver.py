"""
Entity Resolution Orchestrator for UBID Platform.

Coordinates the full resolution pipeline:
1. Load source records from DB
2. Normalize all text fields
3. Apply blocking to create candidate pairs
4. Score each candidate pair
5. Classify into auto-link / review / separate
6. Cluster auto-linked records
7. Generate UBIDs for resolved clusters
8. Store review-needed pairs for human workflow
"""

import json
from datetime import datetime
from collections import defaultdict
from database.schema import get_connection
from engine.normalizer import normalize_business_name, normalize_address
from engine.blocker import build_blocks, generate_candidate_pairs, get_block_stats
from engine.matcher import compute_similarity, THRESHOLD_AUTO_LINK, THRESHOLD_REVIEW
from engine.ubid_manager import generate_ubid, create_ubid_record, link_record_to_ubid, choose_canonical


def load_source_records():
    """Load all source records from DB."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM source_records ORDER BY id")
    records = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return records


def normalize_records(records):
    """Normalize name and address fields for all records."""
    conn = get_connection()
    cursor = conn.cursor()

    for rec in records:
        rec["normalized_name"] = normalize_business_name(rec["raw_name"])
        rec["normalized_address"] = normalize_address(rec["raw_address"])

        # Update DB
        cursor.execute("""
            UPDATE source_records
            SET normalized_name = ?, normalized_address = ?
            WHERE id = ?
        """, (rec["normalized_name"], rec["normalized_address"], rec["id"]))

    conn.commit()
    conn.close()
    return records


def build_record_index(records):
    """Build a dict from record id to record for quick lookup."""
    return {rec["id"]: rec for rec in records}


def cluster_auto_links(auto_links):
    """
    Build connected components from auto-linked pairs using Union-Find.
    Returns list of clusters (sets of record ids).
    """
    parent = {}

    def find(x):
        while parent.get(x, x) != x:
            parent[x] = parent.get(parent[x], parent[x])
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for (id_a, id_b), _ in auto_links:
        parent.setdefault(id_a, id_a)
        parent.setdefault(id_b, id_b)
        union(id_a, id_b)

    # Group by root
    clusters = defaultdict(set)
    for node in parent:
        clusters[find(node)].add(node)

    # Also add singletons (records that didn't match anything)
    return list(clusters.values())


def run_resolution():
    """
    Execute the full entity resolution pipeline.
    Returns a summary of results.
    """
    print("[RESOLVE] Starting entity resolution pipeline...")
    start_time = datetime.now()

    # Step 1: Load records
    records = load_source_records()
    print(f"[RESOLVE] Loaded {len(records)} source records")

    # Step 2: Normalize
    records = normalize_records(records)
    print(f"[RESOLVE] Normalized all records")

    # Step 3: Blocking
    blocks = build_blocks(records)
    block_stats = get_block_stats(blocks)
    print(f"[RESOLVE] Created {block_stats['total_blocks']} blocks")
    print(f"[RESOLVE] Block types: {block_stats['blocks_by_type']}")

    # Step 4: Generate candidate pairs
    candidate_pairs = generate_candidate_pairs(blocks)
    print(f"[RESOLVE] Generated {len(candidate_pairs)} candidate pairs")

    # Step 5: Score all pairs
    record_index = build_record_index(records)

    auto_links = []  # (pair, evidence)
    review_pairs = []  # (pair, score, evidence)
    separate_pairs = []

    conn = get_connection()
    cursor = conn.cursor()

    # Clear old match candidates
    cursor.execute("DELETE FROM match_candidates")
    cursor.execute("DELETE FROM ubid_master")
    cursor.execute("DELETE FROM ubid_linkages")

    for (id_a, id_b), shared_blocks in candidate_pairs.items():
        rec_a = record_index.get(id_a)
        rec_b = record_index.get(id_b)

        if not rec_a or not rec_b:
            continue

        # Skip same-record comparison
        if rec_a["source_system"] == rec_b["source_system"] and rec_a["source_id"] == rec_b["source_id"]:
            continue

        score, evidence, classification = compute_similarity(rec_a, rec_b)

        evidence["shared_blocks"] = list(shared_blocks)

        if classification == "auto_link":
            auto_links.append(((id_a, id_b), evidence))
        elif classification == "review":
            review_pairs.append(((id_a, id_b), score, evidence))

            # Store in match_candidates for reviewer
            cursor.execute("""
                INSERT INTO match_candidates
                (record_a_id, record_b_id, similarity_score, match_evidence, status)
                VALUES (?, ?, ?, ?, 'pending')
            """, (id_a, id_b, score, json.dumps(evidence)))
        else:
            separate_pairs.append(((id_a, id_b), score))

    conn.commit()
    print(f"[RESOLVE] Scored all pairs: {len(auto_links)} auto-link, "
          f"{len(review_pairs)} review, {len(separate_pairs)} separate")

    # Step 6: Cluster auto-linked records
    clusters = cluster_auto_links(auto_links)
    print(f"[RESOLVE] Formed {len(clusters)} clusters from auto-links")

    # Also find records not in any cluster (singletons)
    all_clustered_ids = set()
    for c in clusters:
        all_clustered_ids.update(c)

    singletons = [rec["id"] for rec in records if rec["id"] not in all_clustered_ids]
    print(f"[RESOLVE] {len(singletons)} singleton records (no auto-link)")

    # Step 7: Generate UBIDs
    ubid_count = 0

    # Process clusters
    for cluster in clusters:
        cluster_records = [record_index[rid] for rid in cluster if rid in record_index]
        if not cluster_records:
            continue

        # Find best PAN/GSTIN in cluster
        best_pan = None
        best_gstin = None
        best_pincode = None

        for rec in cluster_records:
            if rec.get("pan") and not best_pan:
                best_pan = rec["pan"]
            if rec.get("gstin") and not best_gstin:
                best_gstin = rec["gstin"]
            if rec.get("pincode") and not best_pincode:
                best_pincode = rec["pincode"]

        # Generate UBID
        result = generate_ubid(best_pincode, best_pan, best_gstin)
        if isinstance(result, tuple):
            ubid, anchor_type, anchor_value = result
        else:
            ubid = result
            anchor_type = anchor_value = None

        # Choose canonical name/address
        canonical_name, canonical_address = choose_canonical(cluster_records)

        # Create UBID record
        create_ubid_record(ubid, anchor_type, anchor_value,
                          canonical_name, canonical_address, best_pincode)

        # Link all cluster records to this UBID
        for rec in cluster_records:
            # Find matching evidence for this record
            evidence_for_rec = {"method": "auto_link", "cluster_size": len(cluster)}
            confidence = THRESHOLD_AUTO_LINK  # minimum for auto-link

            link_record_to_ubid(
                ubid, rec["source_system"], rec["source_id"],
                confidence, evidence_for_rec
            )

        ubid_count += 1

    # Process singletons — each gets its own UBID
    for rec_id in singletons:
        rec = record_index.get(rec_id)
        if not rec:
            continue

        result = generate_ubid(rec.get("pincode"), rec.get("pan"), rec.get("gstin"))
        if isinstance(result, tuple):
            ubid, anchor_type, anchor_value = result
        else:
            # Already exists (PAN-anchored)
            ubid = result
            # Link to existing
            link_record_to_ubid(
                ubid, rec["source_system"], rec["source_id"],
                1.0, {"method": "pan_anchor_existing"}
            )
            continue

        create_ubid_record(
            ubid, anchor_type, anchor_value,
            rec.get("raw_name", ""), rec.get("raw_address", ""),
            rec.get("pincode")
        )

        link_record_to_ubid(
            ubid, rec["source_system"], rec["source_id"],
            1.0, {"method": "singleton"}
        )
        ubid_count += 1

    elapsed = (datetime.now() - start_time).total_seconds()

    summary = {
        "total_source_records": len(records),
        "candidate_pairs": len(candidate_pairs),
        "auto_linked_pairs": len(auto_links),
        "review_pairs": len(review_pairs),
        "separate_pairs": len(separate_pairs),
        "clusters_formed": len(clusters),
        "singletons": len(singletons),
        "ubids_created": ubid_count,
        "elapsed_seconds": round(elapsed, 2),
        "block_stats": block_stats,
    }

    # Log audit
    cursor.execute("""
        INSERT INTO audit_log (action_type, details)
        VALUES (?, ?)
    """, ("resolution_complete", json.dumps(summary)))
    conn.commit()
    conn.close()

    print(f"\n[RESOLVE] === Resolution Complete ===")
    print(f"[RESOLVE] UBIDs created: {ubid_count}")
    print(f"[RESOLVE] Pending reviews: {len(review_pairs)}")
    print(f"[RESOLVE] Time: {elapsed:.2f}s")
    print(f"[RESOLVE] =============================\n")

    return summary


if __name__ == "__main__":
    run_resolution()
