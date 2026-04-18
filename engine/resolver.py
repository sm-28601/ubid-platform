"""
Entity Resolution Orchestrator for UBID Platform.
Implements Incremental Ingestion to preserve reviewer locks.
"""

import json
from datetime import datetime
from collections import defaultdict
from database.schema import get_session
from database.models import SourceRecord, UbidMaster, UbidLinkage, MatchCandidate, AuditLog
from engine.normalizer import normalize_business_name, normalize_address
from engine.blocker import build_blocks, generate_candidate_pairs, get_block_stats
from engine.matcher import compute_similarity, THRESHOLD_AUTO_LINK, THRESHOLD_REVIEW
from engine.ubid_manager import generate_ubid, create_ubid_record, link_record_to_ubid, choose_canonical


def load_source_records(session):
    records_orm = session.query(SourceRecord).order_by(SourceRecord.id).all()
    records = []
    for r in records_orm:
        records.append({
            "id": r.id,
            "source_system": r.source_system,
            "source_id": r.source_id,
            "raw_name": r.raw_name,
            "raw_address": r.raw_address,
            "normalized_name": r.normalized_name,
            "normalized_address": r.normalized_address,
            "pincode": r.pincode,
            "pan": r.pan,
            "gstin": r.gstin,
            "owner_name": r.owner_name,
            "registration_date": r.registration_date,
            "category": r.category
        })
    return records


def get_unresolved_ids(session):
    linked_subs = session.query(SourceRecord.id).join(
        UbidLinkage,
        (SourceRecord.source_system == UbidLinkage.source_system) &
        (SourceRecord.source_id == UbidLinkage.source_id)
    ).filter(UbidLinkage.is_active == True).all()
    return set([r[0] for r in linked_subs])


def get_linked_ubid(session, source_system, source_id):
    link = session.query(UbidLinkage).filter_by(
        source_system=source_system, source_id=source_id, is_active=True
    ).first()
    return link.ubid if link else None


def normalize_records(session, records):
    for rec in records:
        rec["normalized_name"] = normalize_business_name(rec["raw_name"])
        rec["normalized_address"] = normalize_address(rec["raw_address"])
        
        orm_rec = session.query(SourceRecord).get(rec["id"])
        orm_rec.normalized_name = rec["normalized_name"]
        orm_rec.normalized_address = rec["normalized_address"]

    session.commit()
    return records


def build_record_index(records):
    return {rec["id"]: rec for rec in records}


def cluster_auto_links(auto_links):
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

    clusters = defaultdict(set)
    for node in parent:
        clusters[find(node)].add(node)

    return list(clusters.values())


def run_resolution():
    print("[RESOLVE] Starting incremental entity resolution pipeline...")
    start_time = datetime.now()

    session = get_session()
    try:
        records = load_source_records(session)
        print(f"[RESOLVE] Loaded {len(records)} total source records")
        
        resolved_ids = get_unresolved_ids(session)
        all_ids = set([r["id"] for r in records])
        unresolved_ids = all_ids - resolved_ids
        print(f"[RESOLVE] Found {len(unresolved_ids)} unresolved records waiting for ingestion.")
        
        if not unresolved_ids:
            return {"status": "No new records to resolve."}

        records = normalize_records(session, records)
        print(f"[RESOLVE] Normalized all records")

        blocks = build_blocks(records)
        block_stats = get_block_stats(blocks)

        candidate_pairs = generate_candidate_pairs(blocks)
        print(f"[RESOLVE] Generated {len(candidate_pairs)} candidate pairs")

        record_index = build_record_index(records)

        auto_links = []  
        direct_attachments = [] 
        review_pairs = []
        separate_pairs = []

        session.query(MatchCandidate).filter(MatchCandidate.status == 'pending').delete()

        for (id_a, id_b), shared_blocks in candidate_pairs.items():
            if id_a not in unresolved_ids and id_b not in unresolved_ids:
                continue

            rec_a = record_index.get(id_a)
            rec_b = record_index.get(id_b)

            if not rec_a or not rec_b:
                continue

            if rec_a["source_system"] == rec_b["source_system"] and rec_a["source_id"] == rec_b["source_id"]:
                continue

            score, evidence, classification = compute_similarity(rec_a, rec_b)
            evidence["shared_blocks"] = list(shared_blocks)

            if classification == "auto_link":
                if id_a in unresolved_ids and id_b in unresolved_ids:
                    auto_links.append(((id_a, id_b), evidence))
                else:
                    resolved_id = id_a if id_a not in unresolved_ids else id_b
                    unresolved_id = id_b if id_a not in unresolved_ids else id_a
                    direct_attachments.append((unresolved_id, resolved_id, score, evidence))
            elif classification == "review":
                review_pairs.append(((id_a, id_b), score, evidence))
                
                can = MatchCandidate(
                    record_a_id=id_a,
                    record_b_id=id_b,
                    similarity_score=score,
                    match_evidence=json.dumps(evidence),
                    status='pending'
                )
                session.add(can)
            else:
                separate_pairs.append(((id_a, id_b), score))

        session.commit()
        print(f"[RESOLVE] Scored operational pairs: {len(auto_links)} new auto-link pairs, "
              f"{len(direct_attachments)} direct attachments to existing UBIDs, {len(review_pairs)} reviews")

        ubid_attachments = 0
        for (unresolved_id, resolved_id, score, evidence) in direct_attachments:
            if unresolved_id not in unresolved_ids:
                continue
                
            rec_unresolved = record_index[unresolved_id]
            rec_resolved = record_index[resolved_id]
            
            ubid = get_linked_ubid(session, rec_resolved["source_system"], rec_resolved["source_id"])
            if ubid:
                link_record_to_ubid(
                    ubid, rec_unresolved["source_system"], rec_unresolved["source_id"],
                    score, evidence
                )
                unresolved_ids.remove(unresolved_id)
                ubid_attachments += 1

        auto_links_filtered = [pair for pair in auto_links if pair[0][0] in unresolved_ids and pair[0][1] in unresolved_ids]
        clusters = cluster_auto_links(auto_links_filtered)
        print(f"[RESOLVE] Formed {len(clusters)} new clusters from auto-links")

        all_clustered_ids = set()
        for c in clusters:
            all_clustered_ids.update(c)

        singletons = [rid for rid in unresolved_ids if rid not in all_clustered_ids]
        print(f"[RESOLVE] {len(singletons)} singleton records (no auto-link)")

        ubid_count = 0

        for cluster in clusters:
            cluster_records = [record_index[rid] for rid in cluster]
            
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

            result = generate_ubid(best_pincode, best_pan, best_gstin)
            if isinstance(result, tuple):
                ubid, anchor_type, anchor_value = result
            else:
                ubid = result
                anchor_type = anchor_value = None

            canonical_name, canonical_address = choose_canonical(cluster_records)
            create_ubid_record(ubid, anchor_type, anchor_value, canonical_name, canonical_address, best_pincode)

            for rec in cluster_records:
                link_record_to_ubid(
                    ubid, rec["source_system"], rec["source_id"],
                    THRESHOLD_AUTO_LINK, {"method": "auto_link", "cluster_size": len(cluster)}
                )

            ubid_count += 1

        for rec_id in singletons:
            rec = record_index.get(rec_id)
            result = generate_ubid(rec.get("pincode"), rec.get("pan"), rec.get("gstin"))
            if isinstance(result, tuple):
                ubid, anchor_type, anchor_value = result
            else:
                ubid = result
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

            link_record_to_ubid(ubid, rec["source_system"], rec["source_id"], 1.0, {"method": "singleton"})
            ubid_count += 1

        elapsed = (datetime.now() - start_time).total_seconds()

        summary = {
            "total_source_records": len(records),
            "unresolved_processed": len(unresolved_ids) + ubid_attachments + len(all_clustered_ids),
            "direct_attachments": ubid_attachments,
            "new_clusters_formed": len(clusters),
            "new_singletons": len(singletons),
            "review_pairs": len(review_pairs),
            "ubids_created": ubid_count,
            "elapsed_seconds": round(elapsed, 2),
        }

        audit = AuditLog(
            action_type="incremental_resolution_complete",
            details=json.dumps(summary)
        )
        session.add(audit)
        session.commit()

        print(f"\n[RESOLVE] === Incremental Resolution Complete ===")
        print(f"[RESOLVE] New UBIDs created: {ubid_count}")
        print(f"[RESOLVE] Records attached to existing UBIDs: {ubid_attachments}")
        print(f"[RESOLVE] Pending reviews: {len(review_pairs)}")
        print(f"[RESOLVE] Time: {elapsed:.2f}s")
        print(f"[RESOLVE] =======================================\n")

        return summary
        
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

if __name__ == "__main__":
    run_resolution()
