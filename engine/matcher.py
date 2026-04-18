"""
Multi-Signal Matcher for UBID Entity Resolution.

Computes a weighted similarity score between two source records
using multiple signals:
- PAN exact match (0.35)
- GSTIN exact match (0.30)
- Business name similarity — Jaro-Winkler + Token Sort (0.15)
- Address similarity — token overlap (0.10)
- Pincode exact match (0.05)
- Owner/Proprietor name similarity (0.05)

All scores are in [0, 1]. The final weighted score determines
auto-link (≥0.85), review (0.55–0.84), or separate (<0.55).
"""

import json
from difflib import SequenceMatcher
from engine.normalizer import (
    normalize_pan, normalize_gstin,
    extract_name_tokens, extract_address_tokens
)

# ── Confidence thresholds ──
THRESHOLD_AUTO_LINK = 0.85
THRESHOLD_REVIEW = 0.55

# ── Signal weights ──
WEIGHTS = {
    "pan_match": 0.35,
    "gstin_match": 0.30,
    "name_similarity": 0.15,
    "address_similarity": 0.10,
    "pincode_match": 0.05,
    "owner_similarity": 0.05,
}


def jaro_winkler(s1, s2):
    """
    Compute Jaro-Winkler similarity between two strings.
    Returns a value between 0 and 1.
    """
    if not s1 or not s2:
        return 0.0
    if s1 == s2:
        return 1.0

    len_s1, len_s2 = len(s1), len(s2)
    max_dist = max(len_s1, len_s2) // 2 - 1
    if max_dist < 0:
        max_dist = 0

    s1_matches = [False] * len_s1
    s2_matches = [False] * len_s2

    matches = 0
    transpositions = 0

    for i in range(len_s1):
        start = max(0, i - max_dist)
        end = min(i + max_dist + 1, len_s2)

        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(len_s1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    jaro = (
        matches / len_s1 +
        matches / len_s2 +
        (matches - transpositions / 2) / matches
    ) / 3

    # Winkler modification: boost for common prefix
    prefix_len = 0
    for i in range(min(4, len_s1, len_s2)):
        if s1[i] == s2[i]:
            prefix_len += 1
        else:
            break

    return jaro + prefix_len * 0.1 * (1 - jaro)


def token_sort_ratio(s1, s2):
    """
    Compute token-sorted similarity ratio.
    Sorts tokens alphabetically before comparison, handling word order differences.
    """
    if not s1 or not s2:
        return 0.0

    tokens1 = " ".join(sorted(s1.lower().split()))
    tokens2 = " ".join(sorted(s2.lower().split()))

    return SequenceMatcher(None, tokens1, tokens2).ratio()


def token_overlap(tokens1, tokens2):
    """
    Compute Jaccard-like token overlap between two token lists.
    """
    if not tokens1 or not tokens2:
        return 0.0

    set1 = set(tokens1)
    set2 = set(tokens2)

    intersection = set1 & set2
    union = set1 | set2

    if not union:
        return 0.0

    return len(intersection) / len(union)


def compute_similarity(record_a, record_b):
    """
    Compute multi-signal similarity between two source records.

    Returns:
        score: float in [0, 1]
        evidence: dict with per-signal breakdown
        classification: "auto_link" | "review" | "separate"
    """
    evidence = {}
    weighted_score = 0.0

    # ── Signal 1: PAN match ──
    pan_a = normalize_pan(record_a.get("pan"))
    pan_b = normalize_pan(record_b.get("pan"))

    # Also extract PAN from GSTIN if direct PAN not available
    _, pan_from_gstin_a = normalize_gstin(record_a.get("gstin"))
    _, pan_from_gstin_b = normalize_gstin(record_b.get("gstin"))

    eff_pan_a = pan_a or pan_from_gstin_a
    eff_pan_b = pan_b or pan_from_gstin_b

    if eff_pan_a and eff_pan_b:
        pan_score = 1.0 if eff_pan_a == eff_pan_b else 0.0
        evidence["pan_match"] = {
            "score": pan_score,
            "pan_a": eff_pan_a,
            "pan_b": eff_pan_b,
            "source_a": "direct" if pan_a else "from_gstin",
            "source_b": "direct" if pan_b else "from_gstin",
        }
    else:
        pan_score = 0.0
        evidence["pan_match"] = {
            "score": 0.0,
            "note": "One or both records missing PAN"
        }
    weighted_score += pan_score * WEIGHTS["pan_match"]

    # ── Signal 2: GSTIN match ──
    gstin_a, _ = normalize_gstin(record_a.get("gstin"))
    gstin_b, _ = normalize_gstin(record_b.get("gstin"))

    if gstin_a and gstin_b:
        gstin_score = 1.0 if gstin_a == gstin_b else 0.0
        evidence["gstin_match"] = {
            "score": gstin_score,
            "gstin_a": gstin_a,
            "gstin_b": gstin_b,
        }
    else:
        gstin_score = 0.0
        evidence["gstin_match"] = {
            "score": 0.0,
            "note": "One or both records missing GSTIN"
        }
    weighted_score += gstin_score * WEIGHTS["gstin_match"]

    # ── Signal 3: Business name similarity ──
    name_a = record_a.get("normalized_name", "") or ""
    name_b = record_b.get("normalized_name", "") or ""

    if name_a and name_b:
        jw_score = jaro_winkler(name_a, name_b)
        ts_score = token_sort_ratio(name_a, name_b)
        # Take the max of Jaro-Winkler and Token Sort — handles different kinds of variation
        name_score = max(jw_score, ts_score)
        evidence["name_similarity"] = {
            "score": round(name_score, 4),
            "jaro_winkler": round(jw_score, 4),
            "token_sort_ratio": round(ts_score, 4),
            "name_a": name_a,
            "name_b": name_b,
        }
    else:
        name_score = 0.0
        evidence["name_similarity"] = {"score": 0.0, "note": "Missing name"}
    weighted_score += name_score * WEIGHTS["name_similarity"]

    # ── Signal 4: Address similarity ──
    addr_a = record_a.get("normalized_address", "") or ""
    addr_b = record_b.get("normalized_address", "") or ""

    if addr_a and addr_b:
        addr_tokens_a = extract_address_tokens(addr_a)
        addr_tokens_b = extract_address_tokens(addr_b)
        addr_score = token_overlap(addr_tokens_a, addr_tokens_b)
        evidence["address_similarity"] = {
            "score": round(addr_score, 4),
            "address_a": addr_a[:100],
            "address_b": addr_b[:100],
        }
    else:
        addr_score = 0.0
        evidence["address_similarity"] = {"score": 0.0, "note": "Missing address"}
    weighted_score += addr_score * WEIGHTS["address_similarity"]

    # ── Signal 5: Pincode match ──
    pin_a = (record_a.get("pincode") or "").strip()
    pin_b = (record_b.get("pincode") or "").strip()

    if pin_a and pin_b:
        pin_score = 1.0 if pin_a == pin_b else 0.0
        evidence["pincode_match"] = {
            "score": pin_score,
            "pincode_a": pin_a,
            "pincode_b": pin_b,
        }
    else:
        pin_score = 0.0
        evidence["pincode_match"] = {"score": 0.0, "note": "Missing pincode"}
    weighted_score += pin_score * WEIGHTS["pincode_match"]

    # ── Signal 6: Owner name similarity ──
    owner_a = (record_a.get("owner_name") or "").lower().strip()
    owner_b = (record_b.get("owner_name") or "").lower().strip()

    if owner_a and owner_b:
        owner_score = jaro_winkler(owner_a, owner_b)
        evidence["owner_similarity"] = {
            "score": round(owner_score, 4),
            "owner_a": owner_a,
            "owner_b": owner_b,
        }
    else:
        owner_score = 0.0
        evidence["owner_similarity"] = {"score": 0.0, "note": "Missing owner"}
    weighted_score += owner_score * WEIGHTS["owner_similarity"]

    # ── Final classification ──
    weighted_score = round(weighted_score, 4)

    if weighted_score >= THRESHOLD_AUTO_LINK:
        classification = "auto_link"
    elif weighted_score >= THRESHOLD_REVIEW:
        classification = "review"
    else:
        classification = "separate"

    return weighted_score, evidence, classification


def explain_match(score, evidence, classification):
    """
    Generate a human-readable explanation of a match decision.
    """
    lines = [f"Overall Score: {score:.2%} → {classification.upper()}"]
    lines.append("─" * 50)

    for signal, weight in WEIGHTS.items():
        sig_data = evidence.get(signal, {})
        sig_score = sig_data.get("score", 0)
        contribution = sig_score * weight
        bar = "█" * int(sig_score * 20) + "░" * (20 - int(sig_score * 20))
        lines.append(f"  {signal:20s} {bar} {sig_score:.2f} × {weight:.2f} = {contribution:.4f}")

        # Add detail
        if "note" in sig_data:
            lines.append(f"  {'':20s} └─ {sig_data['note']}")
        elif signal == "name_similarity":
            lines.append(f"  {'':20s} └─ \"{sig_data.get('name_a', '')}\"")
            lines.append(f"  {'':20s}    vs \"{sig_data.get('name_b', '')}\"")

    return "\n".join(lines)
