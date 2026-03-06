#!/usr/bin/env python3
"""End-to-end logical verification for PGM grouping and grouped aggregates.

Checks:
1) Group coverage for every product_id in pgm_scored
2) Hybrid grouping behavior sanity (promo collapse, quantity split)
3) Sum preservation after grouped recompute
4) Quadrant point validity (finite entry/expansion)
5) ID compatibility across transition/cart/insight tables
"""

from __future__ import annotations

import argparse
import csv
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Set, Tuple

PROMO_KEYWORDS = [
    "vip", "특가", "사은품", "전용", "체험", "한정", "이벤트", "세일", "sale", "혜택",
    "증정", "비밀판매", "아로셀데이", "타임", "재구매", "여름선물", "광복절", "설 맞이", "세컨드",
]


class VerifyError(Exception):
    pass


def to_number(value, fallback=0.0):
    try:
        out = float(value)
        if out != out:  # NaN
            return fallback
        return out
    except Exception:
        return fallback


def normalize_csv_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    out = []
    for row in rows:
        normalized = {}
        for key, value in (row or {}).items():
            cleaned = str(key or "").replace("\ufeff", "").strip()
            if cleaned:
                normalized[cleaned] = value
        out.append(normalized)
    return out


def read_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise VerifyError(f"missing file: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    return normalize_csv_rows(rows)


def read_product_id(row: Dict[str, str]) -> str:
    for key in ["product_id", "Product_ID", "entry_product_id", "aa_product_id", "pca_product_id", "i", "j"]:
        value = str(row.get(key, "") or "").strip()
        if value:
            return value
    return ""


def read_product_name(row: Dict[str, str]) -> str:
    for key in ["product_name_latest", "Product_Name", "product_name"]:
        value = str(row.get(key, "") or "").strip()
        if value:
            return value
    return ""


def normalize_group_name(name: str) -> str:
    raw = str(name or "").strip()
    if not raw:
        return ""
    removed_prefix = re.sub(r"^(\s*\[[^\]]+\]\s*)+", "", raw)
    return re.sub(r"\s+", " ", removed_prefix).strip()


def parse_leading_bracket_tokens(name: str) -> Tuple[List[str], str]:
    rest = str(name or "").strip()
    tokens: List[str] = []
    while True:
        match = re.match(r"^\s*\[([^\]]+)\]\s*", rest)
        if not match:
            break
        tokens.append(match.group(1).strip())
        rest = rest[match.end():]
    return tokens, rest.strip()


def is_quantity_token(token: str) -> bool:
    raw = str(token or "").strip()
    if not raw:
        return False
    if re.search(r"\d+\s*(매|개|입|ea|ml|mL|g|kg)\b", raw, re.IGNORECASE):
        return True
    if re.search(r"\d+\s*x\s*\d+", raw, re.IGNORECASE):
        return True
    return False


def is_promotion_token(token: str) -> bool:
    raw = str(token or "").strip()
    if not raw:
        return False
    if is_quantity_token(raw):
        return False
    lower = raw.lower()
    return any(keyword in lower for keyword in PROMO_KEYWORDS)


def normalize_group_key_name(name: str) -> str:
    raw = str(name or "").strip()
    if not raw:
        return ""
    tokens, rest = parse_leading_bracket_tokens(raw)
    kept = [token for token in tokens if not is_promotion_token(token)]
    prefix = " ".join(f"[{token}]" for token in kept)
    out = f"{prefix} {rest}".strip()
    return re.sub(r"\s+", " ", out).strip()


def slugify(value: str) -> str:
    text = re.sub(r"[^a-z0-9가-힣]+", "-", str(value or "").lower())
    text = re.sub(r"^-+|-+$", "", text)
    return text[:40]


def hash_string(value: str) -> str:
    s = str(value or "")
    h = 2166136261
    for ch in s:
        h ^= ord(ch)
        h = (h * 16777619) & 0xFFFFFFFF
    return f"{h:08x}"


def build_group_id(seed: str) -> str:
    base = slugify(seed) or "group"
    return f"grp_{base}_{hash_string(seed)}"


def first_defined_value(*values):
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return None


class UnionFind:
    def __init__(self):
        self.parent: Dict[str, str] = {}

    def ensure(self, x: str):
        if x not in self.parent:
            self.parent[x] = x

    def find(self, x: str) -> str:
        self.ensure(x)
        cur = x
        while self.parent[cur] != cur:
            cur = self.parent[cur]
        walk = x
        while self.parent[walk] != walk:
            nxt = self.parent[walk]
            self.parent[walk] = cur
            walk = nxt
        return cur

    def union(self, a: str, b: str):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra

    def union_all(self, ids: Set[str]):
        seq = sorted(ids)
        if len(seq) < 2:
            return
        head = seq[0]
        for item in seq[1:]:
            self.union(head, item)


@dataclass
class ProductMeta:
    product_id: str
    raw_name: str
    norm_name: str
    revenue: float


def sanitize_product_group_map_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    dedup: Dict[str, Dict[str, str]] = {}
    for row in normalize_csv_rows(rows):
        product_id = str(row.get("product_id", "") or "").strip()
        if not product_id:
            continue
        status = str(row.get("status", "") or "").strip().lower()
        if status not in ("grouped", "ungrouped"):
            continue
        group_id = str(row.get("group_id", "") or "").strip()
        group_name = str(row.get("group_name", "") or "").strip()
        if status == "grouped":
            if not group_id and group_name:
                group_id = build_group_id(group_name)
            if not group_name and group_id:
                group_name = group_id
            if not group_id or not group_name:
                continue
        else:
            group_id = ""
            group_name = ""
        dedup[product_id] = {
            "product_id": product_id,
            "status": status,
            "group_id": group_id,
            "group_name": group_name,
            "rule": str(row.get("rule") or "manual").strip() or "manual",
        }
    return list(dedup.values())


def build_auto_groups(anchor_rows: List[Dict[str, str]]):
    product_meta: Dict[str, ProductMeta] = {}
    ids_by_exact: Dict[str, Set[str]] = {}
    ids_by_norm: Dict[str, Set[str]] = {}
    known_ids: Set[str] = set()

    for row in anchor_rows:
        product_id = read_product_id(row)
        raw_name = read_product_name(row)
        if not product_id or not raw_name:
            continue
        known_ids.add(product_id)
        norm_name = normalize_group_key_name(raw_name)
        revenue = to_number(row.get("revenue_90d"), 0.0)
        product_meta[product_id] = ProductMeta(product_id, raw_name, norm_name, revenue)
        ids_by_exact.setdefault(raw_name, set()).add(product_id)
        if norm_name:
            ids_by_norm.setdefault(norm_name, set()).add(product_id)

    uf = UnionFind()
    exact_candidate_count = 0
    normalized_candidate_count = 0
    for id_set in ids_by_exact.values():
        if len(id_set) > 1:
            exact_candidate_count += 1
            uf.union_all(id_set)
    for id_set in ids_by_norm.values():
        if len(id_set) > 1:
            normalized_candidate_count += 1
            uf.union_all(id_set)

    components: Dict[str, List[str]] = {}
    for product_id in known_ids:
        root = uf.find(product_id)
        components.setdefault(root, []).append(product_id)

    id_to_group_id: Dict[str, str] = {}
    group_id_to_name: Dict[str, str] = {}
    group_id_to_rule: Dict[str, str] = {}

    for members in components.values():
        if len(members) < 2:
            continue
        sorted_members = sorted(members)
        metas = [product_meta[pid] for pid in sorted_members if pid in product_meta]
        metas.sort(key=lambda m: m.revenue, reverse=True)
        best = metas[0] if metas else None
        exact_names = {m.raw_name for m in metas}

        display_name = (best.norm_name if best and best.norm_name else (best.raw_name if best else sorted_members[0]))
        seed = f"{display_name}|{'|'.join(sorted_members)}"
        group_id = build_group_id(seed)
        rule = "exact_name" if len(exact_names) == 1 else "normalized_prefix"

        for pid in sorted_members:
            id_to_group_id[pid] = group_id
        group_id_to_name[group_id] = display_name
        group_id_to_rule[group_id] = rule

    return {
        "known_ids": known_ids,
        "product_meta": product_meta,
        "id_to_group_id": id_to_group_id,
        "group_id_to_name": group_id_to_name,
        "group_id_to_rule": group_id_to_rule,
        "exact_candidate_count": exact_candidate_count,
        "normalized_candidate_count": normalized_candidate_count,
    }


def build_grouping_state(anchor_rows: List[Dict[str, str]], product_group_rows: List[Dict[str, str]]):
    auto = build_auto_groups(anchor_rows)
    overrides = sanitize_product_group_map_rows(product_group_rows)

    id_to_group_id = dict(auto["id_to_group_id"])
    group_id_to_name = dict(auto["group_id_to_name"])
    group_id_to_rule = dict(auto["group_id_to_rule"])
    ungrouped_overrides: Set[str] = set()
    invalid_override_count = 0

    for row in overrides:
        product_id = row["product_id"]
        if product_id not in auto["known_ids"]:
            invalid_override_count += 1
            continue
        if row["status"] == "ungrouped":
            id_to_group_id.pop(product_id, None)
            ungrouped_overrides.add(product_id)
            continue
        ungrouped_overrides.discard(product_id)
        id_to_group_id[product_id] = row["group_id"]
        group_id_to_name[row["group_id"]] = row["group_name"]
        group_id_to_rule[row["group_id"]] = row.get("rule") or "manual"

    for product_id in ungrouped_overrides:
        id_to_group_id.pop(product_id, None)

    id_to_entity_id: Dict[str, str] = {}
    entity_id_to_members: Dict[str, List[str]] = defaultdict(list)
    for product_id in auto["known_ids"]:
        entity_id = id_to_group_id.get(product_id, product_id)
        id_to_entity_id[product_id] = entity_id
        entity_id_to_members[entity_id].append(product_id)

    return {
        "id_to_entity_id": id_to_entity_id,
        "entity_id_to_members": entity_id_to_members,
        "group_id_to_name": group_id_to_name,
        "group_id_to_rule": group_id_to_rule,
        "stats": {
            "grouped_entity_count": sum(1 for _, members in entity_id_to_members.items() if len(members) > 1),
            "invalid_override_count": invalid_override_count,
        },
    }


def resolve_entity_id(product_id: str, grouping) -> str:
    product_id = str(product_id or "").strip()
    if not product_id:
        return ""
    return grouping["id_to_entity_id"].get(product_id, product_id)


def transform_anchor_scored_rows(rows: List[Dict[str, str]], grouping):
    grouped: Dict[str, Dict[str, object]] = {}
    sum_fields = [
        "first_customer_cnt", "product_order_cnt_1y", "product_unit_qty_1y", "revenue_90d",
        "AA_Broad", "AA_Heavy", "AA_Qualified", "PCA_Core", "PCA_Deep", "PCA_Scale",
    ]
    weighted_fields = [
        "AA_Score", "PCA_Score", "Entry_Gravity_Score", "Expansion_Gravity_Score",
        "repurchase_rate_90d", "first_customer_ratio", "p50_addl_order_cnt_90d",
        "p75_addl_order_cnt_90d", "p90_addl_order_cnt_90d", "addl_order_rate_90d",
        "p75_retention_days", "PrimaryAnchorScore",
    ]

    for row in rows:
        raw_id = read_product_id(row)
        if not raw_id:
            continue
        entity_id = resolve_entity_id(raw_id, grouping)
        if entity_id not in grouped:
            grouped[entity_id] = {
                "product_id": entity_id,
                "members": set(),
                "weighted": {},
            }
            for field in sum_fields:
                grouped[entity_id][field] = 0.0

        acc = grouped[entity_id]
        acc["members"].add(raw_id)

        weight = max(1.0, to_number(row.get("first_customer_cnt"), 0.0), to_number(row.get("product_order_cnt_1y"), 0.0))

        for field in sum_fields:
            acc[field] = to_number(acc[field], 0.0) + to_number(row.get(field), 0.0)

        for field in weighted_fields:
            value = to_number(row.get(field), math.nan)
            if math.isfinite(value):
                num, den = acc["weighted"].get(field, (0.0, 0.0))
                acc["weighted"][field] = (num + value * weight, den + weight)

    result = []
    for entity_id, acc in grouped.items():
        out = {
            "product_id": entity_id,
            "member_count": len(acc["members"]),
        }
        for field in sum_fields:
            out[field] = to_number(acc[field], 0.0)
        for field in weighted_fields:
            num, den = acc["weighted"].get(field, (0.0, 0.0))
            out[field] = (num / den) if den > 0 else None
        result.append(out)
    return result


def check_hybrid_behavior(anchor_rows: List[Dict[str, str]], grouping) -> Dict[str, int]:
    # quantity signature split check
    stripped_groups: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for row in anchor_rows:
        product_id = read_product_id(row)
        raw_name = read_product_name(row)
        if not product_id or not raw_name:
            continue
        stripped = normalize_group_name(raw_name)
        stripped_groups[stripped].append((product_id, raw_name))

    quantity_mix_violations = 0
    promo_collapsed_pairs = 0

    for _, members in stripped_groups.items():
        if len(members) < 2:
            continue

        by_signature: Dict[str, Set[str]] = defaultdict(set)
        promo_ids: Set[str] = set()
        base_ids: Set[str] = set()

        for product_id, raw_name in members:
            tokens, _ = parse_leading_bracket_tokens(raw_name)
            quantity_tokens = [token for token in tokens if is_quantity_token(token)]
            signature = "|".join(sorted(quantity_tokens))
            by_signature[signature].add(product_id)
            if any(is_promotion_token(token) for token in tokens):
                promo_ids.add(product_id)
            else:
                base_ids.add(product_id)

        signatures = [s for s in by_signature.keys() if s]
        if len(signatures) > 1:
            entity_per_signature = set()
            for signature in signatures:
                sample_id = sorted(by_signature[signature])[0]
                entity_per_signature.add(resolve_entity_id(sample_id, grouping))
            if len(entity_per_signature) < len(signatures):
                quantity_mix_violations += 1

        if promo_ids and base_ids:
            promo_entity = {resolve_entity_id(pid, grouping) for pid in promo_ids}
            base_entity = {resolve_entity_id(pid, grouping) for pid in base_ids}
            if promo_entity & base_entity:
                promo_collapsed_pairs += 1

    return {
        "quantity_mix_violations": quantity_mix_violations,
        "promo_collapsed_pairs": promo_collapsed_pairs,
    }


def verify(args) -> None:
    data_dir = Path(args.data_dir)
    anchor_rows = read_csv(data_dir / "pgm_scored.csv")
    transition_rows = read_csv(data_dir / "pgm_entry_to_expansion_transition.csv")
    cart_rows = read_csv(data_dir / "pgm_basket_gravity.csv")
    cart_detail_rows = read_csv(data_dir / "pgm_basket_gravity_detail.csv")
    aa_journey_rows = read_csv(data_dir / "_insight_entry_cohort_journey.csv")
    aa_path_rows = read_csv(data_dir / "_insight_entry_transition_path.csv")
    ca_profile_rows = read_csv(data_dir / "_insight_basket_gravity_profile.csv")

    product_group_rows = read_csv(Path(args.group_map))

    grouping = build_grouping_state(anchor_rows, product_group_rows)
    grouped_anchor_rows = transform_anchor_scored_rows(anchor_rows, grouping)

    anchor_ids = [read_product_id(row) for row in anchor_rows if read_product_id(row)]
    missing_mapped_ids = [pid for pid in anchor_ids if pid not in grouping["id_to_entity_id"]]

    raw_first_customer_cnt = sum(to_number(row.get("first_customer_cnt"), 0.0) for row in anchor_rows)
    grouped_first_customer_cnt = sum(to_number(row.get("first_customer_cnt"), 0.0) for row in grouped_anchor_rows)
    raw_revenue_90d = sum(to_number(row.get("revenue_90d"), 0.0) for row in anchor_rows)
    grouped_revenue_90d = sum(to_number(row.get("revenue_90d"), 0.0) for row in grouped_anchor_rows)

    quadrant_points = 0
    invalid_quadrant_points = 0
    for row in grouped_anchor_rows:
        entry = to_number(first_defined_value(row.get("AA_Score"), row.get("Entry_Gravity_Score")), math.nan)
        expansion = to_number(first_defined_value(row.get("PCA_Score"), row.get("Expansion_Gravity_Score")), math.nan)
        if math.isfinite(entry) and math.isfinite(expansion):
            quadrant_points += 1
        else:
            invalid_quadrant_points += 1

    known_entity_or_raw_ids = set(grouping["id_to_entity_id"].keys()) | set(grouping["id_to_entity_id"].values())

    def count_missing(rows: List[Dict[str, str]], keys: List[str]) -> int:
        missing = 0
        for row in rows:
            for key in keys:
                value = str(row.get(key, "") or "").strip()
                if not value:
                    continue
                if value not in known_entity_or_raw_ids:
                    missing += 1
        return missing

    transition_missing = count_missing(transition_rows, ["aa_product_id", "pca_product_id"])
    cart_missing = count_missing(cart_rows, ["product_id"])
    cart_detail_missing = count_missing(cart_detail_rows, ["i", "j"])
    aa_journey_missing = count_missing(aa_journey_rows, ["aa_product_id", "entry_product_id"])
    aa_path_missing = count_missing(aa_path_rows, ["aa_product_id", "entry_product_id", "pca_product_id", "expansion_product_id"])
    ca_profile_missing = count_missing(ca_profile_rows, ["product_id"])

    hybrid_check = check_hybrid_behavior(anchor_rows, grouping)

    print("PGM Verification Summary")
    print(f"- data_dir: {data_dir}")
    print(f"- group_map: {args.group_map}")
    print(f"- anchor_products: {len(anchor_ids)}")
    print(f"- grouped_entities: {len(grouped_anchor_rows)}")
    print(f"- grouped_entity_count(>1): {grouping['stats']['grouped_entity_count']}")
    print(f"- invalid_override_count: {grouping['stats']['invalid_override_count']}")
    print(f"- missing_mapped_ids: {len(missing_mapped_ids)}")
    print(f"- first_customer_cnt_delta: {grouped_first_customer_cnt - raw_first_customer_cnt}")
    print(f"- revenue_90d_delta: {grouped_revenue_90d - raw_revenue_90d}")
    print(f"- quadrant_points: {quadrant_points}")
    print(f"- invalid_quadrant_points: {invalid_quadrant_points}")
    print(f"- transition_missing_ids: {transition_missing}")
    print(f"- cart_missing_ids: {cart_missing}")
    print(f"- cart_detail_missing_ids: {cart_detail_missing}")
    print(f"- aa_journey_missing_ids: {aa_journey_missing}")
    print(f"- aa_path_missing_ids: {aa_path_missing}")
    print(f"- ca_profile_missing_ids: {ca_profile_missing}")
    print(f"- hybrid_quantity_mix_violations: {hybrid_check['quantity_mix_violations']}")
    print(f"- hybrid_promo_collapsed_pairs: {hybrid_check['promo_collapsed_pairs']}")

    errors = []
    if missing_mapped_ids:
        errors.append(f"missing group mapping for {len(missing_mapped_ids)} product ids")
    if abs(grouped_first_customer_cnt - raw_first_customer_cnt) > 1e-6:
        errors.append("first_customer_cnt sum mismatch after grouped recompute")
    if abs(grouped_revenue_90d - raw_revenue_90d) > 1e-6:
        errors.append("revenue_90d sum mismatch after grouped recompute")
    if invalid_quadrant_points > 0:
        errors.append("invalid quadrant points found")
    if any(x > 0 for x in [transition_missing, cart_missing, cart_detail_missing, aa_journey_missing, aa_path_missing, ca_profile_missing]):
        errors.append("table id compatibility failure")
    if hybrid_check["quantity_mix_violations"] > 0:
        errors.append("hybrid quantity split violation")

    if errors:
        print("\nErrors:")
        for err in errors:
            print(f"  - {err}")
        raise SystemExit(1)

    print("\nAll PGM checks passed.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data", help="Directory containing csv files")
    parser.add_argument("--group-map", default="data/pgm_product_group_map.csv", help="product_group_map csv path")
    args = parser.parse_args()
    verify(args)


if __name__ == "__main__":
    main()
