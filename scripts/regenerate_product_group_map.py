#!/usr/bin/env python3
"""Regenerate product_group_map CSV from pgm_scored.csv using hybrid grouping rules.

Hybrid rule:
- Remove only promotion-like leading bracket tags (e.g. [VIP], [특가], [사은품]).
- Keep quantity/volume leading bracket tags (e.g. [1매], [14매], [40ml]).
"""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set, Tuple

PROMO_KEYWORDS = [
    "vip", "특가", "사은품", "전용", "체험", "한정", "이벤트", "세일", "sale", "혜택",
    "증정", "비밀판매", "아로셀데이", "타임", "재구매", "여름선물", "광복절", "설 맞이", "세컨드",
]


def to_number(value, fallback=0.0):
    try:
        out = float(value)
        if out != out:  # NaN
            return fallback
        return out
    except Exception:
        return fallback


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


def build_deterministic_group_id(seed: str) -> str:
    base = slugify(seed) or "group"
    return f"grp_{base}_{hash_string(seed)}"


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
    kept_tokens = [token for token in tokens if not is_promotion_token(token)]
    prefix = " ".join(f"[{token}]" for token in kept_tokens)
    out = f"{prefix} {rest}".strip()
    return re.sub(r"\s+", " ", out).strip()


@dataclass
class ProductMeta:
    product_id: str
    raw_name: str
    norm_name: str
    revenue: float


class UnionFind:
    def __init__(self):
        self.parent: Dict[str, str] = {}

    def ensure(self, x: str) -> None:
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

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra

    def union_all(self, ids: Set[str]) -> None:
        seq = sorted(ids)
        if len(seq) < 2:
            return
        head = seq[0]
        for item in seq[1:]:
            self.union(head, item)


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
    for ids in ids_by_exact.values():
        if len(ids) > 1:
            uf.union_all(ids)
    for ids in ids_by_norm.values():
        if len(ids) > 1:
            uf.union_all(ids)

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
        group_id = build_deterministic_group_id(seed)
        rule = "exact_name" if len(exact_names) == 1 else "normalized_prefix"

        for pid in sorted_members:
            id_to_group_id[pid] = group_id
        group_id_to_name[group_id] = display_name
        group_id_to_rule[group_id] = rule

    return id_to_group_id, group_id_to_name, group_id_to_rule


def write_group_map(anchor_rows: List[Dict[str, str]], output_path: Path) -> int:
    id_to_group_id, group_id_to_name, group_id_to_rule = build_auto_groups(anchor_rows)

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = []
    for product_id in sorted(id_to_group_id.keys()):
        group_id = id_to_group_id[product_id]
        rows.append({
            "product_id": product_id,
            "status": "grouped",
            "group_id": group_id,
            "group_name": group_id_to_name.get(group_id, group_id),
            "rule": group_id_to_rule.get(group_id, "normalized_prefix"),
            "updated_at": now_iso,
        })

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["product_id", "status", "group_id", "group_name", "rule", "updated_at"]
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Regenerate pgm_product_group_map.csv with hybrid grouping")
    parser.add_argument("--anchor", default="data/pgm_scored.csv", help="Input pgm_scored CSV path")
    parser.add_argument("--output", default="data/pgm_product_group_map.csv", help="Output product_group_map CSV path")
    args = parser.parse_args()

    anchor_path = Path(args.anchor)
    if not anchor_path.exists():
        raise SystemExit(f"Anchor CSV not found: {anchor_path}")

    with anchor_path.open("r", encoding="utf-8-sig", newline="") as f:
        anchor_rows = list(csv.DictReader(f))

    written = write_group_map(anchor_rows, Path(args.output))
    print(f"Wrote {written} grouped rows -> {args.output}")


if __name__ == "__main__":
    main()
