#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import math
from collections import defaultdict
from pathlib import Path


def read_csv(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def to_float(v, default=0.0):
    try:
        if v is None:
            return default
        s = str(v).strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def to_int(v, default=0):
    try:
        if v is None:
            return default
        s = str(v).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def clamp(v, lo=0.0, hi=1.0):
    return max(lo, min(hi, v))


def pick(row, *keys, default=""):
    for k in keys:
        if k in row and str(row[k]).strip() != "":
            return row[k]
    return default


def write_csv(path: Path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--brand-score", required=True)
    p.add_argument("--anchor-scored", required=True)
    p.add_argument("--anchor-transition", required=True)
    p.add_argument("--cart-anchor", required=True)
    p.add_argument("--cart-anchor-detail", required=True)
    p.add_argument("--out-dir", required=True)
    args = p.parse_args()

    out_dir = Path(args.out_dir)

    brand_score = read_csv(Path(args.brand_score))
    anchor_scored = read_csv(Path(args.anchor_scored))
    anchor_transition = read_csv(Path(args.anchor_transition))
    cart_anchor = read_csv(Path(args.cart_anchor))
    cart_anchor_detail = read_csv(Path(args.cart_anchor_detail))

    today = dt.date.today().isoformat()

    # Product maps
    aa_type_map = {}
    pca_type_map = {}
    revenue_map = {}
    for r in anchor_scored:
        pid = str(pick(r, "product_id", "Product_ID", "\ufeffproduct_id", default="")).strip()
        if not pid:
            continue
        aa_type_map[pid] = pick(r, "AA_Primary_Type", default="Unknown") or "Unknown"
        pca_type_map[pid] = pick(r, "PCA_Primary_Type", default="Unknown") or "Unknown"
        revenue_map[pid] = to_float(pick(r, "revenue_90d", default=0), 0.0)

    # Transition aggregates by AA
    trans_agg = defaultdict(lambda: {"trans_sum": 0.0, "cohort": 0.0, "days_num": 0.0, "days_den": 0.0})
    for r in anchor_transition:
        aa = str(pick(r, "aa_product_id", default="")).strip()
        if not aa:
            continue
        tc = to_float(pick(r, "transition_customer_cnt", "transition_customers", default=0), 0.0)
        cohort = to_float(pick(r, "aa_cohort_customer_cnt", "cohort_customers", default=0), 0.0)
        days = to_float(pick(r, "avg_days_to_pca", default=0), 0.0)
        trans_agg[aa]["trans_sum"] += tc
        trans_agg[aa]["cohort"] = max(trans_agg[aa]["cohort"], cohort)
        if tc > 0 and days > 0:
            trans_agg[aa]["days_num"] += tc * days
            trans_agg[aa]["days_den"] += tc

    # top1 companion lookup from cart detail
    top_companion = {}
    for r in cart_anchor_detail:
        i = str(pick(r, "i", default="")).strip()
        j = str(pick(r, "j", default="")).strip()
        c = to_float(pick(r, "co_order_cnt", default=0), 0.0)
        if not i or not j:
            continue
        if i not in top_companion or c > top_companion[i][1]:
            top_companion[i] = (j, c)
        # symmetric fallback
        if j not in top_companion or c > top_companion[j][1]:
            top_companion[j] = (i, c)

    # 1) aa_cohort_journey.csv
    aa_rows = []
    for r in anchor_scored:
        pid = str(pick(r, "product_id", "Product_ID", "\ufeffproduct_id", default="")).strip()
        if not pid:
            continue

        cohort_customers = max(0, to_int(pick(r, "first_customer_cnt", default=0), 0))
        if cohort_customers <= 0:
            continue

        repeat90 = clamp(to_float(pick(r, "repurchase_rate_90d", default=0), 0.0))
        repeat30 = clamp(repeat90 * 0.72, 0.0, repeat90)
        repeat7 = clamp(repeat30 * 0.55, 0.0, repeat30)

        t = trans_agg.get(pid)
        if t and t["cohort"] > 0:
            pca90 = clamp(t["trans_sum"] / t["cohort"])
            avg_days = t["days_num"] / t["days_den"] if t["days_den"] > 0 else ""
        else:
            pca90 = clamp(repeat90 * 0.70)
            avg_days = ""

        pca30 = clamp(pca90 * 0.62, 0.0, pca90)

        rev90 = to_float(pick(r, "revenue_90d", default=0), 0.0)
        avg_rev90 = rev90 / cohort_customers if cohort_customers > 0 else 0.0

        aa_rows.append(
            {
                "cohort_date": today,
                "aa_product_id": pid,
                "aa_type": pick(r, "AA_Primary_Type", default="Unknown") or "Unknown",
                "cohort_customers": cohort_customers,
                "repeat_7d_rate": round(repeat7, 6),
                "repeat_30d_rate": round(repeat30, 6),
                "repeat_90d_rate": round(repeat90, 6),
                "pca_transition_30d_rate": round(pca30, 6),
                "pca_transition_90d_rate": round(pca90, 6),
                "avg_days_to_pca": round(avg_days, 4) if avg_days != "" else "",
                "avg_revenue_90d": round(avg_rev90, 4),
            }
        )

    aa_rows.sort(key=lambda x: int(x["cohort_customers"]), reverse=True)

    write_csv(
        out_dir / "aa_cohort_journey.csv",
        [
            "cohort_date",
            "aa_product_id",
            "aa_type",
            "cohort_customers",
            "repeat_7d_rate",
            "repeat_30d_rate",
            "repeat_90d_rate",
            "pca_transition_30d_rate",
            "pca_transition_90d_rate",
            "avg_days_to_pca",
            "avg_revenue_90d",
        ],
        aa_rows,
    )

    # 2) aa_transition_path.csv
    trans_rows = []
    for r in anchor_transition:
        aa = str(pick(r, "aa_product_id", default="")).strip()
        pca = str(pick(r, "pca_product_id", default="")).strip()
        if not aa or not pca:
            continue
        trans_rows.append(
            {
                "cohort_date": today,
                "aa_product_id": aa,
                "aa_type": aa_type_map.get(aa, "Unknown"),
                "pca_product_id": pca,
                "transition_customers": to_int(pick(r, "transition_customer_cnt", default=0), 0),
                "transition_rate": round(to_float(pick(r, "transition_rate", default=0), 0.0), 6),
                "avg_days_to_pca": round(to_float(pick(r, "avg_days_to_pca", default=0), 0.0), 4),
            }
        )

    write_csv(
        out_dir / "aa_transition_path.csv",
        [
            "cohort_date",
            "aa_product_id",
            "aa_type",
            "pca_product_id",
            "transition_customers",
            "transition_rate",
            "avg_days_to_pca",
        ],
        trans_rows,
    )

    # 3) ca_profile.csv
    ca_rows = []
    for r in cart_anchor:
        pid = str(pick(r, "product_id", default="")).strip()
        if not pid:
            continue
        top1_companion = top_companion.get(pid, ("", 0))[0]
        ca_rows.append(
            {
                "product_id": pid,
                "ca_type": pick(r, "CA_Primary_Type", default="None") or "None",
                "attach_rate": round(to_float(pick(r, "attach_rate", default=0), 0.0), 6),
                "median_cart_size": round(to_float(pick(r, "median_cart_size", default=0), 0.0), 4),
                "breadth_lift": round(to_float(pick(r, "breadth_lift", default=0), 0.0), 6),
                "companion_count": to_int(pick(r, "companion_cnt", default=0), 0),
                "top1_share": round(to_float(pick(r, "top1_share", default=0), 0.0), 6),
                "top3_share": round(to_float(pick(r, "top3_share", default=0), 0.0), 6),
                "top1_companion_product_id": top1_companion,
            }
        )

    write_csv(
        out_dir / "ca_profile.csv",
        [
            "product_id",
            "ca_type",
            "attach_rate",
            "median_cart_size",
            "breadth_lift",
            "companion_count",
            "top1_share",
            "top3_share",
            "top1_companion_product_id",
        ],
        ca_rows,
    )

    # 4) bii_window.csv (approximation from aggregated inputs)
    b0 = brand_score[0] if brand_score else {}
    bhi = to_float(pick(b0, "BHI", "Brand_Health_Index", "brand_health_index", "Brand_Health_Score", default=0.0), 0.0)
    confidence = pick(b0, "Confidence_Index", default="Medium") or "Medium"

    total_fc = sum(to_float(pick(r, "first_customer_cnt", default=0), 0.0) for r in anchor_scored)
    total_rev90 = sum(to_float(pick(r, "revenue_90d", default=0), 0.0) for r in anchor_scored)
    avg_clv90 = (total_rev90 / total_fc) if total_fc > 0 else 0.0

    if total_fc > 0:
        repeat90_brand = sum(
            to_float(pick(r, "repurchase_rate_90d", default=0), 0.0)
            * to_float(pick(r, "first_customer_cnt", default=0), 0.0)
            for r in anchor_scored
        ) / total_fc
    else:
        repeat90_brand = 0.0

    total_orders = sum(to_float(pick(r, "order_cnt", default=0), 0.0) for r in cart_anchor)
    if total_orders > 0:
        attach_rate_brand = sum(
            to_float(pick(r, "attach_rate", default=0), 0.0)
            * to_float(pick(r, "order_cnt", default=0), 0.0)
            for r in cart_anchor
        ) / total_orders
    else:
        attach_rate_brand = 0.0

    window_params = {
        7: {"active": 0.28, "clv_factor": 0.32, "repeat_factor": 0.35},
        30: {"active": 0.58, "clv_factor": 0.62, "repeat_factor": 0.68},
        90: {"active": 0.85, "clv_factor": 1.00, "repeat_factor": 1.00},
        365: {"active": 1.00, "clv_factor": 1.15, "repeat_factor": 1.12},
    }

    baseline_days = 365
    baseline_clv = max(avg_clv90 * window_params[365]["clv_factor"], 1e-6)

    bii_rows = []
    for w in [7, 30, 90, 365]:
        prm = window_params[w]
        avg_clv_t = max(avg_clv90 * prm["clv_factor"], 1e-6)
        clv_norm = math.sqrt(avg_clv_t / baseline_clv)

        repeat_t = clamp(repeat90_brand * prm["repeat_factor"])
        attach_t = clamp(attach_rate_brand * (0.95 if w < 90 else 1.0))
        depth_t = clamp(0.7 * repeat_t + 0.3 * attach_t)

        customer_strength_norm = math.sqrt(max(prm["active"] * depth_t, 0.0))
        bii = bhi * clv_norm * customer_strength_norm

        bii_rows.append(
            {
                "as_of_date": today,
                "window_days": w,
                "bii": round(bii, 6),
                "bhi": round(bhi, 6),
                "clv_norm": round(clv_norm, 6),
                "customer_strength_norm": round(customer_strength_norm, 6),
                "stage": "Stable",
                "baseline_days": baseline_days,
                "confidence": confidence,
            }
        )

    write_csv(
        out_dir / "bii_window.csv",
        [
            "as_of_date",
            "window_days",
            "bii",
            "bhi",
            "clv_norm",
            "customer_strength_norm",
            "stage",
            "baseline_days",
            "confidence",
        ],
        bii_rows,
    )

    # 5) apf_action_rules.csv (default rules)
    rules = [
        {
            "rule_id": "mk_aa_broad_low_transition",
            "domain": "marketing",
            "condition_expr": "aa_broad_ratio > 0.5 && pca_transition_90d_rate < 0.25",
            "priority": 1,
            "title_ko": "Entry Gravity-Broad 이후 Expansion 전환 강화",
            "action_ko": "Entry Gravity 7일 내 Expansion Gravity-Core 유도 CRM/리타게팅 캠페인을 우선 실행합니다.",
            "impact_ko": "Entry 낭비 축소 및 90일 전이율 개선",
        },
        {
            "rule_id": "mk_transition_concentration",
            "domain": "marketing",
            "condition_expr": "transition_top3_share > 0.65",
            "priority": 2,
            "title_ko": "전이 경로 과집중 완화",
            "action_ko": "상위 경로를 유지하면서 대체 Expansion Gravity 노출 A/B 테스트를 병행합니다.",
            "impact_ko": "경로 리스크 분산",
        },
        {
            "rule_id": "mk_slow_pca",
            "domain": "marketing",
            "condition_expr": "avg_days_to_pca > 18",
            "priority": 1,
            "title_ko": "Expansion Gravity 도달 속도 개선",
            "action_ko": "CRM 발화 시점을 앞당기고 3~7일 구간 혜택을 강화합니다.",
            "impact_ko": "평균 전이 소요일 단축",
        },
        {
            "rule_id": "md_pair_focus",
            "domain": "md",
            "condition_expr": "ca_pair_top1_share_max > 0.7",
            "priority": 1,
            "title_ko": "Basket Gravity-Pair 고정 번들 운영",
            "action_ko": "상위 Pair 상품의 고정 번들/교차추천 슬롯을 우선 운영합니다.",
            "impact_ko": "Basket Gravity 확장률 향상",
        },
        {
            "rule_id": "md_set_landing",
            "domain": "md",
            "condition_expr": "ca_set_breadth_lift_avg > 1.5",
            "priority": 2,
            "title_ko": "Basket Gravity-Set 세트 랜딩 강화",
            "action_ko": "세트형 상품군 랜딩을 분리하고 구성 SKU 재고를 선제 관리합니다.",
            "impact_ko": "AOV 상승 및 이탈 감소",
        },
        {
            "rule_id": "md_scale_inventory",
            "domain": "md",
            "condition_expr": "pca_scale_concentration > 0.55",
            "priority": 1,
            "title_ko": "Expansion Gravity-Scale 재고 방어",
            "action_ko": "Scale 상위 SKU의 안전재고 기준을 상향하고 품절 대응 룰을 적용합니다.",
            "impact_ko": "사슬 붕괴 리스크 완화",
        },
    ]

    write_csv(
        out_dir / "apf_action_rules.csv",
        [
            "rule_id",
            "domain",
            "condition_expr",
            "priority",
            "title_ko",
            "action_ko",
            "impact_ko",
        ],
        rules,
    )

    assumptions = [
        "Generated with aggregated source files only (no raw member-order logs).",
        "cohort_date/as_of_date set to generation date.",
        "aa_cohort_journey repeat_7d/repeat_30d inferred from repeat_90d scaling factors.",
        "bii_window derived using heuristic short/mid/long factors; treat as demo baseline.",
        "apf_action_rules is default template and can be edited by business users.",
    ]
    (out_dir / "GENERATED_NOTES.txt").write_text("\n".join(assumptions) + "\n", encoding="utf-8")

    print(f"Generated files in: {out_dir}")
    for name in [
        "aa_cohort_journey.csv",
        "aa_transition_path.csv",
        "ca_profile.csv",
        "bii_window.csv",
        "apf_action_rules.csv",
        "GENERATED_NOTES.txt",
    ]:
        print(f"- {name}")


if __name__ == "__main__":
    main()
