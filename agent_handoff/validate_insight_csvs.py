#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path


REQUIRED = {
    "aa_cohort_journey.csv": [
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
    "aa_transition_path.csv": [
        "cohort_date",
        "aa_product_id",
        "aa_type",
        "pca_product_id",
        "transition_customers",
        "transition_rate",
        "avg_days_to_pca",
    ],
    "ca_profile.csv": [
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
    "bii_window.csv": [
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
    "apf_action_rules.csv": [
        "rule_id",
        "domain",
        "condition_expr",
        "priority",
        "title_ko",
        "action_ko",
        "impact_ko",
    ],
}


def read_csv(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
        return f, rows


def as_float(v):
    try:
        return float(str(v).strip())
    except Exception:
        return None


def check_rate_range(rows, cols, errors, fname):
    for idx, row in enumerate(rows, start=2):
        for c in cols:
            if c not in row:
                continue
            val = as_float(row[c])
            if val is None:
                continue
            if val < 0 or val > 1:
                errors.append(f"{fname}:{idx} {c} out of range [0,1]: {val}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, help="Directory containing generated csv files")
    args = parser.parse_args()

    d = Path(args.dir)
    errors = []
    warnings = []

    loaded = {}
    for fname, cols in REQUIRED.items():
        p = d / fname
        if not p.exists():
            errors.append(f"missing file: {fname}")
            continue
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames or []
            rows = list(reader)
        loaded[fname] = rows
        missing_cols = [c for c in cols if c not in header]
        if missing_cols:
            errors.append(f"{fname} missing columns: {', '.join(missing_cols)}")
        if len(rows) == 0:
            warnings.append(f"{fname} has no data rows")

    if "aa_cohort_journey.csv" in loaded:
        rows = loaded["aa_cohort_journey.csv"]
        for idx, row in enumerate(rows, start=2):
            r7 = as_float(row.get("repeat_7d_rate", ""))
            r30 = as_float(row.get("repeat_30d_rate", ""))
            r90 = as_float(row.get("repeat_90d_rate", ""))
            if r7 is not None and r30 is not None and r90 is not None:
                if not (r7 <= r30 <= r90):
                    errors.append(f"aa_cohort_journey.csv:{idx} monotonic rule violation ({r7}, {r30}, {r90})")

    rate_cols = [
        "repeat_7d_rate",
        "repeat_30d_rate",
        "repeat_90d_rate",
        "pca_transition_30d_rate",
        "pca_transition_90d_rate",
        "transition_rate",
        "attach_rate",
        "top1_share",
        "top3_share",
    ]
    for fname, rows in loaded.items():
        check_rate_range(rows, rate_cols, errors, fname)

    if "bii_window.csv" in loaded:
        windows = {int(as_float(r.get("window_days")) or -1) for r in loaded["bii_window.csv"]}
        expected = {7, 30, 90, 365}
        if not expected.issubset(windows):
            errors.append(f"bii_window.csv missing windows: {sorted(expected - windows)}")

    if "apf_action_rules.csv" in loaded and len(loaded["apf_action_rules.csv"]) < 3:
        errors.append("apf_action_rules.csv must contain at least 3 rules")

    print("Validation Summary")
    print(f"- directory: {d}")
    print(f"- errors: {len(errors)}")
    print(f"- warnings: {len(warnings)}")

    if warnings:
        print("\\nWarnings:")
        for w in warnings:
            print(f"  - {w}")

    if errors:
        print("\\nErrors:")
        for e in errors:
            print(f"  - {e}")
        raise SystemExit(1)

    print("\\nAll required checks passed.")


if __name__ == "__main__":
    main()

