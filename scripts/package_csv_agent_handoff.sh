#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-agent_handoff}"

mkdir -p "${OUT_DIR}"
cp docs/APF_INSIGHT_CSV_AGENT_PROMPT.md "${OUT_DIR}/"
cp configs/apf_insight_generation_spec.json "${OUT_DIR}/"
cp scripts/generate_insight_csvs.py "${OUT_DIR}/"
cp scripts/validate_insight_csvs.py "${OUT_DIR}/"

cat > "${OUT_DIR}/README.txt" <<'TXT'
APF Insight CSV Agent Handoff Package

Included files:
- APF_INSIGHT_CSV_AGENT_PROMPT.md
- apf_insight_generation_spec.json
- generate_insight_csvs.py
- validate_insight_csvs.py

Recommended flow:
1) Read APF_INSIGHT_CSV_AGENT_PROMPT.md
2) Generate 5 output csv files using spec json
3) Validate output:
   python3 validate_insight_csvs.py --dir <output_dir>
TXT

zip -r "${OUT_DIR}.zip" "${OUT_DIR}" >/dev/null
echo "Created package: ${OUT_DIR}.zip"

