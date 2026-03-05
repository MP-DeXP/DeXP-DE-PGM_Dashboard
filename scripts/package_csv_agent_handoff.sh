#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-agent_handoff}"

mkdir -p "${OUT_DIR}"
cp docs/00_APF_to_PGM_Naming_Migration.md "${OUT_DIR}/"
cp configs/apf_insight_generation_spec.json "${OUT_DIR}/"
cp scripts/generate_insight_csvs.py "${OUT_DIR}/"
cp scripts/validate_insight_csvs.py "${OUT_DIR}/"

cat > "${OUT_DIR}/README.txt" <<'TXT'
PGM Insight CSV Agent Handoff Package

Included files:
- 00_APF_to_PGM_Naming_Migration.md
- apf_insight_generation_spec.json
- generate_insight_csvs.py
- validate_insight_csvs.py

Recommended flow:
1) Read 00_APF_to_PGM_Naming_Migration.md
2) Generate 5 output csv files using spec json
3) Validate output:
   python3 validate_insight_csvs.py --dir <output_dir>
TXT

zip -r "${OUT_DIR}.zip" "${OUT_DIR}" >/dev/null
echo "Created package: ${OUT_DIR}.zip"
