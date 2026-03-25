#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DASHBOARD_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
APF_ROOT="$(cd "${DASHBOARD_ROOT}/.." && pwd)"
CORE_ROOT="${APF_ROOT}/APF_core_logic"

OUT_DIR="${1:-agent_handoff}"

mkdir -p "${OUT_DIR}"
cp "${DASHBOARD_ROOT}/docs/00_APF_to_PGM_Naming_Migration.md" "${OUT_DIR}/"
cp "${DASHBOARD_ROOT}/configs/apf_insight_generation_spec.json" "${OUT_DIR}/"
cp "${DASHBOARD_ROOT}/scripts/generate_insight_csvs.py" "${OUT_DIR}/"
cp "${CORE_ROOT}/validate_pgm_contracts.py" "${OUT_DIR}/"
rm -rf "${OUT_DIR}/pgm_contracts"
cp -R "${CORE_ROOT}/pgm_contracts" "${OUT_DIR}/"

cat > "${OUT_DIR}/README.txt" <<'TXT'
PGM Insight CSV Agent Handoff Package

Included files:
- 00_APF_to_PGM_Naming_Migration.md
- apf_insight_generation_spec.json
- generate_insight_csvs.py
- validate_pgm_contracts.py
- pgm_contracts/

Recommended flow:
1) Read 00_APF_to_PGM_Naming_Migration.md
2) Generate output csv files using spec json
3) Validate output:
   python3 validate_pgm_contracts.py --csv-dir <output_dir>
TXT

zip -r "${OUT_DIR}.zip" "${OUT_DIR}" >/dev/null
echo "Created package: ${OUT_DIR}.zip"
