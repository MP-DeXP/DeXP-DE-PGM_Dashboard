# Handoff Checklist

## Runtime

- `apps/pgm_ops/index.html`가 브라우저에서 직접 열리는가
- 정적 서버 환경에서 `artifacts/view_model/*.csv`를 읽는가
- artifact-backed / mixed / sample fallback 상태가 UI에 명확히 구분되어 보이는가
- `file://` 환경에서 sample fallback으로 안전하게 전환되되, sample이 운영 truth로 읽히지 않도록 문구가 분리되어 있는가

## Pipeline

- `npm run all`이 `raw_extract -> staging -> mart -> view_model -> qa`를 생성하는가
- raw 입력 누락 시 어떤 파일이 비었는지 명확히 보고되는가
- validators 결과가 `artifacts/qa/`에 남는가

## Contracts

- schema registry가 required columns와 grain을 정의하는가
- `product_daily_metrics`, `product_role_profile`, `product_role_state_daily`, `revenue_structure_daily`, `brand_operating_status_daily`가 생성되는가
- overview/product/priority view model이 생성되는가

## Explicitly deferred

- BHI canonical source 확정
  현재는 stub note만 유지하고 primary metric으로 올리지 않음
- member / UTM / channel drill-down 완결
  현재 운영 테이블/우선 점검/상세 패널 어느 곳에도 숨겨진 fallback join을 두지 않음
- transition / return loop experience 완결
  현재 UI는 해당 경험을 재구성하지 않고 handoff boundary로만 명시함
- backend/API 이관

## Production handoff notes

- UI는 mart가 아니라 view_model만 읽도록 유지합니다.
- same-date role state blank rule은 production에서도 유지해야 합니다.
- blank는 mart에서 blank로 유지하고, view_model/UI에서만 `PGM 미관측` 라벨을 붙입니다.
- product master historical gap은 UI 표기 fallback과 QA 지표 둘 다 필요합니다.
- sample fixture는 프로토타입 검증용이며 production truth source가 아닙니다.
