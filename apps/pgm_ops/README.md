# pgm_ops

`pgm_ops`는 PGM 1.0 운영 툴 v0를 위한 additive app입니다. 기존 PGM 코어를 재설계하지 않고, 로컬 CSV artifact를 입력으로 받아 `staging -> mart -> view_model -> UI` 레이어를 분리한 상태판을 제공합니다.

## Scope

- frontend-only, Vanilla HTML / JS / CSS
- file/artifact driven
- Trino silver-first + existing PGM gold consumption을 가정한 CSV 계약
- UI는 `artifacts/view_model/*.csv`만 직접 읽음

## Run

1. 브라우저에서 `apps/pgm_ops/`를 엽니다.
2. 정적 서버로 열면 `artifacts/view_model/*.csv`를 직접 읽습니다.
3. 브라우저에서 해당 파일을 읽지 못할 때만 내장 sample fallback으로 전환됩니다.
4. `?sample=1`을 붙이면 sample fallback 모드를 강제로 확인할 수 있습니다.

중요한 구분:

- artifact-backed 화면만 현재 구현 결과입니다.
- sample fallback 화면은 UI 동작과 copy 검증용 예시이며 운영 판단 근거가 아닙니다.
- 비어 있는 artifact는 sample로 대체하지 않고 그대로 빈 상태로 남깁니다.

## Local pipeline

`apps/pgm_ops/package.json` 기준:

```bash
cd apps/pgm_ops
npm run all
```

실행 결과:

- `artifacts/staging/`
- `artifacts/mart/`
- `artifacts/view_model/`
- `artifacts/qa/`

## Input files

기본 raw 입력 위치는 `artifacts/raw_extract/`입니다.

- `orders.csv`
- `order_items.csv`
- `products.csv`
- `product_daily.csv`
- `pgm_scored.csv`

이 repo에는 sample-safe raw fixture가 포함되어 있어, 런타임 데이터가 없어도 파이프라인과 UI를 바로 검증할 수 있습니다.

UI 내장 sample fallback은 위 fixture와 별개의 브라우저 안전장치입니다. 둘 다 검증용이며 production truth source가 아닙니다.

## Non-goals

- PGM core logic 재설계
- PAI 포함
- channel / member / BHI 완결 구현
- backend / API / DB migration 추가

## Explicit blank rule

- `product_role_state_daily`는 same-date snapshot only입니다.
- same-date snapshot이 없으면 mart에서는 blank를 유지합니다.
- UI와 view_model은 이 blank를 `PGM 미관측`으로만 라벨링하며, latest-available role로 보정하지 않습니다.
- `product_role_profile`의 latest snapshot 사용은 product-grain profile용이며 daily role-state blank 보정에 쓰이지 않습니다.

## Deferred handoff boundary

- BHI는 stub note만 유지하며 primary metric으로 올리지 않습니다.
- member / UTM / channel drill-down은 v0 운영 경로 밖에 둡니다.
- transition / return loop experience는 이 앱에서 재구성하지 않습니다.
- backend/API 이관은 별도 단계입니다.

## Handoff docs

- [artifact_contract.md](./docs/artifact_contract.md)
- [metric_dictionary.md](./docs/metric_dictionary.md)
- [handoff_checklist.md](./docs/handoff_checklist.md)
