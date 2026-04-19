# pgm_ops

`pgm_ops`는 PGM 1.0 운영 툴 v0를 위한 additive app입니다. 기존 PGM 코어를 재설계하지 않고, 로컬 CSV artifact를 입력으로 받아 `staging -> mart -> view_model -> UI` 레이어를 분리한 상태판을 제공합니다.

PGM Ops는 역할 구조 소개판이 아니라, 매출 증감 원인을 역할 축으로 분해해 읽는 운영 콘솔을 목표로 합니다.

현재 기본 화면 흐름은 아래 순서를 기준으로 구성합니다.

1. 어제(최근 확정일) / 최근 7일 / 최근 30일 매출 분해
2. 지금 먼저 볼 점검
3. 역할별 매출 변화 drilldown
4. 필요할 때만 상품 상세 탐색
5. 전환/복귀 보조 진단
6. 데이터 해석 주의 / 관측 범위 안내

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
- `product_role_profile`의 프로필 공백과 `product_role_state_daily`의 기준일 관측 상태 공백은 같은 뜻이 아닙니다.

## Local pipeline

`apps/pgm_ops/package.json` 기준:

```bash
cd apps/pgm_ops
npm run all
```

Rosetta raw snapshot까지 함께 다시 받으려면:

```bash
cd apps/pgm_ops
npm run all -- --refresh-rosetta --as-of-date 2026-04-17 --lookback-days 120 --mx-channel-id 567375433033637313 --mx-platform GODOMALL5
```

또는 raw_extract만 먼저 갱신하려면:

```bash
cd apps/pgm_ops
npm run refresh-rosetta -- --as-of-date 2026-04-17 --lookback-days 120 --mx-channel-id 567375433033637313 --mx-platform GODOMALL5
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

## 관측 상태 공백 원칙

- `product_role_state_daily`는 기준일 당일 스냅샷만 사용합니다.
- 기준일 당일 스냅샷이 없으면 mart에서는 관측 상태 공백을 유지합니다.
- UI와 view_model은 이 공백을 `관측 상태 없음`으로만 라벨링하며, 역할 부재나 프로필 부재와 같은 의미로 합치지 않습니다.
- `product_role_profile`의 최신 스냅샷 사용은 상품 단위 프로필용이며 일별 관측 상태 공백 보정에 쓰이지 않습니다.
- 프로필 정보가 비면 `프로필 정보 없음`으로 분리해서 보여 주고, 일별 관측 상태 공백과 혼용하지 않습니다.

## 해석 메모

- `프로필`과 `상태`는 같은 개념이 아닙니다.
  - `프로필`: 상품 기준 최신 프로필
  - `상태`: 기준일 당일 관측 상태
- `관측 상태 없음`은 역할 부재가 아니라 기준일 당일 스냅샷 부재를 뜻합니다.
- 7일 / 30일 / 90일 비교는 같은 기준일에서 본 기간별 매출 분해 비교입니다.
- 이 비교는 상품별 역할 서사를 확정하지 않으며, “어제 첫구매 유도였고 기준일에는 단골 유도” 같은 서사를 만들지 않습니다.
- 역할은 매출 해석 축이며, 상품 단위 실제 역할 이동 이력을 주장하지 않습니다.

## Deferred handoff boundary

- BHI는 stub note만 유지하며 primary metric으로 올리지 않습니다.
- member / UTM / channel drill-down은 v0 운영 경로 밖에 둡니다.
- transition / return loop experience는 이 앱에서 재구성하지 않습니다.
- backend/API 이관은 별도 단계입니다.

## Handoff docs

- [artifact_contract.md](./docs/artifact_contract.md)
- [metric_dictionary.md](./docs/metric_dictionary.md)
- [handoff_checklist.md](./docs/handoff_checklist.md)
