# Brand Impact Index (BII) v1.0

## 목적
`BII`는 브랜드의 구조 건강도(`BHI`)가 기간별 상업 체력으로 얼마나 발현되고 있는지를 측정하는 다기간 지표다.

Brand Score가 브랜드의 구조를 본다면, BII는 그 구조가 최근 운영 구간에서 어떤 수준의 상업 체력으로 나타나는지를 본다.

## 핵심 정의
각 기간 `t ∈ {1, 7, 30, 90, 365}` 에 대해:

`BII_t = BHI × CLV_t_norm × Customer_Strength_t_norm`

여기서:

- `BHI`: `brand_score.csv`의 5축 기반 Brand Health Index
- `CLV_t_norm = sqrt(Avg_CLV_t / Avg_CLV_baseline)`
- `Customer_Strength_t_norm = sqrt((Active_Customers_t × Depth_t) / Active_Customers_baseline)`
- `Depth_t = 0.7 × RepeatRate_t + 0.3 × AttachRate_t`

## BII가 보는 것
- 구조 건강도는 이미 충분한가
- 그 구조가 최근 기간에 실제 고객 가치로 전환되는가
- 반복구매와 장바구니 깊이가 함께 살아 있는가

즉 BII는 매출 총액이 아니라 `구조의 상업적 발현 상태`를 본다.

## 기간 체계
- `BII_1d`: 하루 체력
- `BII_7d`: 주간 체력
- `BII_30d`: 월간 체력
- `BII_90d`: 분기 체력
- `BII_365d`: 연간 기준 체력

`BII_365d`는 기준선 역할을 하고, 나머지 기간은 그 기준선 대비 최근 상태를 읽는 데 쓴다.

## Baseline
현재 구현은 단일 브랜드 환경에서 self-baseline을 사용한다.

`baseline_days = max(90, min(365, operating_days))`

이 규칙으로 운영 기간이 짧은 브랜드도 계산 가능하게 하고, 너무 짧은 기간 노이즈는 줄인다.

## 입력
구현 노트북 기준 입력은 아래와 같다.

- `brand_score.csv`
- `pgm_scored.csv`
- `pgm_basket_gravity.csv`
- `pgm_product_demand_gravity.csv`
- `order_product_events.csv`
- `core.silver_meta_order`의 결제 금액

## 구현 규칙
- 구현 위치: `03_PGM_BrandHealthImpact.ipynb`
- 범위: `candidate`
- 윈도우: trailing fixed windows `1/7/30/90/365d`
- `BII`는 같은 노트북에서 방금 계산한 `brand_score.csv`의 `BHI`를 입력으로 사용한다.
- `pgm_product_demand_gravity.csv`와 `pgm_basket_gravity.csv`가 없으면 명시적으로 실패해야 한다.

## 해석 가이드
- `BHI` 높고 `BII`도 높음: 구조와 최근 상업 체력이 함께 양호
- `BHI` 높고 `BII` 낮음: 구조는 좋지만 최근 발현 약화
- `BHI` 낮고 `BII` 높음: 단기 성과는 있으나 구조 리스크 존재
- `BHI` 낮고 `BII` 낮음: 구조와 운영 모두 취약

추세 해석은 아래 비율을 함께 본다.

- `bii_90_over_365`
- `bii_30_over_365`
- `bii_7_over_365`

## 주의
- BII는 매출을 직접 대체하지 않는다.
- BII는 5축 구조를 전제로 한다.
- BII는 Brand Score 없이 단독 해석하면 안 된다.
