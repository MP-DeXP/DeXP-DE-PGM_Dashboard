# Brand Impact Index (BII) v1.0

## 목적
`BII`는 브랜드의 구조 건강도(`BHI`)가 기간별 실제 구매 전환력으로 얼마나 발현되고 있는지를 측정하는 다기간 지표다.

Brand Score가 브랜드의 구조를 본다면, BII는 그 구조가 최근 운영 구간에서 어떤 수준의 실제 구매 전환력으로 나타나는지를 본다.

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

즉 BII는 매출 총액이 아니라 `구조의 실제 구매 전환 발현 상태`를 본다.

## 기간 체계
- `BII_1d`: 하루 실제 구매 전환력
- `BII_7d`: 주간 실제 구매 전환력
- `BII_30d`: 월간 실제 구매 전환력
- `BII_90d`: 분기 실제 구매 전환력
- `BII_365d`: 연간 기준 실제 구매 전환력

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
- 보조 구현 위치: `03a_PGM_BrandImpactDailyPulse.ipynb`
- 보조 구현 위치: `03b_PGM_BrandRevenueTimeseries.ipynb`
- 범위: `candidate`
- 윈도우: trailing fixed windows `1/7/30/90/365d`
- 기본 시계열: `analysis_end_date` 기준 최근 90일, 일단위
- `BII`는 같은 노트북에서 방금 계산한 `brand_score.csv`의 `BHI`를 입력으로 사용한다.
- `pgm_product_demand_gravity.csv`와 `pgm_basket_gravity.csv`가 없으면 명시적으로 실패해야 한다.

## 출력
- `brand_impact_windows.csv`
  최종 `analysis_end_date` 기준 1회 계산된 window 비교표
- `brand_impact_index.csv`
  최종 `analysis_end_date` 기준 요약 1행
- `brand_impact_timeseries.csv`
  `as_of_date x window_days` long format 시계열
- `brand_impact_daily_pulse.csv`
  날짜당 1행의 단일 pulse 출력
- `brand_revenue_timeseries.csv`
  `as_of_date x window_days` long format rolling revenue 출력

`brand_impact_timeseries.csv`의 주요 컬럼은 아래와 같다.

- `as_of_date`
- `window_key`
- `window_days`
- `period_start`
- `period_end`
- `bhi`
- `confidence_index`
- `baseline_days`
- `active_customers_baseline`
- `avg_clv_baseline`
- `active_customers_t`
- `repeat_rate_t`
- `attach_rate_t`
- `depth_t`
- `avg_clv_t`
- `clv_t_norm`
- `customer_strength_t_norm`
- `bii_t`
- `stage`
- `scope`

`brand_impact_daily_pulse.csv`의 주요 컬럼은 아래와 같다.

- `as_of_date`
- `bhi`
- `confidence_index`
- `baseline_days`
- `active_customers_baseline`
- `avg_clv_baseline`
- `active_customers_t`
- `repeat_rate_t`
- `attach_rate_t`
- `depth_t`
- `avg_clv_t`
- `clv_t_norm`
- `customer_strength_t_norm`
- `daily_bii_pulse`
- `stage`
- `scope`

`brand_revenue_timeseries.csv`의 주요 컬럼은 아래와 같다.

- `as_of_date`
- `window_days`
- `revenue_t`
- `period_start`
- `period_end`
- `currency`
- `scope`

## 시계열 해석 제약
- 현재 구현은 `historical BHI+BII full recompute`가 아니다.
- 시계열 전체에서 `BHI`는 현재 실행에서 계산된 단일 값을 고정으로 사용한다.
- 날짜별로 달라지는 것은 `CLV`, `Active Customers`, `RepeatRate`, `AttachRate`, `Depth`, `BII`다.
- 따라서 이 시계열은 `구조 점수의 역사`가 아니라 `현재 구조를 기준으로 본 실제 구매 전환력의 최근 추이`로 해석해야 한다.

## Daily Pulse 해석
- `brand_impact_daily_pulse.csv`는 window toggle 없이 보여줄 수 있는 날짜당 단일값 출력을 위한 파일이다.
- 각 날짜의 `daily_bii_pulse`는 해당 날짜 기준 `BII_1d`와 같은 의미를 갖는다.
- 따라서 `brand_impact_timeseries.csv`보다 직관적이지만, 다일 window의 구조적 흐름 비교 정보는 포함하지 않는다.

## Revenue Timeseries 해석
- `brand_revenue_timeseries.csv`는 같은 날짜에 대해 `7/30/90/365` rolling revenue를 함께 제공한다.
- 이 파일은 `BII` 자체가 아니라, 프런트에서 `Revenue / Revenue_365` 같은 보조 비율을 계산하기 위한 reference output이다.
- 각 행은 `특정 날짜 시점의 최근 N일 매출 합계`를 뜻한다.

## 해석 가이드
- `BHI` 높고 `BII`도 높음: 구조와 최근 실제 구매 전환력이 함께 양호
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
