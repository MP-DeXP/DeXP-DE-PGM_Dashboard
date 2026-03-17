# PAI (Purchase Activation Index) v1.0

## 목적
`PAI`는 브랜드의 구매 구조(`PS`)가 기간별 구매 활성도로 얼마나 발현되고 있는지를 측정하는 다기간 지표다.

Brand Score가 브랜드의 구조를 본다면, PAI는 그 구조가 최근 운영 구간에서 어떤 수준의 구매 활성도로 나타나는지를 본다.

## 핵심 정의
각 기간 `t ∈ {1, 7, 30, 90, 365}` 에 대해:

`PAI_t = PS × CLV_t_norm × Customer_Strength_t_norm`

여기서:

- `PS` (`brand_score.csv` legacy column: `BHI`): `brand_score.csv`의 5축 기반 Brand Health Index
- `CLV_t_norm = sqrt(Avg_CLV_t / Avg_CLV_baseline)`
- `Customer_Strength_t_norm = sqrt((Active_Customers_t × Depth_t) / Active_Customers_baseline)`
- `Depth_t = 0.7 × RepeatRate_t + 0.3 × AttachRate_t`

현재 구현에서 `RepeatRate_t`와 `AttachRate_t`의 business definition은 아래와 같다.

- `active_customers_t`: 선택 기간 안에서 1회 이상 주문한 distinct customer 수
- `repeat_customers_t`: 선택 기간 안에서 distinct order가 2회 이상인 customer 수
- `repeat_rate_t = repeat_customers_t / active_customers_t`
- `attach_orders_t`: 선택 기간 안에서 cart size가 2 이상인 order 수
- `total_orders_t`: 선택 기간 안 전체 distinct order 수
- `attach_rate_t = attach_orders_t / total_orders_t`

즉 `repeat_rate_t`는 첫 구매 cohort 기준 재구매율이 아니라, 선택 기간 안의 활성 고객 중 같은 기간에 2회 이상 주문한 고객 비율이다.

## PAI가 보는 것
- 구조 건강도는 이미 충분한가
- 그 구조가 최근 기간에 실제 고객 가치로 전환되는가
- 반복구매와 장바구니 깊이가 함께 살아 있는가

즉 PAI는 매출 총액이 아니라 `구조의 구매 활성도 발현 상태`를 본다.

## 기간 체계
- `PAI_1d`: 하루 구매 활성도
- `PAI_7d`: 주간 구매 활성도
- `PAI_30d`: 월간 구매 활성도
- `PAI_90d`: 분기 구매 활성도
- `PAI_365d`: 연간 기준 구매 활성도

`PAI_365d`는 기준선 역할을 하고, 나머지 기간은 그 기준선 대비 최근 상태를 읽는 데 쓴다.

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
- 보조 구현 위치: `03c_PGM_BrandPurchaseDrivers.ipynb`
- 범위: `candidate`
- 윈도우: trailing fixed windows `1/7/30/90/365d`
- 기본 시계열: `analysis_end_date` 기준 최근 90일, 일단위
- `PAI`는 같은 노트북에서 방금 계산한 `brand_score.csv`의 `PS`를 입력으로 사용한다. 현재 구현에서는 legacy `BHI` 컬럼을 읽는다.
- `pgm_product_demand_gravity.csv`와 `pgm_basket_gravity.csv`가 없으면 명시적으로 실패해야 한다.

## 출력
- `purchase_activation_windows.csv`
  최종 `analysis_end_date` 기준 1회 계산된 window 비교표
- `purchase_activation_index.csv`
  최종 `analysis_end_date` 기준 요약 1행
- `purchase_activation_timeseries.csv`
  `as_of_date x window_days` long format 시계열
- `purchase_activation_daily_pulse.csv`
  날짜당 1행의 단일 pulse 출력
- `purchase_activation_driver_timeseries.csv`
  Hero / Drivers 섹션용 long format driver + momentum 출력
- `brand_revenue_timeseries.csv`
  `as_of_date x window_days` long format rolling revenue 출력

`purchase_activation_timeseries.csv`의 주요 컬럼은 아래와 같다.

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
- `pai_t`
- `stage`
- `scope`

`repeat_rate_t`는 아래 의미를 갖는다.

- 분모: 선택 기간 안에서 1회 이상 주문한 `active_customers_t`
- 분자: 선택 기간 안에서 distinct order가 2회 이상인 `repeat_customers_t`
- 해석: 선택 기간 안에서 고객이 다시 구매하는 정도를 나타내는 고객 기준 반복구매율

`purchase_activation_daily_pulse.csv`의 주요 컬럼은 아래와 같다.

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
- `daily_pai_pulse`
- `stage`
- `scope`

이 파일의 `repeat_rate_t`도 같은 정의를 따른다.

`brand_revenue_timeseries.csv`의 주요 컬럼은 아래와 같다.

- `as_of_date`
- `window_days`
- `revenue_t`
- `period_start`
- `period_end`
- `currency`
- `scope`

`purchase_activation_driver_timeseries.csv`의 주요 컬럼은 아래와 같다.

- `as_of_date`
- `window_days`
- `pai_t`
- `pai_365`
- `momentum_t`
- `active_customers_t`
- `repeat_rate_t`
- `attach_rate_t`
- `avg_clv_t`
- `customers_contribution`
- `repeat_contribution`
- `attach_contribution`
- `clv_contribution`
- `period_start`
- `period_end`
- `baseline_days`
- `stage`
- `scope`
- `momentum_delta_pct`
- `momentum_state`
- `top_driver_1`
- `top_driver_2`
- `hero_summary`

이 파일의 `repeat_rate_t` 역시 같은 정의를 사용하며, `Depth_t = 0.7 × RepeatRate_t + 0.3 × AttachRate_t`의 `RepeatRate_t`에 그대로 들어간다.

## 시계열 해석 제약
- 현재 구현은 `historical PS+PAI full recompute`가 아니다.
- 시계열 전체에서 `PS`는 현재 실행에서 계산된 단일 값을 고정으로 사용한다. 현재 구현에서는 legacy `BHI` 컬럼을 사용한다.
- 날짜별로 달라지는 것은 `CLV`, `Active Customers`, `RepeatRate`, `AttachRate`, `Depth`, `PAI`다.
- 따라서 이 시계열은 `구조 점수의 역사`가 아니라 `현재 구조를 기준으로 본 구매 활성도의 최근 추이`로 해석해야 한다.

## Daily Pulse 해석
- `purchase_activation_daily_pulse.csv`는 window toggle 없이 보여줄 수 있는 날짜당 단일값 출력을 위한 파일이다.
- 각 날짜의 `daily_pai_pulse`는 해당 날짜 기준 `PAI_1d`와 같은 의미를 갖는다.
- 따라서 `purchase_activation_timeseries.csv`보다 직관적이지만, 다일 window의 구조적 흐름 비교 정보는 포함하지 않는다.

## Revenue Timeseries 해석
- `brand_revenue_timeseries.csv`는 같은 날짜에 대해 `7/30/90/365` rolling revenue를 함께 제공한다.
- 이 파일은 `PAI` 자체가 아니라, 프런트에서 `Revenue / Revenue_365` 같은 보조 비율을 계산하기 위한 reference output이다.
- 각 행은 `특정 날짜 시점의 최근 N일 매출 합계`를 뜻한다.

## Brand Momentum
`Brand Momentum`은 `PAI`의 별도 원천 지표가 아니라, 최근 구매 활성도를 연간 기준선과 비교하는 파생 해석 지표다.

One-liner:

- `PS` (`brand_score.csv` legacy column: `BHI`): 브랜드 구조
- `PAI`: 구매 활성도
- `Momentum`: 구매 활성도의 장기 기준 대비 상대 강도

정의:

`Momentum_t = PAI_t / PAI_365`

여기서:

- `PAI_t`: 최근 기간 구매 활성도
- `PAI_365`: 연간 기준 구매 활성도 baseline

대표 예시는 아래와 같다.

- `Momentum_7 = PAI_7 / PAI_365`
- `Momentum_30 = PAI_30 / PAI_365`
- `Momentum_90 = PAI_90 / PAI_365`

해석:

- `Momentum > 1.0`: 최근 구매 활성도가 연간 기준보다 강하다
- `Momentum = 1.0`: 최근 구매 활성도가 연간 기준과 유사하다
- `Momentum < 1.0`: 최근 구매 활성도가 연간 기준보다 약하다

이 값은 보통 아래 질문에 답하는 데 사용한다.

- 최근 구매 활성도가 장기 기준 대비 강화되고 있는가
- 최근 운영이 구매 활성도를 받쳐주고 있는가
- 단기 프로모션이 구매 활성도를 일시적으로 밀어올렸는가
- 구조 대비 최근 구매 활성도가 약화되고 있는가

주의:

- `Momentum`은 직전 시점 대비 증감률이 아니라 `PAI_365` 대비 상대 강도다.
- 따라서 `Momentum > 1.0`은 최근 구매 활성도가 장기 기준보다 강하다는 뜻이지, 반드시 어제보다 상승했다는 뜻은 아니다.
- `Momentum`은 별도 CSV 산출물이 아니라 `purchase_activation_windows.csv`, `purchase_activation_timeseries.csv`의 `PAI_t`와 `PAI_365`로 계산하는 해석 레이어다.

## Brand Purchase Drivers
`purchase_activation_driver_timeseries.csv`는 Hero와 Drivers 섹션을 동시에 지원하는 단일 source다.

정의:

- `window_days in {7,30,90,365}`
- `Momentum_t = PAI_t / PAI_365`
- contribution은 `Momentum_t - 1`을 설명하는 driver decomposition이다

Driver는 아래 4개로 고정한다.

- `active_customers`
- `repeat_rate`
- `attach_rate`
- `avg_clv`

기여도 계산은 heuristic이 아니라 같은 `as_of_date`의 `365d` row를 baseline으로 하는 Shapley decomposition을 사용한다.

평가 함수:

- `depth = clamp(0.7 * repeat_rate + 0.3 * attach_rate, 0, 1)`
- `clv_norm = sqrt(avg_clv / avg_clv_baseline)`
- `customer_strength_norm = sqrt((active_customers * depth) / active_customers_baseline)`
- `pai_cf = bhi * clv_norm * customer_strength_norm`
- `momentum_cf = pai_cf / pai_365`

따라서 각 row에서 아래가 성립해야 한다.

- `customers_contribution + repeat_contribution + attach_contribution + clv_contribution = momentum_t - 1`

Hero 파생 컬럼은 아래 목적을 가진다.

- `momentum_state`: `강화 중 / 안정 / 약화 중 / 회복 중`
- `top_driver_1`, `top_driver_2`: Hero의 주요 원인 2개
- `hero_summary`: 상태를 한 줄로 설명하는 문장

## 해석 가이드
- `PS` (`brand_score.csv` legacy column: `BHI`) 높고 `PAI`도 높음: 구조와 최근 구매 활성도가 함께 양호
- `PS` (`brand_score.csv` legacy column: `BHI`) 높고 `PAI` 낮음: 구조는 좋지만 최근 발현 약화
- `PS` (`brand_score.csv` legacy column: `BHI`) 낮고 `PAI` 높음: 단기 성과는 있으나 구조 리스크 존재
- `PS` (`brand_score.csv` legacy column: `BHI`) 낮고 `PAI` 낮음: 구조와 운영 모두 취약

추세 해석은 아래 비율을 함께 본다.

- `pai_90_over_365`
- `pai_30_over_365`
- `pai_7_over_365`

이 비율들은 UI에서 `Brand Momentum`으로 명명해 사용할 수 있다.

## 주의
- PAI는 매출을 직접 대체하지 않는다.
- PAI는 5축 구조를 전제로 한다.
- PAI는 Brand Score 없이 단독 해석하면 안 된다.


## Migration Note
- Phase 1: 문서/카피를 `PS`로 전환하고, 구현 컬럼 `BHI`는 유지한다.
- Phase 2: `brand_score.csv`에 `PS` alias 컬럼을 추가하고 `BHI`와 병행 유지한다.
- Phase 3: 외부 소비자 전환 후 `BHI` 제거를 검토한다.
- Current implementation note: PS is currently stored in the BHI column of brand_score.csv.
