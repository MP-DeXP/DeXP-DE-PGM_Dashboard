# PGM Dashboard Data Schema

`app.js`에서 사용되는 CSV 스키마 정의입니다.

## 공통 원칙
- 업로드 매칭은 **파일명 키 포함 여부**로 판단합니다.
- 숫자 컬럼은 가능한 한 numeric 타입으로 제공하세요.
- 날짜 컬럼은 `YYYY-MM-DD` 또는 파싱 가능한 ISO 형식을 권장합니다.

## 1) `brand_score` (기존)
용도: Overview, Insight Studio의 BHI/구조축 표시

필수 컬럼:
- BHI 계열 중 1개 이상
  - `Brand_Health_Index`
  - `BHI`
  - `brand_health_index`
  - `Brand_Health_Score`
- `AA_Concentration_Index`
- `Chain_Balance_Index`
- `Confidence_Index`

## 2) `anchor_scored` (기존)
용도: Products, 제품명 매핑, 일부 액션 규칙 계산

필수 컬럼:
- 제품 ID 계열 중 1개 이상
  - `product_id`
  - `Product_ID`
  - `\ufeffproduct_id`
- 제품명 계열 중 1개 이상
  - `product_name_latest`
  - `Product_Name`
  - `product_name`
- `revenue_90d`
- `first_customer_cnt`
- `AA_Score`
- `AA_Primary_Type`
- `PCA_Score`
- `PCA_Primary_Type`

## 3) `anchor_transition` (기존)
용도: 기존 Transitions 페이지

필수 컬럼:
- `aa_product_id`
- `pca_product_id`
- `transition_customer_cnt`
- `avg_days_to_pca`
- `transition_rate`

## 4) `cart_anchor` (기존)
용도: 기존 Cart 페이지 상단 차트

필수 컬럼:
- `product_id`
- `median_cart_size`

## 5) `cart_anchor_detail` (기존)
용도: 기존 Cart 상세 테이블, 연관제품 모달

필수 컬럼:
- `i`
- `j`
- `co_order_cnt`

참고:
- 로딩 시 `String(i) < String(j)` 조건으로 중복 페어를 제거합니다.

---

## 6) `aa_cohort_journey` (신규)
용도: Insight Studio - Entry Gravity Cohort Journey

필수 컬럼:
- `cohort_date`
- `aa_product_id`
- `aa_type`
- `cohort_customers`
- `repeat_7d_rate`
- `repeat_30d_rate`
- `repeat_90d_rate`
- `pca_transition_30d_rate`
- `pca_transition_90d_rate`
- `avg_days_to_pca`
- `avg_revenue_90d`

## 7) `aa_transition_path` (신규)
용도: Insight Studio - Entry Gravity → Expansion Gravity 전이 분석

필수 컬럼:
- `cohort_date`
- `aa_product_id`
- `aa_type`
- `pca_product_id`
- `transition_customers`
- `transition_rate`
- `avg_days_to_pca`

## 8) `ca_profile` (신규)
용도: Insight Studio - Basket Gravity Insight

필수 컬럼:
- `product_id`
- `ca_type` (`Core` / `Pair` / `Set` / `None` 권장)
- `attach_rate`
- `median_cart_size`
- `breadth_lift`
- `companion_count`
- `top1_share`
- `top3_share`
- `top1_companion_product_id`

## 9) `bii_window` (신규)
용도: Insight Studio - Brand Fitness(BII 다기간)

필수 컬럼:
- `as_of_date`
- `window_days` (7, 30, 90, 365)
- `bii`
- `bhi`
- `clv_norm`
- `customer_strength_norm`
- `stage`
- `baseline_days`
- `confidence`

## 10) `apf_action_rules` (신규, 선택)
용도: Action Center 사용자 규칙(내장 규칙 보완)

필수 컬럼:
- `rule_id`
- `domain` (`marketing` / `md`)
- `condition_expr`
- `priority` (1~3 권장)
- `title_ko`
- `action_ko`
- `impact_ko`

`condition_expr` 규칙:
- 비어 있으면 항상 표시
- 지원 예시: `aa_broad_ratio > 0.5 && pca_transition_90d_rate < 0.25`

지원 metric key:
- `aa_broad_ratio`
- `pca_transition_90d_rate`
- `avg_days_to_pca`
- `transition_top3_share`
- `ca_pair_top1_share_max`
- `ca_set_breadth_lift_avg`
- `pca_scale_concentration`

---

## 11) `brand_impact_timeseries` (신규, 선택)
용도: 브랜드 구조 페이지 - 최근 12주 구조 추이

권장 파일명:
- `brand_impact_timeseries.csv`
- `_insight_brand_impact_timeseries.csv`

필수 컬럼:
- `as_of_date`
- `window_days`
- `bhi`
- `bii` 또는 `bii_t`

권장 컬럼:
- `period_start`
- `period_end`
- `clv_norm` 또는 `clv_t_norm`
- `customer_strength_norm` 또는 `customer_strength_t_norm`
- `repeat_rate_t`
- `attach_rate_t`
- `depth_t`
- `stage`
- `baseline_days`
- `confidence` 또는 `confidence_index`

비고:
- 업로드 시 `bii_t`, `clv_t_norm`, `customer_strength_t_norm`도 자동 호환합니다.
- 이 파일이 없으면 브랜드 구조 페이지의 상단 delta와 12주 추이는 빈 상태로 표시됩니다.

---

## 12) `brand_impact_daily_pulse` (신규, 선택)
용도: 브랜드 구조 페이지 - 일별 실제 구매 흐름(Daily Pulse)

권장 파일명:
- `brand_impact_daily_pulse.csv`
- `_insight_brand_impact_daily_pulse.csv`

필수 컬럼:
- `as_of_date`
- `daily_bii_pulse`

권장 컬럼:
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
- `stage`
- `scope`

비고:
- `Daily Pulse`는 날짜당 1행의 단일값을 전제로 합니다.
- 이 파일이 없으면 브랜드 구조 페이지의 메인 일별 흐름 차트는 빈 상태로 표시되고, 보조 `brand_impact_timeseries` 차트는 계속 렌더링됩니다.

## 13) `purchase_activation_driver_timeseries` (신규, 선택)
용도: 브랜드 구조 페이지 - Hero + Brand Purchase Drivers

권장 파일명:
- `purchase_activation_driver_timeseries.csv`
- `_insight_purchase_activation_driver_timeseries.csv`

호환 alias:
- `brand_purchase_driver_timeseries.csv`
- `_insight_brand_purchase_driver_timeseries.csv`

필수 컬럼:
- `as_of_date`
- `window_days`
- `bii_t`
- `bii_365`
- `momentum_t`
- `active_customers_t`
- `repeat_rate_t`
- `attach_rate_t`
- `avg_clv_t`
- `customers_contribution`
- `repeat_contribution`
- `attach_contribution`
- `clv_contribution`

권장 컬럼:
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

비고:
- Hero와 Drivers 섹션을 함께 지원하는 단일 source입니다.
- `window_days`는 `7`, `30`, `90`, `365`를 권장합니다.
- `customers_contribution + repeat_contribution + attach_contribution + clv_contribution = momentum_t - 1` 관계를 기대합니다.
- 이 파일이 없으면 브랜드 구조 페이지의 Hero는 축소 버전으로 표시되고, Drivers는 빈 상태 안내만 렌더링됩니다.

## 14) `brand_structure_timeseries` (신규, 선택)
용도: 브랜드 구조 페이지 - 판매 구조 4카드의 `7/30/90일` 기준 스냅샷

권장 파일명:
- `brand_structure_timeseries.csv`
- `_insight_brand_structure_timeseries.csv`

필수 컬럼:
- `as_of_date`
- `window_days`
- `entry_product_ratio`
- `flow_transition_rate`
- `return_customer_rate`
- `basket_items_per_order`
- `ps_static`

권장 컬럼:
- `entry_top_product_share`
- `flow_top_path_share`
- `return_product_demand_share`
- `basket_attach_rate`
- `period_start`
- `period_end`
- `stage`
- `scope`

비고:
- 이 파일은 `PS history`가 아니라 각 날짜 시점의 `windowed structure snapshot`입니다.
- `ps_static`는 같은 날짜의 `7/30/90` row에서 반복되는 static anchor입니다.
- 브랜드 페이지 판매 구조 섹션은 이 파일이 있으면 선택한 `7/30/90일` 기준으로 카드 수치와 dots를 함께 바꿉니다.
- 이 파일이 없으면 현재 구조 기준 fallback 계산을 사용합니다.
- 선택한 기간 row만 없으면 해당 기간에 한해 fallback으로 내려갑니다.

## 15) `brand_revenue_timeseries` (신규, 선택)
용도: 브랜드 구조 페이지 - Revenue vs. PAI 진단 매트릭스

권장 파일명:
- `brand_revenue_timeseries.csv`
- `_insight_brand_revenue_timeseries.csv`

필수 컬럼:
- `as_of_date`
- `window_days`
- `revenue_t`

권장 컬럼:
- `period_start`
- `period_end`
- `currency`
- `scope`

비고:
- `window_days`는 `7`, `30`, `90`, `365`를 권장합니다.
- 같은 날짜의 `selected window / 365일` 비율을 프런트에서 계산합니다.
- 이 파일이 없으면 브랜드 구조 페이지의 `Revenue vs. BII` 매트릭스는 빈 상태로 표시되고, 다른 카드와 차트는 계속 렌더링됩니다.

---

## 15) `product_group_map` (신규, 선택)
용도: 동일 제품군 수동 그룹 매핑/해제(전 페이지 집계 반영)

권장 파일명:
- `pgm_product_group_map.csv`

alias:
- `product_group_map.csv`
- `_meta_product_group_map.csv`

필수 컬럼:
- `product_id`
- `status` (`grouped` | `ungrouped`)
조건부 필수 컬럼 (`status=grouped`일 때):
- `group_id`
- `group_name`

권장 컬럼:
- `rule` (`exact_name` | `normalized_prefix` | `manual`)
- `updated_at` (ISO datetime)

동작 규칙:
- `grouped`: 지정 그룹으로 강제 매핑
- `ungrouped`: 자동 제안에서 제외하고 독립 유지
- 저장 우선순위:
  1. `data/pgm_product_group_map.csv`
  2. IndexedDB 저장값
  3. 자동 제안(동일 제품명 + 접두어 `[ ... ]` 제거 정규화)

---

## 업로드 키 목록 (파일명 매칭 기준)
- `brand_score`
- `anchor_scored`
- `anchor_transition`
- `cart_anchor`
- `cart_anchor_detail`
- `aa_cohort_journey`
- `aa_transition_path`
- `ca_profile`
- `bii_window`
- `brand_impact_timeseries`
- `brand_impact_daily_pulse`
- `brand_revenue_timeseries`
- `apf_action_rules`
- `product_group_map`
