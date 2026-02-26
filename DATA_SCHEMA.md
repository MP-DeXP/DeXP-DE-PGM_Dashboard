# APF Dashboard Data Schema

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
용도: Products, 상품명 매핑, 일부 액션 규칙 계산

필수 컬럼:
- 상품 ID 계열 중 1개 이상
  - `product_id`
  - `Product_ID`
  - `\ufeffproduct_id`
- 상품명 계열 중 1개 이상
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
용도: 기존 Cart 상세 테이블, 연관상품 모달

필수 컬럼:
- `i`
- `j`
- `co_order_cnt`

참고:
- 로딩 시 `String(i) < String(j)` 조건으로 중복 페어를 제거합니다.

---

## 6) `aa_cohort_journey` (신규)
용도: Insight Studio - AA Cohort Journey

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
용도: Insight Studio - AA → PCA 전이 분석

필수 컬럼:
- `cohort_date`
- `aa_product_id`
- `aa_type`
- `pca_product_id`
- `transition_customers`
- `transition_rate`
- `avg_days_to_pca`

## 8) `ca_profile` (신규)
용도: Insight Studio - Cart/CA Insight

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
- `apf_action_rules`

