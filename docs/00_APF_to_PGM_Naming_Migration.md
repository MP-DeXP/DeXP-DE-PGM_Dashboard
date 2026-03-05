# APF -> PGM Naming Migration

## 목적
이 문서는 기존 `APF (Anchor Product Framework)` 네이밍을 `PGM (Product Gravity Model)`로 전환한 내용을 처음 보는 사람도 빠르게 이해할 수 있도록 정리한다.

## 변경 요약
- Framework 명칭:
- `Anchor Product Framework (APF)` -> `Product Gravity Model (PGM)`
- 표시 축 명칭:
- `AA` -> `Entry Gravity`
- `PCA` -> `Expansion Gravity`
- `CA` -> `Basket Gravity`

## 호환성 정책
- 기술 식별자(`AA/PCA/CA`)는 기존 이름을 유지한다.
- 외부 표시명/문서/신규 출력 파일명은 `PGM` 기준을 사용한다.
- Legacy alias는 1개 릴리즈 동안 병행 유지한다.

## 파일/아티팩트 명칭 매핑
- `anchor_scored.csv` -> `pgm_scored.csv`
- `anchor_transition.csv` -> `pgm_entry_to_expansion_transition.csv`
- `cart_anchor.csv` -> `pgm_basket_gravity.csv`
- `cart_anchor_detail.csv` -> `pgm_basket_gravity_detail.csv`
- `_insight_aa_cohort_journey.csv` -> `_insight_entry_cohort_journey.csv`
- `_insight_aa_transition_path.csv` -> `_insight_entry_transition_path.csv`
- `_insight_ca_profile.csv` -> `_insight_basket_gravity_profile.csv`
- `_insight_apf_action_rules.csv` -> `_insight_pgm_action_rules.csv`

## 스키마 Alias 매핑
- `AA_Score` <-> `Entry_Gravity_Score`
- `PCA_Score` <-> `Expansion_Gravity_Score`
- `AA_Primary_Type` <-> `Entry_Gravity_Primary_Type`
- `PCA_Primary_Type` <-> `Expansion_Gravity_Primary_Type`
- `CA_Primary_Type` <-> `Basket_Gravity_Primary_Type`
- `aa_product_id` <-> `entry_product_id`
- `pca_product_id` <-> `expansion_product_id`
- `avg_days_to_pca` <-> `avg_days_to_expansion`

## 적용 기준일
- 2026-03-03

## 운영 가이드
- 신규 연동은 canonical(`pgm_*`, `Entry/Expansion/Basket Gravity`) 기준으로 작성한다.
- 기존 연동은 alias 제거 전까지 동작하지만, 점진적으로 canonical로 이전한다.
