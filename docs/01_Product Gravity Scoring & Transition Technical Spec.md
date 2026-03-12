# Product Gravity Scoring & Transition Technical Spec

Version: PGM v1.1  
Primary Artifacts:
- `01_PGM_ProductGravity.ipynb`
- `02_PGM_ConvergenceReturnGravity.ipynb`
- `03_PGM_BrandHealthImpact.ipynb`

## 1. 목적

PGM(Product Gravity Model)은 단순 판매량 분석이 아니라
상품 중심의 수요 구조를 설명하는 모델이다.

PGM core는 다음 4개 gravity로 구성된다.

- `Entry Gravity`
- `Expansion Gravity`
- `Convergence Gravity`
- `Return Gravity`

PGM shorthand:

`Acquire -> Expand -> Gather -> Loop`

추가로 `Basket Gravity (CA)`는 core가 아니라
구매 순간의 동시구매 구조를 설명하는 extension module이다.

## 2. 구현 경계

현재 구현은 두 레이어로 분리한다.

### 2.1 메인 노트북

`01_PGM_ProductGravity.ipynb`

이 노트북은 다음을 계산한다.

- `Entry Gravity`
- `Expansion Gravity`
- `AA -> PCA Transition`
- `Basket Gravity`

중요 원칙:

`01_PGM_ProductGravity.ipynb`의 계산 로직은 유지한다.

즉:

- `Entry Gravity` 공식 점수는 기존 구현을 그대로 사용한다.
- `Expansion Gravity` 공식 점수는 기존 구현을 그대로 사용한다.
- `Basket Gravity` 계산도 그대로 유지한다.

### 2.2 브랜드 노트북

`03_PGM_BrandHealthImpact.ipynb`

이 노트북은 메인/후속 노트북 산출물인
`pgm_scored.csv`, `pgm_basket_gravity.csv`, `pgm_product_demand_gravity.csv`, `order_product_events.csv`를 입력으로 받아
다음을 계산한다.

- `Brand Score` (`BHI`, `Confidence_Index`)
- `BII`

### 2.3 후속 노트북

`02_PGM_ConvergenceReturnGravity.ipynb`

이 노트북은 메인 노트북 산출물인
`pgm_scored.csv`와 `order_product_events.csv`를 입력으로 받아
다음을 계산한다.

- `Convergence Gravity`
- `Return Gravity`
- `pgm_product_demand_gravity.csv`

권장 실행 순서:

1. `01_PGM_ProductGravity.ipynb`
2. `02_PGM_ConvergenceReturnGravity.ipynb`
3. `03_PGM_BrandHealthImpact.ipynb`

현재 구현 범위 note:

- `Convergence Gravity`, `Return Gravity`는 현재 `pgm_scored.csv`에 export된 scored product universe를 기준으로 계산한다.
- 전체 catalog 기준 확장은 future extension으로 둔다.

## 3. 공통 데이터 계약

핵심 입력은 다음 이벤트 구조다.

- `member_id`
- `order_id`
- `order_at`
- `product_id`

분석 전제:

- 기본 관측 창은 `90일`
- 고객별 상품 첫 구매 시점을 상품별 cohort의 기준점으로 둔다
- 같은 주문의 동일 상품 재등장은 1회 이벤트로 간주한다
- optional `member_group_id` static filter를 지원한다
- config:
- `USE_MEMBER_GROUP_FILTER`
- `MEMBER_GROUP_FILTER_MODE` (`exclude` / `include`)
- `FILTER_MEMBER_GROUP_IDS`
- 필터 기준은 `core.silver_meta_member` + `core.silver_meta_member_group` 조인으로 해석한다
- `exclude` 모드에서는 `member_id`가 없거나 `member_group_id` 매핑이 없는 주문을 유지한다
- `include` 모드에서는 지정된 `member_group_id`에 명시적으로 매핑된 주문만 남기고 unmatched 주문은 제외한다

## 4. Entry Gravity

### 공식 정의

상품이 고객 수요를 시작시키는 힘.
특히 첫 구매 진입점으로 작동하는 정도.

### 공식 질문

“이 상품은 얼마나 강하게 신규 수요를 여는가?”

### 현재 구현

메인 노트북에서 공식 점수는 아래 산식을 사용한다.

- `AA_ScoreBase = 0.4 * fcr_norm + 0.6 * rev_norm`
- `volume_weight = minmax(log1p(first_customer_cnt))`
- `AA_Score = volume_weight * AA_ScoreBase`

Canonical alias:

- `AA_Score` ↔ `Entry_Gravity_Score`

## 5. Expansion Gravity

### 공식 정의

상품 구매 이후 다음 구매가 발생하도록 수요를 확장시키는 힘.

### 공식 질문

“이 상품은 얼마나 강하게 다음 구매를 일으키는가?”

### 현재 구현

메인 노트북에서 공식 점수는 아래 산식을 사용한다.

- `PCA_ScoreBase = 0.6 * r90_norm + 0.4 * x2_norm`
- `volume_weight = minmax(log1p(first_customer_cnt))`
- `PCA_Score = volume_weight * PCA_ScoreBase`

Canonical alias:

- `PCA_Score` ↔ `Expansion_Gravity_Score`

## 6. Convergence Gravity

### 공식 정의

많은 서로 다른 prior product로부터 수요가 이 상품으로 모여드는 정도.

핵심 패턴:

- `B -> A`
- `C -> A`
- `D -> A`

### 공식 질문

“고객은 여러 경로 끝에 이 상품으로 모이는가?”

### naive metric과의 차이

Convergence Gravity는 단순 인기 상품 점수가 아니다.

- 판매량이 높아도 incoming source가 적으면 convergence는 낮을 수 있다.
- 판매량이 중간이어도 다양한 source에서 도착하면 convergence는 높을 수 있다.

### 계산 원리

- 각 상품의 고객별 첫 구매를 `source cohort`로 정의
- cohort 이후 `90일` 내 첫 next-product를 edge로 생성
- target 상품 기준으로 incoming edge를 집계
- 단, core `Convergence Gravity` score는 `source_product_id != target_product_id`인 non-self incoming edge만 사용한다.
- `A -> A` self-loop는 버리지 않고 diagnostic metric으로 별도 저장한다.

### 핵심 raw metrics

- `converged_customer_cnt_90d`
- `distinct_source_product_cnt_90d`
- `incoming_transition_rate_sum_90d`
- `top1_source_share` (보조 지표)
- `self_loop_transition_customer_cnt_90d` (diagnostic)
- `self_loop_transition_rate_90d` (diagnostic)

### 공식 score

- `Convergence_Gravity_ScoreBase = 0.5*norm(incoming_transition_rate_sum_90d) + 0.3*norm(distinct_source_product_cnt_90d) + 0.2*norm(converged_customer_cnt_90d)`
- `volume_weight = minmax(log1p(total_customer_cnt_for_product))`
- `Convergence_Gravity_Score = volume_weight * Convergence_Gravity_ScoreBase`

## 7. Return Gravity

### 공식 정의

고객이 해당 상품을 떠난 뒤,
중간 상품을 거쳐 다시 그 상품으로 돌아오는 정도.

핵심 패턴:

- `A -> B -> A`
- `A -> B -> C -> A`

### 공식 질문

“이 상품은 중간 구매 뒤 다시 고객을 끌어당기는가?”

### naive metric과의 차이

Return Gravity는 단순 재구매율이 아니다.

- `A -> A`는 simple repeat다.
- `A -> B -> A`는 return loop다.

즉:

- repeat = 같은 상품을 다시 샀는가
- return = 다른 상품을 거친 뒤 다시 돌아왔는가

### 계산 원리

- 각 상품의 고객별 첫 구매를 source cohort로 정의
- cohort 이후 `90일` 내 상품 시퀀스를 본다
- `A`가 다시 등장하되 그 사이에 `A`가 아닌 상품이 1개 이상 있으면 qualified return으로 본다

### 핵심 raw metrics

- `return_customer_rate_90d`
- `return_loop_rate_90d`
- `return_path_diversity_90d`
- `simple_repeat_rate_90d` (비교 지표)

### 공식 score

- `Return_Gravity_ScoreBase = 0.5*norm(return_customer_rate_90d) + 0.3*norm(return_loop_rate_90d) + 0.2*norm(log1p(return_path_diversity_90d))`
- `volume_weight = minmax(log1p(total_customer_cnt_for_product))`
- `Return_Gravity_Score = volume_weight * Return_Gravity_ScoreBase`

## 8. Convergence와 Return의 구분

두 개념은 엄격히 분리한다.

### Convergence

많은 source가 하나의 target로 모인다.

예:

- `B -> A`
- `C -> A`
- `D -> A`

### Return

같은 상품을 떠났다가 다시 돌아온다.

예:

- `A -> B -> A`
- `A -> B -> C -> A`

정리:

- `Convergence = many sources -> one destination`
- `Return = leave A -> come back to A`

## 9. 전이 및 진단 산출물

`02_PGM_ConvergenceReturnGravity.ipynb`는 다음 공식 산출물을 생성한다.

- `pgm_product_transition_edge.csv`
- `pgm_convergence_gravity_product.csv`
- `pgm_return_gravity_product.csv`
- `pgm_return_gravity_loop_detail.csv`
- `pgm_product_demand_gravity.csv`

여기서 `pgm_product_transition_edge.csv`는
과거 `Post-Expansion Chain` 역할을 general product transition edge로 확장한 진단 뷰다.
공식 gravity 이름은 `Convergence Gravity`이며,
`Post-Expansion Chain`은 공식 축이 아니다.

## 10. Basket Gravity의 위치

`Basket Gravity (CA)`는 PGM core 4 gravities가 아니다.

CA는 다음을 설명하는 확장 모듈이다.

- 동일 주문 내 동시구매 구조
- 장바구니 결합력
- 구매 순간의 공간적 구조

즉:

- core 4 gravities = demand acquisition / expansion / convergence / return
- basket = same-order structure extension

## 11. 공식 제품표

`pgm_product_demand_gravity.csv`는 PGM 4 gravity의 공식 제품표다.

최소 컬럼:

- `product_id`
- `Entry_Gravity_Score`
- `Expansion_Gravity_Score`
- `Convergence_Gravity_Score`
- `Return_Gravity_Score`
- `Entry_Gravity_Primary_Type`
- `Expansion_Gravity_Primary_Type`
- `distinct_source_product_cnt_90d`
- `incoming_transition_rate_sum_90d`
- `self_loop_transition_customer_cnt_90d` (diagnostic)
- `self_loop_transition_rate_90d` (diagnostic)
- `return_customer_rate_90d`
- `return_loop_rate_90d`
- `simple_repeat_rate_90d`

## 12. 해석 원칙

PGM은 다음을 설명한다.

- `product -> demand acquisition`
- `product -> demand expansion`
- `product -> demand convergence`
- `product -> demand return`

PGM은 다음을 직접 점수화하지 않는다.

- 절대 판매량
- bestseller ranking
- 광고비 효율
- 원가 구조

즉 PGM은:

`product -> sales`

가 아니라

`product -> demand structure`

를 설명한다.
