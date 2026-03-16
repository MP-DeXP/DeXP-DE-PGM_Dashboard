# Convergence Gravity v0.1

Convergence Gravity v0.1  
― 여러 수요 경로가 한 상품으로 모이는 gather force ―

## Abstract

Convergence Gravity는 수요가 여러 경로를 거쳐 하나의 상품으로 모이는 정도를 설명한다.
이 개념은 단순 인기 상품 점수나 판매량 랭킹과 다르다.

핵심 패턴은 다음과 같다.

- `B -> A`
- `C -> A`
- `D -> A`

즉 Convergence Gravity는
“많은 source가 결국 이 상품으로 모이는가?”
를 묻는 지표다.

PGM shorthand에서 Convergence Gravity는 `Gather`에 해당한다.

## 1. 문제의식

어떤 상품은 직접 유입을 만들지 않아도,
여러 탐색 경로 끝에서 공통 도착점이 된다.

이 상품은 다음과 같은 의미를 가질 수 있다.

- signature product
- demand hub
- central replenishment product

이 역할은 Entry나 Expansion과는 다르다.

## 2. 공식 정의

Convergence Gravity는
많은 서로 다른 prior product로부터 수요가 이 상품으로 모여드는 정도다.

핵심 예시:

- `B -> A`
- `C -> A`
- `D -> A`

Convergence는 `many sources -> one destination`이다.

## 3. Convergence와 Return의 차이

`Convergence`와 `Return`은 다르다.

### Convergence

- `B -> A`
- `C -> A`
- `D -> A`

### Return

- `A -> B -> A`
- `A -> B -> C -> A`

정리:

- `Convergence = gathering demand from many sources`
- `Return = coming back to the same product after leaving it`

## 4. naive metric과의 차이

Convergence Gravity는 다음과 동일하지 않다.

- 높은 매출
- 높은 재구매율
- bestseller ranking

예를 들어:

- 매출은 높지만 incoming path가 한두 개뿐이면 convergence는 낮을 수 있다
- 매출은 중간이지만 다양한 source에서 도착하면 convergence는 높을 수 있다

## 5. 측정 원리

입력:

- `member_id`
- `order_id`
- `order_at`
- `product_id`

기본 규칙:

- 고객별 상품 첫 구매를 source cohort로 정의
- cohort 이후 90일 내 첫 next-product를 edge로 생성
- target 상품 기준 incoming edge를 집계
- core score에는 `source_product_id != target_product_id`인 non-self incoming edge만 포함한다
- `A -> A` self-loop는 self persistence / repeat 성격이 강하므로 score에서는 제외하고 diagnostic metric으로 따로 기록한다

## 6. 핵심 raw metrics

- `converged_customer_cnt_90d`
- `distinct_source_product_cnt_90d`
- `incoming_transition_rate_sum_90d`
- `top1_source_share`
- `self_loop_transition_customer_cnt_90d`
- `self_loop_transition_rate_90d`

이 중 `top1_source_share`는 한 source에 과도하게 의존하는지 보는 보조 지표다.
`self_loop_*`는 core Convergence score가 아니라 diagnostic metric이다.

## 7. 공식 score

`Convergence_Gravity_ScoreBase = 0.5*norm(incoming_transition_rate_sum_90d) + 0.3*norm(distinct_source_product_cnt_90d) + 0.2*norm(converged_customer_cnt_90d)`

`volume_weight = minmax(log1p(total_customer_cnt_for_product))`

`Convergence_Gravity_Score = volume_weight * Convergence_Gravity_ScoreBase`

## 8. 해석

Convergence Gravity가 높은 상품은 다음과 같이 해석할 수 있다.

- `Demand Hub`
- `Gathering Product`
- `Common Destination Product`

이 상품은 여러 구매 경로가 수렴하는 중심이다.

## 9. 진단 뷰와의 관계

기존 `Post-Expansion Chain`은 공식 gravity 이름이 아니다.
이제 그것은 `Convergence Gravity`를 구현하고 해석하기 위한 diagnostic view로 취급한다.

즉:

- official concept: `Convergence Gravity`
- diagnostic view: `product transition edge`

## 10. 시각화 아이디어

- incoming edge graph
- `Convergence vs Return` scatter
- source diversity bar
- top incoming sources decomposition

## 11. 구현 연결

구현은 `02_PGM_ConvergenceReturnGravity.ipynb`에서 수행한다.

주요 산출물:

- `pgm_product_transition_edge.csv`
- `pgm_convergence_gravity_product.csv`
- `pgm_product_demand_gravity.csv`
