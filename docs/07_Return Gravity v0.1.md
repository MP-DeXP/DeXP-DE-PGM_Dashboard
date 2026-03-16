# Return Gravity v0.1

Return Gravity v0.1  
― 고객이 떠난 뒤 다시 돌아오게 만드는 loop force ―

## Abstract

Return Gravity는 상품이 고객을 다시 자기 자신으로 끌어당기는 힘을 설명한다.
이 개념은 단순 재구매율을 다시 부르는 이름이 아니다.

핵심은 다음이다.

- `A -> A` : simple repeat
- `A -> B -> A` : return loop
- `A -> B -> C -> A` : extended return loop

즉 Return Gravity는
고객이 다른 상품을 거친 뒤 다시 같은 상품으로 돌아오는 구조를 포착한다.

PGM shorthand에서 Return Gravity는 `Loop`에 해당한다.

## 1. 문제의식

많은 상품은 반복 구매가 일어난다.
하지만 반복 구매는 구조적으로 매우 다를 수 있다.

- 같은 상품만 연속 구매하는 경우
- 다른 상품을 탐색한 뒤 다시 특정 상품으로 복귀하는 경우

실무적으로 중요한 것은 두 번째다.
왜냐하면 그 상품은 단순히 “자주 사는 상품”이 아니라,
수요 사이클의 중심이기 때문이다.

## 2. 공식 정의

Return Gravity는 다음과 같이 정의한다.

특정 상품 `A`의 고객별 첫 구매 이후 90일 내 시퀀스에서,
`A`가 다시 등장하되 그 사이에 `A`가 아닌 다른 상품이 1개 이상 존재하면
그 재도달 사건을 `qualified return`으로 본다.

예:

- `A -> B -> A`
- `A -> B -> C -> A`

제외:

- `A -> A`
- `A -> B`

## 3. Return Gravity와 단순 재구매율의 차이

`Return Gravity는 재구매율이 아니다.`

재구매율은 같은 상품이 다시 팔렸는지만 본다.
Return Gravity는 그 사이에 다른 상품 전이가 있었는지를 본다.

정리:

- repeat = same product again
- return = leave and come back

## 4. Return Gravity와 Convergence Gravity의 차이

둘은 다르다.

### Convergence

여러 source가 하나의 target로 모인다.

- `B -> A`
- `C -> A`
- `D -> A`

### Return

같은 상품을 떠났다가 다시 돌아온다.

- `A -> B -> A`
- `A -> B -> C -> A`

정리:

- `Convergence = gathering demand from many sources`
- `Return = pulling demand back into the same product`

## 5. 측정 원리

기본 입력:

- `member_id`
- `order_id`
- `order_at`
- `product_id`

기본 규칙:

- 기준점은 고객별 상품 첫 구매
- 관측 창은 기본 `90일`
- 같은 주문은 후속 구매로 보지 않음

## 6. 핵심 raw metrics

- `return_customer_rate_90d`
  qualified return을 1회 이상 만든 고객 비율
- `return_loop_rate_90d`
  고객 수 대비 qualified return loop 수
- `return_path_diversity_90d`
  관찰된 서로 다른 intermediate path 수
- `simple_repeat_rate_90d`
  `A -> A` 비교 지표

## 7. 공식 score

공식 score는 아래 산식을 사용한다.

`Return_Gravity_ScoreBase = 0.5*norm(return_customer_rate_90d) + 0.3*norm(return_loop_rate_90d) + 0.2*norm(log1p(return_path_diversity_90d))`

`volume_weight = minmax(log1p(total_customer_cnt_for_product))`

`Return_Gravity_Score = volume_weight * Return_Gravity_ScoreBase`

## 8. 해석

Return Gravity가 높은 상품은 다음과 같이 해석할 수 있다.

- `Anchor Product`
- `Loop-Center Product`
- `Routine-Return Product`

이 상품은 고객이 여러 상품을 거친 뒤에도 다시 돌아오는 중심점이다.

## 9. naive metric과의 차이

Return Gravity는 다음과 동일하지 않다.

- sales volume
- bestseller rank
- repeat purchase rate

즉 낮은 매출 상품도 높은 Return Gravity를 가질 수 있다.

## 10. 시각화 아이디어

- `Convergence vs Return` scatter
- return loop path bar
- `simple_repeat vs qualified_return` 분해 chart
- loop sankey

## 11. 구현 연결

구현은 `02_PGM_ConvergenceReturnGravity.ipynb`에서 수행한다.

주요 산출물:

- `pgm_return_gravity_product.csv`
- `pgm_return_gravity_loop_detail.csv`
- `pgm_product_demand_gravity.csv`
