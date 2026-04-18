# Metric Dictionary

## M1. `product_daily_revenue`

- grain: `date + product_id`
- definition: 특정 일자 상품 매출 합계
- source: `mart/product_daily_metrics.csv`
- note: product master 미매핑이어도 집계는 유지합니다.

## M2. `product_daily_order_count`

- grain: `date + product_id`
- definition: 특정 일자 상품이 포함된 주문 수
- source: `mart/product_daily_metrics.csv`
- note: order-level metric을 row-level join 후 재합산하지 않습니다.

## M3. `product_role_state_primary`

- grain: `date + product_id`
- definition: same-date snapshot 기준 상품의 주 역할 상태
- source: `mart/product_role_state_daily.csv`
- note: same-date snapshot이 없으면 blank 유지, latest fallback 금지

## M4. `brand_daily_revenue`

- grain: `date`
- definition: 특정 일자 브랜드 총매출
- source: `mart/brand_operating_status_daily.csv`
- note: product-day fact 합산 기준

## M5. `active_product_count`

- grain: `date`
- definition: 특정 일자 매출 또는 주문이 발생한 상품 수
- source: `mart/brand_operating_status_daily.csv`

## M6. `pgm_observed_product_count`

- grain: `date`
- definition: role state가 same-date로 관측된 상품 수
- source: `mart/brand_operating_status_daily.csv`
- note: coverage 해석용 지표이며 매출 제외 조건이 아닙니다.

## M7. `top_product_revenue_share`

- grain: `date`
- definition: 특정 일자 브랜드 총매출 중 1위 상품 비중
- source: `mart/revenue_structure_daily.csv`, `mart/brand_operating_status_daily.csv`

## M8. `revenue_share_by_role_state`

- grain: `date + role_state_primary`
- definition: 특정 일자 각 역할 상태가 차지한 매출 비중
- source: `product_daily_metrics + product_role_state_daily`
- note: blank state는 `PGM 미관측` bucket으로 서빙 가능

## M9. `revenue_7d`, `revenue_30d`, `revenue_90d`

- grain: `date` 또는 `date + product_id`
- definition: 기준일 당일 미포함 rolling revenue 합계
- source: `product_daily_metrics.csv`, `brand_operating_status_daily.csv`
- note: 7/30/90일 모두 동일 규칙. 기준일 당일은 포함하지 않습니다.

## M10. `revenue_day_over_day_change_rate`

- grain: `date` 또는 `date + product_id`
- definition: 당일 매출과 전일 매출의 변화율
- source: `brand_operating_status_daily.csv`, `product_daily_metrics.csv`
- note: 전일 값이 0이면 `0%`로 왜곡하지 않고 `null` 처리합니다.

## M11. `profile_role_primary`

- grain: `product_id`
- definition: 상품의 비교적 안정적인 역할 성향
- source: `mart/product_role_profile.csv`
- note: latest profile snapshot을 쓰지만, daily role-state blank를 메우는 fallback으로 쓰지 않습니다.

## M12. `pgm_observed_coverage`

- grain: `date`
- definition: 활성 상품 중 same-date role state가 관측된 상품 비율
- source: `mart/brand_operating_status_daily.csv`

## Rule summary

- Rolling windows: `7/30/90d` 모두 기준일 당일 미포함
- Day-over-day: 전일 값이 `0`이면 `null`
- Role state blank rule: same-date snapshot 없으면 blank 유지
- View-model labeling rule: blank role state는 UI에서만 `PGM 미관측`으로 라벨링
- Revenue priority rule: 상태 요약은 revenue 신호를 우선하고, 구조 신호는 보정으로 사용
- Priority check rule source: brand 경보는 `brand_operating_status_daily`, 상품 우선순위는 latest `revenue_structure_daily`
