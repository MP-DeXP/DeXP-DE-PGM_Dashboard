# Brand Score 처리 방침 메모

## Rosetta 기준 가능 / 불가

- 직접 가능한 축
  - Entry: `dma.gold_pgm_scored`
  - Expansion: `dma.gold_pgm_scored`
  - Convergence: `dma.gold_pgm_product_demand_gravity`
  - Return: `dma.gold_pgm_product_demand_gravity`
- 직접 부족한 축
  - Basket: 상품 요약형 `pgm_basket_gravity.csv` 직접 source 없음
  - activation 계열: `order_product_events.csv` 직접 source 없음

## core 참고 정의

- 참고 파일: `pipeline/steps/step03_brand_health.py`
- 입력 정의
  - `pgm_scored.csv`
  - `pgm_basket_gravity.csv`
  - `pgm_product_demand_gravity.csv`
  - `order_product_events.csv`
- 출력 정의
  - `brand_score.csv`
  - `purchase_activation_windows.csv`
  - `purchase_activation_index.csv`
  - `purchase_activation_timeseries.csv`
- 산식 정의
  - 5축: Entry / Expansion / Convergence / Return / Basket
  - legacy field는 `BHI`지만 `pgm_ops2` UI 표기는 모두 `Brand Score`로 통일
  - `PS = min(5축) + 0.03 * average(5축)`

## Rosetta source만으로 재현 가능한 것

- Entry / Expansion / Convergence / Return 4축
- basket pair 상세 기반 상품별 basket summary 근사
- confidence 관련 일부 입력
- PS 산식 재현

## 재현 불가능하거나 정의가 부족한 것

- core intermediate와 의미가 동일한 `pgm_basket_gravity.csv` 보장
- core와 동일 의미의 `order_product_events.csv` 보장
- exact parity 보장
- activation 계열 완전 재현

## v1 반영 수준

- 큐: 미반영
- 상세 화면: 표시
- 정의 화면: 표시
- 데이터 상태 화면: 표시
- 구조 맵: 작은 상태칩만 허용

## 상태 규칙

- 핵심 축 결손: `unavailable`
- 산식은 가능하지만 basket/event 재현 미흡: `limited`
- 대부분 재현되지만 일부 intermediate 차이 존재: `provisional`
- 입력/축/산식 유사도 높음: `near-core`
