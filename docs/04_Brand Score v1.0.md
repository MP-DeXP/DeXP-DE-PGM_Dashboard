# Brand Score v1.0

## 목적
Brand Score는 브랜드를 매출이 아니라 수요 구조로 진단하는 프레임이다.

핵심 질문은 다음이다.

- 이 브랜드는 신규 수요를 안정적으로 열 수 있는가
- 다음 구매를 이어갈 수 있는가
- 여러 경로의 수요를 한 상품군으로 모을 수 있는가
- 고객을 다시 돌아오게 만들 수 있는가
- 장바구니 구조까지 포함해 반복 가능한 상업 구조를 갖췄는가

즉 Brand Score는 브랜드의 `Acquire -> Expand -> Gather -> Loop` 구조와 `Basket` 확장축을 함께 본다.

## 구성
`brand_score.csv`는 아래 5개 structure index와 요약 지표로 구성된다.

- `Entry_Structure_Index`
- `Expansion_Structure_Index`
- `Convergence_Structure_Index`
- `Return_Structure_Index`
- `Basket_Structure_Index`
- `PS` (`brand_score.csv` legacy column: `BHI`)
- `Confidence_Index`

보조 진단 컬럼:

- `Entry_Concentration_Risk`
- `Expansion_Balance_Index`
- `Convergence_Coverage_Ratio`
- `Convergence_Source_Diversity_Index`
- `Return_Coverage_Ratio`
- `Return_Concentration_Risk`
- `Basket_Coverage_Ratio`
- `Basket_Balance_Index`

## 입력
현재 구현은 아래 산출물을 결합해 Brand Score를 계산한다.

- `pgm_scored.csv`
- `pgm_basket_gravity.csv`
- `pgm_product_demand_gravity.csv`

`order_product_events.csv`는 Brand Score 자체보다 후속 `PAI(Purchase Activation Index)` 계산을 위해 같은 노트북에서 함께 요구된다.

## 5축 정의
### Entry Structure
브랜드가 신규 수요를 여는 구조 건강도.

- 강도: 제품별 `Entry_Gravity_Score` 평균
- 균형: `Broad`와 `Qualified` 타입의 균형
- 리스크: 상위 3개 entry 상품에 first purchase가 몰리는 정도

공식 산식:

`Entry_Structure_Index = 0.4*avg(Entry_Gravity_Score) + 0.3*entry_type_balance + 0.3*(1-entry_concentration_risk)`

### Expansion Structure
브랜드가 다음 구매를 이어가는 구조 건강도.

- 강도: 제품별 `Expansion_Gravity_Score` 평균
- 커버리지: `Core/Deep/Scale` 타입 제품 비중
- 균형: `Core/Deep/Scale` 분포 균형

공식 산식:

`Expansion_Structure_Index = 0.4*avg(Expansion_Gravity_Score) + 0.3*expansion_coverage_ratio + 0.3*expansion_balance_index`

### Convergence Structure
브랜드가 여러 구매 경로의 수요를 한 상품군으로 모으는 구조 건강도.

- 강도: 제품별 `Convergence_Gravity_Score` 평균
- 커버리지: `distinct_source_product_cnt_90d > 0` 인 제품 비중
- 다양성: 제품별 수렴 source 다양성 평균

공식 산식:

`Convergence_Structure_Index = 0.4*avg(Convergence_Gravity_Score) + 0.3*convergence_coverage_ratio + 0.3*convergence_source_diversity_index`

### Return Structure
브랜드가 고객을 다시 돌아오게 만드는 구조 건강도.

- 강도: 제품별 `Return_Gravity_Score` 평균
- 커버리지: `return_loop_rate_90d > 0` 인 제품 비중
- 리스크: return 수요가 상위 소수 상품에 집중되는 정도

공식 산식:

`Return_Structure_Index = 0.4*avg(Return_Gravity_Score) + 0.3*return_coverage_ratio + 0.3*(1-return_concentration_risk)`

### Basket Structure
브랜드가 동시구매 구조를 얼마나 넓고 균형 있게 갖추는지.

- 강도: 제품별 `attach_rate` 평균
- 커버리지: `Basket_Gravity_Primary_Type in {Core, Pair, Set}` 비중
- 균형: `Core/Pair/Set` 분포 균형

공식 산식:

`Basket_Structure_Index = 0.4*avg(attach_rate) + 0.3*basket_coverage_ratio + 0.3*basket_balance_index`

## PS (Purchase Structure)
`PS`는 5축 equal-axis purchase structure index다. 현재 구현의 legacy technical column name은 `BHI`다.

공식 산식:

`PS = min(Entry, Expansion, Convergence, Return, Basket) + 0.03 * average(all 5 indices)`  (`brand_score.csv` legacy column: `BHI`)

의도는 명확하다.

- 브랜드의 최약점을 먼저 반영한다
- 하지만 나머지 축의 평균 강도도 작은 보정항으로 남긴다

따라서 `PS`는 랭킹 점수보다 구조적 병목 탐지에 더 적합하다.

## Confidence Index
`Confidence_Index`는 구조 진단의 신뢰도를 표시하는 보조 레이어다.

- `brand_first_customer_cnt = sum(first_customer_cnt)`
- `structural_active_product_cnt = count(product with any positive Entry/Expansion/Convergence/Return score or Basket type != None)`

판정 규칙:

- `High`: `brand_first_customer_cnt >= 500` and `structural_active_product_cnt >= 5`
- `Medium`: `brand_first_customer_cnt >= 100` and `structural_active_product_cnt >= 2`
- 그 외 `Low`

## 해석 원칙
- Brand Score는 절대 매출을 직접 점수화하지 않는다.
- Brand Score는 결과가 아니라 구조를 본다.
- `PS` (`brand_score.csv` legacy column: `BHI`)가 높아도 특정 축이 낮으면 그 축이 구조 병목이다.
- `Basket`은 보조 modifier가 아니라 다섯 번째 동등 축이다.

## 운영 메모
- 구현 노트북: `03_PGM_BrandHealthImpact.ipynb`
- 보조 구조 스냅샷 노트북: `03d_PGM_BrandStructureTimeseries.ipynb`
- 선행 실행:
1. `01_PGM_ProductGravity.ipynb`
2. `02_PGM_ConvergenceReturnGravity.ipynb`
3. `03_PGM_BrandHealthImpact.ipynb`

## Windowed Structure Snapshot
`brand_structure_timeseries.csv`는 `PS`의 공식 history가 아니다.

이 파일은 특정 `as_of_date` 시점의 최근 `7/30/90일` 행동 데이터를 기준으로 Entry / Flow / Return / Basket 구조 신호를 같은 기간 언어로 정렬한 windowed structure snapshot이다.

- `Entry` 카드 -> `entry_product_ratio`
- `Flow` 카드 -> `flow_transition_rate`
- `Return` 카드 -> `return_customer_rate`
- `Basket` 카드 -> `basket_items_per_order`

주의:

- `ps_static`는 window별 재계산 구조값이 아니다.
- `ps_static`는 `brand_score.csv`의 legacy `BHI` 컬럼에 저장된 `PS`를 그대로 반복한 static anchor structure다.
- 따라서 `brand_structure_timeseries.csv`는 `PS timeseries`가 아니라 UI용 보조 구조 스냅샷으로 해석해야 한다.
