# PS와 PAI(Purchase Activation Index)의 관계 (v1.0)

> Legacy note: 문서의 이전 `BII` 표기는 모두 `PAI`로 전환되었으며, 구현에서는 한 단계 동안 호환 alias를 유지합니다.

## One-liner
- `PS` (`brand_score.csv` legacy column: `BHI`): 브랜드의 4+1 Gravity 구조 건강도
- `PAI`: 그 구조 건강도가 기간별 구매 활성도로 나타난 상태

## PS (Purchase Structure)가 요약하는 것
`PS`는 아래 5개 구조 축을 하나의 Purchase Structure로 요약한다. 현재 구현에서는 이 값을 `BHI` 컬럼명으로 저장한다.

- `Entry_Structure_Index`
- `Expansion_Structure_Index`
- `Convergence_Structure_Index`
- `Return_Structure_Index`
- `Basket_Structure_Index`

즉 BHI는 브랜드가 `Acquire -> Expand -> Gather -> Loop` 구조와 Basket 확장축을 함께 갖췄는지를 본다.

## PAI가 사용하는 것
`PAI`는 `PS`를 그대로 받아 기간별 구매 활성도로 변환한다. 현재 구현에서는 legacy `BHI` 컬럼을 읽는다.

`PAI_t = PS × CLV_t_norm × Customer_Strength_t_norm`

여기서 `Customer_Strength_t_norm` 안에는 반복구매와 장바구니 깊이가 같이 들어간다.

- `Depth_t = 0.7 × RepeatRate_t + 0.3 × AttachRate_t`
- 현재 구현에서 `RepeatRate_t`는 선택 기간 안 활성 고객 중 같은 기간에 2회 이상 주문한 고객 비율이다.

## 역할 분리
- `PS` (`brand_score.csv` legacy column: `BHI`)는 구조의 본질을 본다.
- `PAI`는 구조의 최근 발현 상태를 본다.
- `Momentum`은 그 구매 활성도가 장기 기준 대비 얼마나 강한지를 본다.
- `Drivers`는 그 Momentum을 무엇이 만들었는지를 본다.

따라서 두 지표는 중복이 아니다.

- `PS` (`brand_score.csv` legacy column: `BHI`)가 낮으면 구조 병목이 있다는 뜻이다.
- `PAI`가 낮으면 최근 구매 활성도가 약하다는 뜻이다.
- `PS` (`brand_score.csv` legacy column: `BHI`)가 높고 `PAI`가 낮으면 구조는 있으나 최근 구매 활성도가 약한 상태다.
- `PS` (`brand_score.csv` legacy column: `BHI`)가 낮고 `PAI`가 높으면 단기 성과는 있으나 구조 리스크가 남아 있다.

실무 해석은 아래 흐름으로 읽는다.

- `PS` (`brand_score.csv` legacy column: `BHI`) → 구조
- `PAI` → 구매 활성도
- `Momentum` → 구매 활성도 변화 방향에 대한 상대 강도 해석
- `Drivers` → 변화의 원인

즉 `구조 -> 구매 활성도 -> 변화`의 흐름이다.

`Momentum`은 별도 독립 점수가 아니라 `PAI_t / PAI_365` ratio 해석 레이어다.

`purchase_activation_driver_timeseries.csv`는 이 해석을 Hero/Drivers UI에 바로 연결하기 위한 공식 파생 산출물이다.

## Structure Snapshot과의 관계
`brand_structure_timeseries.csv`는 `PS`나 `PAI`를 대체하는 지표가 아니다.

이 파일은 각 `as_of_date` 시점의 최근 `7/30/90일` 행동 데이터를 기준으로 Structure UI 4카드를 같은 기간 기준으로 맞춰주기 위한 windowed structure snapshot이다.

`Entry/Flow`는 window 안 첫 관측 주문을 쓰지 않는다.

- `Entry`: 고객의 브랜드 전체 이력 기준 실제 첫 구매가 window 안에 들어온 cohort
- `Flow`: 그 cohort의 실제 두 번째 구매가 window 안에 들어온 비율과 대표 경로

- Entry -> `entry_product_ratio`
- Flow -> `flow_transition_rate`
- Return -> `return_customer_rate`
- Basket -> `basket_items_per_order`

여기서 `ps_static`는 동일 날짜의 `7/30/90` row에서 변하지 않는다.

- `ps_static` = `brand_score.csv`의 legacy `BHI` 컬럼에 저장된 `PS`
- 즉 `brand_structure_timeseries.csv`는 `PS history`가 아니라, `PS`를 anchor로 둔 행동 기반 구조 스냅샷이다.

## 운영 규칙
- 계산 위치: `03_PGM_BrandHealthImpact.ipynb`
- 선행 실행:
1. `01_PGM_ProductGravity.ipynb`
2. `02_PGM_ConvergenceReturnGravity.ipynb`
3. `03_PGM_BrandHealthImpact.ipynb`
- `purchase_activation_index.csv`의 `bhi`는 `brand_score.csv`의 legacy `BHI` 컬럼에 저장된 `PS`와 일치해야 한다.
