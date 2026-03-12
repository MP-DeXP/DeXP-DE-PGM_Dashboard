# BHI와 BII의 관계 (v1.0)

## One-liner
- `BHI`: 브랜드의 4+1 Gravity 구조 건강도
- `BII`: 그 구조 건강도가 기간별 상업 체력으로 나타난 상태

## BHI가 요약하는 것
`BHI`는 아래 5개 구조 축을 하나의 브랜드 건강도로 요약한다.

- `Entry_Structure_Index`
- `Expansion_Structure_Index`
- `Convergence_Structure_Index`
- `Return_Structure_Index`
- `Basket_Structure_Index`

즉 BHI는 브랜드가 `Acquire -> Expand -> Gather -> Loop` 구조와 Basket 확장축을 함께 갖췄는지를 본다.

## BII가 사용하는 것
`BII`는 `BHI`를 그대로 받아 기간별 상업 체력으로 변환한다.

`BII_t = BHI × CLV_t_norm × Customer_Strength_t_norm`

여기서 `Customer_Strength_t_norm` 안에는 반복구매와 장바구니 깊이가 같이 들어간다.

- `Depth_t = 0.7 × RepeatRate_t + 0.3 × AttachRate_t`

## 역할 분리
- `BHI`는 구조의 본질을 본다.
- `BII`는 구조의 최근 발현 상태를 본다.

따라서 두 지표는 중복이 아니다.

- `BHI`가 낮으면 구조 병목이 있다는 뜻이다.
- `BII`가 낮으면 최근 상업 체력이 약하다는 뜻이다.
- `BHI`가 높고 `BII`가 낮으면 구조는 있으나 최근 전환력이 약한 상태다.
- `BHI`가 낮고 `BII`가 높으면 단기 성과는 있으나 구조 리스크가 남아 있다.

## 운영 규칙
- 계산 위치: `03_PGM_BrandHealthImpact.ipynb`
- 선행 실행:
1. `01_PGM_ProductGravity.ipynb`
2. `02_PGM_ConvergenceReturnGravity.ipynb`
3. `03_PGM_BrandHealthImpact.ipynb`
- `brand_impact_index.csv`의 `bhi`는 `brand_score.csv`의 `BHI`와 일치해야 한다.
