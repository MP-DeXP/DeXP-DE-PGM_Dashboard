# BHI와 BII의 관계 (v1.0)

> Naming Migration Note (2026-03-03): 표시명은 `PGM` 기준이며, 기술 식별자(`AA/PCA/CA`)는 호환을 위해 유지합니다.

## 목적
PGM 지표 체계에서 `BHI`와 `BII`의 역할 분리를 명확히 정의한다.

## One-liner
- `BHI`: 브랜드 구조의 건강도(체질) 요약 지표
- `BII`: 구조 건강도(`BHI`)가 기간별 상업 체력으로 발현된 상태 지표

## 수식 관계
`t ∈ {1, 7, 30, 90, 365}` 일 때,

`BII_t = BHI × CLV_t_norm × Customer_Strength_t_norm`

- `CLV_t_norm = sqrt(Avg_CLV_t / Avg_CLV_baseline)`
- `Customer_Strength_t_norm = sqrt((Active_Customers_t × Depth_t) / Active_Customers_baseline)`
- `Depth_t = 0.7 × RepeatRate_t + 0.3 × AttachRate_t`

즉, `BII`는 `BHI`를 기반으로 하되, 기간별 고객 가치/고객 강도를 반영해 동적으로 변한다.

## 해석 가이드
- `BHI` 높고 `BII`도 높음: 구조와 상업 체력이 함께 양호
- `BHI` 높고 `BII` 낮음: 구조는 좋지만 최근 전환 체력 약화
- `BHI` 낮고 `BII` 높음: 단기 성과는 있으나 구조 리스크 점검 필요
- `BHI` 낮고 `BII` 낮음: 구조/운영 모두 개선 필요

추세 확인은 `bii_90_over_365`, `bii_30_over_365`, `bii_7_over_365`를 사용한다.
다만 비율 지표는 `BHI`의 절대 수준을 상쇄하므로, `BHI`와 함께 해석한다.

## 운영 규칙 (현재 구현 기준)
- `BHI`는 `brand_score.csv`에서 입력받아 사용한다.
- `BII`는 `BII_Implementation_v1.ipynb`에서 계산한다.
- 범위(scope): `candidate`
- 윈도우: trailing fixed windows (`1/7/30/90/365d`)
- baseline: `max(90, min(365, operating_days))`
- 출력:
  - `brand_impact_windows.csv`
  - `brand_impact_index.csv`

## 결론
`BHI`와 `BII`는 중복 지표가 아니다.
`BHI`는 구조의 본질을, `BII`는 구조의 기간별 발현 상태를 설명하므로 분리 유지가 타당하다.
