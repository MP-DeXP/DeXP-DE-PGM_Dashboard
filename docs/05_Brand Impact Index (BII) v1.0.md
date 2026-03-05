# Brand Impact Index (BII) v1.0

> Naming Migration Note (2026-03-03): 표시명은 `PGM` 기준이며, 기술 식별자(`AA/PCA/CA`)는 호환을 위해 유지합니다.

Brand Impact Index (BII) v1
― PGM 구조의 상업적 체력을 나타내는 안정형 다기간 지표 ―

1. Executive Overview
Brand Impact Index(BII)는
브랜드의 PGM 구조(AA/CA/PCA)가
실제 상업적 가치로 얼마나 안정적으로 전환되고 있는지를
기간별로 표현하는 상업 체력 지표이다.
BII는 매출을 직접 측정하지 않는다.
대신 매출을 지속적으로 만들어낼 수 있는
**구조적 상업 체력(Commercial Fitness)**을 측정한다.
BII는 다음 특성을 가진다:
- 안정형(Level) 지표
- 다기간 윈도우 기반 (1/7/30/90/365일)
- 단일 브랜드 기준
- Revenue 직접 포함 없음 (CLV를 통해 간접 반영)
- 동적 Baseline 적용
- 성장 단계(Stage) 구분
- 단기/중기/장기 체력 분리

2. PGM와의 정합성
Product Gravity Model(PGM)는 상품의 역할을 정의한다:
- AA: 유입
- CA: 장바구니 확장
- PCA: 구매 사슬 형성
PGM는 구조를 설명한다.
BII는 이 구조가 실제 상업적 가치로 전환되는지를 설명한다.
즉,
BII =
  Structure (BHI)
  × Customer Value (CLV)
  × Customer Base Strength
BII는 구조의 결과적 표현이며,
구조를 대체하지 않는다.

3. 다기간 체계 (Multi-Window Framework)
BII는 매출 지표와 동일한 기간 구조를 가진다.
구분
의미
BII_1d
하루 체력
BII_7d
주간 체력
BII_30d
월간 체력
BII_90d
분기 체력
BII_365d
연간 기준 체력
핵심 원칙
- BII_365는 기준 체력(Baseline Fitness)
- 나머지 기간은 기준 대비 현재 체력을 나타낸다.
365d는 비교 대상이 아니라
장기 구조 체력의 기준점이다.

4. Baseline 정의 (전략 1 적용)
단일 브랜드 환경에서는 시장 평균이 없으므로
Baseline은 자기 기준을 사용한다.
baseline_days =
  max(90, min(365, 운영총기간))
- 최소 90일 확보
- 최대 365일 기준
- 신생 브랜드도 계산 가능
Baseline은 장기 평균 상태를 의미한다.

5. BII 산식
각 기간 t ∈ {1,7,30,90,365}
BII_t =
    BHI
    × CLV_t_norm
    × Customer_Strength_t_norm

5.1 CLV 정규화
CLV_t_norm =
    sqrt( Avg_CLV_t / Avg_CLV_baseline )
- Avg_CLV_t: 해당 기간 고객 평균 누적 가치
- Avg_CLV_baseline: baseline 기간 평균 CLV
- sqrt는 급격한 변동 완화 목적
- Revenue는 CLV를 통해 간접 반영

5.2 Customer Strength (통합형 Depth 적용)
Depth는 기간별 정의를 바꾸지 않는다.
대신 재구매 중심 설계를 유지하면서 CA를 보조 신호로 혼합한다.
5.2.1 RepeatRate_t
RepeatRate_t =
  해당 기간 내 2회 이상 구매 고객 비율
(PCA 기반)

5.2.2 AttachRate_t
AttachRate_t =
  해당 기간 내 cart_size > 1 비율
(CA 기반)

5.2.3 통합 Depth
Depth_t =
  0.7 × RepeatRate_t
+ 0.3 × AttachRate_t
- 재구매를 더 중요한 신호로 유지
- 단기 구간에서 0 수렴 방지
- PGM 철학 정합 (PCA > CA)

5.2.4 Customer_Strength_t_norm
Customer_Strength_t_norm =
    sqrt(
        (Active_Customers_t / Active_Customers_baseline)
        ×
        Depth_t
    )
- 유입 규모 + 고객 질 결합
- 단기 노이즈 완화

6. Stage Classification (전략 2 적용)
운영 기간에 따라 해석 신뢰도를 구분한다.
운영기간
Stage
< 90일
Early
90~180일
Developing
≥ 180일
Stable
Stage는 BII의 절대값을 변경하지 않으며,
해석 안정성을 보조한다.

7. 해석 구조
7.1 365d (기준 체력)
- 브랜드의 장기 상업 체력
- 비교 대상이 아니라 기준점

7.2 단기·중기 체력
BII_90 / BII_365
BII_30 / BII_365
BII_7 / BII_365

비율
해석
≥ 1.15
구조적 강화
0.95 ~ 1.15
안정
0.85 ~ 0.95
경고
< 0.85
구조 약화

8. 365d에서 분모·분자 동일성 문제의 해석
365일 기준에서는:
Avg_CLV_365 ≈ Avg_CLV_baseline
Active_Customers_365 ≈ Active_Customers_baseline
따라서 BII_365는 장기 체력 상태를 나타낸다.
이는 설계상 자연스러운 결과이며,
365d는 “비교 지표”가 아니라 “기준 체력”이다.

9. BII와 매출의 관계
Revenue는 결과 지표다.
BII는 구조적 상업 체력 지표다.
- Revenue 상승 + BII 하락 → 구조 리스크
- Revenue 정체 + BII 상승 → 장기 개선 신호
- Revenue 상승 + BII 상승 → 건강한 성장
BII는 매출을 대체하지 않으며,
매출의 구조적 기반을 설명한다.

10. 결론
Brand Impact Index v1.0은
- PGM 구조를 유지하면서
- 단기·중기·장기 체력을 분리하고
- 단일 브랜드 환경에서 안정적으로 작동하며
- Revenue와 직접 충돌하지 않는
상업 체력 지표다.
BII는 단순 점수가 아니다.
BII는 구조가 돈으로 전환되는 능력의 표현이다.

11. 구현 동기화 노트 (BII_Implementation_v1.ipynb 기준)
본 문서는 개념/원칙 문서이며, 아래 내용은 현재 구현체와의 정합 기준이다.

- 구현 위치: `BII_Implementation_v1.ipynb`
- 입력 파일:
  - `pgm_scored.csv` (legacy alias: `anchor_scored.csv`)
  - `order_product_events.csv`
  - `brand_score.csv`
- 추가 원천:
  - `core.silver_meta_order`에서 결제금액(`order_amount_info.payment_amount`)을 조회
  - 채널 필터: `mx_channel_id == CHANNEL_ID`
- BHI 소스:
  - BII 계산 시 BHI는 재계산하지 않고 `brand_score.csv`의 `BHI`를 사용
- Scope:
  - 현재 구현은 `candidate` scope 고정
  - `order_product_events`에서 추출한 `order_id` 집합으로 대상 주문을 제한
- 기간/윈도우:
  - trailing fixed windows `1/7/30/90/365d`
  - `analysis_end_date` 기준 포함 범위(inclusive)로 계산
- Baseline:
  - `baseline_days = max(90, min(365, operating_days))`
  - 주문 데이터가 비어 있을 경우 안전값 90일 사용
- Stage:
  - `Early`: 운영기간 < 90일
  - `Developing`: 90일 이상 180일 미만
  - `Stable`: 180일 이상
- 안정성 처리:
  - `repeat_rate_t`, `attach_rate_t`, `depth_t`는 `[0,1]` clamp
  - 분모가 0 이하인 정규화/비율 연산은 0으로 처리
  - `bii_t`와 summary 지표는 음수 방지 처리
- 출력:
  - `brand_impact_windows.csv`
  - `brand_impact_index.csv`

운영 해석 시에는 `BII` 단독이 아니라 `BHI`와 함께 본다.
특히 `bii_90_over_365`, `bii_30_over_365`, `bii_7_over_365`는 추세 판단에 유용하지만,
`BHI`의 절대 수준을 대체하지 않는다.
