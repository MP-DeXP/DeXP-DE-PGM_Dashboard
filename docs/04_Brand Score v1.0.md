# Brand Score v1.0

> Naming Migration Note (2026-03-03): 표시명은 `PGM` 기준이며, 기술 식별자(`AA/PCA/CA`)는 호환을 위해 유지합니다.

Brand Score v1.0.2
PGM 기반 브랜드 구조 건강도 평가 백서

Executive Summary
대부분의 브랜드 평가는 여전히 매출 규모, 성장률, 점유율과 같은 결과 지표에 의존한다.
그러나 이러한 지표는 **“왜 이 브랜드가 성장하는지, 혹은 왜 불안하게 성장하는지”**를 설명하지 못한다.
Brand Score v1.0.2는
브랜드를 **성과(Result)**가 아닌 **구조(Structure)**로 평가하기 위한 프레임워크다.
본 프레임은 브랜드를 줄 세우지 않는다.
대신, 브랜드가 고객 생애 가치(CLV)를 안정적으로 만들어낼 수 있는 구조를 가지고 있는지를 진단한다.

1. 문제 정의: 왜 기존 브랜드 평가는 실패하는가
1.1 결과 중심 지표의 한계
- 매출은 크지만 신규 유입이 멈춘 브랜드
- 성장률은 높지만 반복 구매가 없는 브랜드
- 소수 상품에 과도하게 의존하는 브랜드
이들은 모두 **성과 지표만 보면 ‘좋아 보이는 브랜드’**다.
그러나 구조적으로는 언제든 무너질 수 있는 상태다.

1.2 Brand Score의 질문 전환
기존 질문:
“이 브랜드는 얼마나 벌고 있는가?”
Brand Score의 질문:
“이 브랜드는 어떤 구조로 고객 생애 가치를 만들어내고 있는가?”

2. 설계 철학 및 핵심 원칙
원칙 1. 절대값 직접 사용 금지
- 매출 합계
- 고객 수
- 주문 수
👉 점수 입력값으로 사용하지 않는다.

원칙 2. 구조 지표만 사용
- 비율
- 분포
- 균형
- 집중도

원칙 3. 고객이 없어도 평가 가능해야 한다
- 고객 0명 = 실패 ❌
- 고객 0명 = 아직 구조가 검증되지 않음 ⭕

원칙 4. 해석 책임은 사람에게 둔다
- 점수는 요약
- 판단은 구조 해석을 통해 수행

3. Brand Score v1.0.2 전체 구성
Brand Score v1.0.2 =
{
  Brand Structure Vector,
  Brand Health Index (BHI),
  Confidence Index
}

4. Brand Structure Vector (본체)
Brand Structure Vector는 3개의 구조 축으로 구성된다.

4.1 Acquisition Structure (AS)
“이 브랜드는 어떤 방식으로 신규 고객을 데려오는가?”
입력
- AA_Primary_Type 분포 (Broad / Qualified / Heavy)
- 상품별 first_customer_cnt 분포
핵심 구조 지표
- AA_Broad_Ratio
- AA_Qualified_Ratio
- AA_Heavy_Ratio
- Acquisition Concentration Index (상위 N개 상품 유입 집중도)
해석
- Broad 중심 → 확장형 유입
- Qualified 중심 → 효율형 유입
- Concentration ↑ → 구조적 리스크

4.2 Chain Structure (CS)
“이 브랜드는 구매사슬을 어떻게 열고 이어가는가?”
입력
- PCA_Primary_Type 분포 (Core / Deep / Scale)
- avg_days_to_pca (보조)
핵심 구조 지표
- PCA_Core_Ratio
- PCA_Deep_Ratio
- PCA_Scale_Ratio
- Chain Balance Index
해석
- Core만 많음 → 얕은 사슬
- Deep 없음 → 팬 구조 취약
- Scale 없음 → 안정성 부족
- Velocity 느림 → 사슬 탄력 약함

4.3 Value Structure (VS)
“이 브랜드는 CLV를 만들어낼 준비가 된 구조인가?”
⚠️ Value Structure는 성과 평가가 아니라 ‘준비도(Readiness)’ 평가다.

4.3.1 Value Activation Index (VAI)
- PCA_Core 상품 존재 여부
- repurchase_rate_90d 브랜드 중앙값
CLV가 실제로 ‘시작되는 구조’가 있는가?

4.3.2 Value Quality Index (VQI)
- avg_CLV_90d = revenue_90d / first_customer_cnt
- 브랜드 내 상대적 분위수 위치
CLV가 시작되었을 때, 질은 어떤가?

4.3.3 Value Concentration Risk (VCR)
- CLV의 상품 집중도
- Top-N 집중도, 지니계수 등
⚠️ 단, 집중된 상품이 PCA-Scale인 경우
이는 리스크가 아니라 안정화 앵커로 재해석된다.

해석 원칙
- 고객 0 → VAI = 0 → 미검증 상태
- 이는 실패가 아니라 측정 불가 상태다.

5. 통합 요약 지표: Brand Health Index (BHI)
5.1 BHI의 목적
BHI는 브랜드 구조의 ‘균형도’를 요약하는 인덱스다.
- 랭킹 ❌
- 자동 의사결정 ❌
- 인지적 요약 ⭕

5.2 BHI v1.0.2 산식
BHI = min(AB, CB, VR)
    + ε × 평균(나머지 두 축)
- ε ∈ [0.01, 0.05]
- 최약점 우선 원칙 유지
- 미세한 차이는 반영

6. Confidence Index (필수 보조 레이어)
이 Brand Score는 얼마나 신뢰할 수 있는가?
기준 예시
- High
- brand_first_customer_cnt ≥ 500
- PCA_Core 상품 ≥ 3
- Medium
- brand_first_customer_cnt ≥ 100
- PCA_Core 상품 ≥ 1
- Low
- 그 외
⚠️ Confidence Index는
점수에 반영하지 않고, 반드시 시각적으로 강조한다.

7. 해석 및 운영 가이드 (의무 문구)
- “BHI는 우등 성적표가 아니라, 체격 대비 체력 진단서입니다.”
- “점수를 높이기 위해 상품 타입을 억지로 섞지 마십시오.”
- “본 점수에는 절대 매출 규모가 반영되어 있지 않습니다.”

8. 의도적 한계 (Boundary)
Brand Score v1.0.2는 다음을 다루지 않는다.
- 인과 추정
- 실험 기반 증분성
- 미래 CLV 예측
이는 v1.1 이후 영역이다.

9. 공식 정의 (One-liner)
Brand Score v1.0.2는
브랜드의 성과를 평가하는 점수가 아니라,
브랜드가 고객 생애 가치를
안정적으로 만들어낼 수 있는 구조를 가졌는지를
신뢰도와 함께 진단하는 경영 프레임이다.
