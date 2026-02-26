# APF Dashboard

내부 테스트/시연용 APF 분석 대시보드입니다.  
기존 페이지는 유지하고, 신규 메인 페이지 `Insight Studio`를 추가했습니다.

## 핵심 목적
- 마케팅 담당자와 MD가 한 화면에서 함께 의사결정
- AA(첫구매자) 이후 90일 행동/전환 최적화
- BHI/BII/CA/전이 구조를 액션 카드로 연결

## 페이지 구성
- `insights.html`: Insight Studio (기본 진입)
- `index.html?overview=1`: Overview (기존)
- `products.html`: Products
- `transitions.html`: Transitions
- `cart.html`: Cart Analysis

참고:
- `index.html`을 직접 열면 `insights.html`로 자동 이동합니다.
- Overview는 `index.html?overview=1`로 접근합니다.

## 실행 방법
1. 브라우저에서 `insights.html` 파일을 직접 엽니다.
2. 사이드바의 `Upload Data` 버튼을 누릅니다.
3. 필요한 CSV를 다중 선택 업로드합니다.
4. 필터(날짜/AA 타입/AA 상품/Window)로 인사이트를 탐색합니다.

## 업로드 CSV 키
파일명에 아래 키를 포함해야 자동 매칭됩니다.

기존:
- `brand_score`
- `anchor_scored`
- `anchor_transition`
- `cart_anchor`
- `cart_anchor_detail`

신규:
- `aa_cohort_journey`
- `aa_transition_path`
- `ca_profile`
- `bii_window`
- `apf_action_rules` (선택)

## 데이터 처리 전략
- 무거운 계산: 사전 집계 CSV로 제공
- 가벼운 파생 계산: 브라우저에서 실시간 계산
- 저장소: IndexedDB (`APF_Dashboard_DB` / `csv_files`)

## Insight Studio 섹션
- Hero/Story: BHI, BII 90/365, 경고 요약
- AA Cohort Journey: 7/30/90일 행동 퍼널
- AA → PCA Transition: 전이 경로/집중도/소요일
- Cart/CA Insight: CA 타입/장바구니 결합력
- Brand Fitness: BHI 축 + BII 다기간 체력
- Action Center: 마케팅/MD 실행 카드

## 관련 문서
- 에이전트 컨텍스트: `AGENT.md`
- 데이터 스키마: `DATA_SCHEMA.md`

## 참고
- 일부 CSV가 없어도 섹션 단위로 안내 메시지와 함께 렌더됩니다.
- 렌더링 로직은 `app.js`, 공통 스타일은 `style.css`에 있습니다.
