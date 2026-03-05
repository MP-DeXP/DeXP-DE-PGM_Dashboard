# PGM Dashboard 에이전트 컨텍스트

## 1) 프로젝트 목적
- 이 프로젝트는 내부 테스트/시연을 위한 브라우저 기반 분석 대시보드입니다.
- 운영(프로덕션) 목적은 아닙니다.
- 핵심 목표는 CSV 데이터를 빠르게 올리고, PGM 주요 지표를 시각적으로 확인하는 것입니다.

## 2) 기술/구조
- 프런트엔드 단독 구조(백엔드 없음, 빌드 단계 없음).
- 주요 파일:
  - `insights.html` (Insight Studio, 기본 진입)
  - `index.html` (Overview)
  - `products.html` (Products)
  - `transitions.html` (Transitions)
  - `cart.html` (Cart Analysis)
  - `app.js` (전체 로직)
  - `style.css` (공통 스타일)
- 외부 라이브러리는 CDN으로 로드:
  - Chart.js
  - PapaParse
  - Phosphor Icons
  - Google Fonts

## 3) 데이터 로딩 방식
- 데이터 소스: `data/` 경로 CSV 자동 로드 + Upload 모달 수동 업로드 fallback
- 파싱: PapaParse (`header: true`, `dynamicTyping: true`)
- 저장: IndexedDB (`PGM_Dashboard_DB`, store: `csv_files`)
- 필수 데이터셋 키:
  - `brand_score`
  - `anchor_scored`
  - `anchor_transition`
  - `cart_anchor`
  - `cart_anchor_detail`
  - `aa_cohort_journey`
  - `aa_transition_path`
  - `ca_profile`
  - `bii_window`
- 선택 데이터셋 키:
  - `apf_action_rules`

## 4) 실행 방법
1. 브라우저에서 `insights.html`을 직접 엽니다.
2. 앱이 `data/` 경로 CSV를 자동 로드해 IndexedDB를 동기화합니다.
3. 누락 파일만 사이드바의 **Upload Data** 버튼으로 업로드합니다.
4. 좌측 메뉴에서 페이지를 이동하며 확인합니다.

## 5) 현재 동작 메모
- IndexedDB 데이터가 없으면 업로드 안내 화면이 표시됩니다.
- 페이지는 `body` id 기준으로 분기되고, `app.js` 하나에서 렌더링됩니다.
- `index.html`은 기본적으로 `insights.html`로 리다이렉트되며, Overview는 `index.html?overview=1`로 접근합니다.
- Cart 상세 테이블은 검색/정렬/페이지네이션을 지원합니다.
- Products 테이블은 클립보드 복사 및 연관상품 모달을 지원합니다.

## 6) 제약 / 비목표
- 내부용 기준으로, 가독성과 시연 속도를 우선합니다.
- API 연동, 인증, 배포 파이프라인은 없습니다.
- 테스트 스위트/CI는 현재 구성되어 있지 않습니다.

## 7) 안전한 변경 가이드
- 명시적 요청이 없으면 정적 구조(HTML + JS + CSS)를 유지합니다.
- CSV 키 이름과 업로드 매칭 로직은 유지합니다.
- 대규모 구조 변경보다 점진적 UI/로직 개선을 우선합니다.
- 문서 추가 시 온보딩 중심으로 간결하게 작성합니다.

## 8) 권장 추가 문서 (선택)
- `README.md`: 최초 사용자용 빠른 시작 가이드
- `DATA_SCHEMA.md`: CSV별 기대 컬럼 정의
- `CHANGELOG.md`: 시연/실험 변경 이력 관리
