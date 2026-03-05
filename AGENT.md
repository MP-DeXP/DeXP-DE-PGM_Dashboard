# 마케팅 분석 대시보드 에이전트 컨텍스트

## 1) 프로젝트 목적
- 내부 테스트/시연용 브라우저 대시보드입니다.
- 운영(프로덕션) 목적이 아닙니다.
- 복잡한 모델 용어보다 실무자가 바로 이해하는 화면을 우선합니다.

## 2) 기술/구조
- 프런트엔드 단독 구조(백엔드 없음, 빌드 단계 없음).
- 주요 파일:
  - `insights.html` (인사이트 스튜디오, 기본 진입)
  - `index.html` (요약 현황)
  - `products.html` (상품 분석)
  - `transitions.html` (전환 흐름)
  - `cart.html` (장바구니 분석)
  - `app.js` (전체 로직)
  - `style.css` (공통 스타일)

## 3) 데이터 로딩
- 소스: `data/` 자동 로드 + 설정의 수동 업로드
- 파싱: PapaParse (`header: true`, `dynamicTyping: true`)
- 저장: IndexedDB (`PGM_Dashboard_DB`, store: `csv_files`)
- 선택 키:
  - `product_group_map`

## 4) 하드룰
- UI/툴팁은 쉬운 한국어 해요체를 사용합니다.
- UI/툴팁에서 내부 약어/전문용어(AA/PCA/CA/BHI/BII/PGM 등)는 노출하지 않습니다.
- 내부 코드 키/CSV 컬럼은 호환성 때문에 유지합니다.

## 5) 현재 동작 메모
- 사이드바 하단 `설정`에서 다음을 제공합니다.
  - 상품 그룹 관리
  - 데이터 관리(CSV 업로드, 로컬 동기화, 저장 데이터 초기화)
- 상품 상태 4분면은 `집중뷰(기본)`와 `원본 보기` 토글을 지원합니다.
- 그룹 저장 후 Products/Transitions/Cart/Insights 전체 집계에 반영됩니다.
- URL 포커스 파라미터 지원:
  - `products.html?focus=<id>`
  - `transitions.html?focus=<id>`
  - `cart.html?focus=<id>`

## 6) 제약
- 큰 구조 변경보다 시연 가독성과 반응성을 우선합니다.
- API 연동/인증/배포 파이프라인은 없습니다.
