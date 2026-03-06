# PGM 대시보드

내부 테스트/시연용 브라우저 대시보드입니다.
복잡한 내부 용어 대신, 마케터/MD가 바로 이해할 수 있는 쉬운 문장으로 구성했습니다.

## 핵심 목적
- 첫구매 이후 90일 흐름을 빠르게 확인해 의사결정하기
- 상품/리텐션/장바구니/브랜드 건강도를 한 화면 흐름으로 보기
- 지표를 실행 카드로 바로 연결하기

## 페이지 구성
- `insights.html`: 인사이트 스튜디오 (기본 진입)
- `index.html?overview=1`: 대시보드 개요
- `products.html`: 상품 분석 (상품 상태 4분면 포함)
- `transitions.html`: 리텐션 흐름

참고:
- `index.html`을 직접 열면 `insights.html`로 자동 이동합니다.
- `cart.html`은 호환용 리다이렉트 페이지이며 `products.html`로 이동합니다.

## 실행 방법
1. 브라우저에서 `insights.html` 파일을 직접 엽니다.
2. 앱 시작 시 `data/` 폴더 CSV를 자동으로 다시 읽습니다.
3. 누락 파일이 있으면 사이드바 하단 `설정 > 데이터 관리 > CSV 업로드`로 추가합니다.

## 설정 메뉴
사이드바 하단 `설정`에서 아래 기능을 제공합니다.
- 상품 그룹 관리(3단계 마법사: 대상 선택 → 작업 선택 → 검토/저장)
- 데이터 관리(CSV 업로드 / 로컬 파일 다시 불러오기 / 저장 데이터 초기화)

## 4분면 보기
- 기본 모드: `집중뷰` (p5~p95 구간 중심)
- 전환 모드: `원본 보기` (전체 범위)
- 사분면 판정은 항상 원본 중앙값 기준으로 동일합니다.
- 버블 크기: `product_order_cnt_1y / 52` (주간 예상 판매량)

## 상품 그룹 매핑
- 선택 파일 키: `product_group_map`
- 권장 파일명: `pgm_product_group_map.csv`
- alias: `product_group_map.csv`, `_meta_product_group_map.csv`
- 우선순위:
  1. `data/pgm_product_group_map.csv`
  2. IndexedDB 저장값
  3. 자동 제안(동일명 + 접두어 제거)

## 업로드 CSV 키
- `brand_score`
- `anchor_scored`
- `anchor_transition`
- `cart_anchor`
- `cart_anchor_detail`
- `aa_cohort_journey`
- `aa_transition_path`
- `ca_profile`
- `bii_window`
- `apf_action_rules` (선택)
- `product_group_map` (선택)

## 딥링크 포커스
- `products.html?focus=<id>`
- `transitions.html?focus=<id>`
- `cart.html?focus=<id>` → `products.html?focus=<id>`로 자동 전달

## 용어/호환성
- UI/툴팁에서는 내부 약어와 어려운 용어를 사용하지 않습니다.
- CSV 파일명/컬럼명/코드 내부 데이터 키는 기존 호환성을 유지합니다.
