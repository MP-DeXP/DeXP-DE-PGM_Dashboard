# PGM 대시보드

내부 테스트/시연용 브라우저 대시보드입니다.
복잡한 내부 용어 대신, 마케터/MD가 바로 이해할 수 있는 쉬운 문장으로 구성했습니다.

## 핵심 목적
- 구매 구조와 브랜드 상태를 빠르게 확인해 의사결정하기
- 브랜드 구조와 제품 관계 인사이트를 두 화면 흐름으로 보기
- 지표를 실행 카드와 상세 모달로 바로 연결하기

## 페이지 구성
- `apps/brand/`: 브랜드 구조 대시보드
- `apps/products/`: 제품 관계 인사이트 (제품 상태 4분면, 리텐션/장바구니 모달 포함)
- `apps/pgm_ops/`: PGM 1.0 운영 상태판 (frontend-only, artifact-driven, sample fallback은 검증용 예시 전용)
- `apps/decision-dashboard/`: 의사결정 대시보드 (mock-only 세일즈 프로토타입)

## 실행 방법
1. 브라우저에서 `apps/products/`, `apps/brand/`, `apps/pgm_ops/`, 또는 `apps/decision-dashboard/`를 직접 엽니다.
2. `apps/products/`와 `apps/brand/`는 앱 시작 시 `data/` 폴더 CSV를 자동으로 다시 읽습니다.
3. `apps/pgm_ops/`는 `artifacts/view_model/*.csv`를 우선 읽고, 브라우저에서 읽지 못할 때만 내장 sample fallback으로 전환합니다. 빈 artifact를 sample로 대체하지는 않습니다.
4. 누락 파일이 있으면 사이드바 하단 `설정 > 데이터 관리 > CSV 업로드`로 추가합니다.

`apps/pgm_ops/README.md`와 `apps/pgm_ops/docs/`에 blank-rule, deferred scope, handoff checklist가 정리되어 있습니다.

## 의사결정 대시보드 메모
- `apps/decision-dashboard/`는 CSV 업로드 없이 바로 열리는 mock prototype입니다.
- `Products`의 디자인 언어를 재사용하되, 기존 `Products` 앱 코드는 수정하지 않습니다.

## 설정 메뉴
사이드바 하단 `설정`에서 아래 기능을 제공합니다.
- 제품 그룹 관리(3단계 마법사: 대상 선택 → 작업 선택 → 검토/저장)
- 데이터 관리(CSV 업로드 / 로컬 파일 다시 불러오기 / 저장 데이터 초기화)

## 4분면 보기
- 기본 모드: `집중뷰` (p5~p95 구간 중심)
- 전환 모드: `원본 보기` (전체 범위)
- 사분면 판정은 항상 원본 중앙값 기준으로 동일합니다.
- 버블 크기: `product_order_cnt_1y / 52` (주간 예상 판매량)

## 제품 그룹 매핑
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
- `bii_window`
- `brand_impact_timeseries` (선택)
- `brand_impact_daily_pulse` (선택)
- `brand_revenue_timeseries` (선택)
- `purchase_activation_driver_timeseries` (선택)
- `brand_structure_timeseries` (선택)
- `product_group_map` (선택)

## 딥링크 포커스
- `apps/products/?focus=<id>`

## 용어/호환성
- UI/툴팁에서는 내부 약어와 어려운 용어를 사용하지 않습니다.
- CSV 파일명/컬럼명/코드 내부 데이터 키는 기존 호환성을 유지합니다.

## 브랜드 페이지 메모
- 브랜드 페이지 상단은 `Hero = 상태`, `Brand Purchase Drivers = 원인` 구조로 구성합니다.
- `purchase_activation_driver_timeseries`가 있으면 Hero/Drivers를 모두 렌더하고, 없으면 Hero는 축소 버전으로 표시하며 Drivers는 빈 상태 설명만 보여 줍니다.
- 구버전 파일명 `brand_purchase_driver_timeseries.csv`도 alias로 계속 지원합니다.
- `brand_structure_timeseries`가 있으면 판매 구조 4카드는 선택한 `7/30/90일` 기준으로 함께 바뀝니다.
- `brand_structure_timeseries`가 없으면 판매 구조 섹션은 현재 구조 기준 fallback 계산으로 보여 주고, 화면에 그 기준을 명시합니다.
- 브랜드 페이지에서는 현재 브랜드 상태 진단에 집중하기 위해 제품별 기여 목록, 수요 흐름 스냅샷, 수요 구조 맵 UI를 메인 화면에서 숨기고 `apps/products/`로 이동하도록 구성합니다.
- `brand.js` 안의 제품별 기여/흐름/구조 맵 계산 및 렌더링 코드는 향후 재활성화 가능성을 위해 유지합니다.
