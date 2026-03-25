# 제품 관계 인사이트 페이지 UX 명세 | Product Insights Page UX Spec

**버전 Version:** 1.0
**작성일 Date:** 2026-03-25
**대상 독자 Audience:** 기획자, UI/UX 디자이너, 프론트엔드 개발자
**관련 파일 Related files:** `apps/products/index.html`, `products.js`

---

## 목차 | Table of Contents

1. [페이지 개요 | Page Overview](#1-페이지-개요--page-overview)
2. [진입 경로 | Entry Points](#2-진입-경로--entry-points)
3. [전역 상태 | View State](#3-전역-상태--view-state)
4. [레이아웃 구조 | Layout Structure](#4-레이아웃-구조--layout-structure)
5. [제품 테이블 | Product Table](#5-제품-테이블--product-table)
6. [차트 영역 | Chart Area](#6-차트-영역--chart-area)
7. [4분면 차트 | Quadrant Chart](#7-4분면-차트--quadrant-chart)
8. [사이드 패널 | Side Panel](#8-사이드-패널--side-panel)
9. [수요 그래프 모달 | Demand Graph Modal](#9-수요-그래프-모달--demand-graph-modal)
10. [설정 메뉴 | Settings Menu](#10-설정-메뉴--settings-menu)
11. [용어 규칙 | Terminology Rules](#11-용어-규칙--terminology-rules)
12. [빈 상태 및 오류 처리 | Empty States & Error Handling](#12-빈-상태-및-오류-처리--empty-states--error-handling)
13. [반응형 및 렌더링 제약 | Responsive & Rendering Constraints](#13-반응형-및-렌더링-제약--responsive--rendering-constraints)

---

## 1. 페이지 개요 | Page Overview

**페이지 제목:** 제품 관계 인사이트
**사이드바 메뉴:** 제품 관계 인사이트 (아이콘: `ph-tag`)
**HTML 엔트리:** `apps/products/index.html`
**`<body>` ID:** `page-products`

### 목적 Purpose

이 페이지는 제품들 사이의 수요 구조 관계를 시각화하고, 기획자가 각 제품의 역할(신규 유입 / 재구매 연결)을 파악해 마케팅·운영 의사결정을 내릴 수 있도록 돕는다.

단순 판매량 순위가 아니라 **수요 구조** 관점(첫구매 유입 강점 / 재구매 연결 강점)으로 제품을 분류한다.

---

## 2. 진입 경로 | Entry Points

| 진입 방법 | 설명 |
|---|---|
| 일반 진입 | `apps/products/index.html` 직접 접근 |
| 딥링크 (제품 포커스) | `apps/products/?focus=<product_id>` |

### 딥링크 동작 Deeplink Behavior

- `?focus=<product_id>` 파라미터가 존재하면 페이지 로드 완료 직후 해당 제품을 자동 선택한다.
- 선택 시 사이드 패널이 열리고 4분면 차트가 해당 제품에 포커스된다.
- 유효하지 않은 `product_id`가 전달되면 포커스 없이 기본 상태로 로드된다.

---

## 3. 전역 상태 | View State

모든 뷰 상태는 `AppState.viewState.products` 네임스페이스 아래에 관리된다.

### 3.1 기본 상태 Default State

| 키 Key | 타입 Type | 기본값 Default | 설명 Description |
|---|---|---|---|
| `sortCol` | string | `'revenue_90d'` | 제품 테이블 정렬 기준 컬럼 |
| `sortDesc` | boolean | `true` | 정렬 방향 (true = 내림차순) |
| `coreSortKey` | string | `'entry'` | 핵심 제품 탭 기본 선택 (`'entry'` \| `'expansion'`) |
| `searchQuery` | string | `''` | 제품 검색어 |
| `chartView` | string | `'quadrant'` | 차트 뷰 모드 (`'quadrant'` \| `'demand-graph'`) |
| `demandGraphTab` | string | `'transition'` | 수요 그래프 탭 (`'transition'` \| `'basket'`) |
| `sidePanelOpen` | string \| null | `null` | 현재 열린 사이드 패널의 product_id (없으면 null) |

### 3.2 4분면 차트 상태 Quadrant State

| 키 Key | 타입 Type | 기본값 Default | 설명 Description |
|---|---|---|---|
| `quadrant.selectedId` | string | `''` | 현재 선택된 제품 ID |
| `quadrant.history` | string[] | `[]` | 선택 이력 (뒤로가기 지원용) |
| `quadrant.scaleMode` | string | `'focus'` | 축 스케일 모드 (`'focus'` \| `'raw'`) |
| `quadrant.edgeMode` | string | `'representative'` | 엣지 표시 모드 (`'representative'` \| `'convergence'`) |

---

## 4. 레이아웃 구조 | Layout Structure

```
app-container
├── sidebar (nav.sidebar)
│   ├── sidebar-header (로고 + 접기 버튼)
│   ├── nav-links
│   │   ├── 구매 활성도 (→ apps/brand/)
│   │   └── 제품 관계 인사이트 [active]
│   └── sidebar-bottom
│       └── 설정 (#settings-trigger)
└── main-content
    ├── top-bar (GNB)
    │   ├── gnb-left: ph-lightbulb 아이콘 + "제품 관계 인사이트"
    │   └── gnb-right: 회사 배지 + 유저 아바타
    └── #content-area
        ├── #loading-indicator (로딩 중 표시)
        ├── 검색바 + 차트뷰 전환 토글
        ├── [왼쪽] 제품 테이블 패널
        └── [오른쪽] 차트 영역
            ├── 4분면 차트 (chartView = 'quadrant')
            └── 수요 그래프 진입 카드 (chartView = 'demand-graph')
```

### 사이드 패널 Side Panel

사이드 패널은 제품 선택 시 오버레이 또는 우측 슬라이드인 방식으로 열린다. `sidePanelOpen`에 선택된 `product_id`가 저장된다.

---

## 5. 제품 테이블 | Product Table

### 5.1 개요

- 기본 정렬: `revenue_90d` 내림차순
- 상위 80% 누적 점유 기준(gravity share) 이상의 제품을 핵심 제품 구간으로 표시한다.
- 검색어가 있으면 `product_id`, `product_name_latest`, `member_ids` 필드를 기준으로 필터링한다.

### 5.2 탭 구성 Sort Tabs

| 탭 이름 | `coreSortKey` 값 | 정렬 기준 |
|---|---|---|
| 첫구매 유입 | `'entry'` | Entry Gravity 점수 내림차순 |
| 재구매 확장 | `'expansion'` | Expansion Gravity 점수 내림차순 |

기본 선택 탭: **첫구매 유입** (`entry`)

### 5.3 테이블 컬럼 Columns

| 컬럼 표시명 | 데이터 필드 | 비고 |
|---|---|---|
| 제품명 | `product_name_latest` | 2줄 초과 시 말줄임. 클릭 시 전체명·ID 확인 가능 |
| 제품 ID | `product_id` | 보조 표시 |
| 첫구매 유입 점수 | `entry_score` (Entry Gravity) | 해당 탭 활성 시 강조 |
| 재구매 확장 점수 | `expansion_score` (Expansion Gravity) | 해당 탭 활성 시 강조 |
| 90일 매출 | `revenue_90d` | 기본 정렬 기준 |
| 등급 배지 | 4분면 zone key | hero / entry-only / expansion-only / phaseout |

> **UI 규칙:** 컬럼 헤더에 내부 약어(AA, PCA, PGM 등)를 노출하지 않는다.

### 5.4 행 클릭 동작 Row Click

테이블 행을 클릭하면 4분면 차트의 버블 클릭과 동일하게 동작한다.

- `quadrant.selectedId`를 해당 `product_id`로 업데이트
- 사이드 패널 열림 (`sidePanelOpen` = `product_id`)
- 4분면 차트가 해당 제품에 포커스

### 5.5 검색 Search

| 필드 | 검색 적용 여부 |
|---|---|
| `product_id` | O |
| `product_name_latest` | O |
| `member_ids` | O |

검색어 입력 시 실시간 필터링. 검색어 초기화 버튼 제공.

---

## 6. 차트 영역 | Chart Area

### 6.1 차트 뷰 전환 Chart View Toggle

| 값 | 표시명 | 설명 |
|---|---|---|
| `'quadrant'` | 4분면 | 제품을 첫구매 유입 × 재구매 확장 축으로 분류한 산점도 |
| `'demand-graph'` | 수요 그래프 | 제품 간 구매 연결 흐름 모달 진입 카드 |

`chartView` 상태값으로 전환. 기본값: `'quadrant'`

---

## 7. 4분면 차트 | Quadrant Chart

### 7.1 차트 규격 Dimensions

| 속성 | 값 |
|---|---|
| SVG 프레임 | 708 × 585 px |
| 최소 렌더 너비 | 360 px |

최소 렌더 너비(360px) 미만인 경우 차트 렌더링을 중단하고 안내 메시지를 표시한다.

### 7.2 축 정의 Axes

| 축 | 의미 | 데이터 필드 |
|---|---|---|
| X축 (가로) | 첫구매 유입 강점 | `entry_score` (Entry Gravity Score) |
| Y축 (세로) | 재구매 확장 강점 | `expansion_score` (Expansion Gravity Score) |

**구역 기준선 Zone Center:**
전체 제품 점수(뷰포트 클리핑 전 raw 기준)의 중앙값(median)이 X/Y 분할선이 된다.

### 7.3 스케일 모드 Scale Mode

| 모드 | `scaleMode` 값 | 설명 |
|---|---|---|
| 집중뷰 | `'focus'` | p5~p95 범위로 뷰포트를 좁혀 제품 분포를 확대해서 보여줌. 범위 밖 이상치는 엣지 마커(▶ 등)로 표시 |
| 원본 보기 | `'raw'` | 전체 데이터 범위를 그대로 표시 |

**기본값:** `'focus'` (집중뷰)

> 집중뷰에서 아웃라이어가 발생하면 해당 방향 가장자리에 마커를 표시하고, 클릭 시 해당 제품을 선택할 수 있다.

### 7.4 4분면 구역 Zone Definition

구역 기준: `getQuadrantStatus(entry, expansion, centerEntry, centerExpansion)`

| 구역 키 | 표시 레이블 | 색상 | 조건 |
|---|---|---|---|
| `hero` | 우선 확대 대상 | `#1e85fb` (파랑) | highEntry AND highExpansion |
| `entry-only` | 첫구매 강점 제품 | `#6bba25` (초록) | highEntry AND NOT highExpansion |
| `expansion-only` | 재구매 강점 제품 | `#a765fb` (보라) | NOT highEntry AND highExpansion |
| `phaseout` | 개선 필요 | `#f45151` (빨강) | NOT highEntry AND NOT highExpansion |

- `highEntry` = 해당 제품 entry_score >= 전체 중앙값(centerEntry)
- `highExpansion` = 해당 제품 expansion_score >= 전체 중앙값(centerExpansion)

### 7.5 구역별 전략 액션 Zone Actions

각 구역에는 기획자가 바로 참조할 수 있는 전략 가이드가 제공된다. 사이드 패널 및 툴팁에 표시된다.

**우선 확대 대상 (hero)**
- 핵심 지면과 캠페인에서 상시 노출해 성장 모멘텀을 키워요.
- 재고와 배송 가용성을 우선 보호해 품절 손실을 줄여요.

**첫구매 강점 제품 (entry-only)**
- 첫구매 직후 재구매 유도 번들/세트를 전면 배치해 연결을 강화해요.
- 첫 구매 후 3~7일 CRM 리마인드로 다음 구매 전환을 높여요.

**재구매 강점 제품 (expansion-only)**
- 신규 유입 채널과 크리에이티브를 확장해 첫구매 모수를 늘려요.
- 첫구매 강점 제품과의 동시 노출로 유입 구간을 보강해요.

**개선 필요 (phaseout)**
- 가격·구성·메시지 개선 실험으로 반응 회복 가능성을 먼저 확인해요.
- 개선 반응이 낮으면 축소 또는 대체 제품으로 전환해요.

### 7.6 엣지 모드 Edge Mode

버블(제품) 선택 시 해당 제품과의 전이 관계를 엣지(선)로 표시한다.

| 모드 | `edgeMode` 값 | 표시 레이블 | 안내 문구 |
|---|---|---|---|
| 전이 흐름 | `'representative'` | 전이 흐름 | 이 제품에서 이어지거나 이 제품으로 넘어오는 전이 흐름을 보여줘요. |
| 도착 흐름 | `'convergence'` | 도착 흐름 | 이 제품이 다른 제품 다음에 자주 선택되는지 보여줘요. |

**기본값:** `'representative'` (전이 흐름)

**렌더링 상수:**

| 상수명 | 값 | 설명 |
|---|---|---|
| `QUADRANT_CONVERGENCE_EDGE_TOP_N` | 8 | 도착 흐름 모드에서 표시할 최대 엣지 수 |
| `QUADRANT_RETURN_LOOP_TOP_N` | 4 | 리턴 루프 표시 최대 수 |

**엣지 미사용 상태:**
- `emptyGuide` (전이 흐름): '이 제품과 이어지는 전이 흐름이 아직 많지 않아요.'
- `emptyGuide` (도착 흐름): '이 제품이 다음 구매로 자주 이어지는 흐름은 아직 많지 않아요.'
- `unavailableGuide` (전이 흐름): '전이 흐름을 보여줄 데이터가 아직 준비되지 않았어요.'
- `unavailableGuide` (도착 흐름): '도착 흐름을 보여줄 상세 전이 데이터가 아직 준비되지 않았어요.'

### 7.7 버블 선택 및 히스토리 Bubble Selection & History

- 버블 클릭: `quadrant.selectedId` 업데이트, `quadrant.history`에 이전 ID 추가
- 뒤로가기: `quadrant.history` 스택에서 pop하여 이전 선택으로 복귀
- 선택 해제: 배경 클릭 또는 사이드 패널 닫기 버튼

---

## 8. 사이드 패널 | Side Panel

제품 버블 또는 테이블 행 클릭 시 열린다. `sidePanelOpen` = 선택된 `product_id`.

### 8.1 헤더 Header

- 제품명 (`product_name_latest`) — 전체 표시 (말줄임 없음)
- 제품 ID (`product_id`)
- 4분면 구역 배지 (zone label + 색상)
- 닫기 버튼 (× )

### 8.2 핵심 지표 Key Metrics

| 지표명 | 계산식 | 설명 |
|---|---|---|
| 주간 예상 수요량 | `product_order_cnt_1y / 52` | 연간 주문량을 주 단위로 환산 |
| 구매 지속 가능성 | `repurchase_customer_cnt_90d / (first_customer_cnt + repurchase_customer_cnt_90d)` | 90일 기준 재구매 비중 |
| 첫구매 기여 비중 | `해당 제품 first_customer_cnt / 전체 제품 first_customer_cnt 합산` | 전체 대비 이 제품의 신규 유입 비중 |
| 재구매 기여 비중 | `해당 제품 repurchase_customer_cnt_90d / 전체 제품 repurchase_customer_cnt_90d 합산` | 전체 대비 이 제품의 재구매 비중 |

### 8.3 구역 가이드 Zone Guide

해당 제품의 4분면 구역(zone)에 해당하는 전략 가이드를 표시한다. ([7.5 구역별 전략 액션](#75-구역별-전략-액션-zone-actions) 참조)

### 8.4 CTA 버튼 | CTA Button

| 버튼명 | 동작 | 비활성화 조건 |
|---|---|---|
| 추가구매 제품 보기 | 수요 그래프 모달 열기 (해당 제품 포커스) | `product_id`가 `buildTransitionEntitySet`에 포함되지 않을 때 |

비활성화 시 버튼은 disabled 상태로 표시되며, 연결 데이터가 없음을 안내한다.

---

## 9. 수요 그래프 모달 | Demand Graph Modal

`chartView = 'demand-graph'` 전환 또는 사이드 패널 CTA 클릭 시 진입.

### 9.1 탭 구성 Tabs

| 탭 표시명 | `demandGraphTab` 값 | 안내 문구 | 표시 항목 수 |
|---|---|---|---|
| 구매 전이 | `'transition'` | 이 제품 전후로 자주 이어지는 구매 관계를 보여줘요. | 전·후 각 최대 5개 (`DEMAND_GRAPH_TRANSITION_LIMIT = 5`) |
| 장바구니 조합 | `'basket'` | 이 제품과 함께 담기는 관계를 보여줘요. | 최대 6개 (`DEMAND_GRAPH_BASKET_LIMIT = 6`) |

### 9.2 요약 리스트 Summary List

탭 하단에 관련 제품 요약 목록 표시.
`DEMAND_GRAPH_SUMMARY_LIMIT = 5` (최대 5개)

각 항목: 제품명, 전이율 또는 동시 구매 빈도, 연결 방향 표시

### 9.3 빈 상태 Empty States

| 상태 | 표시 문구 |
|---|---|
| 구매 전이 데이터 없음 | 이 제품과 자주 이어지는 연결은 아직 많지 않아요. |
| 장바구니 조합 데이터 없음 | 이 제품과 자주 함께 담기는 조합은 아직 많지 않아요. |
| 구매 전이 데이터 미준비 | 주변 연결 흐름을 보여줄 상세 패턴 데이터가 아직 준비되지 않았어요. |
| 장바구니 조합 데이터 미준비 | 장바구니 조합을 보여줄 패턴 데이터가 아직 준비되지 않았어요. |

### 9.4 그래프 데이터 소스 Data Sources

| 탭 | 사용 CSV | 설명 |
|---|---|---|
| 구매 전이 | `_insight_demand_graph_edges.csv` | 큐레이션된 전이 엣지 |
| 장바구니 조합 | `_insight_demand_graph_patterns.csv` (`pattern_type = 'basket_pair'`) | 동시 구매 패턴 |
| 공통 노드 정보 | `_insight_demand_graph_nodes.csv` | 제품명, 역할, 크기 점수 등 |

---

## 10. 설정 메뉴 | Settings Menu

사이드바 하단 **설정** 항목 클릭 시 열림 (`#settings-trigger`).

### 10.1 제품 그룹 설정 Product Group Settings

같은 제품의 다른 ID를 묶거나 해제하는 3단계 마법사:

1. **대상 선택:** 묶을(또는 해제할) 제품 선택
2. **작업 선택:** 그룹 생성 / 해제 선택
3. **검토 및 저장:** 변경 내용 확인 후 저장

### 10.2 데이터 관리 Data Management

| 기능 | 설명 |
|---|---|
| CSV 업로드 | 새 데이터 파일 업로드 |
| 로컬 다시 불러오기 | 로컬에 저장된 데이터 재로드 |
| 저장 데이터 초기화 | 로컬 저장 데이터 전체 삭제 |

---

## 11. 용어 규칙 | Terminology Rules

### 11.1 하드룰 Hard Rules

1. **해요체 사용:** 모든 UI 문구, 툴팁, 안내 문구는 해요체로 작성한다.
   예: "보여줘요.", "필요해요.", "확인해요."

2. **내부 약어 노출 금지:** 아래 내부 코드명/약어는 UI 및 툴팁에 표시하지 않는다.

| 내부 약어 | 대체 표현 |
|---|---|
| AA | 첫구매 유입 (Entry Gravity) |
| PCA | 재구매 확장 (Expansion Gravity) |
| CA | 장바구니 확장 (Basket Gravity) |
| BHI | (브랜드 구조 관련 내부 지표, 화면 미노출) |
| BII | (브랜드 실전 관련 내부 지표, 화면 미노출) |
| PGM | (내부 모델명, 화면 미노출) |

### 11.2 권장 UI 표현 Preferred UI Terminology

| 개념 | 표시 용어 |
|---|---|
| Entry Gravity | 첫구매 유입 |
| Expansion Gravity | 재구매 확장 / 재구매 연결 |
| Basket Gravity | 장바구니 확장 |
| 집중뷰 (focus scale) | 집중뷰 |
| 원본 보기 (raw scale) | 원본 보기 |
| 전이 흐름 (representative edge) | 전이 흐름 |
| 도착 흐름 (convergence edge) | 도착 흐름 |

---

## 12. 빈 상태 및 오류 처리 | Empty States & Error Handling

### 12.1 로딩 상태 Loading State

- `#loading-indicator` (`.loading`) 표시
- 문구: "제품 데이터를 불러오는 중..."

### 12.2 데이터 없음 No Data

| 상황 | 처리 |
|---|---|
| 검색 결과 없음 | 검색어에 맞는 제품이 없음을 안내. 검색어 초기화 버튼 제공 |
| 전이 엣지 없음 | 차트에서 엣지 미표시. 해당 엣지 모드 안내 문구 표시 |
| 사이드 패널 지표 계산 불가 | '-' 로 표시 |

### 12.3 딥링크 product_id 미존재

`?focus=<product_id>`의 제품이 데이터에 없으면 포커스 없이 기본 상태로 로드하고, 별도 오류 메시지를 표시하지 않는다.

---

## 13. 반응형 및 렌더링 제약 | Responsive & Rendering Constraints

| 항목 | 값 / 동작 |
|---|---|
| 4분면 SVG 프레임 | 708 × 585 px (고정 비율) |
| 최소 렌더 너비 | 360 px — 미만 시 차트 렌더 중단 및 안내 표시 |
| 제품명 표시 | 테이블에서 최대 2줄, 초과 시 말줄임(`...`) |
| 폰트 | Pretendard Variable (CDN 로드) |
| 아이콘 라이브러리 | Phosphor Icons (`@phosphor-icons/web`) |

---

## 부록 A. 주요 데이터 필드 참조 | Appendix A: Key Data Fields

| 필드명 | 출처 파일 | 설명 |
|---|---|---|
| `product_id` | `pgm_scored.csv` | 제품 고유 ID |
| `product_name_latest` | `pgm_scored.csv` | 제품 최신 이름 |
| `entry_score` | `pgm_product_demand_gravity.csv` | Entry Gravity Score |
| `expansion_score` | `pgm_product_demand_gravity.csv` | Expansion Gravity Score |
| `revenue_90d` | `pgm_scored.csv` | 90일 매출 |
| `product_order_cnt_1y` | `pgm_scored.csv` | 1년 주문 건수 |
| `first_customer_cnt` | `pgm_scored.csv` | 첫구매 고객 수 |
| `repurchase_customer_cnt_90d` | `pgm_scored.csv` | 90일 재구매 고객 수 |
| `member_ids` | `pgm_scored.csv` | 연결된 회원 ID 목록 |

---

## 부록 B. 상수 요약 | Appendix B: Constants Reference

| 상수명 | 값 | 위치 |
|---|---|---|
| `QUADRANT_SVG_FRAME` | `{ width: 708, height: 585 }` | `products.js` |
| `QUADRANT_MIN_RENDER_WIDTH` | `360` | `products.js` |
| `QUADRANT_CONVERGENCE_EDGE_TOP_N` | `8` | `products.js` |
| `QUADRANT_RETURN_LOOP_TOP_N` | `4` | `products.js` |
| `DEMAND_GRAPH_TRANSITION_LIMIT` | `5` | `products.js` |
| `DEMAND_GRAPH_BASKET_LIMIT` | `6` | `products.js` |
| `DEMAND_GRAPH_SUMMARY_LIMIT` | `5` | `products.js` |

---

## 부록 C. 4분면 구역 컬러 팔레트 | Appendix C: Zone Color Palette

| 구역 | 레이블 | HEX | 사용처 |
|---|---|---|---|
| `hero` | 우선 확대 대상 | `#1e85fb` | 버블 색상, 배지, 사이드 패널 헤더 강조 |
| `entry-only` | 첫구매 강점 제품 | `#6bba25` | 버블 색상, 배지 |
| `expansion-only` | 재구매 강점 제품 | `#a765fb` | 버블 색상, 배지 |
| `phaseout` | 개선 필요 | `#f45151` | 버블 색상, 배지 |

---

*이 문서는 `products.js`, `apps/products/index.html`, `docs/마케터_용어_가이드.md` 및 관련 technical spec 문서를 기반으로 작성되었습니다.*
