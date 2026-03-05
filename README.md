# APF Dashboard

내부 테스트/시연용 APF 분석 대시보드입니다.  
마케터/MD가 바로 이해할 수 있도록 화면 용어를 실무형 한글로 정리했습니다.

## 핵심 목적
- 마케팅 담당자와 MD가 한 화면에서 함께 의사결정
- 첫구매 유입 이후 90일 전환 최적화
- 브랜드 구조 건강도/브랜드 실전 체력/장바구니 확장 신호를 실행 카드로 연결

## 페이지 구성
- `insights.html`: 인사이트 스튜디오 (기본 진입)
- `index.html?overview=1`: 요약 현황
- `products.html`: 상품 분석
- `transitions.html`: 전환 흐름
- `cart.html`: 장바구니 확장

참고:
- `index.html`을 직접 열면 `insights.html`로 자동 이동합니다.
- 요약 현황은 `index.html?overview=1`로 접근합니다.

## 실행 방법
1. 브라우저에서 `insights.html` 파일을 직접 엽니다.
2. 앱이 시작될 때 `data/` CSV를 최우선으로 다시 읽어 IndexedDB 값을 덮어씁니다.
3. `data/`에 없는 파일은 루트 경로 CSV를 자동 탐색해 보충 로드합니다.
4. 자동 로드에서 누락된 파일만 사이드바 `데이터 업로드`로 추가 업로드합니다.
5. 필터(유입 유형/유입 상품/비교 기준 기간)로 인사이트를 탐색합니다.

## 업로드 CSV 키
파일명에 아래 키를 포함해야 자동 매칭됩니다.

PGM canonical 파일명:
- `brand_score` (`brand_score.csv`)
- `anchor_scored` (`pgm_scored.csv`)
- `anchor_transition` (`pgm_entry_to_expansion_transition.csv`)
- `cart_anchor` (`pgm_basket_gravity.csv`)
- `cart_anchor_detail` (`pgm_basket_gravity_detail.csv`)

인사이트 canonical 파일명:
- `aa_cohort_journey` (`_insight_entry_cohort_journey.csv`)
- `aa_transition_path` (`_insight_entry_transition_path.csv`)
- `ca_profile` (`_insight_basket_gravity_profile.csv`)
- `bii_window` (`_insight_bii_window.csv`)
- `apf_action_rules` (`_insight_pgm_action_rules.csv`, 선택)

Legacy alias 호환(업로드 가능):
- `anchor_scored.csv`, `anchor_transition.csv`, `cart_anchor.csv`, `cart_anchor_detail.csv`
- `_insight_aa_cohort_journey.csv`, `_insight_aa_transition_path.csv`, `_insight_ca_profile.csv`, `_insight_apf_action_rules.csv`
- `brand_impact_windows.csv`, `brand_impact_index.csv`는 업로드 시 `bii_window` 포맷으로 자동 변환됩니다.

## 데이터 처리 전략
- 무거운 계산: 사전 집계 CSV로 제공
- 가벼운 파생 계산: 브라우저에서 실시간 계산
- 저장소: IndexedDB (`APF_Dashboard_DB` / `csv_files`)

## 인사이트 스튜디오 섹션
- 인사이트 요약: 브랜드 실전 체력, 90일 대비 연간 추세, 경고 요약
- 브랜드 체력 현황: 체력 방향/상태/즉시 액션을 먼저 확인하고, 상세는 펼쳐서 원인 분석
- 첫구매 고객 흐름: 7/30/90일 재구매 흐름
- 재구매 시작 전환 흐름: 상품 간 전환 경로/집중도/소요일
- 장바구니 확장 인사이트: 확장 유형/동반구매 비율/장바구니 크기
- 실행 카드: 마케팅/MD 실행안

## 관련 문서
- 에이전트 컨텍스트: `AGENT.md`
- 데이터 스키마: `DATA_SCHEMA.md`
- 마케터 용어 가이드: `docs/마케터_용어_가이드.md`

## 용어/호환성 참고
- 화면에는 한글 용어를 사용합니다.
- CSV 파일명/컬럼명/코드 내부 데이터 키는 기존 명칭을 유지합니다.
- 일부 CSV가 없어도 섹션 단위 안내 메시지와 함께 렌더됩니다.
