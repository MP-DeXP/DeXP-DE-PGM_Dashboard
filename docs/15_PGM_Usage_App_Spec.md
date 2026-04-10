# 목적별 활용 검토 앱 기획 명세

**내부 앱 ID / 폴더명:** `pgm_usage`  
**권장 화면/메뉴 라벨:** 목적별 활용 검토  
**상태:** PoC / planning draft  
**작성일:** 2026-04-10  
**주요 입력:** `pgm_product_purpose_effects.csv`  
**참조:** `apps/products/index.html`, `products.js`, `style.css`, `docs/10_Product_Page_UX_Spec.md`, `docs/11_Product_Page_Data_Contract.md`, `docs/14_Products_Purpose_Prototype_Handoff.md`

---

## 1. 앱의 위치

`pgm_usage`는 최종 추천 엔진이 아니라, 제품별 목적 활용 가능성과 포트폴리오 단위 운영 검토 후보를 살펴보는 별도 PoC 화면이다.

내부 구현 식별자는 `pgm_usage`로 유지한다. 다만 사용자가 보는 메뉴명, 화면 제목, 빈 상태 문구에서는 내부 약어성 이름을 직접 노출하지 않고 **목적별 활용 검토**를 기본 라벨로 쓴다.

`products`가 제품 간 수요 구조를 설명한다면, `pgm_usage`는 이미 구조화된 목적별 예상 효과 산출물을 읽고 "어떤 목적에서 어떤 제품을 검토할지"를 정리한다. 따라서 프론트엔드는 효과 판단 로직을 다시 만들지 않고, core output을 표시ㆍ필터링ㆍ비교하는 얇은 레이어로 두는 것이 맞다.

---

## 2. 왜 `products`와 분리해야 하는가

`products` 안에 패널을 더 붙이는 방식은 PoC 속도는 빠르지만, 장기적으로는 정보 구조가 흐려진다. 이 앱은 별도 화면으로 분리하는 것이 맞다.

| 이유 | 설명 |
|---|---|
| 질문이 다름 | `products`는 "이 제품은 수요 구조상 어떤 역할인가"를 묻고, `pgm_usage`는 "이 제품/목적 조합을 운영 검토 후보로 볼 수 있는가"를 묻는다. |
| 상태 모델이 다름 | `products`의 핵심 상태는 선택 제품과 차트 포커스다. `pgm_usage`의 핵심 상태는 목적, 검토 등급, 리스크/전제조건, 포트폴리오 후보 목록이다. |
| 선택 편향을 줄여야 함 | 전체 후보 비교가 현재 선택 제품에 종속되면 "전체 구조 기준 후보"가 아니라 "선택 제품 설명"으로 오해된다. 별도 앱에서는 포트폴리오 영역을 선택 상태와 독립적으로 유지할 수 있다. |
| 데이터 소유권이 다름 | 현재 목적 해석 로직은 `products.js`의 prototype UI interpretation layer다. `pgm_usage`는 `pgm_product_purpose_effects.csv` 같은 structured core output을 소비해야 한다. |
| 과도한 제품 페이지 확장을 막음 | `products`는 4분면, 수요 그래프, 그룹 편집, 사이드 패널을 이미 가진다. 목적별 활용 검토까지 넣으면 주요 작업이 서로 경쟁한다. |

정리하면, `pgm_usage`는 `products`의 하위 탭이 아니라 목적별 활용 가능성과 운영 검토 후보를 따로 보는 전용 앱이어야 한다.

---

## 3. 핵심 질문 2개

이 앱은 두 질문만 선명하게 답해야 한다.

### Q1. 선택한 제품은 목적별로 어떤 차이와 주의점이 있는가?

사용자가 제품을 선택했을 때, 해당 제품의 목적별 행을 비교한다.

- 어떤 목적은 검토 가능한가?
- 어떤 목적은 작은 실험 수준인가?
- 어떤 목적은 근거 부족 또는 비대상인가?
- 리스크 플래그나 전제조건 플래그가 있는가?
- 같은 제품 안에서 목적 간 우선순위와 주의점은 어떻게 다른가?

이 영역은 제품 선택에 따라 바뀐다.

### Q2. 전체 카탈로그에서 활용 검토 후보는 무엇인가?

선택 제품과 무관하게 전체 제품-목적 조합을 정리한다.

- 지금 검토 가능한 제품-목적 조합은 어디인가?
- 작은 실험 후보, 제한 적용 후보, 넓은 적용 후보를 어떻게 나눌 것인가?
- 리스크가 있는 후보와 전제조건이 필요한 후보는 무엇인가?
- 제외 또는 제한 가중치가 적용된 후보는 별도 검토 대상인가, 숨김 대상인가?

이 영역은 선택 제품과 독립적이어야 한다. 선택 제품은 강조 표시될 수 있지만, 후보 산정 자체를 바꾸면 안 된다.

---

## 4. 권장 정보 구조

권장 IA는 다음 하나로 고정한다. 내부 경로와 사용자가 보는 라벨을 분리해서 관리한다.

```text
apps/pgm_usage/
└── 목적별 활용 검토
    ├── 상단 컨텍스트 바
    │   ├── 화면 제목: 목적별 활용 검토
    │   ├── 데이터 상태: snapshot_name, row count, product count
    │   └── 공통 필터: 목적, 검토 범위, 근거 수준, 리스크/전제조건
    ├── 전체 후보 검토
    │   ├── 검토 요약 스트립
    │   ├── 목적별 후보 보드
    │   └── 후보 테이블
    └── 선택 상품 목적별 비교
        ├── 제품 선택/검색
        ├── 목적별 비교 카드
        └── 선택 제품의 후보 행 상세
```

라벨 대안은 다음처럼 정리한다.

| 라벨 | 판단 |
|---|---|
| **목적별 활용 검토** | **1차 권장.** 목적, 활용 가능성, 검토 성격이 모두 드러나며 내부 약어를 노출하지 않는다. |
| 상품 활용 검토 | 대안. 더 짧지만 목적별 비교라는 핵심이 약해질 수 있다. |
| 기대효과 검토 | 대안. 효과 중심으로는 명확하지만 최종 효과 판정처럼 읽히지 않도록 주의가 필요하다. |

### 대안 검토

| 대안 | 판단 |
|---|---|
| `products` 사이드 패널에 통합 | 비권장. 제품 수요 구조 해석과 활용 검토 후보가 섞이고, 전체 후보가 선택 상태에 오염될 수 있다. |
| 목적별 독립 페이지 4개 | 비권장. PoC에서 화면 수가 늘고, 제품별 비교가 어려워진다. |
| 테이블 하나로만 구성 | 비권장. 빠르게 만들 수는 있지만, "포트폴리오 후보"와 "선택 제품 비교"의 사고 흐름이 분리되지 않는다. |

선택안은 `전체 후보 검토 + 선택 상품 목적별 비교` 2영역 구조다. 이 구조가 PoC로 충분히 얇으면서도 사용자의 두 작업을 분리한다.

---

## 5. 권장 메인 레이아웃

### 데스크톱

```text
top-bar
└── 화면 제목 / company badge

content-area
├── review-toolbar (full width)
│   ├── 목적 필터
│   ├── 검토 범위 필터
│   ├── 근거 수준 필터
│   ├── 리스크/전제조건 토글
│   └── 제품 검색
├── summary-strip (full width)
│   ├── 검토 가능 후보
│   ├── 작은 실험 후보
│   ├── 리스크 포함 후보
│   └── 근거 부족/비대상
└── review-workspace
    ├── left 60-65%: 전체 후보 검토
    └── right 35-40%: 선택 상품 목적별 비교
```

권장 순서와 비중:

1. **전체 후보 검토를 먼저 배치한다.** 이 앱의 기본 작업은 포트폴리오 후보 검토다. 사용자가 제품을 고르기 전에도 의미 있는 화면이어야 한다.
2. **선택 상품 목적별 비교는 오른쪽 고정 검토 패널로 둔다.** 후보 테이블에서 행을 클릭하거나 제품 검색으로 선택하면 즉시 갱신된다.
3. **비중은 전체 후보 60-65%, 선택 상품 35-40%가 적절하다.** 후보 목록과 목적별 보드가 더 많은 수평 공간을 필요로 하고, 제품 상세는 행 단위 비교가 중심이므로 좁은 패널에서도 작동한다.

### 모바일

모바일에서는 순서를 유지하되, 선택 상품 영역은 하단 시트 또는 접이식 섹션으로 둔다.

```text
toolbar
summary-strip
전체 후보 검토
선택 상품 목적별 비교
```

모바일에서도 첫 화면은 전체 후보 검토가 먼저 와야 한다.

---

## 6. 화면 구조 상세

### 6.1 검토 툴바

역할: 전체 후보를 좁히되, 판단 로직을 바꾸지 않는다.

필터:

| 필터 | 값 |
|---|---|
| 목적 | 전체, 신규 유입/첫 구매 확대, 다음 구매 연결 강화, 다시 찾는 구매 강화, 함께 담기 확장 |
| 검토 범위 | 전체, 넓은 적용 검토, 제한 적용 검토, 작은 실험 검토, 현재 검토 제외 |
| 근거 수준 | 전체, 높음, 중간, 낮음 |
| 효과 강도 | 전체, 높음, 중간, 낮음 |
| 플래그 | 리스크 포함, 전제조건 필요, 둘 다 제외 |
| 후보 적격성 | 전체, 검토 가능, 제한 반영, 제외 |
| 제품 검색 | `product_id`, `product_name_latest` |

PoC에서는 정렬/필터만 제공하고, 사용자가 후보를 저장하거나 상태를 변경하는 흐름은 넣지 않는다.

### 6.2 요약 스트립

상단에 숫자 요약을 둔다. 단, 성과 약속처럼 보이면 안 된다.

권장 카드:

| 카드 | 계산 |
|---|---|
| 검토 가능 후보 | `effect_status in (testable, operational_candidate)` 또는 `effect_scope in (small_test, limited_rollout, broad_rollout)` |
| 작은 실험 후보 | `effect_scope = small_test` |
| 제한/넓은 적용 후보 | `effect_scope in (limited_rollout, broad_rollout)` |
| 주의 필요 후보 | `effect_risk_flag = true` 또는 `effect_precondition_flag = true` |
| 근거 부족/비대상 | `effect_status in (insufficient, not_applicable)` 또는 `effect_scope = not_recommended` |

라벨은 "추천"이 아니라 "검토 후보"를 사용한다.

### 6.3 전체 후보 검토

전체 후보 검토 영역은 두 레이어로 구성한다.

#### A. 목적별 후보 보드

목적별로 카드 4개를 둔다.

각 목적 카드:

- 목적명
- 검토 가능 후보 수
- 상위 후보 3-5개
- 각 후보의 `effect_scope`, `effect_confidence`, `effect_strength`
- 리스크/전제조건 배지
- "비교 보류" 또는 "근거 부족" 상태 표시

정렬 기준:

1. `effect_scope`: `broad_rollout` > `limited_rollout` > `small_test` > `not_recommended`
2. `effect_confidence`: `high` > `medium` > `low`
3. `effect_strength`: `high` > `medium` > `low`
4. `effect_signal_score`
5. `effect_maturity_score`

이 정렬은 "최종 추천"이 아니라 "검토 우선순위"다. UI 문구에도 그렇게 표시한다.

#### B. 후보 테이블

목적별 보드 아래 또는 같은 영역의 하단에 테이블을 둔다.

권장 컬럼:

| 컬럼 | 데이터 |
|---|---|
| 제품 | `product_name_latest`, `product_id` |
| 목적 | `purpose_key`를 한국어 라벨로 변환 |
| 검토 범위 | `effect_scope` |
| 상태 | `effect_status` |
| 예상 방향 | `effect_direction` |
| 강도 | `effect_strength` |
| 근거 수준 | `effect_confidence` |
| 주요 지표 | `effect_primary_metric`, `effect_primary_metric_value` |
| 보조 지표 | `effect_secondary_metric`, `effect_secondary_metric_value` |
| 주의 | `effect_precondition_flag`, `effect_risk_flag` |
| 상품 역할 | `merchandise_role` |

행 클릭 동작:

- 오른쪽 선택 상품 목적별 비교 패널의 제품을 해당 `product_id`로 변경
- 클릭한 `purpose_key`를 패널 안에서 강조
- 전체 후보 정렬이나 후보 산정은 바꾸지 않음

### 6.4 선택 상품 목적별 비교

선택 제품 패널은 제품 1개 안에서 목적별 차이와 주의점을 보여준다.

구성:

1. 제품 헤더
   - 제품명
   - 제품 ID
   - `snapshot_name`
   - 목적 행 수
2. 목적별 비교 카드 4개
   - 목적명
   - 검토 상태
   - 검토 범위
   - 효과 방향/강도/근거 수준
   - 주요 지표/보조 지표
   - 전제조건/리스크 배지
   - 짧은 해석 문장
3. 상세 행
   - `effect_rationale_code`는 그대로 노출하지 않고, 매핑된 설명이 있을 때만 표시
   - `effect_signal_score`, `effect_maturity_score`는 디버그 모드 또는 tooltip 수준으로 제한

선택 제품이 없을 때:

- "왼쪽 후보를 선택하거나 제품명을 검색하면 목적별 비교를 볼 수 있습니다." 정도의 빈 상태를 표시한다.
- 임의로 1위 제품을 자동 선택하지 않는다. 자동 선택은 추천처럼 보일 수 있다.

---

## 7. `pgm_product_purpose_effects.csv`로 바로 쓸 수 있는 필드

현재 validation output 기준 컬럼은 다음과 같다.

| 필드 | 바로 쓰는 방법 |
|---|---|
| `snapshot_name` | 데이터 스냅샷/브랜드 컨텍스트 표시 |
| `product_id` | 제품 선택, 테이블 row key, products 딥링크 연결 |
| `product_name_latest` | 제품명 표시 |
| `purpose_key` | 목적 필터, 목적별 그룹핑 |
| `effect_status` | 검토 상태 라벨 |
| `effect_primary_metric` | 주요 근거 지표명 |
| `effect_primary_metric_value` | 주요 근거 지표값 |
| `effect_secondary_metric` | 보조 근거 지표명 |
| `effect_secondary_metric_value` | 보조 근거 지표값 |
| `effect_direction` | 예상 방향. `increase`, `unclear`, `not_applicable` |
| `effect_strength` | 효과 강도 라벨. `high`, `medium`, `low` |
| `effect_confidence` | 근거 수준 라벨. `high`, `medium`, `low` |
| `effect_scope` | 화면에서 가장 중요한 검토 범위. `small_test`, `limited_rollout`, `broad_rollout`, `not_recommended` |
| `effect_precondition_flag` | 전제조건 필요 배지 |
| `effect_risk_flag` | 리스크 포함 배지 |
| `effect_rationale_code` | 해석 코드. v1에서는 직접 노출보다 매핑 필요 |
| `effect_signal_score` | 정렬 보조값 또는 디버그 값 |
| `effect_maturity_score` | 정렬 보조값 또는 디버그 값 |
| `merchandise_role` | 상품 역할 배지 |
| `purpose_candidate_eligibility` | 적격성 필터. `allow`, `downweight`, `exclude` |
| `purpose_candidate_weight_multiplier` | 후보 제한 가중치 근거. v1 기본 UI에서는 숨김 |
| `eligibility_rule_matched` | 제외/제한 가중치 사유. v1에서는 요약 매핑 후 제한 노출 |

목적 라벨 권장 매핑:

| `purpose_key` | UI 라벨 |
|---|---|
| `entry-growth` | 신규 유입 / 첫 구매 확대 |
| `next-purchase` | 다음 구매 연결 강화 |
| `return-strength` | 다시 찾는 구매 강화 |
| `basket-expansion` | 함께 담기 확장 |

상태 라벨 권장 매핑:

| 원본 값 | UI 라벨 |
|---|---|
| `operational_candidate` | 운영 검토 후보 |
| `testable` | 실험 검토 후보 |
| `hypothesis_only` | 가설 수준 |
| `insufficient` | 근거 부족 |
| `not_applicable` | 비대상 |

검토 범위 라벨 권장 매핑:

| 원본 값 | UI 라벨 |
|---|---|
| `broad_rollout` | 넓은 적용 검토 |
| `limited_rollout` | 제한 적용 검토 |
| `small_test` | 작은 실험 검토 |
| `not_recommended` | 현재 검토 제외 |

---

## 8. 부족한 필드

`pgm_product_purpose_effects.csv`만으로 PoC는 가능하지만, 다음 필드는 있으면 앱 품질이 크게 좋아진다.

| 부족한 필드 | 필요한 이유 | PoC 처리 |
|---|---|---|
| 산출 버전 / 로직 버전 | heuristic v0.1임을 명확히 표시하고 버전별 결과 차이를 추적해야 함 | 문서와 화면 안내 문구로 처리 |
| `as_of_date` / window | 어떤 기간 기준 판단인지 표시해야 함 | `snapshot_name`만 표시 |
| 목적별 한국어 설명 | `purpose_key`만으로는 사용자가 목적을 바로 이해하기 어려움 | 프론트 라벨 맵으로 처리 |
| rationale code 한국어 매핑 | `effect_rationale_code`를 그대로 보이면 내부 코드처럼 보임 | v1에서는 핵심 코드만 수동 매핑 |
| 제품 그룹핑 적용 결과 | `products`는 `pgm_product_group_map.csv`를 반영하지만, purpose effects가 raw product 기준이면 화면 간 제품 단위가 어긋날 수 있음 | PoC에서는 CSV의 `product_id`를 기준으로 표시하고, 그룹핑 차이는 명시 |
| 현재 매출/수요 규모 | 운영 검토 후보의 비즈니스 크기감을 함께 봐야 함 | 선택적으로 `pgm_scored.csv`를 join |
| 기존 products 점수 | Entry/Expansion/Return/Convergence 구조 점수와 함께 보면 해석이 쉬움 | v1 이후 선택 join |
| 검토 소유자/검토 상태 | 실제 운영 검토 흐름에는 필요 | PoC에서는 제외 |
| 실험 결과 피드백 | 예상 효과와 실제 결과를 비교해야 calibration 가능 | PoC 범위 밖 |
| 관련 제품/페어 상세 | basket/transition 후보의 구체 연결을 설명하려면 필요 | products 딥링크 또는 demand graph로 연결 |

최소 PoC에서는 `pgm_product_purpose_effects.csv` 단독으로 시작하고, `pgm_scored.csv` join은 2단계로 두는 것이 적절하다.

---

## 9. v1에서 보여줄 것과 숨길 것

### v1에서 보여줄 것

- 목적별 후보 수와 상태 분포
- `effect_scope`, `effect_status`, `effect_confidence`, `effect_strength`
- 전제조건/리스크 플래그
- 제품별 목적 비교 카드
- 주요/보조 metric 이름과 값
- `merchandise_role`
- `purpose_candidate_eligibility`가 `downweight` 또는 `exclude`인 경우의 제한 배지
- "heuristic v0.1 / 검토용" 안내 문구

### v1에서 숨기거나 낮은 우선순위로 둘 것

- `effect_signal_score`의 정확한 숫자
- `effect_maturity_score`의 정확한 숫자
- `purpose_candidate_weight_multiplier`
- `eligibility_rule_matched` 원문
- `effect_rationale_code` 원문
- "최고 추천", "정답", "자동 실행"처럼 보이는 CTA
- 후보 간 미세한 점수 차이를 순위처럼 강조하는 UI

정확한 점수는 PoC에서 의사결정 확신을 과하게 만들 수 있다. 정렬에는 쓰되, 기본 화면에서는 라벨과 범위 중심으로 보여주는 편이 안전하다.

---

## 10. `products` 디자인 언어 재사용

`pgm_usage`는 새로운 성격의 앱이지만 시각 언어는 `products`를 유지한다.

재사용할 패턴:

| `products` 패턴 | `pgm_usage` 적용 |
|---|---|
| 사이드바 / top-bar 구조 | 동일한 shell 유지. nav에는 `목적별 활용 검토` 추가 |
| `Pretendard` 폰트, 기본 색상, 여백 체계 | `style.css`의 기존 토큰과 컴포넌트 톤 재사용 |
| 흰색 panel + 옅은 border | 전체 후보 검토와 선택 상품 목적별 비교 영역에 적용 |
| badge/chip 스타일 | scope, status, confidence, risk/precondition에 적용 |
| action-row 패턴 | 상세 행의 "근거", "주의", "검토 범위" 표시 |
| products side panel 구성 | 선택 상품 목적별 비교 패널에 유사한 section-card 구조 적용 |
| 테이블 행 클릭으로 상세 갱신 | 후보 테이블 → 선택 제품 패널 갱신 |

주의할 점:

- `products`의 4분면 색상 의미를 그대로 가져오면 안 된다. `pgm_usage`의 색상은 zone이 아니라 검토 범위와 상태를 표현해야 한다.
- 목적 후보 카드는 너무 많은 gradient를 쓰지 않고, products의 옅은 배경/경계 중심으로 간다.
- 이 앱의 주 동작은 "검토"이지 "실행"이므로 버튼은 과하게 강조하지 않는다.

권장 body id:

```html
<body id="page-pgm-usage">
```

권장 파일 구조:

```text
apps/pgm_usage/
└── index.html

pgm_usage.js
style.css
app.js
```

`app.js`에는 CSV registry만 추가하고, 앱별 렌더링은 `pgm_usage.js`에서 담당하는 구조가 가장 얇다.

---

## 11. 이 앱이 절대 하면 안 되는 오해 유도

1. **"최적 상품" 또는 "정답 추천"처럼 표현하지 않는다.**  
   이 앱은 후보 검토 화면이다. "추천", "베스트", "1위"보다 "검토 후보", "먼저 볼 후보", "비교 필요"를 쓴다.

2. **예상 효과를 causal truth처럼 말하지 않는다.**  
   현재 expected-effect logic은 heuristic v0.1이다. "효과가 난다"가 아니라 "효과 가설을 검토할 수 있다"로 표현한다.

3. **`broad_rollout`을 자동 실행 승인처럼 보이게 하지 않는다.**  
   넓은 적용 검토가 가능하다는 뜻이지, 운영 승인이나 예산 배정 결론이 아니다.

4. **선택 제품 기준으로 전체 후보를 재계산하지 않는다.**  
   선택 제품은 강조만 한다. 글로벌 후보는 전체 카탈로그 기준으로 고정돼야 한다.

5. **근거 부족을 실패처럼 표현하지 않는다.**  
   `insufficient`와 `not_applicable`은 "지금 판단하지 말라"는 상태다. 제품 가치가 낮다는 뜻으로 쓰면 안 된다.

---

## 12. 현재 단계의 최소 사용자 의사결정

지금 필요한 결정은 세 가지면 충분하다.

| 결정 | 권장안 |
|---|---|
| 앱 ID / 메뉴 라벨 | 내부 앱 ID와 폴더명은 `pgm_usage`, 화면/메뉴 라벨은 **목적별 활용 검토**로 고정 |
| v1 기본 정렬 | `effect_scope` → `effect_confidence` → `effect_strength` → `effect_signal_score` 순 |
| `pgm_scored.csv` join 여부 | PoC 1차는 단독 CSV로 시작, 2차에서 매출/수요 규모 join |

그 외 결정은 구현 중 데이터 상태를 보며 조정해도 된다. 특히 threshold 재설계나 effect logic 변경은 frontend 범위가 아니다.

---

## 13. 다음 구현 단계

### Step 1. 데이터 등록

- `app.js`의 `REQUIRED_FILES`에 `productPurposeEffects` 추가
- `AppState.data`와 `AppState.rawData`에 `productPurposeEffects: []` 추가
- canonical filename은 `pgm_product_purpose_effects.csv`
- alias는 필요 시 `product_purpose_effects.csv` 정도만 허용

### Step 2. 앱 skeleton 추가

- `apps/pgm_usage/index.html` 생성
- shell은 `apps/products/index.html` 구조를 복사하되 body id, title, active nav만 변경
- title과 active nav의 사용자 표시 라벨은 `목적별 활용 검토`를 사용
- `pgm_usage.js`를 별도 생성
- full app 구현 전에는 loading/empty/data count만 렌더링해도 충분

### Step 3. 정규화 모델 작성

`pgm_usage.js` 안에서 다음 순서의 얇은 model builder를 둔다.

```text
raw rows
→ normalizePurposeEffectRow
→ group by purpose_key
→ group by product_id
→ buildGlobalReviewModel
→ buildSelectedProductTradeoffModel
```

프론트에서 새 heuristic을 만들지 않는다. 정렬, 필터, 라벨 매핑만 담당한다.

### Step 4. 전체 후보 검토 구현

- toolbar
- summary strip
- 목적별 후보 보드
- 후보 테이블
- row click → 선택 상품 업데이트

### Step 5. 선택 상품 목적별 비교 구현

- 제품 검색
- 선택 제품의 목적별 4개 카드
- 클릭한 목적 강조
- risk/precondition 상태 표시

### Step 6. PoC 검증

검증 기준:

- `pgm_product_purpose_effects.csv`가 없을 때 명확한 empty state가 나온다.
- 전체 후보 영역은 제품 선택과 무관하게 동일하다.
- 제품 선택 시 선택 상품 목적별 비교만 바뀐다.
- `insufficient`, `not_applicable`, `exclude`가 추천처럼 보이지 않는다.
- score 숫자를 앞세우지 않는다.
- products 화면의 시각 언어와 어긋나지 않는다.

---

## 14. PoC 범위 밖

다음은 지금 만들지 않는다.

- 검토 후보 저장/승인 흐름
- 후보 자동 실행
- threshold 튜닝 UI
- core effect logic 재구현
- 실험 결과 회수/학습 루프
- 목적별 상세 causal explanation
- products 4분면 재구성

이 범위를 넘기면 앱이 기획 PoC가 아니라 미완성 추천 엔진처럼 보일 가능성이 높다.
