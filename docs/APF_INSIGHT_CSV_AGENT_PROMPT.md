# APF Insight CSV 생성 Agent 전달 프롬프트

아래 지침에 따라 기존 5개 CSV를 입력으로 받아 신규 5개 CSV를 생성하라.

## 목표
- APF Insight Studio에서 사용하는 신규 데이터셋 생성
- 입력 데이터가 집계 테이블만 있어도 동작해야 함
- 출력 컬럼/타입/파일명은 계약을 엄격히 준수

## 입력 파일 (기존 5개)
- `brand_score.csv`
- `anchor_scored.csv`
- `anchor_transition.csv`
- `cart_anchor.csv`
- `cart_anchor_detail.csv`

## 출력 파일 (신규 5개)
- `aa_cohort_journey.csv`
- `aa_transition_path.csv`
- `ca_profile.csv`
- `bii_window.csv`
- `apf_action_rules.csv`

## 생성 계약
- 상세 규격은 `configs/apf_insight_generation_spec.json`을 따른다.
- 출력 파일명/컬럼명은 정확히 동일해야 한다.
- UTF-8 BOM 허용, 헤더 필수.

## 생성 모드
1. `aggregated_fallback`:
- raw member-order 로그가 없을 때 사용
- `anchor_scored`, `anchor_transition`, `cart_anchor` 기반 추정 생성 허용

2. `raw_preferred`:
- raw가 있으면 실제 코호트/윈도우 계산으로 대체
- 현재는 `aggregated_fallback`을 기본값으로 한다

## 핵심 규칙
- `aa_transition_path`: `anchor_transition`를 기반으로 생성
  - `cohort_date`는 생성일로 채움
  - `aa_type`은 `anchor_scored.AA_Primary_Type`에서 조인
- `ca_profile`: `cart_anchor` + `cart_anchor_detail`에서 생성
  - `ca_type`은 `CA_Primary_Type` 매핑
  - `top1_companion_product_id`는 `co_order_cnt` 최대 짝으로 선정
- `aa_cohort_journey`:
  - `repeat_90d_rate`는 `anchor_scored.repurchase_rate_90d`
  - `repeat_30d_rate`, `repeat_7d_rate`는 90일 비율 기반 축소계수로 추정(예: 0.72, 0.55)
  - `pca_transition_90d_rate`는 가능하면 `anchor_transition` 집계 사용
- `bii_window`:
  - `window_days`는 7/30/90/365 고정
  - raw가 없으면 CLV/active/depth를 휴리스틱으로 추정
  - `stage`는 기본 `Stable`
- `apf_action_rules`:
  - 기본 템플릿 6개 규칙 생성
  - `domain`, `condition_expr`, `priority`, `title_ko`, `action_ko`, `impact_ko` 포함

## 품질 체크
- `aa_cohort_journey`에서 `repeat_7d_rate <= repeat_30d_rate <= repeat_90d_rate` 유지
- `transition_rate`는 0~1 범위
- `bii_window`는 window 4개 행이 모두 존재
- `apf_action_rules`는 최소 3개 이상 규칙

## 산출물 검증
- 생성 후 `python3 scripts/validate_insight_csvs.py --dir <output_dir>` 실행
- 오류 0건일 때만 완료 처리

