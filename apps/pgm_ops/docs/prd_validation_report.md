# PGM 1.0 운영 툴 PRD 검증 보고서

검증 기준은 Obsidian의 `50_MERCURY X/20_Laboratory/30_Product/PGM 1.0 운영 툴 PRD`와 연결 문서, 그리고 현재 `apps/pgm_ops` 코드/아티팩트입니다.  
이번 판정은 문서 설명이 아니라 실제 코드와 생성된 CSV artifact를 우선 근거로 삼았습니다.

## 분류 기준

- `matched`: PRD 요구와 현재 구현이 같은 수준으로 확인됨
- `partial`: 방향은 맞지만 범위가 축소되었거나 동기화 자동화가 완결되지 않음
- `deferred`: PRD 또는 연결 문서에서 보류로 남긴 항목이며 현재도 그 상태를 유지함
- `missing`: PRD 요구가 있으나 현재 코드/아티팩트에서 근거를 찾지 못함

## 요약

- matched: 9
- partial: 4
- deferred: 1
- missing: 0

## 항목별 판정

| 검증축 | 기대 내용 | 현재 구현 / 아티팩트 근거 | 판정 | 비고 |
|---|---|---|---|---|
| 애디티브 구조 | 기존 PGM 코어를 건드리지 않고 상태판을 덧붙이는 구조 | `raw -> staging -> mart -> view_model -> qa -> UI` 레이어 유지 | matched | 기존 앱 바깥으로 격리되어 있음 |
| UI 읽기 경로 | UI는 `view_model`만 읽고 중간 레이어는 직접 읽지 않음 | 브라우저 기본 경로가 `/api/pgm-ops/view-model/*.csv`이며 UI는 view_model만 소비 | matched | static 직접 읽기 대신 서비스 기본값으로 전환 |
| Daily / Weekly / Monthly 상태판 | 일/주/월 카드 상태판 존재 | `overview_daily_cards`, `overview_weekly_cards`, `overview_monthly_cards` 생성 및 렌더 | matched | 화면과 CSV 둘 다 존재 |
| 7 / 30 / 90 윈도우 | 전일 비교와 7/30/90 window 반영 | `revenue_windows.js`와 overview card builder에 rolling window 계산은 존재하고, `overview_role_contribution`, `brand_role_window_comparison`까지 생성됨. 다만 역할 source인 `gold_pgm_product_demand_gravity`/`gold_pgm_scored` tenant 데이터는 현재 `2026-04-05`~`2026-04-17` 12~13일 범위만 확인됨 | partial | 매출 window는 채워지지만 30/90일 역할 구성은 완전한 same-date history가 아니라 부분 관측 + fallback 성격이 남음 |
| role profile / state 분리 | product-grain profile과 same-date state를 분리 | `product_role_profile`, `product_role_state_daily`, `PGM 미관측` UI 라벨 유지 | matched | latest fallback 금지 유지 |
| Revenue Structure 결합 | 역할 구조와 매출 구조를 함께 읽는 운영 관점 | `revenue_structure_daily`, `revenue_structure_chart`, `product_table`가 연결됨 | matched | overview / products / priority에 공통 근거로 사용 |
| 운영 우선점검 대상과 이유 | 우선 점검 대상, 이유, suggested check를 제공 | `priority_checks.csv`와 priority 화면에 이유, evidence, rule source 노출 | matched | transition / return 근거도 함께 연결됨 |
| CSV 서비스화 | 정적 파일 직접 경로 대신 CSV 서비스 엔드포인트 제공 | `server.js`에서 `/api/pgm-ops/view-model`, `/mart`, `/qa`, `/meta/load-status.json` 제공 | matched | 요청 시 계산 없이 최신 artifact만 응답 |
| transition 보조 패널 | 상품별 상위 이동 pair와 전환율/소요일 노출 | `product_transition_summary.csv`, `transition_summary.csv`, products / priority 보조 패널 렌더 | matched | 완결 경험이 아니라 운영 보조 패널 수준으로 구현 |
| QA 산출물 | validation / coverage / PRD summary 산출물 유지 | `validation_summary.csv`, `coverage_report.csv`, `validation_report.md`, `prd_validation_summary.csv` 존재 | matched | 코드 기준 검증 리포트까지 추가됨 |
| Rosetta 실데이터 동기화 | Rosetta 실데이터를 기반으로 CSV snapshot을 갱신 | fixture 상품명이 아니라 실제 상품명/실제 지표 기반 raw CSV와 view_model CSV가 생성됨 | partial | 현재 저장소 내부에는 Rosetta 재동기화 자동 스크립트가 없고, 이번 스냅샷은 동기화 단계 산출물로 반영됨 |
| member / UTM / channel 보조 근거 | 메인 판단 프레임은 아니지만 보조 evidence는 노출 | `members.csv`, `order_with_utm.csv`, `revenue_inflow_context.csv` 생성. UI는 UTM 중심 보조 맥락 노출 | partial | member 드릴다운은 아직 UI 미노출 |
| return loop 구현 수준 | 복귀율 / 반복율 / 루프율 요약 제공 | `product_return_loop_summary.csv`, `return_loop_summary.csv`, products / priority 보조 패널 렌더 | partial | PRD가 암시한 더 깊은 경험 완결은 아직 아님 |
| BHI primary metric | BHI source 확정 후 primary metric 반영 | BHI는 여전히 primary metric으로 올리지 않고 보류 메모 유지 | deferred | source 미확정 상태를 그대로 명시 |

## 판정 메모

- 실제 artifact에는 더 이상 fixture 상품명(`Hydra Serum` 등)이 아니라 Rosetta 기반 실상품명이 들어갑니다.
- 앱 기본 로딩 경로는 `/api/pgm-ops`로 전환되었고, `load-status.json`으로 최신 snapshot과 QA 상태를 함께 확인할 수 있습니다.
- 7/30/90 window 계산 로직은 유지되며 화면도 해당 블록을 렌더합니다.
- 다만 Rosetta 확인 결과 `silver_fact_product`는 장기 이력이 충분하지만, 역할 source인 `gold_pgm_product_demand_gravity`는 `2026-04-06`~`2026-04-17` 12일, `gold_pgm_scored`는 `2026-04-05`~`2026-04-17` 13일 범위만 존재합니다.
- 따라서 `7일 역할 구성 변화`는 실관측 기반으로 볼 수 있지만, `30일/90일 역할 구성 변화`는 완전한 historical role attribution이 아니라 현재 가용한 역할 관측 범위와 window metric을 함께 쓰는 partial 구현으로 봐야 합니다.
- `transition / return-loop / UTM`은 PRD의 보조 evidence 축으로는 반영되었지만, full drill-down 제품 경험으로 확장된 것은 아닙니다.
- Rosetta 경계는 브라우저 런타임이 아니라 동기화 단계에만 두었습니다. 이 점은 이번 구현 방향과 일치하지만, 재현 가능한 동기화 자동화는 후속 과제로 남아 있습니다.

## 결론

현재 `pgm_ops`는 상태판 핵심 구조, CSV 서비스화, 실제 상품명 기반 snapshot, transition / return-loop / UTM 보조 근거까지 포함하는 수준으로 PRD와 상당 부분 정렬되었습니다.  
남은 차이는 주로 `Rosetta 재동기화 자동화`, `member drill-down`, `BHI canonical source 확정` 세 축에 집중됩니다.

추가 메모:

- 현재 운영 목적은 역할 구조 소개가 아니라 매출 증감 원인 분해판에 더 가깝습니다.
- 역할은 매출 해석 축으로 사용하며, 상품 단위 실제 역할 이동 이력을 주장하지 않습니다.
