# pgm_ops2

`pgm_ops2`는 `PGM 1.0 운영 툴 PRD`를 기준으로 새로 만든 운영 우선순위 앱이다.

원칙:

- 기존 `pgm_ops`의 화면 구조, 카드 구성, artifact 계약을 기준 진실로 사용하지 않는다.
- 데이터 truth는 Rosetta MCP 명세와 실제 source 가용 범위를 따른다.
- v1 기본 대상 단위는 상품이다.
- 우선순위 메인축은 Revenue + Role이며, Brand Score는 별도 재현 트랙으로만 다룬다.

실행:

```bash
npm run all -- --as-of-date 2026-04-18 --lookback-days 120 --refresh-rosetta
npm run serve
```

Rosetta raw CSV를 앱 경로에 직접 적재할 때는 기본적으로 로컬 Codex의 Rosetta 웹 로그인 세션을 재사용한다.

```bash
codex mcp list
codex mcp login rosetta
npm run refresh-rosetta -- --as-of-date 2026-04-18 --lookback-days 30
```

기본값은 `PGM_OPS2_ROSETTA_AUTH_MODE=auto` 이며 `codex_mcp_bridge -> bearer_token` 순서로 시도한다.

`artifacts/raw_rosetta/__raw_refresh_status.csv` 에는 `auth_missing`, `mcp_bridge_failed`, `source_empty`, `query_failed` 중 하나가 failure code로 남는다.

보조 경로로 bearer token 방식도 계속 지원한다.

```bash
export PGM_OPS2_ROSETTA_AUTH_MODE="bearer"
export PGM_OPS2_ROSETTA_BEARER_TOKEN="<rosetta bearer token>"
npm run refresh-rosetta -- --as-of-date 2026-04-18 --lookback-days 30
```

artifact 계층:

- `artifacts/raw_rosetta`
- `artifacts/staging`
- `artifacts/mart`
- `artifacts/view_model`
- `artifacts/qa`
