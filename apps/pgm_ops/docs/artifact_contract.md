# Artifact Contract

`pgm_ops`는 한 파일, 한 grain 원칙을 따릅니다. 모든 컬럼명은 `snake_case`, ID는 문자열, 날짜는 `YYYY-MM-DD`, boolean은 `true/false` 문자열 기준입니다.

## Common rules

- UI는 `raw_extract`, `staging`, `mart`를 직접 읽지 않고 `view_model`만 읽습니다.
- `date + product_id` grain mart끼리만 동일 grain join을 허용합니다.
- `product_daily_metrics.csv`와 `stg_order_items.csv`의 row-level direct join은 금지합니다.
- `product master`가 비어도 전체 집계는 유지하고, 표시명만 fallback 허용합니다.
- `role state`는 same-date snapshot only입니다. latest available fallback은 금지합니다.
- 브라우저 sample fallback은 artifact read failure일 때만 허용합니다. 비어 있는 artifact를 sample로 치환하지 않습니다.

## Mart contracts

### `product_daily_metrics.csv`

- grain: `date + product_id`
- pk candidate: `date, product_id`
- required columns:
  - `date`
  - `product_id`
  - `product_name`
  - `order_count`
  - `quantity`
  - `revenue`
- optional columns:
  - `product_name_source`

### `product_role_profile.csv`

- grain: `product_id`
- pk candidate: `product_id`
- required columns:
  - `product_id`
  - `profile_role_primary`
  - `profile_role_secondary`
  - `profile_confidence`

### `product_role_state_daily.csv`

- grain: `date + product_id`
- pk candidate: `date, product_id`
- required columns:
  - `date`
  - `product_id`
  - `role_state_primary`
  - `role_state_confidence`
  - `pgm_observed_flag`
- optional columns:
  - `role_state_source`

### `revenue_structure_daily.csv`

- grain: `date + product_id`
- pk candidate: `date, product_id`
- required columns:
  - `date`
  - `product_id`
  - `revenue`
  - `revenue_share_in_brand_day`
  - `revenue_rank_in_brand_day`

### `brand_operating_status_daily.csv`

- grain: `date`
- pk candidate: `date`
- required columns:
  - `date`
  - `brand_revenue`
  - `brand_order_count`
  - `active_product_count`
  - `pgm_observed_product_count`
  - `top_product_revenue_share`
  - `dominant_role_state_in_revenue`
  - `status_summary_label`
- optional columns:
  - `pgm_observed_coverage`
  - `status_reason`

## View-model contracts

### `overview_daily_cards.csv`
### `overview_weekly_cards.csv`
### `overview_monthly_cards.csv`

- grain: `period + card_key`
- required columns:
  - `period`
  - `card_key`
  - `label`
  - `value`
  - `delta`
  - `reason`
  - `as_of_date`

### `product_table.csv`

- grain: latest snapshot `product_id`
- required columns:
  - `product_id`
  - `product_name`
  - `profile_role_primary`
  - `role_state_primary`
  - `pgm_observed_flag`
  - `revenue`
  - `revenue_share_in_brand_day`
  - `revenue_rank_in_brand_day`
  - `revenue_7d`
  - `revenue_30d`
  - `revenue_90d`
- optional columns:
  - `role_state_source`
  - `product_name_source`

### `product_detail_header.csv`

- grain: latest snapshot `product_id`
- required columns:
  - `product_id`
  - `headline`
  - `summary`
  - `priority_hint`

### `role_structure_chart.csv`

- grain: latest snapshot `role_state_primary`
- required columns:
  - `role_state_primary`
  - `revenue`
  - `revenue_share`
  - `product_count`

### `revenue_structure_chart.csv`

- grain: latest snapshot `product_id`
- required columns:
  - `product_id`
  - `product_name`
  - `revenue`
  - `revenue_share_in_brand_day`
  - `role_state_primary`

### `priority_checks.csv`

- grain: latest snapshot `priority_rank`
- required columns:
  - `priority_rank`
  - `priority`
  - `entity_type`
  - `entity_id`
  - `label`
  - `reason`
  - `suggested_check`
  - `evidence`
- optional columns:
  - `rule_source`

priority rule source notes:

- brand-level priority trigger는 `brand_operating_status_daily`를 기준으로 합니다.
- product inspection order는 latest `revenue_structure_daily` rank/share를 기준으로 좁힙니다.
- role-state blank check는 `product_role_state_daily.role_state_source=blank`를 그대로 사용하며 latest role fallback을 허용하지 않습니다.
