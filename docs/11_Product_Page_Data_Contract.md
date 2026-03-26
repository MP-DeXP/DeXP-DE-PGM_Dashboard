# Product Page Data Contract

**Audience:** Data engineers and backend engineers
**Scope:** Defines which CSV files feed which screens on the Product Page, how data is merged and transformed, and what fallback behavior is expected when files are absent.
**Column-level schema:** See `DATA_SCHEMA.md`. This document does not re-list field types or value ranges.

> **⚠️ 프로덕션 전환 예정 사항**
>
> - **데이터 로딩:** 현재는 IndexedDB + CSV 업로드 구조이나, 프로덕션에서는 **API 호출로 교체** 예정. 각 CSV 키(`anchorScored`, `productDemandGravity` 등)에 대응하는 API 엔드포인트가 정의되어야 함.
> - **제품 그룹핑:** 프로덕션 범위에서 **제외** 예정. `pgm_product_group_map.csv` 및 관련 병합 로직은 구현하지 않음.

---

## 1. Storage Layer

All CSV files are loaded at app initialization and stored in IndexedDB.

| Parameter | Value |
|---|---|
| DB name | `PGM_Dashboard_DB` |
| Object store | `csv_files` |
| Version | `1` |

Files are keyed by the logical key defined in `REQUIRED_FILES` (see Section 2). The frontend reads all files in parallel on startup; no file is lazy-loaded.

---

## 2. REQUIRED_FILES Registry

Defined in `app.js`. Each entry declares the logical key used throughout the app, the canonical filename the backend must produce, and accepted aliases for backward compatibility.

| Logical key | Canonical filename | Aliases |
|---|---|---|
| `anchorScored` | `pgm_scored.csv` | `anchor_scored.csv` |
| `anchorTransition` | `pgm_entry_to_expansion_transition.csv` | `anchor_transition.csv` |
| `productDemandGravity` | `pgm_product_demand_gravity.csv` | `product_demand_gravity.csv` |
| `productTransitionEdge` | `pgm_product_transition_edge.csv` | `product_transition_edge.csv` |
| `returnGravityLoopDetail` | `pgm_return_gravity_loop_detail.csv` | `return_gravity_loop_detail.csv` |
| `cartAnchor` | `pgm_basket_gravity.csv` | `cart_anchor.csv` |
| `cartAnchorDetail` | `pgm_basket_gravity_detail.csv` | `cart_anchor_detail.csv` |
| `insightDemandGraphNodes` | `_insight_demand_graph_nodes.csv` | `insight_demand_graph_nodes.csv` |
| `insightDemandGraphEdges` | `_insight_demand_graph_edges.csv` | `insight_demand_graph_edges.csv` |
| `insightDemandGraphPatterns` | `_insight_demand_graph_patterns.csv` | `insight_demand_graph_patterns.csv` |
| `aaCohortJourney` | `_insight_entry_cohort_journey.csv` | _(none)_ |
| `aaTransitionPath` | `_insight_entry_transition_path.csv` | _(none)_ |
| `productGroupMap` | `pgm_product_group_map.csv` | `product_group_map.csv`, `_meta_product_group_map.csv` |
| `apfActionRules` | `_insight_pgm_action_rules.csv` | _(none)_ |

The alias list exists solely for file-upload matching. Pipeline outputs should always use the canonical filename.

---

## 3. CSV ↔ Screen Mapping

| CSV file | Consumed by |
|---|---|
| `pgm_scored.csv` | Quadrant chart (bubble position and size), Side panel (KPI metrics), Product table |
| `pgm_product_demand_gravity.csv` | Quadrant chart (gravity scores), Side panel, Product table |
| `pgm_entry_to_expansion_transition.csv` | Quadrant edge rendering (representative mode), Demand graph modal — transition tab (legacy fallback) |
| `pgm_product_transition_edge.csv` | Quadrant edge rendering, Demand graph modal — transition tab (primary raw source) |
| `pgm_return_gravity_loop_detail.csv` | Quadrant edge rendering (return loop mode) |
| `pgm_basket_gravity.csv` | Demand graph modal — basket tab (basket-level summary) |
| `pgm_basket_gravity_detail.csv` | Demand graph modal — basket tab (product pair detail) |
| `_insight_demand_graph_nodes.csv` | Demand graph modal (enhanced node data) |
| `_insight_demand_graph_edges.csv` | Demand graph modal — transition tab (pre-computed, preferred source) |
| `_insight_demand_graph_patterns.csv` | Demand graph modal (pattern summaries) |
| `pgm_product_group_map.csv` | Entire Product page (product grouping, `entity_id` mapping) |

---

## 4. Data Merge and Transformation Logic

### 4.1 Gravity Score Resolution (`getMergedCoreDemandRows`)

Merges `pgm_scored.csv` and `pgm_product_demand_gravity.csv` per product. Resolution rules:

- **Entry_Gravity_Score**: `productDemandGravity` takes precedence over `anchorScored`.
  - Fallback alias from `anchorScored`: `AA_Score` → treated as `Entry_Gravity_Score`.
- **Expansion_Gravity_Score**: same precedence order.
  - Fallback alias from `anchorScored`: `PCA_Score` → treated as `Expansion_Gravity_Score`.
- **Return_Gravity_Score**: sourced exclusively from `productDemandGravity`. Not available in `anchorScored`.
- **Convergence_Gravity_Score**: sourced exclusively from `productDemandGravity`. Not available in `anchorScored`.

If `productDemandGravity` is absent entirely, the app falls back to `AA_Score` / `PCA_Score` from `pgm_scored.csv` for the two primary axes. Return and Convergence scores will be unavailable.

### 4.2 Product Name Resolution (`getProductName`)

Priority order (first hit wins):

1. `grouping.entityIdToName` — display name associated with the resolved `entity_id`
2. `grouping.rawNameById` — raw name from `pgm_product_group_map.csv`
3. `product_id` string — used verbatim as a last resort

### 4.3 Product Grouping

`pgm_product_group_map.csv` maps raw `product_id` values to a shared `entity_id`. Products that resolve to the same `entity_id` are merged into one logical product for display:

- Additive fields (e.g., counts): summed across the group.
- Rate fields: weighted-averaged across the group.

The mapping must be applied before any gravity score merge or table rendering.

### 4.4 Transition Entity Set (`buildTransitionEntitySet`)

Computed as the union of all `product_id` values present in:
- `pgm_entry_to_expansion_transition.csv` (`anchorTransition`)
- `pgm_product_transition_edge.csv` (`productTransitionEdge`)

This set determines whether the "추가구매 제품 보기" CTA is enabled for a given product in the side panel. If a product's `entity_id` has no representation in either file, the CTA is disabled.

---

## 5. Per-Screen Field Requirements

### 5.1 Quadrant Chart

| Category | Fields |
|---|---|
| Required | `product_id`, `Entry_Gravity_Score` (or `AA_Score`), `Expansion_Gravity_Score` (or `PCA_Score`) |
| Optional | `product_order_cnt_1y` (bubble size; defaults to `0` if absent) |

**Center computation:** The quadrant center point is the median of `Entry_Gravity_Score` and the median of `Expansion_Gravity_Score` computed across **all valid products** (not filtered to selection). This is recalculated each time the full product list changes.

### 5.2 Side Panel

Source file: `pgm_scored.csv` for the fields below.

| Metric | Source field | Computation |
|---|---|---|
| Raw first-purchase count | `first_customer_cnt` | Direct read |
| Raw repurchase count | `repurchase_customer_cnt_90d` | Direct read |
| 90-day revenue | `revenue_90d` | Direct read |
| 첫구매 기여 비중 | `first_customer_cnt` | This product's value / SUM of all products' `first_customer_cnt` |
| 재구매 기여 비중 | `repurchase_customer_cnt_90d` | This product's value / SUM of all products' `repurchase_customer_cnt_90d` |

### 5.3 Demand Graph Modal — Transition Tab

Data source priority (first available source wins):

1. `_insight_demand_graph_edges.csv` — pre-computed edges, preferred. Used as-is.
2. `pgm_product_transition_edge.csv` — raw edges. The frontend prunes to **top-5 per side** (top-5 inbound, top-5 outbound) by edge weight before rendering.
3. `pgm_entry_to_expansion_transition.csv` — legacy representative-mode fallback. Used only if both sources above are absent.

### 5.4 Demand Graph Modal — Basket Tab

Source: `pgm_basket_gravity_detail.csv` for product pair rows.

- **Deduplication:** A pair `(i, j)` is kept only when `String(i) < String(j)`. This eliminates duplicate reversed pairs. The backend may produce both directions; the frontend deduplicates client-side.
- **Sorting:** By `co_order_cnt` descending.
- **Display limit:** Top `DEMAND_GRAPH_BASKET_LIMIT = 6` pairs are shown.

Basket-level summary (tab header metrics) comes from `pgm_basket_gravity.csv`.

### 5.5 Product Table

- Reads the merged row set produced by `getMergedCoreDemandRows`.
- Applies a **top-80% cumulative gravity share cutoff** per axis: the threshold is `0.8`.
- Products that fall below the 80% threshold on both axes still appear in the quadrant chart but are **excluded from the table**.
- Sorting and filtering in the table operate on the already-merged rows.

---

## 6. Loading Behavior and Required vs. Optional Files

| File | Behavior if absent |
|---|---|
| `pgm_scored.csv` (`anchorScored`) | **REQUIRED.** If absent, the entire Product page renders an empty state with an upload CTA. No other computation proceeds. |
| `pgm_product_demand_gravity.csv` | Optional. Falls back to `AA_Score` / `PCA_Score` from `pgm_scored.csv`. Return and Convergence scores unavailable. |
| `pgm_product_group_map.csv` | Optional. Products are not grouped; each raw `product_id` treated as its own entity. |
| `pgm_product_transition_edge.csv` | Optional. Transition tab falls back to `pgm_entry_to_expansion_transition.csv`. |
| `pgm_entry_to_expansion_transition.csv` | Optional (legacy). Only used if `productTransitionEdge` and `insightDemandGraphEdges` are both absent. |
| `_insight_demand_graph_edges.csv` | Optional. Falls back to `pgm_product_transition_edge.csv`. |
| All other files | Optional. Independent fallbacks; absent files suppress the corresponding UI section. |

All files are fetched from IndexedDB in parallel at app init. There is no lazy loading or on-demand fetch after initialization.

---

## 7. UI Label ↔ Field Cross-Reference

| UI label (Korean) | Field name | CSV source |
|---|---|---|
| 첫구매 유입 강점 | `Entry_Gravity_Score` (or `AA_Score`) | `pgm_scored.csv` / `pgm_product_demand_gravity.csv` |
| 재구매 확장 강점 | `Expansion_Gravity_Score` (or `PCA_Score`) | Same as above |
| 복귀 강점 | `Return_Gravity_Score` | `pgm_product_demand_gravity.csv` |
| 수렴 강점 | `Convergence_Gravity_Score` | `pgm_product_demand_gravity.csv` |
| 주간 예상 수요량 | `product_order_cnt_1y / 52` | `pgm_scored.csv` (computed by frontend) |
| 구매 지속 가능성 | `repurchase_rate_90d` | `pgm_scored.csv` |
| 첫구매 기여 비중 | `first_customer_cnt / total` | `pgm_scored.csv` (computed by frontend) |
| 재구매 기여 비중 | `repurchase_customer_cnt_90d / total` | `pgm_scored.csv` (computed by frontend) |

For full field definitions, valid value ranges, and data types for every field listed above, refer to `DATA_SCHEMA.md`.
