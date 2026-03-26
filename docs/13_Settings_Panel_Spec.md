# Settings Panel Spec

**Document:** 13_Settings_Panel_Spec.md
**Audience:** Frontend developers and UI/UX designers
**Scope:** Full specification for the Settings Panel — access, layout, both tabs, state model, persistence, and all edge cases. A developer should be able to implement the panel from this document alone.

---

## 1. Overview

The Settings Panel is a global utility panel accessible from the sidebar. It covers two concerns that affect the entire dashboard:

1. **제품 그룹 관리** — Operator-defined logical grouping of product SKUs into named entities.
2. **데이터 관리** — CSV upload, local sync from `/data/`, and IndexedDB reset.

> **⚠️ 프로덕션 스코프 제외 항목**
>
> - **제품 그룹 관리 탭 — 프로덕션 제외:** 이 탭 전체(3단계 마법사, grouping 상태 모델, `pgm_product_group_map.csv` import/export)는 프로덕션 버전에서 구현하지 않음. 현재 앱(데모/테스트용) 기준 스펙임.
> - **데이터 관리 탭 (CSV 업로드) — API로 교체:** 프로덕션에서는 CSV 업로드/로컬 동기화 대신 **API 연동으로 데이터를 자동 로드**. 설정 패널의 "데이터 관리" 탭 자체가 필요 없어질 수 있음.

The panel must not cover the main chart area when open. It opens as a side panel or overlay anchored to the sidebar, leaving the primary content area visible and interactive.

---

## 2. Access

| Property | Value |
|---|---|
| Trigger | Sidebar bottom → "설정" button |
| Behavior | Opens Settings Panel |
| Panel type | Side panel or modal overlay — must not occlude the main chart view |
| Close | "X" button in panel header, click outside the panel, or pressing Escape |
| Initial tab | Determined by `AppState.viewState.settings.activeTab` (default: `'grouping'`) |

The sidebar "설정" button acts as a toggle: clicking it while the panel is already open closes the panel.

---

## 3. Panel Layout

```
┌─────────────────────────────────────────────┐
│  설정                                    [X] │
├─────────────────────────────────────────────┤
│  [제품 그룹]  [데이터 관리]                    │  ← Tab bar
├─────────────────────────────────────────────┤
│                                             │
│  (Active tab content)                       │
│                                             │
└─────────────────────────────────────────────┘
```

- Header: "설정" title + close button
- Tab bar: two tabs, visually indicates active tab
- Content area: scrollable, renders active tab
- Active tab is persisted to `AppState.viewState.settings.activeTab` on switch

### Tab Labels

| `activeTab` value | Tab label |
|---|---|
| `'grouping'` | 제품 그룹 |
| `'data'` | 데이터 관리 |

---

## 4. Unsaved Changes Guard

If the operator has unsaved changes in the 제품 그룹 wizard (Step 2 or Step 3, any changes staged but not saved), warn before allowing:

- Switching to the 데이터 관리 tab
- Closing the Settings Panel

Confirmation dialog copy:

> **변경 사항이 저장되지 않았어요.**
> 지금 닫으면 그룹 설정 변경 사항이 사라져요. 그래도 닫을까요?
>
> [계속 편집하기]  [저장하지 않고 닫기]

"계속 편집하기" returns focus to the wizard. "저장하지 않고 닫기" discards draft state and proceeds.

---

## 5. Tab 1: 제품 그룹 관리 (Product Grouping)

### 5.1 Purpose

Allows operators to group multiple raw product SKUs (`product_id`) into a single logical entity. For example, an operator might group "아메리카노(S)", "아메리카노(M)", "아메리카노(L)" into a single entity named "아메리카노". Once grouped, the quadrant chart, product table, and all metrics aggregate by `entity_id` rather than by raw `product_id`.

### 5.2 Grouping State Model

The grouping state is a runtime object built from `pgm_product_group_map.csv` and modified by user actions. Its shape:

```javascript
grouping = {
  // entity_id → display name (either user-set or derived from group_name in CSV)
  entityIdToName: Map<entityId, displayName>,

  // entity_id → Set of product_ids that belong to this entity
  entityIdToMemberIds: Map<entityId, Set<product_id>>,

  // product_id → most recent product name string (raw name, from CSV or data)
  rawNameById: Map<product_id, product_name_latest>,

  // Set of entity_ids that represent grouped entities (≥2 products)
  groupedEntityIds: Set<entityId>,

  // Set of product_ids that are not part of any group (entity_id === product_id)
  ungroupedProductIds: Set<product_id>
}
```

**Invariants:**
- Every `product_id` in the dataset must resolve to exactly one `entity_id`.
- For ungrouped products: `entity_id === product_id`.
- For grouped products: `entity_id` is a shared identifier across all members of the group.
- `groupedEntityIds` and `ungroupedProductIds` must be mutually exclusive and collectively cover all products.

### 5.3 Entry State Display

Before launching the wizard, the tab shows a summary of the current grouping state:

```
현재 그룹 현황

  그룹 수: N개
  그룹에 속한 제품 수: N개
  그룹 없는 단일 제품 수: N개

  [그룹 설정 시작하기]   [가져오기]   [내보내기]
```

- **그룹 설정 시작하기**: opens the 3-step wizard at Step 1
- **내보내기**: downloads current grouping as `pgm_product_group_map.csv` (see Section 5.8)
- **가져오기**: opens file picker, accepts `pgm_product_group_map.csv` to override current grouping (see Section 5.8)

If no grouping data exists yet (empty state), show:

```
아직 제품 그룹이 없어요.
같은 제품의 여러 옵션(예: 사이즈, 용량)을 하나로 묶어서 분석할 수 있어요.

  [그룹 만들기]
```

### 5.4 3-Step Wizard

The wizard uses a step indicator in the panel header area:

```
① 대상 선택  →  ② 작업 선택  →  ③ 검토/저장
```

The current step is highlighted. Completed steps are visually marked as done. Steps cannot be skipped forward, but the operator can go back.

---

#### Step 1: 대상 선택 (Target Selection)

**Purpose:** Select which products or entities to operate on.

**Display:**

The operator sees a searchable list. Each row is either:
- A **grouped entity**: shows the entity display name + member count badge (e.g., "아메리카노 (3개 제품)")
- An **ungrouped product**: shows the raw product name

**Controls:**

| Control | Behavior |
|---|---|
| Search field | Filters list by name (case-insensitive, Korean-safe) |
| Row checkbox | Selects/deselects item |
| "전체 선택" checkbox | Selects all visible rows |
| [다음 단계] button | Active only if ≥1 item is selected; advances to Step 2 |
| [취소] button | Aborts wizard, returns to entry state (with unsaved changes guard) |

**Selection rules:**
- Any combination of grouped entities and ungrouped products can be selected.
- Selecting a grouped entity implicitly selects all its members for operations that act on individual products (e.g., 그룹 이동).
- The available actions in Step 2 depend on what types of items were selected (see Step 2 rules).

---

#### Step 2: 작업 선택 (Action Selection)

**Purpose:** Choose what to do with the selected products/entities.

**Available actions and their enabling conditions:**

| Action label | Condition for availability | Description |
|---|---|---|
| 그룹 만들기 | Selected items include ≥2 ungrouped products (or a mix that produces ≥2 ungrouped products) | Creates a new entity from the selected ungrouped products |
| 그룹 이동 | Selected items include ≥1 product (grouped or ungrouped); an existing target entity must exist | Moves selected products into an existing entity |
| 그룹 해제 | Selected items include ≥1 grouped entity | Splits the entity back into individual ungrouped products |
| 이름 변경 | Selected items include exactly 1 grouped entity | Renames the entity's display name |

If no action is available based on the selection, show an explanatory message (e.g., "선택한 항목으로는 수행할 수 있는 작업이 없어요.").

**Action detail UI per selection:**

**그룹 만들기:**
```
새 그룹 이름을 입력해 주세요.

  [그룹 이름 입력]  (placeholder: "예: 아메리카노")

묶을 제품:
  • 아메리카노(S)
  • 아메리카노(M)
  • 아메리카노(L)
```
- Group name field is required; [다음 단계] disabled if empty.
- Group name must be unique among existing entity display names.

**그룹 이동:**
```
이동할 대상 그룹을 선택해 주세요.

  [그룹 선택 드롭다운]  (lists all existing grouped entities)

이동할 제품:
  • 콜드브루(S)
```
- Dropdown lists existing grouped entities by display name.
- [다음 단계] disabled until a target group is selected.
- If no existing groups are available (no grouped entities), this action is unavailable.

**그룹 해제:**
```
아래 그룹을 개별 제품으로 분리할 거예요.

  그룹: 아메리카노  →  아메리카노(S), 아메리카노(M), 아메리카노(L)

분리하면 각 제품이 독립적으로 분석돼요.
```
- No additional input required. [다음 단계] is always enabled once this action is selected.

**이름 변경:**
```
새 그룹 이름을 입력해 주세요.

  현재 이름: 아메리카노
  새 이름: [입력 필드]
```
- New name field is required; [다음 단계] disabled if empty or unchanged.
- New name must be unique among existing entity display names.

**Navigation:**
- [이전 단계] returns to Step 1 (selection is preserved).
- [취소] aborts wizard (with unsaved changes guard).

---

#### Step 3: 검토/저장 (Review and Save)

**Purpose:** Show a human-readable summary of the pending change before committing.

**Display examples by action:**

**그룹 만들기:**
```
다음과 같이 그룹을 만들 거예요.

  새 그룹:  아메리카노
  포함 제품: 아메리카노(S), 아메리카노(M), 아메리카노(L)
```

**그룹 이동:**
```
다음 제품을 '아메리카노' 그룹으로 이동할 거예요.

  이동할 제품: 콜드브루(S)
  대상 그룹:   아메리카노
```

**그룹 해제:**
```
'아메리카노' 그룹을 해제할 거예요.

  해제 후 개별 제품으로:
  • 아메리카노(S)
  • 아메리카노(M)
  • 아메리카노(L)
```

**이름 변경:**
```
그룹 이름을 변경할 거예요.

  기존 이름:  아메리카노
  새 이름:    에스프레소 베이스
```

**Navigation:**
- [저장하기] commits the change (see Section 5.5).
- [이전 단계] returns to Step 2.
- [취소] aborts wizard (with unsaved changes guard).

### 5.5 Save Behavior

On [저장하기]:

1. Apply the action to the in-memory `grouping` state object.
2. Serialize the updated grouping to `pgm_product_group_map.csv` format (see Section 5.7).
3. Write the serialized data to IndexedDB under key `'product_group_map'`.
4. Trigger a full data re-aggregation:
   - Quadrant chart re-renders with entities as units.
   - Product table re-renders with aggregated rows.
   - All metrics (revenue, customer counts, gravity scores) re-aggregate by `entity_id`.
5. Return the panel to the entry state (Step 1 summary view), reflecting the updated grouping counts.

**Aggregation rules on group save:**
- Additive fields (e.g., `product_order_cnt_1y`, `first_customer_cnt`, `repurchase_customer_cnt_90d`, `revenue_90d`): summed across all members of the entity.
- Rate/score fields (e.g., `Entry_Gravity_Score`, `Expansion_Gravity_Score`, gravity scores): weighted average across members, using order count (`product_order_cnt_1y`) as the weight. If order count is unavailable, use a simple arithmetic mean.

### 5.6 Grouping State Initialization

On app startup, read `pgm_product_group_map.csv` from IndexedDB (`key: 'product_group_map'`). Parse it to build the `grouping` runtime object:

1. For each row where `status === 'grouped'`: add `product_id` to `entityIdToMemberIds[group_id]`; set `entityIdToName[group_id] = group_name`.
2. For each row where `status === 'ungrouped'`: add `product_id` to `ungroupedProductIds`; set `entity_id = product_id`.
3. Populate `rawNameById` from any available product name data (e.g., `pgm_scored.csv` `product_name` field).
4. Populate `groupedEntityIds` as all keys of `entityIdToMemberIds`.

If `pgm_product_group_map.csv` is absent: all products are treated as ungrouped. `entity_id === product_id` for every product. No UI error is shown.

### 5.7 `pgm_product_group_map.csv` Schema

This file is the persistence format for the grouping state. It is both exported by the app and read on startup.

| Column | Type | Required | Description |
|---|---|---|---|
| `product_id` | string | Yes | Raw SKU identifier, matches `product_id` in data files |
| `status` | string | Yes | `'grouped'` — product belongs to a group; `'ungrouped'` — product stands alone |
| `group_id` | string | If grouped | Identifier for the entity this product belongs to. Must be stable across renames. Empty string or omitted if ungrouped |
| `group_name` | string | If grouped | Human-readable display name for the entity. Empty string or omitted if ungrouped |
| `rule` | string | Yes | How this mapping was set: `'manual'` (user-defined), `'exact_name'` (auto-matched by identical name), `'normalized_prefix'` (auto-matched by name prefix normalization) |
| `updated_at` | string | Yes | ISO 8601 datetime of last modification (e.g., `2026-03-25T09:00:00.000Z`) |

**Example rows:**

```csv
product_id,status,group_id,group_name,rule,updated_at
아메리카노(S),grouped,entity_001,아메리카노,manual,2026-03-25T09:00:00.000Z
아메리카노(M),grouped,entity_001,아메리카노,manual,2026-03-25T09:00:00.000Z
아메리카노(L),grouped,entity_001,아메리카노,manual,2026-03-25T09:00:00.000Z
콜드브루,ungrouped,,,manual,2026-03-25T09:00:00.000Z
```

**Rules:**
- Each `product_id` appears exactly once.
- `group_id` must not change when `group_name` is renamed (rename only updates `group_name`).
- All rows for members of the same group must share the same `group_id` and `group_name`.
- When a user saves a change, `updated_at` is set to the current UTC time for all affected rows.

### 5.8 Import and Export

#### Export (내보내기)

- Triggered by the "내보내기" button on the entry state.
- Serializes the current `grouping` runtime state to CSV per the schema in Section 5.7.
- Downloads as `pgm_product_group_map.csv`.
- All rows with `status === 'grouped'` are written first (sorted by `group_id`, then `product_id`), followed by `status === 'ungrouped'` rows (sorted by `product_id`).
- Does not close the settings panel.

#### Import (가져오기)

- Triggered by the "가져오기" button on the entry state.
- Opens a file picker (accepts `.csv` only).
- On file selection:
  1. Parse the CSV (PapaParse, `header: true`, `dynamicTyping: false`).
  2. Validate: required columns (`product_id`, `status`, `rule`, `updated_at`) must be present. If validation fails, show error:
     > "파일 형식이 맞지 않아요. `pgm_product_group_map.csv` 형식의 파일을 올려 주세요."
  3. If valid: overwrite IndexedDB entry under key `'product_group_map'` with the new data.
  4. Rebuild the `grouping` runtime state from the imported data.
  5. Trigger full data re-aggregation (same as Section 5.5 save behavior).
  6. Show success feedback:
     > "그룹 설정을 가져왔어요. N개 그룹, M개 제품이 적용됐어요."
- **Caution:** Import completely replaces the current grouping. There is no merge behavior.
- If the operator has unsaved wizard changes when they click "가져오기", show the unsaved changes guard first.

---

## 6. Tab 2: 데이터 관리 (Data Management)

This tab has three independent sub-sections, stacked vertically:

```
─────────────────────────────
 CSV 업로드
─────────────────────────────
 로컬 동기화
─────────────────────────────
 저장 데이터 초기화
─────────────────────────────
```

### 6.1 CSV 업로드

**Purpose:** Let operators manually upload CSV files to populate or update the dashboard data stored in IndexedDB.

**Layout:**

```
CSV 업로드

파일을 선택하거나 끌어다 놓으세요.
지원 파일 목록은 아래를 참고해 주세요.

  [파일 선택하기]

지원 파일:
  pgm_scored.csv, pgm_product_demand_gravity.csv, ...
```

**Upload flow:**

1. Operator selects one or more files via the file picker (or drag-and-drop if supported).
2. For each file:
   a. Match the filename against all `filename` (canonical) and `aliases` values across all entries in `REQUIRED_FILES` (defined in `app.js` lines 10–116).
   b. **On match:** Parse the CSV using PapaParse (`header: true`, `dynamicTyping: true`). Store the parsed data to IndexedDB under the matching logical key. After all files are stored, trigger a full app data reload.
   c. **On no match:** Display an error for that file:
      > `"[filename]"은 인식할 수 없는 파일이에요. 지원 파일 목록에 있는 이름의 파일을 올려 주세요.`
3. Multiple files can be uploaded in a single operation. Each file is matched and stored independently. A file that fails matching does not prevent other files from being processed.

**Post-upload feedback (per file):**

| Result | Feedback |
|---|---|
| Success | "[canonical_filename] 업로드 완료" with green indicator |
| No match | "[filename] — 인식할 수 없는 파일이에요" with red indicator |
| Parse error | "[filename] — 파일을 읽는 중 오류가 발생했어요" with red indicator |

**Supported files reference table** (derived from `REQUIRED_FILES` in `app.js`):

| Logical Key | Canonical Filename | Accepted Aliases |
|---|---|---|
| `brandScore` | `brand_score.csv` | _(none)_ |
| `anchorScored` | `pgm_scored.csv` | `anchor_scored.csv` |
| `anchorTransition` | `pgm_entry_to_expansion_transition.csv` | `anchor_transition.csv` |
| `productDemandGravity` | `pgm_product_demand_gravity.csv` | `product_demand_gravity.csv` |
| `productTransitionEdge` | `pgm_product_transition_edge.csv` | `product_transition_edge.csv` |
| `returnGravityLoopDetail` | `pgm_return_gravity_loop_detail.csv` | `return_gravity_loop_detail.csv` |
| `insightDemandGraphNodes` | `_insight_demand_graph_nodes.csv` | `insight_demand_graph_nodes.csv` |
| `insightDemandGraphEdges` | `_insight_demand_graph_edges.csv` | `insight_demand_graph_edges.csv` |
| `insightDemandGraphPatterns` | `_insight_demand_graph_patterns.csv` | `insight_demand_graph_patterns.csv` |
| `cartAnchor` | `pgm_basket_gravity.csv` | `cart_anchor.csv` |
| `cartAnchorDetail` | `pgm_basket_gravity_detail.csv` | `cart_anchor_detail.csv` |
| `aaCohortJourney` | `_insight_entry_cohort_journey.csv` | `_insight_aa_cohort_journey.csv`, `aa_cohort_journey.csv` |
| `aaTransitionPath` | `_insight_entry_transition_path.csv` | `_insight_aa_transition_path.csv`, `aa_transition_path.csv` |
| `biiWindow` | `_insight_bii_window.csv` | `bii_window.csv`, `brand_impact_windows.csv`, `brand_impact_index.csv`, `purchase_activation_windows.csv`, `purchase_activation_index.csv` |
| `brandImpactTimeseries` | `_insight_brand_impact_timeseries.csv` | `brand_impact_timeseries.csv`, `purchase_activation_timeseries.csv`, `_insight_purchase_activation_timeseries.csv` |
| `brandImpactDailyPulse` | `_insight_brand_impact_daily_pulse.csv` | `brand_impact_daily_pulse.csv`, `purchase_activation_daily_pulse.csv`, `_insight_purchase_activation_daily_pulse.csv` |
| `brandRevenueTimeseries` | `_insight_brand_revenue_timeseries.csv` | `brand_revenue_timeseries.csv` |
| `brandPurchaseDriverTimeseries` | `_insight_purchase_activation_driver_timeseries.csv` | `purchase_activation_driver_timeseries.csv`, `_insight_brand_purchase_driver_timeseries.csv`, `brand_purchase_driver_timeseries.csv` |
| `brandStructureTimeseries` | `_insight_brand_structure_timeseries.csv` | `brand_structure_timeseries.csv` |
| `apfActionRules` | `_insight_pgm_action_rules.csv` | `_insight_apf_action_rules.csv`, `apf_action_rules.csv` |
| `productGroupMap` | `pgm_product_group_map.csv` | `product_group_map.csv`, `_meta_product_group_map.csv` |

The source of truth for this table is `REQUIRED_FILES` in `app.js` (lines 10–116). If the two diverge, `app.js` governs.

### 6.2 로컬 동기화 (Local Reload)

**Purpose:** Load CSV files from the local `/data/` directory path using `fetch()`. Useful when running the dashboard from a local server where files are placed alongside the HTML.

**Layout:**

```
로컬 동기화

/data/ 폴더에 있는 CSV 파일을 불러와요.

  [동기화 시작하기]

마지막 동기화: 2026-03-25 09:00  (또는 "동기화 기록 없음")
```

**Sync flow:**

1. Operator clicks "동기화 시작하기". Button shows a loading indicator.
2. For each entry in `REQUIRED_FILES`, fetch the canonical filename from the relative path `./data/<filename>` (e.g., `fetch('./data/pgm_scored.csv')`).
3. **If fetch succeeds (HTTP 200):** Parse and store to IndexedDB (same as upload flow). Mark file as synced.
4. **If fetch fails (404 or network error):** Silently skip. Do not show an error for missing files — absence is expected for optional files.
5. After all fetches complete: trigger app data reload.
6. Show a summary:
   > "동기화 완료: N개 파일을 불러왔어요."
   > (If 0 files loaded): "불러온 파일이 없어요. /data/ 폴더에 파일이 있는지 확인해 주세요."

**Notes:**
- Aliases are not tried during local sync; only canonical filenames are fetched.
- If `pgm_scored.csv` is found and loaded, the main dashboard data will refresh. If it is absent, the existing IndexedDB data (if any) remains intact.

### 6.3 저장 데이터 초기화 (Reset Stored Data)

**Purpose:** Clear all CSV data from IndexedDB, returning the dashboard to an empty state.

**Layout:**

```
저장 데이터 초기화

저장된 CSV 데이터를 모두 삭제해요.
제품 그룹 설정은 별도로 유지돼요.

  [데이터 초기화]  (destructive action styling — red or outlined warning style)
```

**Reset flow:**

1. Operator clicks "데이터 초기화".
2. Show confirmation dialog before any data is deleted:
   > **정말 초기화할까요?**
   > 저장된 모든 CSV 데이터가 삭제돼요. 이 작업은 되돌릴 수 없어요.
   > 제품 그룹 설정(`pgm_product_group_map`)은 삭제되지 않아요.
   >
   > [취소]  [초기화하기]
3. On "초기화하기":
   a. Clear all entries from the IndexedDB `csv_files` object store **except** the `'product_group_map'` key.
   b. Clear `AppState.data` (set all fields to `null` or `[]` per their default types), preserving the grouping runtime state.
   c. Trigger a UI re-render. The main dashboard shows an empty state with an upload CTA.
4. On "취소": dismiss dialog, no action.

**What is cleared:**
- All `REQUIRED_FILES` entries stored in IndexedDB (except `productGroupMap`).

**What is preserved:**
- `'product_group_map'` key in IndexedDB.
- The `grouping` runtime state in memory.
- `AppState.viewState` (panel positions, active tabs, etc.).

**Implementation note:** If the implementation requires a full page reload after reset (e.g., to cleanly reinitialize chart instances), that is acceptable. The empty state should show:

```
데이터가 없어요.
CSV 파일을 업로드하면 대시보드를 시작할 수 있어요.

  [파일 업로드하기]
```

The "파일 업로드하기" CTA should link directly to or open the CSV 업로드 section within the Settings Panel.

---

## 7. AppState Integration

### 7.1 Settings-related state

```javascript
AppState.viewState.settings = {
  activeTab: 'grouping'  // 'grouping' | 'data'
}
```

`activeTab` is updated immediately on tab click (before the unsaved changes guard fires — the guard can revert if the operator chooses to stay). Persist this value in-memory; it does not need to survive a page reload.

### 7.2 Grouping state location

The `grouping` runtime object (Section 5.2) lives outside of `AppState` or as a named property within it (e.g., `AppState.helpers.grouping` or a module-level constant). Its authoritative source at startup is IndexedDB key `'product_group_map'`. During a session it is mutated in-place and re-persisted on each save.

---

## 8. UI/UX Requirements

| Requirement | Detail |
|---|---|
| Language register | All copy uses 해요체 Korean (e.g., "저장할게요", "삭제됐어요") |
| Panel positioning | Must not occlude the main chart view when open |
| Step indicator | Wizard displays current step as "① ② ③" with active step highlighted |
| Destructive actions | Reset and import-override buttons use a visually distinct destructive style (e.g., red border or red text) |
| Loading states | File upload, local sync, and save all show loading indicators while async operations run |
| Error states | Inline error messages within the section that failed; no full-panel error takeover |
| Empty states | When no data and no groups exist, both tabs show descriptive empty state copy with a primary CTA |
| Keyboard | Escape closes the panel (with unsaved changes guard if applicable) |
| Focus management | When panel opens, focus moves to the panel header or first interactive element; when panel closes, focus returns to the "설정" sidebar button |

---

## 9. Edge Cases and Implementation Notes

| Scenario | Expected behavior |
|---|---|
| Upload a file that matches `productGroupMap` (e.g., `pgm_product_group_map.csv`) | Treat as a grouping import: follow the import validation flow (Section 5.8), not just a raw CSV store |
| Group with only 1 member after 그룹 해제 | That product becomes ungrouped; the entity is removed from `groupedEntityIds` |
| Rename to an existing group name | Show inline validation error: "이미 사용 중인 이름이에요." |
| 그룹 이동 when target group would exceed a reasonable member count | No hard limit enforced; allow any size. Consider a soft warning at >20 members |
| Import CSV with unknown columns | Ignore unknown columns; proceed with recognized ones |
| Import CSV with missing `updated_at` | Backfill with current UTC timestamp on import |
| Local sync triggered when no `/data/` folder exists | All fetches return 404; silently skip all; show "불러온 파일이 없어요" message |
| Reset while wizard is open | Close wizard first (unsaved changes guard), then proceed with reset |
| App reload triggered while Settings Panel is open | Panel closes on reload; `activeTab` preference is not persisted across reloads |
