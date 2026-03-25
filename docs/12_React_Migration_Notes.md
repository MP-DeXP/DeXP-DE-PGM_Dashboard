# React Migration Notes — APF Dashboard

Version: 0.1
Status: Draft — Behavioral Contract
Audience: React frontend developers migrating the APF Dashboard from Vanilla JS to React.

This document is a behavioral contract. It defines what must be preserved, what must not change, and where the hard problems are. It is not a tutorial for React.

---

## 1. Current Architecture Overview

The current app is Vanilla JS with no build step and no framework. All logic runs in a single global `AppState` object. DOM manipulation is imperative. There is no module system — scripts are concatenated in load order.

The target is a React-based SPA. The migration must preserve all behavioral invariants described in this document. UI changes are out of scope unless explicitly noted.

---

## 2. AppState Structure and React Mapping

### 2.1 Current structure (`app.js`)

```javascript
AppState = {
  data: { /* loaded CSV data */ },
  rawData: { /* original CSV rows */ },
  viewState: {
    products: {
      sortCol, sortDesc, coreSortKey,
      searchQuery,
      chartView,           // 'quadrant' | 'demand-graph'
      demandGraphTab,      // 'transition' | 'basket'
      sidePanelOpen,
      quadrant: {
        selectedId,        // currently selected product
        history,           // navigation back stack
        filters,
        scaleMode,         // 'focus' | 'raw'
        edgeMode,          // 'representative' | 'convergence'
        groupingEditorOpen
      }
    }
  },
  charts: {},   // Chart.js instances (ephemeral, DOM-tied)
  helpers: {}   // cached computations (pan state, drag state, scroll tokens)
}
```

### 2.2 State split in React

| Field | React home |
|---|---|
| `AppState.data` | Context (`DataProvider`) — large, rarely changes |
| `AppState.rawData` | Same Context as `data` |
| `selectedId` | Local state in `<ProductPage>` (or URL param) |
| `scaleMode` | Local state in `<ProductPage>` |
| `edgeMode` | Local state in `<ProductPage>` |
| `demandGraphTab` | Local state in `<DemandGraphModal>` |
| `sidePanelOpen` | Local state in `<ProductPage>` |
| `searchQuery` | Local state in `<ProductPage>` |
| `sortCol`, `sortDesc` | Local state in `<ProductTable>` |
| `charts` (Chart.js instances) | `useRef` — never React state |
| `helpers` (pan offset, drag, scroll tokens) | `useRef` — never React state |
| `quadrant.history` | Local state in `<ProductPage>` |
| `quadrant.filters` | Local state in `<ProductPage>` |
| `quadrant.groupingEditorOpen` | Local state in `<SettingsPanel>` |

**Rule:** Any value that changes on every pointer event (pan offset, drag position, scroll token) must live in a ref. Putting these in React state will cause frame-rate regressions.

---

## 3. Suggested Component Boundaries

```
<DataProvider>
  └─ <ProductPage>
       ├─ <ProductQuadrantChart>
       │    └─ <QuadrantTooltip>
       ├─ <QuadrantSidePanel>
       ├─ <DemandGraphModal>
       ├─ <ProductTable>
       └─ <SettingsPanel>
```

### 3.1 Component contracts

**`<DataProvider>`**
- Loads all CSVs from IndexedDB on mount.
- Runs all `transformX()` functions after load.
- Provides `{ data, rawData, isLoading, error }` via Context.
- Children do not render until `isLoading === false`.
- Alias resolution (see Section 6) happens here, not in consumers.

**`<ProductPage>`**
- Top-level page component.
- Owns: `selectedId`, `scaleMode`, `edgeMode`, `sidePanelOpen`, `searchQuery`, `quadrant.history`, `quadrant.filters`, `chartView`.
- Handles the four `selectedId` entry points (see Section 4.3).
- Does not own loaded data — reads from Context.

**`<ProductQuadrantChart>`**
- SVG-based. Does not use Canvas or Chart.js.
- Receives the output of `buildQuadrantModel()` as a prop.
- Does not compute the quadrant model internally — the model is passed in.
- Pan state (offset x/y) lives in refs inside this component, not in `<ProductPage>`.
- Exposes an `onSelect(id)` callback for bubble clicks.

**`<QuadrantSidePanel>`**
- Receives `selectedProduct` object and `status` string as props.
- Does not read `selectedId` from context — it receives the resolved product.
- Contains the "추가구매 제품 보기" CTA (see Section 4.4).

**`<DemandGraphModal>`**
- Receives `selectedId` and `tab` ('transition' | 'basket') as props.
- Opens on CTA click from `<QuadrantSidePanel>`.
- Owns demandGraphTab local state internally.
- Transition and basket graphs are separate child components (see Section 5.3).

**`<ProductTable>`**
- Receives `coreDemandModel` as a prop.
- Owns its own sort state (`sortCol`, `sortDesc`) locally.
- Exposes an `onSelect(id)` callback for row clicks.

**`<SettingsPanel>`**
- Grouping wizard + data management UI.
- Owns `groupingEditorOpen` locally.

**`<QuadrantTooltip>`**
- Positioned overlay, rendered inside `<ProductQuadrantChart>`.
- Controlled entirely by pointer state held in a ref.
- Position and content are written to the DOM via ref, not via state re-render.

---

## 4. Critical Rendering Invariants

These invariants must be preserved exactly. Violations will produce incorrect outputs silently.

### 4.1 Quadrant center is computed from ALL products

The quadrant center (median x, median y) **must be computed from the full `AppState.data` product set**, not from any filtered or viewport-clipped subset.

```javascript
// CORRECT
const center = computeMedianCenter(data.anchorScored); // all products

// WRONG — do not do this
const center = computeMedianCenter(visibleProducts); // filtered by viewport
```

Scale mode changes the view transform but does not change the center. Do not recompute the center when `scaleMode` changes.

### 4.2 Scale mode toggle must not re-center the viewport

When `scaleMode` toggles between `'focus'` and `'raw'`, the pan offset must be preserved. The viewport does not jump.

The pan offset lives in a ref (`panOffsetRef`), not in state. The `scaleMode` state change triggers a re-render that applies a new scale transform to the existing pan offset — it does not reset the offset.

```javascript
// In <ProductQuadrantChart>
const panOffsetRef = useRef({ x: 0, y: 0 });

// On scaleMode change: apply new scale, keep panOffsetRef unchanged
useLayoutEffect(() => {
  applyTransform(svgRef.current, panOffsetRef.current, scaleMode);
}, [scaleMode]);
```

### 4.3 selectedId has exactly four entry points

`selectedId` is a single piece of state owned by `<ProductPage>`. All four entry points write to the same setter:

| Entry point | How it calls the setter |
|---|---|
| Quadrant bubble click | `onSelect(id)` callback from `<ProductQuadrantChart>` |
| Product table row click | `onSelect(id)` callback from `<ProductTable>` |
| Demand graph node click | `focusQuadrantFromDemandDriver(id)` → same setter |
| URL deeplink (`?focus=<id>`) | Read on mount in `useEffect([], [])` in `<ProductPage>` |

There must be no secondary or shadow copies of `selectedId` in other components or in context. If a component needs `selectedId`, it receives it as a prop from `<ProductPage>`.

### 4.4 "추가구매 제품 보기" CTA disabled state

The CTA button must be evaluated **before render**, not inside an event handler.

```javascript
// In <QuadrantSidePanel> or <ProductPage>
const transitionEntitySet = buildTransitionEntitySet(data, selectedId);
const ctaEnabled = transitionEntitySet.has(selectedId);

// Pass to panel:
<QuadrantSidePanel
  selectedProduct={selectedProduct}
  demandCtaEnabled={ctaEnabled}
  onDemandCtaClick={...}
/>
```

Rendering the button as enabled and then disabling it on click is not acceptable.

### 4.5 Zone classification uses raw median center

Quadrant zone classification (hero / entry-only / expansion-only / phaseout) uses the same raw median center as the quadrant rendering. The `scaleMode` value is irrelevant to zone assignment.

```javascript
// CORRECT — zone classification
const zone = classifyZone(product, rawMedianCenter);

// WRONG
const zone = classifyZone(product, scaledCenter); // never pass a scaled center
```

---

## 5. DOM-Dependent Patterns

These patterns require careful treatment in React. Each one is documented with its current vanilla pattern and the required React equivalent.

### 5.1 Demand graph edge paths (measurement required)

**Current pattern:**
1. Render nodes in DOM.
2. `rAF + setTimeout` (`scheduleDemandGraphEdgeLayout`) measures `getBoundingClientRect` on each node.
3. SVG edge paths are computed from measured positions.
4. Edges are written to DOM.

**React equivalent:** Two-pass render using `useLayoutEffect`.

```jsx
function DemandGraphCanvas({ nodes, edges }) {
  const nodeRefs = useRef({});
  const [edgePaths, setEdgePaths] = useState([]);

  // Pass 1: nodes render (no edges yet)
  // Pass 2: measure, compute paths, set state
  useLayoutEffect(() => {
    const measured = {};
    for (const [id, el] of Object.entries(nodeRefs.current)) {
      if (el) measured[id] = el.getBoundingClientRect();
    }
    setEdgePaths(computeEdgePaths(measured, edges));
  }, [nodes, edges]);

  return (
    <div className="demand-graph-canvas">
      {nodes.map(n => (
        <DemandGraphNode key={n.id} node={n} ref={el => nodeRefs.current[n.id] = el} />
      ))}
      <svg className="edge-layer">
        {edgePaths.map(p => <path key={p.id} d={p.d} />)}
      </svg>
    </div>
  );
}
```

If the container can resize, attach a `ResizeObserver` inside the `useLayoutEffect` to re-measure on resize.

### 5.2 Transition graph layout (hidden-pass measurement)

**Current pattern:** `buildTransitionFixedLayout`
1. Cards are rendered.
2. Card widths are measured.
3. Positions are recalculated from measured widths.
4. Cards are repositioned.

**React equivalent:** Render in a hidden pass, measure, then position.

```jsx
function TransitionGraph({ rows }) {
  const containerRef = useRef(null);
  const [layout, setLayout] = useState(null);

  useLayoutEffect(() => {
    if (!containerRef.current) return;
    const cardEls = containerRef.current.querySelectorAll('[data-card-id]');
    const widths = {};
    cardEls.forEach(el => { widths[el.dataset.cardId] = el.offsetWidth; });
    setLayout(buildTransitionFixedLayout(rows, widths));
  }, [rows]);

  return (
    <div ref={containerRef} style={{ visibility: layout ? 'visible' : 'hidden' }}>
      {renderCards(rows, layout)}
    </div>
  );
}
```

The `visibility: hidden` pass prevents a flash of un-laid-out content without causing a layout skip.

### 5.3 Basket graph layout (no measurement needed)

`getBasketGraphPosition` computes angle-based positions from percent-to-scene coordinate mapping. Positions are deterministic given the data — no DOM measurement is required.

This component can be a straightforward SVG render with no two-pass logic. Do not add unnecessary measurement passes.

**Basket and transition graphs must remain separate components.** They have different layout engines, different data sources, and different node interaction patterns. Do not merge them into a shared generic graph component.

### 5.4 Panel height synchronization

**Current pattern:** `syncQuadrantPanelHeights` runs on resize, sets explicit heights to synchronize the quadrant chart and side panel.

**React equivalent:**

```jsx
function ProductPage() {
  const containerRef = useRef(null);

  useLayoutEffect(() => {
    const ro = new ResizeObserver(() => {
      if (!containerRef.current) return;
      const h = containerRef.current.offsetHeight;
      containerRef.current.style.setProperty('--panel-height', `${h}px`);
    });
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, []);

  return <div ref={containerRef} className="product-layout">...</div>;
}
```

Use CSS custom properties (`--panel-height`) rather than writing inline styles directly to child elements.

### 5.5 Demand graph pan (refs, not state)

**Current pattern:** `startDemandGraphPan` tracks pointer delta from a captured origin.

**React equivalent:** All pan state lives in refs. No state setter is called during `pointermove`.

```javascript
const panOriginRef = useRef(null);
const panOffsetRef = useRef({ x: 0, y: 0 });

function onPointerDown(e) {
  panOriginRef.current = { x: e.clientX - panOffsetRef.current.x, y: e.clientY - panOffsetRef.current.y };
}

function onPointerMove(e) {
  if (!panOriginRef.current) return;
  panOffsetRef.current = { x: e.clientX - panOriginRef.current.x, y: e.clientY - panOriginRef.current.y };
  applyPanTransform(graphEl, panOffsetRef.current); // direct DOM write
}

function onPointerUp() {
  panOriginRef.current = null;
}
```

Calling `setState` on every `pointermove` event will produce visible frame drops and is not acceptable.

---

## 6. Data Loading Architecture

### 6.1 Current flow

1. App init → `loadDataFromDB` / `loadOptionalDataFromDB`
2. All CSVs loaded in parallel from IndexedDB
3. `transformX()` functions convert raw rows
4. `AppState.data` populated
5. Page renders

### 6.2 React equivalent

```jsx
// DataProvider.jsx
function DataProvider({ children }) {
  const [state, setState] = useState({ data: null, rawData: null, isLoading: true, error: null });

  useEffect(() => {
    async function load() {
      try {
        const rawResults = await loadAllFromIndexedDB(REQUIRED_FILES); // parallel reads
        const data = transformAll(rawResults);                          // all transforms
        const rawData = rawResults;
        setState({ data, rawData, isLoading: false, error: null });
      } catch (err) {
        setState({ data: null, rawData: null, isLoading: false, error: err });
      }
    }
    load();
  }, []);

  if (state.isLoading) return <LoadingScreen />;
  if (state.error) return <ErrorScreen error={state.error} />;

  return (
    <DataContext.Provider value={state}>
      {children}
    </DataContext.Provider>
  );
}
```

### 6.3 REQUIRED_FILES — single source of truth

The `REQUIRED_FILES` config from `app.js` (lines 10–116) must be kept in one place in the React codebase. The recommended location is `src/config/requiredFiles.js`. It must not be duplicated or partially re-declared in individual components.

The full current config is:

```javascript
// src/config/requiredFiles.js
export const REQUIRED_FILES = {
  brandScore:                    { key: 'brand_score',                        filename: 'brand_score.csv' },
  anchorScored:                  { key: 'anchor_scored',                      filename: 'pgm_scored.csv',                               aliases: ['anchor_scored.csv'] },
  anchorTransition:              { key: 'anchor_transition',                  filename: 'pgm_entry_to_expansion_transition.csv',         aliases: ['anchor_transition.csv'] },
  productDemandGravity:          { key: 'product_demand_gravity',             filename: 'pgm_product_demand_gravity.csv',                aliases: ['product_demand_gravity.csv'] },
  productTransitionEdge:         { key: 'product_transition_edge',            filename: 'pgm_product_transition_edge.csv',               aliases: ['product_transition_edge.csv'] },
  returnGravityLoopDetail:       { key: 'return_gravity_loop_detail',         filename: 'pgm_return_gravity_loop_detail.csv',            aliases: ['return_gravity_loop_detail.csv'] },
  insightDemandGraphNodes:       { key: 'insight_demand_graph_nodes',         filename: '_insight_demand_graph_nodes.csv',               aliases: ['insight_demand_graph_nodes.csv'] },
  insightDemandGraphEdges:       { key: 'insight_demand_graph_edges',         filename: '_insight_demand_graph_edges.csv',               aliases: ['insight_demand_graph_edges.csv'] },
  insightDemandGraphPatterns:    { key: 'insight_demand_graph_patterns',      filename: '_insight_demand_graph_patterns.csv',            aliases: ['insight_demand_graph_patterns.csv'] },
  cartAnchor:                    { key: 'cart_anchor',                        filename: 'pgm_basket_gravity.csv',                       aliases: ['cart_anchor.csv'] },
  cartAnchorDetail:              { key: 'cart_anchor_detail',                 filename: 'pgm_basket_gravity_detail.csv',                 aliases: ['cart_anchor_detail.csv'] },
  aaCohortJourney:               { key: 'aa_cohort_journey',                  filename: '_insight_entry_cohort_journey.csv',             aliases: ['_insight_aa_cohort_journey.csv', 'aa_cohort_journey.csv'] },
  aaTransitionPath:              { key: 'aa_transition_path',                 filename: '_insight_entry_transition_path.csv',            aliases: ['_insight_aa_transition_path.csv', 'aa_transition_path.csv'] },
  caProfile:                     { key: 'ca_profile',                         filename: '_insight_basket_gravity_profile.csv',           aliases: ['_insight_ca_profile.csv', 'ca_profile.csv'] },
  biiWindow:                     { key: 'bii_window',                         filename: '_insight_bii_window.csv',                       aliases: ['bii_window.csv', 'brand_impact_windows.csv', 'brand_impact_index.csv', 'purchase_activation_windows.csv', 'purchase_activation_index.csv'] },
  brandImpactTimeseries:         { key: 'brand_impact_timeseries',            filename: '_insight_brand_impact_timeseries.csv',          aliases: ['brand_impact_timeseries.csv', 'purchase_activation_timeseries.csv', '_insight_purchase_activation_timeseries.csv'] },
  brandImpactDailyPulse:         { key: 'brand_impact_daily_pulse',           filename: '_insight_brand_impact_daily_pulse.csv',         aliases: ['brand_impact_daily_pulse.csv', 'purchase_activation_daily_pulse.csv', '_insight_purchase_activation_daily_pulse.csv'] },
  brandRevenueTimeseries:        { key: 'brand_revenue_timeseries',           filename: '_insight_brand_revenue_timeseries.csv',         aliases: ['brand_revenue_timeseries.csv'] },
  brandPurchaseDriverTimeseries: { key: 'brand_purchase_driver_timeseries',   filename: '_insight_purchase_activation_driver_timeseries.csv', aliases: ['purchase_activation_driver_timeseries.csv', '_insight_brand_purchase_driver_timeseries.csv', 'brand_purchase_driver_timeseries.csv'] },
  brandStructureTimeseries:      { key: 'brand_structure_timeseries',         filename: '_insight_brand_structure_timeseries.csv',       aliases: ['brand_structure_timeseries.csv'] },
  apfActionRules:                { key: 'apf_action_rules',                   filename: '_insight_pgm_action_rules.csv',                 aliases: ['_insight_apf_action_rules.csv', 'apf_action_rules.csv'] },
  productGroupMap:               { key: 'product_group_map',                  filename: 'pgm_product_group_map.csv',                     aliases: ['product_group_map.csv', '_meta_product_group_map.csv'] },
};
```

### 6.4 Alias resolution contract

When a user uploads a CSV file, the system must match it against both the primary `filename` and the `aliases` array for each entry in `REQUIRED_FILES`. This matching logic must run in `DataProvider` (or a utility it calls), not in upload UI components.

```javascript
// src/utils/resolveFileKey.js
export function resolveFileKey(uploadedFilename, requiredFiles) {
  for (const [configKey, config] of Object.entries(requiredFiles)) {
    if (uploadedFilename === config.filename) return config.key;
    if (config.aliases?.includes(uploadedFilename)) return config.key;
  }
  return null; // unrecognized file
}
```

This function must not be inlined at call sites. It is the single resolution path.

---

## 7. URL State

### 7.1 Required URL params

| Param | Value | Semantics |
|---|---|---|
| `?focus=<id>` | product ID string | Deep-link to a selected product on mount |
| `?tab=<tab>` | `transition` or `basket` | Optional: open demand graph to a specific tab |

### 7.2 On-mount behavior

`<ProductPage>` reads `?focus` in a `useEffect` with an empty dependency array (`[]`). This fires after first render. If a valid product ID is found, it calls the same `setSelectedId` setter used by all other entry points (see Section 4.3).

```javascript
useEffect(() => {
  const params = new URLSearchParams(window.location.search);
  const focusId = params.get('focus');
  if (focusId && isValidProductId(focusId, data)) {
    setSelectedId(focusId);
  }
}, []); // runs once on mount
```

Invalid or unrecognized IDs must be silently ignored (do not throw).

### 7.3 URL updates on selection

When `selectedId` changes via user interaction, update the URL using `history.replaceState` (not `pushState`). Deep-links must be shareable but back-button navigation within the page should not stack entries.

---

## 8. What Not to Do

These are anti-patterns specific to this migration:

1. **Do not compute quadrant center from filtered data.** The center is always from the full dataset. See Section 4.1.

2. **Do not put pan offset or drag position in React state.** Any field that updates on pointer events must be a ref. See Sections 4.2 and 5.5.

3. **Do not merge the transition graph and basket graph into one component.** They have different layout engines. See Section 5.3.

4. **Do not duplicate REQUIRED_FILES or alias arrays.** There is exactly one config object. See Section 6.3.

5. **Do not perform alias resolution in upload UI components.** It belongs in `DataProvider` / `resolveFileKey`. See Section 6.4.

6. **Do not render the "추가구매 제품 보기" CTA as enabled when the entity set does not include selectedId.** The check runs before render, not on click. See Section 4.4.

7. **Do not use `useEffect` for layout measurements.** Use `useLayoutEffect` whenever you need to read from the DOM before the browser paints. See Sections 5.1 and 5.2.

8. **Do not scatter `selectedId` into multiple components as local state.** It lives in `<ProductPage>` and flows down as props. See Section 4.3.

---

## 9. File Organization (Recommended)

```
src/
  config/
    requiredFiles.js       ← REQUIRED_FILES config (single source)
  context/
    DataContext.js         ← Context definition
    DataProvider.jsx       ← loads data, provides context
  pages/
    ProductPage.jsx        ← owns viewState, all selectedId writes
  components/
    ProductQuadrantChart/
      index.jsx
      useQuadrantPan.js    ← pan logic in a custom hook (refs only)
    QuadrantSidePanel/
    QuadrantTooltip/
    DemandGraphModal/
      index.jsx
      TransitionGraph.jsx  ← separate from basket
      BasketGraph.jsx      ← separate from transition
    ProductTable/
    SettingsPanel/
  utils/
    resolveFileKey.js      ← alias resolution
    buildQuadrantModel.js  ← pure function, no DOM dependency
    buildTransitionFixedLayout.js
    getBasketGraphPosition.js
    buildTransitionEntitySet.js
```

Pure model-building functions (`buildQuadrantModel`, `buildTransitionFixedLayout`, `getBasketGraphPosition`, `buildTransitionEntitySet`) must remain pure — no DOM access, no React imports. They are called from components but are independently testable.

---

## 10. Migration Sequence (Suggested Order)

This order minimizes the surface of broken state at each step.

1. `DataProvider` + `DataContext` — get data loading working first. All subsequent work depends on it.
2. `<ProductTable>` — simplest component, no DOM measurement, no pan.
3. `<ProductPage>` shell — establish `selectedId` ownership and the four entry points.
4. `<QuadrantSidePanel>` — receives props, no complex layout.
5. `<ProductQuadrantChart>` — SVG rendering + pan. Validate invariants 4.1 and 4.2 here.
6. `<DemandGraphModal>` → `<TransitionGraph>` first, then `<BasketGraph>`.
7. `<SettingsPanel>` — grouping wizard, data upload with alias resolution.
8. URL param handling — add after core selection flow is verified.

Do not attempt to migrate the entire state tree at once. Migrate one component boundary at a time and verify behavioral invariants against the Vanilla JS implementation before proceeding.
