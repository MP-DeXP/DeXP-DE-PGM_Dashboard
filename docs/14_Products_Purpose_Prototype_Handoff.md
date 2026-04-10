# Products Purpose Prototype Handoff

## Why this handoff exists

The dashboard now has a working purpose prototype layer, but it should be treated as a **prototype UI interpretation layer**, not the long-term owner of purpose logic.

This handoff records what was added in the dashboard, what was actually validated, where the current caveats are, and what should now move to core.

## Files touched / current prototype surface

Files touched:
- `products.js`
- `style.css`

Current prototype surface is split into two visible layers:

1. **Selected-product purpose guide**
   - asks: for the currently selected product, which purposes are usable / cautionary / weak / insufficient?
   - implemented in the current purpose-analysis and selected-use rendering flow in `products.js`

2. **Global purpose comparison layer**
   - asks: across the full product structure, which products look like the main or comparison candidates for each purpose?
   - rendered as a separate global review layer in `products.js`
   - styled in `style.css`

Net effect:
- dashboard usefulness improved
- but this is still **not strong operational guidance yet**

## Current layer split and invariant rules

### Selected-product purpose guide
- selection-dependent layer
- should evaluate the **currently selected product** against each purpose
- acceptable for this layer to change when selection changes

### Global purpose comparison layer
- selection-invariant layer
- should evaluate purpose candidates from the **overall product structure**, not from the currently selected product
- the selected product may be highlighted as "currently viewing," but selection must **not** change who the global top candidates are

This invariant matters because otherwise the dashboard turns a global comparison layer into a selection-biased explanation layer.

## What was validated in dashboard vs what is now delegated to core

### Validated in dashboard
- The UI split itself was useful:
  - one layer for "can I use this selected product for this purpose?"
  - one layer for "who are the overall candidates for this purpose?"
- Real-data review showed this split is easier to read than one mixed purpose block.
- The dashboard became better at surfacing caution, weak evidence, and compare-hold states instead of overclaiming certainty.

### Now delegated to core
- Purpose-candidate eligibility logic
- Purpose expected-effect logic
- Structured effect outputs
- Any reusable normalization needed to interpret transition / return / basket signals consistently

In other words:
- dashboard validated the **presentation split**
- core should now own the **actual logic outputs**

## Known caveats (include return-shape caveat)

1. **Return-shape caveat**
   - Real-data validation found that the frontend purpose logic only read return-strength sensibly after aggregating `pgm_return_gravity_loop_detail.csv` into the shape it expected.
   - This is a warning sign that the frontend is doing data-shape repair that should not live in UI code.

2. **Frontend-owned logic is still heuristic and fragile**
   - The current prototype recreates interpretation logic from available CSVs inside `products.js`.
   - That makes it harder to test, version, and keep aligned with core changes.

3. **Current usefulness is still limited**
   - The prototype is more useful than before, but it still does not amount to strong operational guidance.
   - It helps frame review; it does not yet justify confident action on its own.

4. **Expected effect should not remain as UI-only text logic**
   - Especially after the core addition of `01c_PGM_ProductExpectedEffect.ipynb` and `pipeline/purpose_expected_effect.py`, the dashboard should stop being the place where expected effect is inferred ad hoc.

## Why frontend should stop owning more of this logic

- **Logic quality**: purpose / effect interpretation is data logic, not presentation logic.
- **Testability**: core outputs are easier to validate than UI-embedded heuristics.
- **Consistency**: the same structured output can feed dashboard, API, and later production systems.
- **Calibration**: thresholds and evidence rules belong where they can be versioned and recalibrated centrally.
- **Selection safety**: keeping logic in the frontend increases the risk that global comparisons become contaminated by current-selection state.

Short version:
- frontend should render, annotate, and explain
- core should classify, score, and structure

## Immediate next integration priorities

1. **Integrate structured core outputs into the dashboard**
   - start consuming `pgm_product_role_map.csv`
   - start consuming `pgm_product_purpose_effects.csv`

2. **Stop recreating more purpose logic in `products.js`**
   - keep only display mapping, fallback behavior, and UI state handling in the frontend

3. **Preserve the current layer boundary**
   - selected-product guide stays selection-dependent
   - global comparison layer stays selection-invariant

4. **Move return interpretation upstream**
   - any aggregation or normalization needed for return-strength should happen in core output generation, not in dashboard-only shaping logic

5. **Define conservative fallback behavior**
   - if structured core outputs are absent, the dashboard may show reduced prototype guidance
   - but it should avoid silently inventing more operational logic in the frontend
