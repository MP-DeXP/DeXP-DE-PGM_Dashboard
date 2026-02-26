# APF Dashboard

Browser-only APF analytics dashboard for internal testing and demo.

## Scope
- Internal demo/test usage only
- No backend/API server
- No build pipeline

## Pages
- `index.html`: Overview
- `products.html`: Products
- `transitions.html`: Transitions
- `cart.html`: Cart Analysis

## Quick Start
1. Start a static server in project root.
2. Open `http://localhost:8000/index.html`.
3. Click `Upload Data` in the sidebar.
4. Select CSV files.
5. Move between pages from the left navigation.

```bash
python3 -m http.server 8000
```

## Required CSV Keys
Upload files that contain these key names in their filename:
- `brand_score`
- `anchor_scored`
- `anchor_transition`
- `cart_anchor`
- `cart_anchor_detail`

Example filenames:
- `brand_score.csv`
- `anchor_scored_2026-02-26.csv`

## Data Storage
- Parsed in browser via PapaParse
- Stored in IndexedDB:
  - DB: `APF_Dashboard_DB`
  - Store: `csv_files`

## Related Docs
- Agent context: `AGENT.md`
- CSV schema: `DATA_SCHEMA.md`

## Notes
- If IndexedDB has no data, UI shows upload prompt.
- All rendering logic is in `app.js`.
- Shared visual styles are in `style.css`.

