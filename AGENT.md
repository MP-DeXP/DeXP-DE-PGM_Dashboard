# APF Dashboard Agent Context

## 1) Project Purpose
- This project is a browser-only analytics dashboard for internal testing/demo.
- It is **not** intended for production operations.
- Main goal: quickly load CSV data and explore key APF metrics visually.

## 2) Tech/Architecture
- Frontend only (no backend, no build step).
- Files:
  - `index.html` (Overview)
  - `products.html` (Products)
  - `transitions.html` (Transitions)
  - `cart.html` (Cart Analysis)
  - `app.js` (all app logic)
  - `style.css` (shared styling)
- External libs are loaded via CDN:
  - Chart.js
  - PapaParse
  - Phosphor Icons
  - Google Fonts

## 3) Data Loading Model
- Data source: local CSV files selected by user in Upload modal.
- Parsing: PapaParse (`header: true`, `dynamicTyping: true`).
- Storage: IndexedDB (`APF_Dashboard_DB`, store: `csv_files`).
- Required dataset keys:
  - `brand_score`
  - `anchor_scored`
  - `anchor_transition`
  - `cart_anchor`
  - `cart_anchor_detail`

## 4) How to Run
1. Start a static file server in project root.
2. Open `index.html` through `http://localhost` (recommended).
3. Click **Upload Data** and select CSV files.
4. Navigate between pages using the left sidebar.

Example:
```bash
python3 -m http.server 8000
```

## 5) Current Behavior Notes
- If no IndexedDB data exists, UI asks for CSV upload.
- Pages are routed by `body` id and rendered from one `app.js`.
- Cart detail table supports search, sorting, pagination.
- Product rows support copy-to-clipboard and related-products modal.

## 6) Constraints / Non-Goals
- Internal usage only: readability and demo speed are prioritized.
- There is no API integration, auth, or deployment pipeline.
- No test suite or CI is currently configured.

## 7) Safe Change Guidelines
- Keep it static/simple (HTML + JS + CSS) unless explicitly requested.
- Preserve CSV key names and existing upload matching behavior.
- Prefer incremental UI/logic updates over large architectural rewrites.
- If adding docs, keep onboarding-focused and brief.

## 8) Recommended Next Docs (Optional)
- `README.md`: quick-start for first-time users.
- `DATA_SCHEMA.md`: expected columns for each CSV file.
- `CHANGELOG.md`: lightweight history for demo iteration tracking.

