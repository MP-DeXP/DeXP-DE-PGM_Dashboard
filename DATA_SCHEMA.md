# APF Dashboard Data Schema

This file defines expected CSV structures used by `app.js`.

## 1) `brand_score`
Used in Overview page.

Required fields:
- One of:
  - `Brand_Health_Index`
  - `BHI`
  - `brand_health_index`
  - `Brand_Health_Score`
- `AA_Concentration_Index`
- `Chain_Balance_Index`
- `Confidence_Index`

Expected types:
- Numeric for index fields

## 2) `anchor_scored`
Used in Products page and name mapping for other pages.

Required fields:
- Product ID (one of):
  - `product_id`
  - `Product_ID`
  - `\ufeffproduct_id` (BOM-prefixed header fallback)
- Product name (one of):
  - `product_name_latest`
  - `Product_Name`
  - `product_name`
- `revenue_90d`
- `first_customer_cnt`
- `AA_Score`
- `AA_Primary_Type`
- `PCA_Score`
- `PCA_Primary_Type`

Expected types:
- Numeric: `revenue_90d`, `first_customer_cnt`, `AA_Score`, `PCA_Score`
- Text/categorical: ID/name/type fields

## 3) `anchor_transition`
Used in Transitions page.

Required fields:
- `aa_product_id`
- `pca_product_id`
- `transition_customer_cnt`
- `avg_days_to_pca`
- `transition_rate`

Expected types:
- Text: `aa_product_id`, `pca_product_id`
- Numeric: `transition_customer_cnt`, `avg_days_to_pca`, `transition_rate`

Notes:
- `transition_rate` is displayed as percent (`value * 100`).

## 4) `cart_anchor`
Used for cart summary chart.

Required fields:
- `product_id`
- `median_cart_size`

Expected types:
- Text: `product_id`
- Numeric: `median_cart_size`

## 5) `cart_anchor_detail`
Used in cart detail table and related-products modal.

Required fields:
- `i` (product A id)
- `j` (product B id)
- `co_order_cnt`

Expected types:
- Text: `i`, `j`
- Numeric: `co_order_cnt`

Notes:
- During load, rows are de-duplicated by keeping only entries where `String(i) < String(j)`.

## Filename Matching Rule
The upload logic maps files by filename substring matching against keys:
- `brand_score`
- `anchor_scored`
- `anchor_transition`
- `cart_anchor`
- `cart_anchor_detail`

Use filenames that clearly include these keys to avoid mismatch.

