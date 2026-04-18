export const WINDOWS = [7, 30, 90];
export const PERIODS = ['daily', 'weekly', 'monthly'];
export const PRIORITY_LEVELS = ['high', 'medium', 'low'];

export const RAW_INPUT_FILES = {
    orders: 'orders.csv',
    order_items: 'order_items.csv',
    products: 'products.csv',
    product_daily: 'product_daily.csv',
    pgm_scored: 'pgm_scored.csv'
};

export const STAGING_FILES = {
    stg_orders: 'stg_orders.csv',
    stg_order_items: 'stg_order_items.csv',
    stg_products: 'stg_products.csv',
    stg_product_daily: 'stg_product_daily.csv',
    stg_pgm_scored: 'stg_pgm_scored.csv'
};

export const MART_FILES = {
    product_daily_metrics: 'product_daily_metrics.csv',
    product_role_profile: 'product_role_profile.csv',
    product_role_state_daily: 'product_role_state_daily.csv',
    revenue_structure_daily: 'revenue_structure_daily.csv',
    brand_operating_status_daily: 'brand_operating_status_daily.csv'
};

export const VIEW_MODEL_FILES = {
    overview_daily_cards: 'overview_daily_cards.csv',
    overview_weekly_cards: 'overview_weekly_cards.csv',
    overview_monthly_cards: 'overview_monthly_cards.csv',
    product_table: 'product_table.csv',
    product_detail_header: 'product_detail_header.csv',
    role_structure_chart: 'role_structure_chart.csv',
    revenue_structure_chart: 'revenue_structure_chart.csv',
    priority_checks: 'priority_checks.csv'
};

export const QA_FILES = {
    raw_extract_manifest: 'raw_extract_manifest.csv',
    validation_summary: 'validation_summary.csv',
    coverage_report: 'coverage_report.csv',
    validation_report: 'validation_report.md'
};

export const ROLE_LABEL_FALLBACK = 'PGM 미관측';
