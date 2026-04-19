export const WINDOWS = [7, 30, 90];
export const PERIODS = ['daily', 'weekly', 'monthly'];
export const PRIORITY_LEVELS = ['high', 'medium', 'low'];
export const ROLE_KEYS = ['entry', 'expansion', 'return', 'convergence'];
export const ROLE_HISTORY_MODES = ['same_date_only', 'latest_available'];
export const DEFAULT_ROLE_HISTORY_MODE = ROLE_HISTORY_MODES[0];
export const DEFAULT_EXTRACT_LOOKBACK_DAYS = 120;
export const ROLE_LABEL_FALLBACK = '관측 상태 없음';
export const PROFILE_LABEL_FALLBACK = '프로필 정보 없음';
export const ROLE_LABELS = {
    entry: '첫구매 유도',
    expansion: '단골 유도',
    return: '반복 구매',
    convergence: '구매 집중'
};

export const RAW_INPUT_FILES = {
    orders: 'orders.csv',
    order_items: 'order_items.csv',
    products: 'products.csv',
    product_daily: 'product_daily.csv',
    pgm_scored: 'pgm_scored.csv',
    product_window_metrics: 'product_window_metrics.csv',
    brand_window_metrics: 'brand_window_metrics.csv',
    members: 'members.csv',
    order_with_utm: 'order_with_utm.csv',
    pgm_transition_edge: 'pgm_transition_edge.csv',
    pgm_loop_detail: 'pgm_loop_detail.csv'
};

export const STAGING_FILES = {
    stg_orders: 'stg_orders.csv',
    stg_order_items: 'stg_order_items.csv',
    stg_products: 'stg_products.csv',
    stg_product_daily: 'stg_product_daily.csv',
    stg_pgm_scored: 'stg_pgm_scored.csv',
    stg_product_window_metrics: 'stg_product_window_metrics.csv',
    stg_brand_window_metrics: 'stg_brand_window_metrics.csv',
    stg_members: 'stg_members.csv',
    stg_order_with_utm: 'stg_order_with_utm.csv',
    stg_pgm_transition_edge: 'stg_pgm_transition_edge.csv',
    stg_pgm_loop_detail: 'stg_pgm_loop_detail.csv'
};

export const MART_FILES = {
    product_daily_metrics: 'product_daily_metrics.csv',
    product_role_profile: 'product_role_profile.csv',
    product_role_state_daily: 'product_role_state_daily.csv',
    revenue_structure_daily: 'revenue_structure_daily.csv',
    brand_operating_status_daily: 'brand_operating_status_daily.csv',
    role_revenue_daily: 'role_revenue_daily.csv',
    role_product_membership_window: 'role_product_membership_window.csv',
    product_transition_summary: 'product_transition_summary.csv',
    product_return_loop_summary: 'product_return_loop_summary.csv'
};

export const VIEW_MODEL_FILES = {
    overview_daily_cards: 'overview_daily_cards.csv',
    overview_weekly_cards: 'overview_weekly_cards.csv',
    overview_monthly_cards: 'overview_monthly_cards.csv',
    overview_role_contribution: 'overview_role_contribution.csv',
    overview_revenue_story: 'overview_revenue_story.csv',
    overview_role_delta: 'overview_role_delta.csv',
    overview_role_drilldown: 'overview_role_drilldown.csv',
    product_table: 'product_table.csv',
    product_detail_header: 'product_detail_header.csv',
    role_structure_chart: 'role_structure_chart.csv',
    brand_role_structure: 'brand_role_structure.csv',
    brand_role_window_comparison: 'brand_role_window_comparison.csv',
    revenue_structure_chart: 'revenue_structure_chart.csv',
    priority_checks: 'priority_checks.csv',
    transition_summary: 'transition_summary.csv',
    return_loop_summary: 'return_loop_summary.csv',
    revenue_inflow_context: 'revenue_inflow_context.csv'
};

export const QA_FILES = {
    raw_extract_manifest: 'raw_extract_manifest.csv',
    validation_summary: 'validation_summary.csv',
    coverage_report: 'coverage_report.csv',
    validation_report: 'validation_report.md',
    prd_validation_summary: 'prd_validation_summary.csv'
};

export function getRoleLabel(roleKey) {
    if (!roleKey) {
        return ROLE_LABEL_FALLBACK;
    }

    return ROLE_LABELS[roleKey] ?? roleKey;
}

export function getRoleSortOrder(roleKey) {
    const index = ROLE_KEYS.indexOf(roleKey);
    return index === -1 ? ROLE_KEYS.length : index;
}
