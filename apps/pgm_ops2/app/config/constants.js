export const WINDOWS = [1, 7, 30, 90];
export const DEFAULT_WINDOW_KEY = '7d';
export const PRIORITY_LEVELS = ['즉시 확인', '주의 관찰', '정상 유지'];
export const BRAND_SCORE_STATUSES = ['unavailable', 'limited', 'provisional', 'near-core'];
export const DEFAULT_LOOKBACK_DAYS = 120;
export const DEFAULT_AS_OF_DATE = '';
export const ENABLE_NEAR_CORE_STATUS = false;

export const RAW_DATASET_MIN_HISTORY_DAYS = {
    products: 0,
    product_revenue_daily: 180,
    brand_purchase_daily: 180,
    orders_header: 90,
    order_lines: 90,
    order_utm: 90,
    brand_score_events: 90,
    pgm_scored: 30,
    pgm_demand_signals: 30,
    pgm_entry_to_expansion_transition: 30,
    pgm_transition_edges: 30,
    pgm_return_loops: 30,
    pgm_basket_pairs: 30
};

export const RAW_FILE_NAMES = {
    orders_header: 'orders_header.csv',
    order_lines: 'order_lines.csv',
    products: 'products.csv',
    order_utm: 'order_utm.csv',
    product_revenue_daily: 'product_revenue_daily.csv',
    pgm_scored: 'pgm_scored.csv',
    pgm_demand_signals: 'pgm_demand_signals.csv',
    pgm_entry_to_expansion_transition: 'pgm_entry_to_expansion_transition.csv',
    pgm_transition_edges: 'pgm_transition_edges.csv',
    pgm_return_loops: 'pgm_return_loops.csv',
    pgm_basket_pairs: 'pgm_basket_pairs.csv',
    brand_purchase_daily: 'brand_purchase_daily.csv',
    brand_score_events: 'brand_score_events.csv'
};

export const RAW_METADATA_FILE_NAME = '__raw_refresh_status.csv';

export const STAGING_FILE_NAMES = {
    stg_product_revenue_daily: 'stg_product_revenue_daily.csv',
    stg_role_source_daily: 'stg_role_source_daily.csv',
    stg_priority_inputs_daily: 'stg_priority_inputs_daily.csv',
    stg_data_freshness: 'stg_data_freshness.csv',
    stg_reconstructed_order_product_events: 'stg_reconstructed_order_product_events.csv',
    stg_reconstructed_basket_summary: 'stg_reconstructed_basket_summary.csv',
    stg_brand_score_reconstruction_inputs: 'stg_brand_score_reconstruction_inputs.csv'
};

export const MART_FILE_NAMES = {
    mart_product_revenue_windows: 'mart_product_revenue_windows.csv',
    mart_product_role_taxonomy_daily: 'mart_product_role_taxonomy_by_window.csv',
    mart_product_priority_basis: 'mart_product_priority_basis.csv',
    mart_priority_queue_snapshot: 'mart_priority_queue_snapshot.csv',
    mart_segment_structure_snapshot: 'mart_segment_structure_snapshot.csv',
    mart_data_health_snapshot: 'mart_data_health_snapshot.csv',
    mart_brand_score_brand_level: 'mart_brand_score_brand_level.csv',
    mart_brand_score_product_contributors: 'mart_brand_score_product_contributors.csv',
    mart_reconstruction_registry: 'mart_reconstruction_registry.csv',
    mart_brand_score_reconstruction: 'mart_brand_score_reconstruction.csv',
    mart_brand_score_validation_status: 'mart_brand_score_validation_status.csv'
};

export const VIEW_MODEL_FILE_NAMES = {
    vm_priority_queue: 'vm_priority_queue.csv',
    vm_queue_summary: 'vm_queue_summary.csv',
    vm_segment_map: 'vm_segment_map.csv',
    vm_structure_map_cells: 'vm_structure_map_cells.csv',
    vm_product_detail: 'vm_product_detail.csv',
    vm_definition_rules: 'vm_definition_rules.csv',
    vm_data_health: 'vm_data_health.csv',
    vm_data_health_overview: 'vm_data_health_overview.csv',
    vm_data_health_detail: 'vm_data_health_detail.csv',
    vm_brand_score_panel: 'vm_brand_score_panel.csv',
    vm_brand_score_product_contributors: 'vm_brand_score_product_contributors.csv',
    vm_reconstruction_registry: 'vm_reconstruction_registry.csv',
    vm_iteration_log: 'vm_iteration_log.csv'
};

export const QA_FILE_NAMES = {
    raw_manifest: 'raw_manifest.csv',
    validation_summary: 'validation_summary.csv',
    validation_report: 'validation_report.md',
    tone_audit: 'tone_audit.csv',
    implementation_scope: 'implementation_scope.csv'
};

export const ROLE_TAXONOMY = [
    '첫구매기여',
    '재구매확장기여',
    '반복구매기여',
    '동시구매기여'
];

export const DATA_STATE_LABELS = {
    available: '정상',
    partial: '부분 관측',
    missing: '데이터 부족',
    unavailable: '계산 불가',
    limited: '제한적 반영',
    provisional: '잠정 계산',
    near_core: '고유사도'
};

export const QUEUE_REASON_TYPES = ['Revenue', 'Role', 'Brand Score'];
