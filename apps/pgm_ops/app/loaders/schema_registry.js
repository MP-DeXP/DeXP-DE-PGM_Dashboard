import { MART_FILES, QA_FILES, RAW_INPUT_FILES, STAGING_FILES, VIEW_MODEL_FILES } from '../config/constants.js';

function defineSchema(filename, grain, required, options = {}) {
    return {
        filename,
        grain,
        required,
        nullable: options.nullable ?? [],
        primaryKey: options.primaryKey ?? [],
        allowedExtras: options.allowedExtras ?? true
    };
}

export const SCHEMA_REGISTRY = {
    raw: {
        orders: defineSchema(RAW_INPUT_FILES.orders, ['order_id'], ['order_id', 'order_date', 'member_id', 'order_total'], {
            primaryKey: ['order_id']
        }),
        order_items: defineSchema(RAW_INPUT_FILES.order_items, ['order_id', 'product_id'], ['order_id', 'product_id', 'product_name', 'quantity', 'revenue', 'customer_id']),
        products: defineSchema(RAW_INPUT_FILES.products, ['product_id'], ['product_id', 'product_name', 'brand_name', 'product_status', 'list_image', 'detail_image', 'detail_url'], {
            primaryKey: ['product_id']
        }),
        product_daily: defineSchema(RAW_INPUT_FILES.product_daily, ['date', 'product_id'], ['date', 'product_id', 'order_count', 'quantity', 'revenue'], {
            primaryKey: ['date', 'product_id']
        }),
        pgm_scored: defineSchema(RAW_INPUT_FILES.pgm_scored, ['snapshot_date', 'product_id'], ['snapshot_date', 'product_id', 'profile_role_primary', 'profile_role_secondary', 'profile_confidence', 'role_state_primary', 'role_state_confidence', 'pgm_observed_flag'], {
            primaryKey: ['snapshot_date', 'product_id']
        }),
        product_window_metrics: defineSchema(RAW_INPUT_FILES.product_window_metrics, ['product_id'], ['product_id', 'revenue_today', 'revenue_prev_day', 'revenue_7d', 'revenue_30d', 'revenue_90d'], {
            primaryKey: ['product_id']
        }),
        brand_window_metrics: defineSchema(RAW_INPUT_FILES.brand_window_metrics, ['as_of_date'], ['as_of_date', 'revenue_today', 'revenue_prev_day', 'revenue_7d', 'revenue_7d_prev', 'revenue_30d', 'revenue_30d_prev', 'revenue_90d', 'revenue_90d_prev'], {
            primaryKey: ['as_of_date']
        }),
        members: defineSchema(RAW_INPUT_FILES.members, ['member_id'], ['member_id', 'mx_member_id', 'member_type', 'is_sms_receive', 'is_email_receive', 'last_login_at', 'mx_channel_id', 'mx_platform'], {
            primaryKey: ['member_id']
        }),
        order_with_utm: defineSchema(RAW_INPUT_FILES.order_with_utm, ['order_id'], ['order_id', 'order_date', 'purchase_amount', 'utm_source', 'utm_medium', 'utm_campaign', 'session_count', 'valid_session_count'], {
            primaryKey: ['order_id']
        }),
        pgm_transition_edge: defineSchema(RAW_INPUT_FILES.pgm_transition_edge, ['date', 'source_product_id', 'target_product_id'], ['date', 'source_product_id', 'target_product_id', 'transition_customer_cnt', 'source_cohort_customer_cnt', 'transition_rate', 'avg_days_to_transition'], {
            primaryKey: ['date', 'source_product_id', 'target_product_id']
        }),
        pgm_loop_detail: defineSchema(RAW_INPUT_FILES.pgm_loop_detail, ['date', 'customer_id', 'source_product_id', 'return_product_id', 'return_order_id'], ['date', 'customer_id', 'source_product_id', 'return_product_id', 'return_days', 'intermediate_step_cnt', 'intermediate_distinct_product_cnt', 'qualified_return_flag', 'simple_repeat_comparison_flag'], {
            nullable: ['return_order_id'],
            primaryKey: ['date', 'customer_id', 'source_product_id', 'return_product_id', 'return_order_id']
        })
    },
    staging: {
        stg_orders: defineSchema(STAGING_FILES.stg_orders, ['order_id'], ['order_id', 'date', 'member_id', 'order_total'], {
            primaryKey: ['order_id']
        }),
        stg_order_items: defineSchema(STAGING_FILES.stg_order_items, ['order_id', 'product_id'], ['order_id', 'product_id', 'product_name', 'quantity', 'revenue', 'customer_id']),
        stg_products: defineSchema(STAGING_FILES.stg_products, ['product_id'], ['product_id', 'product_name', 'brand_name', 'product_status', 'list_image', 'detail_image', 'detail_url'], {
            primaryKey: ['product_id']
        }),
        stg_product_daily: defineSchema(STAGING_FILES.stg_product_daily, ['date', 'product_id'], ['date', 'product_id', 'order_count', 'quantity', 'revenue'], {
            primaryKey: ['date', 'product_id']
        }),
        stg_pgm_scored: defineSchema(STAGING_FILES.stg_pgm_scored, ['date', 'product_id'], ['date', 'product_id', 'profile_role_primary', 'profile_role_secondary', 'profile_confidence', 'role_state_primary', 'role_state_confidence', 'pgm_observed_flag'], {
            primaryKey: ['date', 'product_id']
        }),
        stg_product_window_metrics: defineSchema(STAGING_FILES.stg_product_window_metrics, ['product_id'], ['product_id', 'revenue_today', 'revenue_prev_day', 'revenue_7d', 'revenue_30d', 'revenue_90d'], {
            primaryKey: ['product_id']
        }),
        stg_brand_window_metrics: defineSchema(STAGING_FILES.stg_brand_window_metrics, ['as_of_date'], ['as_of_date', 'revenue_today', 'revenue_prev_day', 'revenue_7d', 'revenue_7d_prev', 'revenue_30d', 'revenue_30d_prev', 'revenue_90d', 'revenue_90d_prev'], {
            primaryKey: ['as_of_date']
        }),
        stg_members: defineSchema(STAGING_FILES.stg_members, ['member_id'], ['member_id', 'mx_member_id', 'member_type', 'is_sms_receive', 'is_email_receive', 'last_login_at', 'mx_channel_id', 'mx_platform'], {
            primaryKey: ['member_id']
        }),
        stg_order_with_utm: defineSchema(STAGING_FILES.stg_order_with_utm, ['order_id'], ['order_id', 'date', 'purchase_amount', 'utm_source', 'utm_medium', 'utm_campaign', 'session_count', 'valid_session_count'], {
            primaryKey: ['order_id']
        }),
        stg_pgm_transition_edge: defineSchema(STAGING_FILES.stg_pgm_transition_edge, ['date', 'source_product_id', 'target_product_id'], ['date', 'source_product_id', 'target_product_id', 'transition_customer_cnt', 'source_cohort_customer_cnt', 'transition_rate', 'avg_days_to_transition'], {
            primaryKey: ['date', 'source_product_id', 'target_product_id']
        }),
        stg_pgm_loop_detail: defineSchema(STAGING_FILES.stg_pgm_loop_detail, ['date', 'customer_id', 'source_product_id', 'return_product_id', 'return_order_id'], ['date', 'customer_id', 'source_product_id', 'return_product_id', 'return_days', 'intermediate_step_cnt', 'intermediate_distinct_product_cnt', 'qualified_return_flag', 'simple_repeat_comparison_flag'], {
            nullable: ['return_order_id'],
            primaryKey: ['date', 'customer_id', 'source_product_id', 'return_product_id', 'return_order_id']
        })
    },
    mart: {
        product_daily_metrics: defineSchema(MART_FILES.product_daily_metrics, ['date', 'product_id'], ['date', 'product_id', 'product_name', 'order_count', 'quantity', 'revenue', 'image_url', 'detail_url'], {
            primaryKey: ['date', 'product_id']
        }),
        product_role_profile: defineSchema(MART_FILES.product_role_profile, ['product_id'], ['product_id', 'profile_role_primary', 'profile_role_secondary', 'profile_confidence'], {
            primaryKey: ['product_id'],
            nullable: ['profile_role_secondary', 'profile_confidence']
        }),
        product_role_state_daily: defineSchema(MART_FILES.product_role_state_daily, ['date', 'product_id'], ['date', 'product_id', 'role_state_primary', 'role_state_confidence', 'pgm_observed_flag'], {
            primaryKey: ['date', 'product_id'],
            nullable: ['role_state_primary', 'role_state_confidence']
        }),
        revenue_structure_daily: defineSchema(MART_FILES.revenue_structure_daily, ['date', 'product_id'], ['date', 'product_id', 'revenue', 'revenue_share_in_brand_day', 'revenue_rank_in_brand_day'], {
            primaryKey: ['date', 'product_id']
        }),
        brand_operating_status_daily: defineSchema(MART_FILES.brand_operating_status_daily, ['date'], ['date', 'brand_revenue', 'brand_order_count', 'active_product_count', 'pgm_observed_product_count', 'top_product_revenue_share', 'dominant_role_state_in_revenue', 'status_summary_label'], {
            primaryKey: ['date']
        }),
        product_transition_summary: defineSchema(MART_FILES.product_transition_summary, ['date', 'product_id', 'transition_rank'], ['date', 'product_id', 'product_name', 'target_product_id', 'target_product_name', 'transition_rank', 'transition_customer_cnt', 'source_cohort_customer_cnt', 'transition_rate', 'avg_days_to_transition'], {
            primaryKey: ['date', 'product_id', 'transition_rank']
        }),
        product_return_loop_summary: defineSchema(MART_FILES.product_return_loop_summary, ['date', 'product_id'], ['date', 'product_id', 'product_name', 'return_case_count', 'qualified_return_count', 'qualified_return_rate', 'simple_repeat_rate', 'return_loop_rate', 'avg_return_days'], {
            primaryKey: ['date', 'product_id']
        })
    },
    view_model: {
        overview_daily_cards: defineSchema(VIEW_MODEL_FILES.overview_daily_cards, ['period', 'card_key'], ['period', 'card_key', 'label', 'value', 'delta', 'reason', 'as_of_date'], {
            primaryKey: ['period', 'card_key'],
            nullable: ['delta']
        }),
        overview_weekly_cards: defineSchema(VIEW_MODEL_FILES.overview_weekly_cards, ['period', 'card_key'], ['period', 'card_key', 'label', 'value', 'delta', 'reason', 'as_of_date'], {
            primaryKey: ['period', 'card_key'],
            nullable: ['delta']
        }),
        overview_monthly_cards: defineSchema(VIEW_MODEL_FILES.overview_monthly_cards, ['period', 'card_key'], ['period', 'card_key', 'label', 'value', 'delta', 'reason', 'as_of_date'], {
            primaryKey: ['period', 'card_key'],
            nullable: ['delta']
        }),
        product_table: defineSchema(VIEW_MODEL_FILES.product_table, ['product_id'], ['product_id', 'product_name', 'profile_role_primary', 'role_state_primary', 'pgm_observed_flag', 'revenue', 'revenue_share_in_brand_day', 'revenue_rank_in_brand_day', 'revenue_7d', 'revenue_30d', 'revenue_90d', 'top_transition_target_name', 'top_transition_rate', 'qualified_return_rate', 'return_loop_rate', 'simple_repeat_rate', 'image_url', 'detail_url'], {
            primaryKey: ['product_id']
        }),
        product_detail_header: defineSchema(VIEW_MODEL_FILES.product_detail_header, ['product_id'], ['product_id', 'headline', 'summary', 'priority_hint'], {
            primaryKey: ['product_id']
        }),
        role_structure_chart: defineSchema(VIEW_MODEL_FILES.role_structure_chart, ['role_state_primary'], ['role_state_primary', 'revenue', 'revenue_share', 'product_count'], {
            primaryKey: ['role_state_primary']
        }),
        revenue_structure_chart: defineSchema(VIEW_MODEL_FILES.revenue_structure_chart, ['product_id'], ['product_id', 'product_name', 'revenue', 'revenue_share_in_brand_day', 'role_state_primary'], {
            primaryKey: ['product_id']
        }),
        priority_checks: defineSchema(VIEW_MODEL_FILES.priority_checks, ['priority_rank'], ['priority_rank', 'priority', 'entity_type', 'entity_id', 'label', 'reason', 'suggested_check', 'evidence'], {
            primaryKey: ['priority_rank']
        }),
        transition_summary: defineSchema(VIEW_MODEL_FILES.transition_summary, ['product_id', 'transition_rank'], ['product_id', 'product_name', 'target_product_id', 'target_product_name', 'transition_rank', 'transition_rate', 'avg_days_to_transition', 'transition_customer_cnt', 'as_of_date', 'product_image_url', 'target_product_image_url'], {
            primaryKey: ['product_id', 'transition_rank']
        }),
        return_loop_summary: defineSchema(VIEW_MODEL_FILES.return_loop_summary, ['product_id'], ['product_id', 'product_name', 'return_case_count', 'qualified_return_rate', 'simple_repeat_rate', 'return_loop_rate', 'avg_return_days', 'as_of_date', 'product_image_url'], {
            primaryKey: ['product_id']
        }),
        revenue_inflow_context: defineSchema(VIEW_MODEL_FILES.revenue_inflow_context, ['context_key'], ['context_key', 'label', 'value', 'detail', 'as_of_date'], {
            primaryKey: ['context_key']
        })
    },
    qa: {
        validation_summary: defineSchema(QA_FILES.validation_summary, ['artifact_name', 'check_name'], ['artifact_name', 'check_name', 'status', 'message']),
        coverage_report: defineSchema(QA_FILES.coverage_report, ['metric_name'], ['metric_name', 'metric_value', 'message']),
        raw_extract_manifest: defineSchema(QA_FILES.raw_extract_manifest, ['artifact_name'], ['artifact_name', 'required', 'exists', 'row_count', 'notes']),
        prd_validation_summary: defineSchema(QA_FILES.prd_validation_summary, ['validation_key'], ['validation_key', 'status', 'prd_source', 'implemented_artifact', 'notes'])
    }
};

export function getSchema(layer, key) {
    return SCHEMA_REGISTRY[layer]?.[key] ?? null;
}

export function listSchemasByLayer(layer) {
    return SCHEMA_REGISTRY[layer] ?? {};
}
