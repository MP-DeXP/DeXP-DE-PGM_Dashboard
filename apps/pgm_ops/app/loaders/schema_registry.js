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
        products: defineSchema(RAW_INPUT_FILES.products, ['product_id'], ['product_id', 'product_name', 'brand_name', 'product_status'], {
            primaryKey: ['product_id']
        }),
        product_daily: defineSchema(RAW_INPUT_FILES.product_daily, ['date', 'product_id'], ['date', 'product_id', 'order_count', 'quantity', 'revenue'], {
            primaryKey: ['date', 'product_id']
        }),
        pgm_scored: defineSchema(RAW_INPUT_FILES.pgm_scored, ['snapshot_date', 'product_id'], ['snapshot_date', 'product_id', 'profile_role_primary', 'profile_role_secondary', 'profile_confidence', 'role_state_primary', 'role_state_confidence', 'pgm_observed_flag'], {
            primaryKey: ['snapshot_date', 'product_id']
        })
    },
    staging: {
        stg_orders: defineSchema(STAGING_FILES.stg_orders, ['order_id'], ['order_id', 'date', 'member_id', 'order_total'], {
            primaryKey: ['order_id']
        }),
        stg_order_items: defineSchema(STAGING_FILES.stg_order_items, ['order_id', 'product_id'], ['order_id', 'product_id', 'product_name', 'quantity', 'revenue', 'customer_id']),
        stg_products: defineSchema(STAGING_FILES.stg_products, ['product_id'], ['product_id', 'product_name', 'brand_name', 'product_status'], {
            primaryKey: ['product_id']
        }),
        stg_product_daily: defineSchema(STAGING_FILES.stg_product_daily, ['date', 'product_id'], ['date', 'product_id', 'order_count', 'quantity', 'revenue'], {
            primaryKey: ['date', 'product_id']
        }),
        stg_pgm_scored: defineSchema(STAGING_FILES.stg_pgm_scored, ['date', 'product_id'], ['date', 'product_id', 'profile_role_primary', 'profile_role_secondary', 'profile_confidence', 'role_state_primary', 'role_state_confidence', 'pgm_observed_flag'], {
            primaryKey: ['date', 'product_id']
        })
    },
    mart: {
        product_daily_metrics: defineSchema(MART_FILES.product_daily_metrics, ['date', 'product_id'], ['date', 'product_id', 'product_name', 'order_count', 'quantity', 'revenue'], {
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
        product_table: defineSchema(VIEW_MODEL_FILES.product_table, ['product_id'], ['product_id', 'product_name', 'profile_role_primary', 'role_state_primary', 'pgm_observed_flag', 'revenue', 'revenue_share_in_brand_day', 'revenue_rank_in_brand_day', 'revenue_7d', 'revenue_30d', 'revenue_90d'], {
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
        })
    },
    qa: {
        validation_summary: defineSchema(QA_FILES.validation_summary, ['artifact_name', 'check_name'], ['artifact_name', 'check_name', 'status', 'message']),
        coverage_report: defineSchema(QA_FILES.coverage_report, ['metric_name'], ['metric_name', 'metric_value', 'message']),
        raw_extract_manifest: defineSchema(QA_FILES.raw_extract_manifest, ['artifact_name'], ['artifact_name', 'required', 'exists', 'row_count', 'notes'])
    }
};

export function getSchema(layer, key) {
    return SCHEMA_REGISTRY[layer]?.[key] ?? null;
}

export function listSchemasByLayer(layer) {
    return SCHEMA_REGISTRY[layer] ?? {};
}
