import { MART_FILES, RAW_INPUT_FILES, ROLE_LABEL_FALLBACK, STAGING_FILES, VIEW_MODEL_FILES } from '../config/constants.js';
import { buildProductDailyMetrics } from '../transforms/revenue/revenue_daily.js';
import { buildRevenueStructureDaily } from '../transforms/revenue/revenue_structure.js';
import { buildBrandOperatingStatusDaily } from '../transforms/brand/operating_status.js';
import { buildProductRoleProfile } from '../transforms/role/role_profile.js';
import { buildProductRoleStateDaily } from '../transforms/role/role_state.js';
import { enrichBrandRevenueWindows, enrichProductRevenueWindows, buildBrandWindowSnapshot } from '../transforms/revenue/revenue_windows.js';
import { buildProductTransitionSummary } from '../transforms/transition/transition_summary.js';
import { buildProductReturnLoopSummary } from '../transforms/transition/return_loop_summary.js';
import { buildDailyOverviewCards } from '../view_models/overview/daily_cards.js';
import { buildWeeklyOverviewCards } from '../view_models/overview/weekly_cards.js';
import { buildMonthlyOverviewCards } from '../view_models/overview/monthly_cards.js';
import { buildProductTable } from '../view_models/products/product_table.js';
import { buildProductDetailHeader } from '../view_models/products/product_detail.js';
import { buildTransitionSummaryView } from '../view_models/products/transition_summary.js';
import { buildReturnLoopSummaryView } from '../view_models/products/return_loop_summary.js';
import { buildRevenueInflowContext } from '../view_models/products/revenue_inflow_context.js';
import { buildRoleStructureChart } from '../view_models/structures/role_structure.js';
import { buildRevenueStructureChart } from '../view_models/structures/revenue_structure.js';
import { buildPriorityChecks } from '../view_models/alerts/priority_checks.js';
import { normalizeRows } from '../transforms/base/normalize_values.js';
import { standardizeColumns } from '../transforms/base/standardize_columns.js';
import { listSchemasByLayer } from '../loaders/schema_registry.js';
import { runDuplicationCheck } from '../validators/duplication_check.js';
import { runFreshnessCheck } from '../validators/freshness_check.js';
import { runGrainCheck } from '../validators/grain_check.js';
import { runNullCheck } from '../validators/null_check.js';
import { runSchemaCheck } from '../validators/schema_check.js';
import { getLatestDate } from '../transforms/base/date_windows.js';

function stageRawDataset(datasetKey, rows) {
    const standardized = standardizeColumns(rows, datasetKey);

    switch (datasetKey) {
        case 'orders':
            return normalizeRows(standardized, {
                idFields: ['order_id', 'member_id'],
                dateFields: ['date'],
                numberFields: ['order_total']
            });
        case 'order_items':
            return normalizeRows(standardized, {
                idFields: ['order_id', 'product_id', 'customer_id'],
                numberFields: ['quantity', 'revenue']
            });
        case 'products':
            return normalizeRows(standardized, {
                idFields: ['product_id']
            });
        case 'product_daily':
            return normalizeRows(standardized, {
                idFields: ['product_id'],
                dateFields: ['date'],
                numberFields: ['order_count', 'quantity', 'revenue']
            });
        case 'pgm_scored':
            return normalizeRows(standardized, {
                idFields: ['product_id'],
                dateFields: ['date'],
                numberFields: ['profile_confidence', 'role_state_confidence'],
                booleanFields: ['pgm_observed_flag']
            });
        case 'product_window_metrics':
            return normalizeRows(standardized, {
                idFields: ['product_id'],
                numberFields: ['revenue_today', 'revenue_prev_day', 'revenue_7d', 'revenue_30d', 'revenue_90d']
            });
        case 'brand_window_metrics':
            return normalizeRows(standardized, {
                dateFields: ['as_of_date'],
                numberFields: ['revenue_today', 'revenue_prev_day', 'revenue_7d', 'revenue_7d_prev', 'revenue_30d', 'revenue_30d_prev', 'revenue_90d', 'revenue_90d_prev']
            });
        case 'members':
            return normalizeRows(standardized, {
                idFields: ['member_id', 'mx_member_id'],
                booleanFields: ['is_sms_receive', 'is_email_receive']
            });
        case 'order_with_utm':
            return normalizeRows(standardized, {
                idFields: ['order_id', 'utm_source', 'utm_medium', 'utm_campaign'],
                dateFields: ['date'],
                numberFields: ['purchase_amount', 'session_count', 'valid_session_count']
            });
        case 'pgm_transition_edge':
            return normalizeRows(standardized, {
                idFields: ['source_product_id', 'target_product_id'],
                dateFields: ['date'],
                numberFields: ['transition_customer_cnt', 'source_cohort_customer_cnt', 'transition_rate', 'avg_days_to_transition']
            });
        case 'pgm_loop_detail':
            return normalizeRows(standardized, {
                idFields: ['customer_id', 'source_product_id', 'return_product_id', 'source_order_id', 'return_order_id', 'intermediate_path'],
                dateFields: ['date'],
                numberFields: ['return_days', 'intermediate_step_cnt', 'intermediate_distinct_product_cnt'],
                booleanFields: ['qualified_return_flag', 'simple_repeat_comparison_flag']
            });
        default:
            return standardized;
    }
}

export function buildStagingArtifacts(rawArtifacts) {
    return {
        stg_orders: stageRawDataset('orders', rawArtifacts.orders ?? []),
        stg_order_items: stageRawDataset('order_items', rawArtifacts.order_items ?? []),
        stg_products: stageRawDataset('products', rawArtifacts.products ?? []),
        stg_product_daily: stageRawDataset('product_daily', rawArtifacts.product_daily ?? []),
        stg_pgm_scored: stageRawDataset('pgm_scored', rawArtifacts.pgm_scored ?? []),
        stg_product_window_metrics: stageRawDataset('product_window_metrics', rawArtifacts.product_window_metrics ?? []),
        stg_brand_window_metrics: stageRawDataset('brand_window_metrics', rawArtifacts.brand_window_metrics ?? []),
        stg_members: stageRawDataset('members', rawArtifacts.members ?? []),
        stg_order_with_utm: stageRawDataset('order_with_utm', rawArtifacts.order_with_utm ?? []),
        stg_pgm_transition_edge: stageRawDataset('pgm_transition_edge', rawArtifacts.pgm_transition_edge ?? []),
        stg_pgm_loop_detail: stageRawDataset('pgm_loop_detail', rawArtifacts.pgm_loop_detail ?? [])
    };
}

export function buildMartArtifacts(stagingArtifacts) {
    const productDailyMetricsBase = buildProductDailyMetrics(stagingArtifacts);
    const productDailyMetrics = enrichProductRevenueWindows(productDailyMetricsBase, stagingArtifacts.stg_product_window_metrics ?? []);
    const productRoleProfile = buildProductRoleProfile(stagingArtifacts.stg_pgm_scored);
    const productRoleStateDaily = buildProductRoleStateDaily(stagingArtifacts.stg_pgm_scored, productDailyMetricsBase);
    const revenueStructureDaily = buildRevenueStructureDaily(productDailyMetricsBase);
    const brandOperatingStatusDailyBase = buildBrandOperatingStatusDaily(productDailyMetricsBase, productRoleStateDaily, revenueStructureDaily);
    const brandOperatingStatusDaily = enrichBrandRevenueWindows(brandOperatingStatusDailyBase, stagingArtifacts.stg_brand_window_metrics ?? []);
    const productTransitionSummary = buildProductTransitionSummary(
        stagingArtifacts.stg_pgm_transition_edge,
        stagingArtifacts.stg_products,
        stagingArtifacts.stg_order_items
    );
    const productReturnLoopSummary = buildProductReturnLoopSummary(
        stagingArtifacts.stg_pgm_loop_detail,
        stagingArtifacts.stg_products,
        stagingArtifacts.stg_order_items
    );

    return {
        product_daily_metrics: productDailyMetrics,
        product_role_profile: productRoleProfile,
        product_role_state_daily: productRoleStateDaily,
        revenue_structure_daily: revenueStructureDaily,
        brand_operating_status_daily: brandOperatingStatusDaily,
        product_transition_summary: productTransitionSummary,
        product_return_loop_summary: productReturnLoopSummary
    };
}

export function buildViewModelArtifacts(martArtifacts, stagingArtifacts = {}) {
    const windowSnapshot = buildBrandWindowSnapshot(martArtifacts.brand_operating_status_daily, stagingArtifacts.stg_brand_window_metrics ?? []);
    const productTable = buildProductTable(
        martArtifacts.product_daily_metrics,
        martArtifacts.product_role_profile,
        martArtifacts.product_role_state_daily,
        martArtifacts.revenue_structure_daily,
        martArtifacts.product_transition_summary,
        martArtifacts.product_return_loop_summary
    );
    const transitionSummary = buildTransitionSummaryView(martArtifacts.product_transition_summary, stagingArtifacts.stg_products ?? []);
    const returnLoopSummary = buildReturnLoopSummaryView(martArtifacts.product_return_loop_summary, stagingArtifacts.stg_products ?? []);

    return {
        overview_daily_cards: buildDailyOverviewCards(martArtifacts.brand_operating_status_daily),
        overview_weekly_cards: buildWeeklyOverviewCards(martArtifacts.brand_operating_status_daily, windowSnapshot),
        overview_monthly_cards: buildMonthlyOverviewCards(martArtifacts.brand_operating_status_daily, windowSnapshot),
        product_table: productTable,
        product_detail_header: buildProductDetailHeader(productTable),
        role_structure_chart: buildRoleStructureChart(martArtifacts.product_daily_metrics, martArtifacts.product_role_state_daily),
        revenue_structure_chart: buildRevenueStructureChart(productTable),
        priority_checks: buildPriorityChecks(
            martArtifacts.brand_operating_status_daily,
            martArtifacts.revenue_structure_daily,
            martArtifacts.product_role_state_daily,
            productTable
        ),
        transition_summary: transitionSummary,
        return_loop_summary: returnLoopSummary,
        revenue_inflow_context: buildRevenueInflowContext(stagingArtifacts.stg_order_with_utm ?? [])
    };
}

export function buildRawExtractManifest(rawArtifacts) {
    return Object.entries(RAW_INPUT_FILES).map(([artifactName]) => ({
        artifact_name: artifactName,
        required: 'true',
        exists: rawArtifacts[artifactName]?.length ? 'true' : 'false',
        row_count: rawArtifacts[artifactName]?.length ?? 0,
        notes: rawArtifacts[artifactName]?.length ? 'loaded' : 'missing_or_empty'
    }));
}

export function buildCoverageReport(martArtifacts) {
    const latestDate = getLatestDate(martArtifacts.product_daily_metrics);
    const latestProductRows = martArtifacts.product_daily_metrics.filter((row) => row.date === latestDate);
    const latestRoleRows = martArtifacts.product_role_state_daily.filter((row) => row.date === latestDate);
    const fallbackUsage = latestProductRows.filter((row) => row.product_name_source === 'order_item_fallback').length;
    const blankRoleStates = latestRoleRows.filter((row) => !row.role_state_primary).length;
    const observedCount = latestRoleRows.filter((row) => row.pgm_observed_flag === 'true').length;

    return [
        {
            metric_name: 'product_master_fallback_usage_rate',
            metric_value: latestProductRows.length ? fallbackUsage / latestProductRows.length : 0,
            message: 'product_name fallback usage on the latest snapshot'
        },
        {
            metric_name: 'pgm_observed_coverage',
            metric_value: latestRoleRows.length ? observedCount / latestRoleRows.length : 0,
            message: 'same-date PGM observation coverage on the latest snapshot'
        },
        {
            metric_name: 'role_state_blank_rate',
            metric_value: latestRoleRows.length ? blankRoleStates / latestRoleRows.length : 0,
            message: 'blank role state rate on the latest snapshot; no latest-role fallback applied'
        }
    ];
}

export function buildValidationSummary(artifactsByLayer) {
    const results = [];
    const schemasByLayer = {
        staging: listSchemasByLayer('staging'),
        mart: listSchemasByLayer('mart'),
        view_model: listSchemasByLayer('view_model')
    };

    Object.entries(artifactsByLayer).forEach(([layer, artifacts]) => {
        Object.entries(artifacts).forEach(([artifactName, rows]) => {
            const schema = schemasByLayer[layer]?.[artifactName];

            if (!schema) {
                return;
            }

            results.push(runSchemaCheck(artifactName, rows, schema));
            results.push(runGrainCheck(artifactName, rows, schema));
            results.push(runNullCheck(artifactName, rows, schema));
            results.push(runFreshnessCheck(artifactName, rows));
            results.push(runDuplicationCheck(artifactName, rows, schema.primaryKey.length ? schema.primaryKey : schema.grain));
        });
    });

    return results;
}

export function buildValidationReport(validationSummary, coverageReport) {
    const groupedByStatus = validationSummary.reduce((map, row) => {
        if (!map.has(row.status)) {
            map.set(row.status, []);
        }
        map.get(row.status).push(row);
        return map;
    }, new Map());

    const lines = [
        '# pgm_ops 검증 리포트',
        '',
        '## 요약',
        `- pass: ${(groupedByStatus.get('pass') ?? []).length}`,
        `- warn: ${(groupedByStatus.get('warn') ?? []).length}`,
        `- fail: ${(groupedByStatus.get('fail') ?? []).length}`,
        '',
        '## 커버리지',
        ...coverageReport.map((row) => `- ${row.metric_name}: ${row.metric_value} (${row.message})`),
        '',
        '## 세부 결과'
    ];

    validationSummary.forEach((row) => {
        lines.push(`- [${row.status}] ${row.artifact_name} / ${row.check_name}: ${row.message}`);
    });

    return `${lines.join('\n')}\n`;
}

export function buildAllArtifacts(rawArtifacts) {
    const stagingArtifacts = buildStagingArtifacts(rawArtifacts);
    const martArtifacts = buildMartArtifacts(stagingArtifacts);
    const viewModelArtifacts = buildViewModelArtifacts(martArtifacts, stagingArtifacts);
    const coverageReport = buildCoverageReport(martArtifacts);
    const validationSummary = buildValidationSummary({
        staging: stagingArtifacts,
        mart: martArtifacts,
        view_model: viewModelArtifacts
    });

    return {
        stagingArtifacts,
        martArtifacts,
        viewModelArtifacts,
        qaArtifacts: {
            raw_extract_manifest: buildRawExtractManifest(rawArtifacts),
            validation_summary: validationSummary,
            coverage_report: coverageReport,
            validation_report: buildValidationReport(validationSummary, coverageReport)
        }
    };
}

export const ARTIFACT_FILE_MAP = {
    raw: RAW_INPUT_FILES,
    staging: STAGING_FILES,
    mart: MART_FILES,
    view_model: VIEW_MODEL_FILES
};

export function summarizeMockedAreas() {
    return [
        'BHI 컨텍스트는 여전히 stub이며 핵심 운영 지표로 노출하지 않습니다.',
        `same-date role snapshot이 없는 상품은 mart에서 blank를 유지하고 view_model/UI에서만 ${ROLE_LABEL_FALLBACK} 라벨을 붙입니다.`,
        'member/UTM은 보조 근거까지만 노출하며 메인 판단 프레임으로 끌어올리지 않습니다.'
    ];
}
