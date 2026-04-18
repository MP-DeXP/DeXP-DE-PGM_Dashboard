import { MART_FILES, RAW_INPUT_FILES, ROLE_LABEL_FALLBACK, STAGING_FILES, VIEW_MODEL_FILES } from '../config/constants.js';
import { buildProductDailyMetrics } from '../transforms/revenue/revenue_daily.js';
import { buildRevenueStructureDaily } from '../transforms/revenue/revenue_structure.js';
import { buildBrandOperatingStatusDaily } from '../transforms/brand/operating_status.js';
import { buildProductRoleProfile } from '../transforms/role/role_profile.js';
import { buildProductRoleStateDaily } from '../transforms/role/role_state.js';
import { enrichBrandRevenueWindows, enrichProductRevenueWindows, buildBrandWindowSnapshot } from '../transforms/revenue/revenue_windows.js';
import { buildDailyOverviewCards } from '../view_models/overview/daily_cards.js';
import { buildWeeklyOverviewCards } from '../view_models/overview/weekly_cards.js';
import { buildMonthlyOverviewCards } from '../view_models/overview/monthly_cards.js';
import { buildProductTable } from '../view_models/products/product_table.js';
import { buildProductDetailHeader } from '../view_models/products/product_detail.js';
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
        stg_pgm_scored: stageRawDataset('pgm_scored', rawArtifacts.pgm_scored ?? [])
    };
}

export function buildMartArtifacts(stagingArtifacts) {
    const productDailyMetricsBase = buildProductDailyMetrics(stagingArtifacts);
    const productDailyMetrics = enrichProductRevenueWindows(productDailyMetricsBase);
    const productRoleProfile = buildProductRoleProfile(stagingArtifacts.stg_pgm_scored);
    const productRoleStateDaily = buildProductRoleStateDaily(stagingArtifacts.stg_pgm_scored, productDailyMetricsBase);
    const revenueStructureDaily = buildRevenueStructureDaily(productDailyMetricsBase);
    const brandOperatingStatusDailyBase = buildBrandOperatingStatusDaily(productDailyMetricsBase, productRoleStateDaily, revenueStructureDaily);
    const brandOperatingStatusDaily = enrichBrandRevenueWindows(brandOperatingStatusDailyBase);

    return {
        product_daily_metrics: productDailyMetrics,
        product_role_profile: productRoleProfile,
        product_role_state_daily: productRoleStateDaily,
        revenue_structure_daily: revenueStructureDaily,
        brand_operating_status_daily: brandOperatingStatusDaily
    };
}

export function buildViewModelArtifacts(martArtifacts) {
    const windowSnapshot = buildBrandWindowSnapshot(martArtifacts.brand_operating_status_daily);
    const productTable = buildProductTable(
        martArtifacts.product_daily_metrics,
        martArtifacts.product_role_profile,
        martArtifacts.product_role_state_daily,
        martArtifacts.revenue_structure_daily
    );

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
        )
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
        '# pgm_ops validation report',
        '',
        '## Summary',
        `- pass: ${(groupedByStatus.get('pass') ?? []).length}`,
        `- warn: ${(groupedByStatus.get('warn') ?? []).length}`,
        `- fail: ${(groupedByStatus.get('fail') ?? []).length}`,
        '',
        '## Coverage',
        ...coverageReport.map((row) => `- ${row.metric_name}: ${row.metric_value} (${row.message})`),
        '',
        '## Findings'
    ];

    validationSummary.forEach((row) => {
        lines.push(`- [${row.status}] ${row.artifact_name} / ${row.check_name}: ${row.message}`);
    });

    return `${lines.join('\n')}\n`;
}

export function buildAllArtifacts(rawArtifacts) {
    const stagingArtifacts = buildStagingArtifacts(rawArtifacts);
    const martArtifacts = buildMartArtifacts(stagingArtifacts);
    const viewModelArtifacts = buildViewModelArtifacts(martArtifacts);
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
        'BHI context is intentionally stubbed and not surfaced as a primary metric.',
        `Products without same-date role snapshots stay blank in mart and are only labeled as ${ROLE_LABEL_FALLBACK} in view_model/UI.`,
        'Member and UTM joins remain outside the v0 operating path.'
    ];
}
