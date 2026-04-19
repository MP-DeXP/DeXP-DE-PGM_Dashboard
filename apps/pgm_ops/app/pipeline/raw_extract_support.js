import {
    DEFAULT_EXTRACT_LOOKBACK_DAYS,
    DEFAULT_ROLE_HISTORY_MODE,
    RAW_INPUT_FILES,
    ROLE_HISTORY_MODES,
    WINDOWS
} from '../config/constants.js';
import { getLatestDate, shiftDate } from '../transforms/base/date_windows.js';

const DATE_FIELD_BY_DATASET = {
    orders: 'order_date',
    product_daily: 'date',
    pgm_scored: 'snapshot_date',
    brand_window_metrics: 'as_of_date',
    order_with_utm: 'order_date',
    pgm_transition_edge: 'date',
    pgm_loop_detail: 'date'
};

function isIsoDate(value) {
    return /^\d{4}-\d{2}-\d{2}$/.test(String(value ?? ''));
}

function toPositiveInteger(value, fallback) {
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function normalizeSampleOption(sample) {
    if (sample === true || sample === false || sample == null) {
        return sample;
    }

    const parsed = Number.parseInt(sample, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : true;
}

function getDateField(datasetKey) {
    return DATE_FIELD_BY_DATASET[datasetKey] ?? null;
}

function getDatasetDates(rows, datasetKey) {
    const dateField = getDateField(datasetKey);

    if (!dateField || !rows.length) {
        return [];
    }

    return [...new Set(
        rows
            .map((row) => row[dateField])
            .filter((value) => isIsoDate(value))
            .sort((left, right) => left.localeCompare(right))
    )];
}

function getExpectedLookbackDays(datasetKey, context) {
    if (!context.asOfDate) {
        return '';
    }

    return getDateField(datasetKey) ? context.lookbackDays : '';
}

function getCoverageStatus(exists, coverageRatio) {
    if (!exists) {
        return 'missing';
    }

    if (coverageRatio === '') {
        return 'not_applicable';
    }

    return Number(coverageRatio) >= 1 ? 'full' : 'partial';
}

function buildCoverageMetadata(rows, datasetKey, context) {
    const dates = getDatasetDates(rows, datasetKey);

    if (!dates.length) {
        return {
            min_date: '',
            max_date: '',
            covered_days: '',
            expected_lookback_days: getExpectedLookbackDays(datasetKey, context),
            coverage_ratio: '',
            coverage_status: getCoverageStatus(rows.length > 0, '')
        };
    }

    const maxDate = dates[dates.length - 1];
    const minDate = dates[0];
    const boundedDates = context.asOfDate
        ? dates.filter((date) => date <= context.asOfDate && date >= context.windowStartDate)
        : dates;
    const coveredDays = boundedDates.length;
    const expectedDays = getExpectedLookbackDays(datasetKey, context);
    const coverageRatio = expectedDays
        ? Math.min(1, coveredDays / Math.max(1, Number(expectedDays)))
        : '';

    return {
        min_date: minDate,
        max_date: maxDate,
        covered_days: coveredDays,
        expected_lookback_days: expectedDays,
        coverage_ratio: coverageRatio,
        coverage_status: getCoverageStatus(rows.length > 0, coverageRatio)
    };
}

function buildProductDailyIndex(productDailyRows, context) {
    const boundedRows = productDailyRows.filter((row) => {
        const date = row.date;
        return isIsoDate(date) && (!context.asOfDate || (date >= context.windowStartDate && date <= context.asOfDate));
    });
    const revenueByProductDate = new Map();
    const brandRevenueByDate = new Map();
    const latestRowsByProduct = new Map();

    boundedRows.forEach((row) => {
        const revenue = Number(row.revenue ?? 0);
        const key = `${row.product_id}|${row.date}`;
        revenueByProductDate.set(key, revenue);
        brandRevenueByDate.set(row.date, (brandRevenueByDate.get(row.date) ?? 0) + revenue);

        if (row.date === context.asOfDate) {
            latestRowsByProduct.set(row.product_id, row);
        }
    });

    return {
        boundedRows,
        revenueByProductDate,
        brandRevenueByDate,
        latestRowsByProduct
    };
}

function sumWindowRevenue(getValue, asOfDate, windowDays) {
    let total = 0;

    for (let offset = 1; offset <= windowDays; offset += 1) {
        total += getValue(shiftDate(asOfDate, -offset));
    }

    return total;
}

function deriveProductWindowMetrics(productDailyRows, context) {
    const index = buildProductDailyIndex(productDailyRows, context);
    const previousDate = context.asOfDate ? shiftDate(context.asOfDate, -1) : null;

    return [...index.latestRowsByProduct.values()]
        .sort((left, right) => String(left.product_id ?? '').localeCompare(String(right.product_id ?? '')))
        .map((row) => {
            const getRevenue = (date) => Number(index.revenueByProductDate.get(`${row.product_id}|${date}`) ?? 0);

            return {
                product_id: row.product_id,
                revenue_today: Number(row.revenue ?? 0),
                revenue_prev_day: previousDate ? getRevenue(previousDate) : 0,
                revenue_7d: sumWindowRevenue(getRevenue, context.asOfDate, 7),
                revenue_30d: sumWindowRevenue(getRevenue, context.asOfDate, 30),
                revenue_90d: sumWindowRevenue(getRevenue, context.asOfDate, 90)
            };
        });
}

function deriveBrandWindowMetrics(productDailyRows, context) {
    const index = buildProductDailyIndex(productDailyRows, context);

    if (!context.asOfDate) {
        return [];
    }

    const getRevenue = (date) => Number(index.brandRevenueByDate.get(date) ?? 0);

    return [{
        as_of_date: context.asOfDate,
        revenue_today: getRevenue(context.asOfDate),
        revenue_prev_day: getRevenue(shiftDate(context.asOfDate, -1)),
        revenue_7d: sumWindowRevenue(getRevenue, context.asOfDate, 7),
        revenue_7d_prev: sumWindowRevenue(getRevenue, shiftDate(context.asOfDate, -7), 7),
        revenue_30d: sumWindowRevenue(getRevenue, context.asOfDate, 30),
        revenue_30d_prev: sumWindowRevenue(getRevenue, shiftDate(context.asOfDate, -30), 30),
        revenue_90d: sumWindowRevenue(getRevenue, context.asOfDate, 90),
        revenue_90d_prev: sumWindowRevenue(getRevenue, shiftDate(context.asOfDate, -90), 90)
    }];
}

function shouldSynthesizeProductWindows(rawArtifacts, context) {
    if (!(rawArtifacts.product_daily ?? []).length || !context.asOfDate) {
        return false;
    }

    if (!(rawArtifacts.product_window_metrics ?? []).length) {
        return true;
    }

    const latestProductDailyDate = getLatestDate(rawArtifacts.product_daily);
    return Boolean(context.asOfDate) && context.asOfDate !== latestProductDailyDate;
}

function shouldSynthesizeBrandWindows(rawArtifacts, context) {
    if (!(rawArtifacts.product_daily ?? []).length || !context.asOfDate) {
        return false;
    }

    if (!(rawArtifacts.brand_window_metrics ?? []).length) {
        return true;
    }

    return !rawArtifacts.brand_window_metrics.some((row) => row.as_of_date === context.asOfDate);
}

export function normalizeExtractOptions(options = {}) {
    const lookbackDays = toPositiveInteger(options.lookbackDays, DEFAULT_EXTRACT_LOOKBACK_DAYS);
    const roleHistoryMode = ROLE_HISTORY_MODES.includes(options.roleHistoryMode)
        ? options.roleHistoryMode
        : DEFAULT_ROLE_HISTORY_MODE;

    return {
        asOfDate: isIsoDate(options.asOfDate) ? options.asOfDate : '',
        lookbackDays,
        mxChannelId: options.mxChannelId ? String(options.mxChannelId) : '',
        mxPlatform: options.mxPlatform ? String(options.mxPlatform) : '',
        sample: normalizeSampleOption(options.sample),
        roleHistoryMode
    };
}

export function buildExtractContext(rawArtifacts, options = {}) {
    const normalized = normalizeExtractOptions(options);
    const latestProductDailyDate = getLatestDate(rawArtifacts.product_daily ?? []);
    const asOfDate = normalized.asOfDate || latestProductDailyDate || '';
    const windowStartDate = asOfDate ? shiftDate(asOfDate, -(normalized.lookbackDays - 1)) : '';
    const warnings = [];

    if (normalized.mxChannelId || normalized.mxPlatform) {
        warnings.push({
            code: 'local_filter_not_applied',
            message: '로컬 raw snapshot만으로는 mx filter를 모든 원천 파일에 일관 적용할 수 없습니다.'
        });
    }

    if (normalized.sample) {
        warnings.push({
            code: 'sample_preview_only',
            message: 'sample 옵션은 현재 요약/검증 메타데이터에만 반영되며 원본 snapshot 재추출은 수행하지 않습니다.'
        });
    }

    if (normalized.lookbackDays < Math.max(...WINDOWS)) {
        warnings.push({
            code: 'lookback_shorter_than_max_window',
            message: `lookback_days=${normalized.lookbackDays} 이므로 ${Math.max(...WINDOWS)}일 윈도우는 부분 커버리지일 수 있습니다.`
        });
    }

    if (!latestProductDailyDate) {
        warnings.push({
            code: 'product_daily_missing',
            message: 'product_daily.csv가 비어 있어 synthetic window metric 생성이 제한됩니다.'
        });
    } else if (asOfDate && asOfDate > latestProductDailyDate) {
        warnings.push({
            code: 'as_of_date_after_latest_snapshot',
            message: `요청한 as_of_date(${asOfDate})가 product_daily 최신일(${latestProductDailyDate})보다 뒤입니다.`
        });
    }

    return {
        ...normalized,
        asOfDate,
        windowStartDate,
        latestProductDailyDate,
        warnings
    };
}

export function prepareRawArtifacts(rawArtifacts, options = {}) {
    const extractContext = buildExtractContext(rawArtifacts, options);
    const resolvedRawArtifacts = {
        ...rawArtifacts
    };
    const syntheticArtifacts = {};
    const warnings = [...extractContext.warnings];

    if (shouldSynthesizeProductWindows(rawArtifacts, extractContext)) {
        resolvedRawArtifacts.product_window_metrics = deriveProductWindowMetrics(rawArtifacts.product_daily ?? [], extractContext);
        syntheticArtifacts.product_window_metrics = 'derived_from_product_daily';
        warnings.push({
            code: 'synthetic_product_window_metrics',
            message: 'product_window_metrics.csv를 product_daily.csv 기준으로 합성했습니다.'
        });
    }

    if (shouldSynthesizeBrandWindows(rawArtifacts, extractContext)) {
        resolvedRawArtifacts.brand_window_metrics = deriveBrandWindowMetrics(rawArtifacts.product_daily ?? [], extractContext);
        syntheticArtifacts.brand_window_metrics = 'derived_from_product_daily';
        warnings.push({
            code: 'synthetic_brand_window_metrics',
            message: 'brand_window_metrics.csv를 product_daily.csv 기준으로 합성했습니다.'
        });
    }

    return {
        rawArtifacts: resolvedRawArtifacts,
        extractContext: {
            ...extractContext,
            warnings,
            syntheticArtifacts
        }
    };
}

export function buildRawExtractManifest(rawArtifacts, options = {}) {
    const context = options.extractContext ?? buildExtractContext(rawArtifacts, options);
    const syntheticArtifacts = context.syntheticArtifacts ?? {};
    const warningList = context.warnings ?? [];

    return Object.entries(RAW_INPUT_FILES).map(([artifactName]) => {
        const rows = rawArtifacts[artifactName] ?? [];
        const exists = rows.length > 0;
        const coverage = buildCoverageMetadata(rows, artifactName, context);
        const syntheticReason = syntheticArtifacts[artifactName] ?? '';
        const artifactWarnings = [];

        if (!exists) {
            artifactWarnings.push({
                code: 'missing_or_empty',
                message: `${artifactName} snapshot이 비어 있습니다.`
            });
        }

        if (coverage.coverage_status === 'partial') {
            artifactWarnings.push({
                code: 'partial_lookback_coverage',
                message: `${artifactName} 날짜 커버리지가 요청 lookback 대비 부분적입니다.`
            });
        }

        if (syntheticReason) {
            artifactWarnings.push({
                code: 'synthetic_snapshot',
                message: `${artifactName}는 ${syntheticReason} 방식으로 생성되었습니다.`
            });
        }

        if (context.mxChannelId || context.mxPlatform || context.sample) {
            artifactWarnings.push(...warningList.filter((warning) => (
                warning.code === 'local_filter_not_applied' || warning.code === 'sample_preview_only'
            )));
        }

        return {
            artifact_name: artifactName,
            required: 'true',
            exists: exists ? 'true' : 'false',
            row_count: rows.length,
            notes: syntheticReason || (exists ? 'loaded' : 'missing_or_empty'),
            as_of_date: context.asOfDate,
            expected_lookback_days: coverage.expected_lookback_days,
            min_date: coverage.min_date,
            max_date: coverage.max_date,
            covered_days: coverage.covered_days,
            coverage_ratio: coverage.coverage_ratio,
            coverage_status: coverage.coverage_status,
            synthetic: syntheticReason ? 'true' : 'false',
            synthetic_reason: syntheticReason,
            role_history_mode: context.roleHistoryMode,
            requested_mx_channel_id: context.mxChannelId,
            requested_mx_platform: context.mxPlatform,
            sample: context.sample === true ? 'true' : (context.sample || ''),
            warning_count: artifactWarnings.length,
            warning_code: artifactWarnings.map((warning) => warning.code).join('|'),
            warning_message: artifactWarnings.map((warning) => warning.message).join(' | ')
        };
    });
}

export function summarizeExtractCoverage(rawArtifacts, options = {}) {
    const context = options.extractContext ?? buildExtractContext(rawArtifacts, options);
    const manifest = options.manifest ?? buildRawExtractManifest(rawArtifacts, { extractContext: context });
    const productDailyCoverage = manifest.find((row) => row.artifact_name === 'product_daily') ?? {};
    const pgmCoverage = manifest.find((row) => row.artifact_name === 'pgm_scored') ?? {};
    const syntheticCount = manifest.filter((row) => row.synthetic === 'true').length;

    return [
        {
            metric_name: 'role_history_mode',
            metric_value: context.roleHistoryMode,
            message: '현재 파이프라인이 role state history를 해석하는 모드'
        },
        {
            metric_name: 'raw_product_daily_lookback_coverage',
            metric_value: productDailyCoverage.coverage_ratio ?? '',
            message: '요청 lookback 대비 product_daily 날짜 커버리지'
        },
        {
            metric_name: 'raw_pgm_scored_lookback_coverage',
            metric_value: pgmCoverage.coverage_ratio ?? '',
            message: '요청 lookback 대비 pgm_scored 날짜 커버리지'
        },
        {
            metric_name: 'synthetic_raw_artifact_count',
            metric_value: syntheticCount,
            message: '원본 파일 부재/불일치로 synthetic 생성된 raw artifact 수'
        },
        {
            metric_name: 'raw_extract_warning_count',
            metric_value: context.warnings?.length ?? 0,
            message: 'run_extract/raw snapshot 해석 단계에서 누적된 warning 수'
        }
    ];
}
