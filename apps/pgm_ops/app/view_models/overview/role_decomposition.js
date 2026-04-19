import { getRoleLabel } from '../../config/constants.js';
import { getLatestDate, listDateRange, shiftDate } from '../../transforms/base/date_windows.js';
import { safeDivide } from '../../transforms/base/null_handling.js';
import { compareRoleKeys } from '../../transforms/role/role_helpers.js';
import { buildRoleHistoryMeta, inferRoleHistoryMode } from './role_history_mode.js';

export const OVERVIEW_ROLE_PERIODS = [
    {
        period: 'daily',
        label: '어제(최근 확정일)',
        supportWindowDays: 1,
        currentStartOffset: 0,
        currentEndOffset: 0,
        previousStartOffset: 1,
        previousEndOffset: 1
    },
    {
        period: 'weekly',
        label: '최근 7일',
        supportWindowDays: 7,
        currentStartOffset: 6,
        currentEndOffset: 0,
        previousStartOffset: 13,
        previousEndOffset: 7
    },
    {
        period: 'monthly',
        label: '최근 30일',
        supportWindowDays: 30,
        currentStartOffset: 29,
        currentEndOffset: 0,
        previousStartOffset: 59,
        previousEndOffset: 30
    }
];

function buildWindowDates(asOfDate, startOffset, endOffset) {
    if (!asOfDate) {
        return [];
    }

    return listDateRange(
        shiftDate(asOfDate, -startOffset),
        shiftDate(asOfDate, -endOffset)
    );
}

function compareProducts(left, right) {
    const revenueGap = Number(right.current_revenue ?? 0) - Number(left.current_revenue ?? 0);
    if (revenueGap !== 0) {
        return revenueGap;
    }

    const previousGap = Number(right.previous_revenue ?? 0) - Number(left.previous_revenue ?? 0);
    if (previousGap !== 0) {
        return previousGap;
    }

    return String(left.product_name ?? '').localeCompare(String(right.product_name ?? ''));
}

function compareRoles(left, right) {
    const revenueGap = Number(right.current_revenue ?? 0) - Number(left.current_revenue ?? 0);
    if (revenueGap !== 0) {
        return revenueGap;
    }

    const previousGap = Number(right.previous_revenue ?? 0) - Number(left.previous_revenue ?? 0);
    if (previousGap !== 0) {
        return previousGap;
    }

    return compareRoleKeys(left.role_state_primary, right.role_state_primary);
}

function buildDeltaRate(currentValue, previousValue) {
    const previous = Number(previousValue ?? 0);

    if (!previous) {
        return null;
    }

    return (Number(currentValue ?? 0) - previous) / previous;
}

function finalizeProductRow(productRow, roleCurrentRevenue, rolePreviousRevenue, totalCurrentRevenue) {
    return {
        ...productRow,
        revenue_delta: Number(productRow.current_revenue ?? 0) - Number(productRow.previous_revenue ?? 0),
        revenue_delta_rate: buildDeltaRate(productRow.current_revenue, productRow.previous_revenue),
        current_share_in_role: safeDivide(Number(productRow.current_revenue ?? 0), roleCurrentRevenue) ?? 0,
        current_share_in_period: safeDivide(Number(productRow.current_revenue ?? 0), totalCurrentRevenue) ?? 0,
        previous_share_in_role: safeDivide(Number(productRow.previous_revenue ?? 0), rolePreviousRevenue) ?? 0
    };
}

function buildRoleRows(periodConfig, rows, roleLookup, currentDateSet, previousDateSet, asOfDate, currentCoveredDays, previousCoveredDays, roleHistoryMeta) {
    const roleBuckets = new Map();
    let currentTotalRevenue = 0;
    let previousTotalRevenue = 0;

    rows.forEach((row) => {
        const revenue = Number(row.revenue ?? 0);
        const bucketName = currentDateSet.has(row.date)
            ? 'current'
            : previousDateSet.has(row.date)
                ? 'previous'
                : null;

        if (!bucketName) {
            return;
        }

        const roleState = roleLookup.get(`${row.date}|${row.product_id}`)?.role_state_primary ?? '';
        const roleKey = roleState ?? '';

        if (!roleBuckets.has(roleKey)) {
            roleBuckets.set(roleKey, {
                period: periodConfig.period,
                role_state_primary: roleKey,
                role_label: getRoleLabel(roleKey),
                current_revenue: 0,
                previous_revenue: 0,
                current_revenue_share: 0,
                previous_revenue_share: 0,
                revenue_share_delta: 0,
                revenue_delta: 0,
                revenue_delta_rate: null,
                role_rank: 0,
                as_of_date: asOfDate,
                support_window_days: periodConfig.supportWindowDays,
                current_covered_days: currentCoveredDays,
                previous_covered_days: previousCoveredDays,
                expected_window_days: periodConfig.supportWindowDays,
                partial_history_flag: currentCoveredDays < periodConfig.supportWindowDays || previousCoveredDays < periodConfig.supportWindowDays ? 'true' : 'false',
                role_history_mode: roleHistoryMeta.role_history_mode,
                role_history_mode_label: roleHistoryMeta.role_history_mode_label,
                role_history_warning_level: roleHistoryMeta.role_history_warning_level,
                role_history_warning_title: roleHistoryMeta.role_history_warning_title,
                role_history_warning_copy: roleHistoryMeta.role_history_warning_copy,
                role_history_basis_copy: roleHistoryMeta.role_history_basis_copy,
                coverage_summary: roleHistoryMeta.coverage_summary,
                products: new Map()
            });
        }

        const roleBucket = roleBuckets.get(roleKey);
        const productKey = row.product_id ?? '';

        if (!roleBucket.products.has(productKey)) {
            roleBucket.products.set(productKey, {
                period: periodConfig.period,
                role_state_primary: roleKey,
                role_label: getRoleLabel(roleKey),
                product_id: productKey,
                product_name: row.product_name ?? '',
                image_url: row.image_url ?? '',
                detail_url: row.detail_url ?? '',
                current_revenue: 0,
                previous_revenue: 0,
                current_share_in_role: 0,
                current_share_in_period: 0,
                previous_share_in_role: 0,
                revenue_delta: 0,
                revenue_delta_rate: null,
                product_rank: 0,
                as_of_date: asOfDate,
                support_window_days: periodConfig.supportWindowDays,
                current_covered_days: currentCoveredDays,
                previous_covered_days: previousCoveredDays,
                expected_window_days: periodConfig.supportWindowDays,
                partial_history_flag: currentCoveredDays < periodConfig.supportWindowDays || previousCoveredDays < periodConfig.supportWindowDays ? 'true' : 'false',
                role_history_mode: roleHistoryMeta.role_history_mode,
                role_history_mode_label: roleHistoryMeta.role_history_mode_label,
                role_history_warning_level: roleHistoryMeta.role_history_warning_level,
                role_history_warning_title: roleHistoryMeta.role_history_warning_title,
                role_history_warning_copy: roleHistoryMeta.role_history_warning_copy,
                role_history_basis_copy: roleHistoryMeta.role_history_basis_copy,
                coverage_summary: roleHistoryMeta.coverage_summary
            });
        }

        const productBucket = roleBucket.products.get(productKey);
        productBucket.product_name = productBucket.product_name || row.product_name || '';
        productBucket.image_url = productBucket.image_url || row.image_url || '';
        productBucket.detail_url = productBucket.detail_url || row.detail_url || '';

        if (bucketName === 'current') {
            roleBucket.current_revenue += revenue;
            productBucket.current_revenue += revenue;
            currentTotalRevenue += revenue;
        } else {
            roleBucket.previous_revenue += revenue;
            productBucket.previous_revenue += revenue;
            previousTotalRevenue += revenue;
        }
    });

    const rankedRoles = [...roleBuckets.values()]
        .filter((row) => Number(row.current_revenue ?? 0) > 0 || Number(row.previous_revenue ?? 0) > 0)
        .sort(compareRoles)
        .map((roleRow, index) => {
            const currentRevenue = Number(roleRow.current_revenue ?? 0);
            const previousRevenue = Number(roleRow.previous_revenue ?? 0);
            const currentRevenueShare = safeDivide(currentRevenue, currentTotalRevenue) ?? 0;
            const previousRevenueShare = safeDivide(previousRevenue, previousTotalRevenue) ?? 0;
            const products = [...roleRow.products.values()]
                .map((productRow) => finalizeProductRow(productRow, currentRevenue, previousRevenue, currentTotalRevenue))
                .filter((productRow) => Number(productRow.current_revenue ?? 0) > 0 || Number(productRow.previous_revenue ?? 0) > 0)
                .sort(compareProducts)
                .map((productRow, productIndex) => ({
                    ...productRow,
                    product_rank: productIndex + 1
                }));

            return {
                ...roleRow,
                current_revenue_share: currentRevenueShare,
                previous_revenue_share: previousRevenueShare,
                revenue_share_delta: currentRevenueShare - previousRevenueShare,
                revenue_delta: currentRevenue - previousRevenue,
                revenue_delta_rate: buildDeltaRate(currentRevenue, previousRevenue),
                role_rank: index + 1,
                products
            };
        });

    return {
        period: periodConfig.period,
        period_label: periodConfig.label,
        as_of_date: asOfDate,
        support_window_days: periodConfig.supportWindowDays,
        current_revenue: currentTotalRevenue,
        previous_revenue: previousTotalRevenue,
        revenue_delta: currentTotalRevenue - previousTotalRevenue,
        revenue_delta_rate: buildDeltaRate(currentTotalRevenue, previousTotalRevenue),
        current_covered_days: currentCoveredDays,
        previous_covered_days: previousCoveredDays,
        expected_window_days: periodConfig.supportWindowDays,
        partial_history_flag: currentCoveredDays < periodConfig.supportWindowDays || previousCoveredDays < periodConfig.supportWindowDays ? 'true' : 'false',
        role_history_mode: roleHistoryMeta.role_history_mode,
        role_history_mode_label: roleHistoryMeta.role_history_mode_label,
        role_history_warning_level: roleHistoryMeta.role_history_warning_level,
        role_history_warning_title: roleHistoryMeta.role_history_warning_title,
        role_history_warning_copy: roleHistoryMeta.role_history_warning_copy,
        role_history_basis_copy: roleHistoryMeta.role_history_basis_copy,
        coverage_summary: roleHistoryMeta.coverage_summary,
        roles: rankedRoles
    };
}

export function buildOverviewRoleAnalytics(productDailyMetrics, productRoleStateDaily) {
    const asOfDate = getLatestDate(productDailyMetrics);

    if (!asOfDate) {
        return [];
    }

    const roleLookup = new Map(productRoleStateDaily.map((row) => [`${row.date}|${row.product_id}`, row]));
    const roleDates = new Set(productRoleStateDaily.map((row) => row.date).filter(Boolean));

    return OVERVIEW_ROLE_PERIODS.map((periodConfig) => {
        const currentDates = buildWindowDates(asOfDate, periodConfig.currentStartOffset, periodConfig.currentEndOffset);
        const previousDates = buildWindowDates(asOfDate, periodConfig.previousStartOffset, periodConfig.previousEndOffset);
        const currentDateSet = new Set(currentDates);
        const previousDateSet = new Set(previousDates);
        const scopedRows = productDailyMetrics.filter((row) => currentDateSet.has(row.date) || previousDateSet.has(row.date));
        const scopedRoleRows = productRoleStateDaily.filter((row) => currentDateSet.has(row.date) || previousDateSet.has(row.date));
        const currentCoveredDays = currentDates.filter((date) => roleDates.has(date)).length;
        const previousCoveredDays = previousDates.filter((date) => roleDates.has(date)).length;
        const roleHistoryMeta = buildRoleHistoryMeta({
            roleHistoryMode: inferRoleHistoryMode({
                roleHistorySignals: scopedRoleRows.map((row) => row.role_history_mode ?? row.role_state_source ?? row.revenue_source ?? ''),
                currentCoveredDays,
                previousCoveredDays,
                expectedWindowDays: periodConfig.supportWindowDays
            }),
            currentCoveredDays,
            previousCoveredDays,
            expectedWindowDays: periodConfig.supportWindowDays,
            periodLabel: periodConfig.label
        });

        return buildRoleRows(
            periodConfig,
            scopedRows,
            roleLookup,
            currentDateSet,
            previousDateSet,
            asOfDate,
            currentCoveredDays,
            previousCoveredDays,
            roleHistoryMeta
        );
    });
}
