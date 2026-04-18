import { computeWindowComparison, enrichRowsWithRollingRevenue, getLatestDate } from '../base/date_windows.js';

export function enrichBrandRevenueWindows(brandRows) {
    return enrichRowsWithRollingRevenue(
        brandRows.map((row) => ({
            ...row,
            revenue: Number(row.brand_revenue ?? 0)
        }))
    ).map((row) => ({
        ...row,
        brand_revenue_day_over_day_change_rate: row.revenue_day_over_day_change_rate
    }));
}

export function enrichProductRevenueWindows(productRows) {
    return enrichRowsWithRollingRevenue(productRows, ['product_id']);
}

export function buildBrandWindowSnapshot(brandRows) {
    const latestDate = getLatestDate(brandRows);
    const sortedRows = [...brandRows].sort((left, right) => left.date.localeCompare(right.date));

    return {
        latestDate,
        weekly: computeWindowComparison(sortedRows, 'brand_revenue', latestDate, 7),
        monthly: computeWindowComparison(sortedRows, 'brand_revenue', latestDate, 30),
        quarterly: computeWindowComparison(sortedRows, 'brand_revenue', latestDate, 90)
    };
}
