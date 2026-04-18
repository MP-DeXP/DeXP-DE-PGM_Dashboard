import { computeWindowComparison, enrichRowsWithRollingRevenue, getLatestDate } from '../base/date_windows.js';

function buildRate(currentValue, previousValue) {
    const previous = Number(previousValue ?? 0);

    if (!previous) {
        return null;
    }

    return (Number(currentValue ?? 0) - previous) / previous;
}

export function enrichBrandRevenueWindows(brandRows, brandWindowMetrics = []) {
    const enrichedRows = enrichRowsWithRollingRevenue(
        brandRows.map((row) => ({
            ...row,
            revenue: Number(row.brand_revenue ?? 0)
        }))
    ).map((row) => ({
        ...row,
        brand_revenue_day_over_day_change_rate: row.revenue_day_over_day_change_rate
    }));

    const latestDate = getLatestDate(enrichedRows);
    const latestMetrics = brandWindowMetrics.find((row) => row.as_of_date === latestDate) ?? brandWindowMetrics[0];

    if (!latestMetrics) {
        return enrichedRows;
    }

    return enrichedRows.map((row) => {
        if (row.date !== latestDate) {
            return row;
        }

        return {
            ...row,
            brand_revenue: Number(latestMetrics.revenue_today ?? row.brand_revenue ?? 0),
            revenue_7d: Number(latestMetrics.revenue_7d ?? row.revenue_7d ?? 0),
            revenue_30d: Number(latestMetrics.revenue_30d ?? row.revenue_30d ?? 0),
            revenue_90d: Number(latestMetrics.revenue_90d ?? row.revenue_90d ?? 0),
            revenue_day_over_day_change_rate: buildRate(latestMetrics.revenue_today, latestMetrics.revenue_prev_day),
            brand_revenue_day_over_day_change_rate: buildRate(latestMetrics.revenue_today, latestMetrics.revenue_prev_day)
        };
    });
}

export function enrichProductRevenueWindows(productRows, productWindowMetrics = []) {
    const enrichedRows = enrichRowsWithRollingRevenue(productRows, ['product_id']);
    const latestDate = getLatestDate(enrichedRows);
    const metricLookup = new Map(productWindowMetrics.map((row) => [row.product_id, row]));

    if (!latestDate || !metricLookup.size) {
        return enrichedRows;
    }

    return enrichedRows.map((row) => {
        if (row.date !== latestDate) {
            return row;
        }

        const metric = metricLookup.get(row.product_id);

        if (!metric) {
            return row;
        }

        return {
            ...row,
            revenue: Number(metric.revenue_today ?? row.revenue ?? 0),
            revenue_7d: Number(metric.revenue_7d ?? row.revenue_7d ?? 0),
            revenue_30d: Number(metric.revenue_30d ?? row.revenue_30d ?? 0),
            revenue_90d: Number(metric.revenue_90d ?? row.revenue_90d ?? 0),
            revenue_day_over_day_change_rate: buildRate(metric.revenue_today, metric.revenue_prev_day)
        };
    });
}

export function buildBrandWindowSnapshot(brandRows, brandWindowMetrics = []) {
    const latestDate = getLatestDate(brandRows);
    const sortedRows = [...brandRows].sort((left, right) => left.date.localeCompare(right.date));
    const latestMetrics = brandWindowMetrics.find((row) => row.as_of_date === latestDate) ?? brandWindowMetrics[0];

    if (latestMetrics) {
        return {
            latestDate,
            weekly: {
                current: Number(latestMetrics.revenue_7d ?? 0),
                previous: Number(latestMetrics.revenue_7d_prev ?? 0),
                deltaRate: buildRate(latestMetrics.revenue_7d, latestMetrics.revenue_7d_prev)
            },
            monthly: {
                current: Number(latestMetrics.revenue_30d ?? 0),
                previous: Number(latestMetrics.revenue_30d_prev ?? 0),
                deltaRate: buildRate(latestMetrics.revenue_30d, latestMetrics.revenue_30d_prev)
            },
            quarterly: {
                current: Number(latestMetrics.revenue_90d ?? 0),
                previous: Number(latestMetrics.revenue_90d_prev ?? 0),
                deltaRate: buildRate(latestMetrics.revenue_90d, latestMetrics.revenue_90d_prev)
            }
        };
    }

    return {
        latestDate,
        weekly: computeWindowComparison(sortedRows, 'brand_revenue', latestDate, 7),
        monthly: computeWindowComparison(sortedRows, 'brand_revenue', latestDate, 30),
        quarterly: computeWindowComparison(sortedRows, 'brand_revenue', latestDate, 90)
    };
}
