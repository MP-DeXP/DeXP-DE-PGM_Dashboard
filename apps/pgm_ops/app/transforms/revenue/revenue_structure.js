import { safeDivide } from '../base/null_handling.js';

export function buildRevenueStructureDaily(productDailyMetrics) {
    const groupedByDate = productDailyMetrics.reduce((grouped, row) => {
        if (!grouped.has(row.date)) {
            grouped.set(row.date, []);
        }

        grouped.get(row.date).push(row);
        return grouped;
    }, new Map());

    const rows = [];

    groupedByDate.forEach((dateRows, date) => {
        const totalRevenue = dateRows.reduce((sum, row) => sum + Number(row.revenue ?? 0), 0);
        const ranked = [...dateRows].sort((left, right) => Number(right.revenue ?? 0) - Number(left.revenue ?? 0));

        ranked.forEach((row, index) => {
            rows.push({
                date,
                product_id: row.product_id,
                revenue: Number(row.revenue ?? 0),
                revenue_share_in_brand_day: safeDivide(Number(row.revenue ?? 0), totalRevenue),
                revenue_rank_in_brand_day: index + 1
            });
        });
    });

    return rows.sort((left, right) => `${left.date}|${left.product_id}`.localeCompare(`${right.date}|${right.product_id}`));
}
