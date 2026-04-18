import { getLatestDate } from '../../transforms/base/date_windows.js';

export function buildRevenueStructureChart(productTableRows) {
    const latestDate = getLatestDate(productTableRows, 'as_of_date');

    return productTableRows
        .filter((row) => row.as_of_date === latestDate)
        .slice(0, 8)
        .map((row) => ({
            product_id: row.product_id,
            product_name: row.product_name,
            revenue: row.revenue,
            revenue_share_in_brand_day: row.revenue_share_in_brand_day,
            role_state_primary: row.role_state_primary
        }));
}
