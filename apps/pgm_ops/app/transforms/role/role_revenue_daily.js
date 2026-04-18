import { getRoleLabel } from '../../config/constants.js';
import { safeDivide } from '../base/null_handling.js';
import { compareRoleKeys } from './role_helpers.js';

export function buildRoleRevenueDaily(productDailyMetrics, productRoleStateDaily) {
    const roleLookup = new Map(productRoleStateDaily.map((row) => [`${row.date}|${row.product_id}`, row]));
    const groupedByDate = productDailyMetrics.reduce((grouped, row) => {
        if (!grouped.has(row.date)) {
            grouped.set(row.date, []);
        }

        grouped.get(row.date).push(row);
        return grouped;
    }, new Map());

    const rows = [];

    groupedByDate.forEach((dateRows, date) => {
        const roleBuckets = new Map();
        const brandRevenue = dateRows.reduce((sum, row) => sum + Number(row.revenue ?? 0), 0);

        dateRows.forEach((row) => {
            const roleState = roleLookup.get(`${row.date}|${row.product_id}`)?.role_state_primary ?? '';

            if (!roleBuckets.has(roleState)) {
                roleBuckets.set(roleState, {
                    date,
                    role_state_primary: roleState,
                    role_label: getRoleLabel(roleState),
                    revenue: 0,
                    product_count: 0
                });
            }

            const bucket = roleBuckets.get(roleState);
            bucket.revenue += Number(row.revenue ?? 0);
            bucket.product_count += 1;
        });

        [...roleBuckets.values()]
            .sort((left, right) => {
                const revenueGap = Number(right.revenue ?? 0) - Number(left.revenue ?? 0);
                if (revenueGap !== 0) {
                    return revenueGap;
                }

                return compareRoleKeys(left.role_state_primary, right.role_state_primary);
            })
            .forEach((row, index) => {
                rows.push({
                    ...row,
                    revenue_share_in_brand_day: safeDivide(Number(row.revenue ?? 0), brandRevenue),
                    revenue_rank_in_brand_day: index + 1
                });
            });
    });

    return rows.sort((left, right) => {
        const dateCompare = String(left.date ?? '').localeCompare(String(right.date ?? ''));
        if (dateCompare !== 0) {
            return dateCompare;
        }

        return Number(left.revenue_rank_in_brand_day ?? 0) - Number(right.revenue_rank_in_brand_day ?? 0);
    });
}
