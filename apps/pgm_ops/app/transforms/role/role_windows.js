import { ROLE_LABEL_FALLBACK } from '../../config/constants.js';
import { getLatestDate } from '../base/date_windows.js';
import { safeDivide } from '../base/null_handling.js';

export function buildRoleWindowSummary(productDailyMetrics, roleStateDaily) {
    const latestDate = getLatestDate(productDailyMetrics);
    const roleLookup = new Map(roleStateDaily.map((row) => [`${row.date}|${row.product_id}`, row]));
    const latestRows = productDailyMetrics.filter((row) => row.date === latestDate);
    const grouped = new Map();
    const totalRevenue = latestRows.reduce((sum, row) => sum + Number(row.revenue ?? 0), 0);

    latestRows.forEach((row) => {
        // Blank mart states are bucketed under a display label for chart readability only.
        // No latest-available role is ever copied into the daily state.
        const role = roleLookup.get(`${row.date}|${row.product_id}`)?.role_state_primary || ROLE_LABEL_FALLBACK;
        if (!grouped.has(role)) {
            grouped.set(role, {
                role_state_primary: role,
                revenue: 0,
                product_count: 0
            });
        }

        const bucket = grouped.get(role);
        bucket.revenue += Number(row.revenue ?? 0);
        bucket.product_count += 1;
    });

    return [...grouped.values()]
        .map((row) => ({
            ...row,
            revenue_share: safeDivide(row.revenue, totalRevenue)
        }))
        .sort((left, right) => Number(right.revenue ?? 0) - Number(left.revenue ?? 0));
}
