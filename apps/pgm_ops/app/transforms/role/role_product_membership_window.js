import { WINDOWS, getRoleLabel } from '../../config/constants.js';
import { getLatestDate, shiftDate } from '../base/date_windows.js';
import { safeDivide } from '../base/null_handling.js';
import { compareRoleKeys, pickTopRoleByRevenue } from './role_helpers.js';

function getWindowField(windowDays) {
    return `revenue_${windowDays}d`;
}

function buildWindowRoleRevenue(rows, roleLookup) {
    return rows.reduce((grouped, row) => {
        const roleState = roleLookup.get(`${row.date}|${row.product_id}`)?.role_state_primary ?? '';
        const revenue = Number(row.revenue ?? 0);

        grouped.set(roleState, (grouped.get(roleState) ?? 0) + revenue);
        return grouped;
    }, new Map());
}

export function buildRoleProductMembershipWindow(productDailyMetrics, productRoleStateDaily, windows = WINDOWS) {
    const latestDate = getLatestDate(productDailyMetrics);

    if (!latestDate) {
        return [];
    }

    const roleLookup = new Map(productRoleStateDaily.map((row) => [`${row.date}|${row.product_id}`, row]));
    const rowsByProduct = productDailyMetrics.reduce((grouped, row) => {
        if (!grouped.has(row.product_id)) {
            grouped.set(row.product_id, []);
        }

        grouped.get(row.product_id).push(row);
        return grouped;
    }, new Map());
    const latestRows = productDailyMetrics.filter((row) => row.date === latestDate);
    const rows = [];

    latestRows.forEach((latestRow) => {
        const productRows = [...(rowsByProduct.get(latestRow.product_id) ?? [])]
            .sort((left, right) => String(left.date ?? '').localeCompare(String(right.date ?? '')));

        windows.forEach((windowDays) => {
            const windowStart = shiftDate(latestDate, -windowDays);
            const inWindowRows = productRows.filter((row) => row.date >= windowStart && row.date < latestDate);
            const windowRoleRevenue = buildWindowRoleRevenue(inWindowRows, roleLookup);
            const topRole = pickTopRoleByRevenue(windowRoleRevenue);
            const summedWindowRevenue = [...windowRoleRevenue.values()].reduce((sum, value) => sum + Number(value ?? 0), 0);
            const fallbackWindowRevenue = Number(latestRow[getWindowField(windowDays)] ?? 0);
            const useFallbackMetric = summedWindowRevenue <= 0 && fallbackWindowRevenue > 0;
            const roleState = useFallbackMetric
                ? (roleLookup.get(`${latestDate}|${latestRow.product_id}`)?.role_state_primary ?? '')
                : topRole.role_state_primary;
            const windowRevenue = useFallbackMetric ? fallbackWindowRevenue : summedWindowRevenue;

            if (windowRevenue <= 0) {
                return;
            }

            rows.push({
                as_of_date: latestDate,
                window_days: windowDays,
                role_state_primary: roleState,
                role_label: getRoleLabel(roleState),
                product_id: latestRow.product_id,
                product_name: latestRow.product_name,
                image_url: latestRow.image_url ?? '',
                detail_url: latestRow.detail_url ?? '',
                window_revenue: windowRevenue,
                representative_role_revenue: useFallbackMetric ? windowRevenue : topRole.role_revenue,
                share_in_role: 0,
                share_in_brand_window: 0,
                role_rank: 0,
                revenue_source: useFallbackMetric ? 'window_metric_latest_snapshot' : 'window_role_revenue_sum'
            });
        });
    });

    const totalByWindow = rows.reduce((grouped, row) => {
        const key = `${row.as_of_date}|${row.window_days}`;
        grouped.set(key, (grouped.get(key) ?? 0) + Number(row.window_revenue ?? 0));
        return grouped;
    }, new Map());
    const totalByWindowRole = rows.reduce((grouped, row) => {
        const key = `${row.as_of_date}|${row.window_days}|${row.role_state_primary}`;
        grouped.set(key, (grouped.get(key) ?? 0) + Number(row.window_revenue ?? 0));
        return grouped;
    }, new Map());
    const rankedRows = rows.reduce((grouped, row) => {
        const key = `${row.as_of_date}|${row.window_days}|${row.role_state_primary}`;
        if (!grouped.has(key)) {
            grouped.set(key, []);
        }

        grouped.get(key).push(row);
        return grouped;
    }, new Map());

    rankedRows.forEach((groupRows) => {
        groupRows
            .sort((left, right) => {
                const revenueGap = Number(right.window_revenue ?? 0) - Number(left.window_revenue ?? 0);
                if (revenueGap !== 0) {
                    return revenueGap;
                }

                return String(left.product_name ?? '').localeCompare(String(right.product_name ?? ''));
            })
            .forEach((row, index) => {
                row.role_rank = index + 1;
            });
    });

    return rows
        .map((row) => {
            const windowKey = `${row.as_of_date}|${row.window_days}`;
            const roleKey = `${row.as_of_date}|${row.window_days}|${row.role_state_primary}`;

            return {
                ...row,
                share_in_role: safeDivide(Number(row.window_revenue ?? 0), totalByWindowRole.get(roleKey) ?? 0),
                share_in_brand_window: safeDivide(Number(row.window_revenue ?? 0), totalByWindow.get(windowKey) ?? 0)
            };
        })
        .sort((left, right) => {
            const dateCompare = String(left.as_of_date ?? '').localeCompare(String(right.as_of_date ?? ''));
            if (dateCompare !== 0) {
                return dateCompare;
            }

            const windowCompare = Number(left.window_days ?? 0) - Number(right.window_days ?? 0);
            if (windowCompare !== 0) {
                return windowCompare;
            }

            const roleCompare = compareRoleKeys(left.role_state_primary, right.role_state_primary);
            if (roleCompare !== 0) {
                return roleCompare;
            }

            return Number(left.role_rank ?? 0) - Number(right.role_rank ?? 0);
        });
}
