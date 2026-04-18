import { PERIODS, ROLE_KEYS, getRoleLabel } from '../../config/constants.js';
import { getLatestDate } from '../../transforms/base/date_windows.js';
import { compareRoleKeys } from '../../transforms/role/role_helpers.js';

function buildRankedRows(period, rows, asOfDate, supportWindowDays) {
    const grouped = rows.reduce((map, row) => {
        const roleState = row.role_state_primary ?? '';

        if (!map.has(roleState)) {
            map.set(roleState, {
                period,
                role_state_primary: roleState,
                role_label: getRoleLabel(roleState),
                role_revenue: 0,
                role_revenue_share: 0,
                role_rank: 0,
                as_of_date: asOfDate,
                support_window_days: supportWindowDays
            });
        }

        map.get(roleState).role_revenue += Number(row.role_revenue ?? row.revenue ?? row.window_revenue ?? 0);
        return map;
    }, new Map());

    ROLE_KEYS.forEach((roleKey) => {
        if (!grouped.has(roleKey)) {
            grouped.set(roleKey, {
                period,
                role_state_primary: roleKey,
                role_label: getRoleLabel(roleKey),
                role_revenue: 0,
                role_revenue_share: 0,
                role_rank: 0,
                as_of_date: asOfDate,
                support_window_days: supportWindowDays
            });
        }
    });

    const rankedBase = [...grouped.values()]
        .sort((left, right) => {
            const revenueGap = Number(right.role_revenue ?? 0) - Number(left.role_revenue ?? 0);
            if (revenueGap !== 0) {
                return revenueGap;
            }

            return compareRoleKeys(left.role_state_primary, right.role_state_primary);
        });
    const totalRevenue = rankedBase.reduce((sum, row) => sum + Number(row.role_revenue ?? 0), 0);

    return rankedBase.map((row, index) => ({
            ...row,
            role_rank: index + 1,
            role_revenue_share: totalRevenue > 0
                ? Number(row.role_revenue ?? 0) / totalRevenue
                : 0
        }));
}

export function buildOverviewRoleContribution(roleRevenueDaily, roleProductMembershipWindow) {
    const latestDate = getLatestDate(roleRevenueDaily, 'date');
    const dailyRows = roleRevenueDaily.filter((row) => row.date === latestDate);
    const asOfDate = latestDate ?? roleProductMembershipWindow[0]?.as_of_date ?? '';
    const weeklyRows = roleProductMembershipWindow.filter((row) => Number(row.window_days ?? 0) === 7 && row.as_of_date === asOfDate);
    const monthlyRows = roleProductMembershipWindow.filter((row) => Number(row.window_days ?? 0) === 30 && row.as_of_date === asOfDate);

    return PERIODS.flatMap((period) => {
        if (period === 'daily') {
            return buildRankedRows(
                period,
                dailyRows.map((row) => ({ role_state_primary: row.role_state_primary, role_revenue: row.revenue })),
                asOfDate,
                1
            );
        }

        if (period === 'weekly') {
            return buildRankedRows(period, weeklyRows, asOfDate, 7);
        }

        return buildRankedRows(period, monthlyRows, asOfDate, 30);
    });
}
