import { getRoleLabel } from '../../config/constants.js';
import { getLatestDate } from '../../transforms/base/date_windows.js';
import { safeDivide } from '../../transforms/base/null_handling.js';
import { compareRoleKeys } from '../../transforms/role/role_helpers.js';

export function buildBrandRoleStructure(productDailyMetrics, productRoleStateDaily, revenueStructureDaily) {
    const latestDate = getLatestDate(productDailyMetrics);

    if (!latestDate) {
        return [];
    }

    const latestProductRows = productDailyMetrics.filter((row) => row.date === latestDate);
    const roleLookup = new Map(
        productRoleStateDaily
            .filter((row) => row.date === latestDate)
            .map((row) => [row.product_id, row])
    );
    const revenueLookup = new Map(
        revenueStructureDaily
            .filter((row) => row.date === latestDate)
            .map((row) => [row.product_id, row])
    );

    const rows = latestProductRows.map((row) => {
        const roleState = roleLookup.get(row.product_id)?.role_state_primary ?? '';
        const revenueRow = revenueLookup.get(row.product_id);

        return {
            as_of_date: latestDate,
            role_state_primary: roleState,
            role_label: getRoleLabel(roleState),
            product_id: row.product_id,
            product_name: row.product_name,
            image_url: row.image_url ?? '',
            detail_url: row.detail_url ?? '',
            revenue: Number(row.revenue ?? 0),
            revenue_share_in_brand: Number(revenueRow?.revenue_share_in_brand_day ?? 0),
            revenue_share_in_role: 0,
            role_rank: 0
        };
    });

    const totalByRole = rows.reduce((grouped, row) => {
        grouped.set(row.role_state_primary, (grouped.get(row.role_state_primary) ?? 0) + Number(row.revenue ?? 0));
        return grouped;
    }, new Map());
    const groupedRows = rows.reduce((grouped, row) => {
        const key = row.role_state_primary ?? '';
        if (!grouped.has(key)) {
            grouped.set(key, []);
        }

        grouped.get(key).push(row);
        return grouped;
    }, new Map());

    groupedRows.forEach((groupRows) => {
        groupRows
            .sort((left, right) => {
                const revenueGap = Number(right.revenue ?? 0) - Number(left.revenue ?? 0);
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
        .map((row) => ({
            ...row,
            revenue_share_in_role: safeDivide(Number(row.revenue ?? 0), totalByRole.get(row.role_state_primary) ?? 0)
        }))
        .sort((left, right) => {
            const roleCompare = compareRoleKeys(left.role_state_primary, right.role_state_primary);
            if (roleCompare !== 0) {
                return roleCompare;
            }

            return Number(left.role_rank ?? 0) - Number(right.role_rank ?? 0);
        });
}
