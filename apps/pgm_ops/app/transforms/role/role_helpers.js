import { getRoleLabel, getRoleSortOrder } from '../../config/constants.js';

export function compareRoleKeys(leftRole, rightRole) {
    const orderGap = getRoleSortOrder(leftRole) - getRoleSortOrder(rightRole);

    if (orderGap !== 0) {
        return orderGap;
    }

    return getRoleLabel(leftRole).localeCompare(getRoleLabel(rightRole));
}

export function pickTopRoleByRevenue(roleRevenueMap) {
    const ranked = [...roleRevenueMap.entries()]
        .sort((left, right) => {
            const revenueGap = Number(right[1] ?? 0) - Number(left[1] ?? 0);
            if (revenueGap !== 0) {
                return revenueGap;
            }

            return compareRoleKeys(left[0], right[0]);
        });

    if (!ranked.length) {
        return {
            role_state_primary: '',
            role_revenue: 0
        };
    }

    return {
        role_state_primary: ranked[0][0] ?? '',
        role_revenue: Number(ranked[0][1] ?? 0)
    };
}
