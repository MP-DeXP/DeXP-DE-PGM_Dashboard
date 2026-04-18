import { compareRoleKeys } from '../../transforms/role/role_helpers.js';

export function buildBrandRoleWindowComparison(roleProductMembershipWindow) {
    return [...roleProductMembershipWindow]
        .sort((left, right) => {
            const windowCompare = Number(left.window_days ?? 0) - Number(right.window_days ?? 0);
            if (windowCompare !== 0) {
                return windowCompare;
            }

            const roleCompare = compareRoleKeys(left.role_state_primary, right.role_state_primary);
            if (roleCompare !== 0) {
                return roleCompare;
            }

            return Number(left.role_rank ?? 0) - Number(right.role_rank ?? 0);
        })
        .map((row) => ({
            as_of_date: row.as_of_date,
            window_days: row.window_days,
            role_state_primary: row.role_state_primary,
            role_label: row.role_label,
            product_id: row.product_id,
            product_name: row.product_name,
            image_url: row.image_url ?? '',
            detail_url: row.detail_url ?? '',
            window_revenue: row.window_revenue,
            share_in_role: row.share_in_role,
            share_in_brand_window: row.share_in_brand_window,
            role_rank: row.role_rank,
            revenue_source: row.revenue_source
        }));
}
