import { buildOverviewRoleAnalytics } from './role_decomposition.js';

export function buildOverviewRoleDrilldown(productDailyMetrics, productRoleStateDaily) {
    return buildOverviewRoleAnalytics(productDailyMetrics, productRoleStateDaily)
        .flatMap((periodRow) => periodRow.roles.flatMap((roleRow) => roleRow.products.map((productRow) => ({
            period: periodRow.period,
            role_state_primary: roleRow.role_state_primary,
            role_label: roleRow.role_label,
            product_id: productRow.product_id,
            product_name: productRow.product_name,
            image_url: productRow.image_url,
            detail_url: productRow.detail_url,
            current_revenue: productRow.current_revenue,
            previous_revenue: productRow.previous_revenue,
            revenue_delta: productRow.revenue_delta,
            revenue_delta_rate: productRow.revenue_delta_rate,
            current_share_in_role: productRow.current_share_in_role,
            current_share_in_period: productRow.current_share_in_period,
            previous_share_in_role: productRow.previous_share_in_role,
            product_rank: productRow.product_rank,
            as_of_date: productRow.as_of_date,
            support_window_days: productRow.support_window_days,
            current_covered_days: productRow.current_covered_days,
            previous_covered_days: productRow.previous_covered_days,
            expected_window_days: productRow.expected_window_days,
            partial_history_flag: productRow.partial_history_flag,
            role_history_mode: productRow.role_history_mode,
            role_history_mode_label: productRow.role_history_mode_label,
            role_history_warning_level: productRow.role_history_warning_level,
            role_history_warning_title: productRow.role_history_warning_title,
            role_history_warning_copy: productRow.role_history_warning_copy,
            role_history_basis_copy: productRow.role_history_basis_copy,
            coverage_summary: productRow.coverage_summary,
            evidence_status: productRow.evidence_status,
            evidence_status_label: productRow.evidence_status_label,
            can_compare_roles: productRow.can_compare_roles,
            truth_mismatch_flag: productRow.truth_mismatch_flag,
            truth_mismatch_copy: productRow.truth_mismatch_copy
        }))));
}
