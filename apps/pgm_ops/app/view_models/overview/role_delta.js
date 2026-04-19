import { buildOverviewRoleAnalytics } from './role_decomposition.js';

export function buildOverviewRoleDelta(productDailyMetrics, productRoleStateDaily) {
    return buildOverviewRoleAnalytics(productDailyMetrics, productRoleStateDaily)
        .flatMap((periodRow) => periodRow.roles.map((roleRow) => ({
            period: periodRow.period,
            role_state_primary: roleRow.role_state_primary,
            role_label: roleRow.role_label,
            current_revenue: roleRow.current_revenue,
            previous_revenue: roleRow.previous_revenue,
            revenue_delta: roleRow.revenue_delta,
            revenue_delta_rate: roleRow.revenue_delta_rate,
            current_revenue_share: roleRow.current_revenue_share,
            previous_revenue_share: roleRow.previous_revenue_share,
            revenue_share_delta: roleRow.revenue_share_delta,
            role_rank: roleRow.role_rank,
            as_of_date: roleRow.as_of_date,
            support_window_days: roleRow.support_window_days,
            current_covered_days: roleRow.current_covered_days,
            previous_covered_days: roleRow.previous_covered_days,
            expected_window_days: roleRow.expected_window_days,
            partial_history_flag: roleRow.partial_history_flag,
            role_history_mode: roleRow.role_history_mode,
            role_history_mode_label: roleRow.role_history_mode_label,
            role_history_warning_level: roleRow.role_history_warning_level,
            role_history_warning_title: roleRow.role_history_warning_title,
            role_history_warning_copy: roleRow.role_history_warning_copy,
            role_history_basis_copy: roleRow.role_history_basis_copy,
            coverage_summary: roleRow.coverage_summary,
            evidence_status: roleRow.evidence_status,
            evidence_status_label: roleRow.evidence_status_label,
            can_compare_roles: roleRow.can_compare_roles,
            truth_mismatch_flag: roleRow.truth_mismatch_flag,
            truth_mismatch_copy: roleRow.truth_mismatch_copy
        })));
}
