import { getLatestDate, shiftDate } from '../../transforms/base/date_windows.js';
import {
    buildOverviewRoleAnalytics,
    ROLE_EVIDENCE_STATUS_LIMITED,
    ROLE_EVIDENCE_STATUS_UNAVAILABLE
} from './role_decomposition.js';
import { buildRoleHistoryMeta, isSyntheticRoleHistoryMode } from './role_history_mode.js';

function pickLeadRole(roleRows) {
    return roleRows[0] ?? null;
}

function pickSwingRole(roleRows) {
    return [...roleRows]
        .sort((left, right) => {
            const gap = Math.abs(Number(right.revenue_delta ?? 0)) - Math.abs(Number(left.revenue_delta ?? 0));
            if (gap !== 0) {
                return gap;
            }

            return Number(right.current_revenue ?? 0) - Number(left.current_revenue ?? 0);
        })[0] ?? null;
}

function buildDeltaRate(currentValue, previousValue) {
    const previous = Number(previousValue ?? 0);

    if (!previous) {
        return null;
    }

    return (Number(currentValue ?? 0) - previous) / previous;
}

function hasTruthMismatch(observedValue, truthValue) {
    return Math.abs(Number(observedValue ?? 0) - Number(truthValue ?? 0)) > 0.000001;
}

function buildTruthMismatchCopy(periodRow, revenueTruth) {
    const observedCurrentRevenue = Number(periodRow.current_revenue ?? 0);
    const observedPreviousRevenue = Number(periodRow.previous_revenue ?? 0);
    const truthCurrentRevenue = Number(revenueTruth.current_revenue ?? 0);
    const truthPreviousRevenue = Number(revenueTruth.previous_revenue ?? 0);
    const currentGap = truthCurrentRevenue - observedCurrentRevenue;
    const previousGap = truthPreviousRevenue - observedPreviousRevenue;

    if (!hasTruthMismatch(observedCurrentRevenue, truthCurrentRevenue) && !hasTruthMismatch(observedPreviousRevenue, truthPreviousRevenue)) {
        return periodRow.truth_mismatch_copy;
    }

    return `${periodRow.period_label} 총매출 truth는 ${revenueTruth.truth_source} 기준 ${truthCurrentRevenue}/${truthPreviousRevenue}이고, 역할 근거 합계는 ${observedCurrentRevenue}/${observedPreviousRevenue}라 현재 gap ${currentGap}, 비교 gap ${previousGap}가 있습니다.`;
}

function sortRowsByDate(rows) {
    return [...rows].sort((left, right) => String(left.date ?? '').localeCompare(String(right.date ?? '')));
}

function getDailyTruth(brandRows, windowSnapshot, fallbackAsOfDate) {
    const dailySnapshot = windowSnapshot?.daily ?? null;

    if (dailySnapshot) {
        const currentRevenue = Number(dailySnapshot.current ?? 0);
        const previousRevenue = Number(dailySnapshot.previous ?? 0);

        return {
            as_of_date: windowSnapshot?.latestDate ?? fallbackAsOfDate ?? null,
            current_revenue: currentRevenue,
            previous_revenue: previousRevenue,
            revenue_delta: currentRevenue - previousRevenue,
            revenue_delta_rate: buildDeltaRate(currentRevenue, previousRevenue),
            truth_source: 'brand_window_snapshot'
        };
    }

    const sortedRows = sortRowsByDate(brandRows);
    const latestRow = sortedRows[sortedRows.length - 1] ?? null;
    const asOfDate = latestRow?.date ?? fallbackAsOfDate ?? null;
    const exactPreviousDate = asOfDate ? shiftDate(asOfDate, -1) : null;
    const previousRow = sortedRows.find((row) => row.date === exactPreviousDate)
        ?? sortedRows[sortedRows.length - 2]
        ?? null;
    const currentRevenue = Number(latestRow?.brand_revenue ?? 0);
    const previousRevenue = Number(previousRow?.brand_revenue ?? latestRow?.previous_value ?? 0);

    return {
        as_of_date: asOfDate,
        current_revenue: currentRevenue,
        previous_revenue: previousRevenue,
        revenue_delta: currentRevenue - previousRevenue,
        revenue_delta_rate: buildDeltaRate(currentRevenue, previousRevenue),
        truth_source: 'brand_daily_rows'
    };
}

function getSnapshotTruth(period, brandRows, windowSnapshot, fallbackPeriodRow) {
    const dailyTruth = getDailyTruth(brandRows, windowSnapshot, fallbackPeriodRow.as_of_date);

    if (period === 'daily') {
        return dailyTruth;
    }

    const snapshotKey = period === 'weekly' ? 'weekly' : period === 'monthly' ? 'monthly' : '';
    const snapshot = snapshotKey ? windowSnapshot?.[snapshotKey] ?? null : null;

    if (!snapshot) {
        return {
            as_of_date: dailyTruth.as_of_date ?? fallbackPeriodRow.as_of_date,
            current_revenue: fallbackPeriodRow.current_revenue,
            previous_revenue: fallbackPeriodRow.previous_revenue,
            revenue_delta: fallbackPeriodRow.revenue_delta,
            revenue_delta_rate: fallbackPeriodRow.revenue_delta_rate,
            truth_source: 'role_window_fallback'
        };
    }

    const currentRevenue = Number(snapshot.current ?? 0);
    const previousRevenue = Number(snapshot.previous ?? 0);

    return {
        as_of_date: windowSnapshot?.latestDate ?? dailyTruth.as_of_date ?? fallbackPeriodRow.as_of_date,
        current_revenue: currentRevenue,
        previous_revenue: previousRevenue,
        revenue_delta: currentRevenue - previousRevenue,
        revenue_delta_rate: buildDeltaRate(currentRevenue, previousRevenue),
        truth_source: 'brand_window_snapshot'
    };
}

function buildCoverageFields(periodRow) {
    const expectedWindowDays = Number(periodRow.expected_window_days ?? 0);
    const currentCoveredDays = Number(periodRow.current_covered_days ?? 0);
    const previousCoveredDays = Number(periodRow.previous_covered_days ?? 0);
    const currentCoverageRate = expectedWindowDays ? currentCoveredDays / expectedWindowDays : null;
    const previousCoverageRate = expectedWindowDays ? previousCoveredDays / expectedWindowDays : null;
    const roleHistoryMeta = buildRoleHistoryMeta({
        roleHistoryMode: periodRow.role_history_mode,
        currentCoveredDays,
        previousCoveredDays,
        expectedWindowDays,
        periodLabel: periodRow.period_label
    });

    return {
        current_coverage_rate: currentCoverageRate,
        previous_coverage_rate: previousCoverageRate,
        coverage_summary: periodRow.coverage_summary ?? roleHistoryMeta.coverage_summary,
        history_warning_copy: periodRow.role_history_warning_copy ?? roleHistoryMeta.role_history_warning_copy,
        role_history_mode: roleHistoryMeta.role_history_mode,
        role_history_mode_label: roleHistoryMeta.role_history_mode_label,
        role_history_warning_level: roleHistoryMeta.role_history_warning_level,
        role_history_warning_title: roleHistoryMeta.role_history_warning_title,
        role_history_warning_copy: periodRow.role_history_warning_copy ?? roleHistoryMeta.role_history_warning_copy,
        role_history_basis_copy: periodRow.role_history_basis_copy ?? roleHistoryMeta.role_history_basis_copy,
        evidence_status: periodRow.evidence_status,
        evidence_status_label: periodRow.evidence_status_label,
        can_compare_roles: periodRow.can_compare_roles,
        truth_mismatch_flag: periodRow.truth_mismatch_flag,
        truth_mismatch_copy: periodRow.truth_mismatch_copy
    };
}

function buildHeadline(periodRow, leadRole) {
    if (periodRow.evidence_status === ROLE_EVIDENCE_STATUS_UNAVAILABLE) {
        return `${periodRow.period_label} 브랜드 실매출은 유지했지만 역할 근거가 부족해 역할 비교를 중단했습니다.`;
    }

    if (periodRow.evidence_status === ROLE_EVIDENCE_STATUS_LIMITED) {
        return `${periodRow.period_label} 브랜드 실매출은 전체 기준이지만 역할 근거는 일부 날짜만 있어 truth mismatch를 함께 봐야 합니다.`;
    }

    if (isSyntheticRoleHistoryMode(periodRow.role_history_mode)) {
        if (!leadRole) {
            return `${periodRow.period_label} 실매출은 집계했지만 역할 분해는 latest-role assumption 기준 참고치만 제공합니다.`;
        }

        return `${periodRow.period_label} 역할 분해는 latest-role assumption으로 계산해 ${leadRole.role_label} 축을 우선 참고합니다. 실제 역할 이동으로 단정하지 마세요.`;
    }

    if (periodRow.partial_history_flag === 'true') {
        if (!leadRole) {
            return `${periodRow.period_label} 실매출은 집계됐지만 역할 관측 이력은 일부만 확보되어 있습니다.`;
        }

        return `${periodRow.period_label} 실매출은 전체 합계로 집계했고 역할 해석은 가용 역할 관측 기준으로 ${leadRole.role_label} 축을 먼저 확인합니다.`;
    }

    if (!leadRole) {
        return `${periodRow.period_label} 매출 분해 데이터가 아직 부족합니다.`;
    }

    return `${periodRow.period_label} 매출 변화와 함께 가장 크게 관측된 역할은 ${leadRole.role_label}입니다.`;
}

function buildNote(periodRow, swingRole, coverageFields, truthMismatchCopy) {
    if (periodRow.evidence_status === ROLE_EVIDENCE_STATUS_UNAVAILABLE) {
        return `${truthMismatchCopy} ${coverageFields.role_history_basis_copy}`;
    }

    if (periodRow.evidence_status === ROLE_EVIDENCE_STATUS_LIMITED) {
        if (!swingRole) {
            return `${truthMismatchCopy} ${coverageFields.role_history_warning_copy}`;
        }

        return `${truthMismatchCopy} 현재 관측된 역할 근거 안에서는 ${swingRole.role_label} 변화가 가장 큽니다.`;
    }

    if (isSyntheticRoleHistoryMode(periodRow.role_history_mode)) {
        return `${coverageFields.role_history_warning_copy} 기간 총매출과 역할 변화 해석은 같은 의미가 아니며 실제 역할 이동 이력으로 단정하면 안 됩니다.`;
    }

    if (periodRow.partial_history_flag === 'true') {
        return `${coverageFields.role_history_warning_copy} 기간 총매출과 역할 변화 해석은 같은 의미가 아닙니다.`;
    }

    if (!swingRole) {
        return '역할별 매출 변화가 크지 않아 이번 구간은 구조 해석보다 유지 여부 확인이 우선입니다.';
    }

    return `${swingRole.role_label}의 매출 변화폭이 가장 커서 이 역할 안의 SKU와 보조 신호를 먼저 확인하는 편이 안전합니다.`;
}

function normalizeTruthInputs(brandRowsOrWindowSnapshot, maybeWindowSnapshot) {
    if (Array.isArray(brandRowsOrWindowSnapshot)) {
        return {
            brandRows: brandRowsOrWindowSnapshot,
            windowSnapshot: maybeWindowSnapshot ?? null
        };
    }

    return {
        brandRows: [],
        windowSnapshot: brandRowsOrWindowSnapshot ?? null
    };
}

export function buildOverviewRevenueStory(productDailyMetrics, productRoleStateDaily, brandRowsOrWindowSnapshot = [], maybeWindowSnapshot = null) {
    const { brandRows, windowSnapshot } = normalizeTruthInputs(brandRowsOrWindowSnapshot, maybeWindowSnapshot);

    return buildOverviewRoleAnalytics(productDailyMetrics, productRoleStateDaily).map((periodRow) => {
        const leadRole = pickLeadRole(periodRow.roles);
        const swingRole = pickSwingRole(periodRow.roles);
        const revenueTruth = getSnapshotTruth(periodRow.period, brandRows, windowSnapshot, periodRow);
        const coverageFields = buildCoverageFields(periodRow);
        const truthMismatchFlag = hasTruthMismatch(periodRow.current_revenue, revenueTruth.current_revenue)
            || hasTruthMismatch(periodRow.previous_revenue, revenueTruth.previous_revenue);
        const truthMismatchCopy = truthMismatchFlag
            ? buildTruthMismatchCopy(periodRow, revenueTruth)
            : coverageFields.truth_mismatch_copy;

        return {
            period: periodRow.period,
            period_label: periodRow.period_label,
            as_of_date: revenueTruth.as_of_date ?? periodRow.as_of_date ?? getLatestDate(brandRows),
            support_window_days: periodRow.support_window_days,
            current_revenue: revenueTruth.current_revenue,
            previous_revenue: revenueTruth.previous_revenue,
            revenue_delta: revenueTruth.revenue_delta,
            revenue_delta_rate: revenueTruth.revenue_delta_rate,
            lead_role_state_primary: leadRole?.role_state_primary ?? '',
            lead_role_label: leadRole?.role_label ?? '',
            lead_role_revenue_share: leadRole?.current_revenue_share ?? 0,
            swing_role_state_primary: swingRole?.role_state_primary ?? '',
            swing_role_label: swingRole?.role_label ?? '',
            swing_role_delta_revenue: swingRole?.revenue_delta ?? 0,
            current_covered_days: periodRow.current_covered_days,
            previous_covered_days: periodRow.previous_covered_days,
            expected_window_days: periodRow.expected_window_days,
            partial_history_flag: periodRow.partial_history_flag,
            current_coverage_rate: coverageFields.current_coverage_rate,
            previous_coverage_rate: coverageFields.previous_coverage_rate,
            coverage_summary: coverageFields.coverage_summary,
            role_history_mode: coverageFields.role_history_mode,
            role_history_mode_label: coverageFields.role_history_mode_label,
            role_history_warning_level: coverageFields.role_history_warning_level,
            role_history_warning_title: coverageFields.role_history_warning_title,
            role_history_warning_copy: coverageFields.role_history_warning_copy,
            role_history_basis_copy: coverageFields.role_history_basis_copy,
            history_warning_copy: coverageFields.history_warning_copy,
            evidence_status: coverageFields.evidence_status,
            evidence_status_label: coverageFields.evidence_status_label,
            can_compare_roles: coverageFields.can_compare_roles,
            truth_mismatch_flag: truthMismatchFlag ? 'true' : coverageFields.truth_mismatch_flag,
            truth_mismatch_copy: truthMismatchCopy,
            truth_source: revenueTruth.truth_source,
            story_headline: buildHeadline(periodRow, leadRole),
            story_note: buildNote(periodRow, swingRole, coverageFields, truthMismatchCopy)
        };
    });
}
