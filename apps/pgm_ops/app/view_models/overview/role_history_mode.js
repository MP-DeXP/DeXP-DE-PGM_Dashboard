export const ROLE_HISTORY_MODE_SAME_DATE = 'same_date_observed';
export const ROLE_HISTORY_MODE_PARTIAL = 'partial_same_date';
export const ROLE_HISTORY_MODE_LATEST_ROLE = 'latest_role_assumption';

function normalizeSignal(value) {
    return String(value ?? '').trim().toLowerCase();
}

function buildCoverageSummary(currentCoveredDays, previousCoveredDays, expectedWindowDays) {
    if (!expectedWindowDays) {
        return '역할 이력 커버리지는 아직 계산되지 않았습니다.';
    }

    return `기간 총매출은 ${expectedWindowDays}일 전체 기준으로 집계했고, 역할 비교에 실제로 쓸 수 있는 관측일은 현재 ${currentCoveredDays}/${expectedWindowDays}일, 비교 구간 ${previousCoveredDays}/${expectedWindowDays}일입니다.`;
}

export function normalizeRoleHistoryMode(value) {
    const normalized = normalizeSignal(value);

    if (!normalized) {
        return '';
    }

    if (normalized === ROLE_HISTORY_MODE_SAME_DATE || normalized === ROLE_HISTORY_MODE_PARTIAL || normalized === ROLE_HISTORY_MODE_LATEST_ROLE) {
        return normalized;
    }

    if (
        normalized.includes('latest-role assumption')
        || normalized.includes('latest_role_assumption')
        || normalized.includes('latest role assumption')
        || normalized.includes('latest_snapshot')
        || normalized.includes('synthetic')
        || normalized.includes('assumption')
        || normalized.includes('window_metric')
        || normalized.includes('role_fallback')
        || normalized.includes('role fallback')
    ) {
        return ROLE_HISTORY_MODE_LATEST_ROLE;
    }

    if (normalized.includes('partial')) {
        return ROLE_HISTORY_MODE_PARTIAL;
    }

    if (normalized.includes('same_date') || /\bobserved\b/.test(normalized)) {
        return ROLE_HISTORY_MODE_SAME_DATE;
    }

    return '';
}

export function isSyntheticRoleHistoryMode(value) {
    return normalizeRoleHistoryMode(value) === ROLE_HISTORY_MODE_LATEST_ROLE;
}

export function inferRoleHistoryMode({
    roleHistorySignals = [],
    currentCoveredDays = 0,
    previousCoveredDays = 0,
    expectedWindowDays = 0
} = {}) {
    const normalizedSignals = roleHistorySignals
        .map((value) => normalizeRoleHistoryMode(value))
        .filter(Boolean);

    if (normalizedSignals.includes(ROLE_HISTORY_MODE_LATEST_ROLE)) {
        return ROLE_HISTORY_MODE_LATEST_ROLE;
    }

    if (expectedWindowDays && (currentCoveredDays < expectedWindowDays || previousCoveredDays < expectedWindowDays)) {
        return ROLE_HISTORY_MODE_PARTIAL;
    }

    if (normalizedSignals.includes(ROLE_HISTORY_MODE_SAME_DATE) || currentCoveredDays || previousCoveredDays) {
        return ROLE_HISTORY_MODE_SAME_DATE;
    }

    return ROLE_HISTORY_MODE_PARTIAL;
}

export function buildRoleHistoryMeta({
    roleHistoryMode,
    currentCoveredDays = 0,
    previousCoveredDays = 0,
    expectedWindowDays = 0,
    periodLabel = ''
} = {}) {
    const normalizedMode = normalizeRoleHistoryMode(roleHistoryMode)
        || inferRoleHistoryMode({ currentCoveredDays, previousCoveredDays, expectedWindowDays });
    const coverageSummary = buildCoverageSummary(currentCoveredDays, previousCoveredDays, expectedWindowDays);
    const windowLabel = periodLabel || '이 구간';

    if (normalizedMode === ROLE_HISTORY_MODE_LATEST_ROLE) {
        return {
            role_history_mode: ROLE_HISTORY_MODE_LATEST_ROLE,
            role_history_mode_label: 'latest-role assumption',
            role_history_warning_level: expectedWindowDays > 1 ? 'strong' : 'caution',
            role_history_warning_title: `${windowLabel} 역할 분해는 latest-role assumption 기준입니다.`,
            role_history_warning_copy: `${coverageSummary} 역할 관측이 없는 날짜는 최신 확인 역할로 가정해 채웠습니다. 그래서 역할 증감과 SKU 기여는 참고용 추정치로만 읽어 주세요.`,
            role_history_basis_copy: `${windowLabel} 총매출은 기간 전체 실매출이지만, 역할 변화는 latest-role assumption 기반 참고값이라 실제 역할 이동으로 단정하면 안 됩니다.`,
            coverage_summary: coverageSummary
        };
    }

    if (normalizedMode === ROLE_HISTORY_MODE_PARTIAL) {
        return {
            role_history_mode: ROLE_HISTORY_MODE_PARTIAL,
            role_history_mode_label: 'partial same-date coverage',
            role_history_warning_level: expectedWindowDays > 1 ? 'strong' : 'caution',
            role_history_warning_title: `${windowLabel} 총매출은 전체 기간 기준이지만, 역할 비교는 관측된 일부 날짜만 반영됩니다.`,
            role_history_warning_copy: `${coverageSummary} 총매출 해석은 그대로 보셔도 되지만, 역할 변화 해석은 실제 관측된 날짜만 반영된 제한적 참고값입니다.`,
            role_history_basis_copy: `${windowLabel} 총매출과 역할 변화 해석은 같은 범위를 뜻하지 않으며, 역할 표는 관측 확보분만 반영한 참고값입니다.`,
            coverage_summary: coverageSummary
        };
    }

    return {
        role_history_mode: ROLE_HISTORY_MODE_SAME_DATE,
        role_history_mode_label: 'same-date observed',
        role_history_warning_level: 'none',
        role_history_warning_title: `${windowLabel} 역할 분해는 same-date 관측 기준입니다.`,
        role_history_warning_copy: `${coverageSummary} 실매출 비교와 역할 분해를 같은 기간 창으로 함께 읽어도 됩니다.`,
        role_history_basis_copy: `${windowLabel} 역할 변화는 same-date 관측 기준으로 비교했습니다.`,
        coverage_summary: coverageSummary
    };
}
