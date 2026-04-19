import { PROFILE_LABEL_FALLBACK, ROLE_LABEL_FALLBACK, getRoleLabel } from '../../config/constants.js';
import { isSyntheticRoleHistoryMode } from '../overview/role_history_mode.js';

function getProfileLabel(roleKey) {
    return roleKey && roleKey !== PROFILE_LABEL_FALLBACK
        ? getRoleLabel(roleKey)
        : PROFILE_LABEL_FALLBACK;
}

function getObservedStateLabel(roleKey, observedFlag) {
    return observedFlag === 'true' && roleKey
        ? getRoleLabel(roleKey)
        : ROLE_LABEL_FALLBACK;
}

function normalizeIdentityValue(value) {
    return String(value ?? '').trim().toLowerCase();
}

function isSelfTransition(row) {
    const sourceProductId = String(row.product_id ?? '').trim();
    const targetProductId = String(row.top_transition_target_id ?? '').trim();

    if (sourceProductId && targetProductId) {
        return sourceProductId === targetProductId;
    }

    const sourceProductName = normalizeIdentityValue(row.product_name);
    const targetProductName = normalizeIdentityValue(row.top_transition_target_name);

    return Boolean(sourceProductName && targetProductName && sourceProductName === targetProductName);
}

function getTransitionSummaryText(row) {
    if (!row.top_transition_target_name) {
        return '';
    }

    if (isSelfTransition(row)) {
        return ' 별도 전환 상대 없음으로, 동일 상품 중심 반복 선택이 보입니다.';
    }

    return ` 함께 확인할 전환 상품은 ${row.top_transition_target_name}입니다.`;
}

function getPriorityFollowUpText(row) {
    if (isSelfTransition(row)) {
        return '다음 확인: 매출 흐름과 동일 상품 내 반복 신호를 같이 보세요.';
    }

    return '다음 확인: 매출 흐름과 전환/복귀 신호를 같이 보세요.';
}

function getMissingObservationFollowUpText(row) {
    if (isSelfTransition(row)) {
        return '다음 확인: 관측 누락 여부, 기준일 매출, 동일 상품 내 반복 신호를 순서대로 확인하세요.';
    }

    return '다음 확인: 관측 누락 여부, 기준일 매출, 연결 상품 흐름을 순서대로 확인하세요.';
}

export function buildProductDetailHeader(productTableRows) {
    return productTableRows.map((row) => {
        const syntheticRoleHistory = isSyntheticRoleHistoryMode(row.role_history_mode);

        return {
            product_id: row.product_id,
            headline: `${row.product_name} 운영 요약`,
            summary: syntheticRoleHistory
                ? `상품 기준 프로필은 ${getProfileLabel(row.profile_role_primary)}이며, 기준일 역할 표시는 ${getObservedStateLabel(row.role_state_primary, row.pgm_observed_flag)} 기준 latest-role assumption 참고값입니다. same-date 관측으로 확정하지 마세요.${getTransitionSummaryText(row)}`
                : row.pgm_observed_flag === 'true'
                    ? `상품 기준 프로필은 ${getProfileLabel(row.profile_role_primary)}이며, 기준일 관측 상태는 ${getObservedStateLabel(row.role_state_primary, row.pgm_observed_flag)}입니다.${getTransitionSummaryText(row)}`
                    : `상품 기준 프로필은 ${getProfileLabel(row.profile_role_primary)}이며, 기준일 관측 상태는 ${ROLE_LABEL_FALLBACK}입니다. 기준일 당일 스냅샷만 사용하므로 다른 날짜 상태로 보정하지 않았습니다.${getTransitionSummaryText(row)}`,
            priority_hint: syntheticRoleHistory
                ? `왜: latest-role assumption으로 메운 역할 표시는 실제 기준일 역할과 다를 수 있어 운영 신호를 과해석하면 안 됩니다. 근거: 기준일 매출 ${Number(row.revenue ?? 0).toLocaleString('ko-KR')}원, 직전 7일 누적 ${Number(row.revenue_7d ?? 0).toLocaleString('ko-KR')}원${row.return_loop_rate ? `, 반복 연결 비중 ${(Number(row.return_loop_rate) * 100).toFixed(1)}%` : ''}. ${getPriorityFollowUpText(row)}`
                : row.pgm_observed_flag === 'true'
                    ? `왜: 프로필과 기준일 관측 상태가 같은 방향인지 확인해야 운영 신호를 과해석하지 않습니다. 근거: 기준일 매출 ${Number(row.revenue ?? 0).toLocaleString('ko-KR')}원, 직전 7일 누적 ${Number(row.revenue_7d ?? 0).toLocaleString('ko-KR')}원${row.return_loop_rate ? `, 반복 연결 비중 ${(Number(row.return_loop_rate) * 100).toFixed(1)}%` : ''}. ${getPriorityFollowUpText(row)}`
                    : `왜: 프로필은 있어도 기준일 관측 상태가 비면 최근 확정일 해석을 확정하기 어렵습니다. 근거: 기준일 당일 스냅샷이 없어 ${ROLE_LABEL_FALLBACK}으로 유지했고 최신 역할로 보정하지 않았습니다. ${getMissingObservationFollowUpText(row)}`
        };
    });
}
