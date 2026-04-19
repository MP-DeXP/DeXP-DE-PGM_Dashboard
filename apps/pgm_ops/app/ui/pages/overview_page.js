import { cleanDisplayText, friendlyRoleLabel, renderThumbnail } from '../components/table.js';
import { isSyntheticRoleHistoryMode } from '../../view_models/overview/role_history_mode.js';

function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (character) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
    })[character]);
}

const PERIOD_ORDER = ['daily', 'weekly', 'monthly'];
const BLANK_OVERVIEW_ROLE_KEY = '__blank__';
const PERIOD_META = {
    daily: {
        label: '어제(최근 확정일)',
        question: '어제 대비 매출이 왜 흔들렸는가',
        intent: '즉시성 있는 매출 변동 원인을 읽는 구간입니다.'
    },
    weekly: {
        label: '최근 7일',
        question: '최근 7일 매출 변화의 주요 역할 신호는 무엇인가',
        intent: '최근 운영 실행 효과를 읽는 구간입니다.'
    },
    monthly: {
        label: '최근 30일',
        question: '최근 30일 매출 엔진 구조가 어떻게 달라졌는가',
        intent: '구조적 추세를 읽는 구간입니다.'
    }
};
const OFFICIAL_ROLES = ['entry', 'expansion', 'return', 'convergence'];

function normalizeOverviewRoleKey(roleStatePrimary) {
    return roleStatePrimary ? roleStatePrimary : BLANK_OVERVIEW_ROLE_KEY;
}

function denormalizeOverviewRoleKey(roleStatePrimary) {
    return roleStatePrimary === BLANK_OVERVIEW_ROLE_KEY ? '' : roleStatePrimary;
}

function formatCurrency(value) {
    if (value == null || value === '') {
        return '데이터 없음';
    }

    const numeric = Number(value);
    if (Number.isNaN(numeric)) {
        return cleanDisplayText(value);
    }

    return `${numeric.toLocaleString('ko-KR')}원`;
}

function formatPlainNumber(value) {
    if (value == null || value === '') {
        return '데이터 없음';
    }

    const numeric = Number(value);
    if (Number.isNaN(numeric)) {
        return cleanDisplayText(value);
    }

    return numeric.toLocaleString('ko-KR');
}

function formatPercent(value) {
    if (value == null || value === '') {
        return '데이터 없음';
    }

    const numeric = Number(value);
    if (Number.isNaN(numeric)) {
        return cleanDisplayText(value);
    }

    return `${(numeric * 100).toFixed(1)}%`;
}

function formatDeltaAmount(value) {
    if (value == null || value === '') {
        return '변화 없음';
    }

    const numeric = Number(value);
    if (Number.isNaN(numeric)) {
        return cleanDisplayText(value);
    }

    const sign = numeric > 0 ? '+' : '';
    return `${sign}${numeric.toLocaleString('ko-KR')}원`;
}

function formatDeltaRate(value) {
    if (value == null || value === '') {
        return '비교 불가';
    }

    const numeric = Number(value);
    if (Number.isNaN(numeric)) {
        return cleanDisplayText(value);
    }

    const sign = numeric > 0 ? '+' : '';
    return `${sign}${(numeric * 100).toFixed(1)}%`;
}

function getDeltaTone(value) {
    const numeric = Number(value ?? 0);

    if (Number.isNaN(numeric) || numeric === 0) {
        return 'is-neutral';
    }

    return numeric > 0 ? 'is-positive' : 'is-negative';
}

function getPriorityLabel(priority) {
    return {
        high: '즉시 확인',
        medium: '우선 확인',
        low: '참고 확인'
    }[String(priority ?? '').toLowerCase()] ?? cleanDisplayText(priority);
}

function buildCoverageBadgeLabel(storyRow) {
    if (!storyRow) {
        return '커버리지 확인 전';
    }

    if (storyRow.coverage_summary) {
        return storyRow.coverage_summary;
    }

    const expectedWindowDays = Number(storyRow.expected_window_days ?? 0);
    const currentCoveredDays = Number(storyRow.current_covered_days ?? 0);
    const previousCoveredDays = Number(storyRow.previous_covered_days ?? 0);

    if (!expectedWindowDays) {
        return '커버리지 확인 전';
    }

    return `역할 관측 현재 ${currentCoveredDays}/${expectedWindowDays}일 · 비교 ${previousCoveredDays}/${expectedWindowDays}일`;
}

function buildCoverageSupportCopy(storyRow) {
    if (storyRow?.role_history_warning_copy) {
        return storyRow.role_history_warning_copy;
    }

    if (storyRow?.history_warning_copy) {
        return storyRow.history_warning_copy;
    }

    return storyRow?.partial_history_flag === 'true'
        ? '역할 해석은 관측된 날짜 범위 안에서만 읽어 주세요.'
        : '역할 비교는 같은 기준으로 읽을 수 있습니다.';
}

function sumRevenue(rows, field) {
    return rows.reduce((total, row) => total + Number(row?.[field] ?? 0), 0);
}

function buildRoleSectionScope(storyRow, roleRows, drilldownRows) {
    const observedCurrentRevenue = sumRevenue(roleRows, 'current_revenue');
    const observedPreviousRevenue = sumRevenue(roleRows, 'previous_revenue');
    const truthCurrentRevenue = Number(storyRow?.current_revenue ?? 0);
    const truthPreviousRevenue = Number(storyRow?.previous_revenue ?? 0);

    return {
        observedCurrentRevenue,
        observedPreviousRevenue,
        truthCurrentRevenue,
        truthPreviousRevenue,
        observedCurrentShareOfTruth: truthCurrentRevenue > 0 ? observedCurrentRevenue / truthCurrentRevenue : null,
        observedPreviousShareOfTruth: truthPreviousRevenue > 0 ? observedPreviousRevenue / truthPreviousRevenue : null,
        observedSkuCount: drilldownRows.length
    };
}

function hasPartialHistory(storyRow) {
    return String(storyRow?.partial_history_flag ?? '').toLowerCase() === 'true';
}

function hasSyntheticHistory(storyRow) {
    return isSyntheticRoleHistoryMode(storyRow?.role_history_mode);
}

function hasRoleHistoryGuard(storyRow) {
    return hasPartialHistory(storyRow) || hasSyntheticHistory(storyRow);
}

function getStoryByPeriod(revenueStories) {
    return new Map(revenueStories.map((row) => [row.period, row]));
}

function getRoleRowsForPeriod(roleDeltaRows, period) {
    return roleDeltaRows.filter((row) => row.period === period);
}

function getDefaultRole(roleRows, selectedOverviewRole) {
    const blankRoleRow = roleRows.find((row) => !row.role_state_primary);
    const officialRows = roleRows.filter((row) => OFFICIAL_ROLES.includes(row.role_state_primary));

    if (selectedOverviewRole === BLANK_OVERVIEW_ROLE_KEY && blankRoleRow) {
        return BLANK_OVERVIEW_ROLE_KEY;
    }

    if (officialRows.some((row) => row.role_state_primary === selectedOverviewRole)) {
        return selectedOverviewRole;
    }

    return [...officialRows].sort((left, right) => {
        const gap = Math.abs(Number(right.revenue_delta ?? 0)) - Math.abs(Number(left.revenue_delta ?? 0));
        if (gap !== 0) {
            return gap;
        }

        return Number(right.current_revenue ?? 0) - Number(left.current_revenue ?? 0);
    })[0]?.role_state_primary ?? officialRows[0]?.role_state_primary ?? '';
}

function getRoleCards(roleRows) {
    const officialCards = OFFICIAL_ROLES.map((roleKey) => {
        const matched = roleRows.find((row) => row.role_state_primary === roleKey);

        return matched ?? {
            period: roleRows[0]?.period ?? 'daily',
            role_state_primary: roleKey,
            role_label: friendlyRoleLabel(roleKey),
            current_revenue: 0,
            previous_revenue: 0,
            revenue_delta: 0,
            revenue_delta_rate: null,
            current_revenue_share: 0,
            previous_revenue_share: 0,
            revenue_share_delta: 0,
            partial_history_flag: roleRows[0]?.partial_history_flag ?? 'true'
        };
    });

    const blankRoleRow = roleRows.find((row) => !row.role_state_primary);

    return blankRoleRow
        ? [...officialCards, blankRoleRow]
        : officialCards;
}

function getBlankRoleRow(roleRows) {
    return roleRows.find((row) => !row.role_state_primary);
}

function getDrilldownRowsForRole(roleDrilldownRows, period, roleStatePrimary) {
    const normalizedRoleStatePrimary = denormalizeOverviewRoleKey(roleStatePrimary);

    return roleDrilldownRows
        .filter((row) => row.period === period && String(row.role_state_primary ?? '') === String(normalizedRoleStatePrimary ?? ''))
        .sort((left, right) => {
            const gap = Math.abs(Number(right.revenue_delta ?? 0)) - Math.abs(Number(left.revenue_delta ?? 0));
            if (gap !== 0) {
                return gap;
            }

            return Number(right.current_revenue ?? 0) - Number(left.current_revenue ?? 0);
        });
}

function getTopProducts(rows, mode) {
    if (mode === 'positive') {
        return rows.filter((row) => Number(row.revenue_delta ?? 0) > 0).sort((left, right) => Number(right.revenue_delta ?? 0) - Number(left.revenue_delta ?? 0)).slice(0, 3);
    }

    if (mode === 'negative') {
        return rows.filter((row) => Number(row.revenue_delta ?? 0) < 0).sort((left, right) => Number(left.revenue_delta ?? 0) - Number(right.revenue_delta ?? 0)).slice(0, 3);
    }

    return [...rows]
        .sort((left, right) => Number(right.current_revenue ?? 0) - Number(left.current_revenue ?? 0))
        .slice(0, 4);
}

function renderMiniProductRow(row, productLookup, tone = '') {
    const product = productLookup.get(row.product_id) ?? row;
    const imageUrl = product.image_url || product.product_image_url || product.list_image || product.detail_image || '';
    const deltaTone = tone || getDeltaTone(row.revenue_delta);

    return `
        <button class="ops-mini-product-row ${deltaTone}" type="button" data-product-id="${escapeHtml(row.product_id)}" data-product-jump="true">
            <div class="ops-mini-product-main">
                ${renderThumbnail({ imageUrl, alt: row.product_name, size: 'xs' })}
                <div class="ops-mini-product-copy">
                    <strong>${escapeHtml(row.product_name)}</strong>
                    <small>${escapeHtml(formatCurrency(row.current_revenue))}</small>
                </div>
            </div>
            <div class="ops-mini-product-meta">
                <span class="ops-mini-product-cta">작업면</span>
                <strong class="ops-inline-delta ${escapeHtml(deltaTone)}">${escapeHtml(formatDeltaAmount(row.revenue_delta))}</strong>
                <small>${escapeHtml(`비중 ${formatPercent(row.current_share_in_role)}`)}</small>
            </div>
        </button>
    `;
}

function buildPriorityAttrs(row) {
    return row.entity_type === 'product'
        ? ` data-product-id="${escapeHtml(row.entity_id)}" data-product-jump="true"`
        : '';
}

function renderPriorityAction(row, { lead = false } = {}) {
    return `
        <button class="ops-priority-drilldown ${lead ? 'ops-overview-priority-lead is-selected' : ''} is-${escapeHtml(row.priority)}" type="button"${buildPriorityAttrs(row)}>
            <div class="ops-priority-drilldown-head">
                <strong>${escapeHtml(cleanDisplayText(row.label))}</strong>
                <div class="ops-priority-drilldown-meta">
                    ${row.entity_type === 'product' ? '<span class="ops-priority-link">SKU 작업면</span>' : ''}
                    <span class="ops-pill badge">${escapeHtml(getPriorityLabel(row.priority))}</span>
                </div>
            </div>
            <p>${escapeHtml(cleanDisplayText(row.reason))}</p>
            <small>${escapeHtml(cleanDisplayText(row.suggested_check))}</small>
        </button>
    `;
}

function renderOperatingStatePanel({
    storiesByPeriod,
    selectedOverviewPeriod,
    selectedStory,
    latestDate,
    statusBadge,
    roleSectionScope,
    swingRole
}) {
    const activeMeta = PERIOD_META[selectedOverviewPeriod] ?? PERIOD_META.daily;
    const coverageLabel = roleSectionScope.observedCurrentShareOfTruth == null
        ? '데이터 없음'
        : formatPercent(roleSectionScope.observedCurrentShareOfTruth);
    const swingLabel = swingRole?.role_label ?? friendlyRoleLabel(selectedStory?.swing_role_state_primary || '');
    const swingDelta = swingRole?.revenue_delta ?? selectedStory?.swing_role_delta_revenue ?? 0;

    return `
        <section class="ops-overview-state-panel">
            <div class="ops-section-head ops-overview-head">
                <div>
                    <h3>현재 상태</h3>
                    <p>${escapeHtml(cleanDisplayText(selectedStory?.period_label ?? activeMeta.label))} 실매출 기준 요약</p>
                </div>
                <div class="ops-product-head-meta">
                    <span class="ops-pill badge">${escapeHtml(selectedStory?.as_of_date ?? latestDate ?? '기준일 없음')}</span>
                    <span class="ops-pill badge">${escapeHtml(statusBadge ?? '상태 미확인')}</span>
                </div>
            </div>
            <div class="pgm-chart-tab-group" role="tablist" aria-label="매출 비교 구간">
                ${PERIOD_ORDER.map((period) => {
                    const story = storiesByPeriod.get(period);
                    const meta = PERIOD_META[period];

                    return `
                        <button class="pgm-chart-tab ${selectedOverviewPeriod === period ? 'is-active' : ''}" type="button" role="tab" aria-selected="${selectedOverviewPeriod === period ? 'true' : 'false'}" data-overview-period="${escapeHtml(period)}">
                            ${escapeHtml(story?.period_label ?? meta.label)}
                        </button>
                    `;
                }).join('')}
            </div>
            <div class="ops-overview-state-strip">
                <article class="ops-overview-state-card is-primary">
                    <span>${escapeHtml(selectedStory?.period_label ?? activeMeta.label)} 매출</span>
                    <strong>${escapeHtml(formatCurrency(selectedStory?.current_revenue ?? 0))}</strong>
                    <small class="${escapeHtml(getDeltaTone(selectedStory?.revenue_delta ?? 0))}">${escapeHtml(`${formatDeltaAmount(selectedStory?.revenue_delta ?? 0)} · ${formatDeltaRate(selectedStory?.revenue_delta_rate)}`)}</small>
                    <p>${escapeHtml(`직전 ${formatCurrency(selectedStory?.previous_revenue ?? 0)}`)}</p>
                </article>
                <article class="ops-overview-state-card">
                    <span>현재 매출 중심 역할</span>
                    <strong>${escapeHtml(friendlyRoleLabel(selectedStory?.lead_role_state_primary || ''))}</strong>
                    <small>${escapeHtml(`비중 ${formatPercent(selectedStory?.lead_role_revenue_share ?? 0)}`)}</small>
                    <p>${escapeHtml(`가장 큰 변동 ${swingLabel} · ${formatDeltaAmount(swingDelta)}`)}</p>
                </article>
                <article class="ops-overview-state-card">
                    <span>${escapeHtml(hasSyntheticHistory(selectedStory) ? '가정 역할 분해 범위' : hasPartialHistory(selectedStory) ? '관측 역할 분해 범위' : '역할 분해 범위')}</span>
                    <strong>${escapeHtml(coverageLabel)}</strong>
                    <small>${escapeHtml(`근거 ${formatCurrency(roleSectionScope.observedCurrentRevenue)} / 전체 ${formatCurrency(roleSectionScope.truthCurrentRevenue)}`)}</small>
                    <p>${escapeHtml(buildCoverageBadgeLabel(selectedStory))}</p>
                </article>
            </div>
            ${hasRoleHistoryGuard(selectedStory) ? `
                <p class="ops-overview-guard">${escapeHtml(buildCoverageSupportCopy(selectedStory))}</p>
            ` : ''}
        </section>
    `;
}

function renderPriorityBlock(priorityRows, selectedStory) {
    if (!priorityRows.length) {
        return `
            <section class="ops-panel ops-section card ops-overview-priority-panel">
                <div class="ops-section-head">
                    <div>
                        <h2>먼저 볼 항목</h2>
                        <p>${escapeHtml(cleanDisplayText(selectedStory?.period_label ?? '선택 구간'))}에 바로 이어서 볼 항목이 없습니다.</p>
                    </div>
                </div>
                <div class="ops-empty empty-state compact">
                    <strong>지금 먼저 볼 점검 항목이 없습니다.</strong>
                </div>
            </section>
        `;
    }

    const [leadRow, ...queueRows] = priorityRows.slice(0, 4);

    return `
        <section class="ops-panel ops-section card ops-overview-priority-panel">
            <div class="ops-section-head">
                <div>
                    <h2>먼저 볼 항목</h2>
                    <p>${escapeHtml(cleanDisplayText(selectedStory?.period_label ?? '선택 구간'))} 기준 우선 점검 순서입니다.</p>
                </div>
                <span class="ops-pill badge">${escapeHtml(`${priorityRows.length}개`)}</span>
            </div>
            ${renderPriorityAction(leadRow, { lead: true })}
            ${queueRows.length ? `
                <div class="ops-overview-priority-queue">
                    ${queueRows.map((row) => renderPriorityAction(row)).join('')}
                </div>
            ` : ''}
            <p class="chart-hint">맨 위 SKU 항목은 바로 SKU 작업면으로 이어지고, 아래 항목은 같은 기준으로 후속 확인 순서를 보여줍니다.</p>
        </section>
    `;
}

function buildRoleReason(selectedRoleRow, storyRow) {
    if (!selectedRoleRow) {
        return '선택한 역할의 매출 변화 설명 데이터가 아직 없습니다.';
    }

    if (hasSyntheticHistory(storyRow)) {
        return `${storyRow?.period_label ?? '이 구간'} 총매출은 전체 합계이고, ${selectedRoleRow.role_label} 역할 변화는 latest-role assumption 기반 참고값입니다.`;
    }

    if (hasPartialHistory(storyRow)) {
        return `${storyRow?.period_label ?? '이 구간'} 총매출은 전체 기간 합계이고, ${selectedRoleRow.role_label} 역할 변화는 가용 역할 관측 기준 참고값입니다.`;
    }

    const direction = Number(selectedRoleRow.revenue_delta ?? 0) > 0 ? '늘어난' : Number(selectedRoleRow.revenue_delta ?? 0) < 0 ? '빠진' : '유지된';

    return `${storyRow?.period_label ?? '이 구간'} 매출을 읽을 때 ${selectedRoleRow.role_label}은 ${direction} 역할 매출 축으로 먼저 확인할 가치가 있습니다.`;
}

function renderRelatedPriorityRows(priorityRows, drilldownRows) {
    const productIds = new Set(drilldownRows.slice(0, 6).map((row) => row.product_id));
    const relatedRows = priorityRows.filter((row) => row.entity_type === 'brand' || productIds.has(row.entity_id)).slice(0, 4);

    if (!relatedRows.length) {
        return '';
    }

    return relatedRows.map((row) => `
        <article class="ops-support-item">
            <strong>${escapeHtml(cleanDisplayText(row.label))}</strong>
            <p>${escapeHtml(cleanDisplayText(row.reason))}</p>
            <small>${escapeHtml(cleanDisplayText(row.suggested_check))}</small>
        </article>
    `).join('');
}

function isRestrictedComparisonPeriod(storyRow) {
    return Boolean(storyRow && storyRow.period !== 'daily' && hasRoleHistoryGuard(storyRow));
}

function hasTruthMismatch(roleSectionScope) {
    const currentGap = Math.abs(Number(roleSectionScope.truthCurrentRevenue ?? 0) - Number(roleSectionScope.observedCurrentRevenue ?? 0));
    const previousGap = Math.abs(Number(roleSectionScope.truthPreviousRevenue ?? 0) - Number(roleSectionScope.observedPreviousRevenue ?? 0));
    return currentGap > 0 || previousGap > 0;
}

function renderRestrictionBlock(storyRow) {
    if (!isRestrictedComparisonPeriod(storyRow)) {
        return '';
    }

    return `
        <article class="ops-overview-alert-block is-restricted">
            <div class="ops-overview-alert-head">
                <strong>${escapeHtml(`${storyRow.period_label ?? '선택 구간'} 역할 비교 제한`)}</strong>
                <span class="ops-pill badge">제한 상태</span>
            </div>
            <p>${escapeHtml(buildCoverageSupportCopy(storyRow))}</p>
            <small>${escapeHtml(buildCoverageBadgeLabel(storyRow))}</small>
        </article>
    `;
}

function renderTruthMismatchBlock(roleSectionScope, storyRow) {
    if (!hasTruthMismatch(roleSectionScope)) {
        return '';
    }

    const currentGap = Number(roleSectionScope.truthCurrentRevenue ?? 0) - Number(roleSectionScope.observedCurrentRevenue ?? 0);
    const previousGap = Number(roleSectionScope.truthPreviousRevenue ?? 0) - Number(roleSectionScope.observedPreviousRevenue ?? 0);

    return `
        <article class="ops-overview-alert-block is-mismatch">
            <div class="ops-overview-alert-head">
                <strong>합계 불일치</strong>
                <span class="ops-pill badge">${escapeHtml(storyRow?.truth_source ?? '기준 원천')}</span>
            </div>
            <p>${escapeHtml(`${storyRow?.period_label ?? '선택 구간'} 총매출과 역할 분해 합계가 다릅니다. 비교표는 역할 관측분 기준 참고치로 읽고, 전체 매출 판단은 실매출 기준값을 유지합니다.`)}</p>
            <div class="ops-overview-alert-metrics">
                <span>${escapeHtml(`현재 갭 ${formatDeltaAmount(currentGap)}`)}</span>
                <span>${escapeHtml(`직전 갭 ${formatDeltaAmount(previousGap)}`)}</span>
            </div>
        </article>
    `;
}

function renderEvidencePanel({
    roleCards,
    selectedOverviewRole,
    storyRow,
    roleSectionScope,
    selectedRoleRow,
    drilldownRows,
    productLookup,
    priorityRows,
    blankRoleRow
}) {
    const positiveRows = getTopProducts(drilldownRows, 'positive');
    const negativeRows = getTopProducts(drilldownRows, 'negative');
    const partialHistory = hasPartialHistory(storyRow);
    const syntheticHistory = hasSyntheticHistory(storyRow);
    const currentRoleMetricLabel = syntheticHistory ? '현재 가정 역할 매출' : partialHistory ? '현재 관측 역할 매출' : '현재 역할 매출';
    const previousRoleMetricLabel = syntheticHistory ? '직전 가정 역할 매출' : partialHistory ? '직전 관측 역할 매출' : '직전 역할 매출';
    const roleDeltaMetricLabel = syntheticHistory ? '가정 매출 변화' : partialHistory ? '관측 매출 변화' : '매출 변화';
    const positiveSkuTitle = syntheticHistory ? '상승 기여 SKU(latest-role assumption)' : partialHistory ? '상승 기여 SKU(관측분)' : '상승 기여 SKU';
    const negativeSkuTitle = syntheticHistory ? '하락 기여 SKU(latest-role assumption)' : partialHistory ? '하락 기여 SKU(관측분)' : '하락 기여 SKU';
    const currentObservedShareLabel = roleSectionScope.observedCurrentShareOfTruth == null
        ? '0.0%'
        : `${(roleSectionScope.observedCurrentShareOfTruth * 100).toFixed(1)}%`;
    const previousObservedShareLabel = roleSectionScope.observedPreviousShareOfTruth == null
        ? '0.0%'
        : `${(roleSectionScope.observedPreviousShareOfTruth * 100).toFixed(1)}%`;
    const currentShareLabel = syntheticHistory ? '현재 가정 비중' : partialHistory ? '현재 관측 비중' : '현재 비중';
    const previousShareLabel = syntheticHistory ? '직전 가정 비중' : partialHistory ? '직전 관측 비중' : '직전 비중';
    const positiveSkuMarkup = positiveRows.length
        ? positiveRows.map((row) => renderMiniProductRow(row, productLookup, 'is-positive')).join('')
        : '<p class="chart-hint">상승 기여 SKU 없음</p>';
    const negativeSkuMarkup = negativeRows.length
        ? negativeRows.map((row) => renderMiniProductRow(row, productLookup, 'is-negative')).join('')
        : '<p class="chart-hint">하락 기여 SKU 없음</p>';
    const relatedPriorityMarkup = renderRelatedPriorityRows(priorityRows, drilldownRows);
    const primarySkuRow = positiveRows[0] ?? getTopProducts(drilldownRows)[0] ?? negativeRows[0] ?? null;

    return `
        <section class="ops-panel ops-section card ops-overview-evidence-panel">
            <div class="ops-section-head">
                <div>
                    <h3>왜 그런가</h3>
                    <p>${escapeHtml(cleanDisplayText(storyRow?.story_headline ?? storyRow?.question_label ?? PERIOD_META[storyRow?.period ?? 'daily'].question))}</p>
                </div>
                <span class="ops-pill badge">${escapeHtml(selectedRoleRow?.role_label ?? '역할 선택')}</span>
            </div>
            <div class="ops-overview-role-toolbar">
                <strong>역할 비교</strong>
                <div class="pgm-seg-group" role="tablist" aria-label="역할 선택">
                    ${roleCards.map((row) => `
                        <button class="pgm-seg-btn ${selectedOverviewRole === normalizeOverviewRoleKey(row.role_state_primary) ? 'is-active' : ''}" type="button" role="tab" aria-selected="${selectedOverviewRole === normalizeOverviewRoleKey(row.role_state_primary) ? 'true' : 'false'}" data-overview-role="${escapeHtml(normalizeOverviewRoleKey(row.role_state_primary))}">
                            ${escapeHtml(row.role_label)}
                        </button>
                    `).join('')}
                </div>
            </div>
            ${renderRestrictionBlock(storyRow)}
            ${renderTruthMismatchBlock(roleSectionScope, storyRow)}
            <div class="table-container ops-overview-compact-table">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>역할</th>
                            <th>${escapeHtml(currentRoleMetricLabel)}</th>
                            <th>${escapeHtml(roleDeltaMetricLabel)}</th>
                            <th>${escapeHtml(currentShareLabel)}</th>
                            <th>${escapeHtml(previousShareLabel)}</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${roleCards.map((row) => `
                            <tr class="${selectedOverviewRole === normalizeOverviewRoleKey(row.role_state_primary) ? 'row-focused' : ''}" data-overview-role="${escapeHtml(normalizeOverviewRoleKey(row.role_state_primary))}">
                                <td>${escapeHtml(row.role_label)}</td>
                                <td>${escapeHtml(formatCurrency(row.current_revenue))}</td>
                                <td class="${escapeHtml(getDeltaTone(row.revenue_delta))}">${escapeHtml(`${formatDeltaAmount(row.revenue_delta)} · ${formatDeltaRate(row.revenue_delta_rate)}`)}</td>
                                <td>${escapeHtml(formatPercent(row.current_revenue_share))}</td>
                                <td>${escapeHtml(formatPercent(row.previous_revenue_share))}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
            <section class="ops-overview-evidence-detail">
                <div class="ops-overview-evidence-detail-head">
                    <div>
                        <span class="ops-eyebrow">선택 역할 상세</span>
                        <h4>${escapeHtml(selectedRoleRow?.role_label ?? '역할 선택')}</h4>
                        <p>${escapeHtml(buildRoleReason(selectedRoleRow, storyRow))}</p>
                    </div>
                    ${primarySkuRow ? `
                        <button class="pgm-chart-tab ops-sku-entry-cta" type="button" data-product-id="${escapeHtml(primarySkuRow.product_id)}" data-product-jump="true">
                            ${escapeHtml(`${cleanDisplayText(primarySkuRow.product_name)} 작업면`)}
                        </button>
                    ` : ''}
                </div>
                <div class="ops-overview-evidence-metrics">
                    <article class="ops-overview-evidence-card">
                        <span>${escapeHtml(storyRow?.period_label ?? '선택 구간')} 총매출</span>
                        <strong>${escapeHtml(formatCurrency(storyRow?.current_revenue ?? 0))}</strong>
                        <small class="${escapeHtml(getDeltaTone(storyRow?.revenue_delta ?? 0))}">${escapeHtml(`${formatDeltaAmount(storyRow?.revenue_delta ?? 0)} · ${formatDeltaRate(storyRow?.revenue_delta_rate)}`)}</small>
                    </article>
                    <article class="ops-overview-evidence-card is-primary">
                        <span>${escapeHtml(currentRoleMetricLabel)}</span>
                        <strong>${escapeHtml(formatCurrency(selectedRoleRow?.current_revenue ?? 0))}</strong>
                        <small>${escapeHtml(`${formatPlainNumber(roleSectionScope.observedSkuCount)}개 SKU 근거`)}</small>
                    </article>
                    <article class="ops-overview-evidence-card">
                        <span>${escapeHtml(previousRoleMetricLabel)}</span>
                        <strong>${escapeHtml(formatCurrency(selectedRoleRow?.previous_revenue ?? 0))}</strong>
                        <small>${escapeHtml(syntheticHistory ? 'latest-role assumption 비교' : partialHistory ? '가용 역할 관측분 비교' : '동일 역할 비교')}</small>
                    </article>
                    <article class="ops-overview-evidence-card">
                        <span>${escapeHtml(roleDeltaMetricLabel)}</span>
                        <strong class="${escapeHtml(getDeltaTone(selectedRoleRow?.revenue_delta ?? 0))}">${escapeHtml(formatDeltaAmount(selectedRoleRow?.revenue_delta ?? 0))}</strong>
                        <small>${escapeHtml(formatDeltaRate(selectedRoleRow?.revenue_delta_rate))}</small>
                    </article>
                </div>
                <div class="ops-overview-evidence-columns">
                    <section class="ops-overview-evidence-sku-block">
                        <div class="ops-overview-evidence-subhead">
                            <h5>SKU 기여</h5>
                            <span>행을 누르면 바로 SKU 작업면으로 이동합니다.</span>
                        </div>
                        <div class="ops-overview-evidence-sku-columns">
                            <div class="ops-overview-evidence-sku-column">
                                <strong>${escapeHtml(positiveSkuTitle)}</strong>
                                ${positiveSkuMarkup}
                            </div>
                            <div class="ops-overview-evidence-sku-column">
                                <strong>${escapeHtml(negativeSkuTitle)}</strong>
                                ${negativeSkuMarkup}
                            </div>
                        </div>
                    </section>
                    <section class="ops-overview-evidence-support-block">
                        <div class="ops-overview-evidence-subhead">
                            <h5>해석 보조</h5>
                            <span>제한 상태와 후속 확인 대상을 함께 둡니다.</span>
                        </div>
                        <div class="ops-support-list">
                            <article class="ops-support-item">
                                <strong>${escapeHtml(storyRow?.period_label ?? '이 구간')} 해석 기준</strong>
                                <p>${escapeHtml(cleanDisplayText(storyRow?.story_note ?? PERIOD_META[storyRow?.period ?? 'daily'].intent))}</p>
                                <small>${escapeHtml(`${buildCoverageBadgeLabel(storyRow)} · 직전 분해 ${previousObservedShareLabel}`)}</small>
                            </article>
                            <article class="ops-support-item">
                                <strong>현재 역할 분해 범위</strong>
                                <p>${escapeHtml(`현재 ${currentObservedShareLabel} · 현재 ${formatCurrency(roleSectionScope.observedCurrentRevenue)} / 전체 ${formatCurrency(roleSectionScope.truthCurrentRevenue)}`)}</p>
                                <small>${escapeHtml(`직전 ${previousObservedShareLabel} · ${formatCurrency(roleSectionScope.observedPreviousRevenue)} / ${formatCurrency(roleSectionScope.truthPreviousRevenue)}`)}</small>
                            </article>
                            ${relatedPriorityMarkup}
                            ${blankRoleRow && (Number(blankRoleRow.current_revenue ?? 0) > 0 || Number(blankRoleRow.previous_revenue ?? 0) > 0) ? `
                                <article class="ops-support-item">
                                    <strong>관측 상태 없음 매출</strong>
                                    <p>${escapeHtml(syntheticHistory
                                        ? `공식 역할 밖 ${formatCurrency(blankRoleRow.current_revenue)}은 latest-role assumption과 별도로 남깁니다.`
                                        : `공식 역할 밖 ${formatCurrency(blankRoleRow.current_revenue)}은 별도 상태로 유지합니다.`)}</p>
                                </article>
                            ` : ''}
                        </div>
                    </section>
                </div>
            </section>
        </section>
    `;
}

export function renderOverviewPage({
    revenueStories = [],
    roleDeltaRows = [],
    roleDrilldownRows = [],
    productRows = [],
    priorityRows = [],
    selectedOverviewPeriod = 'daily',
    selectedOverviewRole = '',
    latestDate,
    statusBadge
}) {
    const storiesByPeriod = getStoryByPeriod(revenueStories);
    const activePeriod = PERIOD_ORDER.includes(selectedOverviewPeriod) ? selectedOverviewPeriod : PERIOD_ORDER[0];
    const selectedStory = storiesByPeriod.get(activePeriod) ?? null;
    const allRoleRows = getRoleRowsForPeriod(roleDeltaRows, activePeriod);
    const roleCards = getRoleCards(allRoleRows);
    const blankRoleRow = getBlankRoleRow(allRoleRows);
    const activeRole = getDefaultRole(allRoleRows, selectedOverviewRole);
    const selectedRoleRow = roleCards.find((row) => normalizeOverviewRoleKey(row.role_state_primary) === activeRole) ?? roleCards[0] ?? null;
    const drilldownRows = getDrilldownRowsForRole(roleDrilldownRows, activePeriod, normalizeOverviewRoleKey(selectedRoleRow?.role_state_primary));
    const productLookup = new Map(productRows.map((row) => [row.product_id, row]));
    const swingRole = [...roleCards]
        .sort((left, right) => Math.abs(Number(right.revenue_delta ?? 0)) - Math.abs(Number(left.revenue_delta ?? 0)))[0] ?? null;
    const roleSectionScope = buildRoleSectionScope(selectedStory, allRoleRows, drilldownRows);

    return `
        <section class="ops-overview-stack">
            ${renderPriorityBlock(priorityRows, selectedStory)}
            ${renderOperatingStatePanel({
                storiesByPeriod,
                selectedOverviewPeriod: activePeriod,
                selectedStory,
                latestDate,
                statusBadge,
                roleSectionScope,
                swingRole
            })}
            ${renderEvidencePanel({
                roleCards,
                selectedOverviewRole: activeRole,
                storyRow: selectedStory,
                roleSectionScope,
                selectedRoleRow,
                drilldownRows,
                productLookup,
                priorityRows,
                blankRoleRow
            })}
        </section>
    `;
}
