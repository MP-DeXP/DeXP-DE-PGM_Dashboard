import { cleanDisplayText, friendlyRoleLabel } from '../components/table.js';

function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (character) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
    })[character]);
}

const PERIOD_CONFIG = {
    daily: {
        title: 'Daily',
        subtitle: '오늘 기준',
        revenueField: 'revenue'
    },
    weekly: {
        title: 'Weekly',
        subtitle: '최근 7일',
        revenueField: 'revenue_7d'
    },
    monthly: {
        title: 'Monthly',
        subtitle: '최근 30일',
        revenueField: 'revenue_30d'
    }
};

const ROLE_ORDER = ['entry', 'expansion', 'return', 'convergence', 'blank'];

function formatCurrency(value) {
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

function formatDelta(value) {
    if (value == null || value === '') {
        return null;
    }

    const numeric = Number(value);
    if (Number.isNaN(numeric)) {
        return cleanDisplayText(value);
    }

    const sign = numeric > 0 ? '+' : '';
    return `${sign}${(numeric * 100).toFixed(1)}%`;
}

function getDeltaTone(value) {
    const numeric = Number(value);
    if (Number.isNaN(numeric) || numeric === 0) {
        return 'is-neutral';
    }

    return numeric > 0 ? 'is-positive' : 'is-negative';
}

function normalizeRoleKey(value) {
    const normalized = String(value ?? '').trim().toLowerCase();
    if (!normalized) {
        return 'blank';
    }

    if (normalized === 'pgm 미관측' || normalized === '상태 미확인' || normalized === '데이터 없음' || normalized === 'blank') {
        return 'blank';
    }

    return normalized;
}

function sortRoleRows(rows) {
    return [...rows].sort((left, right) => {
        const rankGap = Number(left.role_rank ?? 99) - Number(right.role_rank ?? 99);
        if (rankGap !== 0) {
            return rankGap;
        }

        return ROLE_ORDER.indexOf(normalizeRoleKey(left.role_state_primary)) - ROLE_ORDER.indexOf(normalizeRoleKey(right.role_state_primary));
    });
}

function deriveContributionRows(period, productRows) {
    const revenueField = PERIOD_CONFIG[period]?.revenueField ?? 'revenue';
    const grouped = new Map();

    productRows.forEach((row) => {
        const revenue = Number(row?.[revenueField] ?? 0);
        if (!revenue) {
            return;
        }

        const roleKey = normalizeRoleKey(row.role_state_primary);
        const existing = grouped.get(roleKey) ?? { role_state_primary: roleKey, role_revenue: 0 };
        existing.role_revenue += revenue;
        grouped.set(roleKey, existing);
    });

    const rows = [...grouped.values()];
    const totalRevenue = rows.reduce((sum, row) => sum + row.role_revenue, 0);

    return rows
        .map((row) => ({
            ...row,
            role_revenue_share: totalRevenue ? row.role_revenue / totalRevenue : 0
        }))
        .sort((left, right) => right.role_revenue - left.role_revenue)
        .map((row, index) => ({
            period,
            role_state_primary: row.role_state_primary,
            role_label: friendlyRoleLabel(row.role_state_primary),
            role_revenue: row.role_revenue,
            role_revenue_share: row.role_revenue_share,
            role_rank: index + 1
        }));
}

function getContributionRowsForPeriod(period, roleContributionRows, productRows) {
    const explicitRows = sortRoleRows(
        roleContributionRows
            .filter((row) => row.period === period)
            .map((row) => ({
                ...row,
                role_state_primary: row.role_state_primary ?? row.role_key ?? row.role_label
            }))
    );

    if (explicitRows.length) {
        return explicitRows.map((row) => ({
            ...row,
            role_label: row.role_label || friendlyRoleLabel(row.role_state_primary)
        }));
    }

    return deriveContributionRows(period, productRows);
}

function getRevenueCard(period, cards) {
    const preferredKey = {
        daily: 'brand_revenue',
        weekly: 'revenue_7d',
        monthly: 'revenue_30d'
    }[period];

    return cards.find((row) => row.card_key === preferredKey) ?? cards[0] ?? null;
}

function renderContributionList(rows) {
    if (!rows.length) {
        return `
            <div class="ops-empty compact">
                <strong>역할 기여 데이터가 아직 없습니다.</strong>
            </div>
        `;
    }

    return `
        <div class="ops-role-ranking-list">
            ${rows.map((row) => `
                <article class="ops-role-ranking-item ${Number(row.role_rank) === 1 ? 'is-top' : ''}">
                    <div class="ops-role-ranking-copy">
                        <span>${escapeHtml(row.role_label)}</span>
                        <strong>${escapeHtml(formatPercent(row.role_revenue_share))}</strong>
                    </div>
                    <small>${escapeHtml(`${formatCurrency(row.role_revenue)} · ${Number(row.role_rank)}위`)}</small>
                    <div class="ops-bar-track">
                        <div class="ops-bar-fill" style="width:${Math.max(8, Number(row.role_revenue_share ?? 0) * 100)}%"></div>
                    </div>
                </article>
            `).join('')}
        </div>
    `;
}

function renderPeriodColumn(period, cards, roleContributionRows, productRows, latestDate) {
    const config = PERIOD_CONFIG[period];
    const revenueCard = getRevenueCard(period, cards);
    const summaryCard = cards[cards.length - 1] ?? revenueCard;
    const contributions = getContributionRowsForPeriod(period, roleContributionRows, productRows);
    const topRole = contributions[0];

    return `
        <article class="ops-period-card">
            <div class="ops-period-head">
                <div>
                    <span class="ops-period-eyebrow">${escapeHtml(config.title)}</span>
                    <h3>${escapeHtml(config.subtitle)}</h3>
                </div>
                <span class="ops-pill">${escapeHtml(latestDate ?? '기준일 없음')}</span>
            </div>
            <div class="ops-period-metric">
                <span>${escapeHtml(revenueCard?.label ?? '기간 매출')}</span>
                <strong>${escapeHtml(formatCurrency(revenueCard?.value))}</strong>
                ${revenueCard?.delta ? `<small class="${escapeHtml(getDeltaTone(revenueCard.delta))}">${escapeHtml(formatDelta(revenueCard.delta) ?? '')}</small>` : ''}
            </div>
            ${topRole ? `
                <div class="ops-period-role-hero">
                    <span class="ops-period-kicker">가장 크게 끌고 있는 역할</span>
                    <div class="ops-period-role-hero-main">
                        <strong>${escapeHtml(topRole.role_label)}</strong>
                        <span>${escapeHtml(formatPercent(topRole.role_revenue_share))}</span>
                    </div>
                    <p>${escapeHtml(`${formatCurrency(topRole.role_revenue)} 매출 기여`)}</p>
                </div>
            ` : ''}
            <div class="ops-period-role-block">
                <div class="ops-period-role-head">
                    <strong>역할별 매출 기여</strong>
                    <span>전체 순위</span>
                </div>
                ${renderContributionList(contributions)}
            </div>
            <p class="ops-period-summary">${escapeHtml(cleanDisplayText(summaryCard?.reason ?? '어떤 역할이 매출을 끌고 있는지 먼저 보고, 그 다음 브랜드 전체 구조에서 상품 구성을 확인하세요.'))}</p>
        </article>
    `;
}

export function renderOverviewPage({
    dailyCards = [],
    weeklyCards = [],
    monthlyCards = [],
    roleContributionRows = [],
    productRows = [],
    latestDate
}) {
    return `
        <section class="ops-panel ops-section ops-overview-panel">
            <div class="ops-section-head ops-overview-head">
                <div>
                    <h2>기간별 역할 기여 비교판</h2>
                    <p>각 기간에 어떤 역할이 매출을 끌고 있는지 먼저 확인하고, 아래에서 브랜드 전체 구조를 이어서 봅니다.</p>
                </div>
                <span class="ops-pill">역할 기여 중심</span>
            </div>
            <div class="ops-overview-grid">
                ${renderPeriodColumn('daily', dailyCards, roleContributionRows, productRows, latestDate)}
                ${renderPeriodColumn('weekly', weeklyCards, roleContributionRows, productRows, latestDate)}
                ${renderPeriodColumn('monthly', monthlyCards, roleContributionRows, productRows, latestDate)}
            </div>
        </section>
    `;
}
