function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (character) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
    })[character]);
}

function friendlyText(value) {
    const text = String(value ?? '').trim();
    if (!text) {
        return '데이터 없음';
    }

    return text
        .replace(/same-date snapshot/gi, '당일 기준')
        .replace(/same-date role snapshot/gi, '당일 기준')
        .replace(/same-date role state/gi, '당일 상태')
        .replace(/same-date blank/gi, '데이터 없음')
        .replace(/role-state/gi, '상태')
        .replace(/runtime mode/gi, '상태')
        .replace(/artifact-backed/gi, '실데이터 연결')
        .replace(/sample fallback/gi, '예시 데이터')
        .replace(/latest role fallback/gi, '보정')
        .replace(/\bblank\b/gi, '데이터 없음')
        .replace(/PGM 미관측/gi, '상태 미확인')
        .replace(/Deferred:/gi, '')
        .replace(/\s+/g, ' ')
        .trim();
}

function formatCardValue(card, cardKey = '') {
    if (card == null || card === '') {
        return '데이터 없음';
    }

    const text = String(card).trim();
    const normalizedKey = String(cardKey ?? '');
    const shouldShowPercent = /(share|coverage|rate|ratio|delta)/i.test(normalizedKey);

    if (!Number.isNaN(Number(text)) && text !== '') {
        if (shouldShowPercent) {
            return `${(Number(text) * 100).toFixed(1)}%`;
        }

        return Number(text).toLocaleString('ko-KR');
    }

    return friendlyText(text);
}

function formatCardDelta(delta) {
    if (delta == null || delta === '') {
        return null;
    }

    const numeric = Number(delta);
    if (Number.isNaN(numeric)) {
        return friendlyText(delta);
    }

    const sign = numeric > 0 ? '+' : '';
    return `${sign}${(numeric * 100).toFixed(1)}%`;
}

function getDeltaTone(delta) {
    if (delta == null || delta === '') {
        return 'is-neutral';
    }

    const numeric = Number(delta);
    if (Number.isNaN(numeric)) {
        return 'is-neutral';
    }

    if (numeric > 0) {
        return 'is-positive';
    }

    if (numeric < 0) {
        return 'is-negative';
    }

    return 'is-neutral';
}

function renderPeriodColumn(title, cards, subtitle, latestDate) {
    const hero = cards[0];
    const supporting = cards.slice(1, 4);

    return `
        <article class="ops-period-card">
            <div class="ops-period-head">
                <div>
                    <span class="ops-period-eyebrow">${escapeHtml(title)}</span>
                    <h3>${escapeHtml(subtitle)}</h3>
                </div>
                <span class="ops-pill">${escapeHtml(latestDate ?? '기준일 없음')}</span>
            </div>
            ${hero ? `
                <div class="ops-period-hero">
                    <span class="ops-period-kicker">${escapeHtml(hero.label)}</span>
                    <strong>${escapeHtml(formatCardValue(hero.value, hero.card_key))}</strong>
                    ${hero.delta != null && hero.delta !== '' ? `<span class="ops-period-delta ${getDeltaTone(hero.delta)}">${escapeHtml(formatCardDelta(hero.delta) ?? '')}</span>` : ''}
                    <p>${escapeHtml(friendlyText(hero.reason))}</p>
                </div>
            ` : `
                <div class="ops-empty compact">
                    <strong>표시할 데이터가 없습니다.</strong>
                </div>
            `}
            <div class="ops-period-support">
                ${supporting.map((card) => `
                    <article class="ops-period-mini">
                        <span>${escapeHtml(card.label)}</span>
                        <strong>${escapeHtml(formatCardValue(card.value, card.card_key))}</strong>
                        <p>${escapeHtml(friendlyText(card.reason))}</p>
                    </article>
                `).join('')}
            </div>
            ${supporting.length ? `<p class="ops-period-summary">${escapeHtml(friendlyText(supporting[supporting.length - 1]?.reason ?? hero?.reason ?? ''))}</p>` : ''}
        </article>
    `;
}

export function renderOverviewPage({ dailyCards = [], weeklyCards = [], monthlyCards = [], latestDate }) {
    return `
        <section class="ops-panel ops-section ops-overview-panel">
            <div class="ops-section-head ops-overview-head">
                <div>
                    <h2>Daily / Weekly / Monthly 비교판</h2>
                    <p>세 기간을 같은 화면에 두고 매출 흐름과 구조 신호를 함께 읽을 수 있게 정리했습니다.</p>
                </div>
                <span class="ops-pill">실시간 비교</span>
            </div>
            <div class="ops-overview-grid">
                ${renderPeriodColumn('Daily', dailyCards, '오늘 흐름', latestDate)}
                ${renderPeriodColumn('Weekly', weeklyCards, '7일 흐름', latestDate)}
                ${renderPeriodColumn('Monthly', monthlyCards, '30일 흐름', latestDate)}
            </div>
        </section>
    `;
}
