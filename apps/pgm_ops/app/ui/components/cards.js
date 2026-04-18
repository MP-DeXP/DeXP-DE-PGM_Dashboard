function getDeltaTone(delta) {
    if (delta == null || delta === '') {
        return 'is-neutral';
    }

    if (Number(delta) > 0) {
        return 'is-positive';
    }

    if (Number(delta) < 0) {
        return 'is-negative';
    }

    return 'is-neutral';
}

function formatPercent(value) {
    if (value == null || value === '') {
        return '데이터 없음';
    }

    const numeric = Number(value);
    if (Number.isNaN(numeric)) {
        return String(value);
    }

    return `${(numeric * 100).toFixed(1)}%`;
}

function formatValue(row) {
    if (typeof row.value === 'string' && Number.isNaN(Number(row.value))) {
        return row.value;
    }

    if (String(row.card_key).includes('share') || String(row.card_key).includes('coverage')) {
        return formatPercent(row.value);
    }

    return Number(row.value).toLocaleString('ko-KR');
}

export function renderCards(rows) {
    if (!rows.length) {
        return '<div class="ops-empty"><strong>표시할 카드가 없습니다.</strong><p>화면 데이터를 아직 불러오지 못했습니다.</p></div>';
    }

    return `
        <div class="ops-card-grid">
            ${rows.map((row) => `
                <article class="ops-card">
                    <span class="ops-card-label">${row.label}</span>
                    <strong class="ops-card-value">${formatValue(row)}</strong>
                    <span class="ops-card-delta ${getDeltaTone(row.delta)}">${formatPercent(row.delta)}</span>
                    <p>${row.reason}</p>
                </article>
            `).join('')}
        </div>
    `;
}

export function renderKpiStack(rows) {
    return `
        <div class="ops-kpi-stack">
            ${rows.map((row) => `
                <div class="ops-kpi">
                    <label>${row.label}</label>
                    <strong>${typeof row.value === 'number' ? row.value.toLocaleString('ko-KR') : row.value}</strong>
                    <span class="ops-inline-delta ${getDeltaTone(row.delta)}">${formatPercent(row.delta)}</span>
                </div>
            `).join('')}
        </div>
    `;
}
