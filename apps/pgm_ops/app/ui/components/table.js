function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (character) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
    })[character]);
}

const ROLE_LABEL_MAP = {
    entry: '첫구매 유도',
    expansion: '단골 유도',
    return: '반복 구매',
    convergence: '구매 집중'
};

export function cleanDisplayText(value) {
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
        .replace(/\bentry\b/gi, '첫구매 유도')
        .replace(/\bexpansion\b/gi, '단골 유도')
        .replace(/\breturn\b/gi, '반복 구매')
        .replace(/\bconvergence\b/gi, '구매 집중')
        .replace(/\bproduct\b/gi, '상품')
        .replace(/\bbrand\b/gi, '브랜드')
        .replace(/Deferred:/gi, '')
        .replace(/\s+/g, ' ')
        .trim();
}

export function friendlyRoleLabel(value) {
    const normalized = String(value ?? '').trim().toLowerCase();

    if (!normalized || normalized === 'blank') {
        return '상태 미확인';
    }

    return ROLE_LABEL_MAP[normalized] ?? cleanDisplayText(value);
}

export function friendlyStateLabel(value) {
    const normalized = String(value ?? '').trim().toLowerCase();
    if (!normalized) {
        return '상태 미확인';
    }

    if (normalized === 'pgm 미관측' || normalized === 'blank') {
        return '상태 미확인';
    }

    if (ROLE_LABEL_MAP[normalized]) {
        return ROLE_LABEL_MAP[normalized];
    }

    return cleanDisplayText(value);
}

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

function formatText(value) {
    const text = cleanDisplayText(value);
    return text || '데이터 없음';
}

function buildInitials(text) {
    return String(text ?? '')
        .replace(/\s+/g, '')
        .slice(0, 2)
        .toUpperCase() || '상품';
}

function getImageUrl(row) {
    return row?.image_url
        || row?.product_image_url
        || row?.list_image
        || row?.detail_image
        || row?.target_product_image_url
        || '';
}

function getDetailUrl(row) {
    return row?.detail_url || row?.product_detail_url || '';
}

export function renderThumbnail({ imageUrl, alt, size = 'md', className = '', label = '' }) {
    const initials = buildInitials(alt);
    const classes = ['ops-thumb', `is-${size}`, className].filter(Boolean).join(' ');

    return `
        <div class="${classes}">
            <div class="ops-thumb-fallback">${escapeHtml(initials)}</div>
            ${imageUrl ? `<img class="ops-thumb-image" src="${escapeHtml(imageUrl)}" alt="${escapeHtml(alt)}" loading="lazy" onerror="this.remove();" />` : ''}
            ${label ? `<span class="ops-thumb-label">${escapeHtml(label)}</span>` : ''}
        </div>
    `;
}

export function renderProductGallery(rows, selectedProductId) {
    if (!rows.length) {
        return `
            <div class="ops-empty">
                <strong>표시할 상품이 없습니다.</strong>
                <p>실데이터 연결이 완료되면 선택 리스트가 이 영역에 나타납니다.</p>
            </div>
        `;
    }

    return `
        <div class="ops-product-list">
            ${rows.map((row) => {
                const imageUrl = getImageUrl(row);
                return `
                    <article class="ops-product-list-item ${row.product_id === selectedProductId ? 'is-selected' : ''}" data-product-id="${escapeHtml(row.product_id)}">
                        ${renderThumbnail({ imageUrl, alt: row.product_name, size: 'xs', className: 'ops-product-thumb' })}
                        <div class="ops-product-list-body">
                            <div class="ops-product-list-head">
                                <strong>${escapeHtml(row.product_name)}</strong>
                                <span>${escapeHtml(friendlyStateLabel(row.role_state_primary))}</span>
                            </div>
                            <div class="ops-product-list-meta">
                                <span>${escapeHtml(friendlyRoleLabel(row.profile_role_primary))}</span>
                                <span>${escapeHtml(formatCurrency(row.revenue_30d || row.revenue))}</span>
                            </div>
                            <p class="ops-product-list-summary">${escapeHtml(formatText(`${formatCurrency(row.revenue)} · 오늘 기준`))}</p>
                        </div>
                    </article>
                `;
            }).join('')}
        </div>
    `;
}

export function renderProductDetail(detailRow, productRow) {
    if (!detailRow || !productRow) {
        return `
            <aside class="ops-detail-panel">
                <div class="ops-empty">
                    <strong>상품을 선택하세요.</strong>
                    <p>좌측 리스트에서 상품을 고르면 해석 패널이 이 영역에 연결됩니다.</p>
                </div>
            </aside>
        `;
    }

    const imageUrl = getImageUrl(productRow);
    const detailUrl = getDetailUrl(productRow);
    const observedLabel = productRow.pgm_observed_flag === 'true' ? '실데이터 관측' : '상태 미확인';

    return `
        <aside class="ops-detail-panel">
            <div class="ops-detail-hero">
                ${renderThumbnail({ imageUrl, alt: productRow.product_name, className: 'ops-detail-thumb' })}
                <div class="ops-detail-hero-copy">
                    <span class="ops-detail-eyebrow">선택 상품</span>
                    <strong>${escapeHtml(productRow.product_name)}</strong>
                    <p>${escapeHtml(cleanDisplayText(detailRow.summary))}</p>
                    <div class="ops-product-chips">
                        <span class="ops-pill">${escapeHtml(friendlyRoleLabel(productRow.profile_role_primary))}</span>
                        <span class="ops-pill">${escapeHtml(friendlyStateLabel(productRow.role_state_primary))}</span>
                        <span class="ops-pill">${escapeHtml(observedLabel)}</span>
                    </div>
                </div>
                <div class="ops-detail-hero-metrics">
                    <div class="ops-detail-hero-metric">
                        <label>오늘 매출</label>
                        <strong>${escapeHtml(formatCurrency(productRow.revenue))}</strong>
                    </div>
                    <div class="ops-detail-hero-metric">
                        <label>7일 누적</label>
                        <strong>${escapeHtml(formatCurrency(productRow.revenue_7d))}</strong>
                    </div>
                    <div class="ops-detail-hero-metric">
                        <label>브랜드 내 비중</label>
                        <strong>${escapeHtml(formatPercent(productRow.revenue_share_in_brand_day))}</strong>
                    </div>
                    ${detailUrl ? `<a class="ops-product-link" href="${escapeHtml(detailUrl)}" target="_blank" rel="noreferrer">상품 상세 열기</a>` : ''}
                </div>
            </div>
            <div class="ops-note">${escapeHtml(cleanDisplayText(detailRow.priority_hint))}</div>
        </aside>
    `;
}

export function renderProductTable(rows, selectedProductId) {
    return renderProductGallery(rows, selectedProductId);
}
