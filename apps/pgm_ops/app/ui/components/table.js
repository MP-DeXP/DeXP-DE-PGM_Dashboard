function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (character) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
    })[character]);
}

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
        .replace(/Deferred:/gi, '')
        .replace(/\s+/g, ' ')
        .trim();
}

export function friendlyStateLabel(value) {
    const text = cleanDisplayText(value);
    if (!text) {
        return '상태 미확인';
    }

    if (text === 'PGM 미관측') {
        return '상태 미확인';
    }

    if (text.toLowerCase() === 'blank') {
        return '데이터 없음';
    }

    return text;
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
                <p>실데이터 연결이 완료되면 상품 카드가 이 영역에 나타납니다.</p>
            </div>
        `;
    }

    return `
        <div class="ops-product-grid">
            ${rows.map((row) => {
                const imageUrl = getImageUrl(row);
                const detailUrl = getDetailUrl(row);
                return `
                    <article class="ops-product-card ${row.product_id === selectedProductId ? 'is-selected' : ''}" data-product-id="${escapeHtml(row.product_id)}">
                        <div class="ops-product-media">
                            ${renderThumbnail({ imageUrl, alt: row.product_name, className: 'ops-product-thumb' })}
                        </div>
                        <div class="ops-product-body">
                            <div class="ops-product-title-row">
                                <div>
                                    <strong>${escapeHtml(row.product_name)}</strong>
                                    <span>${escapeHtml(row.product_id)}</span>
                                </div>
                                ${detailUrl ? `<a class="ops-product-link" href="${escapeHtml(detailUrl)}" target="_blank" rel="noreferrer">상세</a>` : ''}
                            </div>
                            <div class="ops-product-chips">
                                <span class="ops-pill">${escapeHtml(formatText(row.profile_role_primary))}</span>
                                <span class="ops-pill">${escapeHtml(friendlyStateLabel(row.role_state_primary))}</span>
                            </div>
                            <div class="ops-product-metrics">
                                <div>
                                    <span>오늘</span>
                                    <strong>${escapeHtml(formatCurrency(row.revenue))}</strong>
                                </div>
                                <div>
                                    <span>7일</span>
                                    <strong>${escapeHtml(formatCurrency(row.revenue_7d))}</strong>
                                </div>
                                <div>
                                    <span>30일</span>
                                    <strong>${escapeHtml(formatCurrency(row.revenue_30d))}</strong>
                                </div>
                                <div>
                                    <span>90일</span>
                                    <strong>${escapeHtml(formatCurrency(row.revenue_90d))}</strong>
                                </div>
                            </div>
                            <p class="ops-product-summary">${escapeHtml(formatText(row.top_transition_target_name ? `${row.top_transition_target_name}로 이어지는 흐름이 보입니다.` : '이동 신호를 함께 보며 우선순위를 판단합니다.'))}</p>
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
                    <p>카드를 클릭하면 상세 정보가 이 영역에 연결됩니다.</p>
                </div>
            </aside>
        `;
    }

    const imageUrl = getImageUrl(productRow);
    const detailUrl = getDetailUrl(productRow);

    return `
        <aside class="ops-detail-panel">
            <div class="ops-detail-hero">
                ${renderThumbnail({ imageUrl, alt: productRow.product_name, className: 'ops-detail-thumb' })}
                <div class="ops-detail-hero-copy">
                    <strong>${escapeHtml(productRow.product_name)}</strong>
                    <p>${escapeHtml(cleanDisplayText(detailRow.summary))}</p>
                    <div class="ops-product-chips">
                        <span class="ops-pill">${escapeHtml(formatText(productRow.profile_role_primary))}</span>
                        <span class="ops-pill">${escapeHtml(friendlyStateLabel(productRow.role_state_primary))}</span>
                        <span class="ops-pill">${escapeHtml(productRow.pgm_observed_flag === 'true' ? '관측됨' : '상태 미확인')}</span>
                    </div>
                    ${detailUrl ? `<a class="ops-product-link" href="${escapeHtml(detailUrl)}" target="_blank" rel="noreferrer">상품 상세 열기</a>` : ''}
                </div>
            </div>
            <div class="ops-detail-grid">
                <div class="ops-detail-metric">
                    <label>오늘 매출</label>
                    <strong>${escapeHtml(formatCurrency(productRow.revenue))}</strong>
                </div>
                <div class="ops-detail-metric">
                    <label>7일 누적</label>
                    <strong>${escapeHtml(formatCurrency(productRow.revenue_7d))}</strong>
                </div>
                <div class="ops-detail-metric">
                    <label>30일 누적</label>
                    <strong>${escapeHtml(formatCurrency(productRow.revenue_30d))}</strong>
                </div>
                <div class="ops-detail-metric">
                    <label>90일 누적</label>
                    <strong>${escapeHtml(formatCurrency(productRow.revenue_90d))}</strong>
                </div>
                <div class="ops-detail-metric">
                    <label>매출 비중</label>
                    <strong>${escapeHtml(formatPercent(productRow.revenue_share_in_brand_day))}</strong>
                </div>
                <div class="ops-detail-metric">
                    <label>상위 이동</label>
                    <strong>${escapeHtml(formatText(productRow.top_transition_target_name))}</strong>
                </div>
                <div class="ops-detail-metric">
                    <label>전환율</label>
                    <strong>${escapeHtml(formatPercent(productRow.top_transition_rate))}</strong>
                </div>
                <div class="ops-detail-metric">
                    <label>복귀율</label>
                    <strong>${escapeHtml(formatPercent(productRow.qualified_return_rate))}</strong>
                </div>
                <div class="ops-detail-metric">
                    <label>루프율</label>
                    <strong>${escapeHtml(formatPercent(productRow.return_loop_rate))}</strong>
                </div>
            </div>
            <div class="ops-note">${escapeHtml(cleanDisplayText(detailRow.priority_hint))}</div>
        </aside>
    `;
}

export function renderProductTable(rows, selectedProductId) {
    return renderProductGallery(rows, selectedProductId);
}
