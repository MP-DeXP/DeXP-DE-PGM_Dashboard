import { cleanDisplayText, renderProductDetail, renderProductGallery, renderThumbnail } from '../components/table.js';

function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (character) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
    })[character]);
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

function normalizeProductIdentity(value) {
    return String(value ?? '').trim().toLowerCase();
}

function isSelfTransition(row) {
    const sourceProductId = String(row?.product_id ?? '').trim();
    const targetProductId = String(row?.target_product_id ?? row?.top_transition_target_id ?? '').trim();

    if (sourceProductId && targetProductId) {
        return sourceProductId === targetProductId;
    }

    const sourceProductName = normalizeProductIdentity(row?.product_name);
    const targetProductName = normalizeProductIdentity(row?.target_product_name ?? row?.top_transition_target_name);

    return Boolean(sourceProductName && targetProductName && sourceProductName === targetProductName);
}

function renderCompactPanel(title, subtitle, rows, emptyMessage, renderRow) {
    return `
        <section class="ops-panel ops-section ops-compact-panel card">
            <div class="ops-section-head">
                <div>
                    <h3>${escapeHtml(title)}</h3>
                    ${subtitle ? `<p class="chart-hint">${escapeHtml(cleanDisplayText(subtitle))}</p>` : ''}
                </div>
            </div>
            ${rows.length ? `
                <div class="table-container">
                    <div class="ops-compact-list">
                        ${rows.map((row) => `
                            <article class="ops-compact-item">
                                ${renderRow(row)}
                            </article>
                        `).join('')}
                    </div>
                </div>
            ` : `
                <div class="ops-empty empty-state compact">
                    <strong>${escapeHtml(emptyMessage)}</strong>
                </div>
            `}
        </section>
    `;
}

function renderTransitionRow(row) {
    const imageUrl = row.product_image_url || row.list_image || row.detail_image || '';
    const targetImageUrl = row.target_product_image_url || row.target_list_image || row.target_detail_image || '';
    const selfTransition = isSelfTransition(row);

    return `
        <div class="ops-transition-item">
            <div class="ops-transition-visual">
                ${renderThumbnail({ imageUrl, alt: row.product_name, size: 'xs', label: '출발' })}
                <span class="ops-transition-arrow">→</span>
                ${renderThumbnail({ imageUrl: targetImageUrl, alt: row.target_product_name, size: 'xs', label: '도착' })}
            </div>
            <div class="ops-transition-copy">
                <strong>${escapeHtml(selfTransition ? `${row.product_name} 동일 상품 중심 반복 선택` : `${row.product_name} 다음 선택 ${row.target_product_name}`)}</strong>
                <p>${escapeHtml(`${selfTransition ? '동일 상품 내 반복 신호' : '다음 선택 비중'} ${formatPercent(row.transition_rate)} · 평균 ${formatCurrency(row.avg_days_to_transition)}일`)}</p>
                <small>${escapeHtml(`관측 ${formatCurrency(row.transition_customer_cnt)}건`)}</small>
            </div>
        </div>
    `;
}

function renderReturnRow(row) {
    const imageUrl = row.product_image_url || row.list_image || row.detail_image || '';

    return `
        <div class="ops-transition-item">
            ${renderThumbnail({ imageUrl, alt: row.product_name, size: 'xs', label: '상품' })}
            <div class="ops-transition-copy">
                <strong>${escapeHtml(row.product_name)}</strong>
                <p>${escapeHtml(`재방문율 ${formatPercent(row.qualified_return_rate)} · 반복 연결 비중 ${formatPercent(row.return_loop_rate)}`)}</p>
                <small>${escapeHtml(`반복 구매율 ${formatPercent(row.simple_repeat_rate)} · 평균 ${formatCurrency(row.avg_return_days)}일`)}</small>
            </div>
        </div>
    `;
}

export function renderProductsPage({
    productRows,
    detailRows,
    selectedProductId,
    searchQuery,
    transitionSummaryRows = [],
    returnLoopSummaryRows = [],
    revenueInflowRows = [],
    priorityRows = []
}) {
    void revenueInflowRows;
    void priorityRows;

    const selectedProduct = productRows.find((row) => row.product_id === selectedProductId) ?? productRows[0];
    const selectedDetail = detailRows.find((row) => row.product_id === selectedProduct?.product_id);

    const selectedTransitionRows = transitionSummaryRows
        .filter((row) => row.product_id === selectedProduct?.product_id)
        .slice(0, 3);
    const selectedLoopRows = returnLoopSummaryRows
        .filter((row) => row.product_id === selectedProduct?.product_id)
        .slice(0, 1);

    return `
        <section class="ops-products-stack" id="ops-products-section">
            <section class="ops-panel ops-section ops-secondary-workspace-panel card">
                <div class="ops-section-head">
                    <div>
                        <h3>SKU 탐색 데스크</h3>
                        <p class="chart-hint">overview에서 고른 SKU를 이어 보고, 전환·복귀 신호만 붙여 빠르게 읽는 작업면입니다.</p>
                    </div>
                    <span class="ops-pill badge">보조 탐색</span>
                </div>
                <div class="ops-workspace-shell ops-secondary-workspace">
                    <aside class="ops-selection-panel pgm-product-table-card card">
                        <div class="pgm-product-table-top">
                            <div>
                                <h3>SKU 선택</h3>
                                <p class="chart-hint">overview에서 이어서 확인할 SKU를 고르거나, 복귀할 상품을 다시 찾는 탐색기입니다.</p>
                            </div>
                            <div class="ops-product-head-meta">
                                <span class="ops-pill badge">${escapeHtml(`${productRows.length}개`)}</span>
                            </div>
                        </div>
                        <div class="pgm-product-table-filter-row">
                            <div class="search-container ops-product-search-shell">
                                <div class="search-combo">
                                    <div class="search-wrapper pgm-product-table-search-wrap">
                                        <i class="ph ph-magnifying-glass"></i>
                                        <input class="search-input pgm-product-table-search-input" type="search" id="ops-product-search" placeholder="상품명 또는 상품 번호로 찾기" value="${escapeHtml(searchQuery)}">
                                    </div>
                                </div>
                            </div>
                        </div>
                        ${renderProductGallery(productRows, selectedProduct?.product_id)}
                    </aside>

                    <div class="ops-analysis-workspace">
                        ${renderProductDetail(selectedDetail, selectedProduct)}

                        <div class="ops-support-stack">
                            ${renderCompactPanel(
                                '전환 보조 진단',
                                '선택 SKU 다음 흐름만 빠르게 확인합니다. 역할 서사를 확정하는 카드가 아닙니다.',
                                selectedTransitionRows,
                                '선택한 상품의 다음 선택 신호가 아직 없습니다.',
                                renderTransitionRow
                            )}
                            ${renderCompactPanel(
                                '복귀 보조 진단',
                                '반복 구매와 재방문 신호를 보조로 붙여 봅니다.',
                                selectedLoopRows,
                                '선택한 상품의 재방문 신호가 아직 없습니다.',
                                renderReturnRow
                            )}
                        </div>
                    </div>
                </div>
            </section>
        </section>
    `;
}
