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
        <section class="ops-compact-panel">
            <div class="ops-section-head">
                <div>
                    <h3>${escapeHtml(title)}</h3>
                    ${subtitle ? `<p class="chart-hint">${escapeHtml(cleanDisplayText(subtitle))}</p>` : ''}
                </div>
            </div>
            ${rows.length ? `
                <div class="ops-compact-list">
                    ${rows.map((row) => `
                        <article class="ops-compact-item">
                            ${renderRow(row)}
                        </article>
                    `).join('')}
                </div>
            ` : `
                <p class="chart-hint">${escapeHtml(emptyMessage)}</p>
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

function renderWorkspaceContext(selectedProduct) {
    const focusCopy = selectedProduct?.product_name
        ? `운영 요약에서 넘겨받은 SKU: ${cleanDisplayText(selectedProduct.product_name)}`
        : '운영 요약에서 넘겨받은 SKU를 여기서 이어 확인합니다.';

    return `
        <header class="ops-workspace-context">
            <span class="ops-workspace-kicker">SKU 작업면</span>
            <p class="ops-workspace-context-line">${escapeHtml(focusCopy)}</p>
        </header>
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

    const selectedProduct = productRows.find((row) => row.product_id === selectedProductId) ?? productRows[0];
    const selectedDetail = detailRows.find((row) => row.product_id === selectedProduct?.product_id);
    const selectedPriority = priorityRows.find((row) => row.entity_type === 'product' && row.entity_id === selectedProduct?.product_id) ?? null;

    const selectedTransitionRows = transitionSummaryRows
        .filter((row) => row.product_id === selectedProduct?.product_id)
        .slice(0, 3);
    const selectedLoopRows = returnLoopSummaryRows
        .filter((row) => row.product_id === selectedProduct?.product_id)
        .slice(0, 1);
    const transitionPanel = renderCompactPanel(
        '전환 참고',
        '다음 선택 흐름만 짧게 확인합니다.',
        selectedTransitionRows,
        '다음 선택 신호 없음',
        renderTransitionRow
    );
    const returnPanel = renderCompactPanel(
        '복귀 참고',
        '반복 구매 신호만 보조로 붙입니다.',
        selectedLoopRows,
        '재방문 신호 없음',
        renderReturnRow
    );
    const workspaceLead = selectedPriority
        ? cleanDisplayText(selectedPriority.reason || selectedPriority.label)
        : '운영 요약에서 넘긴 SKU를 여기서 바로 분석합니다.';

    return `
        <section class="ops-products-stack" id="ops-products-section">
            <div class="ops-workspace-shell">
                <div class="ops-analysis-workspace">
                    ${renderWorkspaceContext(selectedProduct)}
                    <div class="ops-workspace-summary card">
                        <strong>분석 시작점</strong>
                        <p>${escapeHtml(workspaceLead)}</p>
                    </div>

                    <div class="ops-workspace-body">
                        ${renderProductDetail(selectedDetail, selectedProduct)}

                        <div class="ops-support-stack">
                            ${transitionPanel}
                            ${returnPanel}
                        </div>
                    </div>
                </div>

                <aside class="ops-selection-panel">
                    <div class="pgm-product-table-top">
                        <div>
                            <h3>SKU 선택</h3>
                            <p class="chart-hint">작업면에서 이어 볼 SKU를 빠르게 전환합니다.</p>
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
            </div>
        </section>
    `;
}
