import { renderRevenueStructure, renderRoleStructure } from '../components/charts.js';
import { cleanDisplayText, friendlyStateLabel, renderProductDetail, renderProductGallery, renderThumbnail } from '../components/table.js';

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

function renderCompactPanel(title, subtitle, rows, emptyMessage, renderRow) {
    return `
        <section class="ops-panel ops-section ops-compact-panel">
            <div class="ops-section-head">
                <div>
                    <h3>${escapeHtml(title)}</h3>
                    ${subtitle ? `<p>${escapeHtml(cleanDisplayText(subtitle))}</p>` : ''}
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
                <div class="ops-empty compact">
                    <strong>${escapeHtml(emptyMessage)}</strong>
                </div>
            `}
        </section>
    `;
}

function renderTransitionRow(row) {
    const imageUrl = row.product_image_url || row.list_image || row.detail_image || '';
    const targetImageUrl = row.target_product_image_url || row.target_list_image || row.target_detail_image || '';

    return `
        <div class="ops-transition-item">
            <div class="ops-transition-visual">
                ${renderThumbnail({ imageUrl, alt: row.product_name, size: 'sm', label: '출발' })}
                <span class="ops-transition-arrow">→</span>
                ${renderThumbnail({ imageUrl: targetImageUrl, alt: row.target_product_name, size: 'sm', label: '도착' })}
            </div>
            <div class="ops-transition-copy">
                <strong>${escapeHtml(row.product_name)} → ${escapeHtml(row.target_product_name)}</strong>
                <p>${escapeHtml(`전환율 ${formatPercent(row.transition_rate)} · 평균 ${formatCurrency(row.avg_days_to_transition)}일`)}</p>
                <small>${escapeHtml(`유입 ${formatCurrency(row.transition_customer_cnt)}건 · 기준 ${formatCurrency(row.source_cohort_customer_cnt)}건`)}</small>
            </div>
        </div>
    `;
}

function renderReturnRow(row) {
    const imageUrl = row.product_image_url || row.list_image || row.detail_image || '';

    return `
        <div class="ops-transition-item">
            ${renderThumbnail({ imageUrl, alt: row.product_name, size: 'sm', label: '상품' })}
            <div class="ops-transition-copy">
                <strong>${escapeHtml(row.product_name)}</strong>
                <p>${escapeHtml(`복귀율 ${formatPercent(row.qualified_return_rate)} · 루프율 ${formatPercent(row.return_loop_rate)} · 반복율 ${formatPercent(row.simple_repeat_rate)}`)}</p>
                <small>${escapeHtml(`복귀 ${formatCurrency(row.return_case_count)}건 · 평균 ${formatCurrency(row.avg_return_days)}일`)}</small>
            </div>
        </div>
    `;
}

function renderInflowRow(row) {
    return `
        <div class="ops-compact-row">
            <strong>${escapeHtml(row.label)}</strong>
            <p>${escapeHtml(cleanDisplayText(row.detail))}</p>
            <small>${escapeHtml(String(row.value ?? ''))}</small>
        </div>
    `;
}

export function renderProductsPage({
    productRows,
    detailRows,
    selectedProductId,
    roleStructureRows,
    revenueStructureRows,
    searchQuery,
    transitionSummaryRows = [],
    returnLoopSummaryRows = [],
    revenueInflowRows = []
}) {
    const selectedProduct = productRows.find((row) => row.product_id === selectedProductId) ?? productRows[0];
    const selectedDetail = detailRows.find((row) => row.product_id === selectedProduct?.product_id);
    const friendlyRoleStructureRows = roleStructureRows.map((row) => ({
        ...row,
        role_state_primary: friendlyStateLabel(row.role_state_primary)
    }));
    const friendlyRevenueStructureRows = revenueStructureRows.map((row) => ({
        ...row,
        role_state_primary: friendlyStateLabel(row.role_state_primary)
    }));
    const selectedTransitionRows = transitionSummaryRows
        .filter((row) => row.product_id === selectedProduct?.product_id)
        .slice(0, 3);
    const selectedLoopRow = returnLoopSummaryRows.find((row) => row.product_id === selectedProduct?.product_id);
    const selectedInflowRows = revenueInflowRows.slice(0, 4);

    return `
        <section class="ops-panel ops-section ops-product-gallery-panel">
            <div class="ops-section-head ops-product-head">
                <div>
                    <h3>상품 갤러리</h3>
                    <p>상품 사진과 핵심 지표를 함께 보면서 어떤 상품이 먼저 움직이는지 빠르게 확인합니다.</p>
                </div>
                <div class="ops-product-head-meta">
                    <span class="ops-pill">${escapeHtml(`${productRows.length}개`)}</span>
                </div>
            </div>
            <div class="ops-table-toolbar ops-product-toolbar">
                <input class="ops-search" type="search" id="ops-product-search" placeholder="상품명 또는 상품 번호로 찾기" value="${escapeHtml(searchQuery)}">
                <span class="ops-meta-text">선택한 카드가 아래 상세 패널로 이어집니다.</span>
            </div>
            ${renderProductGallery(productRows, selectedProduct?.product_id)}
        </section>

        <section class="ops-product-focus-grid">
            <div class="ops-product-focus-main">
                ${renderProductDetail(selectedDetail, selectedProduct)}
            </div>
            <div class="ops-product-focus-side">
                ${renderCompactPanel(
                    '전환이 많은 이동',
                    '선택한 상품이 어디로 자주 이어지는지 봅니다.',
                    selectedTransitionRows,
                    '선택한 상품의 이동 신호가 아직 없습니다.',
                    renderTransitionRow
                )}
                ${renderCompactPanel(
                    '복귀 흐름',
                    '다시 돌아오는 흐름과 반복 패턴을 확인합니다.',
                    selectedLoopRow ? [selectedLoopRow] : [],
                    '선택한 상품의 복귀 흐름이 아직 없습니다.',
                    renderReturnRow
                )}
                ${renderCompactPanel(
                    '유입 맥락',
                    '상품을 보는 보조 맥락입니다.',
                    selectedInflowRows,
                    '유입 맥락 데이터가 없습니다.',
                    renderInflowRow
                )}
            </div>
        </section>

        <section class="ops-panel ops-section">
            <div class="ops-section-head">
                <div>
                    <h3>구조 보기</h3>
                    <p>상품별 역할 묶음과 매출 집중도를 함께 보면 운영 우선순위를 더 쉽게 좁힐 수 있습니다.</p>
                </div>
            </div>
            <div class="ops-structure-grid">
                <div class="ops-structure-card">
                    ${renderRoleStructure(friendlyRoleStructureRows)}
                </div>
                <div class="ops-structure-card">
                    ${renderRevenueStructure(friendlyRevenueStructureRows)}
                </div>
            </div>
        </section>
    `;
}
