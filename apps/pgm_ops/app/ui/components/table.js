function formatCurrency(value) {
    return Number(value ?? 0).toLocaleString('ko-KR');
}

function formatPercent(value) {
    if (value == null || value === '') {
        return 'blank';
    }
    return `${(Number(value) * 100).toFixed(1)}%`;
}

function formatStateSource(row) {
    return row.role_state_source === 'same_date_snapshot'
        ? 'same-date snapshot'
        : 'same-date blank';
}

export function renderProductTable(rows, selectedProductId) {
    return `
        <div class="ops-table-wrap">
            <table class="ops-table">
                <thead>
                    <tr>
                        <th>상품</th>
                        <th>Profile</th>
                        <th>State</th>
                        <th>일매출</th>
                        <th>매출 비중</th>
                        <th>7d</th>
                        <th>30d</th>
                    </tr>
                </thead>
                <tbody>
                    ${rows.map((row) => `
                        <tr data-product-id="${row.product_id}" class="${row.product_id === selectedProductId ? 'is-selected' : ''}">
                            <td class="ops-table-product">
                                <strong>${row.product_name}</strong>
                                <small>${row.product_id}</small>
                            </td>
                            <td>${row.profile_role_primary}</td>
                            <td>${row.role_state_primary}<small class="ops-table-subtle">${formatStateSource(row)}</small></td>
                            <td>${formatCurrency(row.revenue)}</td>
                            <td>${formatPercent(row.revenue_share_in_brand_day)}</td>
                            <td>${formatCurrency(row.revenue_7d)}</td>
                            <td>${formatCurrency(row.revenue_30d)}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>
    `;
}

export function renderProductDetail(detailRow, productRow) {
    if (!detailRow || !productRow) {
        return `
            <aside class="ops-detail-panel">
                <div class="ops-empty">
                    <strong>상품을 선택하세요.</strong>
                    <p>테이블에서 상품을 선택하면 profile, state, revenue를 함께 볼 수 있습니다.</p>
                </div>
            </aside>
        `;
    }

    return `
        <aside class="ops-detail-panel">
            <div class="ops-detail-header">
                <div>
                    <strong>${detailRow.headline}</strong>
                    <p class="ops-detail-summary">${detailRow.summary}</p>
                </div>
                <span class="ops-pill">${productRow.role_state_source === 'same_date_snapshot' ? 'same-date observed' : 'same-date blank'}</span>
            </div>
            <div class="ops-detail-grid">
                <div class="ops-detail-metric">
                    <label>현재 상태</label>
                    <strong>${productRow.role_state_primary}</strong>
                </div>
                <div class="ops-detail-metric">
                    <label>Profile</label>
                    <strong>${productRow.profile_role_primary}</strong>
                </div>
                <div class="ops-detail-metric">
                    <label>오늘 매출</label>
                    <strong>${formatCurrency(productRow.revenue)}</strong>
                </div>
                <div class="ops-detail-metric">
                    <label>브랜드 내 비중</label>
                    <strong>${formatPercent(productRow.revenue_share_in_brand_day)}</strong>
                </div>
                <div class="ops-detail-metric">
                    <label>상태 근거</label>
                    <strong>${formatStateSource(productRow)}</strong>
                </div>
            </div>
            <div class="ops-note">${detailRow.priority_hint}</div>
        </aside>
    `;
}
