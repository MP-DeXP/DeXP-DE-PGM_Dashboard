function formatPercent(value) {
    return `${(Number(value ?? 0) * 100).toFixed(1)}%`;
}

export function renderRoleStructure(rows) {
    if (!rows.length) {
        return '<div class="ops-empty"><strong>Role structure 데이터가 없습니다.</strong></div>';
    }

    return `
        <div class="ops-structure-list">
            ${rows.map((row) => `
                <div class="ops-structure-item">
                    <div class="ops-structure-item-head">
                        <div>
                            <strong>${row.role_state_primary}</strong>
                            <p>${row.product_count}개 상품</p>
                        </div>
                        <span>${formatPercent(row.revenue_share)}</span>
                    </div>
                    <div class="ops-bar-track">
                        <div class="ops-bar-fill" style="width:${Math.max(6, Number(row.revenue_share ?? 0) * 100)}%"></div>
                    </div>
                </div>
            `).join('')}
        </div>
    `;
}

export function renderRevenueStructure(rows) {
    if (!rows.length) {
        return '<div class="ops-empty"><strong>Revenue structure 데이터가 없습니다.</strong></div>';
    }

    return `
        <div class="ops-structure-list">
            ${rows.map((row) => `
                <div class="ops-structure-item ops-revenue-bar">
                    <div class="ops-structure-item-head">
                        <div>
                            <strong>${row.product_name}</strong>
                            <p>${row.role_state_primary}</p>
                        </div>
                        <span>${formatPercent(row.revenue_share_in_brand_day)}</span>
                    </div>
                    <div class="ops-bar-track">
                        <div class="ops-bar-fill" style="width:${Math.max(6, Number(row.revenue_share_in_brand_day ?? 0) * 100)}%"></div>
                    </div>
                </div>
            `).join('')}
        </div>
    `;
}
