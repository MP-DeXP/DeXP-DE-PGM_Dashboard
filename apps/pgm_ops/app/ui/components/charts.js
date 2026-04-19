function formatPercent(value) {
    return `${(Number(value ?? 0) * 100).toFixed(1)}%`;
}

export function renderRoleStructure(rows) {
    if (!rows.length) {
        return '<div class="ops-empty empty-state"><strong>역할 구조 데이터가 없습니다.</strong></div>';
    }

    return `
        <div class="ops-structure-list">
            <div class="table-container">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>역할</th>
                            <th>상품 수</th>
                            <th>매출 비중</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${rows.map((row) => `
                            <tr>
                                <td>${row.role_state_primary}</td>
                                <td>${row.product_count}개 상품</td>
                                <td>${formatPercent(row.revenue_share)}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
            <p class="chart-hint">역할별 매출 비중은 products 표 문법에 맞춰 동일한 표 구조로 보여줍니다.</p>
        </div>
    `;
}

export function renderRevenueStructure(rows) {
    if (!rows.length) {
        return '<div class="ops-empty empty-state"><strong>매출 구조 데이터가 없습니다.</strong></div>';
    }

    return `
        <div class="ops-structure-list">
            <div class="table-container">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>상품</th>
                            <th>역할</th>
                            <th>브랜드 내 비중</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${rows.map((row) => `
                            <tr>
                                <td>${row.product_name}</td>
                                <td>${row.role_state_primary}</td>
                                <td>${formatPercent(row.revenue_share_in_brand_day)}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
            <p class="chart-hint">상위 매출 구조도 products 표 카드와 같은 table-container/data-table 문법을 따릅니다.</p>
        </div>
    `;
}
