import { renderRevenueStructure, renderRoleStructure } from '../components/charts.js';
import { renderProductDetail, renderProductTable } from '../components/table.js';

export function renderProductsPage({ productRows, detailRows, selectedProductId, roleStructureRows, revenueStructureRows, searchQuery }) {
    const selectedProduct = productRows.find((row) => row.product_id === selectedProductId) ?? productRows[0];
    const selectedDetail = detailRows.find((row) => row.product_id === selectedProduct?.product_id);

    return `
        <section class="ops-panel ops-section">
            <div class="ops-section-head">
                <div>
                    <h3>Role Structure × Revenue Structure</h3>
                    <p>상품별 profile, state, revenue를 같은 화면에서 읽되, same-date role-state blank는 latest 값으로 메우지 않고 UI에서만 `PGM 미관측`으로 표기합니다.</p>
                </div>
            </div>
            <div class="ops-structure-grid">
                <div>
                    ${renderRoleStructure(roleStructureRows)}
                </div>
                <div>
                    ${renderRevenueStructure(revenueStructureRows)}
                </div>
            </div>
        </section>

        <section class="ops-panel ops-section">
            <div class="ops-section-head">
                <div>
                    <h3>상품 운영 테이블</h3>
                    <p>검색 결과 ${productRows.length}개 상품 · blank state는 same-date snapshot 미존재를 뜻합니다.</p>
                </div>
            </div>
            <div class="ops-table-toolbar">
                <input class="ops-search" type="search" id="ops-product-search" placeholder="상품명 또는 product_id 검색" value="${searchQuery}">
            </div>
            <div class="ops-table-layout">
                ${renderProductTable(productRows, selectedProduct?.product_id)}
                ${renderProductDetail(selectedDetail, selectedProduct)}
            </div>
        </section>
    `;
}
