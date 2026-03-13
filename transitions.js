// Transitions page logic

function renderTransitions() {
    destroyCarts();
    const container = document.getElementById('content-area');
    container.innerHTML = `
        ${renderSearchUI('transitions', '제품명 또는 ID 검색', { includeModeSelect: true })}
        <p class="chart-hint">${RETENTION_90D_FLOW_LABEL}을 기준으로 보여줘요.</p>
        <div id="transitions-table-container"></div>
    `;
    applyFriendlyUi(container);
    renderTransitionsTable();
}

function renderTransitionsTable() {
    const tableContainer = document.getElementById('transitions-table-container');
    if (!tableContainer) return;
    const transitions = AppState.data.anchorTransition || [];
    const { sortCol, sortDesc, searchQuery, searchMode } = AppState.viewState.transitions;
    const focusEntityId = String(AppState.helpers.focusEntityId || '').trim();
    const getName = (id) => getProductName(id);

    let filteredData = [...transitions];
    if (searchQuery) {
        filteredData = transitions.filter((row) => {
            const fromTokens = buildEntitySearchTokens(row.aa_product_id, getName);
            const toTokens = buildEntitySearchTokens(row.pca_product_id, getName);
            return matchesSearchQuery(
                searchQuery,
                searchMode,
                [...fromTokens.ids, ...toTokens.ids],
                [...fromTokens.names, ...toTokens.names]
            );
        });
    }

    const sortedData = filteredData.sort((a, b) => {
        let valA = a[sortCol];
        let valB = b[sortCol];
        if (sortCol === 'aa_product_id' || sortCol === 'pca_product_id') {
            valA = getName(valA);
            valB = getName(valB);
        }
        if (valA === undefined || valA === null) valA = 0;
        if (valB === undefined || valB === null) valB = 0;
        if (valA < valB) return sortDesc ? 1 : -1;
        if (valA > valB) return sortDesc ? -1 : 1;
        return 0;
    });

    const displayData = sortedData.slice(0, 200);
    const getSortIndicator = (col) => sortCol === col ? (sortDesc ? ' ▼' : ' ▲') : '';
    const sortLabelMap = {
        aa_product_id: '첫구매 유입 제품',
        pca_product_id: '재구매 제품',
        transition_customer_cnt: '90일 재구매 고객수',
        avg_days_to_pca: '평균 재구매 소요일',
        transition_rate: '90일 재구매율'
    };
    const sortLabel = sortLabelMap[sortCol] || sortCol;

    const rows = displayData.map((row) => `
        <tr class="${focusEntityId && (String(row.aa_product_id) === focusEntityId || String(row.pca_product_id) === focusEntityId) ? 'row-focused' : ''}">
            <td>${renderProductCell(getName(row.aa_product_id), row.aa_product_id, 44)}</td>
            <td>${renderProductCell(getName(row.pca_product_id), row.pca_product_id, 44)}</td>
            <td>${formatNumber(row.transition_customer_cnt)}</td>
            <td>${formatNumber(row.avg_days_to_pca, 1)}</td>
            <td>${formatPercent(row.transition_rate, 2)}</td>
        </tr>
    `).join('');
    const emptyMessage = searchQuery
        ? `검색 결과가 없습니다. (검색어: ${escapeHtml(searchQuery)})`
        : '표시할 90일 리텐션 데이터가 없어요.';
    const bodyRows = rows || `<tr><td colspan="5" style="text-align:center;color:var(--text-muted); padding:1rem;">${emptyMessage}</td></tr>`;

    tableContainer.innerHTML = `
        <div class="card animate-fade-in"><h3>상위 200개 90일 리텐션 흐름 (정렬 기준: ${escapeHtml(sortLabel)})</h3>
            <div class="table-container">
                <table class="data-table">
                    <thead><tr>
                        <th onclick="handleTransitionSort('aa_product_id')">첫구매 유입 제품${getSortIndicator('aa_product_id')}</th>
                        <th onclick="handleTransitionSort('pca_product_id')">재구매 제품${getSortIndicator('pca_product_id')}</th>
                        <th onclick="handleTransitionSort('transition_customer_cnt')">90일 재구매 고객수${getSortIndicator('transition_customer_cnt')}</th>
                        <th onclick="handleTransitionSort('avg_days_to_pca')">평균 재구매 소요일${getSortIndicator('avg_days_to_pca')}</th>
                        <th onclick="handleTransitionSort('transition_rate')">90일 재구매율${getSortIndicator('transition_rate')}</th>
                    </tr></thead>
                    <tbody>${bodyRows}</tbody>
                </table>
            </div>
        </div>
    `;
    applyFriendlyUi(tableContainer);

    window.handleTransitionSort = (col) => {
        if (AppState.viewState.transitions.sortCol === col) AppState.viewState.transitions.sortDesc = !AppState.viewState.transitions.sortDesc;
        else {
            AppState.viewState.transitions.sortCol = col;
            AppState.viewState.transitions.sortDesc = true;
        }
        renderTransitionsTable();
    };
}
