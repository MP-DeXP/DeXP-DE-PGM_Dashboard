import { cleanDisplayText, friendlyRoleLabel, friendlyStateLabel, renderProductDetail, renderProductGallery, renderThumbnail } from '../components/table.js';

function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (character) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
    })[character]);
}

const ROLE_ORDER = ['entry', 'expansion', 'return', 'convergence', 'blank'];
const WINDOW_CONFIG = [
    { days: 7, label: '7일' },
    { days: 30, label: '30일' },
    { days: 90, label: '90일' }
];

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

function normalizeRoleKey(value) {
    const normalized = String(value ?? '').trim().toLowerCase();
    if (!normalized || normalized === 'blank' || normalized === '상태 미확인' || normalized === '데이터 없음') {
        return 'blank';
    }

    return normalized;
}

function getRoleSortIndex(roleKey) {
    const index = ROLE_ORDER.indexOf(normalizeRoleKey(roleKey));
    return index === -1 ? ROLE_ORDER.length : index;
}

function createProductLookup(rows) {
    return new Map(rows.map((row) => [row.product_id, row]));
}

function deriveCurrentRoleRows(productRows) {
    const grouped = new Map();
    const brandTotalRevenue = productRows.reduce((sum, row) => sum + Number(row.revenue ?? 0), 0);

    productRows.forEach((row) => {
        const roleKey = normalizeRoleKey(row.role_state_primary);
        const revenue = Number(row.revenue ?? 0);
        if (!revenue) {
            return;
        }

        const bucket = grouped.get(roleKey) ?? [];
        bucket.push({
            role_state_primary: roleKey,
            product_id: row.product_id,
            product_name: row.product_name,
            revenue,
            revenue_share_in_brand: brandTotalRevenue ? revenue / brandTotalRevenue : 0
        });
        grouped.set(roleKey, bucket);
    });

    return [...grouped.entries()].flatMap(([, rows]) => {
        const roleTotal = rows.reduce((sum, row) => sum + row.revenue, 0);
        return rows
            .sort((left, right) => right.revenue - left.revenue)
            .map((row, index) => ({
                ...row,
                revenue_share_in_role: roleTotal ? row.revenue / roleTotal : 0,
                role_rank: index + 1
            }));
    });
}

function deriveWindowRows(productRows) {
    return WINDOW_CONFIG.flatMap(({ days }) => {
        const field = `revenue_${days}d`;
        const grouped = new Map();

        productRows.forEach((row) => {
            const roleKey = normalizeRoleKey(row.role_state_primary);
            const revenue = Number(row[field] ?? 0);
            if (!revenue) {
                return;
            }

            const bucket = grouped.get(roleKey) ?? [];
            bucket.push({
                as_of_date: row.as_of_date,
                window_days: String(days),
                role_state_primary: roleKey,
                product_id: row.product_id,
                product_name: row.product_name,
                window_revenue: revenue
            });
            grouped.set(roleKey, bucket);
        });

        return [...grouped.entries()].flatMap(([, rows]) => {
            const roleTotal = rows.reduce((sum, row) => sum + row.window_revenue, 0);
            return rows
                .sort((left, right) => right.window_revenue - left.window_revenue)
                .map((row, index) => ({
                    ...row,
                    share_in_role: roleTotal ? row.window_revenue / roleTotal : 0,
                    role_rank: index + 1
                }));
        });
    });
}

function buildRoleSections({
    productRows,
    currentRows,
    windowRows,
    roleStructureRows
}) {
    const productLookup = createProductLookup(productRows);
    const roleSummaryMap = new Map(
        roleStructureRows.map((row) => [normalizeRoleKey(row.role_state_primary), row])
    );

    const currentRoleMap = new Map();
    currentRows.forEach((row) => {
        const roleKey = normalizeRoleKey(row.role_state_primary);
        const bucket = currentRoleMap.get(roleKey) ?? [];
        bucket.push(row);
        currentRoleMap.set(roleKey, bucket);
    });

    const windowRoleMap = new Map();
    windowRows.forEach((row) => {
        const roleKey = normalizeRoleKey(row.role_state_primary);
        const windowKey = `${roleKey}:${row.window_days}`;
        const bucket = windowRoleMap.get(windowKey) ?? [];
        bucket.push(row);
        windowRoleMap.set(windowKey, bucket);
    });

    const roleKeys = [...new Set([
        ...currentRoleMap.keys(),
        ...windowRows.map((row) => normalizeRoleKey(row.role_state_primary)),
        ...roleSummaryMap.keys()
    ])].sort((left, right) => getRoleSortIndex(left) - getRoleSortIndex(right));

    return roleKeys.map((roleKey) => {
        const currentMembers = [...(currentRoleMap.get(roleKey) ?? [])]
            .sort((left, right) => Number(right.revenue ?? 0) - Number(left.revenue ?? 0))
            .slice(0, 6)
            .map((row) => ({
                ...row,
                product_name: row.product_name ?? productLookup.get(row.product_id)?.product_name,
                image_url: productLookup.get(row.product_id)?.image_url,
                detail_url: productLookup.get(row.product_id)?.detail_url
            }));

        const summary = roleSummaryMap.get(roleKey);
        const roleRevenue = Number(summary?.revenue ?? currentMembers.reduce((sum, row) => sum + Number(row.revenue ?? 0), 0));
        const roleShare = Number(summary?.revenue_share ?? currentMembers[0]?.revenue_share_in_brand ?? 0);
        const roleProductCount = Number(summary?.product_count ?? currentMembers.length);

        const windows = WINDOW_CONFIG.map(({ days, label }) => {
            const rows = [...(windowRoleMap.get(`${roleKey}:${days}`) ?? [])]
                .sort((left, right) => Number(right.window_revenue ?? 0) - Number(left.window_revenue ?? 0))
                .slice(0, 4)
                .map((row) => ({
                    ...row,
                    product_name: row.product_name ?? productLookup.get(row.product_id)?.product_name,
                    image_url: productLookup.get(row.product_id)?.image_url,
                    detail_url: productLookup.get(row.product_id)?.detail_url
                }));

            return {
                days,
                label,
                rows
            };
        });

        return {
            roleKey,
            roleLabel: friendlyRoleLabel(roleKey),
            roleRevenue,
            roleShare,
            roleProductCount,
            currentMembers,
            windows
        };
    });
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
                ${renderThumbnail({ imageUrl, alt: row.product_name, size: 'xs', label: '출발' })}
                <span class="ops-transition-arrow">→</span>
                ${renderThumbnail({ imageUrl: targetImageUrl, alt: row.target_product_name, size: 'xs', label: '도착' })}
            </div>
            <div class="ops-transition-copy">
                <strong>${escapeHtml(row.product_name)} → ${escapeHtml(row.target_product_name)}</strong>
                <p>${escapeHtml(`전환율 ${formatPercent(row.transition_rate)} · 평균 ${formatCurrency(row.avg_days_to_transition)}일`)}</p>
                <small>${escapeHtml(`유입 ${formatCurrency(row.transition_customer_cnt)}건`)}</small>
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
                <p>${escapeHtml(`복귀율 ${formatPercent(row.qualified_return_rate)} · 루프율 ${formatPercent(row.return_loop_rate)}`)}</p>
                <small>${escapeHtml(`반복율 ${formatPercent(row.simple_repeat_rate)} · 평균 ${formatCurrency(row.avg_return_days)}일`)}</small>
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

function renderPriorityRow(row) {
    const priorityMap = {
        high: '높음',
        medium: '중간',
        low: '낮음'
    };

    return `
        <div class="ops-priority-compact-item is-${escapeHtml(row.priority)}">
            <div class="ops-priority-compact-head">
                <strong>${escapeHtml(cleanDisplayText(row.label))}</strong>
                <span class="ops-pill">${escapeHtml(priorityMap[row.priority] ?? cleanDisplayText(row.priority))}</span>
            </div>
            <p>${escapeHtml(cleanDisplayText(row.reason))}</p>
            <small>${escapeHtml(cleanDisplayText(row.suggested_check))}</small>
        </div>
    `;
}

function renderSignalCard(label, value, note = '') {
    return `
        <article class="ops-signal-card">
            <span>${escapeHtml(label)}</span>
            <strong>${escapeHtml(value)}</strong>
            ${note ? `<p>${escapeHtml(note)}</p>` : ''}
        </article>
    `;
}

function renderSelectedStructure(selectedProduct, selectedDetail) {
    if (!selectedProduct || !selectedDetail) {
        return `
            <section class="ops-panel ops-section">
                <div class="ops-empty">
                    <strong>선택 상품 구조를 표시할 수 없습니다.</strong>
                </div>
            </section>
        `;
    }

    return `
        <section class="ops-panel ops-section">
            <div class="ops-section-head">
                <div>
                    <h3>선택 상품 구조</h3>
                    <p>브랜드 전체 구조를 본 뒤, 이 상품이 어떤 역할과 흐름을 가지는지 더 깊게 보는 영역입니다.</p>
                </div>
                <span class="ops-pill">선택 반영</span>
            </div>
            <div class="ops-selected-structure-intro">
                <strong>${escapeHtml(cleanDisplayText(selectedDetail.headline || `${selectedProduct.product_name} 해석`))}</strong>
                <p>${escapeHtml(cleanDisplayText(selectedDetail.summary))}</p>
            </div>
            <div class="ops-selected-structure-grid">
                ${renderSignalCard('주요 역할', friendlyRoleLabel(selectedProduct.profile_role_primary))}
                ${renderSignalCard('현재 상태', friendlyStateLabel(selectedProduct.role_state_primary))}
                ${renderSignalCard('오늘 매출', formatCurrency(selectedProduct.revenue))}
                ${renderSignalCard('7일 누적', formatCurrency(selectedProduct.revenue_7d))}
                ${renderSignalCard('30일 누적', formatCurrency(selectedProduct.revenue_30d))}
                ${renderSignalCard('90일 누적', formatCurrency(selectedProduct.revenue_90d))}
                ${renderSignalCard('브랜드 내 비중', formatPercent(selectedProduct.revenue_share_in_brand_day))}
                ${renderSignalCard('다음 연결 상품', cleanDisplayText(selectedProduct.top_transition_target_name))}
                ${renderSignalCard('상위 전환율', formatPercent(selectedProduct.top_transition_rate))}
                ${renderSignalCard('복귀율', formatPercent(selectedProduct.qualified_return_rate))}
                ${renderSignalCard('루프율', formatPercent(selectedProduct.return_loop_rate))}
            </div>
            <div class="ops-note">${escapeHtml(cleanDisplayText(selectedDetail.priority_hint))}</div>
        </section>
    `;
}

function renderBrandRoleMember(row) {
    return `
        <div class="ops-role-product-row">
            <div class="ops-role-product-main">
                ${renderThumbnail({ imageUrl: row.image_url, alt: row.product_name, size: 'xs' })}
                <div>
                    <strong>${escapeHtml(row.product_name)}</strong>
                    <small>${escapeHtml(`${formatCurrency(row.revenue)} · 역할 내 ${formatPercent(row.revenue_share_in_role)}`)}</small>
                </div>
            </div>
            <span>${escapeHtml(`${row.role_rank ?? '-'}위`)}</span>
        </div>
    `;
}

function renderWindowRoleMember(row, label) {
    return `
        <div class="ops-window-role-item">
            <div class="ops-role-product-main">
                ${renderThumbnail({ imageUrl: row.image_url, alt: row.product_name, size: 'xs' })}
                <div>
                    <strong>${escapeHtml(row.product_name)}</strong>
                    <small>${escapeHtml(`${formatCurrency(row.window_revenue)} · ${label} 기여 ${formatPercent(row.share_in_role)}`)}</small>
                </div>
            </div>
        </div>
    `;
}

function renderBrandStructure(roleSections) {
    return `
        <section class="ops-panel ops-section ops-brand-structure-panel">
            <div class="ops-section-head">
                <div>
                    <h3>브랜드 전체 구조</h3>
                    <p>각 역할 안에 어떤 상품이 들어 있는지 보고, 7일·30일·90일 기준으로 구성이 어떻게 달라지는지 함께 읽습니다.</p>
                </div>
                <span class="ops-pill">전체 기준판</span>
            </div>
            ${roleSections.length ? `
                <div class="ops-brand-role-grid">
                    ${roleSections.map((section) => `
                        <article class="ops-brand-role-card">
                            <div class="ops-brand-role-head">
                                <div>
                                    <span>${escapeHtml(section.roleLabel)}</span>
                                    <strong>${escapeHtml(formatCurrency(section.roleRevenue))}</strong>
                                </div>
                                <div class="ops-brand-role-meta">
                                    <small>${escapeHtml(`${section.roleProductCount}개 상품`)}</small>
                                    <small>${escapeHtml(`${formatPercent(section.roleShare)} 비중`)}</small>
                                </div>
                            </div>

                            <div class="ops-brand-role-now">
                                <div class="ops-brand-role-subhead">
                                    <strong>현재 포함 상품</strong>
                                    <span>최신 기준</span>
                                </div>
                                ${section.currentMembers.length ? `
                                    <div class="ops-brand-role-list">
                                        ${section.currentMembers.map(renderBrandRoleMember).join('')}
                                    </div>
                                ` : `
                                    <div class="ops-empty compact">
                                        <strong>현재 포함 상품이 없습니다.</strong>
                                    </div>
                                `}
                            </div>

                            <div class="ops-brand-role-window-grid">
                                ${section.windows.map((window) => `
                                    <div class="ops-brand-role-window">
                                        <div class="ops-brand-role-subhead">
                                            <strong>${escapeHtml(window.label)} 구성</strong>
                                            <span>상위 흐름</span>
                                        </div>
                                        ${window.rows.length ? `
                                            <div class="ops-window-role-list">
                                                ${window.rows.map((row) => renderWindowRoleMember(row, window.label)).join('')}
                                            </div>
                                        ` : `
                                            <div class="ops-empty compact">
                                                <strong>${escapeHtml(`${window.label} 기준 데이터 없음`)}</strong>
                                            </div>
                                        `}
                                    </div>
                                `).join('')}
                            </div>
                        </article>
                    `).join('')}
                </div>
            ` : `
                <div class="ops-empty">
                    <strong>브랜드 전체 구조 데이터가 아직 없습니다.</strong>
                </div>
            `}
        </section>
    `;
}

export function renderProductsPage({
    productRows,
    allProductRows = [],
    detailRows,
    selectedProductId,
    roleStructureRows,
    revenueStructureRows,
    brandRoleStructureRows = [],
    brandRoleWindowComparisonRows = [],
    searchQuery,
    transitionSummaryRows = [],
    returnLoopSummaryRows = [],
    revenueInflowRows = [],
    priorityRows = []
}) {
    const selectedProduct = productRows.find((row) => row.product_id === selectedProductId) ?? productRows[0];
    const selectedDetail = detailRows.find((row) => row.product_id === selectedProduct?.product_id);
    const sourceProductRows = allProductRows.length ? allProductRows : productRows;
    const currentRoleRows = brandRoleStructureRows.length ? brandRoleStructureRows : deriveCurrentRoleRows(sourceProductRows);
    const windowRoleRows = brandRoleWindowComparisonRows.length ? brandRoleWindowComparisonRows : deriveWindowRows(sourceProductRows);
    const roleSections = buildRoleSections({
        productRows: sourceProductRows,
        currentRows: currentRoleRows,
        windowRows: windowRoleRows,
        roleStructureRows
    });

    const selectedTransitionRows = transitionSummaryRows
        .filter((row) => row.product_id === selectedProduct?.product_id)
        .slice(0, 3);
    const selectedLoopRows = returnLoopSummaryRows
        .filter((row) => row.product_id === selectedProduct?.product_id)
        .slice(0, 1);
    const selectedInflowRows = revenueInflowRows.slice(0, 3);
    const selectedPriorityRows = priorityRows
        .filter((row) => row.entity_type === 'product' && row.entity_id === selectedProduct?.product_id)
        .slice(0, 2);
    const prioritySupportRows = selectedPriorityRows.length ? selectedPriorityRows : priorityRows.slice(0, 2);

    return `
        <section class="ops-products-stack">
            ${renderBrandStructure(roleSections)}

            <section class="ops-workspace-shell">
                <aside class="ops-panel ops-section ops-selection-panel">
                    <div class="ops-section-head ops-product-head">
                        <div>
                            <h3>상품 선택</h3>
                            <p>브랜드 전체 구조를 본 뒤, 특정 상품을 더 깊게 확인할 때 쓰는 보조 탐색기입니다.</p>
                        </div>
                        <div class="ops-product-head-meta">
                            <span class="ops-pill">${escapeHtml(`${productRows.length}개`)}</span>
                        </div>
                    </div>
                    <div class="ops-table-toolbar ops-product-toolbar">
                        <input class="ops-search" type="search" id="ops-product-search" placeholder="상품명 또는 상품 번호로 찾기" value="${escapeHtml(searchQuery)}">
                    </div>
                    ${renderProductGallery(productRows, selectedProduct?.product_id)}
                </aside>

                <div class="ops-analysis-workspace">
                    ${renderProductDetail(selectedDetail, selectedProduct)}

                    <div class="ops-analysis-grid">
                        ${renderSelectedStructure(selectedProduct, selectedDetail)}

                        <div class="ops-support-stack">
                            ${renderCompactPanel(
                                '전환 흐름',
                                '선택한 상품에서 다음으로 이어지는 흐름입니다.',
                                selectedTransitionRows,
                                '선택한 상품의 전환 흐름이 아직 없습니다.',
                                renderTransitionRow
                            )}
                            ${renderCompactPanel(
                                '복귀 흐름',
                                '다시 돌아오는 패턴과 반복 흐름입니다.',
                                selectedLoopRows,
                                '선택한 상품의 복귀 흐름이 아직 없습니다.',
                                renderReturnRow
                            )}
                            ${renderCompactPanel(
                                '유입 맥락',
                                '선택 해석을 보조하는 공통 유입 맥락입니다.',
                                selectedInflowRows,
                                '유입 맥락 데이터가 없습니다.',
                                renderInflowRow
                            )}
                            ${renderCompactPanel(
                                '함께 볼 점검 근거',
                                '선택 상품과 함께 확인할 보조 신호입니다.',
                                prioritySupportRows,
                                '함께 볼 점검 근거가 없습니다.',
                                renderPriorityRow
                            )}
                        </div>
                    </div>
                </div>
            </section>
        </section>
    `;
}
