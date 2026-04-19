function escapeHtml(value) {
    return String(value ?? '').replace(/[&<>"']/g, (character) => ({
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;'
    })[character]);
}

const ROLE_LABEL_MAP = {
    entry: '첫구매 유도',
    expansion: '단골 유도',
    return: '반복 구매',
    convergence: '구매 집중'
};
const OBSERVATION_BLANK_LABEL = '관측 상태 없음';
const PROFILE_FALLBACK_LABEL = '프로필 정보 없음';

export function cleanDisplayText(value) {
    const text = String(value ?? '').trim();
    if (!text) {
        return '데이터 없음';
    }

    return text
        .replace(/same[-_ ]date snapshot/gi, '기준일 스냅샷')
        .replace(/same[-_ ]date role snapshot/gi, '기준일 스냅샷')
        .replace(/same-date role state/gi, '기준일 관측 상태')
        .replace(/same-date blank/gi, `기준일 ${OBSERVATION_BLANK_LABEL}`)
        .replace(/role-state/gi, '상태')
        .replace(/runtime mode/gi, '상태')
        .replace(/artifact-backed/gi, '실데이터 연결')
        .replace(/sample fallback/gi, '예시 데이터')
        .replace(/latest role fallback/gi, '최신 역할 보정')
        .replace(/\bblank\b/gi, OBSERVATION_BLANK_LABEL)
        .replace(/PGM 미관측/gi, OBSERVATION_BLANK_LABEL)
        .replace(/상태 미확인/gi, OBSERVATION_BLANK_LABEL)
        .replace(/\bentry\b/gi, '첫구매 유도')
        .replace(/\bexpansion\b/gi, '단골 유도')
        .replace(/\breturn\b/gi, '반복 구매')
        .replace(/\bconvergence\b/gi, '구매 집중')
        .replace(/\bproduct\b/gi, '상품')
        .replace(/\bbrand\b/gi, '브랜드')
        .replace(/Deferred:/gi, '')
        .replace(/\s+/g, ' ')
        .trim();
}

export function friendlyRoleLabel(value) {
    const normalized = String(value ?? '').trim().toLowerCase();

    if (!normalized || normalized === 'blank') {
        return OBSERVATION_BLANK_LABEL;
    }

    if (normalized === PROFILE_FALLBACK_LABEL.toLowerCase()) {
        return PROFILE_FALLBACK_LABEL;
    }

    return ROLE_LABEL_MAP[normalized] ?? cleanDisplayText(value);
}

export function friendlyStateLabel(value) {
    const normalized = String(value ?? '').trim().toLowerCase();
    if (!normalized) {
        return OBSERVATION_BLANK_LABEL;
    }

    if (normalized === 'pgm 미관측' || normalized === 'blank' || normalized === OBSERVATION_BLANK_LABEL) {
        return OBSERVATION_BLANK_LABEL;
    }

    if (ROLE_LABEL_MAP[normalized]) {
        return ROLE_LABEL_MAP[normalized];
    }

    return cleanDisplayText(value);
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
            <div class="ops-empty empty-state">
                <strong>표시할 상품이 없습니다.</strong>
                <p>실데이터 연결이 완료되면 선택 리스트가 이 영역에 나타납니다.</p>
            </div>
        `;
    }

    return `
        <div class="table-container">
            <table class="data-table ops-product-gallery-table">
                <thead>
                    <tr>
                        <th>상품</th>
                        <th>프로필</th>
                        <th>관측 상태</th>
                        <th>최근 매출</th>
                    </tr>
                </thead>
                <tbody>
            ${rows.map((row) => {
                const imageUrl = getImageUrl(row);
                return `
                    <tr class="clickable ${row.product_id === selectedProductId ? 'row-focused' : ''}" data-product-id="${escapeHtml(row.product_id)}">
                        <td>
                            <div class="ops-product-list-item">
                                ${renderThumbnail({ imageUrl, alt: row.product_name, size: 'xs', className: 'ops-product-thumb' })}
                                <div class="ops-product-list-body">
                                    <div class="ops-product-list-head">
                                        <strong>${escapeHtml(row.product_name)}</strong>
                                    </div>
                                    <p class="ops-product-list-summary">${escapeHtml(formatText(`${row.pgm_observed_flag === 'true' ? '기준일 관측 반영' : '기준일 관측 상태 없음'} · ${formatCurrency(row.revenue)} · 최근 확정일 기준`))}</p>
                                </div>
                            </div>
                        </td>
                        <td><span class="badge">${escapeHtml(friendlyRoleLabel(row.profile_role_primary))}</span></td>
                        <td><span class="badge">${escapeHtml(friendlyStateLabel(row.role_state_primary))}</span></td>
                        <td>${escapeHtml(formatCurrency(row.revenue_30d || row.revenue))}</td>
                    </tr>
                `;
            }).join('')}
                </tbody>
            </table>
        </div>
    `;
}

export function renderProductDetail(detailRow, productRow) {
    if (!detailRow || !productRow) {
        return `
            <aside class="ops-detail-panel pgm-side card">
                <div class="ops-empty empty-state">
                    <strong>상품을 선택하세요.</strong>
                    <p>좌측 리스트에서 상품을 고르면 해석 패널이 이 영역에 연결됩니다.</p>
                </div>
            </aside>
        `;
    }

    const imageUrl = getImageUrl(productRow);
    const detailUrl = getDetailUrl(productRow);
    const observedLabel = productRow.pgm_observed_flag === 'true' ? '기준일 관측 있음' : '기준일 관측 상태 없음';

    return `
        <aside class="ops-detail-panel pgm-side card">
            <div class="pgm-side-summary">
                <div class="pgm-side-hero">
                    <span class="pgm-badge badge">${escapeHtml(observedLabel)}</span>
                    <div class="ops-detail-hero">
                        ${renderThumbnail({ imageUrl, alt: productRow.product_name, className: 'ops-detail-thumb' })}
                        <div class="ops-detail-hero-copy">
                            <span class="ops-detail-eyebrow">선택 상품</span>
                            <strong>${escapeHtml(productRow.product_name)}</strong>
                            <p class="chart-hint">${escapeHtml(cleanDisplayText(detailRow.summary))}</p>
                        </div>
                    </div>
                </div>

                <section class="pgm-side-section-card pgm-side-section-card--insight">
                    <h4>핵심 지표</h4>
                    <div class="pgm-metrics">
                        <div>
                            <label>기준일 매출</label>
                            <strong>${escapeHtml(formatCurrency(productRow.revenue))}</strong>
                            <span>최근 확정일 기준</span>
                        </div>
                        <div>
                            <label>직전 7일 누적</label>
                            <strong>${escapeHtml(formatCurrency(productRow.revenue_7d))}</strong>
                            <span>짧은 기간 비교</span>
                        </div>
                        <div>
                            <label>기준일 브랜드 내 비중</label>
                            <strong>${escapeHtml(formatPercent(productRow.revenue_share_in_brand_day))}</strong>
                            <span>브랜드 내 당일 비중</span>
                        </div>
                        <div>
                            <label>직전 30일 누적</label>
                            <strong>${escapeHtml(formatCurrency(productRow.revenue_30d))}</strong>
                            <span>보조 기간 비교</span>
                        </div>
                    </div>
                    ${detailUrl ? `<a class="btn-primary ops-product-link" href="${escapeHtml(detailUrl)}" target="_blank" rel="noreferrer">상품 상세 열기</a>` : ''}
                </section>

                <section class="pgm-side-section-card">
                    <h4>역할 문맥</h4>
                    <div class="pgm-demand-share-grid">
                        <div class="pgm-demand-share-card">
                            <label>상품 기준 프로필</label>
                            <strong>${escapeHtml(friendlyRoleLabel(productRow.profile_role_primary))}</strong>
                            <span>최신 프로필 스냅샷</span>
                        </div>
                        <div class="pgm-demand-share-card">
                            <label>기준일 관측 상태</label>
                            <strong>${escapeHtml(friendlyStateLabel(productRow.role_state_primary))}</strong>
                            <span>${escapeHtml(observedLabel)}</span>
                        </div>
                    </div>
                </section>

                <div class="insight-note">${escapeHtml(cleanDisplayText(detailRow.priority_hint))}</div>
            </div>
        </aside>
    `;
}

export function renderProductTable(rows, selectedProductId) {
    return renderProductGallery(rows, selectedProductId);
}
