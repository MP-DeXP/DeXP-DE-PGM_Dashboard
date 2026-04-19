const state = {
    view: 'priority',
    bundle: null,
    selectedProductId: ''
};

const VIEW_MODEL_FILES = {
    vm_priority_queue: ['vm_priority_queue.csv'],
    vm_queue_summary: ['vm_queue_summary.csv'],
    vm_segment_map: ['vm_segment_map.csv'],
    vm_structure_map_cells: ['vm_structure_map_cells.csv', 'vm_role_revenue_matrix.csv'],
    vm_product_detail: ['vm_product_detail.csv'],
    vm_definition_rules: ['vm_operation_rules.csv', 'vm_definition_rules.csv'],
    vm_data_health: ['vm_data_health.csv'],
    vm_data_health_overview: ['vm_data_health_overview.csv', 'vm_operations_overview.csv'],
    vm_data_health_detail: ['vm_data_health_detail.csv'],
    vm_brand_score_panel: ['vm_brand_score_panel.csv'],
    vm_brand_score_product_contributors: ['vm_brand_score_product_contributors.csv'],
    vm_reconstruction_registry: ['vm_reconstruction_registry.csv'],
    vm_iteration_log: ['vm_iteration_log.csv']
};

const MART_FILES = {
    mart_product_revenue_windows: ['mart_product_revenue_windows.csv'],
    mart_product_role_taxonomy_daily: ['mart_product_role_taxonomy_daily.csv'],
    mart_product_priority_basis: ['mart_product_priority_basis.csv'],
    mart_priority_queue_snapshot: ['mart_priority_queue_snapshot.csv'],
    mart_segment_structure_snapshot: ['mart_role_revenue_matrix_snapshot.csv', 'mart_segment_structure_snapshot.csv'],
    mart_data_health_snapshot: ['mart_operations_overview_snapshot.csv', 'mart_data_health_snapshot.csv'],
    mart_brand_score_reconstruction: ['mart_brand_score_reconstruction.csv'],
    mart_brand_score_validation_status: ['mart_brand_status_summary.csv', 'mart_brand_score_validation_status.csv']
};

const QA_FILES = {
    raw_manifest: ['operations_manifest.csv', 'raw_manifest.csv'],
    validation_summary: ['operations_checks.csv', 'validation_summary.csv'],
    validation_report: ['validation_report.md'],
    tone_audit: ['tone_audit.csv'],
    implementation_scope: ['implementation_scope.csv']
};

const REVENUE_SEGMENT_ORDER = ['감소', '유지', '증가'];
const ROLE_ORDER = ['첫구매기여', '재구매확장기여', '반복구매기여', '동시구매기여', '관측 없음'];

function escapeHtml(value) {
    return String(value ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function parseCsv(text) {
    if (!text || !text.trim()) {
        return [];
    }

    const rows = [];
    let currentCell = '';
    let currentRow = [];
    let inQuotes = false;

    for (let index = 0; index < text.length; index += 1) {
        const character = text[index];
        const nextCharacter = text[index + 1];

        if (character === '"') {
            if (inQuotes && nextCharacter === '"') {
                currentCell += '"';
                index += 1;
            } else {
                inQuotes = !inQuotes;
            }
            continue;
        }

        if (character === ',' && !inQuotes) {
            currentRow.push(currentCell);
            currentCell = '';
            continue;
        }

        if ((character === '\n' || character === '\r') && !inQuotes) {
            if (character === '\r' && nextCharacter === '\n') {
                index += 1;
            }

            currentRow.push(currentCell);
            rows.push(currentRow);
            currentCell = '';
            currentRow = [];
            continue;
        }

        currentCell += character;
    }

    currentRow.push(currentCell);
    rows.push(currentRow);

    const [header, ...dataRows] = rows.filter((row) => row.some((cell) => cell !== ''));
    if (!header) {
        return [];
    }

    return dataRows.map((row) => Object.fromEntries(header.map((column, index) => [column, row[index] ?? ''])));
}

function toNumber(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
}

function formatNumber(value, fallback = '-') {
    const number = toNumber(value);
    return number == null ? fallback : number.toLocaleString('ko-KR');
}

function formatPercent(value, { digits = 1, signed = true, fallback = '-' } = {}) {
    const number = toNumber(value);
    if (number == null) {
        return fallback;
    }

    const percentText = `${(number * 100).toFixed(digits)}%`;
    if (signed && number > 0) {
        return `+${percentText}`;
    }

    return percentText;
}

function formatDecimal(value, { digits = 2, fallback = '-' } = {}) {
    const number = toNumber(value);
    return number == null ? fallback : number.toFixed(digits);
}

function textOrFallback(value, fallback = '-') {
    const text = String(value ?? '').trim();
    return text || fallback;
}

function normalizeRevenueSegment(value) {
    const text = textOrFallback(value, '유지');

    if (text.includes('감소') || text.includes('하락')) {
        return '감소';
    }

    if (text.includes('증가') || text.includes('상승')) {
        return '증가';
    }

    return '유지';
}

function translateCompareState(value) {
    if (value === 'available') return '비교 가능';
    if (value === 'unavailable') return '비교 보류';
    if (value === 'partial') return '일부 비교 가능';
    return textOrFallback(value);
}

function translateBrandStatus(value) {
    if (value === 'limited') return '참고용 집계';
    if (value === 'provisional') return '임시 집계';
    if (value === 'unavailable') return '미집계';
    if (value === 'near-core' || value === 'near_core') return '고유사도 후보';
    if (value === 'available') return '정상 반영';
    return textOrFallback(value);
}

function brandStatusClass(value) {
    if (value === 'available' || value === 'near_core') return 'status-ready';
    if (value === 'limited' || value === 'provisional') return 'status-warning';
    if (value === 'unavailable') return 'status-muted';
    return 'status-neutral';
}

function coverageStateClass(value) {
    if (value === '정상') return 'status-ready';
    if (value === '30일까지만 가능') return 'status-warning';
    if (value === 'history 부족' || value === '비교 불가') return 'status-muted';
    return 'status-neutral';
}

function translateRuleGroup(value) {
    if (value === 'Revenue') return '매출 흐름';
    if (value === 'Role') return '역할 분류';
    if (value === 'Brand Score') return '브랜드 상태';
    if (value === '데이터 상태') return '운영 데이터';
    return textOrFallback(value);
}

function translateRuleName(value) {
    if (value === 'same-date snapshot only') return '동일 일자 기준 분류';
    if (value === 'canonical score 우선') return '대표 점수 우선';
    if (value === 'freshness cap') return '최신 반영 범위 적용';
    return textOrFallback(value);
}

function translateRuleStatus(value) {
    if (value === '제한적 반영') return '참고 운영';
    return textOrFallback(value);
}

function translateDetailSection(value) {
    if (value === 'Revenue') return '매출 흐름';
    if (value === 'Role') return '역할 분류';
    if (value === 'Brand Score') return '브랜드 상태';
    return textOrFallback(value);
}

function translateDetailLabel(section, label) {
    if (section === 'Role' && label === '현재 taxonomy') return '현재 역할 분류';
    if (section === 'Revenue' && label === '최근 30일 대비') return '30일 매출 변화';
    if (section === 'Revenue' && label === '비교 상태') return '비교 가능 여부';
    if (section === 'Brand Score' && label === '상태') return '브랜드 반영 상태';
    if (section === 'Brand Score' && label === 'brand-level 상태') return '브랜드 반영 상태';
    if (section === 'Brand Score' && label === '상품 기여 상태') return '상품 기여 상태';
    if (section === '근거' && label === '반복 구매') return '반복 구매 기여';
    if (section === '근거' && label === '동시 구매') return '동시 구매 기여';
    if (section === '근거' && label === '동시구매 분류') return '동시 구매 분류';
    if (section === '근거' && label === '상위 연관 상품') return '상위 연관 상품';
    return textOrFallback(label);
}

function translateDetailValue(section, label, value) {
    if (section === 'Revenue' && label === '비교 상태') {
        return translateCompareState(value);
    }

    if (section === 'Brand Score') {
        return translateBrandStatus(value);
    }

    return textOrFallback(value);
}

function sanitizeOperatingCopy(value) {
    let text = String(value ?? '').trim();

    if (!text) {
        return '-';
    }

    const replacements = [
        ['Brand Score limited', '브랜드 상태는 참고용으로 제공합니다.'],
        ['Brand Score provisional', '브랜드 상태는 임시 집계로 제공합니다.'],
        ['Brand Score unavailable', '브랜드 상태는 아직 집계되지 않았습니다.'],
        ['event 또는 basket freshness 제약으로 provisional을 제한했습니다.', '브랜드 신호 최신 반영 범위를 확인 중이라 참고용으로 제공합니다.'],
        ['event 또는 basket freshness 제약으로 provisional을 제한합니다.', '브랜드 신호 최신 반영 범위를 확인 중이라 참고용으로 제공합니다.'],
        ['직전 동일 길이 기간과 비교 가능합니다.', '직전 같은 길이 기간과 바로 비교할 수 있습니다.'],
        ['직전기간 비교를 위한 history가 부족합니다.', '직전 기간과 비교할 이력이 아직 충분하지 않습니다.'],
        ['Role taxonomy', '역할 분류'],
        ['same-date snapshot', '동일 일자 기준'],
        ['canonical score', '대표 점수'],
        ['source freshness', '데이터 반영 현황'],
        ['source history', '이력 범위']
    ];

    replacements.forEach(([from, to]) => {
        text = text.replaceAll(from, to);
    });

    text = text.replace(/\bavailable\b/gi, '비교 가능');
    text = text.replace(/\blimited\b/gi, '참고용 집계');
    text = text.replace(/\bprovisional\b/gi, '임시 집계');
    text = text.replace(/\bunavailable\b/gi, '미집계');
    text = text.replace(/\btaxonomy\b/gi, '분류');
    text = text.replace(/\bfreshness\b/gi, '최신 반영');
    text = text.replace(/\bwindow\b/gi, '기간');
    text = text.replace(/\bhistory\b/gi, '이력');
    text = text.replace(/\bsource\b/gi, '데이터');
    text = text.replace(/\bRevenue\b/g, '매출');
    text = text.replace(/\bRole\b/g, '역할');

    return text;
}

function priorityClass(level) {
    if (level === '즉시 확인') return 'priority-immediate';
    if (level === '주의 관찰') return 'priority-watch';
    return 'priority-stable';
}

function buildImageAlt(productName) {
    return productName || '상품 이미지';
}

function renderProductThumb(row, { size = 'medium' } = {}) {
    const productName = row.product_name || row.product_id || '';
    const imageUrl = String(row.product_image_url || '').trim();
    const sizeClass = size === 'small' ? 'is-small' : size === 'large' ? 'is-large' : '';
    const altText = escapeHtml(buildImageAlt(productName));

    if (!imageUrl) {
        return `
            <span
                class="product-thumb is-placeholder ${sizeClass}"
                data-product-thumb
                role="img"
                aria-label="${altText}"
            >
                <span class="product-thumb-placeholder" aria-hidden="true"></span>
            </span>
        `;
    }

    return `
        <span class="product-thumb ${sizeClass}" data-product-thumb>
            <img
                src="${escapeHtml(imageUrl)}"
                alt="${altText}"
                loading="lazy"
                data-product-image
            >
            <span class="product-thumb-placeholder" aria-hidden="true"></span>
        </span>
    `;
}

function renderProductIdentity(row, { size = 'medium', showProductId = false } = {}) {
    return `
        <div class="product-identity">
            ${renderProductThumb(row, { size })}
            <div class="product-identity-copy">
                <strong>${escapeHtml(row.product_name || row.product_id)}</strong>
                ${showProductId ? `<p>${escapeHtml(row.product_id || '')}</p>` : ''}
            </div>
        </div>
    `;
}

function renderStatusChip(label, className = 'status-neutral') {
    return `<span class="status-chip ${className}">${escapeHtml(label)}</span>`;
}

function getViewModel(name) {
    return state.bundle?.view_model?.[name] ?? [];
}

function getMartRows(name) {
    return state.bundle?.mart?.[name] ?? [];
}

function getQaRows(name) {
    return state.bundle?.qa?.[name] ?? [];
}

function setTitle(view) {
    const titleMap = {
        priority: '우선순위',
        segments: '구조 맵',
        detail: '상세 보기',
        definitions: '운영 기준',
        health: '운영 현황'
    };
    document.querySelector('#page-title').textContent = titleMap[view] ?? 'PGM 운영 툴';
}

function buildProductMaps() {
    const queueRows = getViewModel('vm_priority_queue');
    const brandLevelRows = getViewModel('vm_brand_score_panel');
    const contributorRows = getViewModel('vm_brand_score_product_contributors');
    const roleRows = getMartRows('mart_product_role_taxonomy_daily');
    const revenueRows = getMartRows('mart_product_revenue_windows');

    return {
        queueMap: new Map(queueRows.map((row) => [row.product_id, row])),
        brandLevelRow: brandLevelRows[0] ?? {},
        contributorMap: new Map(contributorRows.map((row) => [row.product_id, row])),
        roleMap: new Map(roleRows.map((row) => [row.product_id, row])),
        revenueMap: new Map(revenueRows.map((row) => [row.product_id, row]))
    };
}

function orderValues(values, preferredOrder) {
    const uniqueValues = Array.from(new Set(values.filter(Boolean)));
    const ordered = preferredOrder.filter((value) => uniqueValues.includes(value));
    const rest = uniqueValues.filter((value) => !preferredOrder.includes(value)).sort((left, right) => left.localeCompare(right, 'ko'));
    return [...ordered, ...rest];
}

function summarizePriorityCounts(rows) {
    return [
        { label: '즉시 확인', count: rows.filter((row) => row.priority_level === '즉시 확인').length, className: 'priority-immediate' },
        { label: '주의 관찰', count: rows.filter((row) => row.priority_level === '주의 관찰').length, className: 'priority-watch' },
        { label: '정상 유지', count: rows.filter((row) => row.priority_level === '정상 유지').length, className: 'priority-stable' }
    ].filter((item) => item.count > 0);
}

function summarizeBrandStatuses(rows) {
    const counts = new Map();

    rows.forEach((row) => {
        const key = row.brand_score_status || 'unknown';
        counts.set(key, (counts.get(key) ?? 0) + 1);
    });

    return Array.from(counts.entries()).map(([status, count]) => ({
        status,
        label: `${translateBrandStatus(status)} ${formatNumber(count)}개`,
        className: brandStatusClass(status)
    }));
}

function buildSegmentCells(rows) {
    const { queueMap, contributorMap, roleMap, revenueMap } = buildProductMaps();
    const cells = new Map();

    rows.forEach((row) => {
        const revenueSegment = normalizeRevenueSegment(row.revenue_segment);
        const roleTaxonomy = textOrFallback(row.role_taxonomy, '기타');
        const cellKey = `${revenueSegment}::${roleTaxonomy}`;
        const queueRow = queueMap.get(row.product_id) ?? {};
        const brandRow = contributorMap.get(row.product_id) ?? {};
        const roleRow = roleMap.get(row.product_id) ?? {};
        const revenueRow = revenueMap.get(row.product_id) ?? {};

        if (!cells.has(cellKey)) {
            cells.set(cellKey, []);
        }

        cells.get(cellKey).push({
            ...row,
            revenue_segment: revenueSegment,
            role_taxonomy: roleTaxonomy,
            rank: toNumber(queueRow.rank) ?? Number.POSITIVE_INFINITY,
            priority_level: queueRow.priority_level || row.priority_level,
            brand_score_status: brandRow.brand_score_status || row.brand_score_status || '',
            role_score: roleRow.role_score || '',
            revenue_change_rate_30d: revenueRow.revenue_30d_delta_rate || queueRow.revenue_change_rate_30d || ''
        });
    });

    return cells;
}

function renderSummaryCards() {
    const rows = getViewModel('vm_queue_summary');

    if (!rows.length) {
        return '<div class="empty-state">표시할 우선순위 집계가 없습니다.</div>';
    }

    const brandRevenueCurrent = Number(rows[0]?.brand_revenue_30d_current ?? 0);
    const brandRevenuePrevious = Number(rows[0]?.brand_revenue_30d_previous ?? 0);
    const brandRevenueDeltaRate = rows[0]?.brand_revenue_30d_delta_rate === '' || rows[0]?.brand_revenue_30d_delta_rate == null
        ? null
        : Number(rows[0]?.brand_revenue_30d_delta_rate);

    return `
        <div class="summary-grid">
            ${rows.map((row) => `
                <article class="summary-card">
                    <div class="section-kicker">우선순위</div>
                    <strong class="${priorityClass(row.priority_level)}">${escapeHtml(row.product_count)}</strong>
                    <div>${escapeHtml(row.priority_level)}</div>
                </article>
            `).join('')}
            <article class="summary-card">
                <div class="section-kicker">브랜드 매출</div>
                <strong>${escapeHtml(brandRevenueCurrent.toLocaleString('ko-KR'))}</strong>
                <div>최근 30일 합계</div>
                <small>직전 기간 ${escapeHtml(brandRevenuePrevious.toLocaleString('ko-KR'))}</small>
                <small>${escapeHtml(brandRevenueDeltaRate == null ? '비교 보류' : `${formatPercent(brandRevenueDeltaRate)}`)}</small>
            </article>
        </div>
    `;
}

function renderPriorityView() {
    const queueRows = getViewModel('vm_priority_queue');
    const rawMissing = state.bundle?.raw_data_status !== 'real_source_loaded';

    if (!queueRows.length) {
        return `
            <div class="empty-state">
                우선순위 큐가 비어 있습니다.
                ${rawMissing ? '실데이터가 아직 반영되지 않았습니다.' : '현재 기준일에 해당하는 큐 산출물이 없습니다.'}
            </div>
        `;
    }

    return `
        <section class="hero">
            <div class="hero-heading">
                <div>
                    <div class="section-kicker">첫 화면</div>
                    <h3 class="section-title">상품 우선순위 큐</h3>
                </div>
                <p class="muted">매출 흐름과 역할 분류를 기준으로 우선순위를 보고, 브랜드 상태는 참고 정보로 함께 확인합니다.</p>
            </div>
        </section>
        ${renderSummaryCards()}
        <section class="queue-list">
            ${queueRows.map((row) => `
                <article class="queue-row" data-product-row="${escapeHtml(row.product_id)}">
                    <div class="queue-row-main">
                        <div class="queue-row-header">
                            <div class="queue-row-title">
                                <div class="queue-row-rank">
                                    <span class="section-kicker">순위 ${escapeHtml(row.rank)}</span>
                                    <span class="queue-row-id">${escapeHtml(row.product_id || '')}</span>
                                </div>
                                ${renderProductIdentity(row, { size: 'small' })}
                            </div>
                            <div class="queue-row-badges">
                                <div class="pill ${priorityClass(row.priority_level)}">${escapeHtml(row.priority_level)}</div>
                                ${renderStatusChip(translateBrandStatus(row.brand_score_status), brandStatusClass(row.brand_score_status))}
                            </div>
                        </div>
                        <div class="reason-stack">
                            <div class="reason-item">
                                <strong>매출 흐름</strong>
                                <div>${escapeHtml(sanitizeOperatingCopy(row.revenue_reason))}</div>
                            </div>
                            <div class="reason-item">
                                <strong>역할 분류</strong>
                                <div>${escapeHtml(sanitizeOperatingCopy(row.role_reason))}</div>
                            </div>
                            <div class="reason-item">
                                <strong>브랜드 상태</strong>
                                <div>${escapeHtml(sanitizeOperatingCopy(row.brand_score_reason))}</div>
                            </div>
                        </div>
                    </div>
                    <div class="queue-row-meta">
                        <small>역할 분류: ${escapeHtml(textOrFallback(row.role_taxonomy))}</small>
                        <small>매출 반영일: ${escapeHtml(textOrFallback(row.revenue_freshness_max_date))}</small>
                        <small>역할 반영일: ${escapeHtml(textOrFallback(row.role_freshness_max_date))}</small>
                    </div>
                </article>
            `).join('')}
        </section>
    `;
}

function renderSegmentsView() {
    const structureRows = getViewModel('vm_structure_map_cells');
    const rows = structureRows.length ? structureRows : getViewModel('vm_segment_map');

    if (!rows.length) {
        return '<div class="empty-state">구조 맵 데이터가 없습니다.</div>';
    }

    const cells = structureRows.length
        ? new Map(rows.map((row) => [`${normalizeRevenueSegment(row.revenue_segment)}::${textOrFallback(row.role_taxonomy, '관측 없음')}`, row]))
        : buildSegmentCells(rows);
    const revenueSegments = orderValues(rows.map((row) => normalizeRevenueSegment(row.revenue_segment)), [...REVENUE_SEGMENT_ORDER, '비교 불가']);
    const roleTaxonomies = orderValues(rows.map((row) => textOrFallback(row.role_taxonomy, '관측 없음')), ROLE_ORDER);

    return `
        <section class="hero">
            <div class="hero-heading">
                <div>
                    <div class="section-kicker">구조 맵</div>
                    <h3 class="section-title">매출 변화 x 역할 분류</h3>
                </div>
                <p class="muted">Revenue 열과 역할 분류 행을 기준으로 큐를 묶고, 셀 안에서 상품 수와 브랜드 상태를 함께 봅니다.</p>
            </div>
        </section>
        <section class="panel matrix-shell">
            <div class="matrix-scroll">
                <div class="matrix-board" style="grid-template-columns: 150px repeat(${revenueSegments.length}, minmax(230px, 1fr));">
                    <div class="matrix-header matrix-corner">
                        <span class="section-kicker">행</span>
                        <strong>역할 분류</strong>
                    </div>
                    ${revenueSegments.map((segment) => `
                        <div class="matrix-header matrix-column-label">
                            <span class="section-kicker">열</span>
                            <strong>${escapeHtml(segment)}</strong>
                        </div>
                    `).join('')}
                    ${roleTaxonomies.map((roleTaxonomy) => `
                        <div class="matrix-row-label">
                            <span class="section-kicker">역할</span>
                            <strong>${escapeHtml(roleTaxonomy)}</strong>
                        </div>
                        ${revenueSegments.map((segment) => {
                            const cellRecord = cells.get(`${segment}::${roleTaxonomy}`);
                            const cellRows = Array.isArray(cellRecord) ? cellRecord : [];
                            const prioritySummary = structureRows.length
                                ? [
                                    { label: '즉시 확인', count: toNumber(cellRecord?.immediate_count) ?? 0, className: 'priority-immediate' },
                                    { label: '주의 관찰', count: toNumber(cellRecord?.watch_count) ?? 0, className: 'priority-watch' },
                                    { label: '정상 유지', count: toNumber(cellRecord?.stable_count) ?? 0, className: 'priority-stable' }
                                ].filter((item) => item.count > 0)
                                : summarizePriorityCounts(cellRows);
                            const brandSummary = structureRows.length
                                ? [
                                    { label: `${translateBrandStatus('limited')} ${formatNumber(cellRecord?.brand_limited_count, '0')}개`, className: brandStatusClass('limited') },
                                    { label: `${translateBrandStatus('provisional')} ${formatNumber(cellRecord?.brand_provisional_count, '0')}개`, className: brandStatusClass('provisional') },
                                    { label: `${translateBrandStatus('unavailable')} ${formatNumber(cellRecord?.brand_unavailable_count, '0')}개`, className: brandStatusClass('unavailable') }
                                ].filter((item) => !item.label.includes('0개'))
                                : summarizeBrandStatuses(cellRows);
                            const topProducts = structureRows.length
                                ? [1, 2, 3]
                                    .map((index) => ({
                                        product_id: cellRecord?.[`top_product_${index}_id`] || '',
                                        product_name: cellRecord?.[`top_product_${index}_name`] || '',
                                        priority_level: cellRecord?.[`top_product_${index}_priority`] || ''
                                    }))
                                    .filter((product) => product.product_id || product.product_name)
                                : cellRows.slice(0, 3);
                            const productCount = structureRows.length ? (toNumber(cellRecord?.product_count) ?? 0) : cellRows.length;
                            const attentionCount = structureRows.length
                                ? ((toNumber(cellRecord?.immediate_count) ?? 0) + (toNumber(cellRecord?.watch_count) ?? 0))
                                : cellRows.filter((row) => row.priority_level !== '정상 유지').length;

                            if (!productCount) {
                                return `
                                    <div class="matrix-cell matrix-cell-empty">
                                        <div class="matrix-empty">해당 상품이 없습니다.</div>
                                    </div>
                                `;
                            }

                            return `
                                <div class="matrix-cell">
                                    <div class="matrix-cell-top">
                                        <strong class="matrix-count">${escapeHtml(`${formatNumber(productCount)}개 상품`)}</strong>
                                        <span class="matrix-meta">${escapeHtml(`${formatNumber(attentionCount, '0')}건 확인 필요`)}</span>
                                    </div>
                                    <div class="matrix-chip-row">
                                        ${prioritySummary.map((item) => `<span class="pill ${item.className}">${escapeHtml(`${item.label} ${item.count}`)}</span>`).join('')}
                                    </div>
                                    <div class="matrix-product-list">
                                        ${topProducts.map((product) => `
                                            <div class="matrix-product-item">
                                                <strong>${escapeHtml(product.product_name || product.product_id)}</strong>
                                                <span>${escapeHtml(structureRows.length ? `${textOrFallback(product.priority_level, '상태 확인')}` : `${product.priority_level} · 기여 ${formatDecimal(product.role_score, { digits: 2, fallback: '-' })} · 매출 ${formatPercent(product.revenue_change_rate_30d, { fallback: '보류' })}`)}</span>
                                            </div>
                                        `).join('')}
                                    </div>
                                    <div class="matrix-chip-row is-secondary">
                                        ${brandSummary.map((item) => renderStatusChip(item.label, item.className)).join('')}
                                    </div>
                                </div>
                            `;
                        }).join('')}
                    `).join('')}
                </div>
            </div>
        </section>
    `;
}

function renderDetailOverviewCards(selectedQueueRow, roleRow, revenueRow, contributorRow, brandLevelRow) {
    const contributionNote = [
        textOrFallback(selectedQueueRow.role_taxonomy || roleRow.role_taxonomy),
        `기여 점수 ${formatDecimal(roleRow.role_score, { digits: 2, fallback: '-' })}`
    ].join(' · ');

    const brandNote = brandLevelRow.confidence_label
        ? `신뢰도 ${textOrFallback(brandLevelRow.confidence_label)}`
        : '신뢰도 집계 전';
    const contributorNote = contributorRow.contribution_status
        ? `상품 기여 ${translateBrandStatus(contributorRow.contribution_status)}`
        : '상품 기여 집계 전';

    return `
        <div class="detail-overview-grid">
            <article class="overview-card">
                <div class="section-kicker">우선순위</div>
                <strong class="overview-value ${priorityClass(selectedQueueRow.priority_level)}">${escapeHtml(selectedQueueRow.priority_level)}</strong>
                <p class="overview-note">${escapeHtml(sanitizeOperatingCopy(selectedQueueRow.revenue_reason))}</p>
            </article>
            <article class="overview-card">
                <div class="section-kicker">매출 흐름</div>
                <strong class="overview-value">${escapeHtml(formatPercent(revenueRow.revenue_30d_delta_rate ?? selectedQueueRow.revenue_change_rate_30d, { fallback: '보류' }))}</strong>
                <p class="overview-note">${escapeHtml(sanitizeOperatingCopy(revenueRow.revenue_30d_compare_note || selectedQueueRow.revenue_reason))}</p>
            </article>
            <article class="overview-card">
                <div class="section-kicker">상품 기여도</div>
                <strong class="overview-value">${escapeHtml(textOrFallback(roleRow.primary_axis_label || selectedQueueRow.role_taxonomy))}</strong>
                <p class="overview-note">${escapeHtml(contributionNote)}</p>
            </article>
            <article class="overview-card">
                <div class="section-kicker">브랜드 반영 상태</div>
                <strong class="overview-value">${escapeHtml(translateBrandStatus(brandLevelRow.status_label || brandLevelRow.brand_score_status || selectedQueueRow.brand_score_status))}</strong>
                <p class="overview-note">${escapeHtml(`${brandNote} · ${contributorNote} · ${sanitizeOperatingCopy(brandLevelRow.status_reason || selectedQueueRow.brand_score_reason)}`)}</p>
            </article>
        </div>
    `;
}

function renderDetailSectionRows(rows) {
    const sectionOrder = ['헤더', 'Revenue', 'Role', '근거', 'Brand Score'];
    const groups = sectionOrder
        .map((section) => ({
            section,
            rows: rows.filter((row) => row.section === section)
        }))
        .filter((group) => group.rows.length > 0);

    return `
        <div class="detail-section-grid">
            ${groups.map((group) => `
                <article class="detail-section-card">
                    <div class="section-kicker">${escapeHtml(translateDetailSection(group.section))}</div>
                    <dl class="detail-definition-list">
                        ${group.rows.map((row) => `
                            <div class="definition-pair">
                                <dt>${escapeHtml(translateDetailLabel(group.section, row.label))}</dt>
                                <dd>${escapeHtml(translateDetailValue(group.section, row.label, row.value))}</dd>
                                <small>${escapeHtml(sanitizeOperatingCopy(row.note))}</small>
                            </div>
                        `).join('')}
                    </dl>
                </article>
            `).join('')}
        </div>
    `;
}

function renderDetailView() {
    const queueRows = getViewModel('vm_priority_queue');
    const detailRows = getViewModel('vm_product_detail');

    if (!queueRows.length) {
        return '<div class="empty-state">상세 보기 데이터가 없습니다.</div>';
    }

    const { roleMap, contributorMap, brandLevelRow, revenueMap } = buildProductMaps();
    const selectedProductId = state.selectedProductId || queueRows[0].product_id;
    state.selectedProductId = selectedProductId;
    const selectedQueueRow = queueRows.find((row) => row.product_id === selectedProductId) ?? queueRows[0];
    const selectedRows = detailRows.filter((row) => row.product_id === selectedQueueRow.product_id);
    const roleRow = roleMap.get(selectedQueueRow.product_id) ?? {};
    const contributorRow = contributorMap.get(selectedQueueRow.product_id) ?? {};
    const revenueRow = revenueMap.get(selectedQueueRow.product_id) ?? {};

    return `
        <section class="detail-layout">
            <aside class="detail-card detail-sidebar">
                <div class="section-kicker">대상 선택</div>
                <h3>상품 목록</h3>
                <div class="product-picker">
                    ${queueRows.map((row) => `
                        <button
                            class="${row.product_id === selectedQueueRow.product_id ? 'is-selected' : ''}"
                            data-product-pick="${escapeHtml(row.product_id)}"
                        >
                            <span class="picker-product">
                                ${renderProductThumb(row, { size: 'small' })}
                                <span class="picker-product-copy">
                                    <strong>${escapeHtml(row.product_name || row.product_id)}</strong>
                                    <span class="picker-meta-row">
                                        <span class="picker-product-id">${escapeHtml(row.product_id || '')}</span>
                                        <span class="${priorityClass(row.priority_level)}">${escapeHtml(row.priority_level)}</span>
                                    </span>
                                </span>
                            </span>
                        </button>
                    `).join('')}
                </div>
            </aside>
            <section class="detail-card detail-main">
                <div class="section-kicker">상세 보기</div>
                <div class="detail-header-product">
                    ${renderProductThumb(selectedQueueRow, { size: 'medium' })}
                    <div class="detail-header-copy">
                        <h3>${escapeHtml(selectedQueueRow.product_name || selectedQueueRow.product_id)}</h3>
                        <p class="muted">${escapeHtml(selectedQueueRow.product_id)}</p>
                        <div class="inline-chip-row">
                            <span class="pill ${priorityClass(selectedQueueRow.priority_level)}">${escapeHtml(selectedQueueRow.priority_level)}</span>
                            ${renderStatusChip(textOrFallback(selectedQueueRow.role_taxonomy), 'status-neutral')}
                            ${renderStatusChip(translateBrandStatus(contributorRow.contribution_status || selectedQueueRow.brand_score_status), brandStatusClass(contributorRow.contribution_status || selectedQueueRow.brand_score_status))}
                        </div>
                    </div>
                </div>
                ${renderDetailOverviewCards(selectedQueueRow, roleRow, revenueRow, contributorRow, brandLevelRow)}
                ${renderDetailSectionRows(selectedRows)}
            </section>
        </section>
    `;
}

function renderDefinitionsView() {
    const rows = getViewModel('vm_definition_rules');

    if (!rows.length) {
        return '<div class="empty-state">운영 기준 데이터가 없습니다.</div>';
    }

    const groupOrder = ['Revenue', 'Role', 'Brand Score', '데이터 상태', '상품 이미지'];
    const groups = groupOrder
        .map((group) => ({
            group,
            rows: rows.filter((row) => row.rule_group === group)
        }))
        .filter((item) => item.rows.length > 0);

    return `
        <section class="hero">
            <div class="hero-heading">
                <div>
                    <div class="section-kicker">운영 기준</div>
                    <h3 class="section-title">화면에서 쓰는 기준과 안내</h3>
                </div>
                <p class="muted">운영자가 바로 이해할 수 있는 문구로 현재 적용 기준을 정리했습니다.</p>
            </div>
        </section>
        <section class="definition-groups">
            ${groups.map((group) => `
                <article class="detail-card definition-group">
                    <div class="section-kicker">${escapeHtml(translateRuleGroup(group.group))}</div>
                    <h3>${escapeHtml(translateRuleGroup(group.group))}</h3>
                    <div class="definition-rule-grid">
                        ${group.rows.map((row) => `
                            <article class="definition-rule">
                                <div class="definition-status">
                                    <strong>${escapeHtml(translateRuleName(row.rule_name))}</strong>
                                    ${renderStatusChip(translateRuleStatus(row.status_label), row.status_label === '정상' ? 'status-ready' : 'status-warning')}
                                </div>
                                <p>${escapeHtml(sanitizeOperatingCopy(row.rule_definition))}</p>
                            </article>
                        `).join('')}
                    </div>
                </article>
            `).join('')}
        </section>
    `;
}

function renderOverviewCard(title, value, note, className = '') {
    return `
        <article class="overview-card">
            <div class="section-kicker">${escapeHtml(title)}</div>
            <strong class="overview-value ${className}">${escapeHtml(value)}</strong>
            <p class="overview-note">${escapeHtml(note)}</p>
        </article>
    `;
}

function renderHealthView() {
    const overviewRows = getViewModel('vm_data_health_overview');
    const healthRows = getViewModel('vm_data_health_detail').length ? getViewModel('vm_data_health_detail') : getViewModel('vm_data_health');
    const queueRows = getViewModel('vm_priority_queue');
    const brandValidationRows = getMartRows('mart_brand_score_validation_status');
    const rawManifestRows = getQaRows('raw_manifest');
    const validationRows = getQaRows('validation_summary');
    const toneAuditRows = getQaRows('tone_audit');
    const iterationRows = getViewModel('vm_iteration_log');

    const brandValidation = brandValidationRows[0] ?? {};
    const productImageManifest = rawManifestRows.find((row) => row.dataset_key === 'products') ?? {};
    const productImageValidation = validationRows.find((row) => row.check_name === 'product_image_provenance') ?? {};
    const imageSummary = productImageManifest.data_provenance === 'rosetta_direct'
        ? '상품 이미지 반영 완료'
        : sanitizeOperatingCopy(productImageValidation.message || '상품 이미지 연결 확인 필요');
    const queueState = state.bundle?.raw_data_status === 'real_source_loaded'
        ? (queueRows.length ? '운영 가능' : '산출물 확인 필요')
        : '데이터 반영 대기';
    const toneState = toneAuditRows.every((row) => row.status === 'pass') ? '기본 문구 정리 완료' : '문구 확인 필요';

    return `
        <section class="hero">
            <div class="hero-heading">
                <div>
                    <div class="section-kicker">운영 현황</div>
                    <h3 class="section-title">오늘 바로 볼 운영 개요</h3>
                </div>
                <p class="muted">기본 화면에는 운영 준비도와 반영 범위만 두고, 원천 상세와 점검 내역은 아래 접힌 영역으로 내렸습니다.</p>
            </div>
        </section>
        <section class="overview-grid">
            ${renderOverviewCard('운영 준비도', queueState, `우선순위 큐 ${formatNumber(queueRows.length, '0')}건`)}
            ${overviewRows.map((row) => renderOverviewCard(row.area_title, translateBrandStatus(row.status_label) === row.status_label ? textOrFallback(row.summary_value) : `${translateBrandStatus(row.status_label)} · ${textOrFallback(row.summary_value)}`, sanitizeOperatingCopy(row.note))).join('')}
            ${renderOverviewCard('상품/연결 상태', imageSummary, toneState)}
        </section>
        <details class="detail-disclosure">
            <summary>데이터 연결 상세</summary>
            <div class="detail-disclosure-body">
                <table class="detail-disclosure-table">
                    <thead>
                        <tr>
                            <th>데이터 항목</th>
                            <th>행 수</th>
                            <th>시작일</th>
                            <th>최근일</th>
                            <th>반영 차이</th>
                            <th>운영 상태</th>
                            <th>비교 범위</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${healthRows.map((row) => `
                            <tr>
                                <td>${escapeHtml(textOrFallback(row.source_label, row.source_key))}</td>
                                <td>${escapeHtml(formatNumber(row.row_count, '0'))}</td>
                                <td>${escapeHtml(textOrFallback(row.min_date))}</td>
                                <td>${escapeHtml(textOrFallback(row.max_date))}</td>
                                <td>${escapeHtml(`${formatNumber(row.freshness_gap_days, '0')}일`)}</td>
                                <td>${renderStatusChip(textOrFallback(row.data_state), coverageStateClass(row.data_state))}</td>
                                <td>${escapeHtml(sanitizeOperatingCopy(row.coverage_note || row.coverage_state))}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        </details>
        <details class="detail-disclosure">
            <summary>운영 확인 상세</summary>
            <div class="detail-disclosure-body detail-disclosure-grid">
                <article class="grid-card">
                    <div class="section-kicker">화면 확인</div>
                    <h3>문구 점검</h3>
                    <ul class="inline-list">
                        ${toneAuditRows.map((row) => `<li>${escapeHtml(`${row.term}: ${row.status === 'pass' ? '사용 안 함' : '확인 필요'}`)}</li>`).join('')}
                    </ul>
                </article>
                <article class="grid-card">
                    <div class="section-kicker">운영 체크</div>
                    <h3>반영 상태</h3>
                    <ul class="inline-list">
                        ${validationRows.map((row) => `<li>${escapeHtml(sanitizeOperatingCopy(row.message))}</li>`).join('')}
                    </ul>
                </article>
            </div>
        </details>
        <details class="detail-disclosure">
            <summary>반영 이력</summary>
            <div class="detail-disclosure-body">
                ${iterationRows.length ? `
                    <div class="history-list">
                        ${iterationRows.map((row) => `
                            <article class="history-item">
                                <strong>${escapeHtml(`${row.iteration}차 반영`)}</strong>
                                <p>${escapeHtml(sanitizeOperatingCopy(row.change_applied))}</p>
                            </article>
                        `).join('')}
                    </div>
                ` : '<div class="empty-state">표시할 반영 이력이 없습니다.</div>'}
            </div>
        </details>
    `;
}

function render() {
    const root = document.querySelector('#app-root');
    if (!state.bundle) {
        root.innerHTML = '<div class="empty-state">데이터를 불러오는 중입니다.</div>';
        return;
    }

    setTitle(state.view);
    document.querySelector('#as-of-date').textContent = state.bundle.latest_as_of_date || '-';
    document.querySelector('#data-state').textContent = state.bundle.raw_data_status === 'real_source_loaded'
        ? (getViewModel('vm_priority_queue').length ? '운영 가능' : '산출물 없음')
        : '데이터 확인 필요';

    const rendererMap = {
        priority: renderPriorityView,
        segments: renderSegmentsView,
        detail: renderDetailView,
        definitions: renderDefinitionsView,
        health: renderHealthView
    };

    root.innerHTML = (rendererMap[state.view] ?? renderPriorityView)();
    bindImageFallbacks(root);

    root.querySelectorAll('[data-product-pick]').forEach((button) => {
        button.addEventListener('click', () => {
            state.selectedProductId = button.dataset.productPick;
            render();
        });
    });
}

function resolveAppBase() {
    const path = window.location.pathname;
    if (path.endsWith('/index.html')) {
        return `${path.slice(0, -'index.html'.length)}`;
    }
    return path.endsWith('/') ? path : `${path}/`;
}

function resolveArtifactUrl(layer, filename) {
    return new URL(`.${resolveAppBase()}artifacts/${layer}/${filename}`, window.location.origin).toString();
}

async function fetchTextOrThrow(url) {
    const response = await fetch(url, { cache: 'no-store' });
    if (!response.ok) {
        throw new Error(`${response.status} ${response.statusText}`);
    }
    return response.text();
}

async function fetchLayerFileText(layer, candidates) {
    const filenames = Array.isArray(candidates) ? candidates : [candidates];
    let lastError = null;

    for (const filename of filenames) {
        try {
            const text = await fetchTextOrThrow(resolveArtifactUrl(layer, filename));
            return { filename, text };
        } catch (error) {
            lastError = error;
        }
    }

    throw lastError ?? new Error('파일을 불러오지 못했습니다.');
}

async function loadLayerCsv(layer, files) {
    const entries = await Promise.all(
        Object.entries(files).map(async ([key, candidates]) => {
            const { text } = await fetchLayerFileText(layer, candidates);
            if ((Array.isArray(candidates) ? candidates[0] : candidates).endsWith('.md')) {
                return [key, text];
            }
            return [key, parseCsv(text)];
        })
    );

    return Object.fromEntries(entries);
}

function deriveRawDataStatus(bundle) {
    const rows = bundle?.view_model?.vm_data_health ?? [];
    return rows.some((row) => Number(row.row_count ?? 0) > 0) ? 'real_source_loaded' : 'raw_source_missing';
}

function shouldPreferArtifactBundle() {
    const { pathname, port } = window.location;

    if (pathname.startsWith('/apps/pgm_ops2/')) {
        return true;
    }

    if (port === '8000' && pathname.includes('/pgm_ops2')) {
        return true;
    }

    return false;
}

async function loadBundleFromArtifacts() {
    const [viewModel, mart, qa] = await Promise.all([
        loadLayerCsv('view_model', VIEW_MODEL_FILES),
        loadLayerCsv('mart', MART_FILES),
        loadLayerCsv('qa', QA_FILES)
    ]);

    const queueRows = viewModel.vm_priority_queue ?? [];

    return {
        app: 'pgm_ops2',
        generated_at: new Date().toISOString(),
        latest_as_of_date: queueRows[0]?.as_of_date ?? '',
        queue_counts: {},
        raw_data_status: 'raw_source_missing',
        view_model: viewModel,
        mart,
        qa
    };
}

async function loadBundleWithFallback() {
    if (shouldPreferArtifactBundle()) {
        const bundle = await loadBundleFromArtifacts();
        bundle.raw_data_status = deriveRawDataStatus(bundle);
        return bundle;
    }

    try {
        const response = await fetch('/api/pgm-ops2/bundle', { cache: 'no-store' });
        if (response.ok) {
            const bundle = await response.json();
            bundle.raw_data_status = bundle.raw_data_status ?? deriveRawDataStatus(bundle);
            return bundle;
        }
    } catch {
        // 정적 서빙으로 자연스럽게 내려간다.
    }

    const bundle = await loadBundleFromArtifacts();
    bundle.raw_data_status = deriveRawDataStatus(bundle);
    return bundle;
}

async function loadBundle() {
    state.bundle = await loadBundleWithFallback();
    render();
}

function bindImageFallbacks(root) {
    root.querySelectorAll('[data-product-thumb]').forEach((thumb) => {
        const image = thumb.querySelector('[data-product-image]');
        if (!image) {
            return;
        }

        const altText = image.getAttribute('alt') || '상품 이미지';
        const showLoadedState = () => {
            thumb.classList.remove('is-placeholder');
            thumb.classList.add('is-loaded');
            thumb.removeAttribute('role');
            thumb.removeAttribute('aria-label');
        };
        const showFallbackState = () => {
            thumb.classList.remove('is-loaded');
            thumb.classList.add('is-placeholder');
            thumb.setAttribute('role', 'img');
            thumb.setAttribute('aria-label', altText);
            image.removeAttribute('src');
        };

        if (image.complete) {
            if (image.naturalWidth > 0) {
                showLoadedState();
            } else {
                showFallbackState();
            }
            return;
        }

        image.addEventListener('load', showLoadedState, { once: true });
        image.addEventListener('error', showFallbackState, { once: true });
    });
}

function bindNav() {
    document.querySelectorAll('[data-view]').forEach((button) => {
        button.addEventListener('click', () => {
            document.querySelectorAll('[data-view]').forEach((item) => item.classList.remove('is-active'));
            button.classList.add('is-active');
            state.view = button.dataset.view;
            render();
        });
    });
}

bindNav();
loadBundle().catch((error) => {
    document.querySelector('#app-root').innerHTML = `<div class="empty-state">로딩 실패: ${escapeHtml(error.message)}</div>`;
});
