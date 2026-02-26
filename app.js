// APF Dashboard App Logic

// --- Configurations ---
const DB_CONFIG = {
    name: 'APF_Dashboard_DB',
    version: 1,
    store: 'csv_files'
};

const REQUIRED_FILES = {
    brandScore: { key: 'brand_score', filename: 'brand_score.csv' },
    anchorScored: { key: 'anchor_scored', filename: 'anchor_scored.csv' },
    anchorTransition: { key: 'anchor_transition', filename: 'anchor_transition.csv' },
    cartAnchor: { key: 'cart_anchor', filename: 'cart_anchor.csv' },
    cartAnchorDetail: { key: 'cart_anchor_detail', filename: 'cart_anchor_detail.csv' },
    aaCohortJourney: { key: 'aa_cohort_journey', filename: 'aa_cohort_journey.csv' },
    aaTransitionPath: { key: 'aa_transition_path', filename: 'aa_transition_path.csv' },
    caProfile: { key: 'ca_profile', filename: 'ca_profile.csv' },
    biiWindow: { key: 'bii_window', filename: 'bii_window.csv' },
    apfActionRules: { key: 'apf_action_rules', filename: 'apf_action_rules.csv' }
};

// --- App State ---
const AppState = {
    data: {
        brandScore: null,
        anchorScored: null,
        anchorTransition: null,
        cartAnchor: null,
        cartAnchorDetail: [],
        aaCohortJourney: [],
        aaTransitionPath: [],
        caProfile: [],
        biiWindow: [],
        apfActionRules: []
    },
    pagination: {
        cartDetail: {
            currentPage: 1,
            rowsPerPage: 20,
            totalRows: 0
        }
    },
    viewState: {
        products: { sortCol: 'revenue_90d', sortDesc: true, searchQuery: '' },
        transitions: { sortCol: 'transition_customer_cnt', sortDesc: true, searchQuery: '' },
        cart: { sortCol: 'co_order_cnt', sortDesc: true, searchQuery: '' },
        insights: {
            dateFrom: '',
            dateTo: '',
            aaType: 'ALL',
            aaProductId: 'ALL',
            windowDays: 90
        }
    },
    charts: {},
    helpers: {}
};

// --- IndexedDB Wrapper ---
const DB = {
    open: () => {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(DB_CONFIG.name, DB_CONFIG.version);
            request.onupgradeneeded = (e) => {
                const db = e.target.result;
                if (!db.objectStoreNames.contains(DB_CONFIG.store)) db.createObjectStore(DB_CONFIG.store);
            };
            request.onsuccess = (e) => resolve(e.target.result);
            request.onerror = (e) => reject(e.target.error);
        });
    },
    save: async (key, data) => {
        const db = await DB.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(DB_CONFIG.store, 'readwrite');
            tx.objectStore(DB_CONFIG.store).put(data, key);
            tx.oncomplete = () => resolve();
            tx.onerror = (e) => reject(e.target.error);
        });
    },
    get: async (key) => {
        const db = await DB.open();
        return new Promise((resolve, reject) => {
            const request = db.transaction(DB_CONFIG.store, 'readonly').objectStore(DB_CONFIG.store).get(key);
            request.onsuccess = () => resolve(request.result);
            request.onerror = (e) => reject(e.target.error);
        });
    },
    getAllKeys: async () => {
        const db = await DB.open();
        return new Promise((resolve, reject) => {
            const request = db.transaction(DB_CONFIG.store, 'readonly').objectStore(DB_CONFIG.store).getAllKeys();
            request.onsuccess = () => resolve(request.result);
            request.onerror = (e) => reject(e.target.error);
        });
    }
};

// --- Utilities ---
const loadDataFromDB = async (fileConfig) => {
    const data = await DB.get(fileConfig.key);
    if (!data) throw new Error(`필수 데이터가 없습니다: ${fileConfig.filename}`);
    return data;
};

const loadOptionalDataFromDB = async (fileConfig, fallback = []) => {
    try {
        const data = await DB.get(fileConfig.key);
        return data || fallback;
    } catch (_) {
        return fallback;
    }
};

const toNumber = (value, fallback = 0) => {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
};

const toDate = (value) => {
    if (!value) return null;
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date;
};

const formatNumber = (num, decimals = 0) => {
    if (num === null || num === undefined || Number.isNaN(num)) return '-';
    return new Intl.NumberFormat('en-US', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    }).format(num);
};

const formatPercent = (value, decimals = 1) => {
    if (value === null || value === undefined || Number.isNaN(value)) return '-';
    return `${(toNumber(value) * 100).toFixed(decimals)}%`;
};

const weightedAverage = (rows, valueKey, weightKey) => {
    let valueSum = 0;
    let weightSum = 0;
    rows.forEach((row) => {
        const weight = toNumber(row[weightKey], 0);
        const value = toNumber(row[valueKey], NaN);
        if (weight > 0 && Number.isFinite(value)) {
            valueSum += value * weight;
            weightSum += weight;
        }
    });
    return weightSum > 0 ? valueSum / weightSum : null;
};

const sumBy = (rows, key) => rows.reduce((acc, row) => acc + toNumber(row[key], 0), 0);

const escapeHtml = (value) => String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');

const withFallback = (value, fallback = '-') => {
    if (value === null || value === undefined || value === '') return fallback;
    return value;
};

const TERM_LABELS = {
    AA: '첫구매 유입상품',
    PCA: '재구매 시작상품',
    CA: '장바구니 확장상품',
    BHI: '브랜드 구조 건강도',
    BII: '브랜드 실전 체력'
};

const AA_TYPE_LABELS = {
    BROAD: '첫구매 많음',
    QUALIFIED: '재구매 가능성 높음',
    HEAVY: '고객 가치 높음'
};

const PCA_TYPE_LABELS = {
    CORE: '단골의 시작점',
    DEEP: '계속 찾는 상품',
    SCALE: '효자 상품'
};

const CA_TYPE_LABELS = {
    CORE: '장바구니 중심형',
    PAIR: '함께 담는 조합형',
    SET: '세트 확장형',
    NONE: '신호 없음'
};

const STAGE_LABELS = {
    REINFORCING: '강화',
    STRENGTHENING: '강화',
    STABLE: '안정',
    WARNING: '경고',
    WEAK: '약화',
    WEAKENING: '약화',
    RISK: '주의'
};

const UI_TERM_REPLACEMENTS = [
    [/AA-Broad/gi, `${TERM_LABELS.AA}-${AA_TYPE_LABELS.BROAD}`],
    [/AA-Qualified/gi, `${TERM_LABELS.AA}-${AA_TYPE_LABELS.QUALIFIED}`],
    [/AA-Heavy/gi, `${TERM_LABELS.AA}-${AA_TYPE_LABELS.HEAVY}`],
    [/PCA-Core/gi, `${TERM_LABELS.PCA}-${PCA_TYPE_LABELS.CORE}`],
    [/PCA-Deep/gi, `${TERM_LABELS.PCA}-${PCA_TYPE_LABELS.DEEP}`],
    [/PCA-Scale/gi, `${TERM_LABELS.PCA}-${PCA_TYPE_LABELS.SCALE}`],
    [/CA-Pair/gi, `${TERM_LABELS.CA}-${CA_TYPE_LABELS.PAIR}`],
    [/CA-Set/gi, `${TERM_LABELS.CA}-${CA_TYPE_LABELS.SET}`],
    [/BII\s*90\/365/gi, '90일 체력 대비 연간 체력'],
    [/Brand Fitness/gi, '브랜드 체력 현황'],
    [/Action Center/gi, '실행 카드'],
    [/\bBII\b/g, TERM_LABELS.BII],
    [/\bBHI\b/g, TERM_LABELS.BHI],
    [/\bPCA\b/g, TERM_LABELS.PCA],
    [/\bAA\b/g, TERM_LABELS.AA],
    [/\bCA\b/g, TERM_LABELS.CA],
    [/\bTransition\b/gi, '전환 흐름'],
    [/\bJourney\b/gi, '고객 흐름'],
    [/\bFitness\b/gi, '체력 현황']
];

const toAaTypeLabel = (value) => {
    const key = String(withFallback(value, '')).trim();
    if (!key) return '-';
    return AA_TYPE_LABELS[key.toUpperCase()] || key;
};

const toPcaTypeLabel = (value) => {
    const key = String(withFallback(value, '')).trim();
    if (!key) return '-';
    return PCA_TYPE_LABELS[key.toUpperCase()] || key;
};

const toCaTypeLabel = (value) => {
    const key = String(withFallback(value, 'NONE')).trim();
    if (!key) return '-';
    return CA_TYPE_LABELS[key.toUpperCase()] || key;
};

const toStageLabel = (value) => {
    const key = String(withFallback(value, '')).trim();
    if (!key) return '-';
    return STAGE_LABELS[key.toUpperCase()] || key;
};

const replaceUiTerm = (value) => {
    if (value === null || value === undefined) return '';
    let text = String(value);
    UI_TERM_REPLACEMENTS.forEach(([pattern, replacement]) => {
        text = text.replace(pattern, replacement);
    });
    return text;
};

const truncateText = (value, maxLen = 24) => {
    const text = String(value ?? '');
    if (text.length <= maxLen) return text;
    return `${text.slice(0, maxLen - 1)}…`;
};

const renderProductCell = (name, id, maxLen = 24) => {
    const full = `${name} (${id})`;
    return `
        <span class="name-ellipsis" title="${escapeHtml(full)}">${escapeHtml(truncateText(name, maxLen))}</span>
        <div class="sub-id">${escapeHtml(id)}</div>
    `;
};

function renderSearchUI(viewName, placeholder) {
    const query = AppState.viewState[viewName].searchQuery;
    return `
        <div class="search-container animate-fade-in">
            <div class="search-wrapper">
                <i class="ph ph-magnifying-glass"></i>
                <input type="text" class="search-input" placeholder="${placeholder}" 
                       value="${query}" oninput="handleGlobalSearch('${viewName}', this.value)">
            </div>
        </div>
    `;
}

function destroyCarts() {
    Object.values(AppState.charts).forEach((chart) => chart.destroy());
    AppState.charts = {};
}

function buildProductNameMap() {
    const products = AppState.data.anchorScored || [];
    const map = new Map();
    products.forEach((p) => {
        const id = p.product_id || p.Product_ID || p['\ufeffproduct_id'];
        const name = p.product_name_latest || p.Product_Name || p.product_name;
        if (id) map.set(String(id).trim(), name || String(id));
    });
    return map;
}

function getProductName(id) {
    if (!id) return '-';
    if (!AppState.helpers.productNameMap) {
        AppState.helpers.productNameMap = buildProductNameMap();
    }
    return AppState.helpers.productNameMap.get(String(id).trim()) || id;
}

function applyDateFilter(rows, key, fromValue, toValue) {
    if (!rows || rows.length === 0) return [];
    if (!fromValue && !toValue) return rows;

    const fromDate = toDate(fromValue);
    const toDateValue = toDate(toValue);

    return rows.filter((row) => {
        const date = toDate(row[key]);
        if (!date) return false;
        if (fromDate && date < fromDate) return false;
        if (toDateValue && date > toDateValue) return false;
        return true;
    });
}

function renderMissingSection(title, desc) {
    return `
        <section class="insight-section card animate-fade-in">
            <h3>${title}</h3>
            <p class="empty-state">${desc}</p>
        </section>
    `;
}

function initAppUI() {
    let modal = document.getElementById('related-products-modal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'related-products-modal';
        modal.className = 'modal-overlay';
        modal.innerHTML = `
            <div class="modal-card">
                <div class="modal-header">
                    <h3 class="modal-title">연관 상품</h3>
                    <button class="modal-close" onclick="closeRelatedModal()">&times;</button>
                </div>
                <div class="modal-body"></div>
            </div>
        `;
        document.body.appendChild(modal);
        modal.onclick = (e) => { if (e.target === modal) closeRelatedModal(); };
    }
}

window.handleGlobalSearch = (viewName, query) => {
    AppState.viewState[viewName].searchQuery = query;
    if (viewName === 'cart') AppState.pagination.cartDetail.currentPage = 1;

    if (window.searchTimeout) clearTimeout(window.searchTimeout);
    window.searchTimeout = setTimeout(() => {
        if (viewName === 'products') renderProducts();
        else if (viewName === 'transitions') renderTransitions();
        else if (viewName === 'cart') renderCartDetailTable();
    }, 150);
};

window.closeRelatedModal = () => {
    const modal = document.getElementById('related-products-modal');
    if (modal) modal.classList.remove('active');
};

window.copyToClipboard = (text) => {
    navigator.clipboard.writeText(String(text)).then(() => {
        const toast = document.createElement('div');
        toast.innerText = `복사됨: ${text}`;
        toast.style = 'position:fixed; bottom:20px; right:20px; background:var(--primary); color:white; padding:10px 20px; border-radius:8px; z-index:10000; box-shadow:0 4px 12px rgba(0,0,0,0.5);';
        document.body.appendChild(toast);
        setTimeout(() => toast.remove(), 2000);
    });
};

async function showRelatedProducts(productId) {
    initAppUI();
    const modal = document.getElementById('related-products-modal');
    modal.classList.add('active');

    const body = modal.querySelector('.modal-body');
    const title = modal.querySelector('.modal-title');
    const pName = getProductName(productId);

    title.innerText = `함께 구매된 상품: ${pName}`;
    body.innerHTML = '<div class="modal-loading"><div class="spinner"></div><p style="margin-top:1rem">동시구매 데이터를 조회하는 중...</p></div>';

    try {
        if (!AppState.data.cartAnchorDetail || AppState.data.cartAnchorDetail.length === 0) {
            AppState.data.cartAnchorDetail = await loadDataFromDB(REQUIRED_FILES.cartAnchorDetail);
        }

        const qId = String(productId).toLowerCase();
        const related = AppState.data.cartAnchorDetail.filter((d) =>
            String(d.i).toLowerCase() === qId || String(d.j).toLowerCase() === qId
        );

        if (related.length === 0) {
            body.innerHTML = '<p style="text-align:center; padding:4rem; color:var(--text-muted);">해당 상품의 동시구매 데이터가 없습니다.</p>';
            return;
        }

        related.sort((a, b) => toNumber(b.co_order_cnt) - toNumber(a.co_order_cnt));

        const rows = related.slice(0, 30).map((row) => {
            const otherId = String(row.i).toLowerCase() === qId ? row.j : row.i;
            return `
                <tr>
                    <td><span style="color:var(--text-muted); font-size:0.8rem">${escapeHtml(otherId)}</span></td>
                    <td style="font-weight:500">${escapeHtml(getProductName(otherId))}</td>
                    <td style="text-align:right; font-family:var(--font-heading); font-weight:600; color:var(--primary)">${formatNumber(row.co_order_cnt)}</td>
                </tr>
            `;
        }).join('');

        body.innerHTML = `
            <div class="animate-fade-in">
                <table class="mini-table">
                    <thead><tr><th>ID</th><th>상품명</th><th style="text-align:right">동시구매 수</th></tr></thead>
                    <tbody>${rows}</tbody>
                </table>
                <p style="font-size:0.8rem; color:var(--text-muted); margin-top:1.5rem; text-align:center">상위 30개만 표시</p>
            </div>
        `;
    } catch (e) {
        body.innerHTML = `<p style="color:var(--accent); text-align:center; padding:3rem;">데이터 로딩 실패: ${escapeHtml(e.message)}</p>`;
    }
}

// --- Legacy Pages (Overview/Products/Transitions/Cart) ---

function renderOverview() {
    destroyCarts();
    const container = document.getElementById('content-area');
    const data = AppState.data.brandScore ? AppState.data.brandScore[0] : {};

    const bhiRaw = data.Brand_Health_Index || data.BHI || data.brand_health_index || data.Brand_Health_Score;
    const bhi = (bhiRaw !== undefined && bhiRaw !== null && !Number.isNaN(toNumber(bhiRaw, NaN)))
        ? (toNumber(bhiRaw) * 100).toFixed(2)
        : '-';

    const concentration = data.AA_Concentration_Index ? `${(toNumber(data.AA_Concentration_Index) * 100).toFixed(1)}%` : '-';
    const balance = data.Chain_Balance_Index ? formatNumber(data.Chain_Balance_Index, 2) : '-';
    const confidence = data.Confidence_Index || '-';

    container.innerHTML = `
        <div class="animate-fade-in" style="margin-bottom: 2rem;">
            <div class="card" style="text-align: center; background: linear-gradient(135deg, white 0%, var(--primary-light) 100%); border: 1px solid var(--primary); border-width: 2px;">
                <h3 style="color: var(--primary); font-size: 1rem; margin-bottom: 0.5rem; font-weight: 700;">브랜드 구조 건강도</h3>
                <div class="value" style="font-size: 4.5rem; color: var(--primary);">${bhi}</div>
            </div>
        </div>

        <div class="stats-grid animate-fade-in">
            <div class="card">
                <h3>축 1: 첫구매 유입 집중도</h3>
                <div class="value">${concentration}</div>
            </div>
            <div class="card">
                <h3>축 2: 재구매 사슬 균형</h3>
                <div class="value">${balance}</div>
            </div>
            <div class="card">
                <h3>축 3: 신뢰도</h3>
                <div class="value" style="color: var(--accent);">${confidence}</div>
            </div>
        </div>

        <div class="card animate-fade-in" style="margin-top: 2rem; border-left: 4px solid var(--primary);">
            <h3 style="color: var(--primary); text-transform: none; letter-spacing: normal; font-size: 1.1rem;">화면 해석 가이드</h3>
            <p style="color: var(--text-muted); margin-top: 1rem; line-height: 1.6; font-size: 0.95rem;">
                브랜드 구조 건강도는 유입-재구매-가치의 균형을 요약합니다. 매출 규모보다 구조 균형이 유지되는지 먼저 확인하세요.
            </p>
        </div>
    `;
}

function renderProducts() {
    destroyCarts();
    const container = document.getElementById('content-area');
    const data = AppState.data.anchorScored || [];
    const { sortCol, sortDesc, searchQuery } = AppState.viewState.products;

    let filteredData = [...data];
    if (searchQuery) {
        const q = searchQuery.toLowerCase();
        filteredData = data.filter((d) =>
            String(d.product_id).toLowerCase().includes(q) ||
            (d.product_name_latest && d.product_name_latest.toLowerCase().includes(q))
        );
    }

    const sortedData = filteredData.sort((a, b) => {
        let valA = a[sortCol];
        let valB = b[sortCol];
        if (valA === undefined || valA === null) valA = 0;
        if (valB === undefined || valB === null) valB = 0;
        if (valA < valB) return sortDesc ? 1 : -1;
        if (valA > valB) return sortDesc ? -1 : 1;
        return 0;
    });

    const displayData = sortedData.slice(0, 50);
    const top10 = sortedData.slice(0, 10);
    const chartLabels = top10.map((d) => `${(d.product_name_latest || d.product_id || '').substring(0, 15)}...`);
    const chartData = top10.map((d) => toNumber(d[sortCol]));

    const getSortIndicator = (col) => sortCol === col ? (sortDesc ? ' ▼' : ' ▲') : '';
    const sortLabelMap = {
        product_id: '상품 ID',
        product_name_latest: '상품명',
        revenue_90d: '90일 매출',
        first_customer_cnt: '첫구매 고객수',
        AA_Score: '첫구매 유입점수',
        AA_Primary_Type: '첫구매 유입유형',
        PCA_Score: '재구매 시작점수',
        PCA_Primary_Type: '재구매 시작유형'
    };
    const sortLabel = sortLabelMap[sortCol] || sortCol;

    const rows = displayData.map((row) => `
        <tr class="clickable" onclick="showRelatedProducts('${escapeHtml(row.product_id)}')">
            <td>
                <div style="display:flex; align-items:center; gap:0.5rem;">
                    <span>${escapeHtml(row.product_id)}</span>
                    <button class="btn-icon" style="width:24px; height:24px; font-size:0.8rem; border:none; background:var(--primary-light); color:var(--primary);" 
                            onclick="event.stopPropagation(); copyToClipboard('${escapeHtml(row.product_id)}')">
                        <i class="ph ph-copy"></i>
                    </button>
                </div>
            </td>
            <td title="${escapeHtml(row.product_name_latest)}">${escapeHtml(row.product_name_latest || '-')}</td>
            <td>${formatNumber(row.revenue_90d)}</td>
            <td>${formatNumber(row.first_customer_cnt)}</td>
            <td>${formatNumber(row.AA_Score, 4)}</td>
            <td><span class="badge">${escapeHtml(toAaTypeLabel(row.AA_Primary_Type || '-'))}</span></td>
            <td>${formatNumber(row.PCA_Score, 4)}</td>
            <td><span class="badge" style="background: rgba(236, 72, 153, 0.2); color: #f472b6;">${escapeHtml(toPcaTypeLabel(row.PCA_Primary_Type || '-'))}</span></td>
        </tr>
    `).join('');

    container.innerHTML = `
        ${renderSearchUI('products', '상품 ID 또는 이름 검색')}
        <div class="controls-area animate-fade-in" style="margin-bottom:2rem;"><div class="card" style="height:400px;"><canvas id="productsChart"></canvas></div></div>
        <div class="card animate-fade-in"><h3>상위 50개 핵심 상품 (정렬 기준: ${escapeHtml(sortLabel)})</h3>
            <div class="table-container">
                <table class="data-table">
                    <thead><tr>
                        <th onclick="handleProductSort('product_id')">ID${getSortIndicator('product_id')}</th>
                        <th onclick="handleProductSort('product_name_latest')">상품명${getSortIndicator('product_name_latest')}</th>
                        <th onclick="handleProductSort('revenue_90d')">90일 매출${getSortIndicator('revenue_90d')}</th>
                        <th onclick="handleProductSort('first_customer_cnt')">첫구매 고객수${getSortIndicator('first_customer_cnt')}</th>
                        <th onclick="handleProductSort('AA_Score')">첫구매 유입점수${getSortIndicator('AA_Score')}</th>
                        <th onclick="handleProductSort('AA_Primary_Type')">첫구매 유입유형${getSortIndicator('AA_Primary_Type')}</th>
                        <th onclick="handleProductSort('PCA_Score')">재구매 시작점수${getSortIndicator('PCA_Score')}</th>
                        <th onclick="handleProductSort('PCA_Primary_Type')">재구매 시작유형${getSortIndicator('PCA_Primary_Type')}</th>
                    </tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        </div>
    `;

    const ctx = document.getElementById('productsChart').getContext('2d');
    AppState.charts.products = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: chartLabels,
            datasets: [{
                label: sortCol,
                data: chartData,
                backgroundColor: 'rgba(99, 102, 241, 0.6)',
                borderColor: 'rgba(99, 102, 241, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: { display: true, text: `정렬 기준 상위 10개 상품: ${sortLabel}`, color: '#1e293b' }
            },
            scales: {
                y: { ticks: { color: '#64748b' }, grid: { color: 'rgba(0,0,0,0.05)' } },
                x: { ticks: { color: '#64748b' } }
            }
        }
    });

    window.handleProductSort = (col) => {
        if (AppState.viewState.products.sortCol === col) AppState.viewState.products.sortDesc = !AppState.viewState.products.sortDesc;
        else {
            AppState.viewState.products.sortCol = col;
            AppState.viewState.products.sortDesc = true;
        }
        renderProducts();
    };
}

function renderTransitions() {
    destroyCarts();
    const container = document.getElementById('content-area');
    const transitions = AppState.data.anchorTransition || [];
    const { sortCol, sortDesc, searchQuery } = AppState.viewState.transitions;

    const getName = (id) => getProductName(id);

    let filteredData = [...transitions];
    if (searchQuery) {
        const q = searchQuery.toLowerCase();
        filteredData = transitions.filter((d) => {
            const fromId = String(d.aa_product_id).toLowerCase();
            const fromName = getName(d.aa_product_id).toLowerCase();
            return fromId.includes(q) || fromName.includes(q);
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
        aa_product_id: '유입 상품',
        pca_product_id: '재구매 시작 상품',
        transition_customer_cnt: '전환 고객수',
        avg_days_to_pca: '평균 전환 소요일',
        transition_rate: '전환율'
    };
    const sortLabel = sortLabelMap[sortCol] || sortCol;

    const rows = displayData.map((row) => `
        <tr>
            <td><div>${escapeHtml(getName(row.aa_product_id))}</div><div style="font-size:0.8em;color:var(--text-muted);cursor:pointer;" onclick="copyToClipboard('${escapeHtml(row.aa_product_id)}')">${escapeHtml(row.aa_product_id)} <i class="ph ph-copy"></i></div></td>
            <td><div>${escapeHtml(getName(row.pca_product_id))}</div><div style="font-size:0.8em;color:var(--text-muted);cursor:pointer;" onclick="copyToClipboard('${escapeHtml(row.pca_product_id)}')">${escapeHtml(row.pca_product_id)} <i class="ph ph-copy"></i></div></td>
            <td>${formatNumber(row.transition_customer_cnt)}</td>
            <td>${formatNumber(row.avg_days_to_pca, 1)}</td>
            <td>${formatPercent(row.transition_rate, 2)}</td>
        </tr>
    `).join('');

    container.innerHTML = `
        ${renderSearchUI('transitions', '첫구매 유입상품 기준 검색')}
        <div class="card animate-fade-in"><h3>상위 100개 전환 흐름 (정렬 기준: ${escapeHtml(sortLabel)})</h3>
            <div class="table-container">
                <table class="data-table">
                    <thead><tr>
                        <th onclick="handleTransitionSort('aa_product_id')">유입 상품${getSortIndicator('aa_product_id')}</th>
                        <th onclick="handleTransitionSort('pca_product_id')">재구매 시작 상품${getSortIndicator('pca_product_id')}</th>
                        <th onclick="handleTransitionSort('transition_customer_cnt')">전환 고객수${getSortIndicator('transition_customer_cnt')}</th>
                        <th onclick="handleTransitionSort('avg_days_to_pca')">평균 전환 소요일${getSortIndicator('avg_days_to_pca')}</th>
                        <th onclick="handleTransitionSort('transition_rate')">전환율${getSortIndicator('transition_rate')}</th>
                    </tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        </div>
    `;

    window.handleTransitionSort = (col) => {
        if (AppState.viewState.transitions.sortCol === col) AppState.viewState.transitions.sortDesc = !AppState.viewState.transitions.sortDesc;
        else {
            AppState.viewState.transitions.sortCol = col;
            AppState.viewState.transitions.sortDesc = true;
        }
        renderTransitions();
    };
}

function renderCartAnalysis() {
    destroyCarts();
    const container = document.getElementById('content-area');
    const cartData = AppState.data.cartAnchor || [];

    AppState.helpers.productNameMap = buildProductNameMap();
    const getName = (id) => getProductName(id);
    AppState.helpers.getName = getName;

    const sortedCart = [...cartData]
        .sort((a, b) => toNumber(b.median_cart_size) - toNumber(a.median_cart_size))
        .slice(0, 10);

    const chartLabels = sortedCart.map((d) => {
        const n = getName(d.product_id);
        return n.length > 15 ? `${n.substring(0, 15)}...` : n;
    });
    const chartData = sortedCart.map((d) => toNumber(d.median_cart_size));

    container.innerHTML = `
        ${renderSearchUI('cart', '상품 A/B 검색')}
        <div class="stats-grid animate-fade-in"><div class="card" style="grid-column: span 2;"><canvas id="cartChart" style="height:300px;"></canvas></div></div>
        <div class="card animate-fade-in" style="margin-top:2rem;">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:1rem;">
                <h3>장바구니 동시구매 상세</h3><div id="pagination-info" style="color:var(--text-muted); font-size:0.9rem;"></div>
            </div>
            <div id="cart-detail-container" class="table-container"></div>
            <div class="pagination-controls"><button id="prevBtn" class="btn-primary" disabled>이전</button><button id="nextBtn" class="btn-primary" disabled>다음</button></div>
        </div>
    `;

    const ctx = document.getElementById('cartChart').getContext('2d');
    AppState.charts.cart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: chartLabels,
            datasets: [{
                label: '중간 장바구니 크기',
                data: chartData,
                backgroundColor: 'rgba(236, 72, 153, 0.6)',
                borderColor: 'rgba(236, 72, 153, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: { display: true, text: '중간 장바구니 크기 상위 10개 상품', color: '#1e293b' }
            },
            scales: {
                y: { ticks: { color: '#64748b' }, grid: { color: 'rgba(0,0,0,0.05)' } },
                x: { ticks: { color: '#64748b' } }
            }
        }
    });

    if (AppState.data.cartAnchorDetail && AppState.data.cartAnchorDetail.length > 0) renderCartDetailTable();
    else loadDetailData();
}

async function loadDetailData() {
    try {
        const data = await loadDataFromDB(REQUIRED_FILES.cartAnchorDetail);
        const deduplicated = data.filter((row) => String(row.i) < String(row.j));
        AppState.data.cartAnchorDetail = deduplicated;
        AppState.pagination.cartDetail.totalRows = deduplicated.length;
        renderCartDetailTable();
    } catch (_) {
        document.getElementById('cart-detail-container').innerHTML = '<p style="color:var(--text-muted); text-align:center; padding:2rem;">상세 데이터가 없습니다. <button class="btn-primary" style="font-size:0.8rem" onclick="showUploadModal()">CSV 업로드</button></p>';
    }
}

function renderCartDetailTable() {
    const { currentPage, rowsPerPage } = AppState.pagination.cartDetail;
    const { sortCol, sortDesc, searchQuery } = AppState.viewState.cart;
    const getName = AppState.helpers.getName;

    let data = [...(AppState.data.cartAnchorDetail || [])];
    if (searchQuery) {
        const q = searchQuery.toLowerCase();
        data = data.filter((d) => {
            const idI = String(d.i).toLowerCase();
            const idJ = String(d.j).toLowerCase();
            const nameI = String(getName(d.i)).toLowerCase();
            const nameJ = String(getName(d.j)).toLowerCase();
            return idI.includes(q) || idJ.includes(q) || nameI.includes(q) || nameJ.includes(q);
        });
    }

    const totalRows = data.length;

    data.sort((a, b) => {
        let valA = a[sortCol];
        let valB = b[sortCol];
        if (sortCol === 'i' || sortCol === 'j') {
            valA = getName(valA);
            valB = getName(valB);
        }
        if (valA === undefined || valA === null) valA = 0;
        if (valB === undefined || valB === null) valB = 0;
        if (valA < valB) return sortDesc ? 1 : -1;
        if (valA > valB) return sortDesc ? -1 : 1;
        return 0;
    });

    const pData = data.slice((currentPage - 1) * rowsPerPage, currentPage * rowsPerPage);
    const getSortIndicator = (col) => sortCol === col ? (sortDesc ? ' ▼' : ' ▲') : '';

    const rows = pData.map((row) => `
        <tr>
            <td><div>${escapeHtml(getName(row.i))}</div><div style="font-size:0.8em;color:var(--text-muted);cursor:pointer;" onclick="copyToClipboard('${escapeHtml(row.i)}')">${escapeHtml(row.i)} <i class="ph ph-copy"></i></div></td>
            <td><div>${escapeHtml(getName(row.j))}</div><div style="font-size:0.8em;color:var(--text-muted);cursor:pointer;" onclick="copyToClipboard('${escapeHtml(row.j)}')">${escapeHtml(row.j)} <i class="ph ph-copy"></i></div></td>
            <td>${formatNumber(row.co_order_cnt)}</td>
        </tr>
    `).join('');

    document.getElementById('cart-detail-container').innerHTML = `
        <table class="data-table">
            <thead><tr>
                <th onclick="handleCartSort('i')">상품 A${getSortIndicator('i')}</th>
                <th onclick="handleCartSort('j')">상품 B${getSortIndicator('j')}</th>
                <th onclick="handleCartSort('co_order_cnt')">동시구매 수${getSortIndicator('co_order_cnt')}</th>
            </tr></thead>
            <tbody>${rows}</tbody>
        </table>
    `;

    document.getElementById('pagination-info').innerText = `${currentPage} / ${Math.max(1, Math.ceil(totalRows / rowsPerPage))} 페이지`;
    document.getElementById('prevBtn').disabled = currentPage === 1;
    document.getElementById('nextBtn').disabled = currentPage * rowsPerPage >= totalRows;
    document.getElementById('prevBtn').onclick = () => {
        AppState.pagination.cartDetail.currentPage -= 1;
        renderCartDetailTable();
    };
    document.getElementById('nextBtn').onclick = () => {
        AppState.pagination.cartDetail.currentPage += 1;
        renderCartDetailTable();
    };

    window.handleCartSort = (col) => {
        if (AppState.viewState.cart.sortCol === col) AppState.viewState.cart.sortDesc = !AppState.viewState.cart.sortDesc;
        else {
            AppState.viewState.cart.sortCol = col;
            AppState.viewState.cart.sortDesc = true;
        }
        AppState.pagination.cartDetail.currentPage = 1;
        renderCartDetailTable();
    };
}

// --- Insight Studio ---

function buildInsightsModel() {
    const filters = AppState.viewState.insights;

    const aaRowsAll = applyDateFilter(AppState.data.aaCohortJourney || [], 'cohort_date', filters.dateFrom, filters.dateTo)
        .filter((row) => filters.aaType === 'ALL' || String(row.aa_type || '').toLowerCase() === String(filters.aaType).toLowerCase())
        .filter((row) => filters.aaProductId === 'ALL' || String(row.aa_product_id) === String(filters.aaProductId));

    const transitionRowsAll = applyDateFilter(AppState.data.aaTransitionPath || [], 'cohort_date', filters.dateFrom, filters.dateTo)
        .filter((row) => filters.aaType === 'ALL' || String(row.aa_type || '').toLowerCase() === String(filters.aaType).toLowerCase())
        .filter((row) => filters.aaProductId === 'ALL' || String(row.aa_product_id) === String(filters.aaProductId));

    const caRows = AppState.data.caProfile || [];

    const biiRowsAll = applyDateFilter(AppState.data.biiWindow || [], 'as_of_date', filters.dateFrom, filters.dateTo);
    const biiMap = new Map();
    biiRowsAll.forEach((row) => {
        const window = toNumber(row.window_days, null);
        if (window !== null) biiMap.set(window, row);
    });

    const cohortCustomers = sumBy(aaRowsAll, 'cohort_customers');
    const repeat7 = weightedAverage(aaRowsAll, 'repeat_7d_rate', 'cohort_customers');
    const repeat30 = weightedAverage(aaRowsAll, 'repeat_30d_rate', 'cohort_customers');
    const repeat90 = weightedAverage(aaRowsAll, 'repeat_90d_rate', 'cohort_customers');
    const pca30 = weightedAverage(aaRowsAll, 'pca_transition_30d_rate', 'cohort_customers');
    const pca90 = weightedAverage(aaRowsAll, 'pca_transition_90d_rate', 'cohort_customers');
    const avgDaysToPca = weightedAverage(aaRowsAll, 'avg_days_to_pca', 'cohort_customers');
    const avgRevenue90d = weightedAverage(aaRowsAll, 'avg_revenue_90d', 'cohort_customers');

    const aaTypeAggMap = new Map();
    aaRowsAll.forEach((row) => {
        const key = String(row.aa_type || '미분류');
        if (!aaTypeAggMap.has(key)) {
            aaTypeAggMap.set(key, {
                aa_type: key,
                cohort_customers: 0,
                repeat_90d_num: 0,
                pca_90d_num: 0,
                revenue_90d_num: 0,
                avg_days_num: 0,
                avg_days_den: 0
            });
        }
        const target = aaTypeAggMap.get(key);
        const c = toNumber(row.cohort_customers, 0);
        target.cohort_customers += c;
        target.repeat_90d_num += toNumber(row.repeat_90d_rate, 0) * c;
        target.pca_90d_num += toNumber(row.pca_transition_90d_rate, 0) * c;
        target.revenue_90d_num += toNumber(row.avg_revenue_90d, 0) * c;
        const dayValue = toNumber(row.avg_days_to_pca, NaN);
        if (Number.isFinite(dayValue)) {
            target.avg_days_num += dayValue * c;
            target.avg_days_den += c;
        }
    });

    const aaTypeAgg = Array.from(aaTypeAggMap.values()).map((row) => ({
        aa_type: row.aa_type,
        cohort_customers: row.cohort_customers,
        repeat_90d_rate: row.cohort_customers > 0 ? row.repeat_90d_num / row.cohort_customers : null,
        pca_transition_90d_rate: row.cohort_customers > 0 ? row.pca_90d_num / row.cohort_customers : null,
        avg_revenue_90d: row.cohort_customers > 0 ? row.revenue_90d_num / row.cohort_customers : null,
        avg_days_to_pca: row.avg_days_den > 0 ? row.avg_days_num / row.avg_days_den : null
    })).sort((a, b) => b.cohort_customers - a.cohort_customers);

    const transitionRowsSorted = [...transitionRowsAll]
        .sort((a, b) => toNumber(b.transition_customers) - toNumber(a.transition_customers));
    const topTransitionRows = transitionRowsSorted.slice(0, 15);
    const totalTransitions = sumBy(transitionRowsSorted, 'transition_customers');
    const top3Transitions = sumBy(transitionRowsSorted.slice(0, 3), 'transition_customers');
    const top3TransitionShare = totalTransitions > 0 ? top3Transitions / totalTransitions : null;

    const aaCohortMap = new Map();
    aaRowsAll.forEach((row) => {
        aaCohortMap.set(`${row.cohort_date}::${row.aa_product_id}`, toNumber(row.cohort_customers, 0));
    });
    let mismatchCount = 0;
    transitionRowsAll.forEach((row) => {
        const cohort = aaCohortMap.get(`${row.cohort_date}::${row.aa_product_id}`);
        if (cohort && cohort > 0) {
            const expected = toNumber(row.transition_customers, 0) / cohort;
            const actual = toNumber(row.transition_rate, expected);
            if (Math.abs(expected - actual) > 0.02) mismatchCount += 1;
        }
    });

    const monotonicBreakCount = aaRowsAll.filter((row) =>
        toNumber(row.repeat_7d_rate, 0) > toNumber(row.repeat_30d_rate, 0) + 0.0001 ||
        toNumber(row.repeat_30d_rate, 0) > toNumber(row.repeat_90d_rate, 0) + 0.0001
    ).length;

    const caTypeCounts = {};
    caRows.forEach((row) => {
        const type = String(row.ca_type || 'None');
        caTypeCounts[type] = (caTypeCounts[type] || 0) + 1;
    });

    const selectedCa = (filters.aaProductId && filters.aaProductId !== 'ALL')
        ? caRows.find((row) => String(row.product_id) === String(filters.aaProductId))
        : null;

    const bii365 = biiMap.get(365);
    const bii90 = biiMap.get(90);
    const selectedWindowBii = biiMap.get(toNumber(filters.windowDays, 90));
    const bhiRow = AppState.data.brandScore && AppState.data.brandScore[0] ? AppState.data.brandScore[0] : null;

    const metrics = {
        aa_broad_ratio: (() => {
            const broad = aaTypeAgg.find((row) => String(row.aa_type).toLowerCase() === 'broad');
            return cohortCustomers > 0 && broad ? broad.cohort_customers / cohortCustomers : 0;
        })(),
        pca_transition_90d_rate: pca90 || 0,
        avg_days_to_pca: avgDaysToPca || 0,
        transition_top3_share: top3TransitionShare || 0,
        ca_pair_top1_share_max: Math.max(0, ...caRows
            .filter((row) => String(row.ca_type || '').toLowerCase() === 'pair')
            .map((row) => toNumber(row.top1_share, 0))),
        ca_set_breadth_lift_avg: (() => {
            const sets = caRows.filter((row) => String(row.ca_type || '').toLowerCase() === 'set');
            if (sets.length === 0) return 0;
            return sets.reduce((acc, row) => acc + toNumber(row.breadth_lift, 0), 0) / sets.length;
        })(),
        pca_scale_concentration: (() => {
            const rows = (AppState.data.anchorScored || []).filter((row) => String(row.PCA_Primary_Type || row.pca_primary_type || '').toLowerCase() === 'scale');
            const total = rows.reduce((acc, row) => acc + toNumber(row.revenue_90d, 0), 0);
            if (total <= 0 || rows.length === 0) return 0;
            const top = rows
                .map((row) => toNumber(row.revenue_90d, 0))
                .sort((a, b) => b - a)[0];
            return top / total;
        })()
    };

    return {
        filters,
        aaRowsAll,
        transitionRowsAll,
        aaTypeAgg,
        topTransitionRows,
        caRows,
        caTypeCounts,
        selectedCa,
        biiRowsAll,
        biiMap,
        summaries: {
            cohortCustomers,
            repeat7,
            repeat30,
            repeat90,
            pca30,
            pca90,
            avgDaysToPca,
            avgRevenue90d,
            top3TransitionShare,
            mismatchCount,
            monotonicBreakCount,
            bii365: bii365 ? toNumber(bii365.bii, null) : null,
            bii90: bii90 ? toNumber(bii90.bii, null) : null,
            selectedWindowBii: selectedWindowBii ? toNumber(selectedWindowBii.bii, null) : null,
            bhi: bhiRow ? toNumber(bhiRow.Brand_Health_Index || bhiRow.BHI || bhiRow.brand_health_index || bhiRow.Brand_Health_Score, null) : null,
            confidence: (bii90 && bii90.confidence) || (bhiRow && bhiRow.Confidence_Index) || '-'
        },
        metrics,
        brandRow: bhiRow
    };
}

function getInsightWarnings(model) {
    const warnings = [];
    if (model.summaries.monotonicBreakCount > 0) {
        warnings.push(`재구매율 구간 역전 데이터 ${model.summaries.monotonicBreakCount}건 감지`);
    }
    if (model.summaries.mismatchCount > 0) {
        warnings.push(`전이율-전이고객수 불일치 ${model.summaries.mismatchCount}건 감지`);
    }
    if ((model.summaries.pca90 || 0) < 0.2 && model.summaries.cohortCustomers > 0) {
        warnings.push('90일 재구매 시작 도달률이 낮아 첫구매 이후 이탈 위험이 있습니다');
    }
    if ((model.metrics.ca_pair_top1_share_max || 0) > 0.7) {
        warnings.push('장바구니 조합형 집중도가 높아 특정 조합 의존 리스크가 있습니다');
    }
    return warnings.slice(0, 3);
}

function evaluateConditionExpr(expr, metrics) {
    if (!expr || !String(expr).trim()) return true;
    const clauses = String(expr).split('&&').map((s) => s.trim()).filter(Boolean);
    if (clauses.length === 0) return true;

    return clauses.every((clause) => {
        const matched = clause.match(/^([a-zA-Z0-9_]+)\s*(>=|<=|>|<|==)\s*([0-9.]+)$/);
        if (!matched) return false;
        const [, key, op, rawTarget] = matched;
        const value = toNumber(metrics[key], NaN);
        const target = toNumber(rawTarget, NaN);
        if (!Number.isFinite(value) || !Number.isFinite(target)) return false;
        if (op === '>=') return value >= target;
        if (op === '<=') return value <= target;
        if (op === '>') return value > target;
        if (op === '<') return value < target;
        return value === target;
    });
}

function getBuiltInActionCards(model) {
    const m = model.metrics;
    const cards = [];

    if (m.aa_broad_ratio > 0.5 && m.pca_transition_90d_rate < 0.25) {
        cards.push({
            domain: 'marketing',
            priority: 1,
            title: '대량 유입형 상품의 재구매 시작 연결 강화',
            action: '첫구매 후 7일 이내 단골의 시작점 상품으로 이어지도록 CRM/리타게팅을 우선 배치합니다.',
            impact: '재구매 시작 도달률 개선 및 유입 낭비 축소',
            evidence: `${TERM_LABELS.AA}-${AA_TYPE_LABELS.BROAD} 비중 ${formatPercent(m.aa_broad_ratio, 1)} / 90일 재구매 시작 도달률 ${formatPercent(m.pca_transition_90d_rate, 1)}`
        });
    }

    if (m.transition_top3_share > 0.65) {
        cards.push({
            domain: 'marketing',
            priority: 2,
            title: '전이 경로 과집중 완화 실험',
            action: '상위 재구매 시작상품 편중 경로를 유지하되 대체 상품 노출 A/B 테스트를 병행합니다.',
            impact: '경로 리스크 분산 및 안정적 확장',
            evidence: `전이 상위 3경로 비중 ${formatPercent(m.transition_top3_share, 1)}`
        });
    }

    if (m.avg_days_to_pca > 18) {
        cards.push({
            domain: 'marketing',
            priority: 1,
            title: '재구매 시작 도달 속도 개선',
            action: '첫구매 후 메시지 발화 시점을 앞당기고, 3~7일 구간 혜택을 강화합니다.',
            impact: '평균 전이 소요일 단축',
            evidence: `평균 days_to_pca ${formatNumber(m.avg_days_to_pca, 1)}일`
        });
    }

    if (m.ca_pair_top1_share_max > 0.7) {
        cards.push({
            domain: 'md',
            priority: 1,
            title: '장바구니 조합형 고정 번들 운영',
            action: '상위 조합 상품을 고정 번들로 구성하고 교차추천 슬롯을 상단에 고정합니다.',
            impact: '장바구니 확장률 향상',
            evidence: `최대 top1_share ${formatPercent(m.ca_pair_top1_share_max, 1)}`
        });
    }

    if (m.ca_set_breadth_lift_avg > 1.5) {
        cards.push({
            domain: 'md',
            priority: 2,
            title: '장바구니 세트형 랜딩 강화',
            action: '세트형 상품군을 랜딩/기획전으로 분리하고 구성 SKU 재고 안정성을 우선 확보합니다.',
            impact: 'AOV 상승 및 이탈 감소',
            evidence: `${TERM_LABELS.CA}-${CA_TYPE_LABELS.SET} 평균 카테고리 확장 지수 ${formatNumber(m.ca_set_breadth_lift_avg, 2)}`
        });
    }

    if (m.pca_scale_concentration > 0.55) {
        cards.push({
            domain: 'md',
            priority: 1,
            title: '효자 상품 재고 방어',
            action: '효자 상품군의 안전재고 기준을 상향하고 품절 알림 자동화를 적용합니다.',
            impact: '사슬 붕괴 리스크 완화',
            evidence: `${PCA_TYPE_LABELS.SCALE} 매출 집중도 ${formatPercent(m.pca_scale_concentration, 1)}`
        });
    }

    return cards;
}

function getCsvActionCards(model) {
    const rules = AppState.data.apfActionRules || [];
    if (!rules.length) return [];

    return rules
        .filter((rule) => evaluateConditionExpr(rule.condition_expr, model.metrics))
        .map((rule) => ({
            domain: String(rule.domain || 'marketing').toLowerCase() === 'md' ? 'md' : 'marketing',
            priority: toNumber(rule.priority, 2),
            title: replaceUiTerm(withFallback(rule.title_ko, '사용자 규칙 실행안')),
            action: replaceUiTerm(withFallback(rule.action_ko, '-')),
            impact: replaceUiTerm(withFallback(rule.impact_ko, '-')),
            evidence: replaceUiTerm(withFallback(rule.condition_expr, '조건식 없음'))
        }));
}

function renderHeroStory(model) {
    const warnings = getInsightWarnings(model);
    const selectedWindow = toNumber(model.filters.windowDays, 90);
    const selectedWindowRow = model.biiMap.get(selectedWindow);
    const ratio = (model.summaries.bii365 && model.summaries.bii90)
        ? model.summaries.bii90 / model.summaries.bii365
        : null;
    const selectedStage = selectedWindowRow ? toStageLabel(selectedWindowRow.stage) : '-';
    const confidence = (selectedWindowRow && withFallback(selectedWindowRow.confidence, null))
        || model.summaries.confidence
        || '-';

    let ratioStatus = '데이터 부족';
    if (ratio !== null) {
        if (ratio >= 1.15) ratioStatus = '구조적 강화';
        else if (ratio >= 0.95) ratioStatus = '안정';
        else if (ratio >= 0.85) ratioStatus = '경고';
        else ratioStatus = '구조 약화';
    }

    return `
        <section class="insight-section card animate-fade-in">
            <div class="section-headline">
                <h2>인사이트 스튜디오</h2>
                <p>첫구매 유입 이후 90일 전환 최적화를 한 화면에서 확인합니다</p>
            </div>
            <div class="hero-metrics">
                <div class="hero-metric">
                    <label>${TERM_LABELS.BII} ${selectedWindow}일</label>
                    <strong>${model.summaries.selectedWindowBii !== null ? formatNumber(model.summaries.selectedWindowBii, 3) : '-'}</strong>
                </div>
                <div class="hero-metric">
                    <label>90일 체력 대비 연간 체력</label>
                    <strong>${ratio !== null ? formatNumber(ratio, 2) : '-'}</strong>
                    <span>${ratioStatus}</span>
                </div>
                <div class="hero-metric">
                    <label>현재 단계 (${selectedWindow}일)</label>
                    <strong>${escapeHtml(String(selectedStage))}</strong>
                </div>
                <div class="hero-metric">
                    <label>신뢰도</label>
                    <strong>${escapeHtml(String(confidence))}</strong>
                </div>
            </div>
            <div class="warning-list">
                ${warnings.length ? warnings.map((w) => `<span class="warning-chip">${escapeHtml(w)}</span>`).join('') : '<span class="warning-chip neutral">경고 없음</span>'}
            </div>
            <p class="insight-note">메인 지표는 ${TERM_LABELS.BII} 중심으로 보여줍니다. ${TERM_LABELS.BHI}는 하단 참고값에서만 확인하세요.</p>
        </section>
    `;
}

function renderAAJourney(model) {
    if (!model.aaRowsAll.length) {
        return renderMissingSection('첫구매 고객 흐름', 'aa_cohort_journey.csv 데이터가 없어 첫구매 고객 흐름을 표시할 수 없습니다.');
    }

    const s = model.summaries;
    const first = s.cohortCustomers;
    const repeat7Customers = first * toNumber(s.repeat7, 0);
    const repeat30Customers = first * toNumber(s.repeat30, 0);
    const repeat90Customers = first * toNumber(s.repeat90, 0);

    const typeRows = model.aaTypeAgg.map((row) => `
        <tr>
            <td>${escapeHtml(toAaTypeLabel(row.aa_type))}</td>
            <td>${formatNumber(row.cohort_customers)}</td>
            <td>${formatPercent(row.repeat_90d_rate, 1)}</td>
            <td>${formatPercent(row.pca_transition_90d_rate, 1)}</td>
            <td>${formatNumber(row.avg_revenue_90d, 0)}</td>
            <td>${formatNumber(row.avg_days_to_pca, 1)}일</td>
        </tr>
    `).join('');

    return `
        <section id="aa-journey" class="insight-section card animate-fade-in">
            <div class="section-headline">
                <h2>첫구매 고객 흐름</h2>
                <p>첫구매 이후 7/30/90일 행동과 전환 속도</p>
            </div>
            <div class="journey-grid">
                <div class="journey-kpi">
                    <label>첫구매 고객수</label>
                    <strong>${formatNumber(first)}</strong>
                </div>
                <div class="journey-kpi">
                    <label>7일 재구매</label>
                    <strong>${formatNumber(repeat7Customers)}</strong>
                    <span>${formatPercent(s.repeat7, 1)}</span>
                </div>
                <div class="journey-kpi">
                    <label>30일 재구매</label>
                    <strong>${formatNumber(repeat30Customers)}</strong>
                    <span>${formatPercent(s.repeat30, 1)}</span>
                </div>
                <div class="journey-kpi">
                    <label>90일 재구매</label>
                    <strong>${formatNumber(repeat90Customers)}</strong>
                    <span>${formatPercent(s.repeat90, 1)}</span>
                </div>
                <div class="journey-kpi">
                    <label>90일 재구매 시작 도달률</label>
                    <strong>${formatPercent(s.pca90, 1)}</strong>
                </div>
                <div class="journey-kpi">
                    <label>재구매 시작까지 평균 일수</label>
                    <strong>${formatNumber(s.avgDaysToPca, 1)}일</strong>
                </div>
            </div>
            <div class="insight-chart-grid">
                <div class="card chart-card"><canvas id="aaJourneyChart"></canvas></div>
                <div class="card chart-card"><canvas id="aaTopProductChart"></canvas></div>
            </div>
            <div class="table-container" style="margin-top:1rem;">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>유입 유형</th>
                            <th>대상 고객수</th>
                            <th>90일 재구매율</th>
                            <th>90일 재구매 시작 도달률</th>
                            <th>90일 가치</th>
                            <th>평균 소요일</th>
                        </tr>
                    </thead>
                    <tbody>${typeRows}</tbody>
                </table>
            </div>
        </section>
    `;
}

function renderAATransition(model) {
    if (!model.transitionRowsAll.length) {
        return renderMissingSection('재구매 시작 전환 흐름', 'aa_transition_path.csv 데이터가 없어 전환 흐름을 표시할 수 없습니다.');
    }

    const rows = model.topTransitionRows.map((row) => `
        <tr>
            <td>${renderProductCell(getProductName(row.aa_product_id), row.aa_product_id, 30)}</td>
            <td>${renderProductCell(getProductName(row.pca_product_id), row.pca_product_id, 30)}</td>
            <td>${formatNumber(row.transition_customers)}</td>
            <td>${formatPercent(row.transition_rate, 1)}</td>
            <td>${formatNumber(row.avg_days_to_pca, 1)}일</td>
        </tr>
    `).join('');

    return `
        <section id="aa-transition" class="insight-section card animate-fade-in">
            <div class="section-headline">
                <h2>재구매 시작 전환 흐름</h2>
                <p>첫구매 유입상품별 재구매 시작상품 도달 구조와 속도</p>
            </div>
            <div class="journey-grid">
                <div class="journey-kpi">
                    <label>상위 3개 전이 집중도</label>
                    <strong>${formatPercent(model.summaries.top3TransitionShare, 1)}</strong>
                </div>
                <div class="journey-kpi">
                    <label>평균 90일 전이율</label>
                    <strong>${formatPercent(model.summaries.pca90, 1)}</strong>
                </div>
            </div>
            <p class="chart-hint">차트 라벨은 상품명 기준이며, 마우스를 올리면 전체 상품명과 ID를 확인할 수 있습니다.</p>
            <div class="card chart-card"><canvas id="transitionChart"></canvas></div>
            <div class="table-container" style="margin-top:1rem;">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>첫구매 유입상품</th>
                            <th>재구매 시작상품</th>
                            <th>전이 고객수</th>
                            <th>전이율</th>
                            <th>평균 소요일</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        </section>
    `;
}

function renderCASection(model) {
    if (!model.caRows.length) {
        return renderMissingSection('장바구니 확장 인사이트', 'ca_profile.csv 데이터가 없어 장바구니 확장 흐름을 표시할 수 없습니다.');
    }

    const topRows = [...model.caRows]
        .sort((a, b) => toNumber(b.attach_rate) - toNumber(a.attach_rate))
        .slice(0, 10)
        .map((row) => `
            <tr>
                <td>${renderProductCell(getProductName(row.product_id), row.product_id, 30)}</td>
                <td>${escapeHtml(toCaTypeLabel(withFallback(row.ca_type, 'None')))}</td>
                <td>${formatPercent(row.attach_rate, 1)}</td>
                <td>${formatNumber(row.median_cart_size, 2)}</td>
                <td>${formatNumber(row.breadth_lift, 2)}</td>
                <td>${formatPercent(row.top1_share, 1)}</td>
            </tr>
        `).join('');

    const selectedPanel = model.selectedCa
        ? `
        <div class="selected-ca-panel">
            <h4>선택한 유입상품 기준 장바구니 확장</h4>
            <p><strong title="${escapeHtml(getProductName(model.selectedCa.product_id))}">${escapeHtml(truncateText(getProductName(model.selectedCa.product_id), 42))}</strong> (${escapeHtml(model.selectedCa.product_id)})</p>
            <div class="selected-ca-grid">
                <span>확장 유형: ${escapeHtml(toCaTypeLabel(withFallback(model.selectedCa.ca_type, 'None')))}</span>
                <span>동반구매 비율: ${formatPercent(model.selectedCa.attach_rate, 1)}</span>
                <span>중간 장바구니 크기: ${formatNumber(model.selectedCa.median_cart_size, 2)}</span>
                <span>상위 1개 집중도: ${formatPercent(model.selectedCa.top1_share, 1)}</span>
            </div>
        </div>
        `
        : '<div class="selected-ca-panel"><h4>선택한 유입상품 기준 장바구니 확장</h4><p>유입상품 필터를 선택하면 해당 상품의 장바구니 확장 신호를 표시합니다.</p></div>';

    return `
        <section id="cart-ca" class="insight-section card animate-fade-in">
            <div class="section-headline">
                <h2>장바구니 확장 인사이트</h2>
                <p>장바구니 결합력과 동반구매 구조</p>
            </div>
            <div class="insight-chart-grid">
                <div class="card chart-card"><canvas id="caTypeChart"></canvas></div>
                <div class="card chart-card"><canvas id="caTopChart"></canvas></div>
            </div>
            ${selectedPanel}
            <div class="table-container" style="margin-top:1rem;">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>상품</th>
                            <th>확장 유형</th>
                            <th>동반구매 비율</th>
                            <th>중간 장바구니 크기</th>
                            <th>카테고리 확장 지수</th>
                            <th>상위 1개 집중도</th>
                        </tr>
                    </thead>
                    <tbody>${topRows}</tbody>
                </table>
            </div>
        </section>
    `;
}

function renderBrandFitness(model) {
    const brand = model.brandRow;
    const biiRows = [7, 30, 90, 365].map((window) => model.biiMap.get(window)).filter(Boolean);

    if (!biiRows.length) {
        return renderMissingSection('브랜드 체력 현황', 'bii_window.csv 데이터가 없어 브랜드 체력 지표를 표시할 수 없습니다.');
    }

    const selectedWindow = toNumber(model.filters.windowDays, 90);
    const selectedRow = model.biiMap.get(selectedWindow);
    const row90 = model.biiMap.get(90);
    const row365 = model.biiMap.get(365);
    const selectedWindowBii = selectedRow ? toNumber(selectedRow.bii, null) : model.summaries.selectedWindowBii;
    const bii90Value = row90 ? toNumber(row90.bii, null) : model.summaries.bii90;
    const bii365Value = row365 ? toNumber(row365.bii, null) : model.summaries.bii365;
    const stage90 = row90 ? toStageLabel(row90.stage) : '-';
    const confidence = (selectedRow && withFallback(selectedRow.confidence, null))
        || (row90 && withFallback(row90.confidence, null))
        || (brand && withFallback(brand.Confidence_Index, null))
        || '-';
    const ratio = (bii90Value !== null && bii365Value !== null && bii365Value !== 0)
        ? bii90Value / bii365Value
        : null;

    let ratioStatus = '데이터 부족';
    if (ratio !== null) {
        if (ratio >= 1.15) ratioStatus = '구조적 강화';
        else if (ratio >= 0.95) ratioStatus = '안정';
        else if (ratio >= 0.85) ratioStatus = '경고';
        else ratioStatus = '구조 약화';
    }

    const bhiValue = brand ? toNumber(brand.Brand_Health_Index || brand.BHI || brand.brand_health_index || brand.Brand_Health_Score, null) : null;
    const as = brand ? toNumber(brand.AA_Concentration_Index, null) : null;
    const cs = brand ? toNumber(brand.Chain_Balance_Index, null) : null;
    const bhiReferenceText = brand
        ? `참고 구조값: ${TERM_LABELS.BHI} ${bhiValue !== null ? formatNumber(bhiValue * 100, 2) : '-'} | 유입 집중도 ${as !== null ? formatNumber(as * 100, 1) : '-'}% | 재구매 사슬 균형 ${cs !== null ? formatNumber(cs, 2) : '-'}`
        : '참고 구조값: brand_score.csv 미업로드';

    const rows = biiRows.map((row) => `
        <tr>
            <td>${formatNumber(row.window_days, 0)}d</td>
            <td>${formatNumber(row.bii, 3)}</td>
            <td>${formatNumber(row.clv_norm, 3)}</td>
            <td>${formatNumber(row.customer_strength_norm, 3)}</td>
            <td>${escapeHtml(toStageLabel(row.stage))}</td>
        </tr>
    `).join('');

    return `
        <section id="brand-fitness" class="insight-section card animate-fade-in">
            <div class="section-headline">
                <h2>브랜드 체력 현황</h2>
                <p>브랜드 실전 체력의 7/30/90/365일 흐름과 90일 대비 연간 추세를 함께 봅니다</p>
            </div>
            <div class="journey-grid">
                <div class="journey-kpi"><label>${TERM_LABELS.BII} ${selectedWindow}일</label><strong>${selectedWindowBii !== null ? formatNumber(selectedWindowBii, 3) : '-'}</strong></div>
                <div class="journey-kpi"><label>${TERM_LABELS.BII} 90일</label><strong>${bii90Value !== null ? formatNumber(bii90Value, 3) : '-'}</strong></div>
                <div class="journey-kpi"><label>${TERM_LABELS.BII} 365일</label><strong>${bii365Value !== null ? formatNumber(bii365Value, 3) : '-'}</strong></div>
                <div class="journey-kpi"><label>90일 체력 대비 연간 체력</label><strong>${ratio !== null ? formatNumber(ratio, 2) : '-'}</strong><span>${ratioStatus}</span></div>
                <div class="journey-kpi"><label>현재 단계 (90일)</label><strong>${escapeHtml(String(stage90))}</strong></div>
                <div class="journey-kpi"><label>신뢰도</label><strong>${escapeHtml(String(confidence))}</strong></div>
            </div>
            <div class="card chart-card"><canvas id="biiChart"></canvas></div>
            <div class="table-container" style="margin-top:1rem;">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>분석 기간</th>
                            <th>${TERM_LABELS.BII}</th>
                            <th>고객가치 보정</th>
                            <th>고객강도 보정</th>
                            <th>단계</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
            <p class="insight-note">${escapeHtml(bhiReferenceText)}</p>
        </section>
    `;
}

function renderActionCenter(model) {
    const cards = [...getBuiltInActionCards(model), ...getCsvActionCards(model)]
        .sort((a, b) => toNumber(a.priority) - toNumber(b.priority));

    const marketing = cards.filter((card) => card.domain === 'marketing');
    const md = cards.filter((card) => card.domain === 'md');

    const renderCardList = (rows, emptyText) => {
        if (!rows.length) return `<p class="empty-state">${emptyText}</p>`;
        return rows.map((row) => `
            <article class="action-card p${toNumber(row.priority, 2)}">
                <header>
                    <span class="priority">P${toNumber(row.priority, 2)}</span>
                    <h4>${escapeHtml(row.title)}</h4>
                </header>
                <p><strong>근거 지표:</strong> ${escapeHtml(row.evidence)}</p>
                <p><strong>권장 액션:</strong> ${escapeHtml(row.action)}</p>
                <p><strong>예상 영향:</strong> ${escapeHtml(row.impact)}</p>
            </article>
        `).join('');
    };

    return `
        <section id="action-center" class="insight-section card animate-fade-in">
            <div class="section-headline">
                <h2>실행 카드</h2>
                <p>지표를 마케팅/MD 실행안으로 바로 연결합니다</p>
            </div>
            <div class="action-grid">
                <div>
                    <h3 class="action-title">마케팅 실행안</h3>
                    ${renderCardList(marketing, '현재 조건에 맞는 마케팅 실행안이 없습니다.')}
                </div>
                <div>
                    <h3 class="action-title">MD 실행안</h3>
                    ${renderCardList(md, '현재 조건에 맞는 MD 실행안이 없습니다.')}
                </div>
            </div>
        </section>
    `;
}

function renderInsightFilters(model) {
    const filters = model.filters;

    const aaTypes = ['ALL', ...new Set((AppState.data.aaCohortJourney || []).map((row) => String(row.aa_type || '').trim()).filter(Boolean))];
    const aaProducts = ['ALL', ...new Set((AppState.data.aaCohortJourney || []).map((row) => String(row.aa_product_id || '').trim()).filter(Boolean))];

    const aaTypeOptions = aaTypes.map((type) => {
        const selected = String(filters.aaType) === type ? 'selected' : '';
        return `<option value="${escapeHtml(type)}" ${selected}>${escapeHtml(type === 'ALL' ? '전체 유입 유형' : toAaTypeLabel(type))}</option>`;
    }).join('');

    const aaProductOptions = aaProducts.map((id) => {
        const selected = String(filters.aaProductId) === id ? 'selected' : '';
        const label = id === 'ALL' ? '전체 유입 상품' : `${getProductName(id)} (${id})`;
        return `<option value="${escapeHtml(id)}" ${selected}>${escapeHtml(label)}</option>`;
    }).join('');

    return `
        <section class="insight-section card animate-fade-in">
            <div class="filter-grid">
                <label>
                    <span>시작일</span>
                    <input type="date" value="${escapeHtml(filters.dateFrom)}" onchange="updateInsightsFilter('dateFrom', this.value)">
                </label>
                <label>
                    <span>종료일</span>
                    <input type="date" value="${escapeHtml(filters.dateTo)}" onchange="updateInsightsFilter('dateTo', this.value)">
                </label>
                <label>
                    <span>유입 유형</span>
                    <select onchange="updateInsightsFilter('aaType', this.value)">${aaTypeOptions}</select>
                </label>
                <label>
                    <span>유입 상품</span>
                    <select onchange="updateInsightsFilter('aaProductId', this.value)">${aaProductOptions}</select>
                </label>
                <label>
                    <span>기준 기간</span>
                    <select onchange="updateInsightsFilter('windowDays', this.value)">
                        <option value="7" ${toNumber(filters.windowDays) === 7 ? 'selected' : ''}>7일</option>
                        <option value="30" ${toNumber(filters.windowDays) === 30 ? 'selected' : ''}>30일</option>
                        <option value="90" ${toNumber(filters.windowDays) === 90 ? 'selected' : ''}>90일</option>
                        <option value="365" ${toNumber(filters.windowDays) === 365 ? 'selected' : ''}>365일</option>
                    </select>
                </label>
                <button class="btn-primary" type="button" onclick="resetInsightsFilters()">필터 초기화</button>
            </div>
        </section>
    `;
}

function renderAnchorJump() {
    return `
        <nav class="jump-nav card animate-fade-in">
            <a href="#aa-journey">첫구매 고객 흐름</a>
            <a href="#aa-transition">재구매 시작 전환</a>
            <a href="#cart-ca">장바구니 확장</a>
            <a href="#brand-fitness">브랜드 체력</a>
            <a href="#action-center">실행 카드</a>
        </nav>
    `;
}

function renderInsightsCharts(model) {
    const journeyCanvas = document.getElementById('aaJourneyChart');
    if (journeyCanvas) {
        const s = model.summaries;
        AppState.charts.aaJourney = new Chart(journeyCanvas.getContext('2d'), {
            type: 'line',
            data: {
                labels: ['7일', '30일', '90일'],
                datasets: [
                    {
                        label: '재구매율',
                        data: [toNumber(s.repeat7, 0) * 100, toNumber(s.repeat30, 0) * 100, toNumber(s.repeat90, 0) * 100],
                        borderColor: '#6366f1',
                        backgroundColor: 'rgba(99,102,241,0.2)',
                        tension: 0.3,
                        fill: true
                    },
                    {
                        label: '재구매 시작 도달률',
                        data: [toNumber(s.pca30, 0) * 100, toNumber(s.pca90, 0) * 100, toNumber(s.pca90, 0) * 100],
                        borderColor: '#ec4899',
                        backgroundColor: 'rgba(236,72,153,0.12)',
                        tension: 0.3,
                        fill: false
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: { callback: (v) => `${v}%` }
                    }
                }
            }
        });
    }

    const aaTopCanvas = document.getElementById('aaTopProductChart');
    if (aaTopCanvas && model.aaRowsAll.length) {
        const rows = [...model.aaRowsAll]
            .sort((a, b) => toNumber(b.cohort_customers, 0) - toNumber(a.cohort_customers, 0))
            .slice(0, 8)
            .map((row) => {
                const name = getProductName(row.aa_product_id);
                return {
                    shortLabel: truncateText(name, 18),
                    fullLabel: `${name} (${row.aa_product_id})`,
                    cohortCustomers: toNumber(row.cohort_customers, 0)
                };
            });

        AppState.charts.aaTopProduct = new Chart(aaTopCanvas.getContext('2d'), {
            type: 'bar',
            data: {
                labels: rows.map((row) => row.shortLabel),
                datasets: [{
                    label: '첫구매 유입 고객수',
                    data: rows.map((row) => row.cohortCustomers),
                    backgroundColor: 'rgba(16,185,129,0.6)',
                    borderColor: 'rgba(16,185,129,1)',
                    borderWidth: 1
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            title: (items) => rows[items[0].dataIndex].fullLabel
                        }
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        ticks: { precision: 0 }
                    },
                    y: {
                        ticks: {
                            autoSkip: false
                        }
                    }
                }
            }
        });
    }

    const transitionCanvas = document.getElementById('transitionChart');
    if (transitionCanvas && model.topTransitionRows.length) {
        const rows = model.topTransitionRows.slice(0, 8).map((row) => {
            const aaName = getProductName(row.aa_product_id);
            const pcaName = getProductName(row.pca_product_id);
            return {
                row,
                shortLabel: `${truncateText(aaName, 14)} → ${truncateText(pcaName, 14)}`,
                fullLabel: `${aaName} (${row.aa_product_id}) → ${pcaName} (${row.pca_product_id})`
            };
        });
        AppState.charts.transition = new Chart(transitionCanvas.getContext('2d'), {
            type: 'bar',
            data: {
                labels: rows.map((x) => x.shortLabel),
                datasets: [{
                    label: '전이 고객수',
                    data: rows.map((x) => toNumber(x.row.transition_customers, 0)),
                    backgroundColor: 'rgba(99,102,241,0.6)',
                    borderColor: 'rgba(99,102,241,1)',
                    borderWidth: 1
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            title: (items) => rows[items[0].dataIndex].fullLabel
                        }
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        ticks: {
                            precision: 0
                        }
                    },
                    y: {
                        ticks: {
                            autoSkip: false
                        }
                    }
                }
            }
        });
    }

    const caCanvas = document.getElementById('caTypeChart');
    if (caCanvas && Object.keys(model.caTypeCounts).length) {
        const labels = Object.keys(model.caTypeCounts).map((label) => toCaTypeLabel(label));
        const rawTypes = Object.keys(model.caTypeCounts);
        const counts = rawTypes.map((key) => model.caTypeCounts[key]);
        AppState.charts.caType = new Chart(caCanvas.getContext('2d'), {
            type: 'doughnut',
            data: {
                labels,
                datasets: [{
                    data: counts,
                    backgroundColor: ['#6366f1', '#ec4899', '#10b981', '#f59e0b', '#94a3b8']
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom'
                    }
                }
            }
        });
    }

    const caTopCanvas = document.getElementById('caTopChart');
    if (caTopCanvas && model.caRows.length) {
        const rows = [...model.caRows]
            .sort((a, b) => toNumber(b.attach_rate, 0) - toNumber(a.attach_rate, 0))
            .slice(0, 8)
            .map((row) => {
                const name = getProductName(row.product_id);
                return {
                    shortLabel: truncateText(name, 18),
                    fullLabel: `${name} (${row.product_id})`,
                    attachRate: toNumber(row.attach_rate, 0) * 100
                };
            });

        AppState.charts.caTop = new Chart(caTopCanvas.getContext('2d'), {
            type: 'bar',
            data: {
                labels: rows.map((row) => row.shortLabel),
                datasets: [{
                    label: '동반구매 비율(%)',
                    data: rows.map((row) => row.attachRate),
                    backgroundColor: 'rgba(236,72,153,0.55)',
                    borderColor: 'rgba(236,72,153,1)',
                    borderWidth: 1
                }]
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            title: (items) => rows[items[0].dataIndex].fullLabel,
                            label: (ctx) => `동반구매 비율: ${formatNumber(ctx.raw, 1)}%`
                        }
                    }
                },
                scales: {
                    x: {
                        beginAtZero: true,
                        ticks: {
                            callback: (v) => `${v}%`
                        }
                    },
                    y: {
                        ticks: {
                            autoSkip: false
                        }
                    }
                }
            }
        });
    }

    const biiCanvas = document.getElementById('biiChart');
    if (biiCanvas && model.biiRowsAll.length) {
        const rows = [...model.biiRowsAll].sort((a, b) => toNumber(a.window_days) - toNumber(b.window_days));
        AppState.charts.bii = new Chart(biiCanvas.getContext('2d'), {
            type: 'line',
            data: {
                labels: rows.map((row) => `${row.window_days}일`),
                datasets: [{
                    label: TERM_LABELS.BII,
                    data: rows.map((row) => toNumber(row.bii, 0)),
                    borderColor: '#0ea5e9',
                    backgroundColor: 'rgba(14,165,233,0.2)',
                    fill: true,
                    tension: 0.3
                }]
            },
            options: { responsive: true, maintainAspectRatio: false }
        });
    }
}

function renderInsightsPage() {
    destroyCarts();
    AppState.helpers.productNameMap = buildProductNameMap();
    const container = document.getElementById('content-area');
    const model = buildInsightsModel();

    container.innerHTML = `
        <div class="insights-page">
            ${renderHeroStory(model)}
            ${renderAnchorJump()}
            ${renderInsightFilters(model)}
            ${renderAAJourney(model)}
            ${renderAATransition(model)}
            ${renderCASection(model)}
            ${renderBrandFitness(model)}
            ${renderActionCenter(model)}
        </div>
    `;

    renderInsightsCharts(model);
}

window.updateInsightsFilter = (key, value) => {
    if (key === 'windowDays') AppState.viewState.insights[key] = toNumber(value, 90);
    else AppState.viewState.insights[key] = value;
    renderInsightsPage();
};

window.resetInsightsFilters = () => {
    AppState.viewState.insights = {
        dateFrom: '',
        dateTo: '',
        aaType: 'ALL',
        aaProductId: 'ALL',
        windowDays: 90
    };
    renderInsightsPage();
};

// --- Upload Logic ---

function showUploadModal() {
    if (document.getElementById('uploadModal')) document.getElementById('uploadModal').remove();
    document.body.insertAdjacentHTML('beforeend', `
        <div id="uploadModal" style="position:fixed; inset:0; background:rgba(0,0,0,0.8); z-index:9999; display:flex; align-items:center; justify-content:center;">
            <div class="card" style="width:560px; max-width:92%;">
                <div style="display:flex; justify-content:space-between; margin-bottom:1.5rem;"><h3>CSV 업로드</h3><button onclick="document.getElementById('uploadModal').remove()" style="background:none; border:none; color:white; cursor:pointer;"><i class="ph ph-x" style="font-size:1.5rem"></i></button></div>
                <div id="upload-status" style="margin-bottom:1rem; color:var(--text-muted)">여러 CSV를 동시에 선택할 수 있습니다. 파일명에 키를 포함하세요.</div>
                <input type="file" id="file-input" multiple accept=".csv" onchange="handleFiles(this.files)">
                <div style="margin-top:0.8rem; color:var(--text-muted); font-size:0.82rem; line-height:1.5;">
                    지원 키: ${Object.values(REQUIRED_FILES).map((f) => `<code>${f.key}</code>`).join(', ')}
                </div>
                <div id="file-list" style="margin-top:1rem; font-size:0.9rem;"></div>
            </div>
        </div>
    `);
}

window.handleFiles = async (files) => {
    const list = document.getElementById('file-list');
    list.innerHTML = '처리 중...';

    let count = 0;
    const matchedNames = [];

    for (const file of files) {
        const config = Object.values(REQUIRED_FILES)
            .sort((a, b) => b.key.length - a.key.length)
            .find((f) => file.name.toLowerCase().includes(f.key.toLowerCase()));

        if (config) {
            await new Promise((resolve) => {
                Papa.parse(file, {
                    header: true,
                    dynamicTyping: true,
                    skipEmptyLines: true,
                    complete: async (r) => {
                        await DB.save(config.key, r.data);
                        count += 1;
                        matchedNames.push(`${file.name} → ${config.key}`);
                        resolve();
                    }
                });
            });
        }
    }

    if (count > 0) {
        list.innerHTML = `<p style="color:var(--primary)">${count}개 파일 저장 완료. 새로고침합니다.</p><p style="margin-top:0.5rem; color:var(--text-muted)">${escapeHtml(matchedNames.join(' | '))}</p>`;
        setTimeout(() => location.reload(), 1500);
    } else {
        list.innerHTML = '<p style="color:var(--accent)">매칭되는 파일을 찾지 못했습니다. 파일명 키를 확인하세요.</p>';
    }
};

// --- Initialization ---

async function loadInsightsData() {
    const [brandScore, anchorScored, anchorTransition, cartAnchor, cartAnchorDetail, aaCohortJourney, aaTransitionPath, caProfile, biiWindow, apfActionRules] = await Promise.all([
        loadOptionalDataFromDB(REQUIRED_FILES.brandScore, []),
        loadOptionalDataFromDB(REQUIRED_FILES.anchorScored, []),
        loadOptionalDataFromDB(REQUIRED_FILES.anchorTransition, []),
        loadOptionalDataFromDB(REQUIRED_FILES.cartAnchor, []),
        loadOptionalDataFromDB(REQUIRED_FILES.cartAnchorDetail, []),
        loadOptionalDataFromDB(REQUIRED_FILES.aaCohortJourney, []),
        loadOptionalDataFromDB(REQUIRED_FILES.aaTransitionPath, []),
        loadOptionalDataFromDB(REQUIRED_FILES.caProfile, []),
        loadOptionalDataFromDB(REQUIRED_FILES.biiWindow, []),
        loadOptionalDataFromDB(REQUIRED_FILES.apfActionRules, [])
    ]);

    AppState.data.brandScore = brandScore;
    AppState.data.anchorScored = anchorScored;
    AppState.data.anchorTransition = anchorTransition;
    AppState.data.cartAnchor = cartAnchor;
    AppState.data.cartAnchorDetail = cartAnchorDetail;
    AppState.data.aaCohortJourney = aaCohortJourney;
    AppState.data.aaTransitionPath = aaTransitionPath;
    AppState.data.caProfile = caProfile;
    AppState.data.biiWindow = biiWindow;
    AppState.data.apfActionRules = apfActionRules;
}

async function init() {
    const pageId = document.body.id;
    initAppUI();

    const sidebar = document.querySelector('.user-profile');
    if (sidebar) sidebar.innerHTML = '<button class="btn-primary" style="width:100%" onclick="showUploadModal()"><i class="ph ph-upload-simple"></i> 데이터 업로드</button>';

    try {
        const keys = await DB.getAllKeys();
        if (keys.length === 0) {
            document.getElementById('content-area').innerHTML = '<div class="card animate-fade-in" style="text-align:center; padding:4rem;"><i class="ph ph-database" style="font-size:4rem; color:var(--text-muted); margin-bottom:1rem;"></i><h3>데이터 없음</h3><p style="color:var(--text-muted); margin-bottom:2rem;">CSV 파일을 업로드해 시작하세요.</p><button class="btn-primary" onclick="showUploadModal()">지금 업로드</button></div>';
            return;
        }

        if (pageId === 'page-insights') {
            await loadInsightsData();
            renderInsightsPage();
            return;
        }

        if (pageId === 'page-overview') {
            AppState.data.brandScore = await loadDataFromDB(REQUIRED_FILES.brandScore);
            renderOverview();
        } else if (pageId === 'page-products') {
            AppState.data.anchorScored = await loadDataFromDB(REQUIRED_FILES.anchorScored);
            AppState.helpers.productNameMap = buildProductNameMap();
            renderProducts();
        } else if (pageId === 'page-transitions') {
            const [t, s] = await Promise.all([
                loadDataFromDB(REQUIRED_FILES.anchorTransition),
                loadDataFromDB(REQUIRED_FILES.anchorScored)
            ]);
            AppState.data.anchorTransition = t;
            AppState.data.anchorScored = s;
            AppState.helpers.productNameMap = buildProductNameMap();
            renderTransitions();
        } else if (pageId === 'page-cart') {
            const [c, s] = await Promise.all([
                loadDataFromDB(REQUIRED_FILES.cartAnchor),
                loadDataFromDB(REQUIRED_FILES.anchorScored)
            ]);
            AppState.data.cartAnchor = c;
            AppState.data.anchorScored = s;
            AppState.helpers.productNameMap = buildProductNameMap();
            renderCartAnalysis();
        }
    } catch (e) {
        console.error(e);
        document.getElementById('content-area').innerHTML = `<div class="card" style="text-align:center; padding:2rem;"><h3>필수 데이터 누락</h3><p style="color:var(--accent)">${escapeHtml(e.message)}</p><button class="btn-primary" onclick="showUploadModal()">누락 파일 업로드</button></div>`;
    }
}

document.addEventListener('DOMContentLoaded', init);
