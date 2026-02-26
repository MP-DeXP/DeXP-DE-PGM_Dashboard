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
    cartAnchorDetail: { key: 'cart_anchor_detail', filename: 'cart_anchor_detail.csv' }
};

// --- App State ---
const AppState = {
    data: {
        brandScore: null,
        anchorScored: null,
        anchorTransition: null,
        cartAnchor: null,
        cartAnchorDetail: []
    },
    pagination: {
        cartDetail: {
            currentPage: 1,
            rowsPerPage: 20,
            totalRows: 0
        }
    },
    viewState: {
        products: { sortCol: 'revenue_90d', sortDesc: true, searchQuery: "" },
        transitions: { sortCol: 'transition_customer_cnt', sortDesc: true, searchQuery: "" },
        cart: { sortCol: 'co_order_cnt', sortDesc: true, searchQuery: "" }
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
    if (!data) throw new Error(`Data not found: ${fileConfig.filename}`);
    return data;
};

const formatNumber = (num, decimals = 0) => {
    if (num === null || num === undefined || isNaN(num)) return '-';
    return new Intl.NumberFormat('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals }).format(num);
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
    Object.values(AppState.charts).forEach(chart => chart.destroy());
    AppState.charts = {};
}

function initAppUI() {
    // Inject modal HTML once
    let modal = document.getElementById('related-products-modal');
    if (!modal) {
        modal = document.createElement('div');
        modal.id = 'related-products-modal';
        modal.className = 'modal-overlay';
        modal.innerHTML = `
            <div class="modal-card">
                <div class="modal-header">
                    <h3 class="modal-title">Related Products</h3>
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

    // Use a small timeout to avoid stuttering during rapid typing
    if (window.searchTimeout) clearTimeout(window.searchTimeout);
    window.searchTimeout = setTimeout(() => {
        if (viewName === 'products') renderProducts();
        else if (viewName === 'transitions') renderTransitions();
        else if (viewName === 'cart') renderCartDetailTable();
    }, 150);
};

function getProductName(id) {
    if (!id) return '-';
    // Use the product data if loaded
    const products = AppState.data.anchorScored;
    if (!products) return id;
    const p = products.find(prod => String(prod.product_id || prod.Product_ID) === String(id));
    return p ? (p.product_name_latest || p.Product_Name || p.product_name) : id;
}

async function showRelatedProducts(productId) {
    initAppUI(); // Ensure it exists
    const modal = document.getElementById('related-products-modal');
    modal.classList.add('active');

    const body = modal.querySelector('.modal-body');
    const title = modal.querySelector('.modal-title');
    const pName = getProductName(productId);

    title.innerText = `Frequently Bought With: ${pName}`;
    body.innerHTML = `<div class="modal-loading"><div class="spinner"></div><p style="margin-top:1rem">Searching for co-occurrence data...</p></div>`;

    try {
        if (!AppState.data.cartAnchorDetail || AppState.data.cartAnchorDetail.length === 0) {
            AppState.data.cartAnchorDetail = await loadDataFromDB(REQUIRED_FILES.cartAnchorDetail);
        }

        const qId = String(productId).toLowerCase();
        const related = AppState.data.cartAnchorDetail.filter(d =>
            String(d.i).toLowerCase() === qId || String(d.j).toLowerCase() === qId
        );

        if (related.length === 0) {
            body.innerHTML = `<p style="text-align:center; padding:4rem; color:var(--text-muted);">No co-occurrence data found for this product.</p>`;
            return;
        }

        related.sort((a, b) => b.co_order_cnt - a.co_order_cnt);

        let rows = related.slice(0, 30).map(row => {
            const otherId = String(row.i).toLowerCase() === qId ? row.j : row.i;
            return `
                <tr>
                    <td><span style="color:var(--text-muted); font-size:0.8rem">${otherId}</span></td>
                    <td style="font-weight:500">${getProductName(otherId)}</td>
                    <td style="text-align:right; font-family:var(--font-heading); font-weight:600; color:var(--primary)">${formatNumber(row.co_order_cnt)}</td>
                </tr>
            `;
        }).join('');

        body.innerHTML = `
            <div class="animate-fade-in">
                <table class="mini-table">
                    <thead><tr><th>ID</th><th>Product Name</th><th style="text-align:right">Co-Order Count</th></tr></thead>
                    <tbody>${rows}</tbody>
                </table>
                <p style="font-size:0.8rem; color:var(--text-muted); margin-top:1.5rem; text-align:center">Showing top 30 related items</p>
            </div>
        `;
    } catch (e) {
        body.innerHTML = `<p style="color:var(--accent); text-align:center; padding:3rem;">Error loading data: ${e.message}</p>`;
    }
}

window.closeRelatedModal = () => {
    const modal = document.getElementById('related-products-modal');
    if (modal) modal.classList.remove('active');
};

window.copyToClipboard = (text) => {
    navigator.clipboard.writeText(String(text)).then(() => {
        const toast = document.createElement('div');
        toast.innerText = `Copied: ${text}`;
        toast.style = "position:fixed; bottom:20px; right:20px; background:var(--primary); color:white; padding:10px 20px; border-radius:8px; z-index:10000; box-shadow:0 4px 12px rgba(0,0,0,0.5);";
        document.body.appendChild(toast);
        setTimeout(() => toast.remove(), 2000);
    });
};

// --- Rendering Functions ---

function renderOverview() {
    destroyCarts();
    const container = document.getElementById('content-area');
    const data = AppState.data.brandScore ? AppState.data.brandScore[0] : {};

    // BHI and 3 Axes (Try multiple key variations)
    let bhiRaw = data.Brand_Health_Index || data.BHI || data.brand_health_index || data.Brand_Health_Score;
    const bhi = (bhiRaw !== undefined && bhiRaw !== null && !isNaN(bhiRaw))
        ? (bhiRaw * 100).toFixed(2)
        : '-';

    const concentration = data.AA_Concentration_Index ? (data.AA_Concentration_Index * 100).toFixed(1) + '%' : '-';
    const balance = data.Chain_Balance_Index ? formatNumber(data.Chain_Balance_Index, 2) : '-';
    const confidence = data.Confidence_Index || '-';

    container.innerHTML = `
        <div class="animate-fade-in" style="margin-bottom: 2rem;">
            <div class="card" style="text-align: center; background: linear-gradient(135deg, white 0%, var(--primary-light) 100%); border: 1px solid var(--primary); border-width: 2px;">
                <h3 style="color: var(--primary); font-size: 1rem; margin-bottom: 0.5rem; font-weight: 700;">Brand Health Index (BHI)</h3>
                <div class="value" style="font-size: 4.5rem; color: var(--primary);">${bhi}</div>
            </div>
        </div>

        <div class="stats-grid animate-fade-in">
            <div class="card">
                <h3>Axis 1: AA Concentration</h3>
                <div class="value">${concentration}</div>
            </div>
            <div class="card">
                <h3>Axis 2: Chain Balance</h3>
                <div class="value">${balance}</div>
            </div>
            <div class="card">
                <h3>Axis 3: Confidence Index</h3>
                <div class="value" style="color: var(--accent);">${confidence}</div>
            </div>
        </div>
        
        <div class="card animate-fade-in" style="margin-top: 2rem; border-left: 4px solid var(--primary);">
            <h3 style="color: var(--primary); text-transform: none; letter-spacing: normal; font-size: 1.1rem;">Overview Guide</h3>
            <p style="color: var(--text-muted); margin-top: 1rem; line-height: 1.6; font-size: 0.95rem;">
                The <strong>Brand Health Index (BHI)</strong> is a comprehensive metric calculated from three core axes: 
                Concentration, Balance, and Confidence. High BHI indicates a strong and stable brand presence in the marketplace.
            </p>
        </div>
    `;
}

function renderProducts() {
    destroyCarts();
    const container = document.getElementById('content-area');
    const data = AppState.data.anchorScored || [];
    const { sortCol, sortDesc, searchQuery } = AppState.viewState.products;

    // Filter Data
    let filteredData = [...data];
    if (searchQuery) {
        const q = searchQuery.toLowerCase();
        filteredData = data.filter(d =>
            String(d.product_id).toLowerCase().includes(q) ||
            (d.product_name_latest && d.product_name_latest.toLowerCase().includes(q))
        );
    }

    // Sort Data
    const sortedData = filteredData.sort((a, b) => {
        let valA = a[sortCol], valB = b[sortCol];
        if (valA === undefined || valA === null) valA = 0;
        if (valB === undefined || valB === null) valB = 0;
        if (valA < valB) return sortDesc ? 1 : -1;
        if (valA > valB) return sortDesc ? -1 : 1;
        return 0;
    });

    const displayData = sortedData.slice(0, 50);
    const top10 = sortedData.slice(0, 10);
    const chartLabels = top10.map(d => (d.product_name_latest || d.product_id || '').substring(0, 15) + '...');
    const chartData = top10.map(d => d[sortCol]);

    const getSortIndicator = (col) => sortCol === col ? (sortDesc ? ' ▼' : ' ▲') : '';

    let rows = displayData.map(row => `
        <tr class="clickable" onclick="showRelatedProducts('${row.product_id}')">
            <td>
                <div style="display:flex; align-items:center; gap:0.5rem;">
                    <span>${row.product_id}</span>
                    <button class="btn-icon" style="width:24px; height:24px; font-size:0.8rem; border:none; background:var(--primary-light); color:var(--primary);" 
                            onclick="event.stopPropagation(); copyToClipboard('${row.product_id}')">
                        <i class="ph ph-copy"></i>
                    </button>
                </div>
            </td>
            <td title="${row.product_name_latest}">${row.product_name_latest || '-'}</td>
            <td>${formatNumber(row.revenue_90d)}</td>
            <td>${formatNumber(row.first_customer_cnt)}</td>
            <td>${formatNumber(row.AA_Score, 4)}</td>
            <td><span class="badge">${row.AA_Primary_Type || '-'}</span></td>
            <td>${formatNumber(row.PCA_Score, 4)}</td>
            <td><span class="badge" style="background: rgba(236, 72, 153, 0.2); color: #f472b6;">${row.PCA_Primary_Type || '-'}</span></td>
        </tr>
    `).join('');

    container.innerHTML = `
        ${renderSearchUI('products', 'Search by ID or Name...')}
        <div class="controls-area animate-fade-in" style="margin-bottom:2rem;"><div class="card" style="height:400px;"><canvas id="productsChart"></canvas></div></div>
        <div class="card animate-fade-in"><h3>Top 50 Anchor Products (Sorted by: ${sortCol})</h3>
            <div class="table-container">
                <table class="data-table">
                    <thead><tr>
                        <th onclick="handleProductSort('product_id')">ID${getSortIndicator('product_id')}</th>
                        <th onclick="handleProductSort('product_name_latest')">Product Name${getSortIndicator('product_name_latest')}</th>
                        <th onclick="handleProductSort('revenue_90d')">Revenue${getSortIndicator('revenue_90d')}</th>
                        <th onclick="handleProductSort('first_customer_cnt')">Customers${getSortIndicator('first_customer_cnt')}</th>
                        <th onclick="handleProductSort('AA_Score')">AA Score${getSortIndicator('AA_Score')}</th>
                        <th onclick="handleProductSort('AA_Primary_Type')">AA Type${getSortIndicator('AA_Primary_Type')}</th>
                        <th onclick="handleProductSort('PCA_Score')">PCA Score${getSortIndicator('PCA_Score')}</th>
                        <th onclick="handleProductSort('PCA_Primary_Type')">PCA Type${getSortIndicator('PCA_Primary_Type')}</th>
                    </tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        </div>
    `;

    const ctx = document.getElementById('productsChart').getContext('2d');
    AppState.charts.products = new Chart(ctx, {
        type: 'bar',
        data: { labels: chartLabels, datasets: [{ label: sortCol, data: chartData, backgroundColor: 'rgba(99, 102, 241, 0.6)', borderColor: 'rgba(99, 102, 241, 1)', borderWidth: 1 }] },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: { display: true, text: `Top 10 Products by ${sortCol}`, color: '#1e293b' }
            },
            scales: {
                y: {
                    ticks: { color: '#64748b' },
                    grid: { color: 'rgba(0,0,0,0.05)' }
                },
                x: {
                    ticks: { color: '#64748b' }
                }
            }
        }
    });

    window.handleProductSort = (col) => {
        if (AppState.viewState.products.sortCol === col) AppState.viewState.products.sortDesc = !AppState.viewState.products.sortDesc;
        else { AppState.viewState.products.sortCol = col; AppState.viewState.products.sortDesc = true; }
        renderProducts();
    };
}

function renderTransitions() {
    destroyCarts();
    const container = document.getElementById('content-area');
    const transitions = AppState.data.anchorTransition || [];
    const products = AppState.data.anchorScored || [];
    const { sortCol, sortDesc, searchQuery } = AppState.viewState.transitions;

    const productMap = new Map();
    products.forEach(p => {
        const id = p.product_id || p.Product_ID || p['\ufeffproduct_id'];
        const name = p.product_name_latest || p.Product_Name || p.product_name;
        if (id) productMap.set(String(id).trim(), name);
    });

    const getName = (id) => productMap.get(String(id).trim()) || id || '-';

    // Filter Data
    let filteredData = [...transitions];
    if (searchQuery) {
        const q = searchQuery.toLowerCase();
        filteredData = transitions.filter(d => {
            const fromId = String(d.aa_product_id).toLowerCase();
            const fromName = getName(d.aa_product_id).toLowerCase();
            return fromId.includes(q) || fromName.includes(q);
        });
    }

    const sortedData = filteredData.sort((a, b) => {
        let valA = a[sortCol], valB = b[sortCol];
        if (sortCol === 'aa_product_id' || sortCol === 'pca_product_id') { valA = getName(valA); valB = getName(valB); }
        if (valA === undefined || valA === null) valA = 0;
        if (valB === undefined || valB === null) valB = 0;
        if (valA < valB) return sortDesc ? 1 : -1;
        if (valA > valB) return sortDesc ? -1 : 1;
        return 0;
    });

    const displayData = sortedData.slice(0, 200);
    const getSortIndicator = (col) => sortCol === col ? (sortDesc ? ' ▼' : ' ▲') : '';

    let rows = displayData.map(row => `
        <tr>
            <td><div>${getName(row.aa_product_id)}</div><div style="font-size:0.8em;color:var(--text-muted);cursor:pointer;" onclick="copyToClipboard('${row.aa_product_id}')">${row.aa_product_id} <i class="ph ph-copy"></i></div></td>
            <td><div>${getName(row.pca_product_id)}</div><div style="font-size:0.8em;color:var(--text-muted);cursor:pointer;" onclick="copyToClipboard('${row.pca_product_id}')">${row.pca_product_id} <i class="ph ph-copy"></i></div></td>
            <td>${formatNumber(row.transition_customer_cnt)}</td>
            <td>${formatNumber(row.avg_days_to_pca, 1)}</td>
            <td>${(row.transition_rate * 100).toFixed(2)}%</td>
        </tr>
    `).join('');

    container.innerHTML = `
        ${renderSearchUI('transitions', 'Search From (Anchor) Product...')}
        <div class="card animate-fade-in"><h3>Top 100 Transitions (Sorted by: ${sortCol})</h3>
            <div class="table-container">
                <table class="data-table">
                    <thead><tr>
                        <th onclick="handleTransitionSort('aa_product_id')">From (Anchor)${getSortIndicator('aa_product_id')}</th>
                        <th onclick="handleTransitionSort('pca_product_id')">To (PCA)${getSortIndicator('pca_product_id')}</th>
                        <th onclick="handleTransitionSort('transition_customer_cnt')">Count${getSortIndicator('transition_customer_cnt')}</th>
                        <th onclick="handleTransitionSort('avg_days_to_pca')">Avg Days${getSortIndicator('avg_days_to_pca')}</th>
                        <th onclick="handleTransitionSort('transition_rate')">Rate${getSortIndicator('transition_rate')}</th>
                    </tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        </div>
    `;

    window.handleTransitionSort = (col) => {
        if (AppState.viewState.transitions.sortCol === col) AppState.viewState.transitions.sortDesc = !AppState.viewState.transitions.sortDesc;
        else { AppState.viewState.transitions.sortCol = col; AppState.viewState.transitions.sortDesc = true; }
        renderTransitions();
    };
}

function renderCartAnalysis() {
    destroyCarts();
    const container = document.getElementById('content-area');
    const cartData = AppState.data.cartAnchor || [];
    const products = AppState.data.anchorScored || [];

    const productMap = new Map();
    products.forEach(p => {
        const id = p.product_id || p.Product_ID || p['\ufeffproduct_id'];
        const name = p.product_name_latest || p.Product_Name || p.product_name;
        if (id) productMap.set(String(id).trim(), name);
    });

    const getName = (id) => productMap.get(String(id).trim()) || id || '-';
    AppState.helpers.getName = getName;

    const sortedCart = [...cartData].sort((a, b) => b.median_cart_size - a.median_cart_size).slice(0, 10);
    const chartLabels = sortedCart.map(d => { const n = getName(d.product_id); return n.length > 15 ? n.substring(0, 15) + '...' : n; });
    const chartData = sortedCart.map(d => d.median_cart_size);

    container.innerHTML = `
        ${renderSearchUI('cart', 'Search Product A or B...')}
        <div class="stats-grid animate-fade-in"><div class="card" style="grid-column: span 2;"><canvas id="cartChart" style="height:300px;"></canvas></div></div>
        <div class="card animate-fade-in" style="margin-top:2rem;">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:1rem;">
                <h3>Cart Detail Analysis</h3><div id="pagination-info" style="color:var(--text-muted); font-size:0.9rem;"></div>
            </div>
            <div id="cart-detail-container" class="table-container"></div>
            <div class="pagination-controls"><button id="prevBtn" class="btn-primary" disabled>Previous</button><button id="nextBtn" class="btn-primary" disabled>Next</button></div>
        </div>
    `;

    const ctx = document.getElementById('cartChart').getContext('2d');
    AppState.charts.cart = new Chart(ctx, {
        type: 'bar',
        data: { labels: chartLabels, datasets: [{ label: 'Median Cart Size', data: chartData, backgroundColor: 'rgba(236, 72, 153, 0.6)', borderColor: 'rgba(236, 72, 153, 1)', borderWidth: 1 }] },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                title: { display: true, text: 'Top 10 Products by Median Cart Size', color: '#1e293b' }
            },
            scales: {
                y: {
                    ticks: { color: '#64748b' },
                    grid: { color: 'rgba(0,0,0,0.05)' }
                },
                x: {
                    ticks: { color: '#64748b' }
                }
            }
        }
    });

    if (AppState.data.cartAnchorDetail && AppState.data.cartAnchorDetail.length > 0) renderCartDetailTable();
    else loadDetailData();
}

async function loadDetailData() {
    try {
        const data = await loadDataFromDB(REQUIRED_FILES.cartAnchorDetail);
        // Deduplicate: Only keep pairs where i < j lexicographically
        const deduplicated = data.filter(row => String(row.i) < String(row.j));
        AppState.data.cartAnchorDetail = deduplicated;
        AppState.pagination.cartDetail.totalRows = deduplicated.length;
        renderCartDetailTable();
    } catch (e) {
        document.getElementById('cart-detail-container').innerHTML = `<p style="color:var(--text-muted); text-align:center; padding:2rem;">Detailed data not loaded. <button class="btn-primary" style="font-size:0.8rem" onclick="showUploadModal()">Upload Part 2</button></p>`;
    }
}

function renderCartDetailTable() {
    const { currentPage, rowsPerPage } = AppState.pagination.cartDetail;
    const { sortCol, sortDesc, searchQuery } = AppState.viewState.cart;
    const getName = AppState.helpers.getName;

    // Apply Filtering
    let data = [...AppState.data.cartAnchorDetail];
    if (searchQuery) {
        const q = searchQuery.toLowerCase();
        data = data.filter(d => {
            const idI = String(d.i).toLowerCase();
            const idJ = String(d.j).toLowerCase();
            const nameI = getName(d.i).toLowerCase();
            const nameJ = getName(d.j).toLowerCase();
            return idI.includes(q) || idJ.includes(q) || nameI.includes(q) || nameJ.includes(q);
        });
    }

    const totalRows = data.length;

    // Apply Sorting before pagination
    data.sort((a, b) => {
        let valA = a[sortCol], valB = b[sortCol];
        if (sortCol === 'i' || sortCol === 'j') { valA = getName(valA); valB = getName(valB); }
        if (valA === undefined || valA === null) valA = 0;
        if (valB === undefined || valB === null) valB = 0;
        if (valA < valB) return sortDesc ? 1 : -1;
        if (valA > valB) return sortDesc ? -1 : 1;
        return 0;
    });

    const pData = data.slice((currentPage - 1) * rowsPerPage, currentPage * rowsPerPage);
    const getSortIndicator = (col) => sortCol === col ? (sortDesc ? ' ▼' : ' ▲') : '';

    let rows = pData.map(row => `
        <tr>
            <td><div>${getName(row.i)}</div><div style="font-size:0.8em;color:var(--text-muted);cursor:pointer;" onclick="copyToClipboard('${row.i}')">${row.i} <i class="ph ph-copy"></i></div></td>
            <td><div>${getName(row.j)}</div><div style="font-size:0.8em;color:var(--text-muted);cursor:pointer;" onclick="copyToClipboard('${row.j}')">${row.j} <i class="ph ph-copy"></i></div></td>
            <td>${formatNumber(row.co_order_cnt)}</td>
        </tr>
    `).join('');

    document.getElementById('cart-detail-container').innerHTML = `
        <table class="data-table">
            <thead><tr>
                <th onclick="handleCartSort('i')">Product A${getSortIndicator('i')}</th>
                <th onclick="handleCartSort('j')">Product B${getSortIndicator('j')}</th>
                <th onclick="handleCartSort('co_order_cnt')">Count${getSortIndicator('co_order_cnt')}</th>
            </tr></thead>
            <tbody>${rows}</tbody>
        </table>
    `;

    document.getElementById('pagination-info').innerText = `Showing page ${currentPage} of ${Math.ceil(totalRows / rowsPerPage)}`;
    document.getElementById('prevBtn').disabled = currentPage === 1;
    document.getElementById('nextBtn').disabled = currentPage * rowsPerPage >= totalRows;
    document.getElementById('prevBtn').onclick = () => { AppState.pagination.cartDetail.currentPage--; renderCartDetailTable(); };
    document.getElementById('nextBtn').onclick = () => { AppState.pagination.cartDetail.currentPage++; renderCartDetailTable(); };

    window.handleCartSort = (col) => {
        if (AppState.viewState.cart.sortCol === col) AppState.viewState.cart.sortDesc = !AppState.viewState.cart.sortDesc;
        else { AppState.viewState.cart.sortCol = col; AppState.viewState.cart.sortDesc = true; }
        AppState.pagination.cartDetail.currentPage = 1;
        renderCartDetailTable();
    };
}

// --- Upload Logic ---

function showUploadModal() {
    if (document.getElementById('uploadModal')) document.getElementById('uploadModal').remove();
    document.body.insertAdjacentHTML('beforeend', `
        <div id="uploadModal" style="position:fixed; inset:0; background:rgba(0,0,0,0.8); z-index:9999; display:flex; align-items:center; justify-content:center;">
            <div class="card" style="width:500px; max-width:90%;">
                <div style="display:flex; justify-content:space-between; margin-bottom:1.5rem;"><h3>Upload CSV Data</h3><button onclick="document.getElementById('uploadModal').remove()" style="background:none; border:none; color:white; cursor:pointer;"><i class="ph ph-x" style="font-size:1.5rem"></i></button></div>
                <div id="upload-status" style="margin-bottom:1rem; color:var(--text-muted)">Please select one or more CSV files from source folder.</div>
                <input type="file" id="file-input" multiple accept=".csv" onchange="handleFiles(this.files)">
                <div id="file-list" style="margin-top:1rem; font-size:0.9rem;"></div>
            </div>
        </div>
    `);
}

window.handleFiles = async (files) => {
    const list = document.getElementById('file-list');
    list.innerHTML = 'Processing...';
    let count = 0;
    for (let file of files) {
        const config = Object.values(REQUIRED_FILES)
            .sort((a, b) => b.key.length - a.key.length)
            .find(f => file.name.toLowerCase().includes(f.key.toLowerCase()));
        if (config) {
            await new Promise((resolve) => {
                Papa.parse(file, { header: true, dynamicTyping: true, skipEmptyLines: true, complete: async (r) => { await DB.save(config.key, r.data); count++; resolve(); } });
            });
        }
    }
    if (count > 0) { list.innerHTML = `<p style="color:var(--primary)">Successfully saved ${count} files. Reloading...</p>`; setTimeout(() => location.reload(), 1500); }
    else list.innerHTML = `<p style="color:var(--accent)">No matching files found. Check filenames.</p>`;
};

// --- Initialization ---

async function init() {
    const pageId = document.body.id;
    initAppUI();
    const sidebar = document.querySelector('.user-profile');
    if (sidebar) sidebar.innerHTML = `<button class="btn-primary" style="width:100%" onclick="showUploadModal()"><i class="ph ph-upload-simple"></i> Upload Data</button>`;

    try {
        const keys = await DB.getAllKeys();
        if (keys.length === 0) {
            document.getElementById('content-area').innerHTML = `<div class="card animate-fade-in" style="text-align:center; padding:4rem;"><i class="ph ph-database" style="font-size:4rem; color:var(--text-muted); margin-bottom:1rem;"></i><h3>No Data</h3><p style="color:var(--text-muted); margin-bottom:2rem;">Please upload CSV files to begin.</p><button class="btn-primary" onclick="showUploadModal()">Upload Now</button></div>`;
            return;
        }

        if (pageId === 'page-overview') { AppState.data.brandScore = await loadDataFromDB(REQUIRED_FILES.brandScore); renderOverview(); }
        else if (pageId === 'page-products') { AppState.data.anchorScored = await loadDataFromDB(REQUIRED_FILES.anchorScored); renderProducts(); }
        else if (pageId === 'page-transitions') {
            const [t, s] = await Promise.all([loadDataFromDB(REQUIRED_FILES.anchorTransition), loadDataFromDB(REQUIRED_FILES.anchorScored)]);
            AppState.data.anchorTransition = t; AppState.data.anchorScored = s; renderTransitions();
        }
        else if (pageId === 'page-cart') {
            const [c, s] = await Promise.all([loadDataFromDB(REQUIRED_FILES.cartAnchor), loadDataFromDB(REQUIRED_FILES.anchorScored)]);
            AppState.data.cartAnchor = c; AppState.data.anchorScored = s; renderCartAnalysis();
        }
    } catch (e) {
        console.error(e);
        document.getElementById('content-area').innerHTML = `<div class="card" style="text-align:center; padding:2rem;"><h3>Data Missing</h3><p style="color:var(--accent)">${e.message}</p><button class="btn-primary" onclick="showUploadModal()">Upload Missing File</button></div>`;
    }
}

document.addEventListener('DOMContentLoaded', init);
