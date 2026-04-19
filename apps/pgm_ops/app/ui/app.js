import { VIEW_MODEL_FILES } from '../config/constants.js';
import { APP_SETTINGS } from '../config/settings.js';
import { SAMPLE_FALLBACKS } from '../config/sample_fallbacks.js';
import { getBrowserArtifactPath } from '../config/paths.js';
import { loadArtifactCollection } from '../loaders/artifact_reader.js';
import { renderOverviewPage } from './pages/overview_page.js';
import { renderProductsPage } from './pages/products_page.js';

const OPTIONAL_VIEW_MODEL_FILES = {};
const BLANK_OVERVIEW_ROLE_KEY = '__blank__';
const TOP_LEVEL_TAB_META = {
    overview: {
        label: '운영 요약',
        description: '먼저 볼 운영 이슈와 근거를 읽는 화면입니다.'
    },
    sku_workspace: {
        label: 'SKU 작업면',
        description: '선택한 SKU를 이어서 전환·복귀 신호까지 확인하는 작업면입니다.'
    }
};

function normalizeOverviewRoleKey(roleStatePrimary) {
    return roleStatePrimary ? roleStatePrimary : BLANK_OVERVIEW_ROLE_KEY;
}

function normalizeTopLevelTab(tab) {
    return Object.prototype.hasOwnProperty.call(TOP_LEVEL_TAB_META, tab) ? tab : 'overview';
}

function getSearchParams() {
    return new URLSearchParams(window.location.search);
}

function isSafeStoredArtifactBase(value) {
    if (!value || value === './artifacts') {
        return false;
    }

    if (value.startsWith('/')) {
        return true;
    }

    try {
        const resolved = new URL(value, window.location.href);
        return resolved.origin === window.location.origin;
    } catch {
        return false;
    }
}

function getArtifactBase() {
    const params = getSearchParams();
    const queryBase = params.get('artifactBase');
    const storedBase = window.localStorage.getItem(APP_SETTINGS.localStorageArtifactKey);

    if (queryBase) {
        return queryBase;
    }

    if (isSafeStoredArtifactBase(storedBase)) {
        return storedBase;
    }

    return APP_SETTINGS.defaultArtifactBase;
}

function getStaticArtifactBase() {
    return './artifacts';
}

function shouldForceSampleMode() {
    const sample = getSearchParams().get('sample');
    return ['1', 'true', 'yes'].includes(String(sample ?? '').toLowerCase());
}

function formatDate(value) {
    return value || '없음';
}

function updateChrome(loadMeta, latestDate) {
    const badge = document.getElementById('ops-data-source-badge');
    const updated = document.getElementById('ops-last-updated');
    const pageTitle = document.getElementById('page-title');
    const snapshotDate = loadMeta.latestSnapshotDate ?? latestDate;

    if (!badge || !updated) {
        return;
    }

    if (pageTitle) {
        pageTitle.textContent = '운영 상태판';
    }

    badge.textContent = loadMeta.sourceLabel;
    badge.classList.toggle('is-live', loadMeta.mode === 'artifact');
    badge.classList.toggle('is-sample', loadMeta.mode !== 'artifact');
    updated.textContent = `최근 확정일 ${formatDate(snapshotDate)} · ${loadMeta.chromeNote}`;
}

function applySidebarCollapsedState(isCollapsed) {
    const appContainer = document.querySelector('.app-container');
    const collapseBtn = document.querySelector('.sidebar-collapse-btn');

    if (!appContainer) {
        return;
    }

    appContainer.classList.toggle('is-sidebar-collapsed', Boolean(isCollapsed));

    if (collapseBtn) {
        collapseBtn.title = isCollapsed ? '사이드바 펼치기' : '사이드바 접기';
        collapseBtn.innerHTML = `<i class="ph ${isCollapsed ? 'ph-arrow-line-right' : 'ph-arrow-line-left'}"></i>`;
    }
}

function initializeChrome() {
    const collapseBtn = document.querySelector('.sidebar-collapse-btn');
    const storageKey = 'pgm_sidebar_collapsed';

    const saved = window.localStorage?.getItem(storageKey) === 'true';
    applySidebarCollapsedState(saved);

    if (collapseBtn) {
        collapseBtn.onclick = () => {
            const appContainer = document.querySelector('.app-container');
            const nextState = !appContainer?.classList.contains('is-sidebar-collapsed');
            applySidebarCollapsedState(nextState);

            try {
                window.localStorage?.setItem(storageKey, String(nextState));
            } catch (_) {
                // noop
            }
        };
    }
}

function attachStaticNav() {
    document.querySelectorAll('[data-href]').forEach((button) => {
        button.addEventListener('click', () => {
            window.location.href = button.dataset.href;
        });
    });
}

function getLatestCardDate(cardGroups) {
    return [
        ...(cardGroups.daily ?? []),
        ...(cardGroups.weekly ?? []),
        ...(cardGroups.monthly ?? [])
    ].reduce((latest, row) => (!latest || row.as_of_date > latest ? row.as_of_date : latest), null);
}

function getLatestOverviewDate(models) {
    return (models.overview_revenue_story ?? []).reduce((latest, row) => (!latest || row.as_of_date > latest ? row.as_of_date : latest), null)
        ?? getLatestCardDate({
            daily: models.overview_daily_cards ?? [],
            weekly: models.overview_weekly_cards ?? [],
            monthly: models.overview_monthly_cards ?? []
        });
}

function getDefaultOverviewRole(models, period) {
    return (models.overview_role_delta ?? [])
        .filter((row) => row.period === period && ['entry', 'expansion', 'return', 'convergence'].includes(String(row.role_state_primary ?? '')))
        .sort((left, right) => {
            const gap = Math.abs(Number(right.revenue_delta ?? 0)) - Math.abs(Number(left.revenue_delta ?? 0));
            if (gap !== 0) {
                return gap;
            }

            return Number(right.current_revenue ?? 0) - Number(left.current_revenue ?? 0);
        })[0]?.role_state_primary ?? '';
}

function toModelObject(sourceRows) {
    return Object.fromEntries(
        [...Object.keys(VIEW_MODEL_FILES), ...Object.keys(OPTIONAL_VIEW_MODEL_FILES)].map((key) => [key, [...(sourceRows[key] ?? [])]])
    );
}

function buildLoadMetaFromCollection(collection) {
    const artifactKeys = Object.entries(collection)
        .filter(([, result]) => result.source === 'artifact')
        .map(([key]) => key);
    const fallbackKeys = Object.entries(collection)
        .filter(([, result]) => result.source === 'fallback')
        .map(([key]) => key);
    const emptyArtifactKeys = Object.entries(collection)
        .filter(([, result]) => result.source === 'artifact' && !result.rows.length)
        .map(([key]) => key);

    if (!fallbackKeys.length) {
        return {
            mode: 'artifact',
            sourceLabel: '실데이터',
            chromeNote: '최신 화면 데이터 연결됨',
            note: '모든 화면이 최신 동기화 결과를 읽고 있습니다.',
            fallbackKeys,
            emptyArtifactKeys,
            latestSnapshotDate: null
        };
    }

    if (!artifactKeys.length) {
        return {
            mode: 'fallback',
            sourceLabel: '샘플 보기',
            chromeNote: '예시 데이터만 표시 중',
            note: '실데이터를 읽지 못해 예시 데이터로 표시하고 있습니다.',
            fallbackKeys,
            emptyArtifactKeys,
            latestSnapshotDate: null
        };
    }

    return {
        mode: 'mixed',
        sourceLabel: '혼합 보기',
        chromeNote: `일부 예시 파일 사용: ${fallbackKeys.join(', ')}`,
        note: '일부 화면이 예시 데이터로 표시되고 있습니다.',
        fallbackKeys,
        emptyArtifactKeys,
        latestSnapshotDate: null
    };
}

async function loadServiceStatus(artifactBase) {
    const normalizedBase = artifactBase.replace(/\/$/, '');
    if (!normalizedBase.endsWith('/api/pgm-ops')) {
        return null;
    }

    const response = await fetch(`${normalizedBase}/meta/load-status.json`);
    if (!response.ok) {
        throw new Error(`Failed to read service status: ${response.status}`);
    }

    return response.json();
}

function mergeLoadMeta(collectionMeta, serviceStatus) {
    if (!serviceStatus) {
        return collectionMeta;
    }

    return {
        ...collectionMeta,
        sourceLabel: collectionMeta.mode === 'artifact' ? (serviceStatus.sourceLabel ?? collectionMeta.sourceLabel) : collectionMeta.sourceLabel,
        chromeNote: serviceStatus.chromeNote ?? collectionMeta.chromeNote,
        note: collectionMeta.mode === 'artifact'
            ? (serviceStatus.note ?? collectionMeta.note)
            : collectionMeta.note,
        latestSnapshotDate: serviceStatus.latestSnapshotDate ?? collectionMeta.latestSnapshotDate
    };
}

function filterProducts(rows, query) {
    if (!query) {
        return rows;
    }

    const normalized = query.trim().toLowerCase();
    return rows.filter((row) => `${row.product_name} ${row.product_id}`.toLowerCase().includes(normalized));
}

function getInitialSelectedProductId(models) {
    const productRows = models.product_table ?? [];
    const productIds = new Set(productRows.map((row) => row.product_id));
    const topPriorityProductId = (models.priority_checks ?? []).find((row) => row.entity_type === 'product' && productIds.has(row.entity_id))?.entity_id;

    return topPriorityProductId ?? productRows[0]?.product_id ?? '';
}

function renderTopLevelTabNav(activeTab) {
    const normalizedActiveTab = normalizeTopLevelTab(activeTab);
    const activeTabMeta = TOP_LEVEL_TAB_META[normalizedActiveTab];

    return `
        <nav class="ops-view-switcher" aria-label="PGM Ops 화면 전환">
            <div class="ops-view-switcher-copy">
                <span class="ops-view-switcher-kicker">PGM Ops</span>
                <strong>${activeTabMeta.label}</strong>
            </div>
            <div class="pgm-chart-tab-group" role="tablist" aria-label="PGM Ops 화면 전환">
                ${Object.entries(TOP_LEVEL_TAB_META).map(([tabKey, tabMeta]) => `
                    <button
                        class="pgm-chart-tab ${normalizedActiveTab === tabKey ? 'is-active' : ''}"
                        type="button"
                        role="tab"
                        aria-selected="${normalizedActiveTab === tabKey ? 'true' : 'false'}"
                        data-top-level-tab="${tabKey}">
                        <span>${tabMeta.label}</span>
                    </button>
                `).join('')}
            </div>
        </nav>
    `;
}

function renderApp(container, models, state, loadMeta) {
    const latestDate = getLatestOverviewDate(models);
    const filteredProducts = filterProducts(models.product_table ?? [], state.searchQuery);
    const availablePeriods = new Set((models.overview_revenue_story ?? []).map((row) => row.period));
    const activeTab = normalizeTopLevelTab(state.activeTab);

    state.activeTab = activeTab;

    if (filteredProducts.length && !filteredProducts.some((row) => row.product_id === state.selectedProductId)) {
        state.selectedProductId = filteredProducts[0].product_id;
    }

    if (!availablePeriods.has(state.selectedOverviewPeriod)) {
        state.selectedOverviewPeriod = availablePeriods.has('daily') ? 'daily' : [...availablePeriods][0] ?? 'daily';
    }

    const roleRowsForPeriod = (models.overview_role_delta ?? []).filter((row) => row.period === state.selectedOverviewPeriod);
    if (!roleRowsForPeriod.some((row) => normalizeOverviewRoleKey(row.role_state_primary) === state.selectedOverviewRole)) {
        state.selectedOverviewRole = getDefaultOverviewRole(models, state.selectedOverviewPeriod);
    }

    container.innerHTML = `
        <div class="ops-page-stack">
            ${renderTopLevelTabNav(activeTab)}
            ${activeTab === 'overview'
                ? renderOverviewPage({
                    revenueStories: models.overview_revenue_story ?? [],
                    roleDeltaRows: models.overview_role_delta ?? [],
                    roleDrilldownRows: models.overview_role_drilldown ?? [],
                    productRows: models.product_table ?? [],
                    priorityRows: models.priority_checks ?? [],
                    selectedOverviewPeriod: state.selectedOverviewPeriod,
                    selectedOverviewRole: state.selectedOverviewRole,
                    latestDate,
                    statusBadge: loadMeta.sourceLabel
                })
                : renderProductsPage({
                    productRows: filteredProducts,
                    detailRows: models.product_detail_header ?? [],
                    selectedProductId: state.selectedProductId,
                    searchQuery: state.searchQuery,
                    transitionSummaryRows: models.transition_summary ?? [],
                    returnLoopSummaryRows: models.return_loop_summary ?? [],
                    revenueInflowRows: models.revenue_inflow_context ?? [],
                    priorityRows: models.priority_checks ?? []
                })}
        </div>
    `;

    updateChrome(loadMeta, latestDate);

    container.querySelectorAll('[data-top-level-tab]').forEach((row) => {
        row.addEventListener('click', () => {
            state.activeTab = normalizeTopLevelTab(row.dataset.topLevelTab);
            renderApp(container, models, state, loadMeta);
        });
    });

    const searchInput = document.getElementById('ops-product-search');
    if (searchInput) {
        searchInput.addEventListener('input', (event) => {
            state.searchQuery = event.target.value;
            renderApp(container, models, state, loadMeta);
        });
    }

    container.querySelectorAll('[data-product-id]').forEach((row) => {
        row.addEventListener('click', () => {
            state.selectedProductId = row.dataset.productId;
            if (row.dataset.productJump === 'true') {
                state.searchQuery = '';
                state.activeTab = 'sku_workspace';
            }
            renderApp(container, models, state, loadMeta);
        });
    });

    container.querySelectorAll('[data-overview-period]').forEach((row) => {
        row.addEventListener('click', () => {
            state.selectedOverviewPeriod = row.dataset.overviewPeriod;
            state.selectedOverviewRole = getDefaultOverviewRole(models, state.selectedOverviewPeriod);
            renderApp(container, models, state, loadMeta);
        });
    });

    container.querySelectorAll('[data-overview-role]').forEach((row) => {
        row.addEventListener('click', () => {
            state.selectedOverviewRole = row.dataset.overviewRole;
            renderApp(container, models, state, loadMeta);
        });
    });
}

async function loadViewModels() {
    if (shouldForceSampleMode()) {
        return {
            models: toModelObject(SAMPLE_FALLBACKS),
            loadMeta: {
                mode: 'fallback',
                sourceLabel: '샘플 보기',
                chromeNote: '예시 모드 강제 적용',
                note: '샘플 모드가 강제 적용되었습니다.',
                fallbackKeys: Object.keys(VIEW_MODEL_FILES),
                emptyArtifactKeys: []
            }
        };
    }

    const artifactBase = getArtifactBase();
    const staticArtifactBase = getStaticArtifactBase();
    const definitions = Object.fromEntries(
        Object.entries(VIEW_MODEL_FILES).map(([key, filename]) => [
            key,
            {
                path: getBrowserArtifactPath('view_model', filename, artifactBase),
                alternatePaths: artifactBase.endsWith('/api/pgm-ops')
                    ? [getBrowserArtifactPath('view_model', filename, staticArtifactBase)]
                    : [],
                fallbackRows: SAMPLE_FALLBACKS[key] ?? []
            }
        ])
    );

    const collection = await loadArtifactCollection(definitions);
    const optionalDefinitions = Object.fromEntries(
        Object.entries(OPTIONAL_VIEW_MODEL_FILES).map(([key, filename]) => [
            key,
            {
                path: getBrowserArtifactPath('view_model', filename, artifactBase),
                alternatePaths: artifactBase.endsWith('/api/pgm-ops')
                    ? [getBrowserArtifactPath('view_model', filename, staticArtifactBase)]
                    : [],
                fallbackRows: []
            }
        ])
    );
    const optionalCollection = await loadArtifactCollection(optionalDefinitions);
    const models = Object.fromEntries([
        ...Object.entries(collection).map(([key, result]) => [key, result.rows]),
        ...Object.entries(optionalCollection).map(([key, result]) => [key, result.rows])
    ]);
    let serviceStatus = null;

    try {
        serviceStatus = await loadServiceStatus(artifactBase);
    } catch (error) {
        serviceStatus = null;
    }

    return {
        models,
        loadMeta: mergeLoadMeta(buildLoadMetaFromCollection(collection), serviceStatus)
    };
}

export async function initializePgmOpsApp() {
    initializeChrome();
    attachStaticNav();

    const container = document.getElementById('ops-app');
    container.innerHTML = `
        <div class="card ops-loading-card">
            <div class="spinner" aria-hidden="true"></div>
            <div class="ops-loading-copy">
                <strong>데이터를 불러오는 중입니다.</strong>
                <p>화면에 필요한 최신 데이터를 확인하고 있습니다.</p>
            </div>
        </div>
    `;

    const { models, loadMeta } = await loadViewModels();
    const initialProduct = getInitialSelectedProductId(models);
    const state = {
        selectedProductId: initialProduct,
        searchQuery: '',
        activeTab: 'overview',
        selectedOverviewPeriod: 'daily',
        selectedOverviewRole: ''
    };

    renderApp(container, models, state, loadMeta);
}
