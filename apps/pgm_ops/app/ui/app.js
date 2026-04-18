import { VIEW_MODEL_FILES } from '../config/constants.js';
import { APP_SETTINGS } from '../config/settings.js';
import { SAMPLE_FALLBACKS } from '../config/sample_fallbacks.js';
import { getBrowserArtifactPath } from '../config/paths.js';
import { loadArtifactCollection } from '../loaders/artifact_reader.js';
import { renderOverviewPage } from './pages/overview_page.js';
import { renderProductsPage } from './pages/products_page.js';

const OPTIONAL_VIEW_MODEL_FILES = {
    overview_role_contribution: 'overview_role_contribution.csv',
    brand_role_structure: 'brand_role_structure.csv',
    brand_role_window_comparison: 'brand_role_window_comparison.csv'
};

function getSearchParams() {
    return new URLSearchParams(window.location.search);
}

function getArtifactBase() {
    const params = getSearchParams();
    const queryBase = params.get('artifactBase');
    const storedBase = window.localStorage.getItem(APP_SETTINGS.localStorageArtifactKey);

    if (queryBase) {
        return queryBase;
    }

    if (storedBase && storedBase !== './artifacts') {
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
    const snapshotDate = loadMeta.latestSnapshotDate ?? latestDate;

    if (!badge || !updated) {
        return;
    }

    badge.textContent = loadMeta.sourceLabel;
    badge.classList.toggle('is-live', loadMeta.mode === 'artifact');
    badge.classList.toggle('is-sample', loadMeta.mode !== 'artifact');
    updated.textContent = `기준일 ${formatDate(snapshotDate)} · ${loadMeta.chromeNote}`;
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

function renderApp(container, models, state, loadMeta) {
    const overviewCards = {
        daily: models.overview_daily_cards ?? [],
        weekly: models.overview_weekly_cards ?? [],
        monthly: models.overview_monthly_cards ?? []
    };
    const latestDate = getLatestCardDate(overviewCards);
    const filteredProducts = filterProducts(models.product_table ?? [], state.searchQuery);

    if (filteredProducts.length && !filteredProducts.some((row) => row.product_id === state.selectedProductId)) {
        state.selectedProductId = filteredProducts[0].product_id;
    }

    container.innerHTML = `
        <div class="ops-page-stack">
            ${renderOverviewPage({
                dailyCards: overviewCards.daily,
                weeklyCards: overviewCards.weekly,
                monthlyCards: overviewCards.monthly,
                roleContributionRows: models.overview_role_contribution ?? [],
                productRows: models.product_table ?? [],
                latestDate,
                statusBadge: loadMeta.sourceLabel
            })}
            ${renderProductsPage({
                productRows: filteredProducts,
                allProductRows: models.product_table ?? [],
                detailRows: models.product_detail_header ?? [],
                selectedProductId: state.selectedProductId,
                roleStructureRows: models.role_structure_chart ?? [],
                revenueStructureRows: models.revenue_structure_chart ?? [],
                brandRoleStructureRows: models.brand_role_structure ?? [],
                brandRoleWindowComparisonRows: models.brand_role_window_comparison ?? [],
                searchQuery: state.searchQuery,
                transitionSummaryRows: models.transition_summary ?? [],
                returnLoopSummaryRows: models.return_loop_summary ?? [],
                revenueInflowRows: models.revenue_inflow_context ?? [],
                priorityRows: models.priority_checks ?? []
            })}
        </div>
    `;

    updateChrome(loadMeta, latestDate);

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
    attachStaticNav();

    const container = document.getElementById('ops-app');
    container.innerHTML = '<div class="ops-panel hero"><div class="ops-empty"><strong>데이터를 불러오는 중입니다.</strong><p>화면에 필요한 최신 데이터를 확인하고 있습니다.</p></div></div>';

    const { models, loadMeta } = await loadViewModels();
    const initialProduct = (models.product_table ?? [])[0]?.product_id ?? '';
    const state = {
        selectedProductId: initialProduct,
        searchQuery: ''
    };

    renderApp(container, models, state, loadMeta);
}
