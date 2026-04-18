import { VIEW_MODEL_FILES } from '../config/constants.js';
import { APP_SETTINGS } from '../config/settings.js';
import { SAMPLE_FALLBACKS } from '../config/sample_fallbacks.js';
import { getBrowserArtifactPath } from '../config/paths.js';
import { loadArtifactCollection } from '../loaders/artifact_reader.js';
import { renderOverviewPage } from './pages/overview_page.js';
import { renderProductsPage } from './pages/products_page.js';
import { renderPriorityPage } from './pages/priority_page.js';

function getSearchParams() {
    return new URLSearchParams(window.location.search);
}

function getArtifactBase() {
    const params = getSearchParams();
    return params.get('artifactBase')
        || window.localStorage.getItem(APP_SETTINGS.localStorageArtifactKey)
        || APP_SETTINGS.defaultArtifactBase;
}

function shouldForceSampleMode() {
    const sample = getSearchParams().get('sample');
    return ['1', 'true', 'yes'].includes(String(sample ?? '').toLowerCase());
}

function formatDate(value) {
    return value || 'n/a';
}

function updateChrome(loadMeta, latestDate) {
    const badge = document.getElementById('ops-data-source-badge');
    const updated = document.getElementById('ops-last-updated');

    badge.textContent = loadMeta.sourceLabel;
    badge.classList.toggle('is-live', loadMeta.mode === 'artifact');
    badge.classList.toggle('is-sample', loadMeta.mode !== 'artifact');
    updated.textContent = `latest snapshot ${formatDate(latestDate)} · ${loadMeta.chromeNote}`;
}

function attachStaticNav() {
    document.querySelectorAll('[data-href]').forEach((button) => {
        button.addEventListener('click', () => {
            window.location.href = button.dataset.href;
        });
    });
}

function getLatestCardDate(models) {
    return [
        ...models.overview_daily_cards,
        ...models.overview_weekly_cards,
        ...models.overview_monthly_cards
    ].reduce((latest, row) => (!latest || row.as_of_date > latest ? row.as_of_date : latest), null);
}

function toModelObject(sourceRows) {
    return Object.fromEntries(
        Object.keys(VIEW_MODEL_FILES).map((key) => [key, [...(sourceRows[key] ?? [])]])
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
            sourceLabel: 'artifact-backed',
            chromeNote: 'all view_model files loaded',
            note: '모든 화면이 `artifacts/view_model/*.csv` 실데이터를 직접 읽고 있습니다. 비어 있는 artifact는 그대로 빈 상태로 남고 sample이 섞이지 않습니다.',
            fallbackKeys,
            emptyArtifactKeys
        };
    }

    if (!artifactKeys.length) {
        return {
            mode: 'fallback',
            sourceLabel: 'sample fallback',
            chromeNote: 'all panels are sample-backed',
            note: '브라우저에서 view_model artifact를 읽지 못해 모든 패널이 sample fallback만 보여 줍니다. 이 모드는 구현 예시 확인용이며 운영 판단 근거가 아닙니다.',
            fallbackKeys,
            emptyArtifactKeys
        };
    }

    return {
        mode: 'mixed',
        sourceLabel: 'artifact + sample',
        chromeNote: `sample fallback: ${fallbackKeys.join(', ')}`,
        note: `artifact로 읽힌 패널과 sample fallback 패널이 함께 있습니다. sample fallback 파일: ${fallbackKeys.join(', ')}. artifact로 읽힌 파일만 실제 구현 결과로 해석하세요.`,
        fallbackKeys,
        emptyArtifactKeys
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
    const periodKey = `overview_${state.period}_cards`;
    const cards = models[periodKey] ?? [];
    const latestDate = getLatestCardDate(models);
    const filteredProducts = filterProducts(models.product_table, state.searchQuery);

    if (filteredProducts.length && !filteredProducts.some((row) => row.product_id === state.selectedProductId)) {
        state.selectedProductId = filteredProducts[0].product_id;
    }

    container.innerHTML = `
        <div class="ops-tab-row" id="ops-period-tabs">
            ${['daily', 'weekly', 'monthly'].map((period) => `
                <button class="ops-tab ${period === state.period ? 'is-active' : ''}" type="button" data-period="${period}">
                    ${period.toUpperCase()}
                </button>
            `).join('')}
        </div>
        ${renderOverviewPage(cards, latestDate, {
            sourceLabel: loadMeta.sourceLabel,
            note: loadMeta.note,
            mode: loadMeta.mode,
            fallbackKeys: loadMeta.fallbackKeys,
            emptyArtifactKeys: loadMeta.emptyArtifactKeys
        })}
        <div class="ops-layout">
            <div>
                ${renderProductsPage({
                    productRows: filteredProducts,
                    detailRows: models.product_detail_header,
                    selectedProductId: state.selectedProductId,
                    roleStructureRows: models.role_structure_chart,
                    revenueStructureRows: models.revenue_structure_chart,
                    searchQuery: state.searchQuery
                })}
            </div>
            <div>
                ${renderPriorityPage(models.priority_checks)}
            </div>
        </div>
    `;

    updateChrome(loadMeta, latestDate);

    container.querySelectorAll('[data-period]').forEach((button) => {
        button.addEventListener('click', () => {
            state.period = button.dataset.period;
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
                sourceLabel: 'sample fallback',
                chromeNote: 'forced by ?sample=1',
                note: '`?sample=1`로 sample fallback 모드를 강제했습니다. 이 화면의 값은 샘플 예시이며 실 artifact 결과가 아닙니다.',
                fallbackKeys: Object.keys(VIEW_MODEL_FILES),
                emptyArtifactKeys: []
            }
        };
    }

    const artifactBase = getArtifactBase();
    const definitions = Object.fromEntries(
        Object.entries(VIEW_MODEL_FILES).map(([key, filename]) => [
            key,
            {
                path: getBrowserArtifactPath('view_model', filename, artifactBase),
                fallbackRows: SAMPLE_FALLBACKS[key] ?? []
            }
        ])
    );

    const collection = await loadArtifactCollection(definitions);
    const models = Object.fromEntries(Object.entries(collection).map(([key, result]) => [key, result.rows]));

    return {
        models,
        loadMeta: buildLoadMetaFromCollection(collection)
    };
}

export async function initializePgmOpsApp() {
    attachStaticNav();

    const container = document.getElementById('ops-app');
    container.innerHTML = '<div class="ops-panel hero"><div class="ops-empty"><strong>로딩 중...</strong><p>view_model artifact를 확인하고 있습니다.</p></div></div>';

    const { models, loadMeta } = await loadViewModels();
    const initialProduct = models.product_table[0]?.product_id ?? '';
    const state = {
        period: 'daily',
        selectedProductId: initialProduct,
        searchQuery: ''
    };

    renderApp(container, models, state, loadMeta);
}
