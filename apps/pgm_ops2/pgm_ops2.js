const state = {
    view: 'priority',
    bundle: null,
    selectedProductId: ''
};

const VIEW_MODEL_FILES = {
    vm_priority_queue: 'vm_priority_queue.csv',
    vm_queue_summary: 'vm_queue_summary.csv',
    vm_segment_map: 'vm_segment_map.csv',
    vm_product_detail: 'vm_product_detail.csv',
    vm_definition_rules: 'vm_definition_rules.csv',
    vm_data_health: 'vm_data_health.csv',
    vm_brand_score_panel: 'vm_brand_score_panel.csv',
    vm_iteration_log: 'vm_iteration_log.csv'
};

const MART_FILES = {
    mart_product_revenue_windows: 'mart_product_revenue_windows.csv',
    mart_product_role_taxonomy_daily: 'mart_product_role_taxonomy_daily.csv',
    mart_product_priority_basis: 'mart_product_priority_basis.csv',
    mart_priority_queue_snapshot: 'mart_priority_queue_snapshot.csv',
    mart_segment_structure_snapshot: 'mart_segment_structure_snapshot.csv',
    mart_data_health_snapshot: 'mart_data_health_snapshot.csv',
    mart_brand_score_reconstruction: 'mart_brand_score_reconstruction.csv',
    mart_brand_score_validation_status: 'mart_brand_score_validation_status.csv'
};

const QA_FILES = {
    raw_manifest: 'raw_manifest.csv',
    validation_summary: 'validation_summary.csv',
    validation_report: 'validation_report.md',
    tone_audit: 'tone_audit.csv',
    implementation_scope: 'implementation_scope.csv'
};

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

function priorityClass(level) {
    if (level === '즉시 확인') return 'priority-immediate';
    if (level === '주의 관찰') return 'priority-watch';
    return 'priority-stable';
}

function getViewModel(name) {
    return state.bundle?.view_model?.[name] ?? [];
}

function setTitle(view) {
    const titleMap = {
        priority: '우선순위',
        segments: '구조 맵',
        detail: '상세 보기',
        definitions: '정의 보기',
        health: '데이터 상태'
    };
    document.querySelector('#page-title').textContent = titleMap[view] ?? 'PGM 운영 툴';
}

function renderSummaryCards() {
    const rows = getViewModel('vm_queue_summary');

    if (!rows.length) {
        return '<div class="empty-state">표시할 우선순위 집계가 없습니다.</div>';
    }

    return `
        <div class="summary-grid">
            ${rows.map((row) => `
                <article class="summary-card">
                    <div class="section-kicker">우선순위</div>
                    <strong class="${priorityClass(row.priority_level)}">${escapeHtml(row.product_count)}</strong>
                    <div>${escapeHtml(row.priority_level)}</div>
                </article>
            `).join('')}
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
                ${rawMissing ? '실데이터 raw_rosetta가 아직 적재되지 않았습니다.' : '현재 기준일에 해당하는 큐 산출물이 없습니다.'}
            </div>
        `;
    }

    return `
        <section class="hero">
            <div class="section-kicker">첫 화면</div>
            <h3 class="section-title">상품 우선순위 큐</h3>
            <p class="muted">단일 점수 대신 Revenue, Role, Brand Score 상태를 분리해 표시합니다.</p>
        </section>
        ${renderSummaryCards()}
        <section class="queue-list">
            ${queueRows.map((row) => `
                <article class="queue-row" data-product-row="${escapeHtml(row.product_id)}">
                    <div class="queue-row-header">
                        <div class="queue-row-title">
                            <div class="section-kicker">순위 ${escapeHtml(row.rank)}</div>
                            <h3>${escapeHtml(row.product_name || row.product_id)}</h3>
                            <p>${escapeHtml(row.product_id)}</p>
                        </div>
                        <div class="pill ${priorityClass(row.priority_level)}">${escapeHtml(row.priority_level)}</div>
                    </div>
                    <div class="reason-stack">
                        <div class="reason-item">
                            <strong>Revenue 이유</strong>
                            <div>${escapeHtml(row.revenue_reason)}</div>
                        </div>
                        <div class="reason-item">
                            <strong>Role 이유</strong>
                            <div>${escapeHtml(row.role_reason)}</div>
                        </div>
                        <div class="reason-item">
                            <strong>Brand Score 상태</strong>
                            <div>${escapeHtml(row.brand_score_reason)}</div>
                        </div>
                    </div>
                    <div class="queue-row-meta">
                        <small>Role taxonomy: ${escapeHtml(row.role_taxonomy)}</small>
                        <small>Revenue 최신일: ${escapeHtml(row.revenue_freshness_max_date || '-')} / Role 최신일: ${escapeHtml(row.role_freshness_max_date || '-')}</small>
                    </div>
                </article>
            `).join('')}
        </section>
    `;
}

function renderSegmentsView() {
    const rows = getViewModel('vm_segment_map');

    if (!rows.length) {
        return '<div class="empty-state">구조 맵 데이터가 없습니다.</div>';
    }

    return `
        <section class="panel">
            <div class="section-kicker">구조 맵</div>
            <h3 class="section-title">Revenue 변화 x Role taxonomy</h3>
            <table class="segment-table">
                <thead>
                    <tr>
                        <th>상품</th>
                        <th>Revenue 상태</th>
                        <th>Role taxonomy</th>
                        <th>우선순위</th>
                        <th>Brand Score</th>
                    </tr>
                </thead>
                <tbody>
                    ${rows.map((row) => `
                        <tr>
                            <td>${escapeHtml(row.product_name || row.product_id)}</td>
                            <td>${escapeHtml(row.revenue_segment)}</td>
                            <td>${escapeHtml(row.role_taxonomy)}</td>
                            <td class="${priorityClass(row.priority_level)}">${escapeHtml(row.priority_level)}</td>
                            <td>${escapeHtml(row.brand_score_status)}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </section>
    `;
}

function renderDetailView() {
    const queueRows = getViewModel('vm_priority_queue');
    const detailRows = getViewModel('vm_product_detail');

    if (!queueRows.length) {
        return '<div class="empty-state">상세 보기 데이터가 없습니다.</div>';
    }

    const selectedProductId = state.selectedProductId || queueRows[0].product_id;
    state.selectedProductId = selectedProductId;
    const selectedQueueRow = queueRows.find((row) => row.product_id === selectedProductId) ?? queueRows[0];
    const selectedRows = detailRows.filter((row) => row.product_id === selectedQueueRow.product_id);

    return `
        <section class="detail-layout">
            <aside class="detail-card">
                <div class="section-kicker">대상 선택</div>
                <h3>상품 목록</h3>
                <div class="product-picker">
                    ${queueRows.slice(0, 24).map((row) => `
                        <button
                            class="${row.product_id === selectedQueueRow.product_id ? 'is-selected' : ''}"
                            data-product-pick="${escapeHtml(row.product_id)}"
                        >
                            <strong>${escapeHtml(row.product_name || row.product_id)}</strong><br>
                            <span class="${priorityClass(row.priority_level)}">${escapeHtml(row.priority_level)}</span>
                        </button>
                    `).join('')}
                </div>
            </aside>
            <section class="detail-card">
                <div class="section-kicker">상세 보기</div>
                <h3>${escapeHtml(selectedQueueRow.product_name || selectedQueueRow.product_id)}</h3>
                <p class="muted">${escapeHtml(selectedQueueRow.product_id)}</p>
                <table class="detail-table">
                    <thead>
                        <tr>
                            <th>구간</th>
                            <th>항목</th>
                            <th>값</th>
                            <th>설명</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${selectedRows.map((row) => `
                            <tr>
                                <td>${escapeHtml(row.section)}</td>
                                <td>${escapeHtml(row.label)}</td>
                                <td>${escapeHtml(row.value)}</td>
                                <td>${escapeHtml(row.note)}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </section>
        </section>
    `;
}

function renderDefinitionsView() {
    const rows = getViewModel('vm_definition_rules');

    return `
        <section class="panel">
            <div class="section-kicker">정의 보기</div>
            <h3 class="section-title">운영 규칙</h3>
            <table class="definition-table">
                <thead>
                    <tr>
                        <th>구분</th>
                        <th>규칙</th>
                        <th>정의</th>
                        <th>상태</th>
                    </tr>
                </thead>
                <tbody>
                    ${rows.map((row) => `
                        <tr>
                            <td>${escapeHtml(row.rule_group)}</td>
                            <td>${escapeHtml(row.rule_name)}</td>
                            <td>${escapeHtml(row.rule_definition)}</td>
                            <td>${escapeHtml(row.status_label)}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </section>
    `;
}

function renderHealthView() {
    const healthRows = getViewModel('vm_data_health');
    const brandRows = getViewModel('vm_brand_score_panel');
    const toneAuditRows = state.bundle?.qa?.tone_audit ?? [];
    const iterationRows = getViewModel('vm_iteration_log');

    return `
        <section class="panel">
            <div class="section-kicker">데이터 상태</div>
            <h3 class="section-title">source freshness</h3>
            <table class="health-table">
                <thead>
                    <tr>
                        <th>source</th>
                        <th>row 수</th>
                        <th>min date</th>
                        <th>max date</th>
                        <th>gap days</th>
                        <th>상태</th>
                    </tr>
                </thead>
                <tbody>
                    ${healthRows.map((row) => `
                        <tr>
                            <td>${escapeHtml(row.source_key)}</td>
                            <td>${escapeHtml(row.row_count)}</td>
                            <td>${escapeHtml(row.min_date)}</td>
                            <td>${escapeHtml(row.max_date)}</td>
                            <td>${escapeHtml(row.freshness_gap_days)}</td>
                            <td>${escapeHtml(row.data_state)}</td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </section>
        <section class="summary-grid">
            <article class="grid-card">
                <div class="section-kicker">Brand Score</div>
                <h3>재현 상태</h3>
                <p class="muted">큐에는 연결하지 않고, 상태만 별도 표시합니다.</p>
                <div>${escapeHtml(brandRows[0]?.brand_score_status || 'unavailable')}</div>
            </article>
            <article class="grid-card">
                <div class="section-kicker">Tone Audit</div>
                <h3>화면 문구 점검</h3>
                <p class="muted">${toneAuditRows.every((row) => row.status === 'pass') ? '금지 문구 없음' : '금지 문구 점검 필요'}</p>
            </article>
            <article class="grid-card">
                <div class="section-kicker">반복 기록</div>
                <h3>검증 루프</h3>
                <p class="muted">${iterationRows.map((row) => `${row.iteration}회차: ${row.change_applied}`).join(' / ') || '기록 없음'}</p>
            </article>
        </section>
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
        ? (getViewModel('vm_priority_queue').length ? '실행 완료' : '산출물 없음')
        : '실데이터 미적재';

    const rendererMap = {
        priority: renderPriorityView,
        segments: renderSegmentsView,
        detail: renderDetailView,
        definitions: renderDefinitionsView,
        health: renderHealthView
    };

    root.innerHTML = (rendererMap[state.view] ?? renderPriorityView)();

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

async function loadLayerCsv(layer, files) {
    const entries = await Promise.all(
        Object.entries(files).map(async ([key, filename]) => {
            const text = await fetchTextOrThrow(resolveArtifactUrl(layer, filename));
            if (filename.endsWith('.md')) {
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
        // 정적 서빙 fallback으로 내려간다.
    }

    const bundle = await loadBundleFromArtifacts();
    bundle.raw_data_status = deriveRawDataStatus(bundle);
    return bundle;
}

async function loadBundle() {
    state.bundle = await loadBundleWithFallback();
    render();
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
