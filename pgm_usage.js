// Purpose usage review page logic

const PGM_USAGE_PURPOSES = [
    {
        key: 'entry-growth',
        label: '신규 유입 / 첫 구매 확대',
        shortLabel: '신규 유입',
        guide: '첫 구매를 만들 수 있는 후보를 검토합니다.'
    },
    {
        key: 'next-purchase',
        label: '다음 구매 연결 강화',
        shortLabel: '다음 구매',
        guide: '첫 구매 이후의 연결 가능성을 검토합니다.'
    },
    {
        key: 'return-strength',
        label: '다시 찾는 구매 강화',
        shortLabel: '재방문 구매',
        guide: '반복 구매나 회귀 흐름의 근거를 검토합니다.'
    },
    {
        key: 'basket-expansion',
        label: '함께 담기 확장',
        shortLabel: '함께 담기',
        guide: '동시 구매와 장바구니 확장 가능성을 검토합니다.'
    }
];

const PGM_USAGE_STATUS_LABELS = {
    operational_candidate: '운영 검토 후보',
    testable: '실험 검토 후보',
    hypothesis_only: '가설 수준',
    insufficient: '근거 부족',
    not_applicable: '비대상'
};

const PGM_USAGE_SCOPE_LABELS = {
    broad_rollout: '넓은 적용 검토',
    limited_rollout: '제한 적용 검토',
    small_test: '작은 실험 검토',
    not_recommended: '현재 검토 제외'
};

const PGM_USAGE_LEVEL_LABELS = {
    high: '높음',
    medium: '중간',
    low: '낮음'
};

const PGM_USAGE_DIRECTION_LABELS = {
    increase: '증가 가설',
    unclear: '불명확',
    not_applicable: '비대상'
};

const PGM_USAGE_ELIGIBILITY_LABELS = {
    allow: '검토 가능',
    downweight: '제한 반영',
    exclude: '제외'
};

const PGM_USAGE_ROLE_LABELS = {
    core_merchandise: '핵심 상품',
    bundle_or_set: '세트/번들',
    trial_or_entry_kit: '체험/유입 키트',
    gift_or_promo: '증정/프로모션',
    accessory_or_refill: '액세서리/리필'
};

const PGM_USAGE_METRIC_LABELS = {
    attach_rate: '함께 담김률',
    breadth_lift: '확장 폭',
    entry_customer_ratio: '첫 구매 고객 비중',
    entry_demand_share: '신규 수요 비중',
    transition_rate: '다음 구매 전환율',
    return_customer_rate: '재구매 고객 비중',
    return_demand_share: '재구매 수요 비중',
    repeat_rate: '반복 구매율',
    revenue_90d: '최근 90일 매출'
};

const PGM_USAGE_RATIONALE_LABELS = {
    basket_signal_present: '함께 담김 근거가 관측됨',
    basket_signal_emerging: '함께 담김 신호가 초기 수준',
    basket_pattern_not_typed: '구체 조합 유형은 추가 확인 필요',
    basket_compare_weak: '비교 우위는 제한적',
    basket_evidence_insufficient: '함께 담김 근거가 부족함',
    downweighted_candidate: '후보 가중치가 낮게 반영됨'
};

const PGM_USAGE_SCOPE_ORDER = {
    broad_rollout: 4,
    limited_rollout: 3,
    small_test: 2,
    not_recommended: 1
};

const PGM_USAGE_LEVEL_ORDER = {
    high: 3,
    medium: 2,
    low: 1
};

function pgmUsageEscape(value) {
    if (typeof escapeHtml === 'function') return escapeHtml(value);
    return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
}

function pgmUsageEscapeJs(value) {
    if (typeof escapeJs === 'function') return escapeJs(value);
    return String(value ?? '')
        .replaceAll('\\', '\\\\')
        .replaceAll("'", "\\'")
        .replaceAll('\n', '\\n')
        .replaceAll('\r', '\\r');
}

function pgmUsageEscapeJsAttr(value) {
    return pgmUsageEscape(pgmUsageEscapeJs(value));
}

function pgmUsageNumber(value, fallback = NaN) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
}

function pgmUsageFormatNumber(value, decimals = 0) {
    const num = pgmUsageNumber(value, NaN);
    if (!Number.isFinite(num)) return '-';
    if (typeof formatNumber === 'function') return formatNumber(num, decimals);
    return new Intl.NumberFormat('ko-KR', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    }).format(num);
}

function pgmUsageNormalizeKey(value) {
    return String(value || '').trim();
}

function pgmUsageNormalizeBoolean(value) {
    const normalized = String(value ?? '').trim().toLowerCase();
    return ['true', '1', 'yes', 'y'].includes(normalized);
}

function pgmUsageLabel(map, value, fallback = '-') {
    const key = pgmUsageNormalizeKey(value);
    return map[key] || key || fallback;
}

function pgmUsagePurposeMeta(key) {
    return PGM_USAGE_PURPOSES.find((purpose) => purpose.key === key) || {
        key,
        label: key || '목적 없음',
        shortLabel: key || '목적 없음',
        guide: '목적 라벨 매핑이 필요한 행입니다.'
    };
}

function pgmUsageNormalizeRow(row) {
    const normalized = typeof normalizeCsvRows === 'function'
        ? normalizeCsvRows([row])[0]
        : (row || {});
    const signalScore = pgmUsageNumber(normalized.effect_signal_score, NaN);
    const maturityScore = pgmUsageNumber(normalized.effect_maturity_score, NaN);
    return {
        snapshotName: pgmUsageNormalizeKey(normalized.snapshot_name),
        productId: pgmUsageNormalizeKey(normalized.product_id),
        productName: pgmUsageNormalizeKey(normalized.product_name_latest),
        purposeKey: pgmUsageNormalizeKey(normalized.purpose_key),
        status: pgmUsageNormalizeKey(normalized.effect_status),
        primaryMetric: pgmUsageNormalizeKey(normalized.effect_primary_metric),
        primaryMetricValue: normalized.effect_primary_metric_value,
        secondaryMetric: pgmUsageNormalizeKey(normalized.effect_secondary_metric),
        secondaryMetricValue: normalized.effect_secondary_metric_value,
        direction: pgmUsageNormalizeKey(normalized.effect_direction),
        strength: pgmUsageNormalizeKey(normalized.effect_strength),
        confidence: pgmUsageNormalizeKey(normalized.effect_confidence),
        scope: pgmUsageNormalizeKey(normalized.effect_scope),
        preconditionFlag: pgmUsageNormalizeBoolean(normalized.effect_precondition_flag),
        riskFlag: pgmUsageNormalizeBoolean(normalized.effect_risk_flag),
        rationaleCode: pgmUsageNormalizeKey(normalized.effect_rationale_code),
        signalScore,
        maturityScore,
        merchandiseRole: pgmUsageNormalizeKey(normalized.merchandise_role),
        eligibility: pgmUsageNormalizeKey(normalized.purpose_candidate_eligibility),
        weightMultiplier: normalized.purpose_candidate_weight_multiplier,
        eligibilityRuleMatched: pgmUsageNormalizeKey(normalized.eligibility_rule_matched)
    };
}

function pgmUsageIsReviewable(row) {
    return ['testable', 'operational_candidate'].includes(row.status)
        || ['small_test', 'limited_rollout', 'broad_rollout'].includes(row.scope);
}

function pgmUsageIsInsufficient(row) {
    return ['insufficient', 'not_applicable'].includes(row.status) || row.scope === 'not_recommended';
}

function pgmUsageCompareRows(a, b) {
    const scopeDiff = (PGM_USAGE_SCOPE_ORDER[b.scope] || 0) - (PGM_USAGE_SCOPE_ORDER[a.scope] || 0);
    if (scopeDiff) return scopeDiff;
    const confidenceDiff = (PGM_USAGE_LEVEL_ORDER[b.confidence] || 0) - (PGM_USAGE_LEVEL_ORDER[a.confidence] || 0);
    if (confidenceDiff) return confidenceDiff;
    const strengthDiff = (PGM_USAGE_LEVEL_ORDER[b.strength] || 0) - (PGM_USAGE_LEVEL_ORDER[a.strength] || 0);
    if (strengthDiff) return strengthDiff;
    const signalDiff = pgmUsageNumber(b.signalScore, -1) - pgmUsageNumber(a.signalScore, -1);
    if (signalDiff) return signalDiff;
    const maturityDiff = pgmUsageNumber(b.maturityScore, -1) - pgmUsageNumber(a.maturityScore, -1);
    if (maturityDiff) return maturityDiff;
    return String(a.productName || a.productId).localeCompare(String(b.productName || b.productId), 'ko');
}

function pgmUsageRows() {
    const source = AppState?.data?.productPurposeEffects || AppState?.rawData?.productPurposeEffects || [];
    return (source || [])
        .map(pgmUsageNormalizeRow)
        .filter((row) => row.productId && row.purposeKey);
}

function pgmUsageProducts(rows) {
    const map = new Map();
    rows.forEach((row) => {
        if (!map.has(row.productId)) {
            map.set(row.productId, {
                productId: row.productId,
                productName: row.productName || row.productId,
                rowCount: 0
            });
        }
        const product = map.get(row.productId);
        product.rowCount += 1;
        if (!product.productName && row.productName) product.productName = row.productName;
    });
    return [...map.values()].sort((a, b) => String(a.productName).localeCompare(String(b.productName), 'ko'));
}

function pgmUsageSnapshotLabel(rows) {
    const names = [...new Set(rows.map((row) => row.snapshotName).filter(Boolean))];
    if (!names.length) return '스냅샷 없음';
    if (names.length === 1) return names[0];
    return `${names[0]} 외 ${names.length - 1}개`;
}

function pgmUsageCurrentState() {
    if (!AppState.viewState.pgmUsage) {
        AppState.viewState.pgmUsage = {
            purposeFilter: 'all',
            scopeFilter: 'all',
            confidenceFilter: 'all',
            strengthFilter: 'all',
            flagFilter: 'all',
            eligibilityFilter: 'all',
            productSearch: '',
            selectedProductId: '',
            selectedPurposeKey: ''
        };
    }
    return AppState.viewState.pgmUsage;
}

function pgmUsageApplyFilters(rows, state) {
    const search = String(state.productSearch || '').trim().toLowerCase();
    return rows.filter((row) => {
        if (state.purposeFilter !== 'all' && row.purposeKey !== state.purposeFilter) return false;
        if (state.scopeFilter !== 'all' && row.scope !== state.scopeFilter) return false;
        if (state.confidenceFilter !== 'all' && row.confidence !== state.confidenceFilter) return false;
        if (state.strengthFilter !== 'all' && row.strength !== state.strengthFilter) return false;
        if (state.eligibilityFilter !== 'all' && row.eligibility !== state.eligibilityFilter) return false;
        if (state.flagFilter === 'risk' && !row.riskFlag) return false;
        if (state.flagFilter === 'precondition' && !row.preconditionFlag) return false;
        if (state.flagFilter === 'clean' && (row.riskFlag || row.preconditionFlag)) return false;
        if (search) {
            const haystack = `${row.productId} ${row.productName}`.toLowerCase();
            if (!haystack.includes(search)) return false;
        }
        return true;
    });
}

function pgmUsageSummary(rows) {
    return {
        reviewable: rows.filter(pgmUsageIsReviewable).length,
        smallTest: rows.filter((row) => row.scope === 'small_test').length,
        rollout: rows.filter((row) => ['limited_rollout', 'broad_rollout'].includes(row.scope)).length,
        flagged: rows.filter((row) => row.riskFlag || row.preconditionFlag).length,
        insufficient: rows.filter(pgmUsageIsInsufficient).length
    };
}

function pgmUsageBuildModel() {
    const state = pgmUsageCurrentState();
    const rows = pgmUsageRows();
    const filteredRows = pgmUsageApplyFilters(rows, state).sort(pgmUsageCompareRows);
    const products = pgmUsageProducts(rows);
    const selectedProduct = products.find((product) => product.productId === state.selectedProductId) || null;
    const selectedRows = rows
        .filter((row) => row.productId === state.selectedProductId)
        .sort((a, b) => {
            const aIndex = PGM_USAGE_PURPOSES.findIndex((purpose) => purpose.key === a.purposeKey);
            const bIndex = PGM_USAGE_PURPOSES.findIndex((purpose) => purpose.key === b.purposeKey);
            return (aIndex === -1 ? 99 : aIndex) - (bIndex === -1 ? 99 : bIndex);
        });

    return {
        rows,
        filteredRows,
        products,
        selectedProduct,
        selectedRows,
        snapshotLabel: pgmUsageSnapshotLabel(rows),
        summary: pgmUsageSummary(filteredRows),
        state
    };
}

function pgmUsageRenderOptions(options, selected) {
    return options.map(([value, label]) => `
        <option value="${pgmUsageEscape(value)}" ${selected === value ? 'selected' : ''}>${pgmUsageEscape(label)}</option>
    `).join('');
}

function pgmUsageRenderToolbar(model) {
    const state = model.state;
    const purposeOptions = [['all', '전체 목적'], ...PGM_USAGE_PURPOSES.map((purpose) => [purpose.key, purpose.shortLabel])];
    const scopeOptions = [
        ['all', '전체 검토 범위'],
        ['broad_rollout', '넓은 적용'],
        ['limited_rollout', '제한 적용'],
        ['small_test', '작은 실험'],
        ['not_recommended', '현재 제외']
    ];
    const levelOptions = [
        ['all', '전체'],
        ['high', '높음'],
        ['medium', '중간'],
        ['low', '낮음']
    ];
    const flagOptions = [
        ['all', '전체 플래그'],
        ['risk', '리스크 포함'],
        ['precondition', '전제조건 필요'],
        ['clean', '주의 플래그 제외']
    ];
    const eligibilityOptions = [
        ['all', '전체 적격성'],
        ['allow', '검토 가능'],
        ['downweight', '제한 반영'],
        ['exclude', '제외']
    ];

    return `
        <section class="pgm-usage-toolbar" aria-label="검토 필터">
            <div class="pgm-usage-toolbar-copy">
                <span class="pgm-usage-eyebrow">검토용 초기 화면</span>
                <h2>목적별 활용 검토</h2>
                <p>구조화된 목적별 예상 효과 산출물을 기준으로 후보를 정렬하고 비교합니다. 이 화면은 실행 결론이나 승인으로 보지 않습니다.</p>
            </div>
            <div class="pgm-usage-filter-grid">
                <label>
                    <span>목적</span>
                    <select onchange="handlePgmUsageFilterChange('purposeFilter', this.value)">
                        ${pgmUsageRenderOptions(purposeOptions, state.purposeFilter)}
                    </select>
                </label>
                <label>
                    <span>검토 범위</span>
                    <select onchange="handlePgmUsageFilterChange('scopeFilter', this.value)">
                        ${pgmUsageRenderOptions(scopeOptions, state.scopeFilter)}
                    </select>
                </label>
                <label>
                    <span>근거 수준</span>
                    <select onchange="handlePgmUsageFilterChange('confidenceFilter', this.value)">
                        ${pgmUsageRenderOptions(levelOptions, state.confidenceFilter)}
                    </select>
                </label>
                <label>
                    <span>효과 강도</span>
                    <select onchange="handlePgmUsageFilterChange('strengthFilter', this.value)">
                        ${pgmUsageRenderOptions(levelOptions, state.strengthFilter)}
                    </select>
                </label>
                <label>
                    <span>주의 플래그</span>
                    <select onchange="handlePgmUsageFilterChange('flagFilter', this.value)">
                        ${pgmUsageRenderOptions(flagOptions, state.flagFilter)}
                    </select>
                </label>
                <label>
                    <span>후보 적격성</span>
                    <select onchange="handlePgmUsageFilterChange('eligibilityFilter', this.value)">
                        ${pgmUsageRenderOptions(eligibilityOptions, state.eligibilityFilter)}
                    </select>
                </label>
                <label class="pgm-usage-search-label">
                    <span>제품 검색</span>
                    <input type="search" value="${pgmUsageEscape(state.productSearch)}" placeholder="제품명 또는 ID"
                        oninput="handlePgmUsageFilterChange('productSearch', this.value)">
                </label>
            </div>
        </section>
    `;
}

function pgmUsageRenderContext(model) {
    return `
        <div class="pgm-usage-context">
            <span>스냅샷: <strong>${pgmUsageEscape(model.snapshotLabel)}</strong></span>
            <span>행: <strong>${pgmUsageFormatNumber(model.rows.length)}</strong></span>
            <span>상품: <strong>${pgmUsageFormatNumber(model.products.length)}</strong></span>
            <span>현재 표시: <strong>${pgmUsageFormatNumber(model.filteredRows.length)}</strong></span>
        </div>
    `;
}

function pgmUsageRenderSummary(model) {
    const cards = [
        ['검토 가능 후보', model.summary.reviewable, '운영/실험 또는 적용 범위가 있는 행'],
        ['작은 실험 후보', model.summary.smallTest, '작게 확인할 수 있는 후보'],
        ['제한/넓은 적용 후보', model.summary.rollout, '제한 또는 넓은 적용 검토 범위'],
        ['주의 필요 후보', model.summary.flagged, '리스크나 전제조건 플래그 포함'],
        ['근거 부족/비대상', model.summary.insufficient, '지금 판단을 보류할 행']
    ];
    return `
        <section class="pgm-usage-summary-strip" aria-label="검토 요약">
            ${cards.map(([label, value, helper]) => `
                <article class="pgm-usage-summary-card">
                    <span>${pgmUsageEscape(label)}</span>
                    <strong>${pgmUsageFormatNumber(value)}</strong>
                    <p>${pgmUsageEscape(helper)}</p>
                </article>
            `).join('')}
        </section>
    `;
}

function pgmUsageBadge(label, tone = '') {
    const toneClass = tone ? ` is-${tone}` : '';
    return `<span class="pgm-usage-badge${toneClass}">${pgmUsageEscape(label)}</span>`;
}

function pgmUsageScopeTone(scope) {
    if (scope === 'broad_rollout') return 'strong';
    if (scope === 'limited_rollout') return 'medium';
    if (scope === 'small_test') return 'test';
    if (scope === 'not_recommended') return 'muted';
    return 'plain';
}

function pgmUsageFlagBadges(row) {
    const badges = [];
    if (row.preconditionFlag) badges.push(pgmUsageBadge('전제조건 필요', 'caution'));
    if (row.riskFlag) badges.push(pgmUsageBadge('리스크 포함', 'risk'));
    if (row.eligibility === 'downweight') badges.push(pgmUsageBadge('제한 반영', 'caution'));
    if (row.eligibility === 'exclude') badges.push(pgmUsageBadge('제외', 'muted'));
    if (!badges.length) badges.push(pgmUsageBadge('주의 플래그 없음', 'plain'));
    return badges.join('');
}

function pgmUsageMetricText(metric, value) {
    const label = pgmUsageLabel(PGM_USAGE_METRIC_LABELS, metric, '');
    if (!label) return '-';
    const num = pgmUsageNumber(value, NaN);
    const displayValue = Number.isFinite(num) ? pgmUsageFormatNumber(num, Math.abs(num) < 10 && num % 1 !== 0 ? 2 : 0) : pgmUsageEscape(value || '-');
    return `${label} ${displayValue}`;
}

function pgmUsageInterpretation(row) {
    if (!row) return '해당 목적의 행이 아직 없습니다.';
    if (row.scope === 'not_recommended' || ['insufficient', 'not_applicable'].includes(row.status)) {
        return '현재 데이터에서는 활용 판단을 보류하는 편이 적절합니다.';
    }
    if (row.riskFlag || row.preconditionFlag) {
        return '활용 가능성은 검토하되, 전제조건과 리스크를 먼저 확인해야 합니다.';
    }
    if (row.scope === 'broad_rollout') return '넓은 적용 검토 범위에 있으나 실행 결론은 별도 검토가 필요합니다.';
    if (row.scope === 'limited_rollout') return '제한된 범위에서 운영 검토를 시작할 수 있는 후보입니다.';
    if (row.scope === 'small_test') return '작은 실험으로 가설을 확인하는 후보입니다.';
    return '표시된 상태와 근거 수준을 기준으로 추가 검토가 필요합니다.';
}

function pgmUsageRenderPurposeBoards(model) {
    return `
        <div class="pgm-usage-purpose-board-grid">
            ${PGM_USAGE_PURPOSES.map((purpose) => {
                const rows = model.filteredRows.filter((row) => row.purposeKey === purpose.key);
                const reviewableCount = rows.filter(pgmUsageIsReviewable).length;
                const topRows = rows.slice(0, 5);
                return `
                    <article class="pgm-usage-purpose-card">
                        <div class="pgm-usage-purpose-card-head">
                            <div>
                                <h4>${pgmUsageEscape(purpose.label)}</h4>
                                <p>${pgmUsageEscape(purpose.guide)}</p>
                            </div>
                            <strong>${pgmUsageFormatNumber(reviewableCount)}</strong>
                        </div>
                        <div class="pgm-usage-purpose-list">
                            ${topRows.length ? topRows.map((row) => `
                                <button type="button" class="pgm-usage-candidate-chip ${model.state.selectedProductId === row.productId ? 'is-selected' : ''}"
                                    onclick="selectPgmUsageProduct('${pgmUsageEscapeJsAttr(row.productId)}', '${pgmUsageEscapeJsAttr(row.purposeKey)}')">
                                    <span>${pgmUsageEscape(row.productName || row.productId)}</span>
                                    <small>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_SCOPE_LABELS, row.scope))} · ${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_LEVEL_LABELS, row.confidence))}</small>
                                    <span class="pgm-usage-mini-flags">
                                        ${row.preconditionFlag ? pgmUsageBadge('전제', 'caution') : ''}
                                        ${row.riskFlag ? pgmUsageBadge('리스크', 'risk') : ''}
                                    </span>
                                </button>
                            `).join('') : `
                                <div class="pgm-usage-inline-empty">현재 필터에서 표시할 후보가 없습니다.</div>
                            `}
                        </div>
                    </article>
                `;
            }).join('')}
        </div>
    `;
}

function pgmUsageRenderCandidateTable(model) {
    const rows = model.filteredRows.slice(0, 100);
    const remaining = Math.max(0, model.filteredRows.length - rows.length);
    return `
        <div class="pgm-usage-table-card">
            <div class="pgm-usage-section-head">
                <div>
                    <h3>후보 테이블</h3>
                    <p>정렬은 검토 범위, 근거 수준, 효과 강도, 보조 점수 순서로 적용됩니다.</p>
                </div>
                ${remaining ? `<span>${pgmUsageFormatNumber(remaining)}개 행은 필터를 좁히면 볼 수 있습니다.</span>` : ''}
            </div>
            <div class="table-container pgm-usage-table-wrap">
                <table class="data-table pgm-usage-table">
                    <thead>
                        <tr>
                            <th>제품</th>
                            <th>목적</th>
                            <th>검토 범위</th>
                            <th>상태</th>
                            <th>방향/강도</th>
                            <th>근거</th>
                            <th>주의</th>
                            <th>상품 역할</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${rows.length ? rows.map((row) => `
                            <tr class="clickable ${model.state.selectedProductId === row.productId ? 'row-focused' : ''}"
                                onclick="selectPgmUsageProduct('${pgmUsageEscapeJsAttr(row.productId)}', '${pgmUsageEscapeJsAttr(row.purposeKey)}')">
                                <td>
                                    <div class="pgm-usage-product-cell">
                                        <strong>${pgmUsageEscape(row.productName || row.productId)}</strong>
                                        <span>${pgmUsageEscape(row.productId)}</span>
                                    </div>
                                </td>
                                <td>${pgmUsageEscape(pgmUsagePurposeMeta(row.purposeKey).shortLabel)}</td>
                                <td>${pgmUsageBadge(pgmUsageLabel(PGM_USAGE_SCOPE_LABELS, row.scope), pgmUsageScopeTone(row.scope))}</td>
                                <td>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_STATUS_LABELS, row.status))}</td>
                                <td>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_DIRECTION_LABELS, row.direction))}<br><span>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_LEVEL_LABELS, row.strength))}</span></td>
                                <td>
                                    <div class="pgm-usage-metric-stack">
                                        <span>${pgmUsageEscape(pgmUsageMetricText(row.primaryMetric, row.primaryMetricValue))}</span>
                                        <small>${pgmUsageEscape(pgmUsageMetricText(row.secondaryMetric, row.secondaryMetricValue))}</small>
                                        <small>근거 수준 ${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_LEVEL_LABELS, row.confidence))}</small>
                                    </div>
                                </td>
                                <td><div class="pgm-usage-badge-stack">${pgmUsageFlagBadges(row)}</div></td>
                                <td>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_ROLE_LABELS, row.merchandiseRole))}</td>
                            </tr>
                        `).join('') : `
                            <tr>
                                <td colspan="8" class="pgm-usage-empty-cell">현재 필터에서 표시할 후보가 없습니다.</td>
                            </tr>
                        `}
                    </tbody>
                </table>
            </div>
        </div>
    `;
}

function pgmUsageRenderGlobalArea(model) {
    return `
        <section class="pgm-usage-panel pgm-usage-global-panel">
            <div class="pgm-usage-section-head">
                <div>
                    <h3>전체 후보 검토</h3>
                    <p>선택 상품과 무관하게 전체 제품-목적 조합을 표시합니다.</p>
                </div>
            </div>
            ${pgmUsageRenderPurposeBoards(model)}
            ${pgmUsageRenderCandidateTable(model)}
        </section>
    `;
}

function pgmUsageRenderProductSelector(model) {
    return `
        <div class="pgm-usage-product-selector">
            <label>
                <span>상품 선택</span>
                <select onchange="selectPgmUsageProduct(this.value, '')">
                    <option value="">상품을 선택하세요</option>
                    ${model.products.map((product) => `
                        <option value="${pgmUsageEscape(product.productId)}" ${model.state.selectedProductId === product.productId ? 'selected' : ''}>
                            ${pgmUsageEscape(product.productName)} (${pgmUsageEscape(product.productId)})
                        </option>
                    `).join('')}
                </select>
            </label>
            ${model.state.selectedProductId ? `
                <button type="button" onclick="clearPgmUsageSelectedProduct()">선택 해제</button>
            ` : ''}
        </div>
    `;
}

function pgmUsageRenderSelectedPurposeCards(model) {
    if (!model.selectedProduct) {
        return `
            <div class="pgm-usage-selected-empty">
                <i class="ph ph-cursor-click"></i>
                <p>왼쪽 후보를 선택하거나 상품을 선택하면 목적별 비교를 볼 수 있습니다.</p>
            </div>
        `;
    }

    const selectedByPurpose = new Map(model.selectedRows.map((row) => [row.purposeKey, row]));
    return `
        <div class="pgm-usage-selected-head">
            <div>
                <h3>${pgmUsageEscape(model.selectedProduct.productName || model.selectedProduct.productId)}</h3>
                <p>${pgmUsageEscape(model.selectedProduct.productId)} · ${pgmUsageEscape(model.snapshotLabel)} · 목적 행 ${pgmUsageFormatNumber(model.selectedRows.length)}개</p>
            </div>
        </div>
        <div class="pgm-usage-selected-card-grid">
            ${PGM_USAGE_PURPOSES.map((purpose) => {
                const row = selectedByPurpose.get(purpose.key);
                return `
                    <article class="pgm-usage-selected-purpose-card ${model.state.selectedPurposeKey === purpose.key ? 'is-active' : ''}">
                        <div class="pgm-usage-selected-purpose-title">
                            <h4>${pgmUsageEscape(purpose.label)}</h4>
                            ${row ? pgmUsageBadge(pgmUsageLabel(PGM_USAGE_SCOPE_LABELS, row.scope), pgmUsageScopeTone(row.scope)) : pgmUsageBadge('행 없음', 'muted')}
                        </div>
                        ${row ? `
                            <div class="pgm-usage-purpose-facts">
                                <span>상태 <strong>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_STATUS_LABELS, row.status))}</strong></span>
                                <span>강도 <strong>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_LEVEL_LABELS, row.strength))}</strong></span>
                                <span>근거 <strong>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_LEVEL_LABELS, row.confidence))}</strong></span>
                                <span>방향 <strong>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_DIRECTION_LABELS, row.direction))}</strong></span>
                            </div>
                            <div class="pgm-usage-metric-stack">
                                <span>${pgmUsageEscape(pgmUsageMetricText(row.primaryMetric, row.primaryMetricValue))}</span>
                                <small>${pgmUsageEscape(pgmUsageMetricText(row.secondaryMetric, row.secondaryMetricValue))}</small>
                            </div>
                            <div class="pgm-usage-badge-stack">${pgmUsageFlagBadges(row)}</div>
                            <p>${pgmUsageEscape(pgmUsageInterpretation(row))}</p>
                            ${pgmUsageRenderRationale(row)}
                        ` : `
                            <p>이 상품에는 해당 목적의 산출 행이 아직 없습니다.</p>
                        `}
                    </article>
                `;
            }).join('')}
        </div>
    `;
}

function pgmUsageRenderRationale(row) {
    const labels = String(row?.rationaleCode || '')
        .split('|')
        .map((code) => PGM_USAGE_RATIONALE_LABELS[code])
        .filter(Boolean);
    if (!labels.length) return '';
    return `
        <div class="pgm-usage-rationale">
            ${labels.slice(0, 3).map((label) => `<span>${pgmUsageEscape(label)}</span>`).join('')}
        </div>
    `;
}

function pgmUsageRenderSelectedArea(model) {
    return `
        <aside class="pgm-usage-panel pgm-usage-selected-panel">
            <div class="pgm-usage-section-head">
                <div>
                    <h3>선택 상품 목적별 비교</h3>
                    <p>선택한 상품 안에서 목적별 상태와 주의점을 비교합니다.</p>
                </div>
            </div>
            ${pgmUsageRenderProductSelector(model)}
            ${pgmUsageRenderSelectedPurposeCards(model)}
        </aside>
    `;
}

function pgmUsageRenderEmpty() {
    return `
        <div class="pgm-usage-page">
            <section class="pgm-usage-toolbar">
                <div class="pgm-usage-toolbar-copy">
                    <span class="pgm-usage-eyebrow">검토용 초기 화면</span>
                    <h2>목적별 활용 검토</h2>
                    <p>목적별 활용 검토 데이터가 아직 없습니다. pgm_product_purpose_effects.csv를 업로드하면 후보 검토 화면이 표시됩니다.</p>
                </div>
            </section>
            <div class="pgm-usage-empty-state">
                <i class="ph ph-database"></i>
                <h3>목적별 활용 검토 CSV가 없습니다</h3>
                <p>필요 파일명: pgm_product_purpose_effects.csv</p>
                <button class="btn-primary" type="button" onclick="showUploadModal()">CSV 업로드</button>
            </div>
        </div>
    `;
}

function renderPgmUsage() {
    const container = document.getElementById('content-area');
    if (!container) return;

    const model = pgmUsageBuildModel();
    if (!model.rows.length) {
        container.innerHTML = pgmUsageRenderEmpty();
        if (typeof applyFriendlyUi === 'function') applyFriendlyUi(container);
        return;
    }

    container.innerHTML = `
        <div class="pgm-usage-page">
            ${pgmUsageRenderToolbar(model)}
            ${pgmUsageRenderContext(model)}
            ${pgmUsageRenderSummary(model)}
            <div class="pgm-usage-workspace">
                ${pgmUsageRenderGlobalArea(model)}
                ${pgmUsageRenderSelectedArea(model)}
            </div>
        </div>
    `;
    if (typeof applyFriendlyUi === 'function') applyFriendlyUi(container);
}

window.handlePgmUsageFilterChange = (field, value) => {
    const state = pgmUsageCurrentState();
    if (!Object.prototype.hasOwnProperty.call(state, field)) return;
    state[field] = value;
    renderPgmUsage();
};

window.selectPgmUsageProduct = (productId, purposeKey = '') => {
    const state = pgmUsageCurrentState();
    state.selectedProductId = String(productId || '').trim();
    state.selectedPurposeKey = String(purposeKey || '').trim();
    renderPgmUsage();
};

window.clearPgmUsageSelectedProduct = () => {
    const state = pgmUsageCurrentState();
    state.selectedProductId = '';
    state.selectedPurposeKey = '';
    renderPgmUsage();
};
