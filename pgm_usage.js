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
        key: 'basket-expansion',
        label: '함께 담기 확장',
        shortLabel: '함께 담기',
        guide: '동시 구매와 장바구니 확장 가능성을 검토합니다.'
    },
    {
        key: 'return-strength',
        label: '다시 찾는 구매 강화',
        shortLabel: '재방문 구매',
        guide: '반복 구매나 회귀 흐름의 근거를 검토합니다.'
    }
];

const PGM_USAGE_STATUS_LABELS = {
    operational_candidate: '운영 검토 후보',
    testable: '실험 검토 후보',
    hypothesis_only: '가설 수준',
    insufficient: '근거 부족',
    not_applicable: '비대상'
};

const PGM_USAGE_STATUS_ORDER = {
    operational_candidate: 5,
    testable: 4,
    hypothesis_only: 3,
    insufficient: 2,
    not_applicable: 1
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
    gift_or_freebie: '사은품/증정',
    threshold_or_reward: '조건부 리워드',
    service_or_non_merch: '비상품성 항목',
    trial_or_entry_kit: '체험/유입 키트',
    accessory_or_addon: '액세서리/추가구성',
    bundle_or_set: '세트/번들',
    core_merchandise: '일반 상품',
    unknown: '분류 미상'
};

const PGM_USAGE_METRIC_LABELS = {
    first_customer_cnt: '첫 구매 고객수',
    first_customer_ratio: '첫 구매 고객 비중',
    incoming_transition_customer_cnt_90d: '유입 전이 고객수',
    transition_rate_sum_effective_90d: '전이율 합계',
    attach_rate: '함께 담김률',
    breadth_lift: '확장 폭',
    entry_customer_ratio: '첫 구매 고객 비중',
    entry_demand_share: '신규 수요 비중',
    transition_rate: '다음 구매 전환율',
    return_customer_rate: '재구매 고객 비중',
    return_customer_rate_90d: '재구매 고객 비중',
    return_demand_share: '재구매 수요 비중',
    return_loop_rate_90d: '리턴 루프 비율',
    repeat_rate: '반복 구매율',
    revenue_90d: '최근 90일 매출',
    effect_signal_score: '신호 점수',
    effect_maturity_score: '성숙도 점수'
};

const PGM_USAGE_RATIONALE_LABELS = {
    eligibility_excluded: '적격성 정책상 목적 후보에서 제외됨',
    entry_evidence_insufficient: '신규 유입 목적 근거가 부족함',
    entry_signal_emerging: '신규 유입 신호가 초기 수준',
    entry_signal_present: '신규 유입 신호가 관측됨',
    entry_signal_strong: '신규 유입 신호가 강함',
    entry_hold_weak: '첫 구매 이후 유지 근거가 약함',
    transition_evidence_insufficient: '다음 구매 전이 근거가 부족함',
    transition_signal_emerging: '다음 구매 전이 신호가 초기 수준',
    transition_signal_present: '다음 구매 전이 신호가 관측됨',
    transition_signal_strong: '다음 구매 전이 신호가 강함',
    transition_compare_weak: '전이 경로 비교 구조가 약함',
    transition_slow: '관측 전이 속도가 느림',
    basket_evidence_insufficient: '함께 담기 근거가 부족함',
    basket_signal_emerging: '함께 담김 신호가 초기 수준',
    basket_signal_present: '함께 담김 근거가 관측됨',
    basket_signal_strong: '함께 담김 신호가 강함',
    basket_pattern_not_typed: '구체 조합 유형은 추가 확인 필요',
    basket_compare_weak: '비교 우위는 제한적',
    return_evidence_insufficient: '리턴 강화 근거가 부족함',
    return_signal_emerging: '리턴 강화 신호가 초기 수준',
    return_signal_present: '리턴 강화 신호가 관측됨',
    return_signal_strong: '리턴 강화 신호가 강함',
    return_compare_weak: '리턴 비교 구조가 약함',
    return_cycle_long: '리턴 주기가 길어 선행 확인이 필요함',
    downweighted_candidate: '후보 가중치가 낮게 반영됨',
    evidence_supported: '관측 근거가 판정에 반영됨'
};

const PGM_USAGE_RATIONALE_PREFIX_LABELS = {
    gift_or_freebie: '사은품/증정 제외 규칙',
    threshold_or_reward: '조건부 리워드 제외 규칙',
    service_or_non_merch: '비상품성 항목 제외 규칙',
    trial_or_entry_kit: '체험/유입 키트 보수 반영 규칙',
    accessory_or_addon: '액세서리/추가구성 보수 반영 규칙',
    bundle_or_set: '세트/번들 분류 규칙',
    core_merchandise: '일반 상품 기본 규칙',
    unknown: '상품명 분류 미상'
};

const PGM_USAGE_RELATED_CONTEXT_LABELS = {
    transition_source: '주요 유입 전이 상품',
    basket_companion: '주요 동반구매 상품',
    return_source: '주요 리턴 연관 상품'
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

function pgmUsageJoinKey(snapshotName, productId, purposeKey) {
    return [
        pgmUsageNormalizeKey(snapshotName),
        pgmUsageNormalizeKey(productId),
        pgmUsageNormalizeKey(purposeKey)
    ].join('::');
}

function pgmUsageNormalizeBoolean(value) {
    const normalized = String(value ?? '').trim().toLowerCase();
    return ['true', '1', 'yes', 'y'].includes(normalized);
}

function pgmUsageLabel(map, value, fallback = '-') {
    const key = pgmUsageNormalizeKey(value);
    return map[key] || key || fallback;
}

function pgmUsageSplitPipe(value) {
    return String(value || '')
        .split('|')
        .map((token) => token.trim())
        .filter(Boolean);
}

function pgmUsageRationaleLabel(code) {
    const raw = pgmUsageNormalizeKey(code);
    if (!raw) return '';
    if (PGM_USAGE_RATIONALE_LABELS[raw]) return PGM_USAGE_RATIONALE_LABELS[raw];
    const [prefix, detail] = raw.split(':', 2);
    if (PGM_USAGE_RATIONALE_PREFIX_LABELS[prefix]) {
        return detail
            ? `${PGM_USAGE_RATIONALE_PREFIX_LABELS[prefix]} · ${detail}`
            : PGM_USAGE_RATIONALE_PREFIX_LABELS[prefix];
    }
    return `추가 근거 코드 · ${raw}`;
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
        observedAvgDaysToTransition90d: normalized.observed_avg_days_to_transition_90d ?? normalized.incoming_avg_days_to_transition_90d,
        transitionSourceProductIdsTop3: pgmUsageNormalizeKey(normalized.transition_source_product_ids_top3),
        transitionSourceProductNamesTop3: pgmUsageNormalizeKey(normalized.transition_source_product_names_top3),
        basketRelatedProductIdsTop3: pgmUsageNormalizeKey(normalized.basket_related_product_ids_top3),
        basketRelatedProductNamesTop3: pgmUsageNormalizeKey(normalized.basket_related_product_names_top3),
        returnSourceProductIdsTop3: pgmUsageNormalizeKey(normalized.return_source_product_ids_top3),
        returnSourceProductNamesTop3: pgmUsageNormalizeKey(normalized.return_source_product_names_top3),
        merchandiseRole: pgmUsageNormalizeKey(normalized.merchandise_role),
        eligibility: pgmUsageNormalizeKey(normalized.purpose_candidate_eligibility),
        weightMultiplier: normalized.purpose_candidate_weight_multiplier,
        eligibilityRuleMatched: pgmUsageNormalizeKey(normalized.eligibility_rule_matched),
        action: null
    };
}

function pgmUsageNormalizeActionRow(row) {
    const normalized = typeof normalizeCsvRows === 'function'
        ? normalizeCsvRows([row])[0]
        : (row || {});
    return {
        snapshotName: pgmUsageNormalizeKey(normalized.snapshot_name),
        productId: pgmUsageNormalizeKey(normalized.product_id),
        productName: pgmUsageNormalizeKey(normalized.product_name_latest),
        purposeKey: pgmUsageNormalizeKey(normalized.purpose_key),
        status: pgmUsageNormalizeKey(normalized.effect_status),
        scope: pgmUsageNormalizeKey(normalized.effect_scope),
        confidence: pgmUsageNormalizeKey(normalized.effect_confidence),
        recommendedActionType: pgmUsageNormalizeKey(normalized.recommended_action_type),
        recommendedActionKo: pgmUsageNormalizeKey(normalized.recommended_action_ko),
        expectedEffectKo: pgmUsageNormalizeKey(normalized.expected_effect_ko),
        expectedEffectMetric: pgmUsageNormalizeKey(normalized.expected_effect_metric),
        expectedEffectDirection: pgmUsageNormalizeKey(normalized.expected_effect_direction),
        expectedEffectConfidence: pgmUsageNormalizeKey(normalized.expected_effect_confidence),
        actionPriority: pgmUsageNumber(normalized.action_priority, NaN),
        actionRationaleCode: pgmUsageNormalizeKey(normalized.action_rationale_code),
        guardrailKo: pgmUsageNormalizeKey(normalized.guardrail_ko),
        basketRelatedProductsTop3Ko: pgmUsageNormalizeKey(normalized.basket_related_products_top3_ko),
        relatedProductContextType: pgmUsageNormalizeKey(normalized.related_product_context_type),
        relatedProductIdsTop3: pgmUsageNormalizeKey(normalized.related_product_ids_top3),
        relatedProductNamesTop3: pgmUsageNormalizeKey(normalized.related_product_names_top3),
        sourceEffectPrimaryMetric: pgmUsageNormalizeKey(normalized.source_effect_primary_metric),
        sourceEffectPrimaryMetricValue: normalized.source_effect_primary_metric_value,
        sourceEffectSecondaryMetric: pgmUsageNormalizeKey(normalized.source_effect_secondary_metric),
        sourceEffectSecondaryMetricValue: normalized.source_effect_secondary_metric_value
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
    const actionIndex = pgmUsageActionIndex();
    return (source || [])
        .map(pgmUsageNormalizeRow)
        .filter((row) => row.productId && row.purposeKey)
        .map((row) => ({
            ...row,
            action: actionIndex.get(pgmUsageJoinKey(row.snapshotName, row.productId, row.purposeKey)) || null
        }));
}

function pgmUsageActionRows() {
    const source = AppState?.data?.productPurposeActionCandidates || AppState?.rawData?.productPurposeActionCandidates || [];
    return (source || [])
        .map(pgmUsageNormalizeActionRow)
        .filter((row) => row.snapshotName && row.productId && row.purposeKey);
}

function pgmUsageActionIndex() {
    const index = new Map();
    pgmUsageActionRows().forEach((row) => {
        const key = pgmUsageJoinKey(row.snapshotName, row.productId, row.purposeKey);
        const existing = index.get(key);
        if (!existing || pgmUsageNumber(row.actionPriority, -1) > pgmUsageNumber(existing.actionPriority, -1)) {
            index.set(key, row);
        }
    });
    return index;
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
            compareSearch: '',
            selectedProductId: '',
            compareProductId: '',
            selectedPurposeKey: '',
            actionReviewOpen: true
        };
    }
    if (typeof AppState.viewState.pgmUsage.compareProductId !== 'string') {
        AppState.viewState.pgmUsage.compareProductId = '';
    }
    if (typeof AppState.viewState.pgmUsage.selectedPurposeKey !== 'string') {
        AppState.viewState.pgmUsage.selectedPurposeKey = '';
    }
    if (typeof AppState.viewState.pgmUsage.compareSearch !== 'string') {
        AppState.viewState.pgmUsage.compareSearch = '';
    }
    if (typeof AppState.viewState.pgmUsage.actionReviewOpen !== 'boolean') {
        AppState.viewState.pgmUsage.actionReviewOpen = true;
    }
    if (
        AppState.viewState.pgmUsage.compareProductId
        && AppState.viewState.pgmUsage.compareProductId === AppState.viewState.pgmUsage.selectedProductId
    ) {
        AppState.viewState.pgmUsage.compareProductId = '';
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

function pgmUsageRowsForProduct(rows, productId) {
    return rows
        .filter((row) => row.productId === productId)
        .sort((a, b) => {
            const aIndex = PGM_USAGE_PURPOSES.findIndex((purpose) => purpose.key === a.purposeKey);
            const bIndex = PGM_USAGE_PURPOSES.findIndex((purpose) => purpose.key === b.purposeKey);
            return (aIndex === -1 ? 99 : aIndex) - (bIndex === -1 ? 99 : bIndex);
        });
}

function pgmUsageSearchProducts(products, search, excludedProductId = '', preservedProductId = '') {
    const query = String(search || '').trim().toLowerCase();
    return (products || []).filter((product) => {
        if (product.productId === excludedProductId) return false;
        if (product.productId === preservedProductId) return true;
        if (!query) return true;
        const haystack = `${product.productName || ''} ${product.productId || ''}`.toLowerCase();
        return haystack.includes(query);
    });
}

function pgmUsageBuildModel() {
    const state = pgmUsageCurrentState();
    const rows = pgmUsageRows();
    const actionRows = pgmUsageActionRows();
    const filteredRows = pgmUsageApplyFilters(rows, state).sort(pgmUsageCompareRows);
    const products = pgmUsageProducts(rows);
    const selectedProduct = products.find((product) => product.productId === state.selectedProductId) || null;
    const compareProduct = products.find((product) => product.productId === state.compareProductId) || null;
    if (!selectedProduct && state.selectedProductId) state.selectedProductId = '';
    if (!compareProduct && state.compareProductId) state.compareProductId = '';
    const selectedRows = selectedProduct ? pgmUsageRowsForProduct(rows, selectedProduct.productId) : [];
    const compareRows = compareProduct ? pgmUsageRowsForProduct(rows, compareProduct.productId) : [];
    const compareCandidates = selectedProduct
        ? pgmUsageSearchProducts(products, state.compareSearch, selectedProduct.productId, state.compareProductId)
        : [];

    return {
        rows,
        actionRows,
        filteredRows,
        products,
        selectedProduct,
        compareProduct,
        selectedRows,
        compareRows,
        compareCandidates,
        selectedCount: [selectedProduct, compareProduct].filter(Boolean).length,
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
            <span>액션 후보: <strong>${pgmUsageFormatNumber(model.actionRows.length)}</strong></span>
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
                                <button type="button" class="pgm-usage-candidate-chip"
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
                            <th>선택</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${rows.length ? rows.map((row) => `
                            <tr>
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
                                <td>
                                    <div class="pgm-usage-row-actions">
                                        <button type="button" onclick="selectPgmUsageProduct('${pgmUsageEscapeJsAttr(row.productId)}', '${pgmUsageEscapeJsAttr(row.purposeKey)}')">가이드 보기</button>
                                        <button type="button" onclick="addPgmUsageCompareProduct('${pgmUsageEscapeJsAttr(row.productId)}', '${pgmUsageEscapeJsAttr(row.purposeKey)}')">비교 추가</button>
                                    </div>
                                </td>
                            </tr>
                        `).join('') : `
                            <tr>
                                <td colspan="9" class="pgm-usage-empty-cell">현재 필터에서 표시할 후보가 없습니다.</td>
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
    const compareSearch = String(model.state.compareSearch || '').trim();
    const compareCandidates = model.compareCandidates || [];
    const compareMetaText = !model.state.selectedProductId
        ? ''
        : !compareCandidates.length
            ? '검색 결과가 없어요. 검색어를 바꾸거나 왼쪽 후보 테이블의 비교 추가 버튼을 사용할 수 있어요.'
            : compareSearch
                ? `검색 결과 ${pgmUsageFormatNumber(compareCandidates.length)}개 · 선택하면 바로 비교가 열립니다.`
                : `비교 가능한 다른 상품 ${pgmUsageFormatNumber(compareCandidates.length)}개 · 검색해서 빠르게 찾을 수 있어요.`;
    return `
        <div class="pgm-usage-product-selector">
            <label>
                <span>기준 상품</span>
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
            ${model.state.selectedProductId ? `
                <label class="pgm-usage-compare-slot">
                    <span>비교 추가</span>
                    <input type="search" value="${pgmUsageEscape(model.state.compareSearch)}" placeholder="비교할 상품명 또는 ID 검색"
                        oninput="handlePgmUsageCompareSearch(this.value)">
                    <select onchange="addPgmUsageCompareProduct(this.value, '')">
                        <option value="">${pgmUsageEscape(compareSearch ? '검색 결과에서 상품을 선택하세요' : '두 번째 상품을 선택하세요')}</option>
                        ${compareCandidates.map((product) => `
                            <option value="${pgmUsageEscape(product.productId)}" ${model.state.compareProductId === product.productId ? 'selected' : ''}>
                                ${pgmUsageEscape(product.productName)} (${pgmUsageEscape(product.productId)})
                            </option>
                        `).join('')}
                    </select>
                    <small class="pgm-usage-compare-search-meta">${pgmUsageEscape(compareMetaText)}</small>
                </label>
                ${model.state.compareProductId ? `
                    <button type="button" onclick="clearPgmUsageCompareProduct()">비교 해제</button>
                ` : '<span class="pgm-usage-selector-hint">두 상품까지만 비교할 수 있습니다.</span>'}
            ` : ''}
        </div>
    `;
}

function pgmUsageRowsByPurpose(rows) {
    return new Map((rows || []).map((row) => [row.purposeKey, row]));
}

function pgmUsageFormatRelatedProducts(idsText, namesText, maxItems = 3) {
    const ids = pgmUsageSplitPipe(idsText);
    const names = pgmUsageSplitPipe(namesText);
    const items = [];
    for (let i = 0; i < maxItems; i += 1) {
        const name = names[i] || '';
        const id = ids[i] || '';
        if (name && id) items.push(`${name}(${id})`);
        else if (name) items.push(name);
        else if (id) items.push(id);
    }
    return items.join(', ');
}

function pgmUsageEffectRelatedContext(row) {
    if (!row) return null;
    if (row.purposeKey === 'next-purchase') {
        const text = pgmUsageFormatRelatedProducts(row.transitionSourceProductIdsTop3, row.transitionSourceProductNamesTop3);
        return text ? { label: '전이 유입 상품', text } : null;
    }
    if (row.purposeKey === 'basket-expansion') {
        const text = pgmUsageFormatRelatedProducts(row.basketRelatedProductIdsTop3, row.basketRelatedProductNamesTop3);
        return text ? { label: '동반구매 상품', text } : null;
    }
    if (row.purposeKey === 'return-strength') {
        const text = pgmUsageFormatRelatedProducts(row.returnSourceProductIdsTop3, row.returnSourceProductNamesTop3);
        return text ? { label: '리턴 연관 상품', text } : null;
    }
    return null;
}

function pgmUsageTransitionDaysText(row) {
    if (!row || row.purposeKey !== 'next-purchase') return '';
    const days = pgmUsageNumber(row.observedAvgDaysToTransition90d, NaN);
    if (!Number.isFinite(days) || days <= 0) return '';
    return `관측 평균 전이 약 ${Math.round(days)}일`;
}

function pgmUsageRenderRationale(row) {
    const labels = pgmUsageSplitPipe(row?.rationaleCode)
        .map(pgmUsageRationaleLabel)
        .filter(Boolean);
    if (!labels.length) return '';
    return `
        <div class="pgm-usage-rationale" aria-label="근거 코드">
            ${labels.slice(0, 4).map((label) => `<span>${pgmUsageEscape(label)}</span>`).join('')}
        </div>
    `;
}

function pgmUsageRenderEffectContext(row) {
    if (!row) return '';
    const related = pgmUsageEffectRelatedContext(row);
    const transitionDays = pgmUsageTransitionDaysText(row);
    const eligibilityText = row.eligibilityRuleMatched || row.eligibility
        ? `${pgmUsageLabel(PGM_USAGE_ELIGIBILITY_LABELS, row.eligibility, '적격성 미상')}${row.eligibilityRuleMatched ? ` · ${row.eligibilityRuleMatched}` : ''}`
        : '';
    return `
        <div class="pgm-usage-purpose-facts">
            <span>상태 <strong>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_STATUS_LABELS, row.status))}</strong></span>
            <span>검토 범위 <strong>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_SCOPE_LABELS, row.scope))}</strong></span>
            <span>강도 <strong>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_LEVEL_LABELS, row.strength))}</strong></span>
            <span>근거 <strong>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_LEVEL_LABELS, row.confidence))}</strong></span>
        </div>
        <div class="pgm-usage-metric-stack">
            <span>${pgmUsageEscape(pgmUsageMetricText(row.primaryMetric, row.primaryMetricValue))}</span>
            <small>${pgmUsageEscape(pgmUsageMetricText(row.secondaryMetric, row.secondaryMetricValue))}</small>
            <small>신호 ${pgmUsageFormatNumber(row.signalScore, 2)} · 성숙도 ${pgmUsageFormatNumber(row.maturityScore, 2)}</small>
        </div>
        <div class="pgm-usage-context-list">
            ${eligibilityText ? `<p><strong>적격성</strong><span>${pgmUsageEscape(eligibilityText)}</span></p>` : ''}
            ${transitionDays ? `<p><strong>전이일수</strong><span>${pgmUsageEscape(transitionDays)}</span></p>` : ''}
            ${related ? `<p><strong>${pgmUsageEscape(related.label)}</strong><span>${pgmUsageEscape(related.text)}</span></p>` : ''}
        </div>
        <div class="pgm-usage-badge-stack">${pgmUsageFlagBadges(row)}</div>
        ${pgmUsageRenderRationale(row)}
    `;
}

function pgmUsageRenderActionReview(row, mode = 'single') {
    if (!row) return '';
    const action = row.action;
    if (!action) {
        return `
            <div class="pgm-usage-action-review is-missing">
                <strong>액션 검토</strong>
                <p>연결된 액션 후보를 찾지 못했어요. 지금은 효과 판정만 기준으로 검토합니다.</p>
            </div>
        `;
    }
    const relatedText = action.basketRelatedProductsTop3Ko
        || pgmUsageFormatRelatedProducts(action.relatedProductIdsTop3, action.relatedProductNamesTop3);
    const relatedLabel = PGM_USAGE_RELATED_CONTEXT_LABELS[action.relatedProductContextType] || '연관 상품';
    return `
        <div class="pgm-usage-action-review ${mode === 'compare' ? 'is-compact' : ''}">
            <strong>액션 검토</strong>
            <p>${pgmUsageEscape(action.recommendedActionKo || '추천 액션 문구 없음')}</p>
            ${action.expectedEffectKo ? `<p><span>기대효과</span>${pgmUsageEscape(action.expectedEffectKo)}</p>` : ''}
            ${action.guardrailKo ? `<p><span>주의</span>${pgmUsageEscape(action.guardrailKo)}</p>` : ''}
            ${relatedText ? `<p><span>${pgmUsageEscape(relatedLabel)}</span>${pgmUsageEscape(relatedText)}</p>` : ''}
            <small>우선순위 ${pgmUsageFormatNumber(action.actionPriority)} · 기대 지표 ${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_METRIC_LABELS, action.expectedEffectMetric, action.expectedEffectMetric || '-'))}</small>
        </div>
    `;
}

function pgmUsagePairCautionScore(row) {
    if (!row) return 0;
    let score = 0;
    if (row.preconditionFlag) score += 1;
    if (row.riskFlag) score += 1;
    if (row.eligibility === 'downweight') score += 1;
    if (row.eligibility === 'exclude') score += 2;
    return score;
}

function pgmUsagePairGuardrailText(row) {
    const bits = [];
    if (row?.preconditionFlag) bits.push('전제조건');
    if (row?.riskFlag) bits.push('리스크');
    if (row?.eligibility === 'downweight') bits.push('제한 반영');
    if (row?.eligibility === 'exclude') bits.push('제외');
    return bits.join(', ');
}

function pgmUsagePairComparison(rowA, rowB) {
    const fields = [
        {
            key: 'scope',
            label: '검토 범위',
            rankA: PGM_USAGE_SCOPE_ORDER[rowA?.scope] || 0,
            rankB: PGM_USAGE_SCOPE_ORDER[rowB?.scope] || 0
        },
        {
            key: 'status',
            label: '상태',
            rankA: PGM_USAGE_STATUS_ORDER[rowA?.status] || 0,
            rankB: PGM_USAGE_STATUS_ORDER[rowB?.status] || 0
        },
        {
            key: 'confidence',
            label: '근거 수준',
            rankA: PGM_USAGE_LEVEL_ORDER[rowA?.confidence] || 0,
            rankB: PGM_USAGE_LEVEL_ORDER[rowB?.confidence] || 0
        },
        {
            key: 'strength',
            label: '효과 강도',
            rankA: PGM_USAGE_LEVEL_ORDER[rowA?.strength] || 0,
            rankB: PGM_USAGE_LEVEL_ORDER[rowB?.strength] || 0
        }
    ];
    const firstDiffIndex = fields.findIndex((field) => field.rankA !== field.rankB);
    if (firstDiffIndex === -1) {
        return {
            kind: 'same',
            fields,
            firstDiff: null,
            winnerKey: ''
        };
    }
    const firstDiff = fields[firstDiffIndex];
    return {
        kind: 'different',
        fields,
        firstDiff,
        winnerKey: firstDiff.rankA > firstDiff.rankB ? 'a' : 'b',
        firstDiffIndex
    };
}

function pgmUsagePairReason(fieldKey, winnerRow) {
    if (fieldKey === 'scope') {
        return `검토 범위가 ${pgmUsageLabel(PGM_USAGE_SCOPE_LABELS, winnerRow.scope)} 쪽으로 더 앞서 보여`;
    }
    if (fieldKey === 'status') {
        return `상태가 ${pgmUsageLabel(PGM_USAGE_STATUS_LABELS, winnerRow.status)} 쪽으로 더 앞서 보여`;
    }
    if (fieldKey === 'confidence') {
        return '검토 범위와 상태는 비슷하지만 근거 수준 표기가 더 높아';
    }
    if (fieldKey === 'strength') {
        return '검토 범위·상태·근거 수준은 비슷하지만 효과 강도 표기가 더 높아';
    }
    return '표시된 계약 필드 기준으로 조금 더 앞서 보여';
}

function pgmUsagePairInterpretation(rowA, productA, rowB, productB) {
    if (!rowA && !rowB) return '두 상품 모두 이 목적의 산출 행이 아직 없습니다.';
    if (rowA && !rowB) return `${productA.productName || productA.productId}만 이 목적의 산출 행이 있습니다.`;
    if (!rowA && rowB) return `${productB.productName || productB.productId}만 이 목적의 산출 행이 있습니다.`;
    const comparison = pgmUsagePairComparison(rowA, rowB);
    if (comparison.kind === 'same') {
        const cautionGap = Math.abs(pgmUsagePairCautionScore(rowA) - pgmUsagePairCautionScore(rowB));
        if (cautionGap) {
            return '현재 두 상품은 검토 범위·상태·근거 수준·효과 강도가 비슷합니다. 다만 전제조건·리스크 또는 적격성 차이가 있어 주의 조건까지 같이 확인하는 편이 안전합니다. 전체 상품 순위가 아니라 현재 두 상품 사이의 해석입니다.';
        }
        return '현재 두 상품은 검토 범위·상태·근거 수준·효과 강도가 비슷하게 읽힙니다. 세부 지표와 플래그를 함께 확인합니다. 전체 상품 순위가 아니라 현재 두 상품 사이의 해석입니다.';
    }

    const winner = comparison.winnerKey === 'a' ? productA : productB;
    const loser = comparison.winnerKey === 'a' ? productB : productA;
    const winnerRow = comparison.winnerKey === 'a' ? rowA : rowB;
    const loserRow = comparison.winnerKey === 'a' ? rowB : rowA;
    const winnerName = winner.productName || winner.productId;
    const loserName = loser.productName || loser.productId;
    const winnerCautionScore = pgmUsagePairCautionScore(winnerRow);
    const loserCautionScore = pgmUsagePairCautionScore(loserRow);
    const cautionGap = winnerCautionScore - loserCautionScore;
    const subtleRead = comparison.firstDiffIndex >= 2 || cautionGap > 0;
    const reason = pgmUsagePairReason(comparison.firstDiff.key, winnerRow);

    if (subtleRead) {
        const guardrailText = cautionGap > 0 && pgmUsagePairGuardrailText(winnerRow)
            ? ` 다만 ${pgmUsagePairGuardrailText(winnerRow)}가 함께 보여 보수적으로 읽어야 합니다.`
            : ' 차이가 큰 편은 아니라 보수적으로 읽는 편이 맞습니다.';
        return `${winnerName} 쪽이 ${loserName}보다 이 목적에서는 조금 더 강함으로 읽히지만, ${reason}.${guardrailText} 전체 상품 순위가 아니라 현재 두 상품 사이의 해석입니다.`;
    }

    const guardrailText = cautionGap > 0 && pgmUsagePairGuardrailText(winnerRow)
        ? ` 다만 ${pgmUsagePairGuardrailText(winnerRow)}가 있어 실행 판단은 별도 검토가 필요합니다.`
        : '';
    return `${winnerName} 쪽이 ${loserName}보다 이 목적에서는 더 강함으로 읽힙니다. ${reason}.${guardrailText} 전체 상품 순위가 아니라 현재 두 상품 사이의 해석입니다.`;
}

function pgmUsageRenderSelectedPurposeCards(model) {
    if (!model.selectedProduct) {
        return `
            <div class="pgm-usage-selected-empty">
                <i class="ph ph-cursor-click"></i>
                <p>후보 테이블에서 가이드 보기를 누르거나 기준 상품을 선택하면 활용 가이드가 열립니다.</p>
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
                            <p>${pgmUsageEscape(pgmUsageInterpretation(row))}</p>
                            ${pgmUsageRenderEffectContext(row)}
                            ${model.state.actionReviewOpen ? pgmUsageRenderActionReview(row) : ''}
                        ` : `
                            <p>이 상품에는 해당 목적의 산출 행이 아직 없습니다.</p>
                        `}
                    </article>
                `;
            }).join('')}
        </div>
    `;
}

function pgmUsageRenderCompareColumn(product, row) {
    if (!row) {
        return `
            <div class="pgm-usage-compare-column">
                <h5>${pgmUsageEscape(product.productName || product.productId)}</h5>
                <p class="pgm-usage-muted-copy">이 목적의 산출 행이 없습니다.</p>
            </div>
        `;
    }
    return `
        <div class="pgm-usage-compare-column">
            <h5>${pgmUsageEscape(product.productName || product.productId)}</h5>
            ${pgmUsageBadge(pgmUsageLabel(PGM_USAGE_SCOPE_LABELS, row.scope), pgmUsageScopeTone(row.scope))}
            <div class="pgm-usage-metric-stack">
                <span>${pgmUsageEscape(pgmUsageMetricText(row.primaryMetric, row.primaryMetricValue))}</span>
                <small>${pgmUsageEscape(pgmUsageMetricText(row.secondaryMetric, row.secondaryMetricValue))}</small>
                <small>상태 ${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_STATUS_LABELS, row.status))} · 근거 ${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_LEVEL_LABELS, row.confidence))}</small>
            </div>
            <div class="pgm-usage-badge-stack">${pgmUsageFlagBadges(row)}</div>
            ${pgmUsageRenderRationale(row)}
        </div>
    `;
}

function pgmUsageRenderCompareActionColumn(product, row) {
    return `
        <div class="pgm-usage-compare-action-column">
            <h5>${pgmUsageEscape(product.productName || product.productId)}</h5>
            ${row ? pgmUsageRenderActionReview(row, 'compare') : '<p class="pgm-usage-muted-copy">효과 행이 없어 액션 후보를 연결하지 않았습니다.</p>'}
        </div>
    `;
}

function pgmUsageRenderComparisonCards(model) {
    if (!model.selectedProduct || !model.compareProduct) return pgmUsageRenderSelectedPurposeCards(model);
    const primaryByPurpose = pgmUsageRowsByPurpose(model.selectedRows);
    const compareByPurpose = pgmUsageRowsByPurpose(model.compareRows);
    return `
        <div class="pgm-usage-selected-head">
            <div>
                <h3>${pgmUsageEscape(model.selectedProduct.productName || model.selectedProduct.productId)} ↔ ${pgmUsageEscape(model.compareProduct.productName || model.compareProduct.productId)}</h3>
                <p>${pgmUsageEscape(model.snapshotLabel)} · 현재 선택한 두 상품의 목적별 효과만 비교합니다.</p>
            </div>
        </div>
        <div class="pgm-usage-selected-card-grid">
            ${PGM_USAGE_PURPOSES.map((purpose) => {
                const rowA = primaryByPurpose.get(purpose.key);
                const rowB = compareByPurpose.get(purpose.key);
                return `
                    <article class="pgm-usage-selected-purpose-card ${model.state.selectedPurposeKey === purpose.key ? 'is-active' : ''}">
                        <div class="pgm-usage-selected-purpose-title">
                            <h4>${pgmUsageEscape(purpose.label)}</h4>
                            <span class="pgm-usage-badge is-plain">쌍 비교</span>
                        </div>
                        <p class="pgm-usage-pair-read">${pgmUsageEscape(pgmUsagePairInterpretation(rowA, model.selectedProduct, rowB, model.compareProduct))}</p>
                        <div class="pgm-usage-compare-grid">
                            ${pgmUsageRenderCompareColumn(model.selectedProduct, rowA)}
                            ${pgmUsageRenderCompareColumn(model.compareProduct, rowB)}
                        </div>
                        <div class="pgm-usage-context-list">
                            ${rowA ? pgmUsageRenderCompactPairContext(model.selectedProduct, rowA) : ''}
                            ${rowB ? pgmUsageRenderCompactPairContext(model.compareProduct, rowB) : ''}
                        </div>
                        ${model.state.actionReviewOpen ? `
                            <div class="pgm-usage-action-compare">
                                <strong>지원 액션 검토</strong>
                                <div class="pgm-usage-compare-grid">
                                    ${pgmUsageRenderCompareActionColumn(model.selectedProduct, rowA)}
                                    ${pgmUsageRenderCompareActionColumn(model.compareProduct, rowB)}
                                </div>
                            </div>
                        ` : ''}
                    </article>
                `;
            }).join('')}
        </div>
    `;
}

function pgmUsageRenderCompactPairContext(product, row) {
    const bits = [];
    const transitionDays = pgmUsageTransitionDaysText(row);
    const related = pgmUsageEffectRelatedContext(row);
    if (row.eligibilityRuleMatched) bits.push(`적격성 ${row.eligibilityRuleMatched}`);
    if (transitionDays) bits.push(transitionDays);
    if (related) bits.push(`${related.label} ${related.text}`);
    if (!bits.length) return '';
    return `<p><strong>${pgmUsageEscape(product.productName || product.productId)}</strong><span>${pgmUsageEscape(bits.join(' · '))}</span></p>`;
}

function pgmUsageRenderSelectedArea(model) {
    const isCompare = model.selectedProduct && model.compareProduct;
    const title = isCompare ? '선택 상품 효과 비교' : '선택 상품 활용 가이드';
    const copy = isCompare
        ? '두 상품 사이의 목적별 효과 차이만 해석합니다. 전체 후보 순위로 읽지 않습니다.'
        : '선택한 상품의 목적별 효과, 조건, 지원 액션을 함께 확인합니다.';
    return `
        <aside class="pgm-usage-panel pgm-usage-selected-panel">
            <div class="pgm-usage-section-head">
                <div>
                    <h3>${pgmUsageEscape(title)}</h3>
                    <p>${pgmUsageEscape(copy)}</p>
                </div>
                <button type="button" class="pgm-usage-action-toggle ${model.state.actionReviewOpen ? 'is-on' : ''}" onclick="togglePgmUsageActionReview()">
                    액션 검토 ${model.state.actionReviewOpen ? 'ON' : 'OFF'}
                </button>
            </div>
            ${pgmUsageRenderProductSelector(model)}
            ${isCompare ? pgmUsageRenderComparisonCards(model) : pgmUsageRenderSelectedPurposeCards(model)}
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
    state.compareProductId = '';
    state.compareSearch = '';
    renderPgmUsage();
};

window.handlePgmUsageCompareSearch = (value) => {
    const state = pgmUsageCurrentState();
    state.compareSearch = String(value || '').trim();
    renderPgmUsage();
};

window.addPgmUsageCompareProduct = (productId, purposeKey = '') => {
    const state = pgmUsageCurrentState();
    const nextId = String(productId || '').trim();
    if (!nextId) {
        state.compareProductId = '';
        renderPgmUsage();
        return;
    }
    if (!state.selectedProductId) {
        state.selectedProductId = nextId;
        state.compareProductId = '';
    } else if (nextId !== state.selectedProductId) {
        state.compareProductId = nextId;
    }
    state.selectedPurposeKey = String(purposeKey || state.selectedPurposeKey || '').trim();
    renderPgmUsage();
};

window.clearPgmUsageCompareProduct = () => {
    const state = pgmUsageCurrentState();
    state.compareProductId = '';
    renderPgmUsage();
};

window.togglePgmUsageActionReview = () => {
    const state = pgmUsageCurrentState();
    state.actionReviewOpen = !state.actionReviewOpen;
    renderPgmUsage();
};

window.clearPgmUsageSelectedProduct = () => {
    const state = pgmUsageCurrentState();
    state.selectedProductId = '';
    state.compareProductId = '';
    state.compareSearch = '';
    state.selectedPurposeKey = '';
    renderPgmUsage();
};
