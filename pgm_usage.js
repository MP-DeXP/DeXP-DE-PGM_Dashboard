// Purpose usage review page logic

const PGM_USAGE_COMPARE_SEARCH_DEBOUNCE_MS = 180;
let pgmUsageCompareSearchTimer = null;

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

const PGM_USAGE_STRUCTURAL_SIGNAL_LABELS = {
    strong: '구조 신호 강함',
    medium: '구조 신호 보통',
    weak: '구조 신호 약함'
};

const PGM_USAGE_EXECUTION_READINESS_LABELS = {
    ready: '실행 준비됨',
    exploratory: '탐색 필요',
    not_ready: '준비 전'
};

const PGM_USAGE_OPERATIONAL_SAFETY_LABELS = {
    safe: '운영 안전',
    guarded: '주의 필요',
    fragile: '취약'
};

const PGM_USAGE_PURPOSE_SUBTYPE_LABELS = {
    'basket-breadth': '장바구니 확장형',
    'checkout-cross-sell': '체크아웃 교차추천형',
    'bundle-anchor': '번들 앵커형'
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

const PGM_USAGE_STRUCTURAL_SIGNAL_ORDER = {
    strong: 3,
    medium: 2,
    weak: 1
};

const PGM_USAGE_EXECUTION_READINESS_ORDER = {
    ready: 3,
    exploratory: 2,
    not_ready: 1
};

const PGM_USAGE_OPERATIONAL_SAFETY_ORDER = {
    safe: 3,
    guarded: 2,
    fragile: 1
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

function pgmUsageNormalizeSearchText(value) {
    return String(value ?? '')
        .toLowerCase()
        .replace(/\u00a0/g, ' ');
}

function pgmUsageTokenizeSearchText(value) {
    return pgmUsageNormalizeSearchText(value)
        .replace(/\s+/g, ' ')
        .trim()
        .split(' ')
        .filter(Boolean);
}

function pgmUsageMatchesSearchText(haystack, search) {
    const tokens = pgmUsageTokenizeSearchText(search);
    if (!tokens.length) return true;
    const normalizedHaystack = pgmUsageNormalizeSearchText(haystack).replace(/\s+/g, ' ');
    const compactHaystack = normalizedHaystack.replace(/\s+/g, '');
    return tokens.every((token) => {
        const compactToken = token.replace(/\s+/g, '');
        return normalizedHaystack.includes(token) || compactHaystack.includes(compactToken);
    });
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
        structuralSignal: pgmUsageNormalizeKey(normalized.structural_signal),
        executionReadiness: pgmUsageNormalizeKey(normalized.execution_readiness),
        operationalSafety: pgmUsageNormalizeKey(normalized.operational_safety),
        explanationConfidence: pgmUsageNormalizeKey(normalized.explanation_confidence || normalized.effect_confidence),
        purposeSubtype: pgmUsageNormalizeKey(normalized.purpose_subtype),
        compoundCandidateHint: pgmUsageNormalizeKey(normalized.compound_candidate_hint),
        relationSummaryKo: pgmUsageNormalizeKey(normalized.relation_summary_ko),
        relationRiskSummaryKo: pgmUsageNormalizeKey(normalized.relation_risk_summary_ko),
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
        structuralSignal: pgmUsageNormalizeKey(normalized.structural_signal),
        executionReadiness: pgmUsageNormalizeKey(normalized.execution_readiness),
        operationalSafety: pgmUsageNormalizeKey(normalized.operational_safety),
        explanationConfidence: pgmUsageNormalizeKey(normalized.explanation_confidence || normalized.expected_effect_confidence || normalized.effect_confidence),
        purposeSubtype: pgmUsageNormalizeKey(normalized.purpose_subtype),
        compoundCandidateHint: pgmUsageNormalizeKey(normalized.compound_candidate_hint),
        relationSummaryKo: pgmUsageNormalizeKey(normalized.relation_summary_ko),
        relationRiskSummaryKo: pgmUsageNormalizeKey(normalized.relation_risk_summary_ko),
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
    const structuralDiff = (PGM_USAGE_STRUCTURAL_SIGNAL_ORDER[pgmUsageStructuralSignalValue(b)] || 0) - (PGM_USAGE_STRUCTURAL_SIGNAL_ORDER[pgmUsageStructuralSignalValue(a)] || 0);
    if (structuralDiff) return structuralDiff;
    const readinessDiff = (PGM_USAGE_EXECUTION_READINESS_ORDER[pgmUsageExecutionReadinessValue(b)] || 0) - (PGM_USAGE_EXECUTION_READINESS_ORDER[pgmUsageExecutionReadinessValue(a)] || 0);
    if (readinessDiff) return readinessDiff;
    const safetyDiff = (PGM_USAGE_OPERATIONAL_SAFETY_ORDER[pgmUsageOperationalSafetyValue(b)] || 0) - (PGM_USAGE_OPERATIONAL_SAFETY_ORDER[pgmUsageOperationalSafetyValue(a)] || 0);
    if (safetyDiff) return safetyDiff;
    const explanationDiff = (PGM_USAGE_LEVEL_ORDER[pgmUsageExplanationValue(b)] || 0) - (PGM_USAGE_LEVEL_ORDER[pgmUsageExplanationValue(a)] || 0);
    if (explanationDiff) return explanationDiff;
    const scopeDiff = (PGM_USAGE_SCOPE_ORDER[b.scope] || 0) - (PGM_USAGE_SCOPE_ORDER[a.scope] || 0);
    if (scopeDiff) return scopeDiff;
    const statusDiff = (PGM_USAGE_STATUS_ORDER[pgmUsageStatusValue(b)] || 0) - (PGM_USAGE_STATUS_ORDER[pgmUsageStatusValue(a)] || 0);
    if (statusDiff) return statusDiff;
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
            statusFilter: 'all',
            structuralSignalFilter: 'strong',
            executionReadinessFilter: 'all',
            operationalSafetyFilter: 'all',
            explanationFilter: 'high',
            reviewLaneFilter: 'candidate',
            productSearch: '',
            compareSearch: '',
            selectedProductId: '',
            compareProductId: '',
            selectedPurposeKey: '',
            actionReviewOpen: false,
            topBundleExpanded: false,
            candidateTableExpanded: false
        };
    }
    const state = AppState.viewState.pgmUsage;
    const defaults = {
        purposeFilter: 'all',
        statusFilter: 'all',
        structuralSignalFilter: 'strong',
        executionReadinessFilter: 'all',
        operationalSafetyFilter: 'all',
        explanationFilter: 'high',
        reviewLaneFilter: 'candidate',
        productSearch: '',
        compareSearch: '',
        selectedProductId: '',
        compareProductId: '',
        selectedPurposeKey: '',
        actionReviewOpen: false,
        topBundleExpanded: false,
        candidateTableExpanded: false
    };
    Object.entries(defaults).forEach(([key, defaultValue]) => {
        if (typeof state[key] === 'undefined') state[key] = defaultValue;
    });
    if (typeof state.compareProductId !== 'string') {
        state.compareProductId = '';
    }
    if (typeof state.selectedPurposeKey !== 'string') {
        state.selectedPurposeKey = '';
    }
    if (typeof state.compareSearch !== 'string') {
        state.compareSearch = '';
    }
    if (typeof state.compareSearchComposing !== 'boolean') {
        state.compareSearchComposing = false;
    }
    if (typeof state.actionReviewOpen !== 'boolean') {
        state.actionReviewOpen = false;
    }
    if (typeof state.topBundleExpanded !== 'boolean') {
        state.topBundleExpanded = false;
    }
    if (typeof state.candidateTableExpanded !== 'boolean') {
        state.candidateTableExpanded = false;
    }
    if (state.compareProductId && state.compareProductId === state.selectedProductId) {
        state.compareProductId = '';
    }
    delete state.scopeFilter;
    delete state.confidenceFilter;
    delete state.strengthFilter;
    delete state.flagFilter;
    delete state.eligibilityFilter;
    return state;
}

function pgmUsageCompareMetaText(state, compareCandidates = []) {
    const compareSearch = String(state?.compareSearch || '').trim();
    if (!state?.selectedProductId) return '';
    if (!compareCandidates.length) {
        return '검색 결과가 없어요. 검색어를 바꾸거나 왼쪽 후보 테이블의 비교 추가 버튼을 사용할 수 있어요.';
    }
    if (compareSearch) {
        return `검색 결과 ${pgmUsageFormatNumber(compareCandidates.length)}개 · 선택하면 바로 비교가 열립니다.`;
    }
    return `비교 가능한 다른 상품 ${pgmUsageFormatNumber(compareCandidates.length)}개 · 검색해서 빠르게 찾을 수 있어요.`;
}

function pgmUsageCompareOptionMarkup(compareCandidates = [], compareProductId = '', compareSearch = '') {
    const placeholder = compareSearch && String(compareSearch).trim()
        ? '검색 결과에서 상품을 선택하세요'
        : '두 번째 상품을 선택하세요';
    return `
        <option value="">${pgmUsageEscape(placeholder)}</option>
        ${compareCandidates.map((product) => `
            <option value="${pgmUsageEscape(product.productId)}" ${compareProductId === product.productId ? 'selected' : ''}>
                ${pgmUsageEscape(product.productName)} (${pgmUsageEscape(product.productId)})
            </option>
        `).join('')}
    `;
}

function pgmUsageClearCompareSearchTimer() {
    if (pgmUsageCompareSearchTimer) {
        window.clearTimeout(pgmUsageCompareSearchTimer);
        pgmUsageCompareSearchTimer = null;
    }
}

function pgmUsageRefreshCompareSearchUi() {
    const slot = document.querySelector('.pgm-usage-compare-slot');
    if (!slot) return;
    const model = pgmUsageBuildModel();
    const select = slot.querySelector('select');
    const meta = slot.querySelector('.pgm-usage-compare-search-meta');
    if (select) {
        select.innerHTML = pgmUsageCompareOptionMarkup(
            model.compareCandidates || [],
            model.state.compareProductId,
            model.state.compareSearch
        );
        select.value = model.state.compareProductId || '';
    }
    if (meta) {
        meta.textContent = pgmUsageCompareMetaText(model.state, model.compareCandidates || []);
    }
}

function pgmUsageScheduleCompareSearchRefresh() {
    pgmUsageClearCompareSearchTimer();
    pgmUsageCompareSearchTimer = window.setTimeout(() => {
        pgmUsageCompareSearchTimer = null;
        pgmUsageRefreshCompareSearchUi();
    }, PGM_USAGE_COMPARE_SEARCH_DEBOUNCE_MS);
}

function pgmUsageStatusValue(row) {
    if (row?.status) return row.status;
    if (row?.scope === 'broad_rollout' || row?.scope === 'limited_rollout') return 'operational_candidate';
    if (row?.scope === 'small_test') return 'testable';
    if (row?.scope === 'not_recommended') return row?.eligibility === 'exclude' ? 'not_applicable' : 'insufficient';
    return '';
}

function pgmUsageStructuralSignalValue(row) {
    if (row?.structuralSignal) return row.structuralSignal;
    if (row?.strength) return row.strength;
    const score = pgmUsageNumber(row?.signalScore, NaN);
    if (Number.isFinite(score)) {
        if (score >= 0.7) return 'strong';
        if (score >= 0.4) return 'medium';
        return 'weak';
    }
    return '';
}

function pgmUsageExecutionReadinessValue(row) {
    if (row?.executionReadiness) return row.executionReadiness;
    const status = pgmUsageStatusValue(row);
    if (row?.scope === 'broad_rollout' || status === 'operational_candidate') return 'ready';
    if (row?.scope === 'limited_rollout' || row?.scope === 'small_test' || ['testable', 'hypothesis_only'].includes(status)) return 'exploratory';
    if (row?.scope === 'not_recommended' || ['insufficient', 'not_applicable'].includes(status)) return 'not_ready';
    return '';
}

function pgmUsageOperationalSafetyValue(row) {
    if (row?.operationalSafety) return row.operationalSafety;
    if (row?.eligibility === 'exclude' || row?.riskFlag || row?.scope === 'not_recommended') return 'fragile';
    if (row?.eligibility === 'downweight' || row?.preconditionFlag || row?.scope === 'limited_rollout') return 'guarded';
    return 'safe';
}

function pgmUsageExplanationValue(row) {
    return row?.explanationConfidence || row?.confidence || '';
}

function pgmUsageReviewLaneValue(row) {
    const status = pgmUsageStatusValue(row);
    const readiness = pgmUsageExecutionReadinessValue(row);
    const safety = pgmUsageOperationalSafetyValue(row);
    if (row?.eligibility === 'exclude' || status === 'not_applicable') return 'excluded';
    if (row?.scope === 'not_recommended' && ['fragile'].includes(safety)) return 'excluded';
    if (['insufficient'].includes(status) || readiness === 'not_ready' || row?.scope === 'not_recommended') return 'hold';
    if (['guarded', 'fragile'].includes(safety) || row?.riskFlag || row?.preconditionFlag || row?.eligibility === 'downweight') return 'guarded';
    if (pgmUsageIsReviewable({ ...row, status, scope: row?.scope || (readiness === 'ready' ? 'broad_rollout' : '') })) return 'candidate';
    return 'candidate';
}

function pgmUsageApplyFilters(rows, state) {
    const search = state.productSearch;
    return rows.filter((row) => {
        if (state.purposeFilter !== 'all' && row.purposeKey !== state.purposeFilter) return false;
        if (state.statusFilter !== 'all' && pgmUsageStatusValue(row) !== state.statusFilter) return false;
        if (state.structuralSignalFilter !== 'all' && pgmUsageStructuralSignalValue(row) !== state.structuralSignalFilter) return false;
        if (state.executionReadinessFilter !== 'all' && pgmUsageExecutionReadinessValue(row) !== state.executionReadinessFilter) return false;
        if (state.operationalSafetyFilter !== 'all' && pgmUsageOperationalSafetyValue(row) !== state.operationalSafetyFilter) return false;
        if (state.explanationFilter !== 'all' && pgmUsageExplanationValue(row) !== state.explanationFilter) return false;
        if (state.reviewLaneFilter !== 'all' && pgmUsageReviewLaneValue(row) !== state.reviewLaneFilter) return false;
        if (!pgmUsageMatchesSearchText(`${row.productId} ${row.productName}`, search)) return false;
        return true;
    });
}

function pgmUsageSummary(rows) {
    return {
        strongSignal: rows.filter((row) => pgmUsageStructuralSignalValue(row) === 'strong').length,
        ready: rows.filter((row) => pgmUsageExecutionReadinessValue(row) === 'ready').length,
        constrained: rows.filter((row) => ['guarded', 'fragile'].includes(pgmUsageOperationalSafetyValue(row)) || row.riskFlag || row.preconditionFlag).length,
        lowConfidence: rows.filter((row) => pgmUsageExplanationValue(row) === 'low').length,
        hold: rows.filter((row) => ['insufficient', 'not_applicable'].includes(pgmUsageStatusValue(row)) || pgmUsageExecutionReadinessValue(row) === 'not_ready').length
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
    return (products || []).filter((product) => {
        if (product.productId === excludedProductId) return false;
        if (product.productId === preservedProductId) return true;
        return pgmUsageMatchesSearchText(`${product.productName || ''} ${product.productId || ''}`, search);
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
    const statusOptions = [
        ['all', '전체 판정 상태'],
        ...Object.entries(PGM_USAGE_STATUS_LABELS)
    ];
    const structuralSignalOptions = [
        ['all', '전체 구조 신호'],
        ...Object.entries(PGM_USAGE_STRUCTURAL_SIGNAL_LABELS)
    ];
    const executionReadinessOptions = [
        ['all', '전체 실행 준비도'],
        ...Object.entries(PGM_USAGE_EXECUTION_READINESS_LABELS)
    ];
    const operationalSafetyOptions = [
        ['all', '전체 운영 안전성'],
        ...Object.entries(PGM_USAGE_OPERATIONAL_SAFETY_LABELS)
    ];
    const explanationOptions = [
        ['all', '전체 설명 신뢰도'],
        ['high', '높음'],
        ['medium', '중간'],
        ['low', '낮음']
    ];
    const reviewLaneOptions = [
        ['all', '전체 검토 구간'],
        ['candidate', '우선 검토 후보'],
        ['guarded', '주의 조건 포함'],
        ['hold', '해석 보강 필요'],
        ['excluded', '검토 제외']
    ];

    return `
        <section class="pgm-usage-toolbar" aria-label="검토 필터">
            <div class="pgm-usage-toolbar-copy">
                <span class="pgm-usage-eyebrow">검토용 초기 화면</span>
                <h2>목적별 활용 검토</h2>
                <p>v2 해석 모델 기준으로 판정 상태, 구조 신호, 실행 준비도, 운영 안전성을 같이 보며 후보를 좁힙니다.</p>
            </div>
            <div class="pgm-usage-filter-grid">
                <label>
                    <span>목적</span>
                    <select onchange="handlePgmUsageFilterChange('purposeFilter', this.value)">
                        ${pgmUsageRenderOptions(purposeOptions, state.purposeFilter)}
                    </select>
                </label>
                <label>
                    <span>판정 상태</span>
                    <select onchange="handlePgmUsageFilterChange('statusFilter', this.value)">
                        ${pgmUsageRenderOptions(statusOptions, state.statusFilter)}
                    </select>
                </label>
                <label>
                    <span>구조 신호</span>
                    <select onchange="handlePgmUsageFilterChange('structuralSignalFilter', this.value)">
                        ${pgmUsageRenderOptions(structuralSignalOptions, state.structuralSignalFilter)}
                    </select>
                </label>
                <label>
                    <span>실행 준비도</span>
                    <select onchange="handlePgmUsageFilterChange('executionReadinessFilter', this.value)">
                        ${pgmUsageRenderOptions(executionReadinessOptions, state.executionReadinessFilter)}
                    </select>
                </label>
                <label>
                    <span>운영 안전성</span>
                    <select onchange="handlePgmUsageFilterChange('operationalSafetyFilter', this.value)">
                        ${pgmUsageRenderOptions(operationalSafetyOptions, state.operationalSafetyFilter)}
                    </select>
                </label>
                <label>
                    <span>설명 신뢰도</span>
                    <select onchange="handlePgmUsageFilterChange('explanationFilter', this.value)">
                        ${pgmUsageRenderOptions(explanationOptions, state.explanationFilter)}
                    </select>
                </label>
                <label>
                    <span>검토 구간</span>
                    <select onchange="handlePgmUsageFilterChange('reviewLaneFilter', this.value)">
                        ${pgmUsageRenderOptions(reviewLaneOptions, state.reviewLaneFilter)}
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
        ['구조 신호 강한 후보', model.summary.strongSignal, '구조적으로 목적 해석 가치가 높은 행'],
        ['실행 준비된 후보', model.summary.ready, '바로 검토를 시작할 수 있는 행'],
        ['제약 포함 후보', model.summary.constrained, '리스크나 전제조건을 먼저 확인해야 하는 행'],
        ['설명 불확실 후보', model.summary.lowConfidence, '해석 신뢰도가 낮아 보수적으로 읽어야 하는 행'],
        ['판단 보류 후보', model.summary.hold, '지금 판단을 미루는 편이 맞는 행']
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

function pgmUsageRenderTopBundleBar(model, isExpanded = false) {
    const summaryItems = [
        ['검토 구간', model.state.reviewLaneFilter === 'all' ? '전체' : pgmUsageLabel({
            candidate: '우선 검토 후보',
            guarded: '주의 조건 포함',
            hold: '해석 보강 필요',
            excluded: '검토 제외'
        }, model.state.reviewLaneFilter)],
        ['구조 신호', pgmUsageLabel(PGM_USAGE_STRUCTURAL_SIGNAL_LABELS, model.state.structuralSignalFilter, '전체')],
        ['설명 신뢰도', pgmUsageLabel(PGM_USAGE_LEVEL_LABELS, model.state.explanationFilter, '전체')],
        ['현재 표시', `${pgmUsageFormatNumber(model.filteredRows.length)}개`]
    ];
    return `
        <button
            type="button"
            class="pgm-usage-top-bundle-toggle"
            onclick="togglePgmUsageTopBundle()"
            aria-expanded="${isExpanded ? 'true' : 'false'}"
        >
            <span class="pgm-usage-top-bundle-summary">
                ${summaryItems.map(([label, value]) => `
                    <span class="pgm-usage-top-bundle-pill">
                        <strong>${pgmUsageEscape(label)}</strong>
                        <span>${pgmUsageEscape(value)}</span>
                    </span>
                `).join('')}
            </span>
            <span class="pgm-usage-top-bundle-action">
                ${isExpanded ? '접기' : '펼치기'}
                <i class="ph ${isExpanded ? 'ph-caret-up' : 'ph-caret-down'}"></i>
            </span>
        </button>
    `;
}

function pgmUsageRenderTopBundle(model) {
    const isExpanded = Boolean(model.state.topBundleExpanded);
    return `
        <section class="pgm-usage-top-bundle ${isExpanded ? 'is-expanded' : 'is-collapsed'}" aria-label="상단 검토 요약">
            ${pgmUsageRenderTopBundleBar(model, isExpanded)}
            ${isExpanded ? `
                <div class="pgm-usage-top-bundle-body">
                    ${pgmUsageRenderToolbar(model)}
                    ${pgmUsageRenderContext(model)}
                    ${pgmUsageRenderSummary(model)}
                </div>
            ` : ''}
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
    if (row.executionReadiness === 'not_ready' || row.scope === 'not_recommended' || ['insufficient', 'not_applicable'].includes(row.status)) {
        return '지금은 실행보다 해석 보강과 추가 확인이 먼저인 행입니다.';
    }
    if (row.operationalSafety === 'fragile' || row.riskFlag) {
        return '구조 신호가 있더라도 운영 리스크가 커서 보호장치부터 확인해야 합니다.';
    }
    if (row.preconditionFlag || row.operationalSafety === 'guarded') {
        return '가능성은 보이지만 전제조건과 운영 제약을 먼저 점검해야 합니다.';
    }
    if (row.executionReadiness === 'ready' && row.structuralSignal === 'strong') {
        return '구조 신호와 실행 준비도가 함께 보여 우선 검토 가치가 높은 행입니다.';
    }
    if (row.executionReadiness === 'exploratory') {
        return '해석은 가능하지만 바로 실행하기보다 작은 확인 단계를 두는 편이 좋습니다.';
    }
    return '표시된 구조 신호와 설명 신뢰도를 기준으로 추가 해석이 필요합니다.';
}

function pgmUsageRenderPurposeBoards(model) {
    return `
        <div class="pgm-usage-purpose-board-grid">
            ${PGM_USAGE_PURPOSES.map((purpose) => {
                const rows = model.filteredRows.filter((row) => row.purposeKey === purpose.key);
                const reviewableCount = rows.filter(pgmUsageIsReviewable).length;
                const topRows = rows.slice(0, 5);
                return `
                    <article class="pgm-usage-purpose-card pgm-usage-purpose-card--${pgmUsageEscape(purpose.key)}">
                        <div class="pgm-usage-purpose-card-head">
                            <div>
                                <h4>${pgmUsageEscape(purpose.label)}</h4>
                                <p>${pgmUsageEscape(purpose.guide)}</p>
                            </div>
                            <strong>${pgmUsageFormatNumber(reviewableCount)}</strong>
                        </div>
                        <div class="pgm-usage-purpose-list">
                            ${topRows.length ? topRows.map((row) => {
                                const isSelectedProduct = row.productId === model.state.selectedProductId;
                                const isSelectedFocus = isSelectedProduct && row.purposeKey === model.state.selectedPurposeKey;
                                const chipClassNames = [
                                    'pgm-usage-candidate-chip',
                                    isSelectedProduct ? 'is-selected-product' : '',
                                    isSelectedFocus ? 'is-selected-focus' : ''
                                ].filter(Boolean).join(' ');
                                return `
                                <button type="button" class="${chipClassNames}"
                                    onclick="selectPgmUsageProduct('${pgmUsageEscapeJsAttr(row.productId)}', '${pgmUsageEscapeJsAttr(row.purposeKey)}')">
                                    <span>${pgmUsageEscape(row.productName || row.productId)}</span>
                                    <small>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_STRUCTURAL_SIGNAL_LABELS, pgmUsageStructuralSignalValue(row)))} · ${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_EXECUTION_READINESS_LABELS, pgmUsageExecutionReadinessValue(row)))}</small>
                                    <span class="pgm-usage-mini-flags">
                                        ${isSelectedFocus ? pgmUsageBadge('현재 보고 있음', 'medium') : ''}
                                        ${!isSelectedFocus && isSelectedProduct ? pgmUsageBadge('같은 상품', 'strong') : ''}
                                        ${row.preconditionFlag ? pgmUsageBadge('전제', 'caution') : ''}
                                        ${row.riskFlag ? pgmUsageBadge('리스크', 'risk') : ''}
                                    </span>
                                </button>
                            `;
                            }).join('') : `
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
    const isExpanded = Boolean(model.state.candidateTableExpanded);
    const rows = model.filteredRows.slice(0, 100);
    const remaining = Math.max(0, model.filteredRows.length - rows.length);
    const visibleCountText = `${pgmUsageFormatNumber(rows.length)}개 행 미리보기`;
    const remainingText = remaining ? `${pgmUsageFormatNumber(remaining)}개 행은 필터를 좁히면 볼 수 있습니다.` : '현재 필터 기준 전체 행이 표시됩니다.';
    return `
        <div class="pgm-usage-table-card ${isExpanded ? 'is-expanded' : 'is-collapsed'}">
            <button
                type="button"
                class="pgm-usage-table-toggle"
                onclick="togglePgmUsageCandidateTable()"
                aria-expanded="${isExpanded ? 'true' : 'false'}"
            >
                <span class="pgm-usage-table-toggle-copy">
                    <span class="pgm-usage-table-toggle-title">후보 테이블</span>
                    <span class="pgm-usage-table-toggle-desc">정렬은 구조 신호, 실행 준비도, 운영 안전성, 설명 신뢰도 순서로 적용됩니다.</span>
                </span>
                <span class="pgm-usage-table-toggle-side">
                    <span class="pgm-usage-table-toggle-count">${pgmUsageEscape(visibleCountText)}</span>
                    <span class="pgm-usage-table-toggle-meta">${pgmUsageEscape(remainingText)}</span>
                    <span class="pgm-usage-table-toggle-action">
                        ${isExpanded ? '접기' : '펼치기'}
                        <i class="ph ${isExpanded ? 'ph-caret-up' : 'ph-caret-down'}"></i>
                    </span>
                </span>
            </button>
            ${isExpanded ? `
                <div class="table-container pgm-usage-table-wrap">
                    <table class="data-table pgm-usage-table">
                        <thead>
                            <tr>
                                <th>제품</th>
                                <th>목적</th>
                                <th>구조 신호</th>
                                <th>실행 준비도</th>
                                <th>운영 안전성</th>
                                <th>설명 신뢰도</th>
                                <th>관계 해석</th>
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
                                    <td>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_STRUCTURAL_SIGNAL_LABELS, pgmUsageStructuralSignalValue(row)))}</td>
                                    <td>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_EXECUTION_READINESS_LABELS, pgmUsageExecutionReadinessValue(row)))}</td>
                                    <td>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_OPERATIONAL_SAFETY_LABELS, pgmUsageOperationalSafetyValue(row)))}</td>
                                    <td>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_LEVEL_LABELS, pgmUsageExplanationValue(row)))}</td>
                                    <td>
                                        <div class="pgm-usage-metric-stack">
                                            <span>${pgmUsageEscape(row.relationSummaryKo || '관계 해석 정보 없음')}</span>
                                            <small>${pgmUsageEscape(row.relationRiskSummaryKo || pgmUsageInterpretation(row))}</small>
                                        </div>
                                    </td>
                                    <td>
                                        <div class="pgm-usage-row-actions">
                                            <button type="button" onclick="selectPgmUsageProduct('${pgmUsageEscapeJsAttr(row.productId)}', '${pgmUsageEscapeJsAttr(row.purposeKey)}')">해석 보기</button>
                                            <button type="button" onclick="addPgmUsageCompareProduct('${pgmUsageEscapeJsAttr(row.productId)}', '${pgmUsageEscapeJsAttr(row.purposeKey)}')">비교 추가</button>
                                        </div>
                                    </td>
                                </tr>
                            `).join('') : `
                                <tr>
                                    <td colspan="8" class="pgm-usage-empty-cell">현재 필터에서 표시할 후보가 없습니다.</td>
                                </tr>
                            `}
                        </tbody>
                    </table>
                </div>
            ` : `
                <div class="pgm-usage-table-collapsed-body">
                    <p>목적 보드에서 먼저 우선 후보를 좁힌 뒤, 필요할 때만 상세 행을 펼쳐 확인할 수 있습니다.</p>
                </div>
            `}
        </div>
    `;
}

function pgmUsageRenderGlobalArea(model) {
    return `
        <section class="pgm-usage-panel pgm-usage-global-panel">
            <div class="pgm-usage-global-stage">
                <div class="pgm-usage-global-board-block">
                    ${pgmUsageRenderPurposeBoards(model)}
                </div>
                <div class="pgm-usage-global-table-block">
                    ${pgmUsageRenderCandidateTable(model)}
                </div>
            </div>
        </section>
    `;
}

function pgmUsageRenderProductSelector(model) {
    const compareCandidates = model.compareCandidates || [];
    const compareMetaText = pgmUsageCompareMetaText(model.state, compareCandidates);
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
                        oncompositionstart="handlePgmUsageCompareSearchCompositionStart()"
                        oncompositionend="handlePgmUsageCompareSearchCompositionEnd(this.value)"
                        oninput="handlePgmUsageCompareSearch(this.value)">
                    <select onchange="addPgmUsageCompareProduct(this.value, '')">
                        ${pgmUsageCompareOptionMarkup(compareCandidates, model.state.compareProductId, model.state.compareSearch)}
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

function pgmUsageRenderContextItem(label, value, options = {}) {
    const normalizedLabel = String(label ?? '').trim();
    if (!normalizedLabel) return '';

    const normalizedValues = Array.isArray(value)
        ? value.flat().map((item) => String(item ?? '').trim()).filter(Boolean)
        : [String(value ?? '').trim()].filter(Boolean);

    if (!normalizedValues.length) return '';

    const classNames = ['pgm-usage-context-item'];
    if (options.variant) classNames.push(`is-${options.variant}`);
    if (options.className) classNames.push(options.className);

    const labelMarkup = options.heading
        ? `<h5 class="pgm-usage-context-heading">${pgmUsageEscape(normalizedLabel)}</h5>`
        : `<span class="pgm-usage-context-label">${pgmUsageEscape(normalizedLabel)}</span>`;

    const valueMarkup = normalizedValues.length > 1
        ? `<div class="pgm-usage-context-value-list">${normalizedValues.map((item) => `<span>${pgmUsageEscape(item)}</span>`).join('')}</div>`
        : `<div class="pgm-usage-context-value">${pgmUsageEscape(normalizedValues[0])}</div>`;

    return `
        <article class="${classNames.join(' ')}">
            ${labelMarkup}
            ${valueMarkup}
        </article>
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
            ${pgmUsageRenderContextItem('적격성', eligibilityText)}
            ${pgmUsageRenderContextItem('전이일수', transitionDays)}
            ${related ? pgmUsageRenderContextItem(related.label, related.text) : ''}
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
            key: 'structuralSignal',
            label: '구조 신호',
            rankA: PGM_USAGE_STRUCTURAL_SIGNAL_ORDER[rowA?.structuralSignal] || 0,
            rankB: PGM_USAGE_STRUCTURAL_SIGNAL_ORDER[rowB?.structuralSignal] || 0
        },
        {
            key: 'executionReadiness',
            label: '실행 준비도',
            rankA: PGM_USAGE_EXECUTION_READINESS_ORDER[rowA?.executionReadiness] || 0,
            rankB: PGM_USAGE_EXECUTION_READINESS_ORDER[rowB?.executionReadiness] || 0
        },
        {
            key: 'operationalSafety',
            label: '운영 안전성',
            rankA: PGM_USAGE_OPERATIONAL_SAFETY_ORDER[rowA?.operationalSafety] || 0,
            rankB: PGM_USAGE_OPERATIONAL_SAFETY_ORDER[rowB?.operationalSafety] || 0
        },
        {
            key: 'explanationConfidence',
            label: '설명 신뢰도',
            rankA: PGM_USAGE_LEVEL_ORDER[rowA?.explanationConfidence || rowA?.confidence] || 0,
            rankB: PGM_USAGE_LEVEL_ORDER[rowB?.explanationConfidence || rowB?.confidence] || 0
        },
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
    if (fieldKey === 'structuralSignal') {
        return `구조 신호가 ${pgmUsageLabel(PGM_USAGE_STRUCTURAL_SIGNAL_LABELS, winnerRow.structuralSignal)} 쪽으로 더 선명해 보여`;
    }
    if (fieldKey === 'executionReadiness') {
        return `실행 준비도가 ${pgmUsageLabel(PGM_USAGE_EXECUTION_READINESS_LABELS, winnerRow.executionReadiness)} 쪽으로 더 앞서 보여`;
    }
    if (fieldKey === 'operationalSafety') {
        return `운영 안전성이 ${pgmUsageLabel(PGM_USAGE_OPERATIONAL_SAFETY_LABELS, winnerRow.operationalSafety)} 쪽으로 더 안정적으로 보여`;
    }
    if (fieldKey === 'explanationConfidence') {
        return '구조 신호와 실행 준비도는 비슷하지만 설명 신뢰도 표기가 더 높아';
    }
    if (fieldKey === 'scope') {
        return `검토 범위가 ${pgmUsageLabel(PGM_USAGE_SCOPE_LABELS, winnerRow.scope)} 쪽으로 더 앞서 보여`;
    }
    if (fieldKey === 'status') {
        return `상태가 ${pgmUsageLabel(PGM_USAGE_STATUS_LABELS, winnerRow.status)} 쪽으로 더 앞서 보여`;
    }
    return '표시된 해석 필드 기준으로 조금 더 앞서 보여';
}

function pgmUsagePairInterpretation(rowA, productA, rowB, productB) {
    if (!rowA && !rowB) return '두 상품 모두 이 목적의 산출 행이 아직 없습니다.';
    if (rowA && !rowB) return `${productA.productName || productA.productId}만 이 목적의 산출 행이 있습니다.`;
    if (!rowA && rowB) return `${productB.productName || productB.productId}만 이 목적의 산출 행이 있습니다.`;
    const comparison = pgmUsagePairComparison(rowA, rowB);
    if (comparison.kind === 'same') {
        const cautionGap = Math.abs(pgmUsagePairCautionScore(rowA) - pgmUsagePairCautionScore(rowB));
        if (cautionGap) {
            return '현재 두 상품은 구조 신호·실행 준비도·운영 안전성·설명 신뢰도가 비슷합니다. 다만 전제조건·리스크 또는 적격성 차이가 있어 주의 조건까지 같이 확인하는 편이 안전합니다. 전체 상품 순위가 아니라 현재 두 상품 사이의 해석입니다.';
        }
        return '현재 두 상품은 구조 신호·실행 준비도·운영 안전성·설명 신뢰도가 비슷하게 읽힙니다. 세부 근거와 플래그를 함께 확인합니다. 전체 상품 순위가 아니라 현재 두 상품 사이의 해석입니다.';
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
                <p>후보 테이블에서 해석 보기를 누르거나 기준 상품을 선택하면 purpose dossier가 열립니다.</p>
            </div>
        `;
    }

    const selectedByPurpose = new Map(model.selectedRows.map((row) => [row.purposeKey, row]));
    return `
        <div class="pgm-usage-selected-head">
            <div>
                <h3>${pgmUsageEscape(model.selectedProduct.productName || model.selectedProduct.productId)}</h3>
                <p>${pgmUsageEscape(model.selectedProduct.productId)} · ${pgmUsageEscape(model.snapshotLabel)} · 목적 행 ${pgmUsageFormatNumber(model.selectedRows.length)}개</p>
                <p class="pgm-usage-helper-copy">이 영역은 선택한 상품 기준으로만 바뀌며, 액션보다 해석과 제약을 먼저 보여줍니다.</p>
            </div>
        </div>
        <div class="pgm-usage-selected-card-grid">
            ${PGM_USAGE_PURPOSES.map((purpose) => {
                const row = selectedByPurpose.get(purpose.key);
                return `
                    <article class="pgm-usage-selected-purpose-card ${model.state.selectedPurposeKey === purpose.key ? 'is-active' : ''}">
                        <div class="pgm-usage-selected-purpose-title">
                            <h4>${pgmUsageEscape(purpose.label)}</h4>
                            ${row ? pgmUsageBadge(pgmUsageLabel(PGM_USAGE_STRUCTURAL_SIGNAL_LABELS, row.structuralSignal), 'medium') : pgmUsageBadge('행 없음', 'muted')}
                        </div>
                        ${row ? `
                            <div class="pgm-usage-purpose-facts">
                                <span><strong>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_EXECUTION_READINESS_LABELS, row.executionReadiness))}</strong></span>
                                <span><strong>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_OPERATIONAL_SAFETY_LABELS, row.operationalSafety))}</strong></span>
                                <span>설명 <strong>${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_LEVEL_LABELS, row.explanationConfidence || row.confidence))}</strong></span>
                            </div>
                            <p>${pgmUsageEscape(row.relationRiskSummaryKo || pgmUsageInterpretation(row))}</p>
                            <div class="pgm-usage-context-list">
                                ${pgmUsageRenderContextItem('주요 근거', pgmUsageMetricText(row.primaryMetric, row.primaryMetricValue))}
                                ${pgmUsageRenderContextItem('보조 근거', pgmUsageMetricText(row.secondaryMetric, row.secondaryMetricValue))}
                                ${pgmUsageRenderContextItem('관계 해석', row.relationSummaryKo || '관계 해석 정보 없음')}
                                ${row.purposeSubtype ? pgmUsageRenderContextItem('Subtype', pgmUsageLabel(PGM_USAGE_PURPOSE_SUBTYPE_LABELS, row.purposeSubtype)) : ''}
                                ${row.compoundCandidateHint ? pgmUsageRenderContextItem('Compound', row.compoundCandidateHint) : ''}
                                ${row.relationRiskSummaryKo ? pgmUsageRenderContextItem('제약', row.relationRiskSummaryKo) : ''}
                            </div>
                            <div class="pgm-usage-badge-stack">${pgmUsageFlagBadges(row)}</div>
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
            <div class="pgm-usage-badge-stack">
                ${pgmUsageBadge(pgmUsageLabel(PGM_USAGE_STRUCTURAL_SIGNAL_LABELS, row.structuralSignal), 'medium')}
                ${pgmUsageBadge(pgmUsageLabel(PGM_USAGE_EXECUTION_READINESS_LABELS, row.executionReadiness), 'plain')}
                ${pgmUsageBadge(pgmUsageLabel(PGM_USAGE_OPERATIONAL_SAFETY_LABELS, row.operationalSafety), row.operationalSafety === 'fragile' ? 'risk' : (row.operationalSafety === 'guarded' ? 'caution' : 'plain'))}
            </div>
            <div class="pgm-usage-metric-stack">
                <span>${pgmUsageEscape(row.relationSummaryKo || '관계 해석 정보 없음')}</span>
                <small>설명 신뢰도 ${pgmUsageEscape(pgmUsageLabel(PGM_USAGE_LEVEL_LABELS, row.explanationConfidence || row.confidence))}</small>
                <small>${pgmUsageEscape(pgmUsageMetricText(row.primaryMetric, row.primaryMetricValue))}</small>
            </div>
            <div class="pgm-usage-badge-stack">${pgmUsageFlagBadges(row)}</div>
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
                <p>${pgmUsageEscape(model.snapshotLabel)} · 현재 두 상품 사이의 해석이며 전체 순위가 아닙니다.</p>
                <p class="pgm-usage-helper-copy">비교는 구조 신호, 실행 준비도, 운영 안전성, 설명 신뢰도 차이를 우선 읽습니다.</p>
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
    return pgmUsageRenderContextItem(product.productName || product.productId, bits, {
        heading: true,
        variant: 'product'
    });
}

function pgmUsageRenderSelectedArea(model) {
    const isCompare = model.selectedProduct && model.compareProduct;
    const title = isCompare ? '선택 상품 효과 비교' : '선택 상품 해석';
    const copy = isCompare
        ? '두 상품 사이의 목적별 해석 차이만 봅니다. 전체 후보 순위로 읽지 않습니다.'
        : '선택한 상품의 목적별 구조 신호, 제약, 후속 실행 메모를 함께 봅니다.';
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
            <section class="pgm-usage-toolbar pgm-usage-toolbar-compact">
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
            ${pgmUsageRenderTopBundle(model)}
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
    pgmUsageClearCompareSearchTimer();
    const state = pgmUsageCurrentState();
    state.compareSearchComposing = false;
    state.selectedProductId = String(productId || '').trim();
    state.selectedPurposeKey = String(purposeKey || '').trim();
    state.compareProductId = '';
    state.compareSearch = '';
    renderPgmUsage();
};

window.handlePgmUsageCompareSearch = (value) => {
    const state = pgmUsageCurrentState();
    state.compareSearch = String(value ?? '');
    if (state.compareSearchComposing) return;
    pgmUsageScheduleCompareSearchRefresh();
};

window.handlePgmUsageCompareSearchCompositionStart = () => {
    const state = pgmUsageCurrentState();
    state.compareSearchComposing = true;
    pgmUsageClearCompareSearchTimer();
};

window.handlePgmUsageCompareSearchCompositionEnd = (value) => {
    const state = pgmUsageCurrentState();
    state.compareSearchComposing = false;
    state.compareSearch = String(value ?? '');
    pgmUsageScheduleCompareSearchRefresh();
};

window.addPgmUsageCompareProduct = (productId, purposeKey = '') => {
    pgmUsageClearCompareSearchTimer();
    const state = pgmUsageCurrentState();
    state.compareSearchComposing = false;
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
    pgmUsageClearCompareSearchTimer();
    const state = pgmUsageCurrentState();
    state.compareSearchComposing = false;
    state.compareProductId = '';
    renderPgmUsage();
};

window.togglePgmUsageActionReview = () => {
    const state = pgmUsageCurrentState();
    state.actionReviewOpen = !state.actionReviewOpen;
    renderPgmUsage();
};

window.togglePgmUsageCandidateTable = () => {
    const state = pgmUsageCurrentState();
    state.candidateTableExpanded = !state.candidateTableExpanded;
    renderPgmUsage();
};

window.togglePgmUsageTopBundle = () => {
    const state = pgmUsageCurrentState();
    state.topBundleExpanded = !state.topBundleExpanded;
    renderPgmUsage();
};

window.clearPgmUsageSelectedProduct = () => {
    pgmUsageClearCompareSearchTimer();
    const state = pgmUsageCurrentState();
    state.compareSearchComposing = false;
    state.selectedProductId = '';
    state.compareProductId = '';
    state.compareSearch = '';
    state.selectedPurposeKey = '';
    renderPgmUsage();
};
