// PGM Dashboard App Logic

// --- Configurations ---
const DB_CONFIG = {
    name: 'PGM_Dashboard_DB',
    version: 1,
    store: 'csv_files'
};

const REQUIRED_FILES = {
    brandScore: { key: 'brand_score', filename: 'brand_score.csv' },
    anchorScored: {
        key: 'anchor_scored',
        filename: 'pgm_scored.csv',
        aliases: ['anchor_scored.csv']
    },
    anchorTransition: {
        key: 'anchor_transition',
        filename: 'pgm_entry_to_expansion_transition.csv',
        aliases: ['anchor_transition.csv']
    },
    cartAnchor: {
        key: 'cart_anchor',
        filename: 'pgm_basket_gravity.csv',
        aliases: ['cart_anchor.csv']
    },
    cartAnchorDetail: {
        key: 'cart_anchor_detail',
        filename: 'pgm_basket_gravity_detail.csv',
        aliases: ['cart_anchor_detail.csv']
    },
    aaCohortJourney: {
        key: 'aa_cohort_journey',
        filename: '_insight_entry_cohort_journey.csv',
        aliases: ['_insight_aa_cohort_journey.csv', 'aa_cohort_journey.csv']
    },
    aaTransitionPath: {
        key: 'aa_transition_path',
        filename: '_insight_entry_transition_path.csv',
        aliases: ['_insight_aa_transition_path.csv', 'aa_transition_path.csv']
    },
    caProfile: {
        key: 'ca_profile',
        filename: '_insight_basket_gravity_profile.csv',
        aliases: ['_insight_ca_profile.csv', 'ca_profile.csv']
    },
    biiWindow: {
        key: 'bii_window',
        filename: '_insight_bii_window.csv',
        aliases: ['bii_window.csv', 'brand_impact_windows.csv', 'brand_impact_index.csv']
    },
    apfActionRules: {
        key: 'apf_action_rules',
        filename: '_insight_pgm_action_rules.csv',
        aliases: ['_insight_apf_action_rules.csv', 'apf_action_rules.csv']
    },
    productGroupMap: {
        key: 'product_group_map',
        filename: 'pgm_product_group_map.csv',
        aliases: ['product_group_map.csv', '_meta_product_group_map.csv']
    }
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
        apfActionRules: [],
        productGroupMap: []
    },
    rawData: {
        brandScore: null,
        anchorScored: null,
        anchorTransition: null,
        cartAnchor: null,
        cartAnchorDetail: [],
        aaCohortJourney: [],
        aaTransitionPath: [],
        caProfile: [],
        biiWindow: [],
        apfActionRules: [],
        productGroupMap: []
    },
    viewState: {
        products: {
            sortCol: 'revenue_90d',
            sortDesc: true,
            searchQuery: '',
            quadrant: {
                selectedId: '',
                history: [],
                filters: {},
                groupingEditorOpen: false,
                scaleMode: 'focus',
                scope: 'retention-emphasis'
            }
        },
        transitions: { sortCol: 'transition_customer_cnt', sortDesc: true, searchQuery: '', searchMode: 'all' },
        settings: {
            activeTab: 'grouping'
        },
        insights: {
            dateFrom: '',
            dateTo: '',
            aaType: 'ALL',
            aaProductId: 'ALL',
            windowDays: 90,
            jumpNavOpen: false
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
    },
    clearAll: async () => {
        const db = await DB.open();
        return new Promise((resolve, reject) => {
            const tx = db.transaction(DB_CONFIG.store, 'readwrite');
            tx.objectStore(DB_CONFIG.store).clear();
            tx.oncomplete = () => resolve();
            tx.onerror = (e) => reject(e.target.error);
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

const escapeJs = (value) => String(value ?? '')
    .replaceAll('\\', '\\\\')
    .replaceAll("'", "\\'")
    .replaceAll('\n', '\\n')
    .replaceAll('\r', '\\r');

const withFallback = (value, fallback = '-') => {
    if (value === null || value === undefined || value === '') return fallback;
    return value;
};

const TERM_LABELS = {
    AA: '첫구매 유입 상품',
    PCA: '재구매 상품',
    CA: '장바구니 확장 상품',
    BHI: '브랜드 구조 건강도',
    BII: '브랜드 실전 건강도'
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
    CORE: '기본 확장형',
    PAIR: '함께 담김형',
    SET: '세트형',
    NONE: '독립형'
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

const FITNESS_COMPONENT_LABELS = {
    value: '매출 기여',
    strength: '재구매 강도'
};

const STRUCTURE_LABELS = {
    entry: '신규유입 안정성',
    expansion: '재구매 안정성',
    valueReadiness: '매출확장 준비도'
};

const BANNED_UI_TERMS = [
    /\bEntry\s*Gravity\b/gi,
    /\bExpansion\s*Gravity\b/gi,
    /\bBasket\s*Gravity\b/gi,
    /\bBrand\s*Health\s*Index\b/gi,
    /\bBrand\s*Impact\s*Index\b/gi,
    /\bAA\b/g,
    /\bPCA\b/g,
    /\bCA\b/g,
    /\bBHI\b/g,
    /\bBII\b/g
];

const UI_TERM_REPLACEMENTS = [
    [/AA-Broad/gi, `${TERM_LABELS.AA}-${AA_TYPE_LABELS.BROAD}`],
    [/AA-Qualified/gi, `${TERM_LABELS.AA}-${AA_TYPE_LABELS.QUALIFIED}`],
    [/AA-Heavy/gi, `${TERM_LABELS.AA}-${AA_TYPE_LABELS.HEAVY}`],
    [/PCA-Core/gi, `${TERM_LABELS.PCA}-${PCA_TYPE_LABELS.CORE}`],
    [/PCA-Deep/gi, `${TERM_LABELS.PCA}-${PCA_TYPE_LABELS.DEEP}`],
    [/PCA-Scale/gi, `${TERM_LABELS.PCA}-${PCA_TYPE_LABELS.SCALE}`],
    [/CA-Pair/gi, `${TERM_LABELS.CA}-${CA_TYPE_LABELS.PAIR}`],
    [/CA-Set/gi, `${TERM_LABELS.CA}-${CA_TYPE_LABELS.SET}`],
    [/BII\s*90\/365/gi, '90일 대비 연간 흐름'],
    [/Brand Fitness/gi, '브랜드 건강도'],
    [/Action Center/gi, '실행 카드'],
    [/\bBII\b/g, TERM_LABELS.BII],
    [/\bBHI\b/g, TERM_LABELS.BHI],
    [/\bPCA\b/g, TERM_LABELS.PCA],
    [/\bAA\b/g, TERM_LABELS.AA],
    [/\bCA\b/g, TERM_LABELS.CA],
    [/\bTransition\b/gi, '리텐션 흐름'],
    [/\bJourney\b/gi, '고객 흐름'],
    [/\bFitness\b/gi, '건강도'],
    [/\bEntry Balance\b/gi, STRUCTURE_LABELS.entry],
    [/\bExpansion Balance\b/gi, STRUCTURE_LABELS.expansion],
    [/\bValue Readiness\b/gi, STRUCTURE_LABELS.valueReadiness],
    [/\bEntry\s*Gravity\b/gi, '첫구매 유입'],
    [/\bExpansion\s*Gravity\b/gi, '재구매'],
    [/\bBasket\s*Gravity\b/gi, '장바구니 확장']
];

const METRIC_TOOLTIP_RULES = [
    { pattern: /^첫구매 유입 점수$/, description: '이 상품이 신규 고객 첫 구매를 얼마나 잘 만드는지 보여줘요. 높을수록 유입에 강해요.' },
    { pattern: /^재구매 점수$/, description: '첫 구매 뒤 다음 구매로 이어지게 하는 힘이에요. 높을수록 재구매가 좋아요.' },
    { pattern: /^주간 예상 수요량$/, description: '최근 흐름 기준으로 본 주간 수요 예상치예요.' },
    { pattern: /^첫구매 유입 고객수$/, description: '이 상품을 통해 처음 들어온 고객 수예요.' },
    { pattern: /^7일 재구매$/, description: '첫 구매 후 7일 안에 다시 산 고객 비율이에요.' },
    { pattern: /^30일 재구매$/, description: '첫 구매 후 30일 안에 다시 산 고객 비율이에요.' },
    { pattern: /^90일 재구매$/, description: '첫 구매 후 90일 안에 다시 산 고객 비율이에요.' },
    { pattern: /^90일 재구매 도달률$/, description: '첫 구매 고객 중 90일 안에 재구매 상품까지 간 비율이에요.' },
    { pattern: /^재구매까지 평균 일수$/, description: '첫 구매 후 다음 구매까지 걸린 평균 기간이에요.' },
    { pattern: /^상위 3개 전이 집중도$/, description: '전환이 상위 3개 경로에 얼마나 몰려 있는지 보여줘요.' },
    { pattern: /^평균 90일 전이율$/, description: '첫 구매에서 재구매로 넘어간 평균 비율이에요.' },
    { pattern: /^건강도 방향$/, description: '브랜드 건강도가 지금 좋아지는지, 유지되는지, 약해지는지 보여줘요.' },
    { pattern: /^최근 기준 건강도$/, description: '선택한 기간 기준의 최신 건강도 값이에요.' },
    { pattern: /^90일 대비 연간 흐름$/, description: '단기(90일)와 연간(365일) 흐름 비교값이에요. 1보다 크면 최근 흐름이 더 좋아요.' },
    { pattern: /^신뢰도$/, description: '지표를 믿고 의사결정해도 되는 정도를 보여줘요.' },
    { pattern: /^현재 단계 \(\d+일\)$/, description: '선택 기간 기준으로 지금 브랜드가 어느 단계인지 표시해요.' },
    { pattern: /^브랜드 실전 건강도 \d+일$/, description: '선택한 기간 기준의 브랜드 실전 건강도 지수예요.' },
    { pattern: /^브랜드 구조 건강도$/, description: '상품 구조가 균형적인지 보는 기본 지표예요.' },
    { pattern: /^고객가치$/, description: '유입 고객이 만들어내는 가치 수준이에요.' },
    { pattern: /^재구매 강도$/, description: '고객이 반복 구매하는 힘을 보여줘요.' },
    { pattern: /^계산 건강도\(참고\)$/, description: '구조·고객가치·재구매강도로 계산한 참고용 건강도예요.' },
    { pattern: /^신규유입 안정성$/, description: '신규 유입이 특정 상품에 너무 치우치지 않는지 보여줘요.' },
    { pattern: /^재구매 안정성$/, description: '재구매가 특정 경로에 과도하게 몰리지 않는지 보여줘요.' },
    { pattern: /^매출확장 준비도$/, description: '지금 포트폴리오가 매출 확대를 받을 준비가 되었는지 보여줘요.' },
    { pattern: /^효율·고가치 유입 비중$/, description: '효율 좋고 가치 높은 유입의 비중이에요.' },
    { pattern: /^확장형 유입 비중$/, description: '확장형 유입(Broad) 비중이에요. 너무 높으면 효율이 퍼질 수 있어요.' },
    { pattern: /^유입 집중도$/, description: '신규 유입이 일부 상품에 얼마나 몰려 있는지 보여줘요. 높을수록 쏠림이 커요.' },
    { pattern: /^90일 재구매 고객수$/, description: '첫 구매 후 90일 안에 실제로 재구매한 고객 수예요.' },
    { pattern: /^평균 재구매 소요일$/, description: '첫 구매부터 다음 구매까지 걸린 평균 기간이에요.' },
    { pattern: /^90일 재구매율$/, description: '첫 구매 고객 중 90일 안에 다음 구매로 이어진 비율이에요.' },
    { pattern: /^전환고객수$/, description: '상품 A에서 상품 B로 실제 전환한 고객 수예요.' },
    { pattern: /^평균 전이일수$/, description: '상품 A 구매 후 상품 B로 넘어오기까지 걸린 평균 기간이에요.' },
    { pattern: /^전이율$/, description: '상품 A 고객 중 상품 B로 넘어간 비율이에요.' },
    { pattern: /^동시구매수$/, description: '두 상품이 같은 주문에서 함께 구매된 횟수예요.' }
];

const QUADRANT_TRANSITION_SCOPE_CRITERIA = '리텐션 상품은 유입 상위 핵심 상품과 재구매 핵심 상품을 대상으로, 첫 구매 후 90일 안에 실제로 다음 구매가 발생한 경우만 포함해요. 같은 주문에서 함께 산 건은 제외되고, 전환이 0건이면 목록에 나타나지 않아요.';
const QUADRANT_EDGE_TOP_N = 6;
const RETENTION_90D_FLOW_LABEL = '첫 구매 후 90일 안에 다음 구매로 이어진 리텐션 흐름';

const normalizeCategoryValue = (value, fallback = '') => {
    if (value === null || value === undefined) return fallback;
    if (typeof value === 'number' && Number.isNaN(value)) return fallback;
    const normalized = String(value).trim();
    if (!normalized) return fallback;
    const lower = normalized.toLowerCase();
    if (lower === 'nan' || lower === 'null' || lower === 'undefined') return fallback;
    return normalized;
};

const toAaTypeLabel = (value) => {
    const key = normalizeCategoryValue(value, '미분류');
    if (!key) return '미분류';
    return AA_TYPE_LABELS[key.toUpperCase()] || key;
};

const toPcaTypeLabel = (value) => {
    const key = normalizeCategoryValue(value, '');
    if (!key) return '-';
    return PCA_TYPE_LABELS[key.toUpperCase()] || key;
};

const toCaTypeLabel = (value) => {
    const key = normalizeCategoryValue(value, 'NONE');
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
    return text.replace(/\s{2,}/g, ' ');
};

const softenTone = (value) => String(value || '')
    .replaceAll('합니다.', '해요.')
    .replaceAll('합니다', '해요')
    .replaceAll('됩니다.', '돼요.')
    .replaceAll('됩니다', '돼요')
    .replaceAll('없습니다.', '없어요.')
    .replaceAll('없습니다', '없어요');

const toFriendlyText = (value) => softenTone(replaceUiTerm(value));

const validateUiHardRule = (value, context = 'ui') => {
    const text = String(value || '');
    BANNED_UI_TERMS.forEach((rule) => {
        if (rule.test(text)) {
            console.warn(`[Hard-rule 위반][${context}]`, text);
        }
    });
};

const truncateText = (value, maxLen = 24) => {
    const text = String(value ?? '');
    if (text.length <= maxLen) return text;
    return `${text.slice(0, maxLen - 1)}…`;
};

const renderProductCell = (name, id, maxLen = 24, options = {}) => {
    const fullName = String(name ?? '-');
    const showId = options.showId !== false;
    const showGroupLabel = options.showGroupLabel !== false;
    const groupClickable = options.groupClickable !== false;
    const nameClickMode = String(options.nameClickMode || 'popover').toLowerCase();
    const groupMeta = options.groupMeta || getEntityMeta(id);
    const isGrouped = Boolean(showGroupLabel && groupMeta && groupMeta.memberCount > 1 && groupMeta.entityId);
    const nameClickHandler = nameClickMode === 'focus-quadrant'
        ? `event.stopPropagation();focusQuadrantFromTable('${escapeJs(id)}')`
        : `event.stopPropagation();showProductNamePopover('${escapeJs(fullName)}','${escapeJs(id)}')`;
    const groupLabel = isGrouped
        ? `
            <button
                class="group-chip-trigger ${groupClickable ? '' : 'is-static'}"
                type="button"
                ${groupClickable ? `onclick="event.stopPropagation();openGroupEditorWizard({focusEntityId:'${escapeJs(groupMeta.entityId)}'})"` : 'disabled'}
                title="${escapeHtml(groupMeta.entityName || '')}"
            >
                그룹 ${formatNumber(groupMeta.memberCount, 0)}개
            </button>
        `
        : '';
    return `
        <div class="name-inline-wrap">
            <button class="name-trigger" type="button" onclick="${nameClickHandler}">
                <span class="name-clamp-2">${escapeHtml(fullName)}</span>
            </button>
            ${groupLabel}
        </div>
        ${showId && !isGrouped ? `<div class="sub-id">${escapeHtml(id)}</div>` : ''}
    `;
};

const normalizeCsvRows = (rows) => (rows || []).map((row) => {
    const normalized = {};
    Object.entries(row || {}).forEach(([rawKey, value]) => {
        const key = String(rawKey ?? '').replace(/^\uFEFF/, '').trim();
        if (!key) return;
        normalized[key] = value;
    });
    return normalized;
});

function readProductId(row) {
    return String(
        row?.product_id ||
        row?.Product_ID ||
        row?.entry_product_id ||
        row?.aa_product_id ||
        row?.pca_product_id ||
        row?.i ||
        row?.j ||
        ''
    ).trim();
}

function readProductName(row) {
    return String(row?.product_name_latest || row?.Product_Name || row?.product_name || '').trim();
}

function normalizeGroupName(name) {
    const raw = String(name || '').trim();
    if (!raw) return '';
    const removedPrefix = raw.replace(/^(\s*\[[^\]]+\]\s*)+/g, '');
    return removedPrefix.replace(/\s+/g, ' ').trim();
}

const GROUP_PROMO_TOKEN_KEYWORDS = [
    'vip', '특가', '사은품', '전용', '체험', '한정', '이벤트', '세일', 'sale', '혜택',
    '증정', '비밀판매', '아로셀데이', '타임', '재구매', '여름선물', '광복절', '설 맞이', '세컨드'
];

function parseLeadingBracketTokens(name) {
    let rest = String(name || '').trim();
    const tokens = [];
    while (true) {
        const match = rest.match(/^\s*\[([^\]]+)\]\s*/);
        if (!match) break;
        tokens.push(String(match[1] || '').trim());
        rest = rest.slice(match[0].length);
    }
    return { tokens, rest: rest.trim() };
}

function isQuantityToken(token) {
    const raw = String(token || '').trim();
    if (!raw) return false;
    if (/\d+\s*(매|개|입|ea|ml|mL|g|kg)\b/i.test(raw)) return true;
    if (/\d+\s*x\s*\d+/i.test(raw)) return true;
    return false;
}

function isPromotionToken(token) {
    const raw = String(token || '').trim();
    if (!raw) return false;
    if (isQuantityToken(raw)) return false;
    const lower = raw.toLowerCase();
    return GROUP_PROMO_TOKEN_KEYWORDS.some((keyword) => lower.includes(keyword.toLowerCase()));
}

function normalizeGroupKeyName(name) {
    const raw = String(name || '').trim();
    if (!raw) return '';
    const { tokens, rest } = parseLeadingBracketTokens(raw);
    const keptTokens = tokens.filter((token) => !isPromotionToken(token));
    const prefix = keptTokens.map((token) => `[${token}]`).join(' ');
    const combined = `${prefix} ${rest}`.trim();
    return combined.replace(/\s+/g, ' ').trim();
}

function firstDefinedValue(...values) {
    for (let i = 0; i < values.length; i += 1) {
        const value = values[i];
        if (value === null || value === undefined) continue;
        if (typeof value === 'string' && value.trim() === '') continue;
        return value;
    }
    return undefined;
}

function slugify(value) {
    return String(value || '')
        .toLowerCase()
        .replace(/[^a-z0-9가-힣]+/g, '-')
        .replace(/^-+|-+$/g, '')
        .slice(0, 40);
}

function hashString(value) {
    const s = String(value || '');
    let hash = 2166136261;
    for (let i = 0; i < s.length; i += 1) {
        hash ^= s.charCodeAt(i);
        hash = Math.imul(hash, 16777619);
    }
    return (hash >>> 0).toString(16).padStart(8, '0');
}

function buildDeterministicGroupId(seed) {
    const base = slugify(seed) || 'group';
    return `grp_${base}_${hashString(seed)}`;
}

function nowIso() {
    return new Date().toISOString();
}

function sanitizeProductGroupMapRows(rows) {
    const normalizedRows = normalizeCsvRows(rows);
    const dedup = new Map();
    normalizedRows.forEach((row) => {
        const productId = String(row.product_id || '').trim();
        if (!productId) return;
        const status = String(row.status || '').trim().toLowerCase();
        if (status !== 'grouped' && status !== 'ungrouped') return;
        let groupId = String(row.group_id || '').trim();
        let groupName = String(row.group_name || '').trim();
        if (status === 'grouped') {
            if (!groupId && groupName) groupId = buildDeterministicGroupId(groupName);
            if (!groupName && groupId) groupName = groupId;
            if (!groupId || !groupName) return;
        } else {
            groupId = '';
            groupName = '';
        }
        const rule = String(row.rule || (status === 'grouped' ? 'manual' : 'manual')).trim() || 'manual';
        dedup.set(productId, {
            product_id: productId,
            status,
            group_id: groupId,
            group_name: groupName,
            rule,
            updated_at: String(row.updated_at || nowIso()).trim()
        });
    });
    return Array.from(dedup.values());
}

function buildAutoGroups(anchorRows) {
    const rows = anchorRows || [];
    const productMeta = new Map();
    const idsByExactName = new Map();
    const idsByNormalizedName = new Map();
    const knownIds = new Set();

    rows.forEach((row) => {
        const id = readProductId(row);
        const rawName = readProductName(row);
        if (!id || !rawName) return;
        knownIds.add(id);
        const normName = normalizeGroupKeyName(rawName);
        const revenue = toNumber(row.revenue_90d, 0);
        productMeta.set(id, { id, rawName, normName, revenue });

        if (!idsByExactName.has(rawName)) idsByExactName.set(rawName, new Set());
        idsByExactName.get(rawName).add(id);

        if (normName) {
            if (!idsByNormalizedName.has(normName)) idsByNormalizedName.set(normName, new Set());
            idsByNormalizedName.get(normName).add(id);
        }
    });

    const parent = new Map();
    const ensureNode = (id) => {
        if (!parent.has(id)) parent.set(id, id);
    };
    const find = (id) => {
        ensureNode(id);
        let cur = id;
        while (parent.get(cur) !== cur) {
            cur = parent.get(cur);
        }
        let walk = id;
        while (parent.get(walk) !== walk) {
            const next = parent.get(walk);
            parent.set(walk, cur);
            walk = next;
        }
        return cur;
    };
    const union = (a, b) => {
        const ra = find(a);
        const rb = find(b);
        if (ra !== rb) parent.set(rb, ra);
    };
    const unionAll = (idSet) => {
        const ids = Array.from(idSet || []);
        if (ids.length < 2) return;
        ids.forEach((id) => ensureNode(id));
        const [head, ...rest] = ids;
        rest.forEach((id) => union(head, id));
    };

    let exactCandidateCount = 0;
    idsByExactName.forEach((idSet) => {
        if (idSet.size > 1) {
            exactCandidateCount += 1;
            unionAll(idSet);
        }
    });

    let normalizedCandidateCount = 0;
    idsByNormalizedName.forEach((idSet) => {
        if (idSet.size > 1) {
            normalizedCandidateCount += 1;
            unionAll(idSet);
        }
    });

    const components = new Map();
    knownIds.forEach((id) => {
        ensureNode(id);
        const root = find(id);
        if (!components.has(root)) components.set(root, []);
        components.get(root).push(id);
    });

    const idToGroupId = new Map();
    const groupIdToName = new Map();
    const groupIdToRule = new Map();

    components.forEach((members) => {
        if (members.length < 2) return;
        const sortedMembers = [...members].sort();
        const metas = sortedMembers.map((id) => productMeta.get(id)).filter(Boolean);
        metas.sort((a, b) => b.revenue - a.revenue);
        const best = metas[0];
        const exactNames = new Set(metas.map((m) => m.rawName));
        const normalizedNames = new Set(metas.map((m) => m.normName).filter(Boolean));
        const displayName = best?.normName || best?.rawName || sortedMembers[0];
        const seed = `${displayName}|${sortedMembers.join('|')}`;
        const groupId = buildDeterministicGroupId(seed);
        const rule = exactNames.size === 1 ? 'exact_name' : (normalizedNames.size <= 1 ? 'normalized_prefix' : 'normalized_prefix');

        sortedMembers.forEach((id) => idToGroupId.set(id, groupId));
        groupIdToName.set(groupId, displayName);
        groupIdToRule.set(groupId, rule);
    });

    return {
        knownIds,
        productMeta,
        idToGroupId,
        groupIdToName,
        groupIdToRule,
        exactCandidateCount,
        normalizedCandidateCount
    };
}

function buildGroupingState(anchorRows, productGroupRows) {
    const auto = buildAutoGroups(anchorRows || []);
    const overrides = sanitizeProductGroupMapRows(productGroupRows || []);
    const idToGroupId = new Map(auto.idToGroupId);
    const groupIdToName = new Map(auto.groupIdToName);
    const groupIdToRule = new Map(auto.groupIdToRule);
    const ungroupedOverrides = new Set();
    let invalidOverrideCount = 0;

    overrides.forEach((row) => {
        const id = String(row.product_id || '').trim();
        if (!auto.knownIds.has(id)) {
            invalidOverrideCount += 1;
            return;
        }
        if (row.status === 'ungrouped') {
            idToGroupId.delete(id);
            ungroupedOverrides.add(id);
            return;
        }
        ungroupedOverrides.delete(id);
        idToGroupId.set(id, row.group_id);
        groupIdToName.set(row.group_id, row.group_name);
        groupIdToRule.set(row.group_id, row.rule || 'manual');
    });

    ungroupedOverrides.forEach((id) => idToGroupId.delete(id));

    const idToEntityId = new Map();
    const entityIdToMembers = new Map();
    auto.knownIds.forEach((id) => {
        const entityId = idToGroupId.get(id) || id;
        idToEntityId.set(id, entityId);
        if (!entityIdToMembers.has(entityId)) entityIdToMembers.set(entityId, []);
        entityIdToMembers.get(entityId).push(id);
    });

    const entityIdToName = new Map();
    entityIdToMembers.forEach((members, entityId) => {
        const directName = groupIdToName.get(entityId);
        if (directName) {
            entityIdToName.set(entityId, directName);
            return;
        }
        const metas = members.map((id) => auto.productMeta.get(id)).filter(Boolean);
        metas.sort((a, b) => b.revenue - a.revenue);
        const fallback = metas[0]?.normName || metas[0]?.rawName || entityId;
        entityIdToName.set(entityId, fallback);
    });

    const rawNameById = new Map();
    auto.productMeta.forEach((meta, id) => rawNameById.set(id, meta.rawName || id));

    return {
        idToEntityId,
        idToGroupId,
        entityIdToName,
        entityIdToMembers,
        rawNameById,
        groupIdToRule,
        ungroupedOverrides,
        overrideRows: overrides,
        stats: {
            exactCandidateCount: auto.exactCandidateCount,
            normalizedCandidateCount: auto.normalizedCandidateCount,
            groupedEntityCount: Array.from(entityIdToMembers.keys()).filter((id) => entityIdToMembers.get(id).length > 1).length,
            invalidOverrideCount
        }
    };
}

function resolveEntityId(productId) {
    const id = String(productId || '').trim();
    if (!id) return '';
    const grouping = AppState.helpers.grouping;
    if (!grouping || !grouping.idToEntityId) return id;
    return grouping.idToEntityId.get(id) || id;
}

function getEntityMeta(productId) {
    const raw = String(productId || '').trim();
    const entityId = resolveEntityId(raw);
    const grouping = AppState.helpers.grouping;
    if (!grouping) {
        return {
            rawId: raw,
            entityId: raw,
            entityName: getProductName(raw),
            members: [raw],
            memberCount: raw ? 1 : 0
        };
    }
    const members = grouping.entityIdToMembers?.get(entityId) || [raw];
    const entityName = grouping.entityIdToName?.get(entityId) || grouping.rawNameById?.get(raw) || raw;
    return {
        rawId: raw,
        entityId,
        entityName,
        members,
        memberCount: members.length
    };
}

function sumFields(acc, row, fields) {
    fields.forEach((field) => {
        acc[field] = toNumber(acc[field], 0) + toNumber(row[field], 0);
    });
}

function weightedFieldAssign(acc, row, fields, weight) {
    fields.forEach((field) => {
        if (!acc._weighted[field]) acc._weighted[field] = { num: 0, den: 0 };
        const value = toNumber(row[field], NaN);
        if (!Number.isFinite(value)) return;
        acc._weighted[field].num += value * weight;
        acc._weighted[field].den += weight;
    });
}

function finalizeWeightedFields(acc, fields) {
    fields.forEach((field) => {
        const holder = acc._weighted[field];
        acc[field] = holder && holder.den > 0 ? holder.num / holder.den : null;
    });
}

function determinePrimaryType(typeScores, fallback = '-') {
    const entries = Object.entries(typeScores || {});
    if (!entries.length) return fallback;
    entries.sort((a, b) => toNumber(b[1], 0) - toNumber(a[1], 0));
    if (toNumber(entries[0][1], 0) <= 0) return fallback;
    return entries[0][0];
}

function transformAnchorScoredRows(rows) {
    const src = rows || [];
    const groupMap = new Map();
    const sumFieldsList = [
        'first_customer_cnt', 'product_order_cnt_1y', 'product_unit_qty_1y', 'repurchase_customer_cnt_90d',
        'revenue_90d', 'AA_Broad', 'AA_Heavy', 'AA_Qualified', 'PCA_Core', 'PCA_Deep', 'PCA_Scale'
    ];
    const weightedFields = [
        'AA_Score', 'PCA_Score', 'Entry_Gravity_Score', 'Expansion_Gravity_Score',
        'first_customer_ratio', 'p50_addl_order_cnt_90d',
        'p75_addl_order_cnt_90d', 'p90_addl_order_cnt_90d', 'addl_order_rate_90d',
        'p75_retention_days', 'PrimaryAnchorScore'
    ];

    // 그룹핑 시 PGM 점수는 그대로 두지 않고, entity(그룹) 단위로 다시 집계됩니다.
    // AA/PCA/PrimaryAnchorScore 등 점수형 필드는 아래 weight 기준 가중평균으로 재계산됩니다.
    src.forEach((row) => {
        const id = readProductId(row);
        if (!id) return;
        const entityId = resolveEntityId(id);
        if (!groupMap.has(entityId)) {
            groupMap.set(entityId, {
                product_id: entityId,
                product_name_latest: getEntityMeta(entityId).entityName,
                members: new Set(),
                aaTypeScores: {},
                pcaTypeScores: {},
                _weighted: {}
            });
        }
        const acc = groupMap.get(entityId);
        acc.members.add(id);
        const weight = Math.max(1, toNumber(row.first_customer_cnt, 0), toNumber(row.product_order_cnt_1y, 0));
        sumFields(acc, row, sumFieldsList);
        weightedFieldAssign(acc, row, weightedFields, weight);

        const aaType = normalizeCategoryValue(row.AA_Primary_Type || row.Entry_Gravity_Primary_Type, '');
        if (aaType) acc.aaTypeScores[aaType] = toNumber(acc.aaTypeScores[aaType], 0) + weight;
        const pcaType = normalizeCategoryValue(row.PCA_Primary_Type || row.Expansion_Gravity_Primary_Type, '');
        if (pcaType) acc.pcaTypeScores[pcaType] = toNumber(acc.pcaTypeScores[pcaType], 0) + weight;
    });

    const result = Array.from(groupMap.values()).map((acc) => {
        finalizeWeightedFields(acc, weightedFields);
        const aaPrimary = determinePrimaryType(acc.aaTypeScores, 'Broad');
        const pcaPrimary = determinePrimaryType(acc.pcaTypeScores, 'Core');
        const firstCustomerCnt = toNumber(acc.first_customer_cnt, 0);
        const repurchaseCustomerCnt90d = toNumber(acc.repurchase_customer_cnt_90d, 0);
        return {
            product_id: acc.product_id,
            product_name_latest: acc.product_name_latest,
            first_customer_cnt: firstCustomerCnt,
            repurchase_customer_cnt_90d: repurchaseCustomerCnt90d,
            product_order_cnt_1y: toNumber(acc.product_order_cnt_1y, 0),
            product_unit_qty_1y: toNumber(acc.product_unit_qty_1y, 0),
            revenue_90d: toNumber(acc.revenue_90d, 0),
            AA_Score: acc.AA_Score,
            PCA_Score: acc.PCA_Score,
            Entry_Gravity_Score: acc.Entry_Gravity_Score,
            Expansion_Gravity_Score: acc.Expansion_Gravity_Score,
            // 90일 추가구매 가능성은 raw count 기준으로 다시 계산해야 그룹 상품에서도 왜곡되지 않습니다.
            repurchase_rate_90d: firstCustomerCnt > 0 ? repurchaseCustomerCnt90d / firstCustomerCnt : 0,
            first_customer_ratio: acc.first_customer_ratio,
            p50_addl_order_cnt_90d: acc.p50_addl_order_cnt_90d,
            p75_addl_order_cnt_90d: acc.p75_addl_order_cnt_90d,
            p90_addl_order_cnt_90d: acc.p90_addl_order_cnt_90d,
            addl_order_rate_90d: acc.addl_order_rate_90d,
            p75_retention_days: acc.p75_retention_days,
            PrimaryAnchorScore: acc.PrimaryAnchorScore,
            AA_Primary_Type: aaPrimary,
            PCA_Primary_Type: pcaPrimary,
            Entry_Gravity_Primary_Type: aaPrimary,
            Expansion_Gravity_Primary_Type: pcaPrimary,
            AA_Broad: toNumber(acc.AA_Broad, 0),
            AA_Heavy: toNumber(acc.AA_Heavy, 0),
            AA_Qualified: toNumber(acc.AA_Qualified, 0),
            PCA_Core: toNumber(acc.PCA_Core, 0),
            PCA_Deep: toNumber(acc.PCA_Deep, 0),
            PCA_Scale: toNumber(acc.PCA_Scale, 0),
            member_count: acc.members.size,
            member_ids: Array.from(acc.members).sort().join('|')
        };
    });

    result.sort((a, b) => toNumber(b.revenue_90d, 0) - toNumber(a.revenue_90d, 0));
    return result;
}

function transformAnchorTransitionRows(rows, groupedAnchorRows) {
    const src = rows || [];
    const groupedByPath = new Map();
    const rawCohortByAaEntity = new Map();
    const groupedCohort = new Map((groupedAnchorRows || []).map((row) => [String(row.product_id), toNumber(row.first_customer_cnt, 0)]));

    src.forEach((row) => {
        const aaRaw = String(row.aa_product_id || '').trim();
        const pcaRaw = String(row.pca_product_id || '').trim();
        if (!aaRaw || !pcaRaw) return;
        const aa = resolveEntityId(aaRaw);
        const pca = resolveEntityId(pcaRaw);
        if (!aa || !pca || aa === pca) return;

        const transitionCustomers = toNumber(row.transition_customer_cnt, 0);
        const avgDays = toNumber(row.avg_days_to_pca, NaN);
        const rawCohort = toNumber(row.aa_cohort_customer_cnt, 0);
        if (!rawCohortByAaEntity.has(aa)) rawCohortByAaEntity.set(aa, new Map());
        const aaRawMap = rawCohortByAaEntity.get(aa);
        aaRawMap.set(aaRaw, Math.max(toNumber(aaRawMap.get(aaRaw), 0), rawCohort));

        const key = `${aa}::${pca}`;
        if (!groupedByPath.has(key)) {
            groupedByPath.set(key, {
                aa_product_id: aa,
                pca_product_id: pca,
                transition_customer_cnt: 0,
                avg_days_num: 0,
                avg_days_den: 0
            });
        }
        const acc = groupedByPath.get(key);
        acc.transition_customer_cnt += transitionCustomers;
        if (transitionCustomers > 0 && Number.isFinite(avgDays)) {
            acc.avg_days_num += transitionCustomers * avgDays;
            acc.avg_days_den += transitionCustomers;
        }
    });

    const result = Array.from(groupedByPath.values()).map((acc) => {
        const fallbackCohort = Array.from(rawCohortByAaEntity.get(acc.aa_product_id)?.values() || [])
            .reduce((sum, v) => sum + toNumber(v, 0), 0);
        const cohort = toNumber(groupedCohort.get(acc.aa_product_id), fallbackCohort);
        const rate = cohort > 0 ? acc.transition_customer_cnt / cohort : 0;
        const avgDays = acc.avg_days_den > 0 ? acc.avg_days_num / acc.avg_days_den : null;
        return {
            aa_product_id: acc.aa_product_id,
            pca_product_id: acc.pca_product_id,
            transition_customer_cnt: acc.transition_customer_cnt,
            avg_days_to_pca: avgDays,
            aa_cohort_customer_cnt: cohort,
            transition_rate: rate,
            entry_product_id: acc.aa_product_id,
            expansion_product_id: acc.pca_product_id,
            avg_days_to_expansion: avgDays
        };
    });

    result.sort((a, b) => toNumber(b.transition_customer_cnt, 0) - toNumber(a.transition_customer_cnt, 0));
    return result;
}

function transformCartAnchorDetailRows(rows) {
    const src = rows || [];
    const pairMap = new Map();
    src.forEach((row) => {
        const iRaw = String(row.i || '').trim();
        const jRaw = String(row.j || '').trim();
        if (!iRaw || !jRaw) return;
        const i = resolveEntityId(iRaw);
        const j = resolveEntityId(jRaw);
        if (!i || !j || i === j) return;
        const [a, b] = i < j ? [i, j] : [j, i];
        const key = `${a}::${b}`;
        const co = toNumber(row.co_order_cnt, 0);
        if (!pairMap.has(key)) pairMap.set(key, { i: a, j: b, co_order_cnt: 0 });
        pairMap.get(key).co_order_cnt += co;
    });
    const result = Array.from(pairMap.values())
        .sort((a, b) => toNumber(b.co_order_cnt, 0) - toNumber(a.co_order_cnt, 0))
        .map((row, idx) => ({ ...row, rn: idx + 1 }));
    return result;
}

function buildTopCompanionMapFromDetail(detailRows) {
    const top = new Map();
    (detailRows || []).forEach((row) => {
        const i = String(row.i || '').trim();
        const j = String(row.j || '').trim();
        const co = toNumber(row.co_order_cnt, 0);
        if (!i || !j || i === j || co <= 0) return;
        const currentI = top.get(i);
        if (!currentI || co > currentI.co_order_cnt) top.set(i, { id: j, co_order_cnt: co });
        const currentJ = top.get(j);
        if (!currentJ || co > currentJ.co_order_cnt) top.set(j, { id: i, co_order_cnt: co });
    });
    return top;
}

function transformCartAnchorRows(rows) {
    const src = rows || [];
    const map = new Map();
    src.forEach((row) => {
        const rawId = String(row.product_id || '').trim();
        if (!rawId) return;
        const id = resolveEntityId(rawId);
        if (!id) return;
        if (!map.has(id)) {
            map.set(id, {
                product_id: id,
                order_cnt: 0,
                companion_cnt: 0,
                volume_raw: 0,
                volume_weight: 0,
                attach_num: 0,
                attach_den: 0,
                median_num: 0,
                median_den: 0,
                breadth_num: 0,
                breadth_den: 0,
                top1_num: 0,
                top1_den: 0,
                top3_num: 0,
                top3_den: 0,
                caTypeScores: {}
            });
        }
        const acc = map.get(id);
        const orderCnt = Math.max(1, toNumber(row.order_cnt, 0));
        acc.order_cnt += toNumber(row.order_cnt, 0);
        acc.companion_cnt += toNumber(row.companion_cnt, 0);
        acc.volume_raw += toNumber(row.volume_raw, 0);
        acc.volume_weight += toNumber(row.volume_weight, 0);
        acc.attach_num += toNumber(row.attach_rate, 0) * orderCnt;
        acc.attach_den += orderCnt;
        acc.median_num += toNumber(row.median_cart_size, 0) * orderCnt;
        acc.median_den += orderCnt;
        acc.breadth_num += toNumber(row.breadth_lift, 0) * orderCnt;
        acc.breadth_den += orderCnt;
        acc.top1_num += toNumber(row.top1_share, 0) * orderCnt;
        acc.top1_den += orderCnt;
        acc.top3_num += toNumber(row.top3_share, 0) * orderCnt;
        acc.top3_den += orderCnt;
        const caType = normalizeCategoryValue(row.CA_Primary_Type || row.Basket_Gravity_Primary_Type, 'None');
        acc.caTypeScores[caType] = toNumber(acc.caTypeScores[caType], 0) + orderCnt;
    });

    return Array.from(map.values()).map((acc) => {
        const caType = determinePrimaryType(acc.caTypeScores, 'None');
        return {
            product_id: acc.product_id,
            order_cnt: acc.order_cnt,
            attach_rate: acc.attach_den > 0 ? acc.attach_num / acc.attach_den : 0,
            median_cart_size: acc.median_den > 0 ? acc.median_num / acc.median_den : 0,
            breadth_lift: acc.breadth_den > 0 ? acc.breadth_num / acc.breadth_den : 0,
            companion_cnt: acc.companion_cnt,
            top1_share: acc.top1_den > 0 ? acc.top1_num / acc.top1_den : 0,
            top3_share: acc.top3_den > 0 ? acc.top3_num / acc.top3_den : 0,
            volume_raw: acc.volume_raw,
            volume_weight: acc.volume_weight,
            CA_Primary_Type: caType,
            Basket_Gravity_Primary_Type: caType
        };
    }).sort((a, b) => toNumber(b.order_cnt, 0) - toNumber(a.order_cnt, 0));
}

function transformCaProfileRows(rows, topCompanionMap) {
    const src = rows || [];
    const map = new Map();
    src.forEach((row) => {
        const rawId = String(row.product_id || '').trim();
        if (!rawId) return;
        const id = resolveEntityId(rawId);
        if (!id) return;
        if (!map.has(id)) {
            map.set(id, {
                product_id: id,
                companion_count: 0,
                attach_num: 0,
                attach_den: 0,
                median_num: 0,
                median_den: 0,
                breadth_num: 0,
                breadth_den: 0,
                top1_num: 0,
                top1_den: 0,
                top3_num: 0,
                top3_den: 0,
                caTypeScores: {}
            });
        }
        const acc = map.get(id);
        const companionCount = Math.max(0, toNumber(row.companion_count, 0));
        const weight = companionCount;
        acc.companion_count += companionCount;
        if (weight > 0) {
            acc.attach_num += toNumber(row.attach_rate, 0) * weight;
            acc.attach_den += weight;
            acc.median_num += toNumber(row.median_cart_size, 0) * weight;
            acc.median_den += weight;
            acc.breadth_num += toNumber(row.breadth_lift, 0) * weight;
            acc.breadth_den += weight;
            acc.top1_num += toNumber(row.top1_share, 0) * weight;
            acc.top1_den += weight;
            acc.top3_num += toNumber(row.top3_share, 0) * weight;
            acc.top3_den += weight;
        }
        const caType = normalizeCategoryValue(row.ca_type, 'None');
        if (weight > 0) {
            acc.caTypeScores[caType] = toNumber(acc.caTypeScores[caType], 0) + weight;
        }
    });

    return Array.from(map.values()).map((acc) => {
        const caType = determinePrimaryType(acc.caTypeScores, 'None');
        return {
            product_id: acc.product_id,
            ca_type: caType,
            attach_rate: acc.attach_den > 0 ? acc.attach_num / acc.attach_den : 0,
            median_cart_size: acc.median_den > 0 ? acc.median_num / acc.median_den : 0,
            breadth_lift: acc.breadth_den > 0 ? acc.breadth_num / acc.breadth_den : 0,
            companion_count: acc.companion_count,
            top1_share: acc.top1_den > 0 ? acc.top1_num / acc.top1_den : 0,
            top3_share: acc.top3_den > 0 ? acc.top3_num / acc.top3_den : 0,
            top1_companion_product_id: topCompanionMap.get(acc.product_id)?.id || ''
        };
    }).sort((a, b) => toNumber(b.attach_rate, 0) - toNumber(a.attach_rate, 0));
}

function transformAaCohortJourneyRows(rows) {
    const src = rows || [];
    const map = new Map();
    src.forEach((row) => {
        const rawId = String(row.aa_product_id || row.entry_product_id || '').trim();
        const date = String(row.cohort_date || '').trim();
        if (!rawId || !date) return;
        const id = resolveEntityId(rawId);
        const aaType = normalizeCategoryValue(row.aa_type, 'Unknown');
        const key = `${date}::${id}::${aaType}`;
        if (!map.has(key)) {
            map.set(key, {
                cohort_date: date,
                aa_product_id: id,
                aa_type: aaType,
                cohort_customers: 0,
                repeat_7d_num: 0,
                repeat_30d_num: 0,
                repeat_90d_num: 0,
                pca_30d_num: 0,
                pca_90d_num: 0,
                avg_days_num: 0,
                avg_days_den: 0,
                avg_rev_num: 0
            });
        }
        const acc = map.get(key);
        const cohort = Math.max(0, toNumber(row.cohort_customers, 0));
        acc.cohort_customers += cohort;
        acc.repeat_7d_num += toNumber(row.repeat_7d_rate, 0) * cohort;
        acc.repeat_30d_num += toNumber(row.repeat_30d_rate, 0) * cohort;
        acc.repeat_90d_num += toNumber(row.repeat_90d_rate, 0) * cohort;
        acc.pca_30d_num += toNumber(row.pca_transition_30d_rate, 0) * cohort;
        acc.pca_90d_num += toNumber(row.pca_transition_90d_rate, 0) * cohort;
        const days = toNumber(row.avg_days_to_pca, NaN);
        if (Number.isFinite(days)) {
            acc.avg_days_num += days * cohort;
            acc.avg_days_den += cohort;
        }
        acc.avg_rev_num += toNumber(row.avg_revenue_90d, 0) * cohort;
    });

    return Array.from(map.values()).map((acc) => ({
        cohort_date: acc.cohort_date,
        aa_product_id: acc.aa_product_id,
        entry_product_id: acc.aa_product_id,
        aa_type: acc.aa_type,
        cohort_customers: acc.cohort_customers,
        repeat_7d_rate: acc.cohort_customers > 0 ? acc.repeat_7d_num / acc.cohort_customers : 0,
        repeat_30d_rate: acc.cohort_customers > 0 ? acc.repeat_30d_num / acc.cohort_customers : 0,
        repeat_90d_rate: acc.cohort_customers > 0 ? acc.repeat_90d_num / acc.cohort_customers : 0,
        pca_transition_30d_rate: acc.cohort_customers > 0 ? acc.pca_30d_num / acc.cohort_customers : 0,
        pca_transition_90d_rate: acc.cohort_customers > 0 ? acc.pca_90d_num / acc.cohort_customers : 0,
        avg_days_to_pca: acc.avg_days_den > 0 ? acc.avg_days_num / acc.avg_days_den : null,
        avg_days_to_expansion: acc.avg_days_den > 0 ? acc.avg_days_num / acc.avg_days_den : null,
        avg_revenue_90d: acc.cohort_customers > 0 ? acc.avg_rev_num / acc.cohort_customers : 0
    })).sort((a, b) => toNumber(b.cohort_customers, 0) - toNumber(a.cohort_customers, 0));
}

function transformAaTransitionPathRows(rows, groupedCohortRows) {
    const src = rows || [];
    const cohortMap = new Map();
    (groupedCohortRows || []).forEach((row) => {
        const date = String(row.cohort_date || '').trim();
        const aa = String(row.aa_product_id || '').trim();
        if (!date || !aa) return;
        const key = `${date}::${aa}`;
        cohortMap.set(key, toNumber(cohortMap.get(key), 0) + toNumber(row.cohort_customers, 0));
    });

    const map = new Map();
    src.forEach((row) => {
        const date = String(row.cohort_date || '').trim();
        const aaRaw = String(row.aa_product_id || row.entry_product_id || '').trim();
        const pcaRaw = String(row.pca_product_id || row.expansion_product_id || '').trim();
        if (!date || !aaRaw || !pcaRaw) return;
        const aa = resolveEntityId(aaRaw);
        const pca = resolveEntityId(pcaRaw);
        if (!aa || !pca || aa === pca) return;
        const key = `${date}::${aa}::${pca}`;
        if (!map.has(key)) {
            map.set(key, {
                cohort_date: date,
                aa_product_id: aa,
                pca_product_id: pca,
                transition_customers: 0,
                avg_days_num: 0,
                avg_days_den: 0,
                aaTypeScores: {}
            });
        }
        const acc = map.get(key);
        const trans = toNumber(row.transition_customers, 0);
        const days = toNumber(firstDefinedValue(row.avg_days_to_pca, row.avg_days_to_expansion), NaN);
        const aaType = normalizeCategoryValue(row.aa_type, 'Unknown');
        acc.transition_customers += trans;
        if (trans > 0 && Number.isFinite(days)) {
            acc.avg_days_num += trans * days;
            acc.avg_days_den += trans;
        }
        acc.aaTypeScores[aaType] = toNumber(acc.aaTypeScores[aaType], 0) + trans;
    });

    const result = Array.from(map.values()).map((acc) => {
        const cohort = toNumber(cohortMap.get(`${acc.cohort_date}::${acc.aa_product_id}`), 0);
        const avgDays = acc.avg_days_den > 0 ? acc.avg_days_num / acc.avg_days_den : null;
        return {
            cohort_date: acc.cohort_date,
            aa_product_id: acc.aa_product_id,
            entry_product_id: acc.aa_product_id,
            aa_type: determinePrimaryType(acc.aaTypeScores, 'Unknown'),
            pca_product_id: acc.pca_product_id,
            expansion_product_id: acc.pca_product_id,
            transition_customers: acc.transition_customers,
            transition_rate: cohort > 0 ? acc.transition_customers / cohort : 0,
            avg_days_to_pca: avgDays,
            avg_days_to_expansion: avgDays
        };
    });
    result.sort((a, b) => toNumber(b.transition_customers, 0) - toNumber(a.transition_customers, 0));
    return result;
}

function rebuildDerivedData() {
    const raw = AppState.rawData || {};
    AppState.helpers.grouping = buildGroupingState(raw.anchorScored || [], raw.productGroupMap || []);

    AppState.data.brandScore = raw.brandScore || [];
    AppState.data.biiWindow = raw.biiWindow || [];
    AppState.data.apfActionRules = raw.apfActionRules || [];
    AppState.data.productGroupMap = sanitizeProductGroupMapRows(raw.productGroupMap || []);

    AppState.data.anchorScored = transformAnchorScoredRows(raw.anchorScored || []);
    AppState.data.anchorTransition = transformAnchorTransitionRows(raw.anchorTransition || [], AppState.data.anchorScored);
    AppState.data.cartAnchorDetail = transformCartAnchorDetailRows(raw.cartAnchorDetail || []);
    const topCompanionMap = buildTopCompanionMapFromDetail(AppState.data.cartAnchorDetail);
    AppState.data.cartAnchor = transformCartAnchorRows(raw.cartAnchor || []);
    AppState.data.caProfile = transformCaProfileRows(raw.caProfile || [], topCompanionMap);
    AppState.data.aaCohortJourney = transformAaCohortJourneyRows(raw.aaCohortJourney || []);
    AppState.data.aaTransitionPath = transformAaTransitionPathRows(raw.aaTransitionPath || [], AppState.data.aaCohortJourney);
    AppState.helpers.productNameMap = buildProductNameMap();
}

const asNullableNumber = (value) => {
    const num = Number(value);
    return Number.isFinite(num) ? num : null;
};

const convertBrandImpactWindowsToBiiWindow = (rows) => {
    return normalizeCsvRows(rows).map((row) => {
        const windowKey = String(withFallback(row.window_key, ''));
        const matched = windowKey.match(/(\d+)/);
        const windowDays = matched ? toNumber(matched[1], null) : null;
        return {
            as_of_date: withFallback(row.period_end, row.as_of_date || ''),
            window_days: windowDays,
            bii: asNullableNumber(row.bii_t),
            bhi: asNullableNumber(row.bhi),
            clv_norm: asNullableNumber(row.clv_t_norm),
            customer_strength_norm: asNullableNumber(row.customer_strength_t_norm),
            stage: withFallback(row.stage, '-'),
            baseline_days: asNullableNumber(row.baseline_days),
            confidence: withFallback(row.confidence_index || row.confidence, '-')
        };
    }).filter((row) => row.as_of_date && row.window_days !== null && row.bii !== null);
};

const convertBrandImpactIndexToBiiWindow = (rows) => {
    const windowMap = [
        { days: 1, field: 'bii_1d' },
        { days: 7, field: 'bii_7d' },
        { days: 30, field: 'bii_30d' },
        { days: 90, field: 'bii_90d' },
        { days: 365, field: 'bii_365d' }
    ];

    const normalizedRows = normalizeCsvRows(rows);
    const result = [];
    normalizedRows.forEach((row) => {
        windowMap.forEach((w) => {
            const bii = asNullableNumber(row[w.field]);
            if (bii === null) return;
            result.push({
                as_of_date: withFallback(row.analysis_end_date, row.as_of_date || ''),
                window_days: w.days,
                bii,
                bhi: asNullableNumber(row.bhi),
                clv_norm: asNullableNumber(row.clv_norm),
                customer_strength_norm: asNullableNumber(row.customer_strength_norm),
                stage: withFallback(row.stage, '-'),
                baseline_days: asNullableNumber(row.baseline_days),
                confidence: withFallback(row.confidence_index || row.confidence, '-')
            });
        });
    });

    return result.filter((row) => row.as_of_date);
};

const getUploadFileConfig = (filename) => {
    const lowerName = String(filename || '').toLowerCase();
    const configs = Object.values(REQUIRED_FILES);
    const exact = configs.find((config) => {
        const names = [config.filename, ...(config.aliases || [])]
            .map((name) => String(name).toLowerCase());
        return names.includes(lowerName);
    });
    if (exact) return exact;
    const byNameStem = configs.find((config) => {
        const stems = [config.filename, ...(config.aliases || [])]
            .map((name) => String(name).toLowerCase().replace(/\.csv$/i, ''))
            .filter(Boolean);
        return stems.some((stem) => lowerName.includes(stem));
    });
    if (byNameStem) return byNameStem;
    return configs
        .sort((a, b) => b.key.length - a.key.length)
        .find((config) => lowerName.includes(config.key.toLowerCase()));
};

const preprocessUploadRows = (config, filename, rows) => {
    if (!config) return normalizeCsvRows(rows);
    const lowerName = String(filename || '').toLowerCase();
    if (config.key === REQUIRED_FILES.productGroupMap.key) {
        return sanitizeProductGroupMapRows(rows);
    }
    if (config.key === REQUIRED_FILES.biiWindow.key && lowerName.includes('brand_impact_windows')) {
        return convertBrandImpactWindowsToBiiWindow(rows);
    }
    if (config.key === REQUIRED_FILES.biiWindow.key && lowerName.includes('brand_impact_index')) {
        return convertBrandImpactIndexToBiiWindow(rows);
    }
    return normalizeCsvRows(rows);
};

const getUploadPriority = (config, filename) => {
    const lowerName = String(filename || '').toLowerCase();
    const primaryName = String(config.filename || '').toLowerCase();
    const primaryStem = primaryName.replace(/\.csv$/i, '');
    if (lowerName === primaryName || (primaryStem && lowerName.includes(primaryStem)) || lowerName.includes('_insight_')) return 3;
    if (lowerName.includes('brand_impact_index')) return 2;
    if (lowerName.includes('brand_impact_windows')) return 1;
    if ((config.aliases || []).map((name) => String(name).toLowerCase()).includes(lowerName)) return 2;
    return 1;
};

function normalizeSearchMode(mode) {
    const key = String(mode || '').toLowerCase();
    return ['all', 'name', 'id'].includes(key) ? key : 'all';
}

function renderSearchUI(viewName, placeholder, options = {}) {
    const state = AppState.viewState[viewName] || {};
    const query = state.searchQuery || '';
    const includeModeSelect = Boolean(options.includeModeSelect);
    const mode = normalizeSearchMode(state.searchMode || 'all');
    const selectOptions = [
        ['all', '전체'],
        ['name', '상품명'],
        ['id', 'ID']
    ];
    return `
        <div class="search-container animate-fade-in">
            <div class="search-combo">
                ${includeModeSelect ? `
                    <select class="search-select" onchange="handleSearchModeChange('${viewName}', this.value)">
                        ${selectOptions.map(([value, label]) => `
                            <option value="${value}" ${mode === value ? 'selected' : ''}>${label}</option>
                        `).join('')}
                    </select>
                ` : ''}
                <div class="search-wrapper">
                    <i class="ph ph-magnifying-glass"></i>
                    <input
                        id="search-input-${viewName}"
                        type="text"
                        class="search-input"
                        placeholder="${toFriendlyText(placeholder)}"
                        value="${query}"
                        oncompositionstart="handleSearchCompositionStart('${viewName}')"
                        oncompositionend="handleSearchCompositionEnd('${viewName}', this)"
                        oninput="handleGlobalSearch('${viewName}', this.value, this.selectionStart, this.selectionEnd)"
                    >
                </div>
            </div>
        </div>
    `;
}

function applyFriendlyUi(root = document.body) {
    if (!root) return;
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
    const textNodes = [];
    while (walker.nextNode()) textNodes.push(walker.currentNode);
    textNodes.forEach((node) => {
        if (!node || !node.nodeValue) return;
        const next = toFriendlyText(node.nodeValue);
        if (next !== node.nodeValue) node.nodeValue = next;
        validateUiHardRule(node.nodeValue, 'text-node');
    });
    root.querySelectorAll?.('[title],[placeholder],[aria-label]').forEach((el) => {
        ['title', 'placeholder', 'aria-label'].forEach((attr) => {
            if (!el.hasAttribute(attr)) return;
            const next = toFriendlyText(el.getAttribute(attr));
            el.setAttribute(attr, next);
            validateUiHardRule(next, attr);
        });
    });
    applyMetricTooltips(root);
}

function getMetricTooltip(text) {
    const normalized = String(text || '')
        .replace(/[▲▼]/g, '')
        .replace(/\s+/g, ' ')
        .trim();
    if (!normalized) return '';
    const matched = METRIC_TOOLTIP_RULES.find((rule) => rule.pattern.test(normalized));
    return matched ? matched.description : '';
}

function applyMetricTooltips(root = document.body) {
    if (!root?.querySelectorAll) return;
    const targets = root.querySelectorAll(
        '.hero-metric label, .journey-kpi label, .pgm-metrics label, .structure-item label, .value-driver-item label, .fitness-summary-card label, .data-table th'
    );
    targets.forEach((el) => {
        const tooltip = getMetricTooltip(el.textContent);
        if (!tooltip) {
            el.classList.remove('metric-tooltip-target');
            if (el.dataset.metricTooltipApplied === '1') {
                el.removeAttribute('title');
                el.removeAttribute('aria-label');
                el.removeAttribute('data-metric-tooltip');
                delete el.dataset.metricTooltipApplied;
            }
            return;
        }
        const label = String(el.textContent || '').replace(/\s+/g, ' ').trim();
        el.setAttribute('title', tooltip);
        el.setAttribute('aria-label', `${label} - ${tooltip}`);
        el.setAttribute('data-metric-tooltip', tooltip);
        el.classList.add('metric-tooltip-target');
        el.dataset.metricTooltipApplied = '1';
    });
}

function buildEntitySearchTokens(productId, getName) {
    const meta = getEntityMeta(productId);
    const ids = [
        productId,
        meta.entityId,
        ...(meta.members || [])
    ]
        .map((v) => String(v || '').toLowerCase().trim())
        .filter(Boolean);
    const names = [
        getName(productId),
        meta.entityName
    ]
        .map((v) => String(v || '').toLowerCase().trim())
        .filter(Boolean);
    return { ids, names };
}

function matchesSearchQuery(query, mode, ids = [], names = []) {
    const q = String(query || '').toLowerCase().trim();
    if (!q) return true;
    const idMatch = ids.some((text) => text.includes(q));
    const nameMatch = names.some((text) => text.includes(q));
    const searchMode = normalizeSearchMode(mode);
    if (searchMode === 'id') return idMatch;
    if (searchMode === 'name') return nameMatch;
    return idMatch || nameMatch;
}

function destroyCarts() {
    Object.values(AppState.charts).forEach((chart) => chart.destroy());
    AppState.charts = {};
}

function buildProductNameMap() {
    const products = AppState.data.anchorScored || [];
    const rawProducts = AppState.rawData.anchorScored || [];
    const grouping = AppState.helpers.grouping;
    const map = new Map();
    products.forEach((p) => {
        const id = readProductId(p);
        const name = readProductName(p);
        if (id) map.set(String(id).trim(), name || String(id));
    });
    rawProducts.forEach((p) => {
        const id = readProductId(p);
        const name = readProductName(p);
        if (id && name && !map.has(String(id).trim())) {
            map.set(String(id).trim(), name);
        }
    });
    if (grouping?.entityIdToName && grouping?.idToEntityId) {
        grouping.idToEntityId.forEach((entityId, rawId) => {
            const name = grouping.entityIdToName.get(entityId);
            if (name) map.set(String(rawId).trim(), name);
        });
        grouping.entityIdToName.forEach((name, entityId) => {
            if (name) map.set(String(entityId).trim(), name);
        });
    }
    return map;
}

function getProductName(id) {
    if (!id) return '-';
    const key = String(id).trim();
    const grouping = AppState.helpers.grouping;
    if (grouping?.entityIdToName && grouping.entityIdToName.has(key)) {
        return grouping.entityIdToName.get(key);
    }
    if (grouping?.idToEntityId && grouping.idToEntityId.has(key)) {
        const entityId = grouping.idToEntityId.get(key);
        if (grouping.entityIdToName.has(entityId)) return grouping.entityIdToName.get(entityId);
    }
    if (!AppState.helpers.productNameMap) {
        AppState.helpers.productNameMap = buildProductNameMap();
    }
    return AppState.helpers.productNameMap.get(key) || id;
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

function getInsightSnapshotDates() {
    const pool = [
        ...(AppState.data.aaCohortJourney || []).map((row) => row.cohort_date),
        ...(AppState.data.aaTransitionPath || []).map((row) => row.cohort_date),
        ...(AppState.data.biiWindow || []).map((row) => row.as_of_date)
    ];
    const uniq = new Set();
    pool.forEach((value) => {
        const date = toDate(value);
        if (!date) return;
        const normalized = date.toISOString().slice(0, 10);
        uniq.add(normalized);
    });
    return Array.from(uniq).sort((a, b) => (a < b ? 1 : -1));
}


function restoreSearchInputCursor(viewName, selectionStart = null, selectionEnd = null) {
    const input = document.getElementById(`search-input-${viewName}`);
    if (!input) return;
    input.focus();
    if (Number.isInteger(selectionStart) && Number.isInteger(selectionEnd)) {
        const start = Math.max(0, Math.min(selectionStart, input.value.length));
        const end = Math.max(0, Math.min(selectionEnd, input.value.length));
        try {
            input.setSelectionRange(start, end);
        } catch (_) {
            // ignore selection restore failure
        }
    }
}

window.handleGlobalSearch = (viewName, query, selectionStart = null, selectionEnd = null) => {
    const state = AppState.viewState[viewName];
    if (!state) return;
    state.searchQuery = query;
    if (state.searchComposing) return;

    if (!window.searchTimeouts) window.searchTimeouts = {};
    if (window.searchTimeouts[viewName]) clearTimeout(window.searchTimeouts[viewName]);
    window.searchTimeouts[viewName] = setTimeout(() => {
        if (viewName === 'products') {
            renderProductsTableOnly();
        } else if (viewName === 'transitions') {
            renderTransitionsTable();
        }
        if (viewName === 'transitions') {
            restoreSearchInputCursor(viewName, selectionStart, selectionEnd);
        }
        delete window.searchTimeouts[viewName];
    }, 150);
};

window.handleSearchCompositionStart = (viewName) => {
    const state = AppState.viewState[viewName];
    if (!state) return;
    state.searchComposing = true;
    if (window.searchTimeouts?.[viewName]) {
        clearTimeout(window.searchTimeouts[viewName]);
        delete window.searchTimeouts[viewName];
    }
};

window.handleSearchCompositionEnd = (viewName, inputEl) => {
    const state = AppState.viewState[viewName];
    if (!state) return;
    state.searchComposing = false;
    if (!inputEl) return;
    window.handleGlobalSearch(viewName, inputEl.value, inputEl.selectionStart, inputEl.selectionEnd);
};

window.handleSearchModeChange = (viewName, mode) => {
    if (!AppState.viewState[viewName]) return;
    AppState.viewState[viewName].searchMode = normalizeSearchMode(mode);
    if (viewName === 'transitions') {
        renderTransitionsTable();
        restoreSearchInputCursor(viewName);
        return;
    }
    if (viewName === 'products') {
        renderProducts();
    }
};

window.closeCartFlowModal = () => {
    const modal = document.getElementById('cart-flow-modal');
    if (modal) modal.remove();
};

window.closeRetentionFlowModal = () => {
    const modal = document.getElementById('retention-flow-modal');
    if (modal) modal.remove();
};

window.openRetentionFlowModal = async (entityId) => {
    const focusId = String(entityId || '').trim();
    if (!focusId) return;

    window.closeRetentionFlowModal();
    const modal = document.createElement('div');
    modal.id = 'retention-flow-modal';
    modal.className = 'modal-overlay active';
    modal.innerHTML = `
        <div class="modal-card retention-flow-modal-card">
            <div class="modal-header">
                <h3 class="modal-title">90일 리텐션 흐름</h3>
                <button class="modal-close" type="button" onclick="closeRetentionFlowModal()">&times;</button>
            </div>
            <div class="modal-body">
                <div class="modal-loading">
                    <div class="spinner"></div>
                    <p style="margin-top:1rem">90일 리텐션 흐름 데이터를 불러오는 중이에요.</p>
                </div>
            </div>
        </div>
    `;
    modal.onclick = (event) => {
        if (event.target === modal) window.closeRetentionFlowModal();
    };
    document.body.appendChild(modal);

    const title = modal.querySelector('.modal-title');
    const body = modal.querySelector('.modal-body');
    const focusName = getProductName(focusId);
    title.textContent = `90일 리텐션 흐름 · ${focusName}`;

    try {
        if (!Array.isArray(AppState.data.anchorTransition) || !AppState.data.anchorTransition.length) {
            AppState.rawData.anchorTransition = await loadOptionalDataFromDB(REQUIRED_FILES.anchorTransition, []);
            if (!Array.isArray(AppState.rawData.anchorScored) || !AppState.rawData.anchorScored.length) {
                AppState.rawData.anchorScored = await loadOptionalDataFromDB(REQUIRED_FILES.anchorScored, []);
            }
            rebuildDerivedData();
        }

        const transitions = AppState.data.anchorTransition || [];
        const related = transitions
            .filter((row) => String(row.aa_product_id || '') === focusId)
            .sort((a, b) => toNumber(b.transition_customer_cnt, 0) - toNumber(a.transition_customer_cnt, 0));

        if (!related.length) {
            body.innerHTML = `
                <p class="empty-state" style="margin:0;">
                    고객의 첫 구매 여부와 무관하게, 이 상품 첫 구매 기준이에요. 90일 내 리텐션 흐름이 아직 없어요.
                </p>
            `;
            applyFriendlyUi(modal);
            return;
        }

        const rows = related.slice(0, 200).map((row) => {
            return `
                <tr>
                    <td>${renderProductCell(getProductName(row.pca_product_id), row.pca_product_id, 42, { groupClickable: true })}</td>
                    <td style="text-align:right">${formatNumber(row.transition_customer_cnt)}</td>
                    <td style="text-align:right">${formatPercent(row.transition_rate, 2)}</td>
                    <td style="text-align:right">${formatNumber(row.avg_days_to_pca, 1)}일</td>
                </tr>
            `;
        }).join('');

        body.innerHTML = `
            <div class="retention-flow-summary">
                고객의 첫 구매 여부와 무관하게, <strong>${escapeHtml(focusName)}</strong>의 이 상품 첫 구매 기준으로 90일 내 리텐션 흐름 ${formatNumber(related.length)}개를 보여줘요.
            </div>
            <div class="chart-hint" style="margin-top:0.25rem;">
                기준 안내: 고객의 첫 구매 여부와 무관하게, 이 상품 첫 구매 기준이에요.
            </div>
            <div class="table-container retention-flow-table-wrap">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>다음 구매 상품</th>
                            <th style="text-align:right">90일 재구매 고객수</th>
                            <th style="text-align:right">90일 재구매율</th>
                            <th style="text-align:right">평균 재구매 소요일</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
            <p class="chart-hint" style="margin-top:0.7rem;">상위 200개 경로만 표시해요.</p>
        `;
        applyFriendlyUi(modal);
    } catch (error) {
        body.innerHTML = `<p style="color:var(--accent); text-align:center; padding:2rem;">90일 리텐션 흐름 로딩에 실패했어요: ${escapeHtml(error.message)}</p>`;
        applyFriendlyUi(modal);
    }
};

window.openCartFlowModal = async (entityId) => {
    const focusId = String(entityId || '').trim();
    if (!focusId) return;

    window.closeCartFlowModal();
    const modal = document.createElement('div');
    modal.id = 'cart-flow-modal';
    modal.className = 'modal-overlay active';
    modal.innerHTML = `
        <div class="modal-card retention-flow-modal-card">
            <div class="modal-header">
                <h3 class="modal-title">함께 구매되는 상품</h3>
                <button class="modal-close" type="button" onclick="closeCartFlowModal()">&times;</button>
            </div>
            <div class="modal-body">
                <div class="modal-loading">
                    <div class="spinner"></div>
                    <p style="margin-top:1rem">함께 구매되는 상품 데이터를 불러오는 중이에요.</p>
                </div>
            </div>
        </div>
    `;
    modal.onclick = (event) => {
        if (event.target === modal) window.closeCartFlowModal();
    };
    document.body.appendChild(modal);

    const title = modal.querySelector('.modal-title');
    const body = modal.querySelector('.modal-body');
    const focusName = getProductName(focusId);
    title.textContent = `함께 구매되는 상품 · ${focusName}`;

    try {
        if (!Array.isArray(AppState.data.cartAnchorDetail) || !AppState.data.cartAnchorDetail.length) {
            AppState.rawData.cartAnchorDetail = await loadOptionalDataFromDB(REQUIRED_FILES.cartAnchorDetail, []);
            if (!Array.isArray(AppState.rawData.anchorScored) || !AppState.rawData.anchorScored.length) {
                AppState.rawData.anchorScored = await loadOptionalDataFromDB(REQUIRED_FILES.anchorScored, []);
            }
            rebuildDerivedData();
        }

        const rowsAll = AppState.data.cartAnchorDetail || [];
        const qId = String(focusId).toLowerCase();
        const related = rowsAll
            .filter((row) => String(row.i || '').toLowerCase() === qId || String(row.j || '').toLowerCase() === qId)
            .sort((a, b) => toNumber(b.co_order_cnt, 0) - toNumber(a.co_order_cnt, 0));

        if (!related.length) {
            body.innerHTML = `
                <p class="empty-state" style="margin:0;">
                    이 상품 기준 함께 구매되는 상품 데이터가 아직 없어요.
                </p>
            `;
            applyFriendlyUi(modal);
            return;
        }

        const rows = related.slice(0, 30).map((row) => {
            const otherId = String(row.i || '').toLowerCase() === qId ? row.j : row.i;
            return `
                <tr>
                    <td>${renderProductCell(getProductName(otherId), otherId, 42, { groupClickable: true })}</td>
                    <td style="text-align:right">${formatNumber(row.co_order_cnt)}</td>
                </tr>
            `;
        }).join('');

        body.innerHTML = `
            <div class="retention-flow-summary">
                <strong>${escapeHtml(focusName)}</strong> 기준 함께 구매되는 상품 상위 ${formatNumber(Math.min(related.length, 30))}개를 보여줘요.
            </div>
            <div class="table-container retention-flow-table-wrap">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>연관 상품</th>
                            <th style="text-align:right">동시구매 수</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
            <p class="chart-hint" style="margin-top:0.7rem;">상위 30개 조합만 표시해요.</p>
        `;
        applyFriendlyUi(modal);
    } catch (error) {
        body.innerHTML = `<p style="color:var(--accent); text-align:center; padding:2rem;">함께 구매되는 상품 로딩에 실패했어요: ${escapeHtml(error.message)}</p>`;
        applyFriendlyUi(modal);
    }
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

window.closeNamePopover = () => {
    const modal = document.getElementById('name-popover-modal');
    if (modal) modal.remove();
};

window.showProductNamePopover = (name, id) => {
    window.closeNamePopover();
    const modal = document.createElement('div');
    modal.id = 'name-popover-modal';
    modal.className = 'modal-overlay active';
    modal.innerHTML = `
        <div class="modal-card name-popover-card">
            <div class="modal-header">
                <h3>상품명을 크게 볼게요</h3>
                <button class="modal-close" type="button" onclick="closeNamePopover()">&times;</button>
            </div>
            <div class="modal-body">
                <p class="name-popover-name">${escapeHtml(name)}</p>
                <p class="name-popover-id">상품 ID: ${escapeHtml(id)}</p>
                <div class="name-popover-actions">
                    <button class="btn-primary" type="button" onclick="copyToClipboard('${escapeJs(id)}')">상품 ID 복사</button>
                    <button class="btn-primary" type="button" onclick="closeNamePopover()">닫기</button>
                </div>
            </div>
        </div>
    `;
    modal.onclick = (event) => {
        if (event.target === modal) window.closeNamePopover();
    };
    document.body.appendChild(modal);
};





function ensureGroupEditorState() {
    if (!AppState.viewState.products.groupEditor) {
        AppState.viewState.products.groupEditor = {};
    }
    const state = AppState.viewState.products.groupEditor;
    if (typeof state.query !== 'string') state.query = '';
    if (!Array.isArray(state.selectedIds)) state.selectedIds = [];
    if (!Array.isArray(state.draftOverrides)) {
        state.draftOverrides = sanitizeProductGroupMapRows(AppState.rawData.productGroupMap || []);
    }
    if (!Number.isInteger(state.wizardStep) || state.wizardStep < 1 || state.wizardStep > 3) state.wizardStep = 1;
    if (!['create', 'move', 'ungroup', 'rename'].includes(state.wizardAction)) state.wizardAction = 'create';
    if (typeof state.focusEntityId !== 'string') state.focusEntityId = '';
    if (!state.wizardPayload || typeof state.wizardPayload !== 'object') state.wizardPayload = {};
    if (typeof state.wizardPayload.groupName !== 'string') state.wizardPayload.groupName = '';
    if (typeof state.wizardPayload.targetGroupId !== 'string') state.wizardPayload.targetGroupId = '';
    if (typeof state.wizardPayload.targetGroupName !== 'string') state.wizardPayload.targetGroupName = '';
    if (typeof state.wizardPayload.renameGroupName !== 'string') state.wizardPayload.renameGroupName = '';
    if (typeof state.wizardPayload.keepSelection !== 'boolean') state.wizardPayload.keepSelection = true;
    if (typeof state.isQueryComposing !== 'boolean') state.isQueryComposing = false;
    if (typeof state.lastActionSummary !== 'string') state.lastActionSummary = '';
    return state;
}

function buildGroupEditorRows(state) {
    const rawRows = AppState.rawData.anchorScored || [];
    const auto = buildAutoGroups(rawRows);
    const previewGrouping = buildGroupingState(rawRows, state.draftOverrides || []);
    const overrideMap = new Map((state.draftOverrides || []).map((row) => [String(row.product_id), row]));
    const rows = [];
    const seen = new Set();

    rawRows.forEach((row) => {
        const productId = readProductId(row);
        if (!productId || seen.has(productId)) return;
        seen.add(productId);

        const rawName = readProductName(row) || productId;
        const normName = normalizeGroupName(rawName);
        const autoGroupId = auto.idToGroupId.get(productId) || '';
        const autoGroupName = autoGroupId ? (auto.groupIdToName.get(autoGroupId) || normName || rawName) : '';
        const entityId = previewGrouping.idToEntityId.get(productId) || productId;
        const entityName = previewGrouping.entityIdToName.get(entityId) || rawName;
        const members = previewGrouping.entityIdToMembers.get(entityId) || [productId];
        const override = overrideMap.get(productId);

        let status = '독립';
        let statusClass = 'status-default';
        if (override?.status === 'ungrouped') {
            status = '수동 해제';
            statusClass = 'status-ungrouped';
        } else if (override?.status === 'grouped') {
            status = '수동 그룹';
            statusClass = 'status-manual';
        } else if (autoGroupId) {
            status = '자동 후보';
            statusClass = 'status-auto';
        }

        rows.push({
            productId,
            rawName,
            normName,
            entityId,
            entityName,
            memberCount: members.length,
            autoGroupId,
            autoGroupName,
            override,
            status,
            statusClass,
            groupId: override?.status === 'grouped' ? override.group_id : (entityId !== productId ? entityId : ''),
            groupName: override?.status === 'grouped' ? override.group_name : (entityId !== productId ? entityName : ''),
            rule: override?.rule || (entityId !== productId ? (previewGrouping.groupIdToRule?.get?.(entityId) || auto.groupIdToRule.get(autoGroupId) || 'auto') : '')
        });
    });

    const query = String(state.query || '').trim().toLowerCase();
    let filtered = !query ? rows : rows.filter((row) => {
        const members = previewGrouping.entityIdToMembers.get(row.entityId) || [row.productId];
        return [
            row.productId,
            row.rawName,
            row.normName,
            row.groupId,
            row.groupName,
            row.entityId,
            members.join('|')
        ].some((value) => String(value || '').toLowerCase().includes(query));
    });

    const focusEntityId = String(state.focusEntityId || '').trim();
    if (focusEntityId) filtered = filtered.filter((row) => row.entityId === focusEntityId);

    filtered.sort((a, b) => {
        const aGrouped = a.entityId !== a.productId ? 0 : 1;
        const bGrouped = b.entityId !== b.productId ? 0 : 1;
        if (aGrouped !== bGrouped) return aGrouped - bGrouped;
        return String(a.entityName).localeCompare(String(b.entityName), 'ko');
    });

    return {
        rows: filtered,
        totalRows: rows,
        previewGrouping,
        auto,
        activeFocusEntityId: focusEntityId
    };
}

function getSelectedGroupEditorIds(state) {
    return Array.from(new Set((state.selectedIds || []).map((id) => String(id || '').trim()).filter(Boolean)));
}

function upsertDraftOverrides(state, newRows) {
    const map = new Map((state.draftOverrides || []).map((row) => [String(row.product_id), row]));
    (newRows || []).forEach((row) => {
        const productId = String(row.product_id || '').trim();
        if (!productId) return;
        map.set(productId, row);
    });
    state.draftOverrides = sanitizeProductGroupMapRows(Array.from(map.values()));
}

function getGroupedEntitiesForEditor(previewGrouping) {
    const entities = [];
    (previewGrouping?.entityIdToMembers || new Map()).forEach((members, entityId) => {
        if (!members || members.length < 2) return;
        entities.push({
            groupId: entityId,
            groupName: previewGrouping.entityIdToName.get(entityId) || entityId,
            memberCount: members.length
        });
    });
    entities.sort((a, b) => b.memberCount - a.memberCount);
    return entities;
}

function resolveGroupRenameTarget(previewGrouping, selectedIds) {
    const groupIds = new Set();
    selectedIds.forEach((productId) => {
        const entityId = previewGrouping.idToEntityId.get(productId) || productId;
        const members = previewGrouping.entityIdToMembers.get(entityId) || [];
        if (members.length > 1) groupIds.add(entityId);
    });
    if (groupIds.size !== 1) {
        return {
            error: '같은 그룹에 속한 상품을 선택해야 그룹명을 바꿀 수 있어요.'
        };
    }
    const groupId = Array.from(groupIds)[0];
    const members = previewGrouping.entityIdToMembers.get(groupId) || [];
    const currentName = previewGrouping.entityIdToName.get(groupId) || groupId;
    return {
        groupId,
        members,
        currentName
    };
}

function buildWizardPlan(state, previewGrouping, groupedEntities) {
    const selectedIds = getSelectedGroupEditorIds(state);
    if (!selectedIds.length) {
        return { valid: false, reason: '먼저 대상 상품을 선택하세요.' };
    }
    const action = state.wizardAction || 'create';
    const payload = state.wizardPayload || {};

    if (action === 'create') {
        const groupName = normalizeGroupName(payload.groupName || '');
        if (!groupName) {
            return { valid: false, reason: '새 그룹명을 입력하세요.' };
        }
        const sortedIds = [...selectedIds].sort();
        const groupId = buildDeterministicGroupId(`${groupName}|${sortedIds.join('|')}`);
        return {
            valid: true,
            action,
            actionLabel: '새 그룹 생성',
            updates: sortedIds.map((productId) => buildManualGroupedRow(productId, groupId, groupName, 'manual')),
            summary: [
                `대상 상품: ${formatNumber(sortedIds.length)}개`,
                `생성 그룹명: ${groupName}`,
                `생성 그룹 ID: ${groupId}`
            ],
            nextFocusEntityId: groupId
        };
    }

    if (action === 'move') {
        const targetGroupId = String(payload.targetGroupId || '').trim();
        const targetGroup = groupedEntities.find((group) => group.groupId === targetGroupId);
        if (!targetGroup) {
            return { valid: false, reason: '이동할 기존 그룹을 선택하세요.' };
        }
        return {
            valid: true,
            action,
            actionLabel: '기존 그룹으로 이동',
            updates: selectedIds.map((productId) => buildManualGroupedRow(productId, targetGroup.groupId, targetGroup.groupName, 'manual')),
            summary: [
                `대상 상품: ${formatNumber(selectedIds.length)}개`,
                `이동 그룹명: ${targetGroup.groupName}`,
                `이동 그룹 ID: ${targetGroup.groupId}`
            ],
            nextFocusEntityId: targetGroup.groupId
        };
    }

    if (action === 'ungroup') {
        return {
            valid: true,
            action,
            actionLabel: '그룹 해제',
            updates: selectedIds.map((productId) => ({
                product_id: productId,
                status: 'ungrouped',
                group_id: '',
                group_name: '',
                rule: 'manual',
                updated_at: nowIso()
            })),
            summary: [
                `대상 상품: ${formatNumber(selectedIds.length)}개`,
                '선택한 상품을 그룹에서 해제해요.'
            ],
            nextFocusEntityId: ''
        };
    }

    const renameTarget = resolveGroupRenameTarget(previewGrouping, selectedIds);
    if (renameTarget.error) {
        return { valid: false, reason: renameTarget.error };
    }
    const nextName = normalizeGroupName(payload.renameGroupName || '');
    if (!nextName) {
        return { valid: false, reason: '변경할 그룹명을 입력하세요.' };
    }
    return {
        valid: true,
        action,
        actionLabel: '그룹명 변경',
        updates: renameTarget.members.map((productId) => buildManualGroupedRow(productId, renameTarget.groupId, nextName, 'manual')),
        summary: [
            `대상 그룹 ID: ${renameTarget.groupId}`,
            `변경 전 그룹명: ${renameTarget.currentName}`,
            `변경 후 그룹명: ${nextName}`,
            `적용 상품 수: ${formatNumber(renameTarget.members.length)}개`
        ],
        nextFocusEntityId: renameTarget.groupId
    };
}

function renderGroupEditorStep1TableRows(rows, selectedIdsSet) {
    return (rows || []).slice(0, 600).map((row) => {
        const checked = selectedIdsSet.has(row.productId) ? 'checked' : '';
        const groupText = row.groupId
            ? `${row.groupName || row.groupId} (${row.groupId})`
            : '-';
        const skuHint = row.memberCount > 1 ? `그룹 ${row.memberCount}개` : '단일';
        return `
            <tr class="${checked ? 'selected' : ''}">
                <td>
                    <input type="checkbox" ${checked} onchange="toggleGroupEditorSelection('${escapeHtml(row.productId)}', this.checked)">
                </td>
                <td>
                    <div class="id">${escapeHtml(row.productId)}</div>
                    <div class="sub">${escapeHtml(skuHint)}</div>
                </td>
                <td>
                    <div class="name" title="${escapeHtml(row.rawName)}">${escapeHtml(row.rawName)}</div>
                    <div class="sub">${escapeHtml(row.normName || '-')}</div>
                </td>
                <td>
                    <span class="group-status ${row.statusClass}">${escapeHtml(row.status)}</span>
                </td>
                <td title="${escapeHtml(groupText)}">${escapeHtml(truncateText(groupText, 46))}</td>
            </tr>
        `;
    }).join('');
}

function refreshGroupEditorStep1Results(state) {
    const modal = document.getElementById('group-editor-modal');
    if (!modal) return;
    if ((state.wizardStep || 1) !== 1) return;

    let context = buildGroupEditorRows(state);
    if (context.activeFocusEntityId && !context.totalRows.some((row) => row.entityId === context.activeFocusEntityId)) {
        state.focusEntityId = '';
        context = buildGroupEditorRows(state);
    }
    const selectedIdsSet = new Set(getSelectedGroupEditorIds(state));
    const allFilteredSelected = context.rows.length > 0 && context.rows.every((row) => selectedIdsSet.has(row.productId));

    const metaEl = modal.querySelector('#group-editor-meta');
    if (metaEl) {
        metaEl.textContent = `표시 ${formatNumber(context.rows.length)} / 전체 ${formatNumber(context.totalRows.length)} · 선택 ${formatNumber(selectedIdsSet.size)}`;
    }

    const tbody = modal.querySelector('#group-editor-table-body');
    if (tbody) {
        const rowsHtml = renderGroupEditorStep1TableRows(context.rows, selectedIdsSet);
        tbody.innerHTML = rowsHtml || '<tr><td colspan="5" style="text-align:center;color:var(--text-muted)">검색 결과가 없습니다.</td></tr>';
    }

    const selectAll = modal.querySelector('#group-editor-select-all');
    if (selectAll) selectAll.checked = allFilteredSelected;
}

function renderGroupWizardStep(state, context) {
    const selectedIds = getSelectedGroupEditorIds(state);
    const selectedIdsSet = context.selectedIdsSet;
    const allFilteredSelected = context.rows.length > 0 && context.rows.every((row) => selectedIdsSet.has(row.productId));
    const step = state.wizardStep;
    const action = state.wizardAction || 'create';
    const payload = state.wizardPayload || {};
    const focusInfo = context.activeFocusEntityId
        ? {
            name: context.previewGrouping.entityIdToName.get(context.activeFocusEntityId) || context.activeFocusEntityId,
            count: (context.previewGrouping.entityIdToMembers.get(context.activeFocusEntityId) || []).length
        }
        : null;

    const stepHeader = `
        <div class="group-wizard-steps">
            <button class="group-step ${step === 1 ? 'active' : ''}" type="button" onclick="setGroupWizardStep(1)">1. 대상 선택</button>
            <button class="group-step ${step === 2 ? 'active' : ''}" type="button" onclick="setGroupWizardStep(2)">2. 작업 선택</button>
            <button class="group-step ${step === 3 ? 'active' : ''}" type="button" onclick="setGroupWizardStep(3)">3. 검토/저장</button>
        </div>
    `;

    if (step === 1) {
        const tableRows = renderGroupEditorStep1TableRows(context.rows, selectedIdsSet);
        return `
            ${stepHeader}
            <div class="pgm-group-toolbar">
                <div class="search-wrapper">
                    <i class="ph ph-magnifying-glass"></i>
                    <input
                        id="group-editor-query-input"
                        type="text"
                        class="search-input"
                        placeholder="상품ID / 상품명 / 그룹명 검색"
                        value="${escapeHtml(state.query || '')}"
                        oncompositionstart="setGroupEditorQueryComposing(true)"
                        oncompositionend="handleGroupEditorQueryCompositionEnd(this)"
                        oninput="updateGroupEditorQuery(this.value)"
                    >
                </div>
                <div class="pgm-group-meta" id="group-editor-meta">표시 ${formatNumber(context.rows.length)} / 전체 ${formatNumber(context.totalRows.length)} · 선택 ${formatNumber(selectedIds.length)}</div>
            </div>
            ${focusInfo ? `
                <div class="group-focus-banner">
                    <span>현재 그룹 보기: ${escapeHtml(focusInfo.name)} · ${formatNumber(focusInfo.count)}개 SKU</span>
                    <button class="btn-primary" type="button" onclick="clearGroupEditorFocus()">전체 보기</button>
                </div>
            ` : ''}
            <label class="group-keep-selection">
                <input
                    type="checkbox"
                    ${payload.keepSelection ? 'checked' : ''}
                    onchange="updateGroupWizardPayload('keepSelection', this.checked ? 'true' : 'false')"
                >
                다음 단계에서도 현재 선택을 유지해요.
            </label>
            <div class="table-container group-editor-table-wrap">
                <table class="data-table group-editor-table">
                    <thead>
                        <tr>
                            <th><input id="group-editor-select-all" type="checkbox" ${allFilteredSelected ? 'checked' : ''} onchange="toggleGroupEditorSelectAll(this.checked)"></th>
                            <th>상품 ID</th>
                            <th>상품명</th>
                            <th>적용 상태</th>
                            <th>현재 그룹</th>
                        </tr>
                    </thead>
                    <tbody id="group-editor-table-body">${tableRows || '<tr><td colspan="5" style="text-align:center;color:var(--text-muted)">검색 결과가 없습니다.</td></tr>'}</tbody>
                </table>
            </div>
            ${context.groupedEntities.length ? `
                <div class="pgm-group-entity-list">
                    <h4>현재 그룹 목록</h4>
                    <div class="group-pills">
                        ${context.groupedEntities.slice(0, 24).map((group) => `
                            <button class="group-pill" type="button" title="${escapeHtml(group.groupId)}" onclick="openGroupEditorWizard({focusEntityId:'${escapeJs(group.groupId)}'})">
                                ${escapeHtml(group.groupName)} · ${formatNumber(group.memberCount)}개
                            </button>
                        `).join('')}
                    </div>
                </div>
            ` : ''}
            <div class="group-wizard-nav">
                <span class="chart-hint">대상 상품을 선택한 뒤 다음 단계에서 작업을 고르세요.</span>
                <button class="btn-primary" type="button" onclick="setGroupWizardStep(2)">다음 단계</button>
            </div>
        `;
    }

    if (step === 2) {
        const planPreview = buildWizardPlan(state, context.previewGrouping, context.groupedEntities);
        const renameTarget = resolveGroupRenameTarget(context.previewGrouping, selectedIds);
        const selectedPreview = selectedIds.slice(0, 6).map((id) => getProductName(id)).join(', ');
        return `
            ${stepHeader}
            <div class="group-selected-preview">
                <strong>선택 상품 ${formatNumber(selectedIds.length)}개</strong>
                <span>${escapeHtml(selectedPreview || '-')}</span>
            </div>
            <div class="group-action-grid">
                ${[
                    ['create', '새 그룹 생성', '선택 상품으로 새 그룹을 만들어요.'],
                    ['move', '기존 그룹으로 이동', '이미 있는 그룹으로 이동해요.'],
                    ['ungroup', '그룹 해제', '선택 상품을 그룹에서 해제해요.'],
                    ['rename', '그룹명 변경', '같은 그룹을 선택한 경우 이름만 바꿔요.']
                ].map(([key, label, desc]) => `
                    <button
                        class="group-action-card ${action === key ? 'active' : ''}"
                        type="button"
                        onclick="setGroupWizardAction('${key}')"
                    >
                        <strong>${label}</strong>
                        <span>${desc}</span>
                    </button>
                `).join('')}
            </div>
            <div class="group-action-form">
                ${action === 'create' ? `
                    <label>
                        새 그룹명
                        <input
                            type="text"
                            value="${escapeHtml(payload.groupName || '')}"
                            placeholder="예: 에센스 라인"
                            oninput="updateGroupWizardPayload('groupName', this.value)"
                        >
                    </label>
                ` : ''}
                ${action === 'move' ? `
                    <label>
                        이동할 기존 그룹
                        <select onchange="updateGroupWizardPayload('targetGroupId', this.value)">
                            <option value="">그룹을 선택하세요</option>
                            ${context.groupedEntities.map((group) => `
                                <option value="${escapeHtml(group.groupId)}" ${payload.targetGroupId === group.groupId ? 'selected' : ''}>
                                    ${escapeHtml(group.groupName)} (${escapeHtml(group.groupId)}) · ${formatNumber(group.memberCount)}개
                                </option>
                            `).join('')}
                        </select>
                    </label>
                ` : ''}
                ${action === 'rename' ? `
                    <div class="group-rename-hint">
                        <span>대상 그룹: ${renameTarget.error ? '-' : escapeHtml(renameTarget.currentName)}</span>
                        <span>대상 그룹 ID: ${renameTarget.error ? '-' : escapeHtml(renameTarget.groupId)}</span>
                    </div>
                    <label>
                        변경할 그룹명
                        <input
                            type="text"
                            value="${escapeHtml(payload.renameGroupName || '')}"
                            placeholder="새 그룹명"
                            oninput="updateGroupWizardPayload('renameGroupName', this.value)"
                        >
                    </label>
                ` : ''}
                ${planPreview.valid ? '' : `<p class="group-plan-warning">${escapeHtml(planPreview.reason || '')}</p>`}
            </div>
            <div class="group-wizard-nav">
                <button class="btn-primary" type="button" onclick="setGroupWizardStep(1)">이전 단계</button>
                <button class="btn-primary" type="button" onclick="setGroupWizardStep(3)">다음 단계</button>
            </div>
        `;
    }

    const plan = buildWizardPlan(state, context.previewGrouping, context.groupedEntities);
    return `
        ${stepHeader}
        <div class="group-review-card ${plan.valid ? '' : 'is-invalid'}">
            <h4>변경안 검토</h4>
            ${plan.valid ? `
                <p><strong>작업:</strong> ${escapeHtml(plan.actionLabel)}</p>
                <ul>
                    ${plan.summary.map((line) => `<li>${escapeHtml(line)}</li>`).join('')}
                </ul>
            ` : `
                <p>${escapeHtml(plan.reason || '검토할 변경안이 없습니다.')}</p>
            `}
        </div>
        ${state.lastActionSummary ? `<p class="chart-hint">${escapeHtml(state.lastActionSummary)}</p>` : ''}
        <div class="group-wizard-nav">
            <button class="btn-primary" type="button" onclick="setGroupWizardStep(2)">이전 단계</button>
            <button class="btn-primary" type="button" onclick="applyGroupWizardDraft()" ${plan.valid ? '' : 'disabled'}>변경안 적용</button>
        </div>
    `;
}

function renderGroupEditorModal() {
    const modal = document.getElementById('group-editor-modal');
    if (!modal) return;
    const state = ensureGroupEditorState();
    let { rows, totalRows, previewGrouping, auto, activeFocusEntityId } = buildGroupEditorRows(state);
    if (activeFocusEntityId && !totalRows.some((row) => row.entityId === activeFocusEntityId)) {
        state.focusEntityId = '';
        ({ rows, totalRows, previewGrouping, auto, activeFocusEntityId } = buildGroupEditorRows(state));
    }
    const selectedIds = new Set(getSelectedGroupEditorIds(state));
    const groupedEntities = getGroupedEntitiesForEditor(previewGrouping);
    const manualCount = (state.draftOverrides || []).filter((row) => row.status === 'grouped').length;
    const ungroupedCount = (state.draftOverrides || []).filter((row) => row.status === 'ungrouped').length;
    const invalidCount = previewGrouping?.stats?.invalidOverrideCount || 0;

    modal.innerHTML = `
        <div class="modal-card pgm-modal pgm-group-modal">
            <div class="modal-header">
                <h3>상품 그룹 조회/편집 마법사</h3>
                <button class="modal-close" type="button" onclick="closeGroupEditorModal()">&times;</button>
            </div>
            <div class="modal-body">
                <div class="pgm-group-summary">
                    <span class="chip">자동 후보 그룹 ${formatNumber(auto?.stats?.groupedEntityCount || previewGrouping?.stats?.groupedEntityCount || 0)}개</span>
                    <span class="chip">수동 그룹 지정 ${formatNumber(manualCount)}건</span>
                    <span class="chip">수동 해제 ${formatNumber(ungroupedCount)}건</span>
                    ${invalidCount > 0 ? `<span class="chip warning">무효 매핑 ${formatNumber(invalidCount)}건</span>` : ''}
                </div>
                <div class="pgm-group-actions">
                    <button class="btn-primary" type="button" onclick="triggerGroupMapImport()">CSV 불러오기</button>
                    <button class="btn-primary" type="button" onclick="exportGroupMapCsv()">CSV 내보내기</button>
                    <input id="group-map-import-input" type="file" accept=".csv" style="display:none" onchange="importGroupMapCsv(this.files)">
                </div>
                <div class="group-wizard-body">
                    ${renderGroupWizardStep(state, {
                        rows,
                        totalRows,
                        previewGrouping,
                        groupedEntities,
                        selectedIdsSet: selectedIds,
                        activeFocusEntityId
                    })}
                </div>
                <p class="chart-hint">그룹 지정은 분석용 논리 통합입니다. 원본 상품ID는 유지되며, 저장 후 Products/Transitions/Cart/Insights 집계에 즉시 반영됩니다.</p>
            </div>
            <div class="pgm-group-footer">
                <button class="btn-primary" type="button" onclick="closeGroupEditorModal()">닫기</button>
                <button class="btn-primary" type="button" onclick="saveGroupEdits()">저장 후 반영</button>
            </div>
        </div>
    `;
    applyFriendlyUi(modal);
}

function setGroupEditorSelection(nextSet) {
    const state = ensureGroupEditorState();
    state.selectedIds = Array.from(nextSet);
}

function buildManualGroupedRow(productId, groupId, groupName, rule = 'manual') {
    return {
        product_id: String(productId || '').trim(),
        status: 'grouped',
        group_id: String(groupId || '').trim(),
        group_name: String(groupName || '').trim(),
        rule,
        updated_at: nowIso()
    };
}

window.showGroupEditorModal = () => {
    window.openGroupEditorWizard({ resetDraft: true });
};

window.openGroupEditorWizard = (params = {}) => {
    const options = params && typeof params === 'object' ? params : {};
    const state = ensureGroupEditorState();
    const applyFocusSelection = () => {
        const focusEntityId = String(state.focusEntityId || '').trim();
        if (!focusEntityId) return;
        const preview = buildGroupingState(AppState.rawData.anchorScored || [], state.draftOverrides || []);
        const members = preview.entityIdToMembers.get(focusEntityId) || [];
        if (members.length) state.selectedIds = [...members];
    };
    if (document.getElementById('group-editor-modal')) {
        if (options.resetDraft) {
            state.draftOverrides = sanitizeProductGroupMapRows(AppState.rawData.productGroupMap || []);
            state.selectedIds = [];
            state.query = '';
            if (typeof options.focusEntityId !== 'string') state.focusEntityId = '';
        }
        if (typeof options.focusEntityId === 'string') {
            state.focusEntityId = options.focusEntityId;
            state.query = '';
        }
        applyFocusSelection();
        state.wizardStep = 1;
        state.lastActionSummary = '';
        renderGroupEditorModal();
        return;
    }
    if (options.resetDraft) {
        state.draftOverrides = sanitizeProductGroupMapRows(AppState.rawData.productGroupMap || []);
        state.selectedIds = [];
        state.query = '';
        if (typeof options.focusEntityId !== 'string') state.focusEntityId = '';
    }
    if (typeof options.focusEntityId === 'string') {
        state.focusEntityId = options.focusEntityId;
        state.query = '';
    }
    applyFocusSelection();
    state.wizardStep = 1;
    state.lastActionSummary = '';
    AppState.viewState.products.quadrant.groupingEditorOpen = true;

    const modal = document.createElement('div');
    modal.id = 'group-editor-modal';
    modal.className = 'modal-overlay active';
    modal.addEventListener('click', (event) => {
        if (event.target === modal) window.closeGroupEditorModal();
    });
    document.body.appendChild(modal);
    renderGroupEditorModal();
};

window.closeGroupEditorModal = () => {
    const modal = document.getElementById('group-editor-modal');
    if (modal) modal.remove();
    AppState.viewState.products.quadrant.groupingEditorOpen = false;
};

window.updateGroupEditorQuery = (query) => {
    const state = ensureGroupEditorState();
    state.query = String(query || '');
    if (state.isQueryComposing) return;
    const modal = document.getElementById('group-editor-modal');
    if (modal && (state.wizardStep || 1) === 1) {
        refreshGroupEditorStep1Results(state);
        return;
    }
    renderGroupEditorModal();
};

window.setGroupEditorQueryComposing = (isComposing) => {
    const state = ensureGroupEditorState();
    state.isQueryComposing = Boolean(isComposing);
};

window.handleGroupEditorQueryCompositionEnd = (inputEl) => {
    const state = ensureGroupEditorState();
    state.isQueryComposing = false;
    const value = String(inputEl?.value || '');
    window.updateGroupEditorQuery(value);
};

window.clearGroupEditorFocus = () => {
    const state = ensureGroupEditorState();
    state.focusEntityId = '';
    renderGroupEditorModal();
};

window.toggleGroupEditorSelection = (productId, checked) => {
    const state = ensureGroupEditorState();
    const next = new Set(getSelectedGroupEditorIds(state));
    const id = String(productId || '').trim();
    if (!id) return;
    if (checked) next.add(id);
    else next.delete(id);
    setGroupEditorSelection(next);
    renderGroupEditorModal();
};

window.toggleGroupEditorSelectAll = (checked) => {
    const state = ensureGroupEditorState();
    const { rows } = buildGroupEditorRows(state);
    const next = new Set(getSelectedGroupEditorIds(state));
    rows.forEach((row) => {
        if (checked) next.add(row.productId);
        else next.delete(row.productId);
    });
    setGroupEditorSelection(next);
    renderGroupEditorModal();
};

window.setGroupWizardAction = (action) => {
    const state = ensureGroupEditorState();
    const next = ['create', 'move', 'ungroup', 'rename'].includes(action) ? action : 'create';
    state.wizardAction = next;
    renderGroupEditorModal();
};

window.updateGroupWizardPayload = (key, value) => {
    const state = ensureGroupEditorState();
    if (!state.wizardPayload || typeof state.wizardPayload !== 'object') state.wizardPayload = {};
    if (key === 'keepSelection') {
        state.wizardPayload.keepSelection = String(value) === 'true';
    } else {
        state.wizardPayload[key] = String(value || '');
    }
    renderGroupEditorModal();
};

window.setGroupWizardStep = (step) => {
    const state = ensureGroupEditorState();
    const nextStep = Math.max(1, Math.min(3, toNumber(step, 1)));
    const selectedIds = getSelectedGroupEditorIds(state);
    const { previewGrouping } = buildGroupEditorRows(state);
    const groupedEntities = getGroupedEntitiesForEditor(previewGrouping);
    if (nextStep >= 2 && !selectedIds.length) {
        alert('먼저 대상 상품을 선택하세요.');
        return;
    }
    if (nextStep === 3) {
        const plan = buildWizardPlan(state, previewGrouping, groupedEntities);
        if (!plan.valid) {
            alert(plan.reason || '작업 조건을 확인하세요.');
            return;
        }
    }
    state.wizardStep = nextStep;
    renderGroupEditorModal();
};

window.applyGroupWizardDraft = () => {
    const state = ensureGroupEditorState();
    const { previewGrouping } = buildGroupEditorRows(state);
    const groupedEntities = getGroupedEntitiesForEditor(previewGrouping);
    const plan = buildWizardPlan(state, previewGrouping, groupedEntities);
    if (!plan.valid) {
        alert(plan.reason || '변경안을 만들 수 없습니다.');
        return;
    }
    upsertDraftOverrides(state, plan.updates);
    if (!state.wizardPayload.keepSelection) {
        state.selectedIds = [];
    }
    state.focusEntityId = plan.nextFocusEntityId || state.focusEntityId;
    state.wizardStep = 1;
    state.lastActionSummary = `${plan.actionLabel} 변경안을 적용했어요. 아래 "저장 후 반영"을 누르면 전체 화면에 반영돼요.`;
    renderGroupEditorModal();
};

function buildGroupMapCsv(rows) {
    const header = ['product_id', 'status', 'group_id', 'group_name', 'rule', 'updated_at'];
    const escape = (value) => {
        const text = String(value ?? '');
        if (!/[",\n]/.test(text)) return text;
        return `"${text.replace(/"/g, '""')}"`;
    };
    const body = (rows || []).map((row) => header.map((field) => escape(row[field])).join(','));
    return [header.join(','), ...body].join('\n');
}

window.exportGroupMapCsv = () => {
    const state = ensureGroupEditorState();
    const rows = sanitizeProductGroupMapRows(state.draftOverrides || []);
    const csv = buildGroupMapCsv(rows);
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'pgm_product_group_map.csv';
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
};

window.triggerGroupMapImport = () => {
    const input = document.getElementById('group-map-import-input');
    if (input) input.click();
};

window.importGroupMapCsv = async (files) => {
    const file = files && files[0];
    if (!file) return;
    const state = ensureGroupEditorState();
    try {
        const parsedRows = await new Promise((resolve, reject) => {
            Papa.parse(file, {
                header: true,
                dynamicTyping: true,
                skipEmptyLines: true,
                complete: (result) => resolve(result.data || []),
                error: reject
            });
        });
        state.draftOverrides = sanitizeProductGroupMapRows(parsedRows);
        state.selectedIds = [];
        renderGroupEditorModal();
    } catch (error) {
        alert(`CSV 불러오기에 실패했습니다: ${error.message}`);
    }
};

window.saveGroupEdits = async () => {
    const state = ensureGroupEditorState();
    const sanitized = sanitizeProductGroupMapRows(state.draftOverrides || []);
    await DB.save(REQUIRED_FILES.productGroupMap.key, sanitized);
    AppState.rawData.productGroupMap = sanitized;
    AppState.data.productGroupMap = sanitized;
    // 그룹 설정 저장 직후 파생 데이터를 다시 만들면서 PGM 점수/집계가 그룹 기준으로 즉시 갱신됩니다.
    rebuildDerivedData();
    AppState.helpers.productNameMap = buildProductNameMap();

    const pageId = document.body.id;
    if (pageId === 'page-products') renderProducts();
    else if (pageId === 'page-transitions') renderTransitions();
    else if (pageId === 'page-insights') renderInsightsPage();
    window.closeGroupEditorModal();
};

// --- Upload Logic ---

function renderSettingsTabs() {
    const activeTab = AppState.viewState.settings.activeTab || 'grouping';
    const groupingStats = AppState.helpers.grouping?.stats || {};
    const groupedEntityCount = groupingStats.groupedEntityCount || 0;
    const invalidOverrideCount = groupingStats.invalidOverrideCount || 0;

    const groupingPanel = `
        <div class="settings-panel ${activeTab === 'grouping' ? 'active' : ''}">
            <h4>상품 그룹 관리</h4>
            <p>같은 상품인데 ID가 다른 경우를 묶어 분석 정확도를 높일 수 있어요.</p>
            <div class="settings-kpis">
                <span>현재 그룹 수: ${formatNumber(groupedEntityCount)}개</span>
                <span>무효 매핑: ${formatNumber(invalidOverrideCount)}건</span>
            </div>
            <div class="settings-actions">
                <button class="btn-primary" type="button" onclick="openGroupEditorFromSettings()">상품 그룹 관리 열기</button>
                <button class="btn-primary" type="button" onclick="exportGroupMapCsv()">그룹 CSV 내보내기</button>
            </div>
        </div>
    `;

    const dataPanel = `
        <div class="settings-panel ${activeTab === 'data' ? 'active' : ''}">
            <h4>데이터 관리</h4>
            <p>CSV를 업로드하고, 필요하면 로컬 저장 데이터를 초기화할 수 있어요.</p>
            <div class="settings-actions">
                <button class="btn-primary" type="button" onclick="showUploadModal()">CSV 업로드</button>
                <button class="btn-primary danger" type="button" onclick="clearIndexedDbFromSettings()">저장 데이터 초기화</button>
            </div>
        </div>
    `;

    return `
        <div class="settings-tabs">
            <button class="settings-tab ${activeTab === 'grouping' ? 'active' : ''}" type="button" onclick="switchSettingsTab('grouping')">상품 그룹</button>
            <button class="settings-tab ${activeTab === 'data' ? 'active' : ''}" type="button" onclick="switchSettingsTab('data')">데이터 관리</button>
        </div>
        ${groupingPanel}
        ${dataPanel}
    `;
}

function showSettingsModal() {
    if (document.getElementById('settingsModal')) document.getElementById('settingsModal').remove();
    document.body.insertAdjacentHTML('beforeend', `
        <div id="settingsModal" class="modal-overlay active">
            <div class="modal-card settings-modal-card">
                <div class="modal-header">
                    <h3>설정</h3>
                    <button class="modal-close" type="button" onclick="closeSettingsModal()">&times;</button>
                </div>
                <div id="settings-modal-body" class="modal-body"></div>
            </div>
        </div>
    `);
    const modal = document.getElementById('settingsModal');
    modal.onclick = (event) => {
        if (event.target === modal) window.closeSettingsModal();
    };
    window.renderSettingsModal();
}

window.renderSettingsModal = () => {
    const body = document.getElementById('settings-modal-body');
    if (!body) return;
    body.innerHTML = renderSettingsTabs();
    applyFriendlyUi(body);
};

window.closeSettingsModal = () => {
    const modal = document.getElementById('settingsModal');
    if (modal) modal.remove();
};

window.switchSettingsTab = (tab) => {
    AppState.viewState.settings.activeTab = tab === 'data' ? 'data' : 'grouping';
    window.renderSettingsModal();
};

window.openGroupEditorFromSettings = () => {
    window.closeSettingsModal();
    window.showGroupEditorModal();
};

window.clearIndexedDbFromSettings = async () => {
    if (!window.confirm('로컬에 저장된 CSV를 모두 지울까요?')) return;
    await DB.clearAll();
    alert('저장 데이터를 지웠어요. 화면을 새로고침할게요.');
    location.reload();
};

function showUploadModal() {
    if (document.getElementById('uploadModal')) document.getElementById('uploadModal').remove();
    document.body.insertAdjacentHTML('beforeend', `
        <div id="uploadModal" style="position:fixed; inset:0; background:rgba(0,0,0,0.8); z-index:9999; display:flex; align-items:center; justify-content:center;">
            <div class="card" style="width:560px; max-width:92%;">
                <div style="display:flex; justify-content:space-between; margin-bottom:1.5rem;"><h3>CSV 업로드</h3><button onclick="document.getElementById('uploadModal').remove()" style="background:none; border:none; color:white; cursor:pointer;"><i class="ph ph-x" style="font-size:1.5rem"></i></button></div>
                <div id="upload-status" style="margin-bottom:1rem; color:var(--text-muted)">여러 CSV를 동시에 선택할 수 있습니다.</div>
                <input type="file" id="file-input" multiple accept=".csv" onchange="handleFiles(this.files)">
                <div id="file-list" style="margin-top:1rem; font-size:0.9rem;"></div>
            </div>
        </div>
    `);
    applyFriendlyUi(document.getElementById('uploadModal'));
}

window.handleFiles = async (files) => {
    const list = document.getElementById('file-list');
    list.innerHTML = '처리 중...';

    let count = 0;
    const matchedNames = [];
    const skippedNames = [];
    const savedByKey = new Map();

    for (const file of files) {
        const config = getUploadFileConfig(file.name);

        if (config) {
            const priority = getUploadPriority(config, file.name);
            const existing = savedByKey.get(config.key);
            if (existing && existing.priority >= priority) {
                skippedNames.push(`${file.name} (중복 키: ${config.key})`);
                continue;
            }

            await new Promise((resolve) => {
                Papa.parse(file, {
                    header: true,
                    dynamicTyping: true,
                    skipEmptyLines: true,
                    complete: async (r) => {
                        const preparedRows = preprocessUploadRows(config, file.name, r.data);
                        await DB.save(config.key, preparedRows);
                        count += 1;
                        const replaced = savedByKey.has(config.key);
                        matchedNames.push(`${file.name} → ${config.key}${replaced ? ' (대체 저장)' : ''}`);
                        savedByKey.set(config.key, { priority, file: file.name });
                        resolve();
                    }
                });
            });
        } else {
            skippedNames.push(file.name);
        }
    }

    if (count > 0) {
        const skippedText = skippedNames.length
            ? `<p style="margin-top:0.4rem; color:var(--text-muted)">미매칭 파일: ${escapeHtml(skippedNames.join(', '))}</p>`
            : '';
        list.innerHTML = `<p style="color:var(--primary)">${count}개 파일 저장 완료. 새로고침합니다.</p><p style="margin-top:0.5rem; color:var(--text-muted)">${escapeHtml(matchedNames.join(' | '))}</p>${skippedText}`;
        setTimeout(() => location.reload(), 1500);
    } else {
        list.innerHTML = '<p style="color:var(--accent)">매칭되는 파일을 찾지 못했습니다. 권장 파일명을 확인하세요.</p>';
    }
};

// --- Initialization ---

function applyFocusFromUrl(pageId) {
    const focusRaw = new URLSearchParams(window.location.search).get('focus');
    if (!focusRaw) return;
    const focusEntity = resolveEntityId(focusRaw);
    if (!focusEntity) return;
    AppState.helpers.focusEntityId = focusEntity;

    if (pageId === 'page-products') {
        AppState.viewState.products.searchQuery = focusEntity;
        AppState.viewState.products.quadrant.selectedId = focusEntity;
    } else if (pageId === 'page-transitions') {
        AppState.viewState.transitions.searchQuery = focusEntity;
        AppState.viewState.transitions.searchMode = 'id';
    }
}

async function loadInsightsData() {
    const [brandScore, anchorScored, anchorTransition, cartAnchor, cartAnchorDetail, aaCohortJourney, aaTransitionPath, caProfile, biiWindow, apfActionRules, productGroupMap] = await Promise.all([
        loadOptionalDataFromDB(REQUIRED_FILES.brandScore, []),
        loadOptionalDataFromDB(REQUIRED_FILES.anchorScored, []),
        loadOptionalDataFromDB(REQUIRED_FILES.anchorTransition, []),
        loadOptionalDataFromDB(REQUIRED_FILES.cartAnchor, []),
        loadOptionalDataFromDB(REQUIRED_FILES.cartAnchorDetail, []),
        loadOptionalDataFromDB(REQUIRED_FILES.aaCohortJourney, []),
        loadOptionalDataFromDB(REQUIRED_FILES.aaTransitionPath, []),
        loadOptionalDataFromDB(REQUIRED_FILES.caProfile, []),
        loadOptionalDataFromDB(REQUIRED_FILES.biiWindow, []),
        loadOptionalDataFromDB(REQUIRED_FILES.apfActionRules, []),
        loadOptionalDataFromDB(REQUIRED_FILES.productGroupMap, [])
    ]);

    AppState.rawData.brandScore = brandScore;
    AppState.rawData.anchorScored = anchorScored;
    AppState.rawData.anchorTransition = anchorTransition;
    AppState.rawData.cartAnchor = cartAnchor;
    AppState.rawData.cartAnchorDetail = cartAnchorDetail;
    AppState.rawData.aaCohortJourney = aaCohortJourney;
    AppState.rawData.aaTransitionPath = aaTransitionPath;
    AppState.rawData.caProfile = caProfile;
    AppState.rawData.biiWindow = biiWindow;
    AppState.rawData.apfActionRules = apfActionRules;
    AppState.rawData.productGroupMap = productGroupMap;
    rebuildDerivedData();
}

async function init() {
    const pageId = document.body.id;

    const sidebar = document.querySelector('.user-profile');
    if (sidebar) {
        sidebar.innerHTML = '<button class="btn-primary settings-launch-btn" style="width:100%" onclick="showSettingsModal()"><i class="ph ph-sliders-horizontal"></i> 설정</button>';
    }

    try {
        const keys = await DB.getAllKeys();
        if (keys.length === 0) {
            document.getElementById('content-area').innerHTML = `
                <div class="card animate-fade-in" style="text-align:center; padding:4rem;">
                    <i class="ph ph-database" style="font-size:4rem; color:var(--text-muted); margin-bottom:1rem;"></i>
                    <h3>데이터 없음</h3>
                    <p style="color:var(--text-muted); margin-bottom:2rem;">데이터를 업로드 해 주세요.</p>
                    <button class="btn-primary" onclick="showUploadModal()">지금 업로드</button>
                </div>
            `;
            return;
        }

        if (pageId === 'page-insights') {
            await loadInsightsData();
            applyFocusFromUrl(pageId);
            renderInsightsPage();
            applyFriendlyUi(document.body);
            return;
        }

        if (pageId === 'page-overview') {
            AppState.rawData.brandScore = await loadDataFromDB(REQUIRED_FILES.brandScore);
            AppState.data.brandScore = AppState.rawData.brandScore;
            renderOverview();
            applyFriendlyUi(document.body);
        } else if (pageId === 'page-products') {
            const [s, t, groupMap] = await Promise.all([
                loadDataFromDB(REQUIRED_FILES.anchorScored),
                loadOptionalDataFromDB(REQUIRED_FILES.anchorTransition, []),
                loadOptionalDataFromDB(REQUIRED_FILES.productGroupMap, [])
            ]);
            AppState.rawData.anchorScored = s;
            AppState.rawData.anchorTransition = t;
            AppState.rawData.productGroupMap = groupMap;
            rebuildDerivedData();
            applyFocusFromUrl(pageId);
            renderProducts();
            applyFriendlyUi(document.body);
        } else if (pageId === 'page-transitions') {
            const [t, s, groupMap] = await Promise.all([
                loadDataFromDB(REQUIRED_FILES.anchorTransition),
                loadDataFromDB(REQUIRED_FILES.anchorScored),
                loadOptionalDataFromDB(REQUIRED_FILES.productGroupMap, [])
            ]);
            AppState.rawData.anchorTransition = t;
            AppState.rawData.anchorScored = s;
            AppState.rawData.productGroupMap = groupMap;
            rebuildDerivedData();
            applyFocusFromUrl(pageId);
            renderTransitions();
            applyFriendlyUi(document.body);
        }
    } catch (e) {
        console.error(e);
        document.getElementById('content-area').innerHTML = `<div class="card" style="text-align:center; padding:2rem;"><h3>필수 데이터 누락</h3><p style="color:var(--accent)">${escapeHtml(e.message)}</p><button class="btn-primary" onclick="showUploadModal()">누락 파일 업로드</button></div>`;
    }
}

document.addEventListener('DOMContentLoaded', init);
