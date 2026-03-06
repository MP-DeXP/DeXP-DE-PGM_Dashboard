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

const AUTOLOAD_DIRECTORIES = ['data', ''];

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
    pagination: {
        cartDetail: {
            currentPage: 1,
            rowsPerPage: 20,
            totalRows: 0
        }
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
                scope: 'transition'
            }
        },
        transitions: { sortCol: 'transition_customer_cnt', sortDesc: true, searchQuery: '', searchMode: 'all' },
        cart: { sortCol: 'co_order_cnt', sortDesc: true, searchQuery: '', searchMode: 'all' },
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
    /\bPGM\b/gi,
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
    [/\bTransition\b/gi, '전환 흐름'],
    [/\bJourney\b/gi, '고객 흐름'],
    [/\bFitness\b/gi, '건강도'],
    [/\bEntry Balance\b/gi, STRUCTURE_LABELS.entry],
    [/\bExpansion Balance\b/gi, STRUCTURE_LABELS.expansion],
    [/\bValue Readiness\b/gi, STRUCTURE_LABELS.valueReadiness],
    [/\bEntry\s*Gravity\b/gi, '첫구매 유입'],
    [/\bExpansion\s*Gravity\b/gi, '재구매'],
    [/\bBasket\s*Gravity\b/gi, '장바구니 확장'],
    [/\bPGM\b/gi, '마케팅']
];

const METRIC_TOOLTIP_RULES = [
    { pattern: /^첫구매 유입 점수$/, description: '이 상품이 신규 고객 첫 구매를 얼마나 잘 만드는지 보여줘요. 높을수록 유입에 강해요.' },
    { pattern: /^재구매 점수$/, description: '첫 구매 뒤 다음 구매로 이어지게 하는 힘이에요. 높을수록 재구매가 좋아요.' },
    { pattern: /^주간 예상 판매량$/, description: '최근 흐름 기준으로 본 주간 판매 예상치예요.' },
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

const QUADRANT_TRANSITION_SCOPE_CRITERIA = '리텐션 재구매 상품은 유입 상위 핵심 상품과 재구매 핵심 상품을 대상으로, 첫 구매 후 90일 안에 실제로 다음 구매가 발생한 경우만 포함해요. 같은 주문에서 함께 산 건은 제외되고, 전환이 0건이면 목록에 나타나지 않아요.';
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
    const groupMeta = options.groupMeta || getEntityMeta(id);
    const isGrouped = Boolean(showGroupLabel && groupMeta && groupMeta.memberCount > 1 && groupMeta.entityId);
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
            <button class="name-trigger" type="button" onclick="event.stopPropagation();showProductNamePopover('${escapeJs(fullName)}','${escapeJs(id)}')">
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
        'first_customer_cnt', 'product_order_cnt_1y', 'product_unit_qty_1y',
        'revenue_90d', 'AA_Broad', 'AA_Heavy', 'AA_Qualified', 'PCA_Core', 'PCA_Deep', 'PCA_Scale'
    ];
    const weightedFields = [
        'AA_Score', 'PCA_Score', 'Entry_Gravity_Score', 'Expansion_Gravity_Score',
        'repurchase_rate_90d', 'first_customer_ratio', 'p50_addl_order_cnt_90d',
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
        return {
            product_id: acc.product_id,
            product_name_latest: acc.product_name_latest,
            first_customer_cnt: toNumber(acc.first_customer_cnt, 0),
            product_order_cnt_1y: toNumber(acc.product_order_cnt_1y, 0),
            product_unit_qty_1y: toNumber(acc.product_unit_qty_1y, 0),
            revenue_90d: toNumber(acc.revenue_90d, 0),
            AA_Score: acc.AA_Score,
            PCA_Score: acc.PCA_Score,
            Entry_Gravity_Score: acc.Entry_Gravity_Score,
            Expansion_Gravity_Score: acc.Expansion_Gravity_Score,
            repurchase_rate_90d: acc.repurchase_rate_90d,
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

const listAutoLoadCandidates = (config) => {
    const names = [config.filename, ...(config.aliases || []), `${config.key}.csv`]
        .map((name) => String(name || '').trim())
        .filter(Boolean);
    return Array.from(new Set(names));
};

const parseCsvText = (text) => {
    return new Promise((resolve, reject) => {
        Papa.parse(text, {
            header: true,
            dynamicTyping: true,
            skipEmptyLines: true,
            complete: (result) => resolve(result.data || []),
            error: reject
        });
    });
};

const fetchCsvRowsForConfig = async (config, directories = AUTOLOAD_DIRECTORIES) => {
    const candidates = listAutoLoadCandidates(config);
    for (const dir of directories) {
        for (const name of candidates) {
            const path = dir ? `${dir}/${name}` : name;
            try {
                const response = await fetch(path, { cache: 'no-store' });
                if (!response.ok) continue;
                const text = await response.text();
                const parsedRows = await parseCsvText(text);
                const rows = preprocessUploadRows(config, name, parsedRows);
                return { rows, source: path };
            } catch (_) {
                // file:// 보안 제한 또는 미존재 파일은 다음 후보로 진행
            }
        }
    }
    return null;
};

const syncDataFromLocalCsv = async (existingKeys = []) => {
    const existingSet = new Set(existingKeys || []);
    const loaded = [];

    // 1) data 폴더는 항상 우선 반영(기존 데이터 덮어쓰기)
    for (const config of Object.values(REQUIRED_FILES)) {
        const found = await fetchCsvRowsForConfig(config, ['data']);
        if (!found) continue;
        const mode = existingSet.has(config.key) ? 'reloaded' : 'loaded';
        await DB.save(config.key, found.rows);
        existingSet.add(config.key);
        loaded.push({
            key: config.key,
            source: found.source,
            rows: found.rows.length,
            mode
        });
    }

    // 2) data에 없는 키는 루트에서 보충
    const missingConfigs = Object.values(REQUIRED_FILES).filter((config) => !existingSet.has(config.key));
    for (const config of missingConfigs) {
        const found = await fetchCsvRowsForConfig(config, ['']);
        if (!found) continue;
        await DB.save(config.key, found.rows);
        existingSet.add(config.key);
        loaded.push({
            key: config.key,
            source: found.source,
            rows: found.rows.length,
            mode: 'loaded'
        });
    }

    return { loadedCount: loaded.length, loaded };
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
    if (!AppState.viewState[viewName]) return;
    AppState.viewState[viewName].searchQuery = query;
    if (viewName === 'cart') AppState.pagination.cartDetail.currentPage = 1;

    if (window.searchTimeout) clearTimeout(window.searchTimeout);
    window.searchTimeout = setTimeout(() => {
        if (viewName === 'products') renderProducts();
        else if (viewName === 'transitions') renderTransitionsTable();
        else if (viewName === 'cart') renderCartDetailTable();
        if (viewName === 'transitions') {
            restoreSearchInputCursor(viewName, selectionStart, selectionEnd);
        }
    }, 150);
};

window.handleSearchModeChange = (viewName, mode) => {
    if (!AppState.viewState[viewName]) return;
    AppState.viewState[viewName].searchMode = normalizeSearchMode(mode);
    if (viewName === 'cart') {
        AppState.pagination.cartDetail.currentPage = 1;
        renderCartDetailTable();
        return;
    }
    if (viewName === 'transitions') {
        renderTransitionsTable();
        restoreSearchInputCursor(viewName);
        return;
    }
    if (viewName === 'products') {
        renderProducts();
    }
};

window.closeRelatedModal = () => {
    const modal = document.getElementById('related-products-modal');
    if (modal) modal.classList.remove('active');
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
                    첫 구매 후 90일 내 이 상품 기준 리텐션 재구매 흐름이 아직 없어요.
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
                <strong>${escapeHtml(focusName)}</strong> 기준으로 첫 구매 후 90일 내 리텐션 재구매 흐름 ${formatNumber(related.length)}개를 보여줘요.
            </div>
            <div class="chart-hint" style="margin-top:0.25rem;">
                첫구매 유입 상품: ${renderProductCell(focusName, focusId, 42, { showId: false, groupClickable: true })}
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
            AppState.rawData.cartAnchorDetail = await loadDataFromDB(REQUIRED_FILES.cartAnchorDetail);
            rebuildDerivedData();
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
                브랜드 구조 건강도는 유입 균형, 재구매 균형, 가치 준비도를 함께 보여줘요. 매출 규모보다 구조 균형이 유지되는지 먼저 보면 좋아요.
            </p>
        </div>
    `;
    applyFriendlyUi(container);
}

function percentile(values, p) {
    const nums = (values || []).filter((v) => Number.isFinite(v)).sort((a, b) => a - b);
    if (!nums.length) return 0;
    if (nums.length === 1) return nums[0];
    const idx = Math.floor((nums.length - 1) * p);
    return nums[idx];
}

function getLevelText(value, p33, p66) {
    const v = toNumber(value, NaN);
    if (!Number.isFinite(v)) return '-';
    if (v >= p66) return '높음';
    if (v >= p33) return '보통';
    return '낮음';
}

function getQuadrantStatus(entry, expansion, centerEntry, centerExpansion) {
    const highEntry = toNumber(entry, 0) >= toNumber(centerEntry, 0);
    const highExpansion = toNumber(expansion, 0) >= toNumber(centerExpansion, 0);
    if (highEntry && highExpansion) {
        return {
            key: 'hero',
            label: '히어로 상품',
            color: '#3b82f6',
            summary: '메인 노출을 유지하고, 예산을 우선 배분해요.',
            guide: '메인 노출을 유지하고, 예산을 우선 배분해요.',
            actions: [
                '히어로 슬롯을 고정해 유입과 재구매 모멘텀을 유지해요.',
                '재고와 배송 가용성을 우선 보호해 성장 기회를 놓치지 않게 해요.'
            ]
        };
    }
    if (!highEntry && !highExpansion) {
        return {
            key: 'phaseout',
            label: '정리 검토 구간',
            color: '#ef4444',
            summary: '재고/마진 기준으로 축소 또는 교체를 먼저 검토해요.',
            guide: '재고/마진 기준으로 축소 또는 교체를 먼저 검토해요.',
            actions: [
                '재고·마진 기준으로 유지 여부를 빠르게 판정해요.',
                '단독 운영보다 대체 상품 테스트 슬롯으로 전환해요.'
            ]
        };
    }
    if (highEntry && !highExpansion) {
        return {
            key: 'entry-only',
            label: '유입 유도',
            color: '#14b8a6',
            summary: '첫 구매 유입은 강하니, 번들로 다음 재구매를 유도해요.',
            guide: '첫 구매 유입은 강하니, 번들로 다음 재구매를 유도해요.',
            actions: [
                '번들/세트 제안을 전면에 배치해 재구매 진입 장벽을 낮춰요.',
                '첫 구매 후 7일 내 리마인드 CRM을 집중 운영해요.'
            ]
        };
    }
    return {
        key: 'expansion-only',
        label: '재구매 앵커',
        color: '#8b5cf6',
        summary: '재구매 전환은 강하니, 신규 유입 채널을 보강해요.',
        guide: '재구매 전환은 강하니, 신규 유입 채널을 보강해요.',
        actions: [
            '신규 유입 채널과 크리에이티브를 확장해 모수부터 키워요.',
            '첫구매 유입 상품과의 동시 노출 구성을 강화해요.'
        ]
    };
}

function getFocusRange(points, selected) {
    const entries = points.map((p) => p.entry);
    const expansions = points.map((p) => p.expansion);
    const exP5 = percentile(entries, 0.05);
    const exP95 = percentile(entries, 0.95);
    const eyP5 = percentile(expansions, 0.05);
    const eyP95 = percentile(expansions, 0.95);
    const xPad = Math.max((exP95 - exP5) * 0.08, 0.01);
    const yPad = Math.max((eyP95 - eyP5) * 0.08, 0.01);
    let xMin = exP5 - xPad;
    let xMax = exP95 + xPad;
    let yMin = eyP5 - yPad;
    let yMax = eyP95 + yPad;
    if (selected) {
        xMin = Math.min(xMin, selected.entry - xPad);
        xMax = Math.max(xMax, selected.entry + xPad);
        yMin = Math.min(yMin, selected.expansion - yPad);
        yMax = Math.max(yMax, selected.expansion + yPad);
    }
    if (xMin === xMax) xMax = xMin + 0.02;
    if (yMin === yMax) yMax = yMin + 0.02;
    return { xMin, xMax, yMin, yMax, xPad, yPad };
}

function projectOutlierPoint(point, range) {
    let marker = '';
    const isLeft = point.entry < range.xMin;
    const isRight = point.entry > range.xMax;
    const isBottom = point.expansion < range.yMin;
    const isTop = point.expansion > range.yMax;
    if (isTop && isRight) marker = '↗';
    else if (isTop && isLeft) marker = '↖';
    else if (isBottom && isRight) marker = '↘';
    else if (isBottom && isLeft) marker = '↙';
    else if (isTop) marker = '↑';
    else if (isBottom) marker = '↓';
    else if (isLeft) marker = '←';
    else if (isRight) marker = '→';
    return {
        x: Math.min(Math.max(point.entry, range.xMin), range.xMax),
        y: Math.min(Math.max(point.expansion, range.yMin), range.yMax),
        marker
    };
}

function buildQuadrantScaleModel(points, selected, scaleMode) {
    const entries = points.map((p) => p.entry);
    const expansions = points.map((p) => p.expansion);
    const rawRange = {
        xMin: Math.min(...entries),
        xMax: Math.max(...entries),
        yMin: Math.min(...expansions),
        yMax: Math.max(...expansions)
    };
    if (rawRange.xMin === rawRange.xMax) rawRange.xMax = rawRange.xMin + 0.02;
    if (rawRange.yMin === rawRange.yMax) rawRange.yMax = rawRange.yMin + 0.02;
    const focusRange = getFocusRange(points, selected);
    const activeRange = scaleMode === 'raw' ? rawRange : focusRange;
    return {
        rawRange,
        focusRange,
        activeRange,
        mode: scaleMode
    };
}

function buildTransitionEntitySet() {
    const set = new Set();
    (AppState.data.anchorTransition || []).forEach((row) => {
        const aa = String(firstDefinedValue(row.aa_product_id, row.entry_product_id, '')).trim();
        const pca = String(firstDefinedValue(row.pca_product_id, row.expansion_product_id, '')).trim();
        if (aa) set.add(aa);
        if (pca) set.add(pca);
    });
    return set;
}

function buildVisibleQuadrantEdges(points, selectedId, edgeMode = 'both') {
    const selected = String(selectedId || '').trim();
    if (!selected || !Array.isArray(points) || !points.length) return [];
    const pointIdSet = new Set(points.map((point) => String(point.id || '').trim()).filter(Boolean));
    if (!pointIdSet.has(selected)) return [];

    const normalizedEdgeMode = String(edgeMode || '').toLowerCase() === 'outbound' ? 'outbound' : 'both';
    const edgeMap = new Map();
    const upsertEdge = (from, to, direction, row) => {
        if (!from || !to || from === to) return;
        if (!pointIdSet.has(from) || !pointIdSet.has(to)) return;
        const customers = Math.max(0, toNumber(row.transition_customer_cnt, 0));
        if (customers <= 0) return;

        const key = `${from}::${to}`;
        if (!edgeMap.has(key)) {
            edgeMap.set(key, {
                from,
                to,
                direction,
                transitionCustomers: 0,
                avgDaysNum: 0,
                avgDaysDen: 0,
                peakRate: 0
            });
        }
        const acc = edgeMap.get(key);
        acc.transitionCustomers += customers;
        const avgDays = toNumber(firstDefinedValue(row.avg_days_to_pca, row.avg_days_to_expansion), NaN);
        if (Number.isFinite(avgDays)) {
            acc.avgDaysNum += avgDays * customers;
            acc.avgDaysDen += customers;
        }
        const rate = Math.max(0, toNumber(row.transition_rate, 0));
        acc.peakRate = Math.max(acc.peakRate, rate);
    };

    (AppState.data.anchorTransition || []).forEach((row) => {
        const from = String(firstDefinedValue(row.aa_product_id, row.entry_product_id, '')).trim();
        const to = String(firstDefinedValue(row.pca_product_id, row.expansion_product_id, '')).trim();
        if (!from || !to || from === to) return;

        if (from === selected) upsertEdge(from, to, 'outbound', row);
        if (normalizedEdgeMode === 'both' && to === selected) upsertEdge(from, to, 'inbound', row);
    });

    return Array.from(edgeMap.values())
        .map((row) => ({
            ...row,
            avgDays: row.avgDaysDen > 0 ? row.avgDaysNum / row.avgDaysDen : null
        }))
        .sort((a, b) => {
            const byCustomers = toNumber(b.transitionCustomers, 0) - toNumber(a.transitionCustomers, 0);
            if (byCustomers !== 0) return byCustomers;
            return toNumber(b.peakRate, 0) - toNumber(a.peakRate, 0);
        })
        .slice(0, QUADRANT_EDGE_TOP_N);
}

function buildQuadrantModel(rows, selectedId, scaleMode = 'focus', scope = 'transition', edgeMode = 'both') {
    const transitionEntitySet = buildTransitionEntitySet();
    const normalizedScope = String(scope || '').toLowerCase() === 'all' ? 'all' : 'transition';
    const normalizedEdgeMode = String(edgeMode || '').toLowerCase() === 'outbound' ? 'outbound' : 'both';
    const allPoints = (rows || [])
        .map((row) => {
            const id = String(row.product_id || '').trim();
            const entry = toNumber(firstDefinedValue(row.AA_Score, row.Entry_Gravity_Score), NaN);
            const expansion = toNumber(firstDefinedValue(row.PCA_Score, row.Expansion_Gravity_Score), NaN);
            if (!id || !Number.isFinite(entry) || !Number.isFinite(expansion)) return null;
            const weeklyForecast = Math.max(0, toNumber(row.product_order_cnt_1y, 0) / 52);
            const entityMeta = getEntityMeta(id);
            const memberCount = Math.max(1, toNumber(row.member_count, 1), toNumber(entityMeta.memberCount, 1));
            return {
                id,
                name: getProductName(id),
                entry,
                expansion,
                weeklyForecast,
                revenue90d: toNumber(row.revenue_90d, 0),
                memberCount,
                groupEntityId: entityMeta.entityId || id,
                hasTransition: transitionEntitySet.has(id)
            };
        })
        .filter(Boolean)
        .sort((a, b) => b.revenue90d - a.revenue90d);

    const points = normalizedScope === 'transition'
        ? allPoints.filter((point) => point.hasTransition)
        : allPoints;

    if (!points.length) return null;

    const entries = points.map((p) => p.entry);
    const expansions = points.map((p) => p.expansion);
    const weekly = points.map((p) => p.weeklyForecast);
    const centerEntry = percentile(entries, 0.5);
    const centerExpansion = percentile(expansions, 0.5);
    const entryP33 = percentile(entries, 0.33);
    const entryP66 = percentile(entries, 0.66);
    const expansionP33 = percentile(expansions, 0.33);
    const expansionP66 = percentile(expansions, 0.66);
    const maxWeekly = Math.max(...weekly, 1);

    let activeId = selectedId && points.some((p) => p.id === selectedId) ? selectedId : '';
    if (!activeId && AppState.helpers.focusEntityId && points.some((p) => p.id === AppState.helpers.focusEntityId)) {
        activeId = AppState.helpers.focusEntityId;
    }
    if (!activeId) activeId = points[0].id;
    const selected = points.find((p) => p.id === activeId) || points[0];
    const status = getQuadrantStatus(selected.entry, selected.expansion, centerEntry, centerExpansion);
    const scale = buildQuadrantScaleModel(points, selected, scaleMode);
    const visibleEdges = buildVisibleQuadrantEdges(points, selected.id, normalizedEdgeMode);

    return {
        points,
        selected,
        visibleEdges,
        status,
        centerEntry,
        centerExpansion,
        entryP33,
        entryP66,
        expansionP33,
        expansionP66,
        maxWeekly,
        scaleMode: scale.mode,
        scope: normalizedScope,
        edgeMode: normalizedEdgeMode,
        scaleRange: scale.activeRange,
        focusRange: scale.focusRange,
        rawRange: scale.rawRange
    };
}

function renderQuadrantPanel(model) {
    if (!model) {
        const scope = String(AppState.viewState.products?.quadrant?.scope || 'transition').toLowerCase();
        if (scope === 'transition') {
            return `<p class="empty-state">${escapeHtml(QUADRANT_TRANSITION_SCOPE_CRITERIA)} 범위를 전체로 전환하면 전체 상품 분포를 볼 수 있어요.</p>`;
        }
        return '<p class="empty-state">4분면 계산 대상 상품이 없습니다.</p>';
    }
    const { selected, status } = model;
    const entryLevel = getLevelText(selected.entry, model.entryP33, model.entryP66);
    const expansionLevel = getLevelText(selected.expansion, model.expansionP33, model.expansionP66);
    const memberMeta = selected.memberCount > 1 ? `그룹 상품 (${selected.memberCount}개 SKU)` : '단일 상품';
    const groupedLabel = selected.memberCount > 1
        ? `
            <button class="group-chip-trigger" type="button" onclick="event.stopPropagation();openGroupEditorWizard({focusEntityId:'${escapeJs(selected.groupEntityId || selected.id)}'})">
                그룹 ${formatNumber(selected.memberCount, 0)}개
            </button>
        `
        : '';
    const hasHistory = (AppState.viewState.products.quadrant.history || []).length > 0;
    const statusLegend = [
        { key: 'hero', label: '히어로 상품', color: '#3b82f6', guide: '메인 노출을 유지하고, 예산을 우선 배분해요.' },
        { key: 'phaseout', label: '정리 검토 구간', color: '#ef4444', guide: '재고/마진 기준으로 축소 또는 교체를 먼저 검토해요.' },
        { key: 'entry-only', label: '유입 유도', color: '#14b8a6', guide: '첫 구매 유입은 강하니, 번들로 다음 재구매를 유도해요.' },
        { key: 'expansion-only', label: '재구매 앵커', color: '#8b5cf6', guide: '재구매 전환은 강하니, 신규 유입 채널을 보강해요.' }
    ];
    const transitionCta = selected.hasTransition
        ? `<button class="btn-primary" type="button" onclick="openRetentionFlowModal('${escapeJs(selected.id)}')">90일 리텐션 흐름 보기</button>`
        : `
            <button class="btn-primary" type="button" disabled title="90일 리텐션 재구매 데이터가 없어 이동할 수 없음">90일 리텐션 흐름 보기</button>
            <p class="pgm-link-help">구매 후 90일 내 리텐션 재구매 데이터가 없어 이동할 수 없어요.</p>
        `;
    const edgeModeLabel = model.edgeMode === 'both' ? '양방향 흐름' : '다음 재구매 흐름';
    const edgeGuide = model.visibleEdges?.length
        ? `이 상품 기준 상위 ${formatNumber(model.visibleEdges.length)}개 ${edgeModeLabel}을 시각화했어요.`
        : '이 상품은 90일 내 재구매 흐름 연결이 없어 선이 표시되지 않아요.';
    return `
        <div class="pgm-side-summary">
            <span class="pgm-badge" style="background:${status.color}1f; color:${status.color}; border-color:${status.color}55;">${status.label}</span>
            <div class="pgm-selected-head">
                <h3 title="${escapeHtml(selected.name)}">${escapeHtml(selected.name)}</h3>
                ${groupedLabel}
            </div>
            <p class="pgm-summary">${escapeHtml(status.summary)}</p>
            <div class="pgm-metrics">
                <div><label>첫구매 유입 점수</label><strong>${formatNumber(selected.entry, 3)}</strong><span>${entryLevel}</span></div>
                <div><label>재구매 점수</label><strong>${formatNumber(selected.expansion, 3)}</strong><span>${expansionLevel}</span></div>
                <div><label>주간 예상 판매량</label><strong>${formatNumber(selected.weeklyForecast, 1)}</strong><span>${memberMeta}</span></div>
            </div>
            <div class="pgm-actions">
                <h4>추천 액션</h4>
                <ul>
                    <li>${escapeHtml(status.actions[0])}</li>
                    <li>${escapeHtml(status.actions[1])}</li>
                </ul>
            </div>
            <div class="pgm-status-guide">
                <h4>상태 해석 가이드</h4>
                <p class="pgm-status-current" style="border-color:${status.color}66; background:${status.color}12;">
                    <strong style="color:${status.color};">${status.label}</strong>
                    <span>${escapeHtml(status.guide || status.summary)}</span>
                </p>
                <div class="pgm-status-legend">
                    ${statusLegend.map((item) => `
                        <div class="pgm-status-item ${item.key === status.key ? 'is-active' : ''}" style="border-color:${item.color}44;">
                            <span class="dot" style="background:${item.color};"></span>
                            <strong>${item.label}</strong>
                            <span>${item.guide}</span>
                        </div>
                    `).join('')}
                </div>
            </div>
            <p class="pgm-edge-guide">${escapeHtml(edgeGuide)}</p>
            <div class="pgm-links">
                ${transitionCta}
                <a class="btn-primary" href="cart.html?focus=${encodeURIComponent(selected.id)}">장바구니 보기</a>
            </div>
            <button class="btn-primary pgm-prev-btn" type="button" onclick="selectPreviousQuadrantItem()" ${hasHistory ? '' : 'disabled'}>이전 상품으로</button>
        </div>
    `;
}

function renderProductQuadrant(model) {
    const qState = AppState.viewState.products.quadrant || {};
    const scaleMode = qState.scaleMode || 'focus';
    const scopeMode = qState.scope === 'all' ? 'all' : 'transition';
    const emptyChartMessage = scopeMode === 'transition'
        ? `${QUADRANT_TRANSITION_SCOPE_CRITERIA} 범위를 전체로 바꿔 확인해 주세요.`
        : '표시할 상품이 없습니다.';
    return `
        <div class="card pgm-quadrant-wrap animate-fade-in">
            <div class="pgm-quadrant-head">
                <div>
                    <h3>상품 상태 4분면</h3>
                    <p>첫구매 유입과 재구매 상태를 한눈에 비교해요.</p>
                </div>
                <div class="quadrant-head-controls">
                    <div class="quadrant-scope-toggle">
                        <button
                            class="btn-primary metric-tooltip-target ${scopeMode === 'transition' ? 'is-active' : ''}"
                            type="button"
                            data-metric-tooltip="실제 리텐션 재구매가 확인된 상품만 보여줘요."
                            aria-label="실제 리텐션 재구매가 확인된 상품만 보여줘요."
                            onclick="setQuadrantScopeMode('transition')"
                        >리텐션 재구매 상품만</button>
                        <button
                            class="btn-primary metric-tooltip-target ${scopeMode === 'all' ? 'is-active' : ''}"
                            type="button"
                            data-metric-tooltip="점수 계산된 전체 상품을 보여주며 리텐션 재구매가 아직 없는 상품도 포함해요."
                            aria-label="점수 계산된 전체 상품을 보여주며 리텐션 재구매가 아직 없는 상품도 포함해요."
                            onclick="setQuadrantScopeMode('all')"
                        >전체 상품 보기</button>
                    </div>
                    <span class="quadrant-control-sep" aria-hidden="true">|</span>
                    <div class="quadrant-scale-toggle">
                        <button class="btn-primary ${scaleMode === 'focus' ? 'is-active' : ''}" type="button" onclick="setQuadrantScaleMode('focus')">집중뷰</button>
                        <button class="btn-primary ${scaleMode === 'raw' ? 'is-active' : ''}" type="button" onclick="setQuadrantScaleMode('raw')">원본 보기</button>
                    </div>
                </div>
            </div>
            <div class="pgm-quadrant-body">
                <div class="pgm-chart card chart-card">
                    ${model
        ? '<canvas id="pgmQuadrantChart"></canvas>'
        : `<div class="quadrant-chart-empty"><p>${emptyChartMessage}</p></div>`}
                </div>
                <div class="pgm-side card">${renderQuadrantPanel(model)}</div>
            </div>
            ${scaleMode === 'focus' ? '<p class="quadrant-outlier-note">집중뷰에서는 일부 점이 경계에 압축돼요. 원본 보기를 누르면 전체 분포를 볼 수 있어요.</p>' : ''}
        </div>
    `;
}

function renderQuadrantChart(model) {
    const canvas = document.getElementById('pgmQuadrantChart');
    if (!canvas || !model) return;
    const ctx = canvas.getContext('2d');
    const centerX = model.centerEntry;
    const centerY = model.centerExpansion;
    const selectedId = String(model.selected?.id || '').trim();

    const range = model.scaleRange;
    const chartPoints = model.points.map((p) => {
        const status = getQuadrantStatus(p.entry, p.expansion, centerX, centerY);
        const radius = 6 + 22 * Math.sqrt((p.weeklyForecast || 0) / (model.maxWeekly || 1));
        const isSelected = selectedId && selectedId === p.id;
        const projected = model.scaleMode === 'focus' ? projectOutlierPoint(p, range) : { x: p.entry, y: p.expansion, marker: '' };
        return {
            x: projected.x,
            y: projected.y,
            r: Math.min(28, Math.max(6, isSelected ? radius * 1.12 : radius)),
            productId: p.id,
            productName: p.name,
            weeklyForecast: p.weeklyForecast,
            rawEntry: p.entry,
            rawExpansion: p.expansion,
            outlierMarker: projected.marker,
            status,
            memberCount: p.memberCount,
            isSelected
        };
    }).sort((a, b) => Number(a.isSelected) - Number(b.isSelected));

    AppState.helpers.productsQuadrantModel = model;
    AppState.charts.pgmQuadrant = new Chart(ctx, {
        type: 'bubble',
        data: {
            datasets: [
                {
                    label: '상품',
                    data: chartPoints,
                    backgroundColor: (ctx2) => {
                        const raw = ctx2.raw || {};
                        const base = raw.status?.color || '#64748b';
                        return raw.isSelected ? base : `${base}cc`;
                    },
                    borderColor: '#ffffff',
                    borderWidth: (ctx2) => ((ctx2.raw && ctx2.raw.isSelected) ? 2.4 : 1.2),
                    hoverBorderWidth: 1.8
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        title: (items) => {
                            const item = items[0]?.raw || {};
                            const groupLabel = item.memberCount > 1 ? ` · 그룹 ${formatNumber(item.memberCount, 0)}개` : '';
                            return `${item.productName}${groupLabel}`;
                        },
                        label: (ctx2) => {
                            const raw = ctx2.raw || {};
                            return [
                                `상태: ${raw.status?.label || '-'}`,
                                `첫구매 유입 점수: ${formatNumber(raw.rawEntry, 3)}`,
                                `재구매 점수: ${formatNumber(raw.rawExpansion, 3)}`,
                                raw.outlierMarker ? `집중뷰 경계 표시: ${raw.outlierMarker}` : '',
                                `주간 예상 판매량: ${formatNumber(raw.weeklyForecast, 1)}`,
                                `SKU 수: ${formatNumber(raw.memberCount, 0)}`
                            ].filter(Boolean);
                        }
                    }
                }
            },
            onClick: (_, elements) => {
                if (!elements.length) return;
                const idx = elements[0].index;
                const targetPoint = chartPoints[idx];
                const target = model.points.find((p) => p.id === targetPoint?.productId);
                if (!target) return;
                window.selectQuadrantItem(target.id);
            },
            scales: {
                x: {
                    min: range.xMin,
                    max: range.xMax,
                    title: { display: true, text: '첫구매 유입' },
                    ticks: { display: false },
                    grid: { display: false, drawBorder: false }
                },
                y: {
                    min: range.yMin,
                    max: range.yMax,
                    title: { display: true, text: '재구매' },
                    ticks: { display: false },
                    grid: { display: false, drawBorder: false }
                }
            }
        },
        plugins: [{
            id: 'quadrant-background',
            beforeDraw: (chart) => {
                const { ctx: chartCtx, chartArea, scales } = chart;
                if (!chartArea) return;
                const xCenter = scales.x.getPixelForValue(centerX);
                const yCenter = scales.y.getPixelForValue(centerY);
                const labels = [
                    { text: '재구매 앵커', x: chartArea.left + 12, y: chartArea.top + 10, align: 'left' },
                    { text: '히어로 상품', x: chartArea.right - 12, y: chartArea.top + 10, align: 'right' },
                    { text: '정리 검토 구간', x: chartArea.left + 12, y: chartArea.bottom - 10, align: 'left' },
                    { text: '유입 유도', x: chartArea.right - 12, y: chartArea.bottom - 10, align: 'right' }
                ];
                chartCtx.save();
                chartCtx.fillStyle = 'rgba(139, 92, 246, 0.2)';
                chartCtx.fillRect(chartArea.left, chartArea.top, Math.max(0, xCenter - chartArea.left), Math.max(0, yCenter - chartArea.top));
                chartCtx.fillStyle = 'rgba(59, 130, 246, 0.2)';
                chartCtx.fillRect(xCenter, chartArea.top, Math.max(0, chartArea.right - xCenter), Math.max(0, yCenter - chartArea.top));
                chartCtx.fillStyle = 'rgba(239, 68, 68, 0.2)';
                chartCtx.fillRect(chartArea.left, yCenter, Math.max(0, xCenter - chartArea.left), Math.max(0, chartArea.bottom - yCenter));
                chartCtx.fillStyle = 'rgba(20, 184, 166, 0.2)';
                chartCtx.fillRect(xCenter, yCenter, Math.max(0, chartArea.right - xCenter), Math.max(0, chartArea.bottom - yCenter));
                chartCtx.font = '700 10px Inter, sans-serif';
                chartCtx.fillStyle = '#334155';
                chartCtx.textBaseline = 'middle';
                labels.forEach((label) => {
                    chartCtx.textAlign = label.align;
                    chartCtx.fillText(label.text, label.x, label.y);
                });
                chartCtx.restore();
            }
        }, {
            id: 'selected-edges',
            beforeDatasetsDraw: (chart) => {
                const edges = model.visibleEdges || [];
                if (!edges.length) return;
                const dataset = chart.data?.datasets?.[0];
                const meta = chart.getDatasetMeta(0);
                if (!dataset || !meta) return;

                const pointById = new Map();
                (dataset.data || []).forEach((point, idx) => {
                    if (!point) return;
                    const element = meta.data[idx];
                    if (!element) return;
                    const props = element.getProps(['x', 'y', 'options'], true);
                    pointById.set(point.productId, {
                        x: props.x,
                        y: props.y,
                        r: toNumber(props.options?.radius, 8)
                    });
                });

                const maxCustomers = Math.max(...edges.map((edge) => toNumber(edge.transitionCustomers, 0)), 1);
                const { ctx: chartCtx } = chart;
                chartCtx.save();
                edges.forEach((edge) => {
                    const from = pointById.get(edge.from);
                    const to = pointById.get(edge.to);
                    if (!from || !to) return;
                    const dx = to.x - from.x;
                    const dy = to.y - from.y;
                    const dist = Math.hypot(dx, dy);
                    if (!Number.isFinite(dist) || dist < 6) return;
                    const ux = dx / dist;
                    const uy = dy / dist;
                    const startX = from.x + ux * (from.r + 3);
                    const startY = from.y + uy * (from.r + 3);
                    const endX = to.x - ux * (to.r + 5);
                    const endY = to.y - uy * (to.r + 5);
                    const bendSign = edge.direction === 'inbound' ? -1 : 1;
                    const bend = Math.min(38, Math.max(12, dist * 0.18)) * bendSign;
                    const cx = (startX + endX) / 2 + (-uy * bend);
                    const cy = (startY + endY) / 2 + (ux * bend);
                    const weight = toNumber(edge.transitionCustomers, 0) / maxCustomers;
                    const isInbound = edge.direction === 'inbound';
                    // 미니멀 모드: 단색 저채도 + 선스타일(실선/점선)로만 방향 구분
                    const color = [100, 116, 139];
                    const alpha = 0.08 + (weight * 0.16);

                    chartCtx.beginPath();
                    chartCtx.moveTo(startX, startY);
                    chartCtx.quadraticCurveTo(cx, cy, endX, endY);
                    chartCtx.strokeStyle = `rgba(${color[0]}, ${color[1]}, ${color[2]}, ${alpha})`;
                    chartCtx.lineWidth = 0.8 + (weight * 1.2);
                    chartCtx.lineCap = 'round';
                    chartCtx.setLineDash(isInbound ? [3, 5] : []);
                    chartCtx.stroke();
                    chartCtx.setLineDash([]);

                    const tx = endX - cx;
                    const ty = endY - cy;
                    const tLen = Math.hypot(tx, ty);
                    if (!Number.isFinite(tLen) || tLen <= 0.0001) return;
                    const tux = tx / tLen;
                    const tuy = ty / tLen;
                    const arrowLen = 5 + (weight * 2);
                    const arrowWidth = 2.6 + (weight * 1.1);
                    const leftX = endX - tux * arrowLen + (-tuy * arrowWidth);
                    const leftY = endY - tuy * arrowLen + (tux * arrowWidth);
                    const rightX = endX - tux * arrowLen - (-tuy * arrowWidth);
                    const rightY = endY - tuy * arrowLen - (tux * arrowWidth);
                    chartCtx.beginPath();
                    chartCtx.moveTo(endX, endY);
                    chartCtx.lineTo(leftX, leftY);
                    chartCtx.lineTo(rightX, rightY);
                    chartCtx.closePath();
                    chartCtx.fillStyle = `rgba(${color[0]}, ${color[1]}, ${color[2]}, ${Math.min(0.3, alpha + 0.08)})`;
                    chartCtx.fill();
                });
                chartCtx.restore();
            }
        }, {
            id: 'selected-pulse',
            afterInit: (chart) => {
                if (typeof requestAnimationFrame !== 'function') return;
                const animatePulse = () => {
                    if (!chart || chart._destroyed || !chart.canvas) return;
                    chart.$selectedPulsePhase = ((chart.$selectedPulsePhase || 0) + 0.02) % 1;
                    chart.draw();
                    chart.$selectedPulseRaf = requestAnimationFrame(animatePulse);
                };
                chart.$selectedPulsePhase = 0;
                chart.$selectedPulseRaf = requestAnimationFrame(animatePulse);
            },
            afterDatasetsDraw: (chart) => {
                const dataset = chart.data?.datasets?.[0];
                const meta = chart.getDatasetMeta(0);
                if (!dataset || !meta) return;
                const selectedIndex = (dataset.data || []).findIndex((point) => point && point.isSelected);
                if (selectedIndex < 0) return;
                const element = meta.data[selectedIndex];
                if (!element) return;
                const point = dataset.data[selectedIndex] || {};
                const props = element.getProps(['x', 'y', 'options'], true);
                const baseRadius = toNumber(props.options?.radius, 8);
                const phase = chart.$selectedPulsePhase || 0;
                const pulseRadius = baseRadius + 4 + (phase * 12);
                const alpha = Math.max(0.08, 0.45 * (1 - phase));
                const color = point.status?.color || '#3b82f6';
                const { ctx: chartCtx } = chart;
                chartCtx.save();
                chartCtx.beginPath();
                chartCtx.arc(props.x, props.y, pulseRadius, 0, Math.PI * 2);
                chartCtx.strokeStyle = color;
                chartCtx.globalAlpha = alpha;
                chartCtx.lineWidth = 2;
                chartCtx.stroke();
                chartCtx.restore();
            },
            afterDestroy: (chart) => {
                if (chart.$selectedPulseRaf && typeof cancelAnimationFrame === 'function') {
                    cancelAnimationFrame(chart.$selectedPulseRaf);
                    chart.$selectedPulseRaf = null;
                }
            }
        }, {
            id: 'center-lines',
            afterDraw: (chart) => {
                const { ctx: chartCtx, chartArea, scales } = chart;
                if (!chartArea) return;
                const xCenter = scales.x.getPixelForValue(centerX);
                const yCenter = scales.y.getPixelForValue(centerY);
                chartCtx.save();
                chartCtx.setLineDash([4, 4]);
                chartCtx.strokeStyle = '#93a7c4';
                chartCtx.lineWidth = 1;
                chartCtx.globalAlpha = 0.6;
                chartCtx.beginPath();
                chartCtx.moveTo(xCenter, chartArea.top);
                chartCtx.lineTo(xCenter, chartArea.bottom);
                chartCtx.stroke();
                chartCtx.beginPath();
                chartCtx.moveTo(chartArea.left, yCenter);
                chartCtx.lineTo(chartArea.right, yCenter);
                chartCtx.stroke();
                if (model.scaleMode === 'focus') {
                    const dataset = chart.data.datasets[0];
                    const meta = chart.getDatasetMeta(0);
                    chartCtx.font = '11px Inter, sans-serif';
                    chartCtx.fillStyle = '#334155';
                    chartCtx.textAlign = 'center';
                    chartCtx.textBaseline = 'middle';
                    dataset.data.forEach((point, idx) => {
                        if (!point.outlierMarker) return;
                        const element = meta.data[idx];
                        if (!element) return;
                        const props = element.getProps(['x', 'y', 'options'], true);
                        const r = toNumber(props.options?.radius, 8);
                        chartCtx.fillText(point.outlierMarker, props.x + r + 7, props.y - r - 3);
                    });
                }
                chartCtx.restore();
            }
        }]
    });
}

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
    else if (pageId === 'page-cart') renderCartAnalysis();
    else if (pageId === 'page-insights') renderInsightsPage();
    window.closeGroupEditorModal();
};

function renderProducts() {
    destroyCarts();
    const container = document.getElementById('content-area');
    const data = AppState.data.anchorScored || [];
    const { sortCol, sortDesc, searchQuery } = AppState.viewState.products;
    const qState = AppState.viewState.products.quadrant;
    if (!['transition', 'all'].includes(qState.scope)) qState.scope = 'transition';
    if (!['focus', 'raw'].includes(qState.scaleMode)) qState.scaleMode = 'focus';
    const focusEntityId = String(AppState.helpers.focusEntityId || '').trim();

    let filteredData = [...data];
    if (searchQuery) {
        const q = searchQuery.toLowerCase();
        filteredData = data.filter((d) =>
            String(d.product_id || '').toLowerCase().includes(q) ||
            String(d.member_ids || '').toLowerCase().includes(q) ||
            (d.product_name_latest && String(d.product_name_latest).toLowerCase().includes(q))
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
        first_customer_cnt: '첫구매 유입 고객수',
        AA_Score: '첫구매 유입 점수',
        AA_Primary_Type: '첫구매 유입 유형',
        PCA_Score: '재구매 점수',
        PCA_Primary_Type: '재구매 유형'
    };
    const sortLabel = sortLabelMap[sortCol] || sortCol;

    const rows = displayData.map((row) => {
        const isFocused = focusEntityId && String(row.product_id) === focusEntityId;
        const meta = getEntityMeta(row.product_id);
        const groupedIdCell = meta.memberCount > 1
            ? `
                <button class="group-chip-trigger" type="button" onclick="event.stopPropagation();openGroupEditorWizard({focusEntityId:'${escapeJs(meta.entityId)}'})">
                    그룹 ${formatNumber(meta.memberCount, 0)}개
                </button>
            `
            : `
                <div style="display:flex; align-items:center; gap:0.5rem;">
                    <span>${escapeHtml(row.product_id)}</span>
                    <button class="btn-icon" style="width:24px; height:24px; font-size:0.8rem; border:none; background:var(--primary-light); color:var(--primary);" 
                            onclick="event.stopPropagation(); copyToClipboard('${escapeHtml(row.product_id)}')">
                        <i class="ph ph-copy"></i>
                    </button>
                </div>
            `;
        return `
        <tr class="clickable ${isFocused ? 'row-focused' : ''}" onclick="showRelatedProducts('${escapeHtml(row.product_id)}')">
            <td>
                ${groupedIdCell}
            </td>
            <td>${renderProductCell(row.product_name_latest || '-', row.product_id, 32)}</td>
            <td>${formatNumber(row.revenue_90d)}</td>
            <td>${formatNumber(row.first_customer_cnt)}</td>
            <td>${formatNumber(row.AA_Score, 4)}</td>
            <td><span class="badge">${escapeHtml(toAaTypeLabel(row.AA_Primary_Type || '-'))}</span></td>
            <td>${formatNumber(row.PCA_Score, 4)}</td>
            <td><span class="badge" style="background: rgba(236, 72, 153, 0.2); color: #f472b6;">${escapeHtml(toPcaTypeLabel(row.PCA_Primary_Type || '-'))}</span></td>
        </tr>
    `;
    }).join('');

    const quadrantModel = buildQuadrantModel(
        sortedData,
        qState.selectedId,
        qState.scaleMode || 'focus',
        qState.scope || 'transition',
        'both'
    );
    if (quadrantModel) {
        qState.selectedId = quadrantModel.selected.id;
    }

    container.innerHTML = `
        ${renderProductQuadrant(quadrantModel)}
        ${renderSearchUI('products', '상품 ID 또는 이름 검색')}
        <div class="controls-area animate-fade-in" style="margin-bottom:2rem;"><div class="card" style="height:400px;"><canvas id="productsChart"></canvas></div></div>
        <div class="card animate-fade-in"><h3>상위 50개 핵심 상품 (정렬 기준: ${escapeHtml(sortLabel)})</h3>
            <div class="table-container">
                <table class="data-table">
                    <thead><tr>
                        <th onclick="handleProductSort('product_id')">ID${getSortIndicator('product_id')}</th>
                        <th onclick="handleProductSort('product_name_latest')">상품명${getSortIndicator('product_name_latest')}</th>
                        <th onclick="handleProductSort('revenue_90d')">90일 매출${getSortIndicator('revenue_90d')}</th>
                        <th onclick="handleProductSort('first_customer_cnt')">첫구매 유입 고객수${getSortIndicator('first_customer_cnt')}</th>
                        <th onclick="handleProductSort('AA_Score')">첫구매 유입 점수${getSortIndicator('AA_Score')}</th>
                        <th onclick="handleProductSort('AA_Primary_Type')">첫구매 유입 유형${getSortIndicator('AA_Primary_Type')}</th>
                        <th onclick="handleProductSort('PCA_Score')">재구매 점수${getSortIndicator('PCA_Score')}</th>
                        <th onclick="handleProductSort('PCA_Primary_Type')">재구매 유형${getSortIndicator('PCA_Primary_Type')}</th>
                    </tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        </div>
    `;
    applyFriendlyUi(container);

    renderQuadrantChart(quadrantModel);

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

    window.selectQuadrantItem = (entityId) => {
        const targetId = String(entityId || '').trim();
        if (!targetId) return;
        const qState = AppState.viewState.products.quadrant;
        if (qState.selectedId && qState.selectedId !== targetId) {
            qState.history.push(qState.selectedId);
            if (qState.history.length > 30) qState.history = qState.history.slice(-30);
        }
        qState.selectedId = targetId;
        AppState.helpers.focusEntityId = targetId;
        renderProducts();
    };

    window.selectPreviousQuadrantItem = () => {
        const qState = AppState.viewState.products.quadrant;
        if (!qState.history.length) return;
        const prev = qState.history.pop();
        if (!prev) return;
        qState.selectedId = prev;
        AppState.helpers.focusEntityId = prev;
        renderProducts();
    };

    window.setQuadrantScaleMode = (mode) => {
        const next = String(mode || '').toLowerCase() === 'raw' ? 'raw' : 'focus';
        AppState.viewState.products.quadrant.scaleMode = next;
        renderProducts();
    };

    window.setQuadrantScopeMode = (mode) => {
        const next = String(mode || '').toLowerCase() === 'all' ? 'all' : 'transition';
        AppState.viewState.products.quadrant.scope = next;
        renderProducts();
    };
}

function renderTransitions() {
    destroyCarts();
    const container = document.getElementById('content-area');
    container.innerHTML = `
        ${renderSearchUI('transitions', '상품명 또는 ID 검색', { includeModeSelect: true })}
        <p class="chart-hint">${RETENTION_90D_FLOW_LABEL}을 기준으로 보여줘요.</p>
        <div id="transitions-table-container"></div>
    `;
    applyFriendlyUi(container);
    renderTransitionsTable();
}

function renderTransitionsTable() {
    const tableContainer = document.getElementById('transitions-table-container');
    if (!tableContainer) return;
    const transitions = AppState.data.anchorTransition || [];
    const { sortCol, sortDesc, searchQuery, searchMode } = AppState.viewState.transitions;
    const focusEntityId = String(AppState.helpers.focusEntityId || '').trim();
    const getName = (id) => getProductName(id);

    let filteredData = [...transitions];
    if (searchQuery) {
        filteredData = transitions.filter((row) => {
            const fromTokens = buildEntitySearchTokens(row.aa_product_id, getName);
            const toTokens = buildEntitySearchTokens(row.pca_product_id, getName);
            return matchesSearchQuery(
                searchQuery,
                searchMode,
                [...fromTokens.ids, ...toTokens.ids],
                [...fromTokens.names, ...toTokens.names]
            );
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
        aa_product_id: '첫구매 유입 상품',
        pca_product_id: '재구매 상품',
        transition_customer_cnt: '90일 재구매 고객수',
        avg_days_to_pca: '평균 재구매 소요일',
        transition_rate: '90일 재구매율'
    };
    const sortLabel = sortLabelMap[sortCol] || sortCol;

    const rows = displayData.map((row) => `
        <tr class="${focusEntityId && (String(row.aa_product_id) === focusEntityId || String(row.pca_product_id) === focusEntityId) ? 'row-focused' : ''}">
            <td>${renderProductCell(getName(row.aa_product_id), row.aa_product_id, 44)}</td>
            <td>${renderProductCell(getName(row.pca_product_id), row.pca_product_id, 44)}</td>
            <td>${formatNumber(row.transition_customer_cnt)}</td>
            <td>${formatNumber(row.avg_days_to_pca, 1)}</td>
            <td>${formatPercent(row.transition_rate, 2)}</td>
        </tr>
    `).join('');
    const emptyMessage = searchQuery
        ? `검색 결과가 없습니다. (검색어: ${escapeHtml(searchQuery)})`
        : '표시할 90일 리텐션 재구매 데이터가 없어요.';
    const bodyRows = rows || `<tr><td colspan="5" style="text-align:center;color:var(--text-muted); padding:1rem;">${emptyMessage}</td></tr>`;

    tableContainer.innerHTML = `
        <div class="card animate-fade-in"><h3>상위 200개 90일 리텐션 재구매 흐름 (정렬 기준: ${escapeHtml(sortLabel)})</h3>
            <div class="table-container">
                <table class="data-table">
                    <thead><tr>
                        <th onclick="handleTransitionSort('aa_product_id')">첫구매 유입 상품${getSortIndicator('aa_product_id')}</th>
                        <th onclick="handleTransitionSort('pca_product_id')">재구매 상품${getSortIndicator('pca_product_id')}</th>
                        <th onclick="handleTransitionSort('transition_customer_cnt')">90일 재구매 고객수${getSortIndicator('transition_customer_cnt')}</th>
                        <th onclick="handleTransitionSort('avg_days_to_pca')">평균 재구매 소요일${getSortIndicator('avg_days_to_pca')}</th>
                        <th onclick="handleTransitionSort('transition_rate')">90일 재구매율${getSortIndicator('transition_rate')}</th>
                    </tr></thead>
                    <tbody>${bodyRows}</tbody>
                </table>
            </div>
        </div>
    `;
    applyFriendlyUi(tableContainer);

    window.handleTransitionSort = (col) => {
        if (AppState.viewState.transitions.sortCol === col) AppState.viewState.transitions.sortDesc = !AppState.viewState.transitions.sortDesc;
        else {
            AppState.viewState.transitions.sortCol = col;
            AppState.viewState.transitions.sortDesc = true;
        }
        renderTransitionsTable();
    };
}

function renderCartAnalysis() {
    destroyCarts();
    const container = document.getElementById('content-area');

    AppState.helpers.productNameMap = buildProductNameMap();
    const getName = (id) => getProductName(id);
    AppState.helpers.getName = getName;

    container.innerHTML = `
        ${renderSearchUI('cart', '상품명 또는 ID 검색', { includeModeSelect: true })}
        <div class="card animate-fade-in" style="margin-top:2rem;">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:1rem;">
                <h3>장바구니 동시구매 상세</h3><div id="pagination-info" style="color:var(--text-muted); font-size:0.9rem;"></div>
            </div>
            <div id="cart-detail-container" class="table-container"></div>
            <div class="pagination-controls"><button id="prevBtn" class="btn-primary" disabled>이전</button><button id="nextBtn" class="btn-primary" disabled>다음</button></div>
        </div>
    `;
    applyFriendlyUi(container);

    if (AppState.data.cartAnchorDetail && AppState.data.cartAnchorDetail.length > 0) renderCartDetailTable();
    else loadDetailData();
}

async function loadDetailData() {
    try {
        AppState.rawData.cartAnchorDetail = await loadDataFromDB(REQUIRED_FILES.cartAnchorDetail);
        rebuildDerivedData();
        const deduplicated = AppState.data.cartAnchorDetail || [];
        AppState.pagination.cartDetail.totalRows = deduplicated.length;
        renderCartDetailTable();
    } catch (_) {
        document.getElementById('cart-detail-container').innerHTML = '<p style="color:var(--text-muted); text-align:center; padding:2rem;">상세 데이터가 없습니다. <button class="btn-primary" style="font-size:0.8rem" onclick="showUploadModal()">CSV 업로드</button></p>';
    }
}

function renderCartDetailTable() {
    const { currentPage, rowsPerPage } = AppState.pagination.cartDetail;
    const { sortCol, sortDesc, searchQuery, searchMode } = AppState.viewState.cart;
    const getName = AppState.helpers.getName || ((id) => getProductName(id));
    const focusEntityId = String(AppState.helpers.focusEntityId || '').trim();

    let data = [...(AppState.data.cartAnchorDetail || [])];
    if (searchQuery) {
        data = data.filter((d) => {
            const iTokens = buildEntitySearchTokens(d.i, getName);
            const jTokens = buildEntitySearchTokens(d.j, getName);
            return matchesSearchQuery(
                searchQuery,
                searchMode,
                [...iTokens.ids, ...jTokens.ids],
                [...iTokens.names, ...jTokens.names]
            );
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
        <tr class="${focusEntityId && (String(row.i) === focusEntityId || String(row.j) === focusEntityId) ? 'row-focused' : ''}">
            <td>${renderProductCell(getName(row.i), row.i, 44)}</td>
            <td>${renderProductCell(getName(row.j), row.j, 44)}</td>
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
    applyFriendlyUi(document.getElementById('cart-detail-container'));

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

    const normalizedFilterAaType = normalizeCategoryValue(filters.aaType, 'ALL');
    const isAllAaType = normalizedFilterAaType.toUpperCase() === 'ALL';

    const aaRowsAll = applyDateFilter(AppState.data.aaCohortJourney || [], 'cohort_date', filters.dateFrom, filters.dateTo)
        .filter((row) => {
            if (isAllAaType) return true;
            return normalizeCategoryValue(row.aa_type, '').toLowerCase() === normalizedFilterAaType.toLowerCase();
        })
        .filter((row) => filters.aaProductId === 'ALL' || String(row.aa_product_id) === String(filters.aaProductId));

    const transitionRowsAll = applyDateFilter(AppState.data.aaTransitionPath || [], 'cohort_date', filters.dateFrom, filters.dateTo)
        .filter((row) => {
            if (isAllAaType) return true;
            return normalizeCategoryValue(row.aa_type, '').toLowerCase() === normalizedFilterAaType.toLowerCase();
        })
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
        const key = normalizeCategoryValue(row.aa_type, '미분류');
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
            monotonicBreakCount,
            bii365: bii365 ? toNumber(bii365.bii, null) : null,
            bii90: bii90 ? toNumber(bii90.bii, null) : null,
            selectedWindowBii: selectedWindowBii ? toNumber(selectedWindowBii.bii, null) : null,
            bhi: bhiRow ? toNumber(firstDefinedValue(
                bhiRow.Brand_Health_Index,
                bhiRow.BHI,
                bhiRow.brand_health_index,
                bhiRow.Brand_Health_Score
            ), null) : null,
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
    if ((model.summaries.pca90 || 0) < 0.2 && model.summaries.cohortCustomers > 0) {
        warnings.push('90일 재구매 도달률이 낮아 첫구매 유입 이후 이탈 위험이 있습니다');
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
            title: '대량 유입형 상품의 재구매 강화',
            action: '첫구매 유입 후 7일 이내 단골의 시작점 상품으로 이어지도록 CRM/리타게팅을 우선 배치합니다.',
            impact: '재구매 도달률 개선 및 유입 낭비 축소',
            evidence: `${TERM_LABELS.AA}-${AA_TYPE_LABELS.BROAD} 비중 ${formatPercent(m.aa_broad_ratio, 1)} / 90일 재구매 도달률 ${formatPercent(m.pca_transition_90d_rate, 1)}`
        });
    }

    if (m.transition_top3_share > 0.65) {
        cards.push({
            domain: 'marketing',
            priority: 2,
            title: '전이 경로 과집중 완화 실험',
            action: '상위 재구매 상품 편중 경로를 유지하되 대체 상품 노출 A/B 테스트를 병행합니다.',
            impact: '경로 리스크 분산 및 안정적 확장',
            evidence: `전이 상위 3경로 비중 ${formatPercent(m.transition_top3_share, 1)}`
        });
    }

    if (m.avg_days_to_pca > 18) {
        cards.push({
            domain: 'marketing',
            priority: 1,
            title: '재구매 도달 속도 개선',
            action: '첫구매 유입 후 메시지 발화 시점을 앞당기고, 3~7일 구간 혜택을 강화합니다.',
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
            impact: '장바구니 확장 확장률 향상',
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

function getFitnessTrend(ratio) {
    if (ratio === null || ratio === undefined || Number.isNaN(ratio)) {
        return {
            direction: '판단 보류',
            status: '데이터 부족',
            problem: '90일과 365일 건강도 비교 데이터가 부족합니다.',
            action: '기간 데이터 업로드 상태를 먼저 점검하세요.',
            tone: 'neutral'
        };
    }
    if (ratio >= 1.15) {
        return {
            direction: '개선',
            status: '빠르게 개선 중',
            problem: '최근 건강도가 장기 기준보다 빠르게 좋아지고 있습니다.',
            action: '효율이 높은 유입과 재구매 흐름에 예산과 노출을 확대하세요.',
            tone: 'positive'
        };
    }
    if (ratio >= 0.95) {
        return {
            direction: '유지',
            status: '안정 유지',
            problem: '최근 건강도가 장기 기준과 유사한 안정 구간입니다.',
            action: '현재 운영안을 유지하되 이탈 구간만 미세 조정하세요.',
            tone: 'stable'
        };
    }
    if (ratio >= 0.85) {
        return {
            direction: '하락',
            status: '약화 신호',
            problem: '최근 건강도가 장기 기준 대비 약해지는 신호입니다.',
            action: '첫구매 유입 후 7일 이내 CRM 접점을 앞당겨 재구매 전환을 보강하세요.',
            tone: 'warning'
        };
    }
    return {
        direction: '위험',
        status: '즉시 대응 필요',
        problem: '최근 건강도가 장기 기준 대비 크게 약화된 상태입니다.',
        action: '재구매 상품 노출과 핵심 재고 방어를 최우선으로 전환하세요.',
        tone: 'critical'
    };
}

function renderHeroStory(model) {
    const selectedWindow = toNumber(model.filters.windowDays, 90);
    const selectedWindowRow = model.biiMap.get(selectedWindow);
    const ratio = (model.summaries.bii365 && model.summaries.bii90)
        ? model.summaries.bii90 / model.summaries.bii365
        : null;
    const selectedStage = selectedWindowRow ? toStageLabel(selectedWindowRow.stage) : '-';
    const confidence = (selectedWindowRow && withFallback(selectedWindowRow.confidence, null))
        || model.summaries.confidence
        || '-';
    const trend = getFitnessTrend(ratio);

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
                    <label>90일 건강도 대비 연간 건강도</label>
                    <strong>${ratio !== null ? formatNumber(ratio, 2) : '-'}</strong>
                    <span>${trend.status}</span>
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
            <p class="insight-note">메인 지표는 ${TERM_LABELS.BII} 중심으로 보여줍니다. ${TERM_LABELS.BHI}는 하단 참고값에서만 확인하세요.</p>
        </section>
    `;
}

function renderAAJourney(model) {
    if (!model.aaRowsAll.length) {
        return renderMissingSection('첫구매 유입 고객 흐름', `${REQUIRED_FILES.aaCohortJourney.filename} 데이터가 없어 첫구매 유입 고객 흐름을 표시할 수 없습니다.`);
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
                <h2>첫구매 유입 고객 흐름</h2>
                <p>첫구매 유입 이후 7/30/90일 행동과 전환 속도</p>
            </div>
            <div class="journey-grid">
                <div class="journey-kpi">
                    <label>첫구매 유입 고객수</label>
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
                    <label>90일 재구매 도달률</label>
                    <strong>${formatPercent(s.pca90, 1)}</strong>
                </div>
                <div class="journey-kpi">
                    <label>재구매까지 평균 일수</label>
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
                            <th>Entry 유형</th>
                            <th>대상 고객수</th>
                            <th>90일 재구매율</th>
                            <th>90일 재구매 도달률</th>
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
        return renderMissingSection('90일 리텐션 재구매 흐름', `${REQUIRED_FILES.aaTransitionPath.filename} 데이터가 없어 첫 구매 후 90일 리텐션 흐름을 표시할 수 없습니다.`);
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
                <h2>90일 리텐션 재구매 흐름</h2>
                <p>첫 구매 후 90일 안에 다음 구매로 이어진 경로와 속도를 보여줘요.</p>
            </div>
            <div class="journey-grid">
                <div class="journey-kpi">
                    <label>상위 3개 전이 집중도</label>
                    <strong>${formatPercent(model.summaries.top3TransitionShare, 1)}</strong>
                </div>
                <div class="journey-kpi">
                    <label>평균 90일 재구매율</label>
                    <strong>${formatPercent(model.summaries.pca90, 1)}</strong>
                </div>
            </div>
            <p class="chart-hint">차트 라벨은 상품명 기준이며, 마우스를 올리면 전체 상품명과 ID를 확인할 수 있습니다.</p>
            <div class="card chart-card"><canvas id="transitionChart"></canvas></div>
            <div class="table-container" style="margin-top:1rem;">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>첫구매 유입 상품</th>
                            <th>재구매 상품</th>
                            <th>90일 재구매 고객수</th>
                            <th>90일 재구매율</th>
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
        return renderMissingSection('장바구니 확장 인사이트', `${REQUIRED_FILES.caProfile.filename} 데이터가 없어 장바구니 확장 흐름을 표시할 수 없습니다.`);
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
            <h4>선택한 첫구매 유입 상품 기준 장바구니 확장</h4>
            <p><strong title="${escapeHtml(getProductName(model.selectedCa.product_id))}">${escapeHtml(truncateText(getProductName(model.selectedCa.product_id), 42))}</strong> (${escapeHtml(model.selectedCa.product_id)})</p>
            <div class="selected-ca-grid">
                <span>확장 유형: ${escapeHtml(toCaTypeLabel(withFallback(model.selectedCa.ca_type, 'None')))}</span>
                <span>동반구매 비율: ${formatPercent(model.selectedCa.attach_rate, 1)}</span>
                <span>중간 장바구니 크기: ${formatNumber(model.selectedCa.median_cart_size, 2)}</span>
                <span>상위 1개 집중도: ${formatPercent(model.selectedCa.top1_share, 1)}</span>
            </div>
        </div>
        `
        : '<div class="selected-ca-panel"><h4>선택한 첫구매 유입 상품 기준 장바구니 확장</h4><p>첫구매 유입 상품 필터를 선택하면 해당 상품의 장바구니 확장 신호를 표시합니다.</p></div>';

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
        return renderMissingSection('브랜드 건강도', `${REQUIRED_FILES.biiWindow.filename} 데이터가 없어 브랜드 건강도 지표를 표시할 수 없습니다.`);
    }

    const selectedWindow = toNumber(model.filters.windowDays, 90);
    const selectedRow = model.biiMap.get(selectedWindow);
    const row90 = model.biiMap.get(90);
    const row365 = model.biiMap.get(365);
    const selectedClvNorm = selectedRow ? toNumber(selectedRow.clv_norm, null) : null;
    const selectedCustomerStrengthNorm = selectedRow ? toNumber(selectedRow.customer_strength_norm, null) : null;
    const selectedWindowBii = selectedRow ? toNumber(selectedRow.bii, null) : model.summaries.selectedWindowBii;
    const bii90Value = row90 ? toNumber(row90.bii, null) : model.summaries.bii90;
    const bii365Value = row365 ? toNumber(row365.bii, null) : model.summaries.bii365;
    const selectedStage = selectedRow ? toStageLabel(selectedRow.stage) : '-';
    const confidence = (selectedRow && withFallback(selectedRow.confidence, null))
        || (row90 && withFallback(row90.confidence, null))
        || (brand && withFallback(brand.Confidence_Index, null))
        || '-';
    const ratio = (bii90Value !== null && bii365Value !== null && bii365Value !== 0)
        ? bii90Value / bii365Value
        : null;

    const trend = getFitnessTrend(ratio);

    const bhiValue = brand ? toNumber(firstDefinedValue(
        brand.Brand_Health_Index,
        brand.BHI,
        brand.brand_health_index,
        brand.Brand_Health_Score
    ), null) : null;
    const aaBroadRatio = brand ? toNumber(brand.AA_Broad_Ratio, null) : null;
    const aaQualifiedRatio = brand ? toNumber(brand.AA_Qualified_Ratio, null) : null;
    const aaHeavyRatio = brand ? toNumber(brand.AA_Heavy_Ratio, null) : null;
    const as = brand ? toNumber(brand.AA_Concentration_Index, null) : null;
    const cs = brand ? toNumber(brand.Chain_Balance_Index, null) : null;
    const vs = brand ? toNumber(
        brand.Value_Readiness || brand.Value_Score || brand.Variety_Score || brand.Value_Ready_Index,
        null
    ) : null;
    const asPct = as !== null ? Math.max(0, Math.min(100, as * 100)) : null;
    const csPct = cs !== null ? Math.max(0, Math.min(100, cs * 100)) : null;
    const vsPct = vs !== null ? Math.max(0, Math.min(100, vs * 100)) : null;
    const broadPct = aaBroadRatio !== null ? Math.max(0, Math.min(100, aaBroadRatio * 100)) : null;
    const qualifiedPct = aaQualifiedRatio !== null ? Math.max(0, Math.min(100, aaQualifiedRatio * 100)) : null;
    const heavyPct = aaHeavyRatio !== null ? Math.max(0, Math.min(100, aaHeavyRatio * 100)) : null;
    const qualityMixPct = (qualifiedPct !== null || heavyPct !== null)
        ? toNumber(qualifiedPct, 0) + toNumber(heavyPct, 0)
        : null;
    const hasStructureData = [asPct, csPct, vsPct].some((v) => v !== null);
    const getDriverStatus = (value, good, neutral) => {
        if (value === null) return { label: '데이터 없음', tone: 'neutral' };
        if (good(value)) return { label: '긍정 영향', tone: 'positive' };
        if (neutral(value)) return { label: '중립', tone: 'neutral' };
        return { label: '주의 영향', tone: 'warning' };
    };
    const qualityMixStatus = getDriverStatus(
        qualityMixPct,
        (v) => v >= 35,
        (v) => v >= 20
    );
    const broadStatus = getDriverStatus(
        broadPct,
        (v) => v <= 45,
        (v) => v <= 60
    );
    const concentrationStatus = getDriverStatus(
        asPct,
        (v) => v <= 55,
        (v) => v <= 70
    );
    const chainStatus = getDriverStatus(
        csPct,
        (v) => v >= 70,
        (v) => v >= 55
    );
    const componentBhi = selectedRow
        ? toNumber(selectedRow.bhi, bhiValue)
        : bhiValue;
    const calculatedBii = (componentBhi !== null && selectedClvNorm !== null && selectedCustomerStrengthNorm !== null)
        ? componentBhi * selectedClvNorm * selectedCustomerStrengthNorm
        : null;
    const componentGap = (selectedWindowBii !== null && calculatedBii !== null)
        ? selectedWindowBii - calculatedBii
        : null;
    const bhiReferenceText = brand
        ? `참고 구조값: ${TERM_LABELS.BHI} ${bhiValue !== null ? formatNumber(bhiValue * 100, 2) : '-'} | ${STRUCTURE_LABELS.entry} ${asPct !== null ? formatNumber(asPct, 1) : '-'}% | ${STRUCTURE_LABELS.expansion} ${csPct !== null ? formatNumber(csPct, 1) : '-'}% | ${STRUCTURE_LABELS.valueReadiness} ${vsPct !== null ? formatNumber(vsPct, 1) : '-'}%`
        : '참고 구조값: brand_score.csv 미업로드';

    const rows = biiRows.map((row) => `
        <tr>
            <td>${formatNumber(row.window_days, 0)}일</td>
            <td>${formatNumber(row.bii, 3)}</td>
            <td>${formatNumber(row.clv_norm, 3)}</td>
            <td>${formatNumber(row.customer_strength_norm, 3)}</td>
            <td>${escapeHtml(toStageLabel(row.stage))}</td>
        </tr>
    `).join('');

    // 고객 노출 정책상 임시 비활성화: fitness-summary-grid
    const fitnessSummaryBlock = '';
    /*
    const fitnessSummaryBlock = `
        <div class="fitness-summary-grid">
            <div class="fitness-summary-card tone-${trend.tone}">
                <label>건강도 방향</label>
                <strong>${escapeHtml(trend.direction)}</strong>
                <span>${escapeHtml(trend.status)}</span>
            </div>
            <div class="fitness-summary-card">
                <label>최근 기준 건강도</label>
                <strong>${selectedWindowBii !== null ? formatNumber(selectedWindowBii, 3) : '-'}</strong>
                <span>${selectedWindow}일 기준 · 현재 단계 ${escapeHtml(String(selectedStage))}</span>
            </div>
            <div class="fitness-summary-card">
                <label>90일 대비 연간 흐름</label>
                <strong>${ratio !== null ? formatNumber(ratio, 2) : '-'}</strong>
                <span>${escapeHtml(trend.status)}</span>
            </div>
            <div class="fitness-summary-card">
                <label>신뢰도</label>
                <strong>${escapeHtml(String(confidence))}</strong>
                <span>현재 기준 데이터 신뢰도</span>
            </div>
        </div>
    `;
    */

    // 고객 노출 정책상 임시 비활성화: fitness-explain tone-critical
    const fitnessExplainBlock = trend.tone === 'critical'
        ? ''
        : `
            <div class="fitness-explain tone-${trend.tone}">
                <p><strong>해석:</strong> ${escapeHtml(trend.problem)}</p>
                <p><strong>바로 실행:</strong> ${escapeHtml(trend.action)}</p>
            </div>
        `;

    return `
        <section id="brand-fitness" class="insight-section card animate-fade-in">
            <div class="section-headline">
                <h2>브랜드 건강도</h2>
                <p>최근 건강도가 장기 흐름 대비 개선 중인지 먼저 확인하고, 필요하면 원인 상세를 펼쳐서 봅니다</p>
            </div>
            ${fitnessSummaryBlock}
            ${fitnessExplainBlock}
            <div class="structure-block">
                <h3>브랜드 구조 건강도의 3개 구조</h3>
                ${hasStructureData ? `
                    <div class="card chart-card structure-radar-card">
                        <canvas
                            id="brandStructureRadarChart"
                            data-as="${asPct !== null ? asPct.toFixed(4) : ''}"
                            data-cs="${csPct !== null ? csPct.toFixed(4) : ''}"
                            data-vs="${vsPct !== null ? vsPct.toFixed(4) : ''}"
                        ></canvas>
                    </div>
                ` : '<p class="chart-hint">brand_score.csv의 구조 지표가 없어 레이더 차트를 표시할 수 없습니다.</p>'}
                <div class="structure-grid">
                    <div class="structure-item">
                        <label>${STRUCTURE_LABELS.entry}</label>
                        <strong>${asPct !== null ? formatNumber(asPct, 1) : '-'}${asPct !== null ? '%' : ''}</strong>
                        <span>신규 유입이 한쪽에 쏠리지 않는지</span>
                    </div>
                    <div class="structure-item">
                        <label>${STRUCTURE_LABELS.expansion}</label>
                        <strong>${csPct !== null ? formatNumber(csPct, 1) : '-'}${csPct !== null ? '%' : ''}</strong>
                        <span>재구매가 특정 경로에 과집중되지 않는지</span>
                    </div>
                    <div class="structure-item">
                        <label>${STRUCTURE_LABELS.valueReadiness}</label>
                        <strong>${vsPct !== null ? formatNumber(vsPct, 1) : '-'}${vsPct !== null ? '%' : ''}</strong>
                        <span>매출 확장 여력이 확보되어 있는지</span>
                    </div>
                </div>
                <div class="value-driver-block">
                    <h4>${STRUCTURE_LABELS.valueReadiness} 영향 요소</h4>
                    <div class="value-driver-grid">
                        <div class="value-driver-item ${qualityMixStatus.tone}">
                            <label>효율·고가치 유입 비중</label>
                            <strong>${qualityMixPct !== null ? formatNumber(qualityMixPct, 1) : '-'}${qualityMixPct !== null ? '%' : ''}</strong>
                        <span>${qualityMixStatus.label} · 고가치 유입 비중</span>
                        </div>
                        <div class="value-driver-item ${broadStatus.tone}">
                            <label>확장형 유입 비중</label>
                            <strong>${broadPct !== null ? formatNumber(broadPct, 1) : '-'}${broadPct !== null ? '%' : ''}</strong>
                        <span>${broadStatus.label} · 확장형 유입 비중</span>
                        </div>
                        <div class="value-driver-item ${concentrationStatus.tone}">
                            <label>유입 집중도</label>
                            <strong>${asPct !== null ? formatNumber(asPct, 1) : '-'}${asPct !== null ? '%' : ''}</strong>
                        <span>${concentrationStatus.label} · 높을수록 신규유입 쏠림</span>
                        </div>
                        <div class="value-driver-item ${chainStatus.tone}">
                            <label>${STRUCTURE_LABELS.expansion}</label>
                            <strong>${csPct !== null ? formatNumber(csPct, 1) : '-'}${csPct !== null ? '%' : ''}</strong>
                        <span>${chainStatus.label} · 높을수록 재구매 안정</span>
                        </div>
                    </div>
                    <p class="chart-hint">현재 파일에서는 영향 요소를 구조 관점으로 표시합니다. VAI/VQI/VCR 세부 분해값이 제공되면 이 영역을 더 정밀하게 확장할 수 있습니다.</p>
                </div>
                <p class="chart-hint">3개 구조 중 약한 축이 전체 구조 건강도를 제한할 수 있습니다.</p>
            </div>
            <details id="brand-fitness-details" class="fitness-details">
                <summary>원인 자세히 보기 (구성 요소/추세/기간별 수치)</summary>
                <div class="fitness-details-body">
                    <div class="factor-block">
                        <h3>브랜드 실전 건강도 구성 요소 (${selectedWindow}일)</h3>
                        <p class="chart-hint">구조, ${FITNESS_COMPONENT_LABELS.value}, ${FITNESS_COMPONENT_LABELS.strength} 중 어떤 요소가 변화를 만들었는지 확인합니다.</p>
                        <div class="factor-grid">
                            <div class="journey-kpi">
                                <label>브랜드 구조 건강도</label>
                                <strong>${componentBhi !== null ? formatNumber(componentBhi, 3) : '-'}</strong>
                            </div>
                            <div class="journey-kpi">
                                <label>${FITNESS_COMPONENT_LABELS.value}</label>
                                <strong>${selectedClvNorm !== null ? formatNumber(selectedClvNorm, 3) : '-'}</strong>
                            </div>
                            <div class="journey-kpi">
                                <label>${FITNESS_COMPONENT_LABELS.strength}</label>
                                <strong>${selectedCustomerStrengthNorm !== null ? formatNumber(selectedCustomerStrengthNorm, 3) : '-'}</strong>
                            </div>
                            <div class="journey-kpi">
                                <label>계산 건강도(참고)</label>
                                <strong>${calculatedBii !== null ? formatNumber(calculatedBii, 3) : '-'}</strong>
                                <span>실제 건강도 대비 차이: ${componentGap !== null ? formatNumber(componentGap, 3) : '-'}</span>
                            </div>
                        </div>
                    </div>
                    <div class="journey-grid">
                        <div class="journey-kpi"><label>${TERM_LABELS.BII} ${selectedWindow}일</label><strong>${selectedWindowBii !== null ? formatNumber(selectedWindowBii, 3) : '-'}</strong></div>
                        <div class="journey-kpi"><label>${TERM_LABELS.BII} 90일</label><strong>${bii90Value !== null ? formatNumber(bii90Value, 3) : '-'}</strong></div>
                        <div class="journey-kpi"><label>${TERM_LABELS.BII} 365일</label><strong>${bii365Value !== null ? formatNumber(bii365Value, 3) : '-'}</strong></div>
                        <div class="journey-kpi"><label>90일 대비 연간 흐름</label><strong>${ratio !== null ? formatNumber(ratio, 2) : '-'}</strong><span>${escapeHtml(trend.status)}</span></div>
                    </div>
                    <div class="card chart-card"><canvas id="biiChart"></canvas></div>
                    <div class="table-container" style="margin-top:1rem;">
                        <table class="data-table">
                            <thead>
                                <tr>
                                    <th>분석 기간</th>
                                    <th>${TERM_LABELS.BII}</th>
                                    <th>${FITNESS_COMPONENT_LABELS.value}</th>
                                    <th>${FITNESS_COMPONENT_LABELS.strength}</th>
                                    <th>단계</th>
                                </tr>
                            </thead>
                            <tbody>${rows}</tbody>
                        </table>
                    </div>
                    <p class="insight-note">${escapeHtml(bhiReferenceText)}</p>
                </div>
            </details>
            <div class="fitness-mobile-note">
                <span>상세 수치는 기본 접힘 상태입니다. 필요 시 위 항목을 펼쳐 확인하세요.</span>
            </div>
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
                <p>지표를 바탕으로 마케팅/MD 실행안을 바로 제안합니다</p>
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
    const jumpNavOpen = Boolean(filters.jumpNavOpen);

    const aaTypes = ['ALL', ...new Set(
        (AppState.data.aaCohortJourney || [])
            .map((row) => normalizeCategoryValue(row.aa_type, ''))
            .filter(Boolean)
    )];
    const aaProducts = ['ALL', ...new Set((AppState.data.aaCohortJourney || []).map((row) => String(row.aa_product_id || '').trim()).filter(Boolean))];

    const aaTypeOptions = aaTypes.map((type) => {
        const selected = String(filters.aaType) === type ? 'selected' : '';
        return `<option value="${escapeHtml(type)}" ${selected}>${escapeHtml(type === 'ALL' ? '전체 Entry 유형' : toAaTypeLabel(type))}</option>`;
    }).join('');

    const aaProductOptions = aaProducts.map((id) => {
        const selected = String(filters.aaProductId) === id ? 'selected' : '';
        const label = id === 'ALL' ? '전체 Entry 상품' : `${getProductName(id)} (${id})`;
        return `<option value="${escapeHtml(id)}" ${selected}>${escapeHtml(label)}</option>`;
    }).join('');

    return `
        <section class="insight-section card insight-filters ${jumpNavOpen ? 'is-open' : 'is-collapsed'} animate-fade-in">
            <button
                class="btn-primary jump-toggle-btn"
                type="button"
                onclick="toggleJumpLinks()"
                title="${jumpNavOpen ? '점프 링크 접기' : '점프 링크 펼치기'}"
                aria-label="${jumpNavOpen ? '점프 링크 접기' : '점프 링크 펼치기'}"
            >
                <i class="ph ${jumpNavOpen ? 'ph-x' : 'ph-list'}" aria-hidden="true"></i>
            </button>
            ${jumpNavOpen ? `
                <nav class="filter-jump-nav">
                    <a href="#brand-fitness">브랜드 건강도</a>
                    <a href="#aa-journey">첫구매 유입 고객 흐름</a>
                    <a href="#aa-transition">재구매 전환</a>
                    <a href="#cart-ca">장바구니 확장</a>
                    <a href="#action-center">실행 카드</a>
                </nav>
                <div class="filter-grid">
                    <label class="filter-field">
                        <span>Entry 유형</span>
                        <select onchange="updateInsightsFilter('aaType', this.value)">${aaTypeOptions}</select>
                    </label>
                    <label class="filter-field">
                        <span>Entry 상품</span>
                        <select onchange="updateInsightsFilter('aaProductId', this.value)">${aaProductOptions}</select>
                    </label>
                    <label class="filter-field">
                        <span>비교 기준 기간</span>
                        <select onchange="updateInsightsFilter('windowDays', this.value)">
                            <option value="7" ${toNumber(filters.windowDays) === 7 ? 'selected' : ''}>7일</option>
                            <option value="30" ${toNumber(filters.windowDays) === 30 ? 'selected' : ''}>30일</option>
                            <option value="90" ${toNumber(filters.windowDays) === 90 ? 'selected' : ''}>90일</option>
                            <option value="365" ${toNumber(filters.windowDays) === 365 ? 'selected' : ''}>365일</option>
                        </select>
                    </label>
                    <button
                        class="btn-primary filter-reset-btn filter-reset-icon-btn"
                        type="button"
                        onclick="resetInsightsFilters()"
                        title="필터 초기화"
                        aria-label="필터 초기화"
                    >
                        <i class="ph ph-arrow-counter-clockwise" aria-hidden="true"></i>
                    </button>
                </div>
            ` : ''}
        </section>
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
                        label: '재구매 도달률',
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
        const biiSeries = rows.map((row) => toNumber(row.bii, NaN));
        const clvSeries = rows.map((row) => {
            const v = toNumber(row.clv_norm, NaN);
            return Number.isFinite(v) ? v : null;
        });
        const strengthSeries = rows.map((row) => {
            const v = toNumber(row.customer_strength_norm, NaN);
            return Number.isFinite(v) ? v : null;
        });
        AppState.charts.bii = new Chart(biiCanvas.getContext('2d'), {
            type: 'line',
            data: {
                labels: rows.map((row) => `${row.window_days}일`),
                datasets: [
                    {
                        label: TERM_LABELS.BII,
                        data: biiSeries,
                        borderColor: '#0ea5e9',
                        backgroundColor: 'rgba(14,165,233,0.2)',
                        fill: true,
                        tension: 0.3,
                        borderWidth: 2.2
                    },
                    {
                        label: FITNESS_COMPONENT_LABELS.value,
                        data: clvSeries,
                        borderColor: '#14b8a6',
                        backgroundColor: 'rgba(20,184,166,0.08)',
                        fill: false,
                        borderDash: [5, 4],
                        tension: 0.25,
                        borderWidth: 1.8
                    },
                    {
                        label: FITNESS_COMPONENT_LABELS.strength,
                        data: strengthSeries,
                        borderColor: '#f59e0b',
                        backgroundColor: 'rgba(245,158,11,0.08)',
                        fill: false,
                        borderDash: [4, 4],
                        tension: 0.25,
                        borderWidth: 1.8
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    title: {
                        display: true,
                        text: '브랜드 실전 건강도 구성 요소 추세',
                        color: '#1e293b'
                    }
                }
            }
        });
    }

    const structureRadarCanvas = document.getElementById('brandStructureRadarChart');
    if (structureRadarCanvas) {
        const asPct = toNumber(structureRadarCanvas.dataset.as, null);
        const csPct = toNumber(structureRadarCanvas.dataset.cs, null);
        const vsPct = toNumber(structureRadarCanvas.dataset.vs, null);
        const hasStructureData = [asPct, csPct, vsPct].some((v) => v !== null);
        if (hasStructureData) {
            AppState.charts.brandStructureRadar = new Chart(structureRadarCanvas.getContext('2d'), {
                type: 'radar',
                data: {
                    labels: [STRUCTURE_LABELS.entry, STRUCTURE_LABELS.expansion, STRUCTURE_LABELS.valueReadiness],
                    datasets: [
                        {
                            label: '브랜드 구조 건강도 3축',
                            data: [
                                asPct !== null ? asPct : 0,
                                csPct !== null ? csPct : 0,
                                vsPct !== null ? vsPct : 0
                            ],
                            borderColor: '#2563eb',
                            backgroundColor: 'rgba(37,99,235,0.18)',
                            pointBackgroundColor: '#1d4ed8',
                            borderWidth: 2
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        title: {
                            display: true,
                            text: '브랜드 구조 건강도 레이더(%)',
                            color: '#1e293b'
                        },
                        tooltip: {
                            callbacks: {
                                label: (ctx) => `${ctx.label}: ${formatNumber(ctx.raw, 1)}%`
                            }
                        }
                    },
                    scales: {
                        r: {
                            min: 0,
                            max: 100,
                            ticks: {
                                stepSize: 20,
                                callback: (v) => `${v}%`
                            }
                        }
                    }
                }
            });
        }
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
            ${renderInsightFilters(model)}
            ${renderBrandFitness(model)}
            ${renderAAJourney(model)}
            ${renderAATransition(model)}
            ${renderCASection(model)}
            ${renderActionCenter(model)}
        </div>
    `;
    applyFriendlyUi(container);

    renderInsightsCharts(model);
    bindInsightsInteractions();
}

function bindInsightsInteractions() {
    const detail = document.getElementById('brand-fitness-details');
    if (!detail) return;
    detail.addEventListener('toggle', () => {
        if (detail.open && AppState.charts.bii) {
            AppState.charts.bii.resize();
            AppState.charts.bii.update('none');
        }
    });
}

window.updateInsightsFilter = (key, value) => {
    if (key === 'windowDays') AppState.viewState.insights[key] = toNumber(value, 90);
    else AppState.viewState.insights[key] = value;
    renderInsightsPage();
};

window.toggleJumpLinks = () => {
    AppState.viewState.insights.jumpNavOpen = !Boolean(AppState.viewState.insights.jumpNavOpen);
    renderInsightsPage();
};

window.updateInsightsSnapshot = (value) => {
    const snapshot = String(value || '').trim();
    if (snapshot) {
        AppState.viewState.insights.dateFrom = snapshot;
        AppState.viewState.insights.dateTo = snapshot;
    } else {
        AppState.viewState.insights.dateFrom = '';
        AppState.viewState.insights.dateTo = '';
    }
    renderInsightsPage();
};

window.resetInsightsFilters = () => {
    AppState.viewState.insights = {
        dateFrom: '',
        dateTo: '',
        aaType: 'ALL',
        aaProductId: 'ALL',
        windowDays: 90,
        jumpNavOpen: false
    };
    renderInsightsPage();
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
            <p>데이터를 다시 불러오거나 업로드하고, 필요하면 로컬 저장 데이터를 초기화할 수 있어요.</p>
            <div class="settings-actions">
                <button class="btn-primary" type="button" onclick="showUploadModal()">CSV 업로드</button>
                <button class="btn-primary" type="button" onclick="resyncLocalCsvFromSettings()">로컬 파일 다시 불러오기</button>
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

window.resyncLocalCsvFromSettings = async () => {
    const keys = await DB.getAllKeys();
    const result = await syncDataFromLocalCsv(keys);
    alert(`로컬 동기화가 끝났어요. ${result.loadedCount}개 파일을 반영했어요.`);
    location.reload();
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
    } else if (pageId === 'page-cart') {
        AppState.viewState.cart.searchQuery = focusEntity;
        AppState.viewState.cart.searchMode = 'id';
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
    initAppUI();

    const sidebar = document.querySelector('.user-profile');
    if (sidebar) {
        sidebar.innerHTML = '<button class="btn-primary settings-launch-btn" style="width:100%" onclick="showSettingsModal()"><i class="ph ph-sliders-horizontal"></i> 설정</button>';
    }

    try {
        let keys = await DB.getAllKeys();
        const bootstrap = await syncDataFromLocalCsv(keys);
        if (bootstrap.loadedCount > 0) {
            console.info(
                '[PGM] 로컬 CSV 동기화 완료:',
                bootstrap.loaded.map((item) => `${item.key} <= ${item.source} (${item.rows} rows, ${item.mode})`).join(', ')
            );
        }
        keys = await DB.getAllKeys();
        if (keys.length === 0) {
            document.getElementById('content-area').innerHTML = `
                <div class="card animate-fade-in" style="text-align:center; padding:4rem;">
                    <i class="ph ph-database" style="font-size:4rem; color:var(--text-muted); margin-bottom:1rem;"></i>
                    <h3>데이터 없음</h3>
                    <p style="color:var(--text-muted); margin-bottom:0.6rem;">기본 CSV 자동 로드를 시도했지만 불러오지 못했습니다.</p>
                    <p style="color:var(--text-muted); margin-bottom:2rem;"><code>data/</code> 경로에 CSV를 두거나 수동 업로드를 진행해 주세요.</p>
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
        } else if (pageId === 'page-cart') {
            const [c, d, s, groupMap] = await Promise.all([
                loadDataFromDB(REQUIRED_FILES.cartAnchor),
                loadOptionalDataFromDB(REQUIRED_FILES.cartAnchorDetail, []),
                loadDataFromDB(REQUIRED_FILES.anchorScored),
                loadOptionalDataFromDB(REQUIRED_FILES.productGroupMap, [])
            ]);
            AppState.rawData.cartAnchor = c;
            AppState.rawData.cartAnchorDetail = d;
            AppState.rawData.anchorScored = s;
            AppState.rawData.productGroupMap = groupMap;
            rebuildDerivedData();
            applyFocusFromUrl(pageId);
            renderCartAnalysis();
            applyFriendlyUi(document.body);
        }
    } catch (e) {
        console.error(e);
        document.getElementById('content-area').innerHTML = `<div class="card" style="text-align:center; padding:2rem;"><h3>필수 데이터 누락</h3><p style="color:var(--accent)">${escapeHtml(e.message)}</p><button class="btn-primary" onclick="showUploadModal()">누락 파일 업로드</button></div>`;
    }
}

document.addEventListener('DOMContentLoaded', init);
