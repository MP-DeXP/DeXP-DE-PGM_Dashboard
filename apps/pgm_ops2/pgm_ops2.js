const state = {
    view: 'priority',
    bundle: null,
    selectedProductId: '',
    windowKey: '7d'
};

const VIEW_MODEL_FILES = {
    vm_priority_queue: ['vm_priority_queue.csv'],
    vm_queue_summary: ['vm_queue_summary.csv'],
    vm_segment_map: ['vm_segment_map.csv'],
    vm_structure_map_cells: ['vm_structure_map_cells.csv', 'vm_role_revenue_matrix.csv'],
    vm_product_detail: ['vm_product_detail.csv'],
    vm_definition_rules: ['vm_operation_rules.csv', 'vm_definition_rules.csv'],
    vm_data_health: ['vm_data_health.csv'],
    vm_data_health_overview: ['vm_data_health_overview.csv', 'vm_operations_overview.csv'],
    vm_data_health_detail: ['vm_data_health_detail.csv'],
    vm_brand_score_panel: ['vm_brand_score_panel.csv'],
    vm_brand_direction_summary: ['vm_brand_direction_summary.csv'],
    vm_brand_score_product_contributors: ['vm_brand_score_product_contributors.csv'],
    vm_reconstruction_registry: ['vm_reconstruction_registry.csv'],
    vm_iteration_log: ['vm_iteration_log.csv']
};

const MART_FILES = {
    mart_product_revenue_windows: ['mart_product_revenue_windows.csv'],
    mart_product_role_taxonomy_daily: ['mart_product_role_taxonomy_by_window.csv', 'mart_product_role_taxonomy_daily.csv'],
    mart_product_priority_basis: ['mart_product_priority_basis.csv'],
    mart_priority_queue_snapshot: ['mart_priority_queue_snapshot.csv'],
    mart_segment_structure_snapshot: ['mart_role_revenue_matrix_snapshot.csv', 'mart_segment_structure_snapshot.csv'],
    mart_data_health_snapshot: ['mart_operations_overview_snapshot.csv', 'mart_data_health_snapshot.csv'],
    mart_brand_score_reconstruction: ['mart_brand_score_reconstruction.csv'],
    mart_brand_score_validation_status: ['mart_brand_status_summary.csv', 'mart_brand_score_validation_status.csv']
};

const QA_FILES = {
    raw_manifest: ['operations_manifest.csv', 'raw_manifest.csv'],
    validation_summary: ['operations_checks.csv', 'validation_summary.csv'],
    validation_report: ['validation_report.md'],
    tone_audit: ['tone_audit.csv'],
    implementation_scope: ['implementation_scope.csv']
};

const REVENUE_SEGMENT_ORDER = ['감소', '유지', '증가'];
const ROLE_ORDER = ['첫구매기여', '재구매확장기여', '반복구매기여', '동시구매기여', '관측 없음'];
const WINDOW_ORDER = ['1d', '7d', '30d'];

function getWindowLabel(windowKey) {
    if (windowKey === '1d') return '하루 기준';
    if (windowKey === '30d') return '30일 기준';
    return '7일 기준';
}

function getWindowShortLabel(windowKey) {
    if (windowKey === '1d') return '1일';
    if (windowKey === '30d') return '30일';
    return '7일';
}

function getWindowLead(windowKey) {
    return `${getWindowShortLabel(windowKey)} 기준. 매출·역할 우선, 브랜드 방향 보조.`;
}

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

function toNumber(value) {
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
}

function formatNumber(value, fallback = '-') {
    const number = toNumber(value);
    return number == null ? fallback : number.toLocaleString('ko-KR');
}

function formatPercent(value, { digits = 1, signed = true, fallback = '-' } = {}) {
    const number = toNumber(value);
    if (number == null) {
        return fallback;
    }

    const percentText = `${(number * 100).toFixed(digits)}%`;
    if (signed && number > 0) {
        return `+${percentText}`;
    }

    return percentText;
}

function formatDecimal(value, { digits = 2, fallback = '-' } = {}) {
    const number = toNumber(value);
    return number == null ? fallback : number.toFixed(digits);
}

function toText(value, fallback = '') {
    return value == null ? fallback : String(value);
}

function textOrFallback(value, fallback = '-') {
    const text = String(value ?? '').trim();
    return text || fallback;
}

function normalizeRevenueSegment(value) {
    const text = textOrFallback(value, '유지');

    if (text.includes('감소') || text.includes('하락')) {
        return '감소';
    }

    if (text.includes('증가') || text.includes('상승')) {
        return '증가';
    }

    return '유지';
}

function translateCompareState(value) {
    if (value === 'available') return '비교 가능';
    if (value === 'unavailable') return '비교 보류';
    if (value === 'partial') return '일부 비교 가능';
    return textOrFallback(value);
}

function translateBrandStatus(value) {
    if (value === 'limited') return '브랜드 방향 신호 제한 반영';
    if (value === 'provisional') return '브랜드 구조 변화 신호';
    if (value === 'unavailable') return '브랜드 방향 산출 없음';
    if (value === 'near-core' || value === 'near_core') return '브랜드 구조 변화 신호 검증 중';
    if (value === 'available') return '브랜드 구조 반영 완료';
    return textOrFallback(value);
}

function translateBrandStatusCompact(value) {
    if (value === 'limited') return '제한 반영';
    if (value === 'provisional') return '변화 신호';
    if (value === 'unavailable') return '산출 없음';
    if (value === 'near-core' || value === 'near_core') return '검증 중';
    if (value === 'available') return '반영 완료';
    return textOrFallback(value);
}

function brandStatusClass(value) {
    if (value === 'available' || value === 'near-core' || value === 'near_core') return 'status-ready';
    if (value === 'limited' || value === 'provisional') return 'status-warning';
    if (value === 'unavailable') return 'status-muted';
    return 'status-neutral';
}

function translateDirectionLabel(value) {
    if (value === 'flat') return '변화 작음';
    if (value === 'improving') return '개선';
    if (value === 'deteriorating') return '악화';
    if (value === 'hold') return '판단 보류';
    return textOrFallback(value, '판단 보류');
}

function coverageStateClass(value) {
    if (value === '정상') return 'status-ready';
    if (String(value).includes('비교 가능')) return 'status-ready';
    if (value === '30일까지만 가능') return 'status-warning';
    if (value === 'history 부족' || value === '비교 불가') return 'status-muted';
    return 'status-neutral';
}

function translateConfidenceLabel(value) {
    if (value === 'High') return '높음';
    if (value === 'Medium') return '보통';
    if (value === 'Low') return '낮음';
    return textOrFallback(value);
}

function translateRuleGroup(value) {
    if (value === 'Revenue') return '매출 흐름';
    if (value === 'Role') return '역할 분류';
    if (value === 'Brand Score') return '브랜드 방향';
    if (value === '데이터 상태') return '운영 데이터';
    return textOrFallback(value);
}

function translateRuleName(value) {
    if (value === 'same-date snapshot only') return '동일 일자 기준 분류';
    if (value === 'canonical score 우선') return '대표 점수 우선';
    if (value === 'freshness cap') return '최신 반영 범위 적용';
    if (value === 'reconstruction registry') return '재구성 기준 기록';
    if (value === 'freshness 공개') return '최신일 공개';
    if (value === 'Rosetta products 기준') return '상품 이미지 기준';
    return textOrFallback(value);
}

function translateRuleStatus(value) {
    if (value === '제한적 반영') return '제한 운영';
    return textOrFallback(value);
}

function translateDetailSection(value) {
    if (value === 'Revenue') return '매출 흐름';
    if (value === '매출') return '매출 흐름';
    if (value === 'Role') return '역할 분류';
    if (value === '역할 분류') return '역할 분류';
    if (value === 'Brand Score') return '브랜드 방향';
    if (value === '브랜드 점수') return '브랜드 방향';
    return textOrFallback(value);
}

function translateDetailLabel(section, label) {
    if (section === 'Role' && label === '현재 taxonomy') return '현재 역할 분류';
    if (section === '역할 분류' && label === '현재 역할') return '현재 역할 분류';
    if (section === 'Revenue' && label === '최근 30일 대비') return '30일 매출 변화';
    if (section === '매출' && label === '최근 30일 대비') return '30일 매출 변화';
    if (section === 'Revenue' && label === '비교 상태') return '비교 가능 여부';
    if (section === '매출' && label === '비교 상태') return '비교 가능 여부';
    if (section === 'Brand Score' && label === '상태') return '브랜드 전체 방향';
    if (section === 'Brand Score' && label === 'brand-level 상태') return '브랜드 전체 방향';
    if (section === '브랜드 점수' && label === '브랜드 반영 상태') return '브랜드 전체 방향';
    if (section === '브랜드 점수' && label === '브랜드 전체 방향') return '브랜드 전체 방향';
    if (section === 'Brand Score' && label === '상품 기여 상태') return '상품 기여';
    if (section === '브랜드 점수' && label === '상품 기여 상태') return '상품 기여';
    if (section === '브랜드 점수' && label === '상품 기여') return '상품 기여';
    if (section === '근거' && label === '반복 구매') return '반복 구매 기여';
    if (section === '근거' && label === '동시 구매') return '동시 구매 기여';
    if (section === '근거' && label === '동시구매 분류') return '동시 구매 분류';
    if (section === '근거' && label === '상위 연관 상품') return '상위 연관 상품';
    return textOrFallback(label);
}

function translateDetailValue(section, label, value) {
    if ((section === 'Revenue' || section === '매출') && label === '비교 상태') {
        return translateCompareState(value);
    }

    if ((section === 'Brand Score' || section === '브랜드 점수') && label === '브랜드 전체 방향') {
        return translateDirectionLabel(value);
    }

    if (section === 'Brand Score' || section === '브랜드 점수') {
        return translateBrandStatus(value);
    }

    return textOrFallback(value);
}

function sanitizeOperatingCopy(value) {
    let text = String(value ?? '').trim();

    if (!text) {
        return '-';
    }

    const replacements = [
        ['Brand Score limited', '브랜드 방향 신호를 제한 반영합니다.'],
        ['Brand Score provisional', '브랜드 구조 변화 신호를 읽을 수 있습니다.'],
        ['Brand Score unavailable', '브랜드 방향 산출이 아직 없습니다.'],
        ['Revenue 비교 가능 상태', '매출 비교 가능 범위'],
        ['Role 관측 가능 상태', '역할 분류 확인 범위'],
        ['Brand Score 반영 상태', '브랜드 방향 반영 범위'],
        ['event 또는 basket freshness 제약으로 provisional을 제한했습니다.', '브랜드 신호 최신 반영 범위를 다시 확인하고 있습니다.'],
        ['event 또는 basket freshness 제약으로 provisional을 제한합니다.', '브랜드 신호 최신 반영 범위를 다시 확인하고 있습니다.'],
        ['직전 동일 길이 기간과 비교 가능합니다.', '직전 같은 길이 기간과 바로 비교할 수 있습니다.'],
        ['직전기간 비교를 위한 history가 부족합니다.', '직전 기간과 비교할 이력이 아직 충분하지 않습니다.'],
        ['Role taxonomy', '역할 분류'],
        ['same-date snapshot', '동일 일자 기준'],
        ['canonical score', '대표 점수'],
        ['source freshness', '데이터 반영 현황'],
        ['source history', '이력 범위'],
        ['raw_rosetta.products.csv', 'Rosetta 상품 기준'],
        ['list_image', '상품 이미지 기준']
    ];

    replacements.forEach(([from, to]) => {
        text = text.replaceAll(from, to);
    });

    text = text.replace(/\bavailable\b/gi, '비교 가능');
    text = text.replace(/\blimited\b/gi, '제한 반영');
    text = text.replace(/\bprovisional\b/gi, '구조 변화 신호');
    text = text.replace(/\bunavailable\b/gi, '산출 없음');
    text = text.replace(/\btaxonomy\b/gi, '분류');
    text = text.replace(/\bfreshness\b/gi, '최신 반영');
    text = text.replace(/\bwindow\b/gi, '기간');
    text = text.replace(/\bhistory\b/gi, '이력');
    text = text.replace(/\bsource\b/gi, '데이터');
    text = text.replace(/\bRevenue\b/g, '매출');
    text = text.replace(/\bRole\b/g, '역할');

    return text;
}

function normalizeBrandReason(value, fallback = '브랜드 전체 방향을 아직 판단하지 못했습니다.') {
    const text = String(value ?? '').trim();
    const cleanedText = text
        .replace(/^브랜드 방향 신호 제한 반영:\s*/g, '')
        .replace(/^브랜드 방향 산출 없음:\s*/g, '')
        .replace(/^브랜드 구조 변화 신호:\s*/g, '')
        .replace(/^브랜드 구조 변화 신호 검증 중:\s*/g, '')
        .replace(/^브랜드 구조 반영 완료:\s*/g, '');
    const parts = cleanedText.split(/[|/]/).map((part) => part.trim()).filter(Boolean);
    const normalizedParts = [];

    const pushUnique = (message) => {
        if (message && !normalizedParts.includes(message)) {
            normalizedParts.push(message);
        }
    };

    parts.forEach((part) => {
        const lowered = part.toLowerCase();

        if (
            lowered.includes('event freshness')
            || part.includes('최근 주문 기준')
            || part.includes('최신일이 늦')
            || part.includes('최근 반영이 늦')
            || part.includes('이벤트 최신성 지연')
        ) {
            pushUnique('최근 주문 기준이 늦습니다');
            return;
        }

        if (
            lowered.includes('basket parity')
            || part.includes('raw basket detail')
            || part.includes('동시구매 구조')
            || part.includes('유사도')
            || part.includes('동시구매 재구성 안정성 낮음')
        ) {
            pushUnique('동시구매 구조를 더 확인해야 합니다');
            return;
        }

        if (
            part.includes('핵심 축')
            || part.includes('핵심 입력')
            || part.includes('재구성하지 못')
            || part.includes('구성하지 못')
            || part.includes('집계되지 않았')
            || part.includes('입력 부족')
            || part.includes('핵심 근거 부족')
            || part.includes('상품별 핵심 근거 부족')
        ) {
            pushUnique('핵심 입력이 아직 부족합니다');
            return;
        }

        if (part.includes('정합성 검증 필요') || part.includes('신뢰도 검증 필요')) {
            pushUnique('정합성 확인이 더 필요합니다');
            return;
        }

        if (
            lowered.includes('brand-level')
            || lowered.includes('canonical')
            || lowered.includes('contributor signal')
        ) {
            pushUnique('상품 기여 정보는 보조 정보로 함께 봅니다');
            return;
        }

        if (part.includes('재구성 제약')) {
            pushUnique('재구성 범위를 더 확인해야 합니다');
            return;
        }

        pushUnique(sanitizeOperatingCopy(part));
    });

    return normalizedParts.length ? normalizedParts.join(' · ') : fallback;
}

function buildBrandCaution(status) {
    if (isBrandRestrictedStatus(status)) {
        return '브랜드 전체 방향을 먼저 보고 상품 기여는 그다음으로 읽습니다.';
    }
    return '브랜드 전체 방향을 먼저 읽고 상품 기여는 보조 정보로 함께 봅니다.';
}

function isBrandRestrictedStatus(value) {
    return [
        'limited',
        'unavailable',
        'provisional',
        'near-core',
        '브랜드 방향 신호 제한 반영',
        '브랜드 방향 산출 없음',
        '브랜드 구조 변화 신호',
        '브랜드 구조 변화 신호 검증 중'
    ].includes(toText(value));
}

function inferDirectionKey(directionRow = {}) {
    const directionKey = toText(directionRow.direction_key).trim();
    if (directionKey) {
        return directionKey;
    }

    const label = toText(directionRow.direction_label);
    if (label.includes('개선')) return 'improving';
    if (label.includes('악화')) return 'deteriorating';
    if (label.includes('변화 작음') || label.includes('보합')) return 'flat';
    return 'hold';
}

function getDirectionHeadlineLabel(directionKey) {
    if (directionKey === 'improving') return '개선';
    if (directionKey === 'deteriorating') return '악화';
    if (directionKey === 'flat') return '변화 작음';
    return '판단 보류';
}

function getDirectionStripState(directionKey) {
    if (directionKey === 'improving') return '개선';
    if (directionKey === 'deteriorating') return '악화';
    if (directionKey === 'flat') return '보합';
    return '보류';
}

function getDirectionToneClass(directionKey) {
    if (directionKey === 'improving') return 'is-improving';
    if (directionKey === 'deteriorating') return 'is-deteriorating';
    return 'is-neutral';
}

function getComparisonPeriodShortLabel(label, windowKey = '') {
    const text = toText(label).trim();
    if (text.includes('하루') || windowKey === '1d') return '1일';
    if (text.includes('30일') || windowKey === '30d') return '30일';
    if (text.includes('7일') || windowKey === '7d') return '7일';
    return text;
}

function attachComparisonParticle(label) {
    const text = toText(label).trim();
    if (!text) {
        return '비교 기간';
    }
    const lastChar = text[text.length - 1];
    const code = lastChar.charCodeAt(0);
    const hasBatchim = code >= 0xac00 && code <= 0xd7a3 && (code - 0xac00) % 28 !== 0;
    return `${text}${hasBatchim ? '과' : '와'}`;
}

function buildComparisonShortLabel(directionRow = {}) {
    const directionKey = inferDirectionKey(directionRow);
    const comparisonLabel = getComparisonPeriodShortLabel(
        directionRow.comparison_period_label,
        directionRow.comparison_window_key
    );

    if (directionKey === 'improving') {
        return `${comparisonLabel || '비교 기간'}보다 강함`;
    }
    if (directionKey === 'deteriorating') {
        return `${comparisonLabel || '비교 기간'}보다 약함`;
    }
    if (directionKey === 'flat') {
        return `${attachComparisonParticle(comparisonLabel)} 비슷`;
    }
    return '비교 기준 부족';
}

function collectRestrictionShortLabels(...values) {
    const labels = [];
    const pushUnique = (label) => {
        if (label && !labels.includes(label)) {
            labels.push(label);
        }
    };

    values
        .filter(Boolean)
        .flatMap((value) => String(value).split(/[|/]/))
        .map((part) => part.trim())
        .filter(Boolean)
        .forEach((part) => {
            const lowered = part.toLowerCase();

            if (
                lowered.includes('event freshness')
                || part.includes('이벤트 최신성 지연')
                || part.includes('최근 주문 기준')
                || part.includes('최신일이 늦')
            ) {
                pushUnique('최근 주문 기준 지연');
                return;
            }

            if (
                lowered.includes('basket parity')
                || part.includes('동시구매')
                || part.includes('raw basket detail')
                || part.includes('유사도')
                || part.includes('재구성 안정성 낮음')
            ) {
                pushUnique('동시구매 재구성 제한');
                return;
            }

            if (
                part.includes('핵심 입력')
                || part.includes('핵심 근거')
                || part.includes('입력 부족')
                || part.includes('관측 이력')
                || part.includes('history')
                || part.includes('집계되지 않았')
                || part.includes('부족')
            ) {
                pushUnique('핵심 입력 부족');
                return;
            }

            if (
                part.includes('정합성')
                || part.includes('신뢰도')
                || part.includes('검증')
                || part.includes('확인 필요')
            ) {
                pushUnique('정합성 추가 확인 필요');
            }
        });

    return labels;
}

function buildRestrictionShortLabel(directionRow = {}, brandLevelRow = {}) {
    const restrictionLabels = collectRestrictionShortLabels(
        directionRow.status_note,
        directionRow.status_label,
        brandLevelRow.status_reason,
        brandLevelRow.limitation_reason,
        brandLevelRow.limitation_reason_detail
    );

    if (restrictionLabels.length) {
        return restrictionLabels.slice(0, 2).join(' · ');
    }

    if (isBrandRestrictedStatus(directionRow.status_label || brandLevelRow.status_label || brandLevelRow.brand_score_status)) {
        return '정합성 추가 확인 필요';
    }

    return '';
}

function getTopContributorNames(contributorRows = [], limit = 3) {
    return contributorRows
        .slice()
        .sort((left, right) => {
            const leftRank = toNumber(left.contributor_rank) ?? Number.POSITIVE_INFINITY;
            const rightRank = toNumber(right.contributor_rank) ?? Number.POSITIVE_INFINITY;
            if (leftRank !== rightRank) {
                return leftRank - rightRank;
            }
            const leftShare = toNumber(left.contribution_share) ?? 0;
            const rightShare = toNumber(right.contribution_share) ?? 0;
            return rightShare - leftShare;
        })
        .map((row) => textOrFallback(row.product_name || row.product_id, ''))
        .filter(Boolean)
        .filter((name, index, array) => array.indexOf(name) === index)
        .slice(0, limit);
}

function buildContributionShortLabel(contributorRows = [], { detailed = false } = {}) {
    const names = getTopContributorNames(contributorRows, 3);

    if (detailed && names.length) {
        return `상품 기여: ${names.slice(0, 3).join(', ')}`;
    }

    return '상품 기여: 상위 3개 중심';
}

function buildWindowStripItems(currentWindowKey) {
    const directionRows = getViewModel('vm_brand_direction_summary');
    const rowMap = new Map(directionRows.map((row) => [row.window_key, row]));

    return WINDOW_ORDER.map((windowKey) => {
        const row = rowMap.get(windowKey) ?? {};
        const directionKey = inferDirectionKey(row);

        return {
            window_key: windowKey,
            label: `${getWindowShortLabel(windowKey)} ${getDirectionStripState(directionKey)}`,
            selected: windowKey === currentWindowKey,
            tone_class: getDirectionToneClass(directionKey)
        };
    });
}

function deriveBrandDirectionDisplay(directionRow = {}, brandLevelRow = {}, contributorRows = [], options = {}) {
    const directionKey = inferDirectionKey(directionRow);
    const statusKey = brandLevelRow.brand_score_status || directionRow.status_key || directionRow.status_label;
    const isRestricted = isBrandRestrictedStatus(statusKey) || isBrandRestrictedStatus(directionRow.status_label);

    return {
        headline_label: getDirectionHeadlineLabel(directionKey),
        comparison_short_label: buildComparisonShortLabel(directionRow),
        window_strip_items: buildWindowStripItems(state.windowKey),
        restriction_short_label: buildRestrictionShortLabel(directionRow, brandLevelRow),
        contribution_short_label: buildContributionShortLabel(contributorRows, options),
        status_compact_label: translateBrandStatusCompact(statusKey || (isRestricted ? 'limited' : 'available')),
        tone_class: getDirectionToneClass(directionKey),
        is_restricted: isRestricted
    };
}

function renderBrandDirectionCard(directionRow = {}, brandLevelRow = {}, contributorRows = [], options = {}) {
    const {
        containerClass = 'overview-card',
        showRestrictionReason = false,
        detailed = false
    } = options;
    const display = deriveBrandDirectionDisplay(directionRow, brandLevelRow, contributorRows, { detailed });

    return `
        <article class="${escapeHtml(containerClass)} brand-direction-card ${escapeHtml(display.tone_class)}">
            <div class="section-kicker">브랜드 전체 방향</div>
            <strong class="brand-direction-headline">${escapeHtml(display.headline_label)}</strong>
            <div class="brand-window-strip" aria-label="기간별 브랜드 방향">
                ${display.window_strip_items.map((item) => `
                    <span class="brand-window-strip-item ${escapeHtml(item.tone_class)} ${item.selected ? 'is-selected' : ''}">
                        ${escapeHtml(item.label)}
                    </span>
                `).join('')}
            </div>
            <p class="brand-direction-summary">${escapeHtml(display.comparison_short_label)}</p>
            <p class="brand-direction-support">${escapeHtml(display.contribution_short_label)}</p>
            <div class="brand-direction-meta">
                <span class="brand-direction-meta-chip">${escapeHtml(display.status_compact_label)}</span>
                ${showRestrictionReason && display.is_restricted && display.restriction_short_label
                    ? `<span class="brand-direction-meta-label">${escapeHtml(display.restriction_short_label)}</span>`
                    : ''}
                ${detailed ? `<span class="brand-direction-meta-label">상품 기여는 보조 정보</span>` : ''}
            </div>
        </article>
    `;
}

function getRoleTaxonomyHelper(roleTaxonomy) {
    if (roleTaxonomy === '첫구매기여') return '첫구매 유입이 모이는 상품군';
    if (roleTaxonomy === '재구매확장기여') return '재구매 확장 흐름이 보이는 상품군';
    if (roleTaxonomy === '반복구매기여') return '반복구매 비중이 높은 상품군';
    if (roleTaxonomy === '동시구매기여') return '동시구매 근거가 있는 상품군';
    return '역할 근거가 약한 상품군';
}

function summarizeHealthTrust(overviewRows = []) {
    const revenue = overviewRows.find((row) => row.area_key === 'revenue_compare') ?? {};
    const role = overviewRows.find((row) => row.area_key === 'role_observation') ?? {};
    const parts = [];

    if (toText(revenue.summary_value)) {
        parts.push(`매출은 ${sanitizeOperatingCopy(revenue.summary_value)}`);
    }
    if (toText(role.summary_value)) {
        parts.push(`역할 분류는 ${sanitizeOperatingCopy(role.summary_value)}`);
    }

    return {
        value: parts.length ? '매출과 역할 분류를 우선 확인할 수 있습니다' : '기본 운영 기준을 확인할 수 있습니다',
        note: parts.length ? `${parts.join(' · ')}.` : '매출 흐름과 역할 분류를 우선 기준으로 봅니다.'
    };
}

function summarizeHealthBrandDirection(directionRow = {}, imageSummary = '') {
    const parts = [];
    if (toText(directionRow.direction_note)) {
        parts.push(directionRow.direction_note);
    }
    if (toText(directionRow.status_note)) {
        parts.push(directionRow.status_note);
    }
    if (toText(directionRow.product_contribution_note)) {
        parts.push(directionRow.product_contribution_note);
    }
    if (imageSummary && imageSummary !== '상품 이미지 반영 완료') {
        parts.push(imageSummary);
    }

    return {
        value: toText(directionRow.direction_label, '브랜드 방향 판단 보류'),
        note: parts.length ? parts.join(' ') : '브랜드 전체 방향을 읽을 수 있는 요약이 아직 없습니다.'
    };
}

function summarizeHealthCaution(directionRow = {}) {
    const cautionParts = [
        '브랜드 전체 방향을 먼저 보고 상품 기여는 그 다음으로 읽습니다',
        '브랜드 방향은 현재 시점의 1일·7일·30일 상대 비교로 읽습니다'
    ];

    if (isBrandRestrictedStatus(directionRow.status_label)) {
        cautionParts.push('제한 상태에서는 매출과 역할 분류를 먼저 확인합니다');
    }

    return {
        value: '해석 순서를 먼저 지킵니다',
        note: `${cautionParts.join(' · ')}.`
    };
}

function priorityClass(level) {
    if (level === '즉시 확인') return 'priority-immediate';
    if (level === '주의 관찰') return 'priority-watch';
    return 'priority-stable';
}

function buildImageAlt(productName) {
    return productName || '상품 이미지';
}

function renderProductThumb(row, { size = 'medium' } = {}) {
    const productName = row.product_name || row.product_id || '';
    const imageUrl = String(row.product_image_url || '').trim();
    const sizeClass = size === 'small' ? 'is-small' : size === 'large' ? 'is-large' : '';
    const altText = escapeHtml(buildImageAlt(productName));

    if (!imageUrl) {
        return `
            <span
                class="product-thumb is-placeholder ${sizeClass}"
                data-product-thumb
                role="img"
                aria-label="${altText}"
            >
                <span class="product-thumb-placeholder" aria-hidden="true"></span>
            </span>
        `;
    }

    return `
        <span class="product-thumb ${sizeClass}" data-product-thumb>
            <img
                src="${escapeHtml(imageUrl)}"
                alt="${altText}"
                loading="lazy"
                data-product-image
            >
            <span class="product-thumb-placeholder" aria-hidden="true"></span>
        </span>
    `;
}

function renderProductIdentity(row, { size = 'medium', showProductId = false } = {}) {
    return `
        <div class="product-identity">
            ${renderProductThumb(row, { size })}
            <div class="product-identity-copy">
                <strong>${escapeHtml(row.product_name || row.product_id)}</strong>
                ${showProductId ? `<p>${escapeHtml(row.product_id || '')}</p>` : ''}
            </div>
        </div>
    `;
}

function renderStatusChip(label, className = 'status-neutral') {
    return `<span class="status-chip ${className}">${escapeHtml(label)}</span>`;
}

function getViewModel(name) {
    return state.bundle?.view_model?.[name] ?? [];
}

function getMartRows(name) {
    return state.bundle?.mart?.[name] ?? [];
}

function getQaRows(name) {
    return state.bundle?.qa?.[name] ?? [];
}

function getRowsForWindow(rows) {
    if (!Array.isArray(rows) || !rows.length) {
        return [];
    }
    if (!('window_key' in rows[0])) {
        return rows;
    }
    const scopedRows = rows.filter((row) => row.window_key === state.windowKey);
    return scopedRows.length ? scopedRows : rows.filter((row) => row.window_key === '7d');
}

function readWindowKeyFromUrl() {
    const value = new URLSearchParams(window.location.search).get('window');
    return WINDOW_ORDER.includes(value) ? value : '7d';
}

function readViewFromUrl() {
    const value = new URLSearchParams(window.location.search).get('view');
    return ['priority', 'segments', 'detail', 'definitions', 'health'].includes(value) ? value : 'priority';
}

function syncStateToUrl() {
    const url = new URL(window.location.href);
    url.searchParams.set('window', state.windowKey);
    url.searchParams.set('view', state.view);
    window.history.replaceState({}, '', url);
}

function setTitle(view) {
    const titleMap = {
        priority: '우선순위',
        segments: '구조 맵',
        detail: '상세 보기',
        definitions: '운영 기준',
        health: '운영 현황'
    };
    document.querySelector('#page-title').textContent = titleMap[view] ?? 'PGM 운영 툴';
}

function buildProductMaps() {
    const queueRows = getRowsForWindow(getViewModel('vm_priority_queue'));
    const brandLevelRows = getRowsForWindow(getViewModel('vm_brand_score_panel'));
    const brandDirectionRows = getRowsForWindow(getViewModel('vm_brand_direction_summary'));
    const contributorRows = getRowsForWindow(getViewModel('vm_brand_score_product_contributors'));
    const roleRows = getRowsForWindow(getMartRows('mart_product_role_taxonomy_daily'));
    const revenueRows = getRowsForWindow(getMartRows('mart_product_revenue_windows'));

    return {
        queueMap: new Map(queueRows.map((row) => [row.product_id, row])),
        brandLevelRow: brandLevelRows[0] ?? {},
        brandDirectionRow: brandDirectionRows[0] ?? {},
        contributorMap: new Map(contributorRows.map((row) => [row.product_id, row])),
        roleMap: new Map(roleRows.map((row) => [row.product_id, row])),
        revenueMap: new Map(revenueRows.map((row) => [row.product_id, row]))
    };
}

function getBrandDirectionRow() {
    return getRowsForWindow(getViewModel('vm_brand_direction_summary'))[0] ?? {};
}

function buildQueueBrandDirectionReason(directionRow = {}, contributorRow = {}) {
    const statusKey = contributorRow.contribution_status || 'available';
    const display = deriveBrandDirectionDisplay(directionRow, { brand_score_status: statusKey }, [contributorRow], { detailed: false });
    const restrictionSuffix = display.is_restricted ? `, ${display.status_compact_label}` : '';
    return `${getWindowShortLabel(state.windowKey)} ${getDirectionStripState(inferDirectionKey(directionRow))}${restrictionSuffix}. 상품 기여는 보조 정보.`;
}

function orderValues(values, preferredOrder) {
    const uniqueValues = Array.from(new Set(values.filter(Boolean)));
    const ordered = preferredOrder.filter((value) => uniqueValues.includes(value));
    const rest = uniqueValues.filter((value) => !preferredOrder.includes(value)).sort((left, right) => left.localeCompare(right, 'ko'));
    return [...ordered, ...rest];
}

function summarizePriorityCounts(rows) {
    return [
        { label: '즉시 확인', count: rows.filter((row) => row.priority_level === '즉시 확인').length, className: 'priority-immediate' },
        { label: '주의 관찰', count: rows.filter((row) => row.priority_level === '주의 관찰').length, className: 'priority-watch' },
        { label: '정상 유지', count: rows.filter((row) => row.priority_level === '정상 유지').length, className: 'priority-stable' }
    ].filter((item) => item.count > 0);
}

function summarizeBrandStatuses(rows) {
    const counts = new Map();

    rows.forEach((row) => {
        const key = row.brand_score_status || 'unknown';
        counts.set(key, (counts.get(key) ?? 0) + 1);
    });

    return Array.from(counts.entries()).map(([status, count]) => ({
        status,
        label: `${translateBrandStatusCompact(status)} ${formatNumber(count)}개`,
        className: brandStatusClass(status)
    }));
}

function buildSegmentCells(rows) {
    const { queueMap, contributorMap, roleMap, revenueMap } = buildProductMaps();
    const cells = new Map();

    rows.forEach((row) => {
        const revenueSegment = normalizeRevenueSegment(row.revenue_segment);
        const roleTaxonomy = textOrFallback(row.role_taxonomy, '기타');
        const cellKey = `${revenueSegment}::${roleTaxonomy}`;
        const queueRow = queueMap.get(row.product_id) ?? {};
        const brandRow = contributorMap.get(row.product_id) ?? {};
        const roleRow = roleMap.get(row.product_id) ?? {};
        const revenueRow = revenueMap.get(row.product_id) ?? {};

        if (!cells.has(cellKey)) {
            cells.set(cellKey, []);
        }

        cells.get(cellKey).push({
            ...row,
            revenue_segment: revenueSegment,
            role_taxonomy: roleTaxonomy,
            rank: toNumber(queueRow.rank) ?? Number.POSITIVE_INFINITY,
            priority_level: queueRow.priority_level || row.priority_level,
            brand_score_status: brandRow.brand_score_status || row.brand_score_status || '',
            role_score: roleRow.role_score || '',
            revenue_change_rate: revenueRow.revenue_delta_rate || queueRow.revenue_change_rate || ''
        });
    });

    return cells;
}

function renderSummaryCards() {
    const rows = getRowsForWindow(getViewModel('vm_queue_summary'));
    const brandDirectionRow = getBrandDirectionRow();
    const brandLevelRow = getRowsForWindow(getViewModel('vm_brand_score_panel'))[0] ?? {};
    const contributorRows = getRowsForWindow(getViewModel('vm_brand_score_product_contributors'));

    if (!rows.length) {
        return '<div class="empty-state">표시할 우선순위 집계가 없습니다.</div>';
    }

    const brandRevenueCurrent = Number(rows[0]?.brand_revenue_current ?? 0);
    const brandRevenuePrevious = Number(rows[0]?.brand_revenue_previous ?? 0);
    const brandRevenueDeltaRate = rows[0]?.brand_revenue_delta_rate === '' || rows[0]?.brand_revenue_delta_rate == null
        ? null
        : Number(rows[0]?.brand_revenue_delta_rate);

    return `
        <div class="summary-grid">
            ${rows.map((row) => `
                <article class="summary-card">
                    <div class="section-kicker">우선순위</div>
                    <strong class="${priorityClass(row.priority_level)}">${escapeHtml(row.product_count)}</strong>
                    <div>${escapeHtml(row.priority_level)}</div>
                </article>
            `).join('')}
            <article class="summary-card">
                <div class="section-kicker">브랜드 매출</div>
                <strong>${escapeHtml(brandRevenueCurrent.toLocaleString('ko-KR'))}</strong>
                <div>${escapeHtml(getWindowLabel(state.windowKey))} 합계</div>
                <small>직전 기간 ${escapeHtml(brandRevenuePrevious.toLocaleString('ko-KR'))}</small>
                <small>${escapeHtml(brandRevenueDeltaRate == null ? '비교 보류' : `${formatPercent(brandRevenueDeltaRate)}`)}</small>
            </article>
            ${renderBrandDirectionCard(brandDirectionRow, brandLevelRow, contributorRows, { containerClass: 'summary-card' })}
        </div>
    `;
}

function renderPriorityView() {
    const queueRows = getRowsForWindow(getViewModel('vm_priority_queue'));
    const contributorRows = getRowsForWindow(getViewModel('vm_brand_score_product_contributors'));
    const contributorMap = new Map(contributorRows.map((row) => [row.product_id, row]));
    const brandDirectionRow = getBrandDirectionRow();
    const rawMissing = state.bundle?.raw_data_status !== 'real_source_loaded';

    if (!queueRows.length) {
        return `
            <div class="empty-state">
                우선순위 큐가 비어 있습니다.
                ${rawMissing ? '실데이터가 아직 반영되지 않았습니다.' : '현재 기준일에 해당하는 큐 산출물이 없습니다.'}
            </div>
        `;
    }

    return `
        <section class="hero">
            <div class="hero-heading">
                <div>
                    <div class="section-kicker">첫 화면</div>
                    <h3 class="section-title">상품 우선순위 큐</h3>
                </div>
                <p class="muted">${escapeHtml(getWindowLead(state.windowKey))}</p>
            </div>
        </section>
        ${renderSummaryCards()}
        <section class="queue-list">
            ${queueRows.map((row) => `
                <article class="queue-row" data-product-row="${escapeHtml(row.product_id)}">
                    <div class="queue-row-main">
                        <div class="queue-row-header">
                            <div class="queue-row-title">
                                <div class="queue-row-rank">
                                    <span class="section-kicker">순위 ${escapeHtml(row.rank)}</span>
                                    <span class="queue-row-id">${escapeHtml(row.product_id || '')}</span>
                                </div>
                                ${renderProductIdentity(row, { size: 'small' })}
                            </div>
                            <div class="queue-row-badges">
                                <div class="pill ${priorityClass(row.priority_level)}">${escapeHtml(row.priority_level)}</div>
                                ${renderStatusChip(translateBrandStatusCompact(row.brand_score_status), brandStatusClass(row.brand_score_status))}
                            </div>
                        </div>
                        <div class="reason-stack">
                            <div class="reason-item">
                                <strong>매출 흐름</strong>
                                <div>${escapeHtml(sanitizeOperatingCopy(row.revenue_reason))}</div>
                            </div>
                            <div class="reason-item">
                                <strong>역할 분류</strong>
                                <div>${escapeHtml(sanitizeOperatingCopy(row.role_reason))}</div>
                            </div>
                            <div class="reason-item">
                                <strong>브랜드 방향</strong>
                                <div>${escapeHtml(buildQueueBrandDirectionReason(brandDirectionRow, contributorMap.get(row.product_id) ?? {}))}</div>
                            </div>
                        </div>
                    </div>
                    <div class="queue-row-meta">
                        <small>역할 분류: ${escapeHtml(textOrFallback(row.role_taxonomy))}</small>
                        <small>매출 반영일: ${escapeHtml(textOrFallback(row.revenue_freshness_max_date))}</small>
                        <small>역할 반영일: ${escapeHtml(textOrFallback(row.role_freshness_max_date))}</small>
                    </div>
                </article>
            `).join('')}
        </section>
    `;
}

function renderSegmentsView() {
    const structureRows = getRowsForWindow(getViewModel('vm_structure_map_cells'));
    const rows = structureRows.length ? structureRows : getRowsForWindow(getViewModel('vm_segment_map'));

    if (!rows.length) {
        return '<div class="empty-state">구조 맵 데이터가 없습니다.</div>';
    }

    const cells = structureRows.length
        ? new Map(rows.map((row) => [`${normalizeRevenueSegment(row.revenue_segment)}::${textOrFallback(row.role_taxonomy, '관측 없음')}`, row]))
        : buildSegmentCells(rows);
    const revenueSegments = orderValues(rows.map((row) => normalizeRevenueSegment(row.revenue_segment)), [...REVENUE_SEGMENT_ORDER, '비교 불가']);
    const roleTaxonomies = orderValues(rows.map((row) => textOrFallback(row.role_taxonomy, '관측 없음')), ROLE_ORDER);

    return `
        <section class="hero">
            <div class="hero-heading">
                <div>
                    <div class="section-kicker">구조 맵</div>
                    <h3 class="section-title">${escapeHtml(`${getWindowLabel(state.windowKey)} 매출 변화 x 역할 분류`)}</h3>
                </div>
                <p class="muted">어느 역할군에서 매출 감소와 즉시 확인 항목이 몰리는지 ${escapeHtml(getWindowLabel(state.windowKey))} 기준으로 봅니다.</p>
            </div>
        </section>
        <section class="panel matrix-shell">
            <div class="matrix-scroll">
                <div class="matrix-board" style="grid-template-columns: 150px repeat(${revenueSegments.length}, minmax(230px, 1fr));">
                    <div class="matrix-header matrix-corner">
                        <span class="section-kicker">행</span>
                        <strong>역할 분류</strong>
                    </div>
                    ${revenueSegments.map((segment) => `
                        <div class="matrix-header matrix-column-label">
                            <span class="section-kicker">열</span>
                            <strong>${escapeHtml(segment)}</strong>
                        </div>
                    `).join('')}
                    ${roleTaxonomies.map((roleTaxonomy) => `
                        <div class="matrix-row-label">
                            <span class="section-kicker">역할</span>
                            <strong>${escapeHtml(roleTaxonomy)}</strong>
                        </div>
                        ${revenueSegments.map((segment) => {
                            const cellRecord = cells.get(`${segment}::${roleTaxonomy}`);
                            const cellRows = Array.isArray(cellRecord) ? cellRecord : [];
                            const prioritySummary = structureRows.length
                                ? [
                                    { label: '즉시 확인', count: toNumber(cellRecord?.immediate_count) ?? 0, className: 'priority-immediate' },
                                    { label: '주의 관찰', count: toNumber(cellRecord?.watch_count) ?? 0, className: 'priority-watch' },
                                    { label: '정상 유지', count: toNumber(cellRecord?.stable_count) ?? 0, className: 'priority-stable' }
                                ].filter((item) => item.count > 0)
                                : summarizePriorityCounts(cellRows);
                            const brandSummary = structureRows.length
                                ? [
                                    { status: 'limited', label: `${translateBrandStatusCompact('limited')} ${formatNumber(cellRecord?.brand_limited_count, '0')}개`, className: brandStatusClass('limited') },
                                    { status: 'provisional', label: `${translateBrandStatusCompact('provisional')} ${formatNumber(cellRecord?.brand_provisional_count, '0')}개`, className: brandStatusClass('provisional') },
                                    { status: 'unavailable', label: `${translateBrandStatusCompact('unavailable')} ${formatNumber(cellRecord?.brand_unavailable_count, '0')}개`, className: brandStatusClass('unavailable') }
                                ].filter((item) => !item.label.includes('0개'))
                                : summarizeBrandStatuses(cellRows);
                            const topProducts = structureRows.length
                                ? [1, 2]
                                    .map((index) => ({
                                        product_id: cellRecord?.[`top_product_${index}_id`] || '',
                                        product_name: cellRecord?.[`top_product_${index}_name`] || '',
                                        product_image_url: cellRecord?.[`top_product_${index}_image_url`] || '',
                                        priority_level: cellRecord?.[`top_product_${index}_priority`] || ''
                                    }))
                                    .filter((product) => product.product_id || product.product_name)
                                : cellRows.slice(0, 2);
                            const productCount = structureRows.length ? (toNumber(cellRecord?.product_count) ?? 0) : cellRows.length;
                            const attentionCount = structureRows.length
                                ? ((toNumber(cellRecord?.immediate_count) ?? 0) + (toNumber(cellRecord?.watch_count) ?? 0))
                                : cellRows.filter((row) => row.priority_level !== '정상 유지').length;
                            const overflowCount = Math.max(0, productCount - topProducts.length);
                            const helperNote = getRoleTaxonomyHelper(roleTaxonomy);
                            const brandSummaryVisible = [...brandSummary]
                                .sort((left, right) => {
                                    const priority = { unavailable: 0, limited: 1, provisional: 2, 'near-core': 3, available: 4 };
                                    return (priority[left.status] ?? 9) - (priority[right.status] ?? 9);
                                })
                                .slice(0, 2);

                            if (!productCount) {
                                return `
                                    <div class="matrix-cell matrix-cell-empty">
                                        <div class="matrix-empty">해당 상품이 없습니다.</div>
                                    </div>
                                `;
                            }

                            return `
                                <div class="matrix-cell">
                                    <div class="matrix-cell-top">
                                        <strong class="matrix-count">${escapeHtml(`${formatNumber(productCount)}개 상품`)}</strong>
                                        <span class="matrix-meta">${escapeHtml(`${formatNumber(attentionCount, '0')}건 확인 필요`)}</span>
                                    </div>
                                    <p class="matrix-helper-note">${escapeHtml(helperNote)}</p>
                                    <div class="matrix-chip-row">
                                        ${prioritySummary.map((item) => `<span class="pill ${item.className}">${escapeHtml(`${item.label} ${item.count}`)}</span>`).join('')}
                                    </div>
                                    <div class="matrix-product-list">
                                        ${topProducts.map((product) => `
                                            <div class="matrix-product-item">
                                                <div class="matrix-product-row">
                                                    ${renderProductThumb(product, { size: 'small' })}
                                                    <div class="matrix-product-copy">
                                                        <strong>${escapeHtml(product.product_name || product.product_id)}</strong>
                                                        <span>${escapeHtml(structureRows.length ? `${textOrFallback(product.priority_level, '상태 확인')}` : `${product.priority_level} · 기여 ${formatDecimal(product.role_score, { digits: 2, fallback: '-' })} · 매출 ${formatPercent(product.revenue_change_rate, { fallback: '보류' })}`)}</span>
                                                    </div>
                                                </div>
                                            </div>
                                        `).join('')}
                                        ${overflowCount > 0 ? `<div class="matrix-product-more">외 ${escapeHtml(formatNumber(overflowCount, '0'))}개</div>` : ''}
                                    </div>
                                    <div class="matrix-chip-row is-secondary">
                                        ${brandSummaryVisible.map((item) => renderStatusChip(item.label, item.className)).join('')}
                                    </div>
                                </div>
                            `;
                        }).join('')}
                    `).join('')}
                </div>
            </div>
        </section>
    `;
}

function renderDetailOverviewCards(selectedQueueRow, roleRow, revenueRow, contributorRows, brandLevelRow, brandDirectionRow) {
    const contributionNote = [
        textOrFallback(selectedQueueRow.role_taxonomy || roleRow.role_taxonomy),
        `기여 점수 ${formatDecimal(roleRow.role_score, { digits: 2, fallback: '-' })}`
    ].join(' · ');

    return `
        <div class="detail-overview-grid">
            <article class="overview-card">
                <div class="section-kicker">우선순위</div>
                <strong class="overview-value ${priorityClass(selectedQueueRow.priority_level)}">${escapeHtml(selectedQueueRow.priority_level)}</strong>
                <p class="overview-note">${escapeHtml(sanitizeOperatingCopy(selectedQueueRow.revenue_reason))}</p>
            </article>
            <article class="overview-card">
                <div class="section-kicker">매출 흐름</div>
                <strong class="overview-value">${escapeHtml(formatPercent(revenueRow.revenue_delta_rate ?? selectedQueueRow.revenue_change_rate, { fallback: '보류' }))}</strong>
                <p class="overview-note">${escapeHtml(sanitizeOperatingCopy(revenueRow.revenue_compare_note || selectedQueueRow.revenue_reason))}</p>
            </article>
            <article class="overview-card">
                <div class="section-kicker">상품 기여</div>
                <strong class="overview-value">${escapeHtml(textOrFallback(roleRow.primary_axis_label || selectedQueueRow.role_taxonomy))}</strong>
                <p class="overview-note">${escapeHtml(contributionNote)}</p>
            </article>
            ${renderBrandDirectionCard(brandDirectionRow, brandLevelRow, contributorRows, {
                containerClass: 'overview-card',
                detailed: true,
                showRestrictionReason: true
            })}
        </div>
    `;
}

function renderDetailSectionRows(rows) {
    const sectionOrder = ['헤더', '매출', '역할 분류', '근거', '브랜드 점수', 'Revenue', 'Role', 'Brand Score'];
    const groups = sectionOrder
        .map((section) => ({
            section,
            rows: rows.filter((row) => row.section === section)
        }))
        .filter((group) => group.rows.length > 0);

    return `
        <div class="detail-section-grid">
            ${groups.map((group) => `
                <article class="detail-section-card">
                    <div class="section-kicker">${escapeHtml(translateDetailSection(group.section))}</div>
                    <dl class="detail-definition-list">
                        ${group.rows.map((row) => `
                            <div class="definition-pair">
                                <dt>${escapeHtml(translateDetailLabel(group.section, row.label))}</dt>
                                <dd>${escapeHtml(translateDetailValue(group.section, row.label, row.value))}</dd>
                                <small>${escapeHtml(
                                    group.section === 'Brand Score' || group.section === '브랜드 점수'
                                        ? sanitizeOperatingCopy(row.note)
                                        : sanitizeOperatingCopy(row.note)
                                )}</small>
                            </div>
                        `).join('')}
                    </dl>
                </article>
            `).join('')}
        </div>
    `;
}

function renderDetailView() {
    const queueRows = getRowsForWindow(getViewModel('vm_priority_queue'));
    const detailRows = getRowsForWindow(getViewModel('vm_product_detail'));

    if (!queueRows.length) {
        return '<div class="empty-state">상세 보기 데이터가 없습니다.</div>';
    }

    const contributorRows = getRowsForWindow(getViewModel('vm_brand_score_product_contributors'));
    const { roleMap, contributorMap, brandLevelRow, brandDirectionRow, revenueMap } = buildProductMaps();
    const selectedProductId = state.selectedProductId || queueRows[0].product_id;
    const selectedQueueRow = queueRows.find((row) => row.product_id === selectedProductId) ?? queueRows[0];
    state.selectedProductId = selectedQueueRow.product_id;
    const selectedRows = detailRows.filter((row) => row.product_id === selectedQueueRow.product_id);
    const roleRow = roleMap.get(selectedQueueRow.product_id) ?? {};
    const contributorRow = contributorMap.get(selectedQueueRow.product_id) ?? {};
    const revenueRow = revenueMap.get(selectedQueueRow.product_id) ?? {};

    return `
        <section class="detail-layout">
            <aside class="detail-card detail-sidebar">
                <div class="section-kicker">대상 선택</div>
                <h3>상품 목록</h3>
                <div class="product-picker">
                    ${queueRows.map((row) => `
                        <button
                            class="${row.product_id === selectedQueueRow.product_id ? 'is-selected' : ''}"
                            data-product-pick="${escapeHtml(row.product_id)}"
                        >
                            <span class="picker-product">
                                ${renderProductThumb(row, { size: 'small' })}
                                <span class="picker-product-copy">
                                    <strong>${escapeHtml(row.product_name || row.product_id)}</strong>
                                    <span class="picker-meta-row">
                                        <span class="picker-product-id">${escapeHtml(row.product_id || '')}</span>
                                        <span class="${priorityClass(row.priority_level)}">${escapeHtml(row.priority_level)}</span>
                                    </span>
                                </span>
                            </span>
                        </button>
                    `).join('')}
                </div>
            </aside>
            <section class="detail-card detail-main">
                <div class="section-kicker">상세 보기</div>
                <div class="detail-header-product">
                    ${renderProductThumb(selectedQueueRow, { size: 'medium' })}
                    <div class="detail-header-copy">
                        <h3>${escapeHtml(selectedQueueRow.product_name || selectedQueueRow.product_id)}</h3>
                        <p class="muted">${escapeHtml(selectedQueueRow.product_id)}</p>
                        <div class="inline-chip-row">
                            <span class="pill ${priorityClass(selectedQueueRow.priority_level)}">${escapeHtml(selectedQueueRow.priority_level)}</span>
                            <span class="pill status-neutral">${escapeHtml(getWindowLabel(state.windowKey))}</span>
                            ${renderStatusChip(textOrFallback(selectedQueueRow.role_taxonomy), 'status-neutral')}
                            ${renderStatusChip(translateBrandStatusCompact(contributorRow.contribution_status || selectedQueueRow.brand_score_status), brandStatusClass(contributorRow.contribution_status || selectedQueueRow.brand_score_status))}
                        </div>
                    </div>
                </div>
                ${renderDetailOverviewCards(selectedQueueRow, roleRow, revenueRow, contributorRows, brandLevelRow, brandDirectionRow)}
                ${renderDetailSectionRows(selectedRows)}
            </section>
        </section>
    `;
}

function renderDefinitionsView() {
    const rows = getRowsForWindow(getViewModel('vm_definition_rules'));

    if (!rows.length) {
        return '<div class="empty-state">운영 기준 데이터가 없습니다.</div>';
    }

    const groupOrder = ['매출 비교', '역할 분류', '브랜드 점수', '데이터 상태', '상품 이미지', 'Revenue', 'Role', 'Brand Score'];
    const groups = groupOrder
        .map((group) => ({
            group,
            rows: rows.filter((row) => row.rule_group === group)
        }))
        .filter((item) => item.rows.length > 0);

    return `
        <section class="hero">
            <div class="hero-heading">
                <div>
                    <div class="section-kicker">운영 기준</div>
                    <h3 class="section-title">화면에서 쓰는 기준과 안내</h3>
                </div>
                <p class="muted">${escapeHtml(`${getWindowLabel(state.windowKey)} 기준으로 어떤 신호를 우선 보는지 정리했습니다.`)}</p>
            </div>
        </section>
        <section class="definition-groups">
            ${groups.map((group) => `
                <article class="detail-card definition-group">
                    <div class="section-kicker">${escapeHtml(translateRuleGroup(group.group))}</div>
                    <h3>${escapeHtml(translateRuleGroup(group.group))}</h3>
                    <div class="definition-rule-grid">
                        ${group.rows.map((row) => `
                            <article class="definition-rule">
                                <div class="definition-status">
                                    <strong>${escapeHtml(translateRuleName(row.rule_name))}</strong>
                                    ${renderStatusChip(translateRuleStatus(row.status_label), row.status_label === '정상' ? 'status-ready' : 'status-warning')}
                                </div>
                                <p>${escapeHtml(sanitizeOperatingCopy(row.rule_definition))}</p>
                            </article>
                        `).join('')}
                    </div>
                </article>
            `).join('')}
        </section>
    `;
}

function renderOverviewCard(title, value, note, className = '') {
    return `
        <article class="overview-card ${className}">
            <div class="section-kicker">${escapeHtml(title)}</div>
            <strong class="overview-value">${escapeHtml(value)}</strong>
            <p class="overview-note">${escapeHtml(note)}</p>
        </article>
    `;
}

function renderHealthView() {
    const overviewRows = getRowsForWindow(getViewModel('vm_data_health_overview'));
    const healthRows = getRowsForWindow(getViewModel('vm_data_health_detail').length ? getViewModel('vm_data_health_detail') : getViewModel('vm_data_health'));
    const brandDirectionRow = getBrandDirectionRow();
    const queueRows = getRowsForWindow(getViewModel('vm_priority_queue'));
    const rawManifestRows = getQaRows('raw_manifest');
    const productImageManifest = rawManifestRows.find((row) => row.dataset_key === 'products') ?? {};
    const imageSummary = productImageManifest.data_provenance === 'rosetta_direct'
        ? '상품 이미지 반영 완료'
        : '상품 이미지 일부를 다시 확인해야 합니다';
    const queueState = state.bundle?.raw_data_status === 'real_source_loaded'
        ? (queueRows.length ? '운영 가능' : '산출물 확인 필요')
        : '데이터 반영 대기';
    const trustSummary = summarizeHealthTrust(overviewRows);
    const cautionSummary = summarizeHealthCaution(brandDirectionRow);
    const brandLevelRow = getRowsForWindow(getViewModel('vm_brand_score_panel'))[0] ?? {};
    const contributorRows = getRowsForWindow(getViewModel('vm_brand_score_product_contributors'));
    const detailRows = healthRows.filter((row) => toText(row.source_label || row.source_key));
    const limitedRows = overviewRows.filter((row) => {
        if (row.area_key === 'brand_score') {
            return isBrandRestrictedStatus(row.status_label);
        }
        if (row.area_key === 'product_reference') {
            return imageSummary !== '상품 이미지 반영 완료';
        }
        return row.status_label && row.status_label !== '정상';
    });

    return `
        <section class="hero">
            <div class="hero-heading">
                <div>
                    <div class="section-kicker">운영 현황</div>
                    <h3 class="section-title">오늘 바로 볼 운영 개요</h3>
                </div>
                <p class="muted">${escapeHtml(getWindowLead(state.windowKey))}</p>
            </div>
        </section>
        <section class="overview-grid">
            ${renderOverviewCard('지금 믿고 볼 수 있는 것', trustSummary.value, trustSummary.note, queueState === '운영 가능' ? 'status-ready' : 'status-warning')}
            ${renderBrandDirectionCard(brandDirectionRow, brandLevelRow, contributorRows, {
                containerClass: 'overview-card',
                detailed: true,
                showRestrictionReason: true
            })}
            ${renderOverviewCard('해석 시 주의할 것', cautionSummary.value, cautionSummary.note, 'status-muted')}
        </section>
        <details class="detail-disclosure">
            <summary>세부 근거 보기</summary>
            <div class="detail-disclosure-body">
                <table class="detail-disclosure-table">
                    <thead>
                        <tr>
                            <th>데이터 항목</th>
                            <th>최근일</th>
                            <th>운영 상태</th>
                            <th>비교 범위</th>
                            <th>주의 메모</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${detailRows.map((row) => `
                            <tr>
                                <td>${escapeHtml(textOrFallback(row.source_label, row.source_key))}</td>
                                <td>${escapeHtml(textOrFallback(row.max_date))}</td>
                                <td>${renderStatusChip(textOrFallback(row.data_state), coverageStateClass(row.data_state))}</td>
                                <td>${escapeHtml(sanitizeOperatingCopy(row.coverage_state))}</td>
                                <td>${escapeHtml(sanitizeOperatingCopy(row.coverage_note || row.coverage_state))}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        </details>
        <details class="detail-disclosure">
            <summary>제한 사유 보기</summary>
            <div class="detail-disclosure-body">
                <div class="health-note-list">
                    <article class="health-note-item">
                        <strong>운영 준비도</strong>
                        <p>${escapeHtml(`현재 우선순위 큐 ${formatNumber(queueRows.length, '0')}건을 기준으로 확인할 수 있습니다.`)}</p>
                    </article>
                    <article class="health-note-item">
                        <strong>상품 기준 정보</strong>
                        <p>${escapeHtml(imageSummary)}</p>
                    </article>
                    ${limitedRows.map((row) => `
                        <article class="health-note-item">
                            <strong>${escapeHtml(sanitizeOperatingCopy(row.area_title))}</strong>
                            <p>${escapeHtml(
                                row.area_key === 'brand_score'
                                    ? normalizeBrandReason(row.note)
                                    : row.area_key === 'product_reference'
                                        ? imageSummary
                                        : sanitizeOperatingCopy(row.note)
                            )}</p>
                        </article>
                    `).join('')}
                </div>
            </div>
        </details>
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
    document.querySelector('#period-note').textContent = getWindowLead(state.windowKey);
    document.querySelector('#data-state').textContent = state.bundle.raw_data_status === 'real_source_loaded'
        ? (getRowsForWindow(getViewModel('vm_priority_queue')).length ? '운영 가능' : '산출물 없음')
        : '데이터 확인 필요';
    document.querySelectorAll('[data-view]').forEach((button) => {
        button.classList.toggle('is-active', button.dataset.view === state.view);
    });
    document.querySelectorAll('[data-window-key]').forEach((button) => {
        button.classList.toggle('is-active', button.dataset.windowKey === state.windowKey);
    });

    const rendererMap = {
        priority: renderPriorityView,
        segments: renderSegmentsView,
        detail: renderDetailView,
        definitions: renderDefinitionsView,
        health: renderHealthView
    };

    root.innerHTML = (rendererMap[state.view] ?? renderPriorityView)();
    bindImageFallbacks(root);

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

async function fetchLayerFileText(layer, candidates) {
    const filenames = Array.isArray(candidates) ? candidates : [candidates];
    let lastError = null;

    for (const filename of filenames) {
        try {
            const text = await fetchTextOrThrow(resolveArtifactUrl(layer, filename));
            return { filename, text };
        } catch (error) {
            lastError = error;
        }
    }

    throw lastError ?? new Error('파일을 불러오지 못했습니다.');
}

async function loadLayerCsv(layer, files) {
    const entries = await Promise.all(
        Object.entries(files).map(async ([key, candidates]) => {
            const { text } = await fetchLayerFileText(layer, candidates);
            if ((Array.isArray(candidates) ? candidates[0] : candidates).endsWith('.md')) {
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
        // 정적 서빙으로 자연스럽게 내려간다.
    }

    const bundle = await loadBundleFromArtifacts();
    bundle.raw_data_status = deriveRawDataStatus(bundle);
    return bundle;
}

async function loadBundle() {
    state.windowKey = readWindowKeyFromUrl();
    state.view = readViewFromUrl();
    state.bundle = await loadBundleWithFallback();
    syncStateToUrl();
    render();
}

function bindImageFallbacks(root) {
    root.querySelectorAll('[data-product-thumb]').forEach((thumb) => {
        const image = thumb.querySelector('[data-product-image]');
        if (!image) {
            return;
        }

        const altText = image.getAttribute('alt') || '상품 이미지';
        const showLoadedState = () => {
            thumb.classList.remove('is-placeholder');
            thumb.classList.add('is-loaded');
            thumb.removeAttribute('role');
            thumb.removeAttribute('aria-label');
        };
        const showFallbackState = () => {
            thumb.classList.remove('is-loaded');
            thumb.classList.add('is-placeholder');
            thumb.setAttribute('role', 'img');
            thumb.setAttribute('aria-label', altText);
            image.removeAttribute('src');
        };

        if (image.complete) {
            if (image.naturalWidth > 0) {
                showLoadedState();
            } else {
                showFallbackState();
            }
            return;
        }

        image.addEventListener('load', showLoadedState, { once: true });
        image.addEventListener('error', showFallbackState, { once: true });
    });
}

function bindNav() {
    document.querySelectorAll('[data-view]').forEach((button) => {
        button.addEventListener('click', () => {
            document.querySelectorAll('[data-view]').forEach((item) => item.classList.remove('is-active'));
            button.classList.add('is-active');
            state.view = button.dataset.view;
            syncStateToUrl();
            render();
        });
    });
}

function bindWindowToggle() {
    document.querySelectorAll('[data-window-key]').forEach((button) => {
        button.addEventListener('click', () => {
            state.windowKey = button.dataset.windowKey;
            syncStateToUrl();
            render();
        });
    });
}

bindNav();
bindWindowToggle();
loadBundle().catch((error) => {
    document.querySelector('#app-root').innerHTML = `<div class="empty-state">로딩 실패: ${escapeHtml(error.message)}</div>`;
});
