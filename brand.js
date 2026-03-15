const BRAND_ROLE_META = {
    growth: { label: '성장 엔진', color: '#2563eb', summary: '유입과 확장을 함께 끌어주는 중심 제품이에요.' },
    acquisition: { label: '유입 확보 제품', color: '#0f766e', summary: '첫 진입은 강하지만 다음 확장 연결은 더 보강할 여지가 있어요.' },
    expansion: { label: '확장 제품', color: '#8b5cf6', summary: '확장과 반복 구매를 살려 주는 후속 연결 제품이에요.' },
    peripheral: { label: '주변 제품', color: '#94a3b8', summary: '현재 구조에서는 주변 역할에 머무는 제품이에요.' }
};

const BRAND_COMPONENT_GROUPS = {
    health: '구조 건강도 해석',
    impact: '구매 전환 해석'
};

const BRAND_COMPONENT_META = {
    entryDiversity: {
        label: '유입 다양성',
        chartLabel: '첫 구매 분산',
        badge: '측정값',
        shortDescription: '첫 구매가 일부 제품에 몰리지 않는지 봐요.',
        description: '신규 유입이 일부 제품에만 과도하게 몰리지 않는지 봐요.'
    },
    expansionLadder: {
        label: '확장 사다리',
        chartLabel: '다음 구매 연결',
        badge: '해석용 추정',
        shortDescription: '첫 구매 뒤 다음 구매 연결을 봐요.',
        description: '첫 진입 이후 다음 구매로 이어지는 사다리 두께를 봐요.'
    },
    loopStability: {
        label: '반복 루프 안정성',
        chartLabel: '반복 구매 습관',
        badge: '측정값',
        shortDescription: '반복 구매가 습관처럼 이어지는지 봐요.',
        description: '반복 구매와 루프 패턴이 얼마나 안정적으로 유지되는지 봐요.'
    },
    roleBalance: {
        label: '제품 역할 균형',
        chartLabel: '역할 균형',
        badge: '해석용 추정',
        shortDescription: '제품 역할 분포가 치우치지 않는지 봐요.',
        description: '성장 엔진, 유입 확보, 확장, 주변 역할의 분포를 봐요.'
    },
    coreInfluence: {
        label: '핵심 제품 힘',
        chartLabel: '핵심 제품 힘',
        badge: '해석용 추정',
        shortDescription: '핵심 제품이 흐름 중심을 잡는지 봐요.',
        description: '상위 핵심 제품이 전체 구조를 얼마나 끌어주는지 봐요.'
    },
    graphStrength: {
        label: '수요 연결 강도',
        chartLabel: '제품 간 연결',
        badge: '해석용 추정',
        shortDescription: '제품 사이 연결이 얼마나 살아 있는지 봐요.',
        description: '제품 사이 연결과 대표 흐름의 밀도를 봐요.'
    },
    returnPower: {
        label: '반복 전환 힘',
        chartLabel: '반복 구매 힘',
        badge: '측정값',
        shortDescription: '반복 구매가 실제 구매로 이어지는 힘을 봐요.',
        description: '반복 구매 깊이와 최근 운영 구간의 전환 힘을 함께 봐요.'
    },
    hubConcentration: {
        label: '허브 집중도',
        chartLabel: '허브 쏠림',
        badge: '해석용 추정',
        shortDescription: '구매가 소수 허브에 몰리는지 봐요.',
        description: '소수 허브 제품이 흐름을 어디까지 모으는지 봐요.'
    }
};

const BRAND_INTERPRETATION_TEXT = {
    health: {
        entryDiversity: '일부 제품 쏠림이 있어 구조가 한쪽으로 기울고 있어요.',
        expansionLadder: '첫 구매 뒤 다음 구매로 이어지는 힘이 약해요.',
        loopStability: '다시 사는 흐름은 있지만 아직 습관처럼 굳지는 않았어요.',
        roleBalance: '제품 역할이 한쪽에 몰려 있어 균형이 부족해요.'
    },
    impact: {
        coreInfluence: '핵심 제품은 보이지만 구매 전환을 끌어당기는 힘은 아직 약해요.',
        graphStrength: '제품 사이 연결이 얇아 구매 전환이 넓게 퍼지지 않아요.',
        returnPower: '반복 구매 힘이 약해서 구매 전환으로 충분히 이어지지 않아요.',
        hubConcentration: '소수 제품이 끌고는 있지만 그만큼 의존도도 높아요.'
    }
};

const BRAND_TIMELINE_WINDOW_ORDER = [7, 30, 90, 365];
const BRAND_TIMELINE_WINDOW_GUIDE = {
    7: '최근 며칠 사이 흐름이 갑자기 붙거나 꺾였는지 볼 때 좋아요.',
    30: '최근 한 달 운영이 구매 전환으로 이어지고 있는지 볼 때 좋아요.',
    90: '단기 흔들림을 덜 타고, 구매 전환이 중기적으로 좋아지는지 볼 때 좋아요.',
    365: '시즌성까지 포함해 구매 전환이 장기적으로 유지되는지 볼 때 좋아요.'
};
const BRAND_PULSE_LOOKBACK_DAYS = 90;
const BRAND_DIAGNOSTIC_WINDOW_ORDER = [7, 30, 90];
const BRAND_MATRIX_RATIO_MAX = 1.6;

const BRAND_DIAGNOSTIC_META = {
    structure: {
        title: '브랜드 구조 건강도 vs. 브랜드 구매 전환력 (BII)',
        description: '구조 건강도와 최근 브랜드 구매 전환력을 함께 봐요.',
        xAxisLabel: 'BHI (구조 건강도)',
        yAxisLabel: '브랜드 구매 전환력 / 브랜드 구매 전환력 365일',
        quadrants: {
            highHigh: {
                label: '최우수 (Optimal)',
                summary: '구조도 좋고 최근 브랜드 구매 전환력도 함께 잘 작동하고 있어요.',
                tone: 'mint'
            },
            highLow: {
                label: '실행력 저하 (Execution Drop)',
                summary: '구조는 좋은데 최근 운영 난조로 브랜드 구매 전환력이 약해졌어요.',
                tone: 'amber'
            },
            lowHigh: {
                label: '단기 펌핑 (Illusion)',
                summary: '단기 브랜드 구매 전환력은 높지만 구조 리스크 점검이 필요해요.',
                tone: 'orange'
            },
            lowLow: {
                label: '전면 개선 필요 (Rebuild)',
                summary: '구조와 최근 브랜드 구매 전환력이 모두 약해 기초부터 다시 봐야 해요.',
                tone: 'rose'
            }
        }
    },
    revenue: {
        title: 'Revenue vs. 브랜드 구매 전환력 (BII)',
        description: '매출과 브랜드 구매 전환력을 함께 봐요. BII는 매출을 대체하지 않고, 매출의 구조적 기반을 설명해요.',
        xAxisLabel: 'Revenue / Revenue 365일',
        yAxisLabel: '브랜드 구매 전환력 / 브랜드 구매 전환력 365일',
        quadrants: {
            highHigh: {
                label: '건강한 성장 (Healthy Growth)',
                summary: '매출과 구조 기반 브랜드 구매 전환력이 함께 올라가는 이상적인 상태예요.',
                tone: 'mint'
            },
            highLow: {
                label: '구조 리스크 (Danger Zone)',
                summary: '매출은 버티지만 구조 기반 브랜드 구매 전환력은 약해지고 있어요.',
                tone: 'rose'
            },
            lowHigh: {
                label: '장기 개선 신호 (Hidden Momentum)',
                summary: '매출은 정체돼 보여도 내부 브랜드 구매 전환력 기반은 좋아지고 있어요.',
                tone: 'sky'
            },
            lowLow: {
                label: '동반 침체 (Decline)',
                summary: '매출과 브랜드 구매 전환력이 함께 약해지고 있어 근본 점검이 필요해요.',
                tone: 'slate'
            }
        }
    }
};

function brandRenderMetricName(label, abbr) {
    return `${escapeHtml(label)} <span data-preserve-abbr="1">(${escapeHtml(abbr)})</span>`;
}

function brandClamp(value, min = 0, max = 1) {
    return Math.min(max, Math.max(min, value));
}

function brandSafeDivide(num, den, fallback = 0) {
    return den > 0 ? num / den : fallback;
}

function brandPercentile(values, p) {
    const nums = (values || []).filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
    if (!nums.length) return 0;
    if (nums.length === 1) return nums[0];
    const index = Math.min(nums.length - 1, Math.max(0, Math.floor((nums.length - 1) * p)));
    return nums[index];
}

function brandEntropyFromCounts(values) {
    const nums = (values || []).map((value) => Math.max(0, toNumber(value, 0))).filter((value) => value > 0);
    if (nums.length <= 1) return nums.length === 1 ? 0 : 1;
    const total = nums.reduce((sum, value) => sum + value, 0);
    if (total <= 0) return 0;
    const entropy = nums.reduce((sum, value) => {
        const share = value / total;
        return sum - (share * Math.log(share));
    }, 0);
    return brandClamp(entropy / Math.log(nums.length));
}

function brandNormalizeByReference(value, reference) {
    if (!Number.isFinite(value) || reference <= 0) return 0;
    return brandClamp(value / reference);
}

function brandFormatScore(score) {
    return formatNumber(score, 1);
}

function brandFormatDelta(delta) {
    if (!Number.isFinite(delta)) return '최근 4주 변화 데이터가 아직 없어요.';
    if (Math.abs(delta) < 0.05) return '지난 4주와 거의 비슷해요.';
    const changeAmount = `${formatNumber(Math.abs(delta), 1)}포인트`;
    return delta > 0
        ? `지난 4주보다 ${changeAmount} 높아요.`
        : `지난 4주보다 ${changeAmount} 낮아요.`;
}

function brandAverage(values) {
    const nums = (values || []).filter((value) => Number.isFinite(value));
    if (!nums.length) return null;
    return nums.reduce((sum, value) => sum + value, 0) / nums.length;
}

function brandFormatPulseDelta(delta) {
    if (!Number.isFinite(delta)) return '직전 7일 비교 데이터가 아직 부족해요.';
    if (Math.abs(delta) < 0.05) return '최근 7일 평균이 직전 7일과 거의 비슷해요.';
    const changeAmount = `${formatNumber(Math.abs(delta), 1)}포인트`;
    return delta > 0
        ? `최근 7일 평균이 직전 7일보다 ${changeAmount} 높아요.`
        : `최근 7일 평균이 직전 7일보다 ${changeAmount} 낮아요.`;
}

function brandGetPulsePositionKey(values, latestValue) {
    const nums = (values || []).filter((value) => Number.isFinite(value));
    if (!nums.length || !Number.isFinite(latestValue)) return 'mid';
    const min = Math.min(...nums);
    const max = Math.max(...nums);
    const range = max - min;
    if (range < 0.2) return 'mid';
    const relative = (latestValue - min) / range;
    if (relative >= 0.67) return 'high';
    if (relative <= 0.33) return 'low';
    return 'mid';
}

function brandBuildPulseInterpretation(delta, slope, positionKey) {
    if (Number.isFinite(delta) && delta <= -4 && slope <= -1) {
        return '최근 1주 흐름이 빠르게 약해지고 있어요.';
    }
    if (Number.isFinite(delta) && delta <= -2 && positionKey === 'low') {
        return '최근 1주 흐름이 약해졌고, 지금도 낮은 구간이에요.';
    }
    if (slope >= 1 && positionKey === 'low') {
        return '최근 며칠은 반등 중이지만 아직 낮은 구간이에요.';
    }
    if (Number.isFinite(delta) && delta >= 4 && slope >= 1) {
        return '최근 1주 흐름이 분명히 살아나고 있어요.';
    }
    if (Number.isFinite(delta) && delta >= 2 && positionKey === 'high') {
        return '최근 1주 흐름이 좋아졌고, 지금도 높은 구간이에요.';
    }
    if (Math.abs(slope) <= 0.6 && (!Number.isFinite(delta) || Math.abs(delta) <= 1.2)) {
        return '일별 흐름은 비교적 안정적이지만 크게 강해지지는 않았어요.';
    }
    if (slope <= -1 && positionKey === 'high') {
        return '높은 구간이었지만 최근 며칠은 힘이 빠지고 있어요.';
    }
    return '일별 흐름은 오르내리지만 아직 방향이 또렷하지 않아요.';
}

function brandGetScoreBand(score) {
    if (score >= 75) return '양호';
    if (score >= 55) return '보통';
    return '주의';
}

function brandGetRoleKey(entry, expansion, centerEntry, centerExpansion) {
    const highEntry = toNumber(entry, 0) >= toNumber(centerEntry, 0);
    const highExpansion = toNumber(expansion, 0) >= toNumber(centerExpansion, 0);
    if (highEntry && highExpansion) return 'growth';
    if (highEntry) return 'acquisition';
    if (highExpansion) return 'expansion';
    return 'peripheral';
}

function brandBuildEdgeRows() {
    if ((AppState.data.insightDemandGraphEdges || []).length) {
        return AppState.data.insightDemandGraphEdges.map((row) => ({
            source_product_id: String(row.source_product_id || '').trim(),
            target_product_id: String(row.target_product_id || '').trim(),
            transition_customer_cnt: toNumber(row.transition_customer_cnt, 0),
            transition_rate: toNumber(row.transition_rate, 0),
            avg_days_to_target: toNumber(row.avg_days_to_transition, NaN),
            source_product_name: row.source_product_name || getProductName(row.source_product_id),
            target_product_name: row.target_product_name || getProductName(row.target_product_id)
        }));
    }
    return (AppState.data.anchorTransition || []).map((row) => ({
        source_product_id: String(row.entry_product_id || row.aa_product_id || '').trim(),
        target_product_id: String(row.expansion_product_id || row.pca_product_id || '').trim(),
        transition_customer_cnt: toNumber(row.transition_customer_cnt, 0),
        transition_rate: toNumber(row.transition_rate, 0),
        avg_days_to_target: toNumber(firstDefinedValue(row.avg_days_to_expansion, row.avg_days_to_pca), NaN),
        source_product_name: getProductName(row.entry_product_id || row.aa_product_id),
        target_product_name: getProductName(row.expansion_product_id || row.pca_product_id)
    })).filter((row) => row.source_product_id && row.target_product_id && row.source_product_id !== row.target_product_id);
}

function brandBuildMaps(edgeRows) {
    const cartById = new Map((AppState.data.cartAnchor || []).map((row) => [String(row.product_id), row]));
    const demandById = new Map((AppState.data.productDemandGravity || []).map((row) => [String(row.product_id), row]));
    const nodeById = new Map((AppState.data.insightDemandGraphNodes || []).map((row) => [String(row.product_id), row]));
    const inboundCounts = new Map();
    const outboundCounts = new Map();
    const inboundBreadth = new Map();
    const outboundBreadth = new Map();
    const inboundSources = new Map();
    const outboundTargets = new Map();

    (edgeRows || []).forEach((row) => {
        const source = String(row.source_product_id || '').trim();
        const target = String(row.target_product_id || '').trim();
        const customers = Math.max(0, toNumber(row.transition_customer_cnt, 0));
        if (!source || !target || source === target || customers <= 0) return;
        inboundCounts.set(target, toNumber(inboundCounts.get(target), 0) + customers);
        outboundCounts.set(source, toNumber(outboundCounts.get(source), 0) + customers);
        if (!inboundSources.has(target)) inboundSources.set(target, new Set());
        if (!outboundTargets.has(source)) outboundTargets.set(source, new Set());
        inboundSources.get(target).add(source);
        outboundTargets.get(source).add(target);
        inboundBreadth.set(target, inboundSources.get(target).size);
        outboundBreadth.set(source, outboundTargets.get(source).size);
    });

    return {
        cartById,
        demandById,
        nodeById,
        inboundCounts,
        outboundCounts,
        inboundBreadth,
        outboundBreadth,
        inboundSources,
        outboundTargets
    };
}

function brandGetComponentInterpretation(groupKey, weakestKey, strongestLabel, weakestLabel) {
    const weakText = BRAND_INTERPRETATION_TEXT[groupKey]?.[weakestKey] || `${weakestLabel}이 약해 보여요.`;
    if (groupKey === 'health') {
        return `전체 구조는 돌아가지만 ${weakText}`;
    }
    return `구조는 있어도 ${weakText}`;
}

function brandGetPrimaryReason(groupKey, weakestKey) {
    const label = BRAND_COMPONENT_META[weakestKey]?.label || '핵심 축';
    const reason = BRAND_INTERPRETATION_TEXT[groupKey]?.[weakestKey] || `${label}이 약해 보여요.`;
    return `주된 원인: ${label}. ${reason}`;
}

function brandGetPointSummary(type, componentKey, score) {
    const label = BRAND_COMPONENT_META[componentKey]?.label || '핵심 축';
    if (type === 'strength') {
        const messageMap = {
            entryDiversity: '유입이 한 제품에만 과하게 몰리지 않고 있어요.',
            expansionLadder: '첫 구매 뒤 다음 구매로 이어지는 흐름이 살아 있어요.',
            loopStability: '반복 구매 흐름이 비교적 안정적으로 유지되고 있어요.',
            roleBalance: '제품 역할이 한쪽으로 심하게 기울지 않았어요.',
            coreInfluence: '핵심 제품이 구매 전환의 중심을 잡고 있어요.',
            graphStrength: '제품 사이 연결이 살아 있어 구매 전환이 이어지고 있어요.',
            returnPower: '반복 구매 힘이 구매 전환으로 이어지고 있어요.',
            hubConcentration: '허브 제품이 분산돼 있어 특정 제품 쏠림이 덜해요.'
        };
        return `좋은 점: ${label}. ${messageMap[componentKey] || '비교적 안정적으로 유지되고 있어요.'}`;
    }

    if (score >= 60) {
        return `약한 점: ${label}. 지금은 뚜렷한 약점이 크게 보이지 않아요.`;
    }

    const messageMap = {
        entryDiversity: '유입이 일부 제품에 몰려 구조가 쉽게 흔들릴 수 있어요.',
        expansionLadder: '첫 구매 뒤 다음 구매로 이어지는 연결이 약해요.',
        loopStability: '반복 구매가 꾸준히 굳어지지 않았어요.',
        roleBalance: '제품 역할이 한쪽에 몰려 있어 균형이 부족해요.',
        coreInfluence: '핵심 제품이 구매 전환을 끌어당기는 힘이 아직 약해요.',
        graphStrength: '제품 사이 연결이 얇아 구매 전환이 길게 이어지지 않아요.',
        returnPower: '반복 구매 힘이 약해서 구매 전환으로 충분히 이어지지 않아요.',
        hubConcentration: '실제 구매가 소수 제품에 몰려 의존도가 높아요.'
    };
    return `약한 점: ${label}. ${messageMap[componentKey] || '보강이 필요해 보여요.'}`;
}

function brandBuildPatternCards(points, edgeRows, maps) {
    const cards = [];
    const patterns = AppState.data.insightDemandGraphPatterns || [];
    const seenPaths = new Set();
    const transitionPatterns = patterns
        .filter((row) => {
            const type = String(row.pattern_type || '').toLowerCase();
            return !type.includes('basket') && !type.includes('loop');
        })
        .sort((a, b) => toNumber(b.support_value, 0) - toNumber(a.support_value, 0));

    transitionPatterns.forEach((row) => {
        if (cards.length >= 3) return;
        const anchorId = String(row.anchor_product_id || '').trim();
        const relatedIds = String(row.related_product_ids || '')
            .split('|')
            .map((value) => String(value || '').trim())
            .filter(Boolean);
        const ids = [anchorId, ...relatedIds].filter(Boolean);
        const names = ids.map((id) => getProductName(id));
        const path = names.join(' -> ');
        if (!path || seenPaths.has(path)) return;
        seenPaths.add(path);
        cards.push({
            title: '대표 연쇄',
            path,
            note: `지원 고객 ${formatNumber(toNumber(row.support_value, 0), 0)}명`
        });
    });

    if (cards.length < 3) {
        edgeRows.slice(0, 12).forEach((row) => {
            if (cards.length >= 3) return;
            const path = `${row.source_product_name} -> ${row.target_product_name}`;
            if (seenPaths.has(path)) return;
            seenPaths.add(path);
            cards.push({
                title: '대표 연쇄',
                path,
                note: `전이 고객 ${formatNumber(toNumber(row.transition_customer_cnt, 0), 0)}명`
            });
        });
    }

    const loopRow = (AppState.data.returnGravityLoopDetail || [])[0];
    if (loopRow) {
        cards.push({
            title: '반복 루프',
            path: `${getProductName(loopRow.product_id)} -> ${getProductName(loopRow.first_via_product_id)} -> ${getProductName(loopRow.last_via_product_id)} -> ${getProductName(loopRow.product_id)}`,
            note: `루프 고객 ${formatNumber(toNumber(loopRow.return_loop_customer_cnt, 0), 0)}명`
        });
    } else {
        cards.push({
            title: '반복 루프',
            path: '반복 루프 패턴 데이터가 아직 준비되지 않았어요.',
            note: '루프 상세 데이터가 들어오면 대표 루프를 보여줘요.'
        });
    }

    const inboundEntries = Array.from(maps.inboundCounts.entries()).sort((a, b) => toNumber(b[1], 0) - toNumber(a[1], 0));
    if (inboundEntries.length) {
        const [hubId, customers] = inboundEntries[0];
        const feederNames = Array.from(maps.inboundSources.get(hubId) || [])
            .slice(0, 3)
            .map((id) => getProductName(id));
        const path = feederNames.length
            ? `${feederNames.join(', ')} -> ${getProductName(hubId)}`
            : `${getProductName(hubId)}로 수요가 모여요`;
        cards.push({
            title: '허브 수렴',
            path,
            note: `유입 고객 ${formatNumber(customers, 0)}명`
        });
    } else {
        cards.push({
            title: '허브 수렴',
            path: '대표 수렴 허브를 읽을 데이터가 아직 부족해요.',
            note: '전이 데이터가 쌓이면 어디로 수요가 모이는지 보여줘요.'
        });
    }

    return cards;
}

function brandSelectCurrentImpactRow(rows, preferredWindowDays = null) {
    const preferredWindow = toNumber(preferredWindowDays, null);
    if (preferredWindow !== null) {
        const matchedPreferred = (rows || []).find((row) => toNumber(row.window_days, null) === preferredWindow);
        if (matchedPreferred) return matchedPreferred;
    }
    const ordered = [30, 90, 365, 7, 1];
    for (const windowDays of ordered) {
        const matched = (rows || []).find((row) => toNumber(row.window_days, null) === windowDays);
        if (matched) return matched;
    }
    return rows?.[0] || null;
}

function brandFindDelta(series, field, windowDays, latestDate) {
    const filtered = (series || [])
        .filter((row) => toNumber(row.window_days, null) === windowDays && Number.isFinite(toNumber(row[field], NaN)))
        .sort((a, b) => (a.as_of_date < b.as_of_date ? -1 : 1));
    if (!filtered.length) return null;
    const latest = latestDate || filtered[filtered.length - 1];
    const latestValue = toNumber(latest[field], NaN);
    const latestTs = toDate(latest.as_of_date)?.getTime();
    if (!Number.isFinite(latestValue) || !latestTs) return null;
    const targetTs = latestTs - (28 * 24 * 60 * 60 * 1000);
    let candidate = null;
    filtered.forEach((row) => {
        const rowTs = toDate(row.as_of_date)?.getTime();
        if (!rowTs || rowTs > targetTs) return;
        if (!candidate || rowTs > toDate(candidate.as_of_date).getTime()) candidate = row;
    });
    if (!candidate) return null;
    const baseValue = toNumber(candidate[field], NaN);
    if (!Number.isFinite(baseValue)) return null;
    return (latestValue - baseValue) * 100;
}

function brandBuildTimelineModel(timeseries, healthCurrent, impactWindowDays) {
    const availableWindows = Array.from(new Set((timeseries || []).map((row) => toNumber(row.window_days, null)).filter((value) => value !== null)))
        .filter((value) => BRAND_DIAGNOSTIC_WINDOW_ORDER.includes(value))
        .sort((a, b) => BRAND_TIMELINE_WINDOW_ORDER.indexOf(a) - BRAND_TIMELINE_WINDOW_ORDER.indexOf(b));
    if (!availableWindows.length) {
        return {
            availableWindows: [],
            selectedWindowDays: impactWindowDays,
            healthRows: [],
            impactRows: [],
            healthDelta: null,
            impactDelta: null,
            note: '최근 기간 기준 구매 전환 데이터가 없어 이 비교는 아직 표시하지 않아요.'
        };
    }

    const preferredWindow = availableWindows.includes(AppState.viewState.brand.brandWindowDays)
        ? AppState.viewState.brand.brandWindowDays
        : availableWindows[0];
    AppState.viewState.brand.brandWindowDays = preferredWindow;

    const impactRowsAll = timeseries
        .filter((row) => toNumber(row.window_days, null) === preferredWindow && row.as_of_date)
        .sort((a, b) => (a.as_of_date < b.as_of_date ? -1 : 1));
    const latestImpact = impactRowsAll[impactRowsAll.length - 1] || null;
    const latestTs = latestImpact ? toDate(latestImpact.as_of_date)?.getTime() : null;
    const cutoffTs = latestTs ? latestTs - (84 * 24 * 60 * 60 * 1000) : null;
    const impactRows = cutoffTs
        ? impactRowsAll.filter((row) => {
            const ts = toDate(row.as_of_date)?.getTime();
            return ts && ts >= cutoffTs;
        })
        : impactRowsAll;

    const healthRowsSource = timeseries
        .filter((row) => toNumber(row.window_days, null) === preferredWindow && row.as_of_date)
        .sort((a, b) => (a.as_of_date < b.as_of_date ? -1 : 1));
    const healthRows = cutoffTs
        ? healthRowsSource.filter((row) => {
            const ts = toDate(row.as_of_date)?.getTime();
            return ts && ts >= cutoffTs;
        })
        : healthRowsSource;

    const healthDelta = brandFindDelta(timeseries, 'bhi', preferredWindow, healthRowsSource[healthRowsSource.length - 1]);
    const impactDelta = brandFindDelta(timeseries, 'bii', preferredWindow, latestImpact);

    return {
        availableWindows,
        selectedWindowDays: preferredWindow,
        healthRows,
        impactRows,
        healthDelta,
        impactDelta,
        note: ''
    };
}

function brandBuildDailyPulseModel(rows) {
    const orderedRows = (rows || [])
        .filter((row) => row.as_of_date && Number.isFinite(toNumber(row.daily_bii_pulse, NaN)))
        .sort((a, b) => (a.as_of_date < b.as_of_date ? -1 : 1));
    if (!orderedRows.length) {
        return {
            rows: [],
            delta: null,
            interpretation: '',
            note: '일별 실제 구매 흐름 데이터가 없어 아직 표시하지 않아요.',
            helper: '이 차트는 날짜별 단일값 데이터가 있을 때 보여줘요.'
        };
    }

    const latestRow = orderedRows[orderedRows.length - 1];
    const latestTs = toDate(latestRow.as_of_date)?.getTime();
    const cutoffTs = latestTs ? latestTs - ((BRAND_PULSE_LOOKBACK_DAYS - 1) * 24 * 60 * 60 * 1000) : null;
    const visibleRows = cutoffTs
        ? orderedRows.filter((row) => {
            const ts = toDate(row.as_of_date)?.getTime();
            return ts && ts >= cutoffTs;
        })
        : orderedRows;

    const pulseValues = visibleRows.map((row) => toNumber(row.daily_bii_pulse, 0) * 100);
    const recent7 = pulseValues.slice(-7);
    const prior7 = pulseValues.slice(-14, -7);
    const recent3 = pulseValues.slice(-3);
    const recent14 = pulseValues.slice(-14);
    const recentAvg = brandAverage(recent7);
    const priorAvg = brandAverage(prior7);
    const delta = Number.isFinite(recentAvg) && Number.isFinite(priorAvg) ? recentAvg - priorAvg : null;
    const slope = recent3.length >= 3 ? recent3[recent3.length - 1] - recent3[0] : 0;
    const latestValue = pulseValues[pulseValues.length - 1] ?? null;
    const positionKey = brandGetPulsePositionKey(recent14, latestValue);

    return {
        rows: visibleRows,
        delta,
        latestValue,
        interpretation: brandBuildPulseInterpretation(delta, slope, positionKey),
        note: '',
        helper: '이 차트는 하루 단위 흐름을 보여줘요.'
    };
}

function brandGetAvailableDiagnosticWindows(rows) {
    const available = new Set((rows || [])
        .map((row) => toNumber(row.window_days, null))
        .filter((value) => BRAND_DIAGNOSTIC_WINDOW_ORDER.includes(value)));
    return BRAND_DIAGNOSTIC_WINDOW_ORDER.filter((value) => available.has(value));
}

function brandSelectDiagnosticWindow(rows) {
    const available = brandGetAvailableDiagnosticWindows(rows);
    const preferred = toNumber(AppState.viewState.brand.brandWindowDays, 30);
    if (available.includes(preferred)) {
        AppState.viewState.brand.brandWindowDays = preferred;
        return preferred;
    }
    const fallback = available.includes(30) ? 30 : (available[0] || 30);
    AppState.viewState.brand.brandWindowDays = fallback;
    return fallback;
}

function brandBuildWindowMap(rows, valueField) {
    const byDate = new Map();
    (rows || []).forEach((row) => {
        const date = String(row.as_of_date || '').trim();
        const windowDays = toNumber(row.window_days, null);
        const value = toNumber(row[valueField], NaN);
        if (!date || windowDays === null || !Number.isFinite(value)) return;
        if (!byDate.has(date)) byDate.set(date, new Map());
        byDate.get(date).set(windowDays, row);
    });
    return byDate;
}

function brandBuildRatioPairs(rows, selectedWindowDays, valueField) {
    const byDate = brandBuildWindowMap(rows, valueField);
    return Array.from(byDate.entries()).map(([date, windowMap]) => {
        const selectedRow = windowMap.get(selectedWindowDays);
        const baselineRow = windowMap.get(365);
        if (!selectedRow || !baselineRow) return null;
        const selectedValue = toNumber(selectedRow[valueField], NaN);
        const baselineValue = toNumber(baselineRow[valueField], NaN);
        if (!Number.isFinite(selectedValue) || !Number.isFinite(baselineValue) || baselineValue <= 0) return null;
        return {
            as_of_date: date,
            selectedRow,
            baselineRow,
            ratio: selectedValue / baselineValue
        };
    }).filter(Boolean).sort((a, b) => (a.as_of_date < b.as_of_date ? -1 : 1));
}

function brandFindPreviousPair(pairs, currentDate, daysBack = 28) {
    const currentTs = toDate(currentDate)?.getTime();
    if (!currentTs) return null;
    const targetTs = currentTs - (daysBack * 24 * 60 * 60 * 1000);
    let candidate = null;
    (pairs || []).forEach((pair) => {
        const ts = toDate(pair.as_of_date)?.getTime();
        if (!ts || ts > targetTs) return;
        if (!candidate || ts > toDate(candidate.as_of_date).getTime()) candidate = pair;
    });
    return candidate;
}

function brandGetMatrixStage(quadrants, xHigh, yHigh) {
    if (xHigh && yHigh) return quadrants.highHigh;
    if (xHigh && !yHigh) return quadrants.highLow;
    if (!xHigh && yHigh) return quadrants.lowHigh;
    return quadrants.lowLow;
}

function brandBuildStructureMatrix(healthCurrentScore, currentImpactRow, timeseries, diagnosticWindowDays) {
    const current365Row = (AppState.data.biiWindow || []).find((row) => toNumber(row.window_days, null) === 365);
    if (!currentImpactRow || !current365Row) {
        return {
            kind: 'structure',
            empty: true,
            title: BRAND_DIAGNOSTIC_META.structure.title,
            description: BRAND_DIAGNOSTIC_META.structure.description,
            note: '선택한 기간 또는 365일 기준 브랜드 구매 전환력 데이터가 없어 이 진단은 아직 표시하지 않아요.'
        };
    }

    const currentRatio = brandSafeDivide(toNumber(currentImpactRow.bii, 0), Math.max(toNumber(current365Row.bii, 0), 0.0001), 0);
    const currentPoint = {
        x: healthCurrentScore,
        y: currentRatio,
        as_of_date: currentImpactRow.as_of_date || current365Row.as_of_date || '',
        ratio: currentRatio
    };
    const ratioPairs = brandBuildRatioPairs(timeseries, diagnosticWindowDays, 'bii');
    const previousPair = brandFindPreviousPair(ratioPairs, currentPoint.as_of_date, 28);
    const previousPoint = previousPair ? {
        x: toNumber(previousPair.selectedRow?.bhi, healthCurrentScore) * 100,
        y: previousPair.ratio,
        as_of_date: previousPair.as_of_date,
        ratio: previousPair.ratio
    } : null;
    const xHigh = currentPoint.x >= 60;
    const yHigh = currentPoint.y >= 1;
    const stage = brandGetMatrixStage(BRAND_DIAGNOSTIC_META.structure.quadrants, xHigh, yHigh);

    return {
        kind: 'structure',
        empty: false,
        title: BRAND_DIAGNOSTIC_META.structure.title,
        description: BRAND_DIAGNOSTIC_META.structure.description,
        currentPoint,
        previousPoint,
        thresholdX: 60,
        thresholdY: 1,
        xRange: [0, 100],
        yRange: [0, BRAND_MATRIX_RATIO_MAX],
        xAxisLabel: BRAND_DIAGNOSTIC_META.structure.xAxisLabel,
        yAxisLabel: BRAND_DIAGNOSTIC_META.structure.yAxisLabel,
        stage,
        note: previousPoint ? `최근 4주 동안 ${stage.label} 쪽으로 이동하고 있어요.` : '최근 이동 데이터가 없어 현재 위치만 보여줘요.'
    };
}

function brandBuildRevenueMatrix(revenueTimeseries, impactTimeseries, diagnosticWindowDays) {
    const revenuePairs = brandBuildRatioPairs(revenueTimeseries, diagnosticWindowDays, 'revenue_t');
    if (!revenuePairs.length) {
        return {
            kind: 'revenue',
            empty: true,
            title: BRAND_DIAGNOSTIC_META.revenue.title,
            description: BRAND_DIAGNOSTIC_META.revenue.description,
            note: '매출 진단 데이터가 없어 Revenue vs. 브랜드 구매 전환력 (BII)은 아직 표시하지 않아요.'
        };
    }

    const impactPairs = brandBuildRatioPairs(impactTimeseries, diagnosticWindowDays, 'bii');
    const impactByDate = new Map(impactPairs.map((pair) => [pair.as_of_date, pair]));
    const mergedPairs = revenuePairs
        .map((revenuePair) => {
            const impactPair = impactByDate.get(revenuePair.as_of_date);
            if (!impactPair) return null;
            return {
                as_of_date: revenuePair.as_of_date,
                x: revenuePair.ratio,
                y: impactPair.ratio
            };
        })
        .filter(Boolean)
        .sort((a, b) => (a.as_of_date < b.as_of_date ? -1 : 1));

    if (!mergedPairs.length) {
        return {
            kind: 'revenue',
            empty: true,
            title: BRAND_DIAGNOSTIC_META.revenue.title,
            description: BRAND_DIAGNOSTIC_META.revenue.description,
            note: '같은 날짜 기준 매출과 브랜드 구매 전환력 데이터를 함께 읽을 수 없어 Revenue vs. 브랜드 구매 전환력 (BII)은 아직 표시하지 않아요.'
        };
    }

    const currentPoint = mergedPairs[mergedPairs.length - 1];
    const previousPoint = brandFindPreviousPair(mergedPairs, currentPoint.as_of_date, 28);
    const xHigh = currentPoint.x >= 1;
    const yHigh = currentPoint.y >= 1;
    const stage = brandGetMatrixStage(BRAND_DIAGNOSTIC_META.revenue.quadrants, xHigh, yHigh);

    return {
        kind: 'revenue',
        empty: false,
        title: BRAND_DIAGNOSTIC_META.revenue.title,
        description: BRAND_DIAGNOSTIC_META.revenue.description,
        currentPoint,
        previousPoint,
        thresholdX: 1,
        thresholdY: 1,
        xRange: [0, BRAND_MATRIX_RATIO_MAX],
        yRange: [0, BRAND_MATRIX_RATIO_MAX],
        xAxisLabel: BRAND_DIAGNOSTIC_META.revenue.xAxisLabel,
        yAxisLabel: BRAND_DIAGNOSTIC_META.revenue.yAxisLabel,
        stage,
        note: previousPoint ? `최근 4주 동안 ${stage.label} 쪽으로 이동하고 있어요.` : '최근 이동 데이터가 없어 현재 위치만 보여줘요.'
    };
}

function brandScaleMatrixPoint(value, range) {
    const [min, max] = range;
    if (!Number.isFinite(value) || max <= min) return 50;
    return brandClamp(((value - min) / (max - min)) * 100, 0, 100);
}

function buildBrandDashboardModel() {
    const brandRow = AppState.data.brandScore?.[0] || {};
    const products = AppState.data.anchorScored || [];
    const edgeRows = brandBuildEdgeRows();
    const maps = brandBuildMaps(edgeRows);
    const defaultImpactRow = brandSelectCurrentImpactRow(AppState.data.biiWindow || []);
    const timeseries = AppState.data.brandImpactTimeseries || [];
    const dailyPulseRows = AppState.data.brandImpactDailyPulse || [];
    const revenueTimeseries = AppState.data.brandRevenueTimeseries || [];
    const diagnosticWindowDays = brandSelectDiagnosticWindow(AppState.data.biiWindow || []);
    const currentImpactRow = (AppState.data.biiWindow || []).find((row) => toNumber(row.window_days, null) === diagnosticWindowDays) || defaultImpactRow;

    const centerEntry = products.length ? products.reduce((sum, row) => sum + toNumber(row.Entry_Gravity_Score, 0), 0) / products.length : 0;
    const centerExpansion = products.length ? products.reduce((sum, row) => sum + toNumber(row.Expansion_Gravity_Score, 0), 0) / products.length : 0;

    const productRows = products.map((row) => {
        const id = String(row.product_id || '').trim();
        const cart = maps.cartById.get(id) || {};
        const demand = maps.demandById.get(id) || {};
        const node = maps.nodeById.get(id) || {};
        const entry = toNumber(row.Entry_Gravity_Score, 0);
        const expansion = toNumber(row.Expansion_Gravity_Score, 0);
        const expectedDemand = Math.max(toNumber(row.product_order_cnt_1y, 0) / 52, toNumber(row.first_customer_cnt, 0) / 12, 0.2);
        const roleKey = brandGetRoleKey(entry, expansion, centerEntry, centerExpansion);
        const inboundCustomers = Math.max(
            toNumber(maps.inboundCounts.get(id), 0),
            toNumber(node.incoming_transition_rate_sum_90d, 0) * 1000
        );
        const outboundCustomers = toNumber(maps.outboundCounts.get(id), 0);
        const hubStrength = inboundCustomers + (outboundCustomers * 0.65);
        const breadth = toNumber(maps.inboundBreadth.get(id), 0) + toNumber(maps.outboundBreadth.get(id), 0);
        const repurchaseRate = Math.max(
            toNumber(row.repurchase_rate_90d, 0),
            toNumber(demand.return_customer_rate_90d, 0),
            toNumber(node.return_customer_rate_90d, 0)
        );
        const loopRate = Math.max(
            toNumber(demand.return_loop_rate_90d, 0),
            toNumber(node.return_loop_rate_90d, 0),
            repurchaseRate
        );
        const attachRate = toNumber(cart.attach_rate, 0);
        const combinedStrength = entry + expansion;
        return {
            id,
            name: row.product_name_latest || getProductName(id),
            entry,
            expansion,
            expectedDemand,
            firstCustomers: toNumber(row.first_customer_cnt, 0),
            firstCustomerRatio: toNumber(row.first_customer_ratio, 0),
            repurchaseRate,
            attachRate,
            loopRate,
            inboundCustomers,
            outboundCustomers,
            breadth,
            hubStrength,
            combinedStrength,
            roleKey,
            roleLabel: BRAND_ROLE_META[roleKey].label
        };
    });

    const entryRef = Math.max(brandPercentile(productRows.map((row) => row.entry), 0.9), 0.001);
    const expansionRef = Math.max(brandPercentile(productRows.map((row) => row.expansion), 0.9), 0.001);
    const demandRef = Math.max(brandPercentile(productRows.map((row) => row.expectedDemand), 0.9), 1);
    const hubRef = Math.max(brandPercentile(productRows.map((row) => row.hubStrength), 0.9), 1);
    const breadthRef = Math.max(brandPercentile(productRows.map((row) => row.breadth), 0.9), 1);
    const combinedRef = Math.max(brandPercentile(productRows.map((row) => row.combinedStrength), 0.9), 0.001);

    productRows.forEach((row) => {
        row.entryNorm = brandNormalizeByReference(row.entry, entryRef);
        row.expansionNorm = brandNormalizeByReference(row.expansion, expansionRef);
        row.demandNorm = brandNormalizeByReference(row.expectedDemand, demandRef);
        row.hubNorm = brandNormalizeByReference(row.hubStrength, hubRef);
        row.breadthNorm = brandNormalizeByReference(row.breadth, breadthRef);
        row.combinedNorm = brandNormalizeByReference(row.combinedStrength, combinedRef);
        row.healthContribution = (row.entryNorm * 0.28) + (row.expansionNorm * 0.22) + (brandClamp(row.loopRate) * 0.3) + (brandClamp(row.attachRate) * 0.2);
        row.impactContribution = (row.hubNorm * 0.35) + (row.breadthNorm * 0.25) + (row.demandNorm * 0.2) + (row.combinedNorm * 0.2);
    });

    const roleCounts = {
        growth: productRows.filter((row) => row.roleKey === 'growth').length,
        acquisition: productRows.filter((row) => row.roleKey === 'acquisition').length,
        expansion: productRows.filter((row) => row.roleKey === 'expansion').length,
        peripheral: productRows.filter((row) => row.roleKey === 'peripheral').length
    };

    const totalFirstCustomers = productRows.reduce((sum, row) => sum + row.firstCustomers, 0);
    const totalExpectedDemand = productRows.reduce((sum, row) => sum + row.expectedDemand, 0);
    const totalTransitionCustomers = edgeRows.reduce((sum, row) => sum + toNumber(row.transition_customer_cnt, 0), 0);
    const totalLoopCustomers = (AppState.data.returnGravityLoopDetail || []).reduce((sum, row) => sum + toNumber(row.return_loop_customer_cnt, 0), 0);
    const roleEntropy = brandEntropyFromCounts(Object.values(roleCounts));
    const firstCustomerEntropy = brandEntropyFromCounts(productRows.map((row) => row.firstCustomers));
    const weightedRepurchase = totalFirstCustomers > 0
        ? productRows.reduce((sum, row) => sum + (row.repurchaseRate * row.firstCustomers), 0) / totalFirstCustomers
        : 0;
    const weightedAttach = totalExpectedDemand > 0
        ? productRows.reduce((sum, row) => sum + (row.attachRate * row.expectedDemand), 0) / totalExpectedDemand
        : 0;
    const weightedExpansionNorm = totalFirstCustomers > 0
        ? productRows.reduce((sum, row) => sum + (row.expansionNorm * row.firstCustomers), 0) / totalFirstCustomers
        : 0;
    const transitionBreadthNorm = totalFirstCustomers > 0
        ? productRows.reduce((sum, row) => sum + (row.breadthNorm * row.firstCustomers), 0) / totalFirstCustomers
        : 0;
    const transitionCoverage = brandClamp(brandSafeDivide(totalTransitionCustomers, Math.max(totalFirstCustomers, 1), 0));
    const loopCoverage = brandClamp(brandSafeDivide(totalLoopCustomers, Math.max(totalFirstCustomers, 1), 0) * 4);
    const coreProducts = [...productRows].sort((a, b) => b.impactContribution - a.impactContribution).slice(0, 5);
    const topCoreDemandShare = brandClamp(brandSafeDivide(coreProducts.reduce((sum, row) => sum + row.expectedDemand, 0), Math.max(totalExpectedDemand, 1), 0));
    const topCoreStrength = coreProducts.length
        ? coreProducts.reduce((sum, row) => sum + row.combinedNorm, 0) / coreProducts.length
        : 0;
    const hubRows = [...productRows].sort((a, b) => b.hubStrength - a.hubStrength);
    const hubTop3Share = brandClamp(brandSafeDivide(hubRows.slice(0, 3).reduce((sum, row) => sum + row.hubStrength, 0), Math.max(hubRows.reduce((sum, row) => sum + row.hubStrength, 0), 1), 0));
    const timeline = brandBuildTimelineModel(timeseries, toNumber(brandRow.BHI, 0), toNumber(defaultImpactRow?.window_days, 30));
    const dailyPulse = brandBuildDailyPulseModel(dailyPulseRows);
    const diagnosticSeriesRows = (timeseries || [])
        .filter((row) => toNumber(row.window_days, null) === diagnosticWindowDays)
        .sort((a, b) => (a.as_of_date < b.as_of_date ? -1 : 1));
    const diagnosticLatestRow = diagnosticSeriesRows[diagnosticSeriesRows.length - 1] || null;
    const diagnosticHealthDelta = brandFindDelta(timeseries, 'bhi', diagnosticWindowDays, diagnosticLatestRow);
    const diagnosticImpactDelta = brandFindDelta(timeseries, 'bii', diagnosticWindowDays, diagnosticLatestRow);

    const componentScores = {
        entryDiversity: ((brandClamp(1 - toNumber(brandRow.AA_Concentration_Index, 0)) * 0.65) + (firstCustomerEntropy * 0.35)) * 100,
        expansionLadder: ((weightedExpansionNorm * 0.7) + (transitionBreadthNorm * 0.3)) * 100,
        loopStability: ((brandClamp(weightedRepurchase) * 0.7) + (loopCoverage * 0.3)) * 100,
        roleBalance: roleEntropy * 100,
        coreInfluence: ((topCoreStrength * 0.55) + (topCoreDemandShare * 0.45)) * 100,
        graphStrength: ((transitionCoverage * 0.6) + (transitionBreadthNorm * 0.4)) * 100,
        returnPower: (((brandClamp(toNumber(currentImpactRow?.customer_strength_norm, 0)) * 0.55) + (brandClamp(toNumber(currentImpactRow?.clv_norm, 0)) * 0.25) + (brandClamp(weightedRepurchase) * 0.2))) * 100,
        hubConcentration: hubTop3Share * 100
    };

    const healthComponentKeys = ['entryDiversity', 'expansionLadder', 'loopStability', 'roleBalance'];
    const impactComponentKeys = ['coreInfluence', 'graphStrength', 'returnPower', 'hubConcentration'];
    const weakestHealthKey = [...healthComponentKeys].sort((a, b) => componentScores[a] - componentScores[b])[0];
    const strongestHealthKey = [...healthComponentKeys].sort((a, b) => componentScores[b] - componentScores[a])[0];
    const weakestImpactKey = [...impactComponentKeys].sort((a, b) => componentScores[a] - componentScores[b])[0];
    const strongestImpactKey = [...impactComponentKeys].sort((a, b) => componentScores[b] - componentScores[a])[0];
    const healthCurrentScore = toNumber(firstDefinedValue(brandRow.BHI, brandRow.Brand_Health_Index, brandRow.brand_health_index, brandRow.Brand_Health_Score), 0) * 100;
    const impactCurrentScore = toNumber(currentImpactRow?.bii, 0) * 100;

    const driversHealth = [...productRows]
        .sort((a, b) => b.healthContribution - a.healthContribution)
        .slice(0, 5)
        .map((row) => {
            const reasonMap = {
                entry: row.entryNorm,
                expansion: row.expansionNorm,
                loop: brandClamp(row.loopRate),
                attach: brandClamp(row.attachRate)
            };
            const reasonKey = Object.entries(reasonMap).sort((a, b) => b[1] - a[1])[0][0];
            const reasonText = {
                entry: '유입 진입점을 단단하게 받쳐 줘요.',
                expansion: '다음 구매 사다리를 이어 주는 확장 연결축이에요.',
                loop: '반복 구매가 안정적으로 이어지는 루프 축이에요.',
                attach: '장바구니 결합으로 구조를 넓혀 주는 확장 보조축이에요.'
            }[reasonKey];
            return {
                ...row,
                score: row.healthContribution * 100,
                reasonTag: {
                    entry: '유입 진입점',
                    expansion: '확장 연결축',
                    loop: '반복 루프 축',
                    attach: '장바구니 결합축'
                }[reasonKey],
                reasonText,
                metrics: [
                    `유입 강도 ${formatNumber(row.entry, 3)}`,
                    `90일 재구매 ${formatPercent(row.repurchaseRate, 1)}`,
                    `장바구니 결합 ${formatPercent(row.attachRate, 1)}`
                ]
            };
        });

    const driversImpact = [...productRows]
        .sort((a, b) => b.impactContribution - a.impactContribution)
        .slice(0, 5)
        .map((row) => {
            const reasonMap = {
                hub: row.hubNorm,
                graph: row.breadthNorm,
                demand: row.demandNorm,
                core: row.combinedNorm
            };
            const reasonKey = Object.entries(reasonMap).sort((a, b) => b[1] - a[1])[0][0];
            const reasonText = {
                hub: '여러 흐름을 받아 주는 수요 허브 역할을 해요.',
                graph: '제품 사이 연결 폭을 넓히는 핵심 접속점이에요.',
                demand: '기대 수요량이 커서 구매 전환이 넓게 퍼져요.',
                core: '유입과 확장 강도가 함께 높아 구매 전환의 중심을 만듭니다.'
            }[reasonKey];
            return {
                ...row,
                score: row.impactContribution * 100,
                reasonTag: {
                    hub: '수요 허브',
                    graph: '연결 중심축',
                    demand: '대형 수요축',
                    core: '핵심 영향축'
                }[reasonKey],
                reasonText,
                metrics: [
                    `허브 강도 ${formatNumber(row.hubStrength, 0)}`,
                    `연결 폭 ${formatNumber(row.breadth, 0)}개`,
                    `주간 예상 수요 ${formatNumber(row.expectedDemand, 1)}`
                ]
            };
        });

    const healthInterpretation = componentScores[weakestHealthKey] < 55
        ? brandGetComponentInterpretation('health', weakestHealthKey, BRAND_COMPONENT_META[strongestHealthKey].label, BRAND_COMPONENT_META[weakestHealthKey].label)
        : '유입, 확장, 반복 흐름이 비교적 고르게 버티고 있어요.';
    const impactInterpretation = componentScores.hubConcentration >= 70 && componentScores.coreInfluence >= 60
        ? '구매 전환은 소수 핵심 제품이 끌고 있고, 그만큼 특정 제품 의존도도 높아요.'
        : componentScores[weakestImpactKey] < 50
            ? brandGetComponentInterpretation('impact', weakestImpactKey, BRAND_COMPONENT_META[strongestImpactKey].label, BRAND_COMPONENT_META[weakestImpactKey].label)
            : '핵심 제품과 제품 연결이 살아 있어 구조가 구매 전환으로 이어지고 있어요.';
    const healthPrimaryReason = componentScores[weakestHealthKey] < 55
        ? brandGetPrimaryReason('health', weakestHealthKey)
        : `주된 원인: ${BRAND_COMPONENT_META[strongestHealthKey].label}. 구조의 약한 고리가 두드러지지 않아요.`;
    const impactPrimaryReason = componentScores.hubConcentration >= 70 && componentScores.coreInfluence >= 60
        ? '주된 원인: 허브 집중도. 구매 전환이 소수 핵심 제품에 많이 몰려 있어요.'
        : componentScores[weakestImpactKey] < 50
            ? brandGetPrimaryReason('impact', weakestImpactKey)
            : `주된 원인: ${BRAND_COMPONENT_META[strongestImpactKey].label}. 핵심 연결축이 구매 전환을 받고 있어요.`;
    const healthStrengthPoint = brandGetPointSummary('strength', strongestHealthKey, componentScores[strongestHealthKey]);
    const healthWeakPoint = brandGetPointSummary('weakness', weakestHealthKey, componentScores[weakestHealthKey]);
    const impactStrengthPoint = brandGetPointSummary('strength', strongestImpactKey, componentScores[strongestImpactKey]);
    const impactWeakPoint = brandGetPointSummary('weakness', weakestImpactKey, componentScores[weakestImpactKey]);
    const diagnosticMatrices = {
        availableWindows: brandGetAvailableDiagnosticWindows(AppState.data.biiWindow || []),
        selectedWindowDays: diagnosticWindowDays,
        structure: brandBuildStructureMatrix(healthCurrentScore, currentImpactRow, timeseries, diagnosticWindowDays),
        revenue: brandBuildRevenueMatrix(revenueTimeseries, timeseries, diagnosticWindowDays)
    };

    return {
        brandRow,
        currentImpactRow,
        healthCurrentScore,
        impactCurrentScore,
        healthDelta: diagnosticHealthDelta,
        impactDelta: diagnosticImpactDelta,
        healthInterpretation,
        impactInterpretation,
        healthPrimaryReason,
        impactPrimaryReason,
        healthStrengthPoint,
        healthWeakPoint,
        impactStrengthPoint,
        impactWeakPoint,
        healthStrongestKey: strongestHealthKey,
        healthWeakestKey: weakestHealthKey,
        impactStrongestKey: strongestImpactKey,
        impactWeakestKey: weakestImpactKey,
        impactWindowDays: toNumber(currentImpactRow?.window_days, diagnosticWindowDays || timeline.selectedWindowDays || defaultImpactRow?.window_days || 30),
        roleCounts,
        productRows,
        edgeRows,
        maps,
        dailyPulse,
        timeline,
        diagnosticMatrices,
        componentGroups: [
            {
                title: BRAND_COMPONENT_GROUPS.health,
                summary: '유입, 다음 구매, 반복 구매, 역할 균형을 나눠서 봐요.',
                items: healthComponentKeys.map((key) => ({
                    key,
                    score: componentScores[key],
                    ...BRAND_COMPONENT_META[key]
                }))
            },
            {
                title: BRAND_COMPONENT_GROUPS.impact,
                summary: '핵심 제품, 제품 간 연결, 반복 구매 힘, 허브 쏠림을 봐요.',
                items: impactComponentKeys.map((key) => ({
                    key,
                    score: componentScores[key],
                    ...BRAND_COMPONENT_META[key]
                }))
            }
        ],
        driversHealth,
        driversImpact,
        patternCards: brandBuildPatternCards(productRows, edgeRows, maps),
        centers: {
            entry: centerEntry,
            expansion: centerExpansion
        },
        scoreBands: {
            health: brandGetScoreBand(healthCurrentScore),
            impact: brandGetScoreBand(impactCurrentScore)
        }
    };
}

function renderBrandStateCard(title, summary, score, delta, interpretation, primaryReason, strengthPoint, weakPoint, metaLabel, metaValue, tone, radarCanvasId, detailSectionHtml = '') {
    return `
        <article class="card brand-state-card brand-state-card-${tone}">
            <div class="brand-state-topline">
                <div>
                    <p class="brand-state-eyebrow">${brandRenderMetricName(title.label, title.abbr)}</p>
                    <p class="brand-state-summary">${escapeHtml(summary)}</p>
                    <div class="brand-state-score">${brandFormatScore(score)}</div>
                </div>
                <div class="brand-chip-stack">
                    <span class="brand-status-chip">${escapeHtml(metaLabel)}</span>
                    <span class="brand-status-chip brand-status-chip-muted">${escapeHtml(metaValue)}</span>
                </div>
            </div>
            <div class="brand-state-body">
                <div class="brand-state-content">
                    <p class="brand-state-delta">${escapeHtml(brandFormatDelta(delta))}</p>
                    <p class="brand-state-copy">${escapeHtml(interpretation)}</p>
                    <p class="brand-state-reason">${escapeHtml(primaryReason)}</p>
                    <div class="brand-state-points">
                        <p class="brand-state-point brand-state-point-strong">${escapeHtml(strengthPoint)}</p>
                        <p class="brand-state-point brand-state-point-weak">${escapeHtml(weakPoint)}</p>
                    </div>
                </div>
                <div class="brand-state-mini-radar">
                    <canvas id="${escapeHtml(radarCanvasId)}"></canvas>
                </div>
            </div>
            ${detailSectionHtml}
        </article>
    `;
}

function renderBrandComponentGroup(group) {
    return `
        <details class="brand-component-group">
            <summary class="brand-component-group-head">
                <div>
                    <h3>${escapeHtml(group.title)}</h3>
                    <p>${escapeHtml(group.summary || '')}</p>
                </div>
                <span class="brand-component-toggle">펼쳐 보기</span>
            </summary>
            <div class="brand-component-list">
                ${group.items.map((item) => `
                    <div class="brand-component-item">
                        <div class="brand-component-main">
                            <div>
                                <strong>${escapeHtml(item.label)}</strong>
                                <p>${escapeHtml(item.shortDescription || item.description)}</p>
                            </div>
                            <div class="brand-component-score-wrap">
                                <span class="brand-component-badge">${escapeHtml(item.badge)}</span>
                                <span class="brand-component-score">${brandFormatScore(item.score)}</span>
                            </div>
                        </div>
                    </div>
                `).join('')}
            </div>
        </details>
    `;
}

function renderBrandDriverCard(title, desc, rows) {
    return `
        <section class="card brand-driver-card">
            <div class="brand-section-head">
                <div>
                    <h2>${escapeHtml(title)}</h2>
                    <p>${escapeHtml(desc)}</p>
                </div>
            </div>
            <div class="brand-driver-list">
                ${rows.map((row) => `
                    <article class="brand-driver-item">
                        <div class="brand-driver-main">
                            <div>
                                <h3>${escapeHtml(row.name)}</h3>
                                <div class="brand-driver-tags">
                                    <span class="brand-role-pill brand-role-pill-${escapeHtml(row.roleKey)}">${escapeHtml(row.roleLabel)}</span>
                                    <span class="brand-reason-pill">${escapeHtml(row.reasonTag)}</span>
                                </div>
                            </div>
                            <div class="brand-driver-score">${brandFormatScore(row.score)}</div>
                        </div>
                        <p class="brand-driver-copy">${escapeHtml(row.reasonText)}</p>
                        <p class="brand-driver-metrics">${escapeHtml(row.metrics.join(' · '))}</p>
                    </article>
                `).join('')}
            </div>
        </section>
    `;
}

function renderBrandDiagnosticMatrixCard(matrix) {
    if (matrix.empty) {
        return `
            <article class="brand-matrix-card brand-matrix-card-empty">
                <div class="brand-matrix-head">
                    <div>
                        <h3>${escapeHtml(matrix.title)}</h3>
                        <p>${escapeHtml(matrix.description)}</p>
                    </div>
                </div>
                <div class="brand-empty-state">
                    <p>${escapeHtml(matrix.note)}</p>
                </div>
            </article>
        `;
    }

    const currentLeft = brandScaleMatrixPoint(matrix.currentPoint.x, matrix.xRange);
    const currentBottom = brandScaleMatrixPoint(matrix.currentPoint.y, matrix.yRange);
    const previousLeft = matrix.previousPoint ? brandScaleMatrixPoint(matrix.previousPoint.x, matrix.xRange) : currentLeft;
    const previousBottom = matrix.previousPoint ? brandScaleMatrixPoint(matrix.previousPoint.y, matrix.yRange) : currentBottom;
    const deltaX = currentLeft - previousLeft;
    const deltaY = previousBottom - currentBottom;
    const arrowLength = Math.sqrt((deltaX ** 2) + (deltaY ** 2));
    const arrowAngle = Math.atan2(deltaY, deltaX) * (180 / Math.PI);
    const vectorLength = Math.max(Math.sqrt((deltaX ** 2) + ((currentBottom - previousBottom) ** 2)), 0.0001);
    const unitX = deltaX / vectorLength;
    const unitY = (currentBottom - previousBottom) / vectorLength;
    const previousLabelOffsetX = brandClamp(-unitX * 18, -24, 24);
    const previousLabelOffsetY = brandClamp(unitY * 18 + 18, 12, 28);
    const currentLabelOffsetX = brandClamp(unitX * 18, -24, 24);
    const currentLabelOffsetY = brandClamp(-unitY * 18 - 22, -30, -14);
    const stageToneClass = `brand-matrix-stage-${escapeHtml(matrix.stage.tone)}`;

    return `
        <article class="brand-matrix-card">
            <div class="brand-matrix-head">
                <div>
                    <h3>${escapeHtml(matrix.title)}</h3>
                    <p>${escapeHtml(matrix.description)}</p>
                </div>
                <span class="brand-matrix-stage ${stageToneClass}">${escapeHtml(matrix.stage.label)}</span>
            </div>
            <p class="brand-matrix-summary">${escapeHtml(matrix.stage.summary)}</p>
            <p class="brand-matrix-note">${escapeHtml(matrix.note)}</p>
            <div class="brand-matrix-visual">
                <div class="brand-matrix-axis-label brand-matrix-axis-label-y">${escapeHtml(matrix.yAxisLabel)}</div>
                <div class="brand-matrix-axis-label brand-matrix-axis-label-x">${escapeHtml(matrix.xAxisLabel)}</div>
                <div class="brand-matrix-grid brand-matrix-grid-${escapeHtml(matrix.kind)}" style="--matrix-threshold-x:${brandScaleMatrixPoint(matrix.thresholdX, matrix.xRange)}%; --matrix-threshold-y:${brandScaleMatrixPoint(matrix.thresholdY, matrix.yRange)}%;">
                    <div class="brand-matrix-cell brand-matrix-cell-q1"><strong>${escapeHtml(matrix.kind === 'structure' ? '단기 펌핑' : '장기 개선 신호')}</strong></div>
                    <div class="brand-matrix-cell brand-matrix-cell-q2"><strong>${escapeHtml(matrix.kind === 'structure' ? '최우수' : '건강한 성장')}</strong></div>
                    <div class="brand-matrix-cell brand-matrix-cell-q3"><strong>${escapeHtml(matrix.kind === 'structure' ? '전면 개선 필요' : '동반 침체')}</strong></div>
                    <div class="brand-matrix-cell brand-matrix-cell-q4"><strong>${escapeHtml(matrix.kind === 'structure' ? '실행력 저하' : '구조 리스크')}</strong></div>
                    <div class="brand-matrix-overlay">
                        ${matrix.previousPoint ? `
                            <div
                                class="brand-matrix-arrow brand-matrix-arrow-animated"
                                style="left:${previousLeft}%; bottom:${previousBottom}%; width:${arrowLength}%; --brand-matrix-angle:${arrowAngle}deg;"
                            ></div>
                            <span class="brand-matrix-point brand-matrix-point-prev" style="left:${previousLeft}%; bottom:${previousBottom}%;" title="4주 전"></span>
                            <span class="brand-matrix-point-label brand-matrix-point-label-prev" style="left:${previousLeft}%; bottom:${previousBottom}%; --brand-point-label-x:${previousLabelOffsetX}px; --brand-point-label-y:${previousLabelOffsetY}px;">4주 전</span>
                        ` : ''}
                        <span class="brand-matrix-point brand-matrix-point-current" style="left:${currentLeft}%; bottom:${currentBottom}%;" title="현재"></span>
                        <span class="brand-matrix-point-label brand-matrix-point-label-current" style="left:${currentLeft}%; bottom:${currentBottom}%; --brand-point-label-x:${currentLabelOffsetX}px; --brand-point-label-y:${currentLabelOffsetY}px;">현재</span>
                    </div>
                </div>
                <div class="brand-matrix-scale">
                    <span>Low</span>
                    <span>High</span>
                </div>
            </div>
        </article>
    `;
}

function renderBrandDiagnosticSection(model) {
    const matrices = model.diagnosticMatrices || {};
    return `
        <section class="card brand-diagnostic-card">
            <div class="brand-section-head">
                <div>
                    <h2>지금 브랜드 상태를 진단하면 어디에 있나?</h2>
                    <p>BHI와 브랜드 구매 전환력, 그리고 매출을 함께 보면 지금 상태를 더 직관적으로 읽을 수 있어요.</p>
                </div>
            </div>
            <div class="brand-matrix-grid-wrap">
                ${renderBrandDiagnosticMatrixCard(matrices.structure)}
                ${renderBrandDiagnosticMatrixCard(matrices.revenue)}
            </div>
        </section>
    `;
}

function renderBrandPulsePanel(pulse) {
    if (!pulse.rows.length) {
        return `
            <article class="brand-trend-panel brand-trend-panel-main">
                <div class="brand-mini-chart-head">
                    <h3>${brandRenderMetricName('일별 실제 구매 흐름', 'Daily Pulse')}</h3>
                    <span>데이터 없음</span>
                </div>
                <p class="brand-panel-compare"><strong>이 차트는 이런 때 봐요.</strong> 오늘이나 어제 흐름이 갑자기 좋아졌는지, 꺾였는지 바로 읽을 때 좋아요.</p>
                <div class="brand-empty-state">
                    <p>${escapeHtml(pulse.note)}</p>
                    <p class="brand-empty-state-sub">${escapeHtml(pulse.helper)}</p>
                </div>
            </article>
        `;
    }

    return `
        <article class="brand-trend-panel brand-trend-panel-main">
            <div class="brand-mini-chart-head">
                <h3>${brandRenderMetricName('일별 실제 구매 흐름', 'Daily Pulse')}</h3>
                <span>${escapeHtml(brandFormatPulseDelta(pulse.delta))}</span>
            </div>
            <p class="brand-panel-compare"><strong>이 차트는 이런 때 봐요.</strong> 오늘이나 어제 흐름이 갑자기 좋아졌는지, 꺾였는지 바로 읽을 때 좋아요.</p>
            <div class="brand-mini-chart-wrap">
                <canvas id="brand-daily-pulse-timeline"></canvas>
            </div>
            <p class="brand-pulse-interpretation">${escapeHtml(pulse.interpretation)}</p>
            <p class="brand-mini-chart-caption">${escapeHtml(pulse.helper)}</p>
        </article>
    `;
}

function renderBrandTimelineSupportPanel(timeline) {
    const selectedWindowLabel = `${formatNumber(timeline.selectedWindowDays || 30, 0)}일`;
    const selectedWindowGuide = BRAND_TIMELINE_WINDOW_GUIDE[timeline.selectedWindowDays] || '선택한 기간 기준으로 구매 전환을 해석해요.';
    if (!timeline.availableWindows.length) {
        return `
            <article class="brand-trend-panel brand-trend-panel-secondary">
                <div class="brand-mini-chart-head brand-mini-chart-head-stack">
                    <div>
                        <h3>최근 기간 기준으로 보면 구매 전환은 어떤가?</h3>
                        <p>최근 7일, 30일처럼 기간을 묶어 실제 구매 전환 상태를 봐요.</p>
                    </div>
                </div>
                <div class="brand-empty-state">
                    <p>${escapeHtml(timeline.note)}</p>
                </div>
            </article>
        `;
    }

    return `
        <article class="brand-trend-panel brand-trend-panel-secondary">
            <div class="brand-mini-chart-head brand-mini-chart-head-stack">
                <div>
                    <h3>최근 기간 기준으로 보면 구매 전환은 어떤가?</h3>
                    <p>최근 ${escapeHtml(selectedWindowLabel)}을 묶어, 구조가 실제 구매 전환으로 얼마나 이어지는지 봐요.</p>
                </div>
            </div>
            <p class="brand-panel-compare"><strong>이 차트는 이런 때 봐요.</strong> 최근 7일, 30일처럼 기간을 묶어 구조 상태가 구매 전환으로 이어지고 있는지 볼 때 좋아요.</p>
            <div class="brand-explainer-card">
                <p><strong>BII</strong>는 이 브랜드 구조가 최근 실제 구매 전환으로 얼마나 이어지고 있는지 보는 값이에요.</p>
                <p>Daily Pulse가 하루 흐름이라면, BII는 최근 ${escapeHtml(selectedWindowLabel)}처럼 기간을 묶어 본 구조 상태예요. 각 점은 하루 값이 아니라, 그날까지의 최근 ${escapeHtml(selectedWindowLabel)}을 묶어 계산한 값이에요.</p>
            </div>
            <p class="brand-timeline-context"><strong>${escapeHtml(selectedWindowLabel)} 기준은 이렇게 보면 돼요.</strong> ${escapeHtml(selectedWindowGuide)}</p>
            <div class="brand-mini-chart-head">
                <h3>${brandRenderMetricName(`최근 ${selectedWindowLabel} 기준 구매 전환`, 'BII')}</h3>
                <span>${escapeHtml(brandFormatDelta(timeline.impactDelta))}</span>
            </div>
            <div class="brand-mini-chart-wrap">
                <canvas id="brand-impact-timeline"></canvas>
            </div>
            ${timeline.note ? `<p class="brand-timeline-note">${escapeHtml(timeline.note)}</p>` : ''}
        </article>
    `;
}

function renderBrandTimelineSection(model) {
    return `
        <section class="card brand-timeline-card">
            <div class="brand-section-head">
                <div>
                    <h2>요즘 실제 구매 흐름은 어떤가?</h2>
                    <p>하루 단위 흐름은 메인 차트에서 보고, 최근 기간 기준 구매 전환은 아래에서 보조로 확인해요.</p>
                </div>
            </div>
            <div class="brand-trend-stack">
                ${renderBrandPulsePanel(model.dailyPulse)}
                ${renderBrandTimelineSupportPanel(model.timeline)}
            </div>
        </section>
    `;
}

function renderBrandDashboard() {
    destroyCarts();
    const container = document.getElementById('content-area');
    const topBarActions = document.getElementById('top-bar-actions');
    const model = buildBrandDashboardModel();

    const healthMeta = model.brandRow?.Confidence_Index || '-';
    const impactMeta = `최근 ${formatNumber(model.impactWindowDays, 0)}일`;
    const healthDetailSection = renderBrandComponentGroup(model.componentGroups[0]);
    const impactDetailSection = renderBrandComponentGroup(model.componentGroups[1]);

    if (topBarActions) {
        topBarActions.innerHTML = `
            <div class="brand-topbar-window">
                <span class="brand-topbar-window-label">구매 전환 기준 기간</span>
                <div class="brand-timeline-toggle brand-window-toggle">
                    ${BRAND_DIAGNOSTIC_WINDOW_ORDER.map((windowDays) => `
                        <button
                            class="btn-primary ${model.impactWindowDays === windowDays ? 'is-active' : ''}"
                            type="button"
                            onclick="setBrandWindow(${windowDays})"
                            ${model.diagnosticMatrices?.availableWindows?.includes(windowDays) ? '' : 'disabled'}
                        >${formatNumber(windowDays, 0)}일</button>
                    `).join('')}
                </div>
            </div>
        `;
    }

    container.innerHTML = `
        <div class="brand-dashboard animate-fade-in">
            <section class="brand-state-grid">
                ${renderBrandStateCard({ label: '이 브랜드 구조는 건강한가', abbr: 'BHI' }, '유입, 확장, 반복 구조가 균형 있게 버티는지 보는 값이에요.', model.healthCurrentScore, model.healthDelta, model.healthInterpretation, model.healthPrimaryReason, model.healthStrengthPoint, model.healthWeakPoint, '신뢰도', healthMeta, 'health', 'brand-health-radar-inline', healthDetailSection)}
                ${renderBrandStateCard({ label: '이 브랜드 구조는 실제 구매로 이어지고 있나', abbr: 'BII' }, `최근 ${formatNumber(model.impactWindowDays, 0)}일 기준으로, 이 브랜드 구조가 실제 구매 전환으로 얼마나 이어지고 있는지 보는 값이에요.`, model.impactCurrentScore, model.impactDelta, model.impactInterpretation, model.impactPrimaryReason, model.impactStrengthPoint, model.impactWeakPoint, '기준 기간', impactMeta, 'impact', 'brand-impact-radar-inline', impactDetailSection)}
            </section>

            ${renderBrandDiagnosticSection(model)}

            ${renderBrandTimelineSection(model)}

            <section class="card brand-next-cta-card">
                <div class="brand-section-head">
                    <div>
                        <h2>제품별 기여를 더 자세히 보려면</h2>
                        <p>제품 단위 구조 기여, 구매 전환 기여, 수요 흐름과 역할 맵은 제품 분석 화면에서 이어서 볼 수 있어요.</p>
                    </div>
                    <a class="btn-primary brand-next-cta-link" href="../products/">제품 분석으로 이동</a>
                </div>
                <p class="brand-next-cta-note">브랜드 페이지에서는 현재 브랜드 상태와 진단에 집중하고, 제품별 drill-down과 구조 맵은 제품 분석 화면으로 분리했어요.</p>
            </section>
        </div>
    `;

    renderBrandStateRadarCharts(model);
    renderBrandRoleMapChart(model);
    renderBrandTimelineCharts(model);
    applyFriendlyUi(container);
}

function createBrandRadarChart(canvas, labels, values, tone) {
    if (!canvas) return;
    const color = tone === 'impact' ? '#d97706' : '#2563eb';
    const fill = tone === 'impact' ? 'rgba(217, 119, 6, 0.16)' : 'rgba(37, 99, 235, 0.16)';
    return new Chart(canvas.getContext('2d'), {
        type: 'radar',
        data: {
            labels,
            datasets: [{
                data: values,
                borderColor: color,
                backgroundColor: fill,
                pointBackgroundColor: color,
                borderWidth: 2.2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: (ctx) => `${ctx.label}: ${formatNumber(ctx.raw, 1)}`
                    }
                }
            },
            scales: {
                r: {
                    min: 0,
                    max: 100,
                    ticks: { display: false, stepSize: 20 },
                    angleLines: { color: 'rgba(148, 163, 184, 0.18)' },
                    grid: { color: 'rgba(148, 163, 184, 0.18)' },
                    pointLabels: {
                        color: '#475569',
                        font: { size: 11 }
                    }
                }
            }
        }
    });
}

function renderBrandStateRadarCharts(model) {
    const healthCanvas = document.getElementById('brand-health-radar-inline');
    const impactCanvas = document.getElementById('brand-impact-radar-inline');
    if (healthCanvas) {
        const healthItems = model.componentGroups[0].items;
        AppState.charts.brandHealthRadar = createBrandRadarChart(
            healthCanvas,
            healthItems.map((item) => item.chartLabel || item.label),
            healthItems.map((item) => toNumber(item.score, 0)),
            'health'
        );
    }

    if (impactCanvas) {
        const impactItems = model.componentGroups[1].items;
        AppState.charts.brandImpactRadar = createBrandRadarChart(
            impactCanvas,
            impactItems.map((item) => item.chartLabel || item.label),
            impactItems.map((item) => toNumber(item.score, 0)),
            'impact'
        );
    }
}

function renderBrandRoleMapChart(model) {
    const canvas = document.getElementById('brand-role-map');
    if (!canvas) return;

    const roleOrder = ['growth', 'acquisition', 'expansion', 'peripheral'];
    const demandRef = Math.max(brandPercentile(model.productRows.map((row) => row.expectedDemand), 0.9), 1);
    const datasets = roleOrder.map((roleKey) => ({
        label: BRAND_ROLE_META[roleKey].label,
        data: model.productRows
            .filter((row) => row.roleKey === roleKey)
            .map((row) => ({
                x: row.entry,
                y: row.expansion,
                r: 7 + (brandNormalizeByReference(row.expectedDemand, demandRef) * 19),
                roleKey,
                roleLabel: row.roleLabel,
                name: row.name,
                expectedDemand: row.expectedDemand
            })),
        backgroundColor: BRAND_ROLE_META[roleKey].color,
        borderColor: '#ffffff',
        borderWidth: 1.5
    }));

    const xMax = Math.max(brandPercentile(model.productRows.map((row) => row.entry), 0.95), model.centers.entry) * 1.15 || 1;
    const yMax = Math.max(brandPercentile(model.productRows.map((row) => row.expansion), 0.95), model.centers.expansion) * 1.15 || 1;

    const quadrantPlugin = {
        id: 'brandQuadrantBackground',
        beforeDraw(chart) {
            const { ctx, chartArea, scales } = chart;
            if (!chartArea || !scales.x || !scales.y) return;
            const centerX = scales.x.getPixelForValue(model.centers.entry);
            const centerY = scales.y.getPixelForValue(model.centers.expansion);
            ctx.save();
            ctx.fillStyle = 'rgba(37, 99, 235, 0.06)';
            ctx.fillRect(centerX, chartArea.top, chartArea.right - centerX, centerY - chartArea.top);
            ctx.fillStyle = 'rgba(15, 118, 110, 0.06)';
            ctx.fillRect(centerX, centerY, chartArea.right - centerX, chartArea.bottom - centerY);
            ctx.fillStyle = 'rgba(139, 92, 246, 0.06)';
            ctx.fillRect(chartArea.left, chartArea.top, centerX - chartArea.left, centerY - chartArea.top);
            ctx.fillStyle = 'rgba(148, 163, 184, 0.08)';
            ctx.fillRect(chartArea.left, centerY, centerX - chartArea.left, chartArea.bottom - centerY);
            ctx.strokeStyle = 'rgba(100, 116, 139, 0.45)';
            ctx.setLineDash([6, 6]);
            ctx.beginPath();
            ctx.moveTo(centerX, chartArea.top);
            ctx.lineTo(centerX, chartArea.bottom);
            ctx.moveTo(chartArea.left, centerY);
            ctx.lineTo(chartArea.right, centerY);
            ctx.stroke();
            ctx.setLineDash([]);
            ctx.fillStyle = '#334155';
            ctx.font = '12px Inter';
            ctx.fillText('성장 엔진', chartArea.right - 72, chartArea.top + 18);
            ctx.fillText('확장 제품', chartArea.left + 12, chartArea.top + 18);
            ctx.fillText('유입 확보 제품', chartArea.right - 96, chartArea.bottom - 12);
            ctx.fillText('주변 제품', chartArea.left + 12, chartArea.bottom - 12);
            ctx.restore();
        }
    };

    AppState.charts.brandRoleMap = new Chart(canvas, {
        type: 'bubble',
        data: { datasets },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label(context) {
                            const raw = context.raw || {};
                            return [
                                raw.name,
                                `${raw.roleLabel}`,
                                `주간 예상 수요 ${formatNumber(raw.expectedDemand, 1)}`,
                                `유입 강도 ${formatNumber(raw.x, 3)}`,
                                `확장 강도 ${formatNumber(raw.y, 3)}`
                            ];
                        }
                    }
                }
            },
            scales: {
                x: {
                    min: 0,
                    max: xMax,
                    title: {
                        display: true,
                        text: '유입 강도'
                    },
                    grid: {
                        color: 'rgba(148, 163, 184, 0.15)'
                    }
                },
                y: {
                    min: 0,
                    max: yMax,
                    title: {
                        display: true,
                        text: '확장 강도'
                    },
                    grid: {
                        color: 'rgba(148, 163, 184, 0.15)'
                    }
                }
            }
        },
        plugins: [quadrantPlugin]
    });
}

function renderBrandTimelineCharts(model) {
    const pulseCanvas = document.getElementById('brand-daily-pulse-timeline');
    if (pulseCanvas && model.dailyPulse.rows.length) {
        const pulseValues = model.dailyPulse.rows.map((row) => toNumber(row.daily_bii_pulse, 0) * 100);
        AppState.charts.brandDailyPulseTimeline = new Chart(pulseCanvas, {
            type: 'line',
            data: {
                labels: model.dailyPulse.rows.map((row) => String(row.as_of_date || '').slice(5)),
                datasets: [{
                    label: '일별 실제 구매 흐름 (Daily Pulse)',
                    data: pulseValues,
                    borderColor: '#2563eb',
                    backgroundColor: 'rgba(37, 99, 235, 0.12)',
                    borderWidth: 2.5,
                    tension: 0.28,
                    pointRadius: 2,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                plugins: {
                    legend: { display: false }
                },
                scales: {
                    x: {
                        grid: { display: false }
                    },
                    y: {
                        grid: {
                            color: 'rgba(148, 163, 184, 0.10)',
                            drawTicks: false
                        },
                        ticks: {
                            display: false
                        },
                        border: {
                            display: false
                        }
                    }
                }
            }
        });
    }

    const impactCanvas = document.getElementById('brand-impact-timeline');
    if (!impactCanvas || !model.timeline.availableWindows.length || !model.timeline.impactRows.length) return;
    const impactValues = model.timeline.impactRows.map((row) => toNumber(row.bii, 0) * 100);

    AppState.charts.brandImpactTimeline = new Chart(impactCanvas, {
        type: 'line',
        data: {
            labels: model.timeline.impactRows.map((row) => String(row.as_of_date || '').slice(5)),
            datasets: [{
                label: '최근 기간 기준 구매 전환 (BII)',
                data: impactValues,
                borderColor: '#d97706',
                backgroundColor: 'rgba(217, 119, 6, 0.12)',
                borderWidth: 2.5,
                tension: 0.35,
                pointRadius: 2,
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: {
                    grid: { display: false }
                },
                y: {
                    grid: {
                        color: 'rgba(148, 163, 184, 0.18)'
                    },
                    ticks: {
                        maxTicksLimit: 5,
                        color: 'rgba(100, 116, 139, 0.7)',
                        font: {
                            size: 11
                        },
                        callback(value) {
                            return formatNumber(value, 1);
                        }
                    }
                }
            }
        }
    });
}

window.setBrandWindow = (windowDays) => {
    AppState.viewState.brand.brandWindowDays = toNumber(windowDays, 30);
    renderBrandDashboard();
};

window.renderBrandDashboard = renderBrandDashboard;
