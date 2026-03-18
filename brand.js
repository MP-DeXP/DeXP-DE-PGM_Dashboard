const BRAND_ROLE_META = {
    growth: { label: '성장 엔진', color: '#2563eb', summary: '유입과 확장을 함께 끌어주는 중심 제품이에요.' },
    acquisition: { label: '유입 확보 제품', color: '#0f766e', summary: '첫 진입은 강하지만 다음 확장 연결은 더 보강할 여지가 있어요.' },
    expansion: { label: '확장 제품', color: '#8b5cf6', summary: '확장과 반복 구매를 살려 주는 후속 연결 제품이에요.' },
    peripheral: { label: '주변 제품', color: '#94a3b8', summary: '현재 구조에서는 주변 역할에 머무는 제품이에요.' }
};

const BRAND_COMPONENT_GROUPS = {
    health: '구조 건강도 해석',
    impact: '구매 활성도 해석'
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
        shortDescription: '반복 구매가 구매 활성도로 이어지는 힘을 봐요.',
        description: '반복 구매 깊이와 최근 운영 구간의 구매 활성도 힘을 함께 봐요.'
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
        loopStability: '고객이 돌아오는 구조는 있지만 아직 안정적으로 굳지는 않았어요.',
        roleBalance: '제품 역할이 한쪽에 몰려 있어 균형이 부족해요.'
    },
    impact: {
        coreInfluence: '핵심 제품은 보이지만 구매 활성도를 끌어당기는 힘은 아직 약해요.',
        graphStrength: '제품 사이 연결이 얇아 구매 활성도가 넓게 퍼지지 않아요.',
        returnPower: '고객이 돌아오는 힘이 약해서 구매 활성도로 충분히 이어지지 않아요.',
        hubConcentration: '소수 제품이 끌고는 있지만 그만큼 의존도도 높아요.'
    }
};

const BRAND_TIMELINE_WINDOW_ORDER = [7, 30, 90, 365];
const BRAND_TIMELINE_WINDOW_GUIDE = {
    7: '최근 며칠 사이 흐름이 갑자기 붙거나 꺾였는지 볼 때 좋아요.',
    30: '최근 한 달 운영이 구매 활성도로 이어지고 있는지 볼 때 좋아요.',
    90: '단기 흔들림을 덜 타고, 구매 활성도가 중기적으로 좋아지는지 볼 때 좋아요.',
    365: '시즌성까지 포함해 구매 활성도가 장기적으로 유지되는지 볼 때 좋아요.'
};
const BRAND_PULSE_LOOKBACK_DAYS = 90;
const BRAND_DIAGNOSTIC_WINDOW_ORDER = [7, 30, 90];
const BRAND_MATRIX_RATIO_MAX = 1.6;

const BRAND_DIAGNOSTIC_META = {
    structure: {
        title: '판매 구조와 구매 활성도',
        description: '판매 구조와 최근 구매 흐름을 같이 보면 문제의 종류를 더 빨리 읽을 수 있어요.',
        xAxisLabel: '판매 구조',
        yAxisLabel: '최근 1년 대비 구매 활성도',
        quadrants: {
            highHigh: {
                label: '최우수',
                summary: '구조도 좋고 최근 구매 활성도도 함께 잘 작동하고 있어요.',
                tone: 'mint'
            },
            highLow: {
                label: '실행력 저하',
                summary: '구조는 좋은데 최근 운영 난조로 구매 활성도가 약해졌어요.',
                tone: 'amber'
            },
            lowHigh: {
                label: '단기 펌핑',
                summary: '단기 구매 활성도는 높지만 구조 리스크 점검이 필요해요.',
                tone: 'orange'
            },
            lowLow: {
                label: '전면 개선 필요',
                summary: '구조와 최근 구매 활성도가 모두 약해 기초부터 다시 봐야 해요.',
                tone: 'rose'
            }
        }
    },
    revenue: {
        title: '매출과 구매 활성도',
        description: '매출과 최근 구매 흐름을 함께 보면 지금 성과가 건강한지 같이 읽을 수 있어요.',
        xAxisLabel: '최근 1년 대비 매출',
        yAxisLabel: '최근 1년 대비 구매 활성도',
        quadrants: {
            highHigh: {
                label: '건강한 성장',
                summary: '매출과 구조 기반 구매 활성도가 함께 올라가는 이상적인 상태예요.',
                tone: 'mint'
            },
            highLow: {
                label: '구조 리스크',
                summary: '매출은 버티지만 구조 기반 구매 활성도는 약해지고 있어요.',
                tone: 'rose'
            },
            lowHigh: {
                label: '장기 개선 신호',
                summary: '매출은 정체돼 보여도 내부 구매 활성도 기반은 좋아지고 있어요.',
                tone: 'sky'
            },
            lowLow: {
                label: '동반 침체',
                summary: '매출과 구매 활성도가 함께 약해지고 있어 근본 점검이 필요해요.',
                tone: 'slate'
            }
        }
    }
};

const BRAND_PURCHASE_DRIVER_META = {
    customers: {
        key: 'customers',
        label: '구매 고객',
        engineLabel: 'Customers',
        valueField: 'active_customers_t',
        contributionField: 'customers_contribution'
    },
    repeat: {
        key: 'repeat',
        label: '반복 구매율',
        engineLabel: 'Repeat',
        valueField: 'repeat_rate_t',
        contributionField: 'repeat_contribution'
    },
    attach: {
        key: 'attach',
        label: '장바구니 확장도',
        engineLabel: 'Attach',
        valueField: 'attach_rate_t',
        contributionField: 'attach_contribution'
    },
    clv: {
        key: 'clv',
        label: '고객 가치',
        engineLabel: 'CLV',
        valueField: 'avg_clv_t',
        contributionField: 'clv_contribution'
    }
};

const BRAND_PURCHASE_DRIVER_ORDER = ['customers', 'repeat', 'attach', 'clv'];

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

function brandFormatCompactInteger(value) {
    if (!Number.isFinite(value)) return '-';
    return formatNumber(Math.round(value), 0);
}

function brandFormatCurrencyValue(value) {
    if (!Number.isFinite(value)) return '-';
    return `₩${formatNumber(Math.round(value), 0)}`;
}

function brandNormalizeSignedPercent(value) {
    const numeric = toNumber(value, NaN);
    if (!Number.isFinite(numeric)) return null;
    return Math.abs(numeric) <= 1.5 ? numeric * 100 : numeric;
}

function brandFormatSignedPercent(value) {
    const normalized = brandNormalizeSignedPercent(value);
    if (!Number.isFinite(normalized)) return '→ 안정';
    if (Math.abs(normalized) < 0.5) return '→ 안정';
    return normalized > 0
        ? `▲ +${formatNumber(normalized, 0)}%`
        : `▼ ${formatNumber(normalized, 0)}%`;
}

function brandGetAvailableWindowValues(rows) {
    const available = new Set((rows || [])
        .map((row) => toNumber(row.window_days, null))
        .filter((value) => BRAND_DIAGNOSTIC_WINDOW_ORDER.includes(value)));
    return BRAND_DIAGNOSTIC_WINDOW_ORDER.filter((value) => available.has(value));
}

function brandSelectPageWindow(driverRows, biiRows, timeseries, structureRows = []) {
    const available = new Set([
        ...brandGetAvailableWindowValues(driverRows),
        ...brandGetAvailableWindowValues(biiRows),
        ...brandGetAvailableWindowValues(timeseries),
        ...brandGetAvailableWindowValues(structureRows)
    ]);
    const ordered = BRAND_DIAGNOSTIC_WINDOW_ORDER.filter((value) => available.has(value));
    if (!ordered.length) return {
        selectedWindowDays: 30,
        availableWindows: []
    };
    const preferred = toNumber(AppState.viewState.brand.brandWindowDays, 30);
    const selectedWindowDays = ordered.includes(preferred) ? preferred : (ordered.includes(30) ? 30 : ordered[0]);
    AppState.viewState.brand.brandWindowDays = selectedWindowDays;
    return { selectedWindowDays, availableWindows: ordered };
}

function brandGetLatestRowForWindow(rows, windowDays) {
    const filtered = (rows || [])
        .filter((row) => toNumber(row.window_days, null) === windowDays && row.as_of_date)
        .sort((a, b) => (a.as_of_date < b.as_of_date ? -1 : 1));
    return filtered[filtered.length - 1] || null;
}

function brandFindPreviousRow(rows, windowDays, currentDate, daysBack = 28) {
    const filtered = (rows || [])
        .filter((row) => toNumber(row.window_days, null) === windowDays && row.as_of_date)
        .sort((a, b) => (a.as_of_date < b.as_of_date ? -1 : 1));
    const currentTs = toDate(currentDate)?.getTime();
    if (!currentTs) return null;
    const targetTs = currentTs - (daysBack * 24 * 60 * 60 * 1000);
    let candidate = null;
    filtered.forEach((row) => {
        const rowTs = toDate(row.as_of_date)?.getTime();
        if (!rowTs || rowTs > targetTs) return;
        if (!candidate || rowTs > toDate(candidate.as_of_date).getTime()) candidate = row;
    });
    return candidate;
}

function brandResolveDriverLabel(driverKey) {
    const normalized = String(driverKey || '').trim().toLowerCase();
    if (['customers', 'active_customers', 'active customers'].includes(normalized)) return BRAND_PURCHASE_DRIVER_META.customers.label;
    if (['repeat', 'repeat_rate', 'repeat rate'].includes(normalized)) return BRAND_PURCHASE_DRIVER_META.repeat.label;
    if (['attach', 'attach_rate', 'attach rate'].includes(normalized)) return BRAND_PURCHASE_DRIVER_META.attach.label;
    if (['clv', 'avg_clv', 'customer_value', 'customer value'].includes(normalized)) return BRAND_PURCHASE_DRIVER_META.clv.label;
    return driverKey || '';
}

function brandNormalizeUiCopy(text) {
    return String(text || '')
        .replaceAll('활성 고객', '구매 고객')
        .replaceAll('브랜드 구매력', '구매 활성도')
        .replaceAll('active customers', 'purchase customers')
        .replaceAll('Active Customers', 'Purchase Customers')
        .replaceAll('반등하고 있습니다', '반등하고 있어요')
        .replaceAll('상승하고 있습니다', '올라가고 있어요')
        .replaceAll('하락하고 있습니다', '낮아지고 있어요')
        .replaceAll('회복하고 있습니다', '회복되고 있어요')
        .replaceAll('최근 구매 고객 회복으로', '최근 구매 고객이 회복되며')
        .replaceAll('최근 활성 고객 회복으로', '최근 구매 고객이 회복되며')
        .trim();
}

function brandBuildHeroStateFromMomentum(momentum, deltaPercent) {
    if (!Number.isFinite(momentum)) return '안정적';
    if (momentum < 1 && Number.isFinite(deltaPercent) && deltaPercent > 1) return '반등 중';
    if (momentum > 1.1) return '좋아지는 중';
    if (momentum < 0.9) return '약해지는 중';
    return '안정적';
}

function brandBuildHeroDirectionSentence(windowDays, momentum) {
    if (!Number.isFinite(momentum)) return `최근 ${formatNumber(windowDays, 0)}일 기준 구매 활성도를 아직 충분히 읽지 못하고 있어요.`;
    if (Math.abs(momentum - 1) < 0.03) {
        return `최근 ${formatNumber(windowDays, 0)}일 구매 활성도가 최근 1년 평균과 비슷해요.`;
    }
    return momentum >= 1
        ? `최근 ${formatNumber(windowDays, 0)}일 구매 활성도가 최근 1년 평균보다 높아요.`
        : `최근 ${formatNumber(windowDays, 0)}일 구매 활성도가 최근 1년 평균보다 낮아요.`;
}

function brandBuildHeroFlowSentence(stateLabel) {
    const label = String(stateLabel || '').trim();
    if (label === '반등 중') return '최근 흐름은 약한 구간에서 다시 살아나고 있어요.';
    if (label === '좋아지는 중') return '최근 흐름은 계속 좋아지는 쪽에 가까워요.';
    if (label === '약해지는 중') return '최근 흐름은 다시 약해지는 쪽으로 움직이고 있어요.';
    return '최근 흐름은 크게 흔들리지 않고 있어요.';
}

function brandNormalizeHeroStateLabel(label) {
    const normalized = String(label || '').trim();
    if (normalized === '회복 중') return '반등 중';
    if (normalized === '강화 중') return '좋아지는 중';
    if (normalized === '약화 중') return '약해지는 중';
    if (normalized === '안정') return '안정적';
    return normalized || '안정적';
}

function brandGetFallbackHeroCauses(componentScores) {
    const impactKeys = ['returnPower', 'graphStrength', 'coreInfluence', 'hubConcentration']
        .filter((key) => Number.isFinite(componentScores[key]))
        .sort((a, b) => componentScores[a] - componentScores[b])
        .slice(0, 2);
    const fallbackMap = {
        returnPower: '반복 구매 힘 약화',
        graphStrength: '제품 간 연결 약화',
        coreInfluence: '핵심 제품 힘 약화',
        hubConcentration: '허브 의존 심화'
    };
    return impactKeys.map((key) => fallbackMap[key]).filter(Boolean);
}

function brandBuildHeroModel(driverRows, currentImpactRow, biiRows, timeseries, componentScores, impactInterpretation, selectedWindowDays) {
    const latestDriverRow = brandGetLatestRowForWindow(driverRows, selectedWindowDays);
    if (latestDriverRow) {
        const momentum = toNumber(firstDefinedValue(latestDriverRow.momentum_t, brandSafeDivide(latestDriverRow.bii_t, Math.max(latestDriverRow.bii_365, 0.0001), 0)), NaN);
        const directionValue = brandNormalizeSignedPercent(latestDriverRow.momentum_delta_pct);
        const causes = [latestDriverRow.top_driver_1, latestDriverRow.top_driver_2]
            .map((value) => brandResolveDriverLabel(value))
            .filter(Boolean);
        return {
            empty: false,
            source: 'driver',
            eyebrow: '구매 활성도 상태',
            stateLabel: brandNormalizeHeroStateLabel(latestDriverRow.momentum_state || brandBuildHeroStateFromMomentum(momentum, directionValue)),
            directionLabel: brandFormatSignedPercent(latestDriverRow.momentum_delta_pct),
            directionSentence: brandBuildHeroDirectionSentence(selectedWindowDays, momentum),
            summary: brandNormalizeUiCopy(latestDriverRow.hero_summary || impactInterpretation),
            causes: causes.length ? causes : brandGetFallbackHeroCauses(componentScores)
        };
    }

    const current365Row = (biiRows || []).find((row) => toNumber(row.window_days, null) === 365) || null;
    if (!currentImpactRow || !current365Row) {
        return {
            empty: true,
            eyebrow: '구매 활성도 상태',
            note: '구매 활성도 상태를 계산할 데이터가 아직 부족해요.'
        };
    }

    const momentum = brandSafeDivide(toNumber(currentImpactRow.bii, 0), Math.max(toNumber(current365Row.bii, 0), 0.0001), 0);
    const ratioPairs = brandBuildRatioPairs(timeseries, selectedWindowDays, 'bii');
    const currentPair = ratioPairs[ratioPairs.length - 1] || null;
    const previousPair = currentPair ? brandFindPreviousPair(ratioPairs, currentPair.as_of_date, 28) : null;
    const deltaPercent = currentPair && previousPair && previousPair.ratio > 0
        ? ((currentPair.ratio - previousPair.ratio) / previousPair.ratio) * 100
        : null;
    return {
        empty: false,
        source: 'fallback',
        eyebrow: '구매 활성도 상태',
        stateLabel: brandNormalizeHeroStateLabel(brandBuildHeroStateFromMomentum(momentum, deltaPercent)),
        directionLabel: brandFormatSignedPercent(deltaPercent),
        directionSentence: brandBuildHeroDirectionSentence(selectedWindowDays, momentum),
        summary: impactInterpretation,
        causes: brandGetFallbackHeroCauses(componentScores),
        helper: '드라이버 시계열이 없어 현재 구매 활성도 해석으로 축소 표시하고 있어요.'
    };
}

function brandFormatMomentumValue(momentum) {
    if (!Number.isFinite(momentum)) return '-';
    return `${formatNumber(momentum, 2)}x`;
}

function brandBuildHeroMetrics(selectedWindowDays, revenueTimeseries) {
    const revenueCurrentRow = brandGetLatestRowForWindow(revenueTimeseries, selectedWindowDays);
    const revenuePreviousRow = revenueCurrentRow
        ? brandFindPreviousRow(revenueTimeseries, selectedWindowDays, revenueCurrentRow.as_of_date, 28)
        : null;
    const revenueDeltaPct = revenueCurrentRow && revenuePreviousRow && Math.abs(toNumber(revenuePreviousRow.revenue_t, 0)) > 0.000001
        ? ((toNumber(revenueCurrentRow.revenue_t, 0) - toNumber(revenuePreviousRow.revenue_t, 0)) / toNumber(revenuePreviousRow.revenue_t, 0)) * 100
        : null;

    return [{
        label: '매출',
        value: revenueCurrentRow ? brandFormatCurrencyValue(toNumber(revenueCurrentRow.revenue_t, NaN)) : '데이터 없음',
        meta: revenueCurrentRow ? brandFormatSignedPercent(revenueDeltaPct) : '데이터 없음'
    }];
}

function brandBuildDriverInterpretation(meta, deltaPercent, contribution) {
    if (meta.key === 'customers') {
        if (Number.isFinite(deltaPercent) && deltaPercent > 0) return '최근 구매 고객이 늘고 있어요.';
        if (Number.isFinite(deltaPercent) && deltaPercent < 0) return '최근 구매 고객이 줄고 있어요.';
    }
    if (meta.key === 'repeat') {
        if (Number.isFinite(deltaPercent) && deltaPercent < 0) return '최근 반복 구매가 감소했습니다.';
        if (Number.isFinite(deltaPercent) && deltaPercent > 0) return '최근 반복 구매가 회복되고 있어요.';
    }
    if (meta.key === 'attach') {
        if (Number.isFinite(deltaPercent) && deltaPercent < 0) return '장바구니 확장이 약해지고 있어요.';
        if (Number.isFinite(deltaPercent) && deltaPercent > 0) return '장바구니 확장이 다시 넓어지고 있어요.';
    }
    if (meta.key === 'clv') {
        if (Number.isFinite(deltaPercent) && deltaPercent > 0) return '최근 고객 가치가 올라가고 있어요.';
        if (Number.isFinite(deltaPercent) && deltaPercent < 0) return '최근 고객 가치가 낮아지고 있어요.';
    }
    if (Number.isFinite(contribution) && contribution > 0) return '최근 변화가 전체 흐름을 받치고 있어요.';
    if (Number.isFinite(contribution) && contribution < 0) return '최근 변화가 전체 흐름을 약하게 만들고 있어요.';
    return '최근 변화는 크지 않지만 같이 볼 필요가 있어요.';
}

function brandFormatDriverValue(driverKey, value) {
    if (!Number.isFinite(value)) return '-';
    if (driverKey === 'customers') return brandFormatCompactInteger(value);
    if (driverKey === 'repeat') return formatPercent(value, 0);
    if (driverKey === 'attach') return formatNumber(value, 2);
    return brandFormatCurrencyValue(value);
}

function brandBuildDriversModel(driverRows, selectedWindowDays) {
    const latestRow = brandGetLatestRowForWindow(driverRows, selectedWindowDays);
    if (!latestRow) {
        return {
            empty: true,
            note: '구매 활성도 드라이버 데이터가 없어 원인 카드들은 아직 표시하지 않아요.',
            helper: '이 섹션은 purchase_activation_driver_timeseries.csv가 있을 때 렌더돼요.',
            cards: []
        };
    }

    const previousRow = brandFindPreviousRow(driverRows, selectedWindowDays, latestRow.as_of_date, 28);
    const cards = BRAND_PURCHASE_DRIVER_ORDER.map((driverKey) => {
        const meta = BRAND_PURCHASE_DRIVER_META[driverKey];
        const currentValue = toNumber(latestRow[meta.valueField], NaN);
        const previousValue = toNumber(previousRow?.[meta.valueField], NaN);
        const contribution = toNumber(latestRow[meta.contributionField], NaN);
        const deltaPercent = Number.isFinite(currentValue) && Number.isFinite(previousValue) && Math.abs(previousValue) > 0.000001
            ? ((currentValue - previousValue) / previousValue) * 100
            : null;
        return {
            ...meta,
            value: currentValue,
            displayValue: brandFormatDriverValue(driverKey, currentValue),
            deltaPercent,
            deltaLabel: brandFormatSignedPercent(deltaPercent),
            contribution,
            interpretation: brandBuildDriverInterpretation(meta, deltaPercent, contribution)
        };
    });

    return {
        empty: false,
        cards
    };
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

function brandFormatRatioPercent(value, digits = 0) {
    if (!Number.isFinite(value)) return '-';
    if (value > 0 && value < 0.01) return '<1%';
    return `${formatNumber(value * 100, digits)}%`;
}

function brandGetStructureIndicatorLevel(score) {
    if (!Number.isFinite(score)) return 0;
    return Math.max(1, Math.min(5, Math.round(brandClamp(score / 100, 0, 1) * 5)));
}

function brandBuildStructureIndicator(level) {
    const safeLevel = Math.max(0, Math.min(5, toNumber(level, 0)));
    return Array.from({ length: 5 }, (_, index) => `
        <span class="brand-structure-indicator-dot ${index < safeLevel ? 'is-filled' : ''}"></span>
    `).join('');
}

function brandBuildEntryStructureCard(structureRow, productRows, totalFirstCustomers, brandRow, selectedWindowDays, score) {
    const snapshotRatio = toNumber(structureRow?.entry_product_ratio, NaN);
    const snapshotTopShare = toNumber(structureRow?.entry_top_product_share, NaN);
    if (Number.isFinite(snapshotRatio)) {
        const entryCardScore = brandClamp(snapshotRatio / 0.35, 0, 1) * 100;
        let interpretation = '첫 구매가 여러 제품에서 비교적 고르게 시작되고 있습니다.';
        if (snapshotRatio < 0.12) interpretation = '첫 구매가 일부 제품에만 몰려 있습니다.';
        else if (snapshotRatio < 0.25) interpretation = '첫 구매가 몇 개 핵심 제품 중심으로 만들어지고 있습니다.';
        const metrics = [{
            label: '첫 구매를 만든 제품 비율',
            value: brandFormatRatioPercent(snapshotRatio, 0)
        }];
        if (Number.isFinite(snapshotTopShare)) {
            metrics.push({
                label: '대표 시작 제품 비중',
                value: brandFormatRatioPercent(snapshotTopShare, 0)
            });
        }
        return {
            key: 'entry',
            label: '첫 구매 시작 제품',
            basisLabel: `최근 ${formatNumber(selectedWindowDays, 0)}일 기준`,
            score: entryCardScore,
            indicatorLevel: brandGetStructureIndicatorLevel(entryCardScore),
            metrics,
            interpretation
        };
    }

    const validRows = (productRows || []).filter((row) => Math.max(0, toNumber(row.expectedDemand, 0)) > 0);
    const entryProducts = validRows.filter((row) => Math.max(0, toNumber(row.firstCustomers, 0)) > 0);
    const entryProductRatio = validRows.length ? entryProducts.length / validRows.length : NaN;
    const entryCardScore = Number.isFinite(entryProductRatio)
        ? brandClamp(entryProductRatio / 0.35, 0, 1) * 100
        : score;
    const aaConcentration = toNumber(brandRow?.AA_Concentration_Index, NaN);
    let interpretation = '유입 구조를 아직 충분히 읽지 못하고 있어요.';

    if (Number.isFinite(entryProductRatio)) {
        if (entryProductRatio < 0.12) interpretation = '첫 구매가 일부 제품에만 몰려 있습니다.';
        else if (entryProductRatio < 0.25) interpretation = '첫 구매가 몇 개 핵심 제품 중심으로 만들어지고 있습니다.';
        else interpretation = '첫 구매가 여러 제품에서 비교적 고르게 시작되고 있습니다.';
    } else if (Number.isFinite(aaConcentration)) {
        interpretation = aaConcentration > 0.6
            ? '유입 구조가 일부 제품에 치우쳐 있습니다.'
            : '유입 구조가 비교적 고르게 분산되어 있습니다.';
    }

    const metrics = Number.isFinite(entryProductRatio)
        ? [{
            label: '첫 구매를 만든 제품 비율',
            value: brandFormatRatioPercent(entryProductRatio, 0)
        }]
        : [{ label: 'AA 집중도', value: formatNumber(aaConcentration, 2) }];

    return {
        key: 'entry',
        label: '첫 구매 시작 제품',
        basisLabel: '현재 구조 기준',
        score: entryCardScore,
        indicatorLevel: brandGetStructureIndicatorLevel(entryCardScore),
        metrics,
        interpretation
    };
}

function brandBuildFlowStructureCard(structureRow, edgeRows, totalFirstCustomers, selectedWindowDays, score) {
    const snapshotTransitionRate = toNumber(structureRow?.flow_transition_rate, NaN);
    const snapshotTopPathShare = toNumber(structureRow?.flow_top_path_share, NaN);
    if (Number.isFinite(snapshotTransitionRate)) {
        const transitionStrength = brandClamp(snapshotTransitionRate / 0.35, 0, 1);
        const pathDiversityStrength = Number.isFinite(snapshotTopPathShare)
            ? brandClamp((0.6 - snapshotTopPathShare) / 0.6, 0, 1)
            : 0.5;
        const cardScore = transitionStrength * (0.7 + (pathDiversityStrength * 0.3)) * 100;
        let interpretation = '다음 구매로 비교적 자연스럽게 이어지고 있습니다.';
        if (snapshotTransitionRate < 0.25 && Number.isFinite(snapshotTopPathShare) && snapshotTopPathShare > 0.45) {
            interpretation = '구매 흐름이 일부 경로에만 집중되어 있습니다.';
        } else if (snapshotTransitionRate < 0.25) {
            interpretation = '다음 구매로 이어지는 흐름이 아직 약합니다.';
        } else if (Number.isFinite(snapshotTopPathShare) && snapshotTopPathShare > 0.45) {
            interpretation = '다음 구매 흐름은 생기지만 일부 경로에 집중되어 있습니다.';
        }
        const metrics = [
            {
                label: '첫 구매 뒤 다음 구매로 이어진 비율',
                value: brandFormatRatioPercent(snapshotTransitionRate, 1)
            }
        ];
        if (Number.isFinite(snapshotTopPathShare)) {
            metrics.push({
                label: '가장 많이 이어지는 경로 비중',
                value: brandFormatRatioPercent(snapshotTopPathShare, 1)
            });
        }
        return {
            key: 'flow',
            label: '다음 구매로 이어짐',
            basisLabel: `최근 ${formatNumber(selectedWindowDays, 0)}일 기준`,
            score: cardScore,
            indicatorLevel: brandGetStructureIndicatorLevel(cardScore),
            metrics,
            interpretation
        };
    }

    const totalTransitionCustomers = (edgeRows || []).reduce((sum, row) => sum + Math.max(0, toNumber(row.transition_customer_cnt, 0)), 0);
    const topPathCustomers = (edgeRows || []).reduce((max, row) => Math.max(max, Math.max(0, toNumber(row.transition_customer_cnt, 0))), 0);
    const transitionRate = totalFirstCustomers > 0
        ? brandClamp(brandSafeDivide(totalTransitionCustomers, totalFirstCustomers, 0))
        : NaN;
    const topPathShare = totalTransitionCustomers > 0
        ? brandSafeDivide(topPathCustomers, totalTransitionCustomers, NaN)
        : NaN;
    const transitionStrength = Number.isFinite(transitionRate)
        ? brandClamp(transitionRate / 0.35, 0, 1)
        : NaN;
    const pathDiversityStrength = Number.isFinite(topPathShare)
        ? brandClamp((0.6 - topPathShare) / 0.6, 0, 1)
        : 0.5;
    const normalizedScore = Number.isFinite(transitionStrength)
        ? transitionStrength * (0.7 + (pathDiversityStrength * 0.3))
        : brandClamp(score / 100, 0, 1);
    const cardScore = normalizedScore * 100;

    let interpretation = '구매 흐름 데이터를 아직 충분히 읽지 못하고 있어요.';
    if (Number.isFinite(transitionRate)) {
        if (transitionRate < 0.25 && Number.isFinite(topPathShare) && topPathShare > 0.45) {
            interpretation = '구매 흐름이 일부 경로에만 집중되어 있습니다.';
        } else if (transitionRate < 0.25) {
            interpretation = '다음 구매로 이어지는 흐름이 아직 약합니다.';
        } else if (Number.isFinite(topPathShare) && topPathShare > 0.45) {
            interpretation = '다음 구매 흐름은 생기지만 일부 경로에 집중되어 있습니다.';
        } else {
            interpretation = '다음 구매로 비교적 자연스럽게 이어지고 있습니다.';
        }
    }

    const metrics = [
        { label: '첫 구매 뒤 다음 구매로 이어진 비율', value: brandFormatRatioPercent(transitionRate, 1) }
    ];
    if (Number.isFinite(topPathShare)) {
        metrics.push({ label: '가장 많이 이어지는 경로 비중', value: brandFormatRatioPercent(topPathShare, 1) });
    }

    return {
        key: 'flow',
        label: '다음 구매로 이어짐',
        basisLabel: '현재 구조 기준',
        score: cardScore,
        indicatorLevel: brandGetStructureIndicatorLevel(cardScore),
        metrics,
        interpretation
    };
}

function brandBuildReturnStructureCard(structureRow, productRows, currentImpactRow, weightedRepurchase, score, selectedWindowDays) {
    const snapshotReturnRate = toNumber(structureRow?.return_customer_rate, NaN);
    const snapshotDemandShare = toNumber(structureRow?.return_product_demand_share, NaN);
    if (Number.isFinite(snapshotReturnRate)) {
        const returnScore = brandClamp(snapshotReturnRate / 0.4, 0, 1) * 100;
        let interpretation = '고객이 다시 돌아오는 구조가 비교적 안정적으로 만들어지고 있습니다.';
        if (snapshotReturnRate < 0.2) interpretation = '고객이 다시 돌아오는 구조가 아직 약합니다.';
        else if (snapshotReturnRate < 0.35) interpretation = '고객이 다시 돌아오지만 아직 충분히 두껍지는 않아요.';
        const metrics = [{
            label: '고객이 다시 돌아온 비율',
            value: brandFormatRatioPercent(snapshotReturnRate, 0)
        }];
        if (Number.isFinite(snapshotDemandShare)) {
            metrics.push({
                label: '고객이 돌아오는 제품 수요 비중',
                value: brandFormatRatioPercent(snapshotDemandShare, 0)
            });
        }
        return {
            key: 'return',
            label: '고객이 돌아오는 제품',
            basisLabel: `최근 ${formatNumber(selectedWindowDays, 0)}일 기준`,
            score: returnScore,
            indicatorLevel: brandGetStructureIndicatorLevel(returnScore),
            metrics,
            interpretation
        };
    }

    const repeatRate = toNumber(firstDefinedValue(currentImpactRow?.repeat_rate_t, weightedRepurchase), NaN);
    const validRows = (productRows || []).filter((row) => Number.isFinite(toNumber(row.loopRate, NaN)) || Number.isFinite(toNumber(row.repurchaseRate, NaN)));
    const returnProducts = validRows
        .map((row) => ({
            ...row,
            returnSignal: Math.max(toNumber(row.loopRate, 0), toNumber(row.repurchaseRate, 0))
        }))
        .filter((row) => row.returnSignal >= 0.35)
        .sort((a, b) => b.returnSignal - a.returnSignal);
    const totalDemand = validRows.reduce((sum, row) => sum + Math.max(0, toNumber(row.expectedDemand, 0)), 0);
    const returnDemandShare = totalDemand > 0
        ? returnProducts.reduce((sum, row) => sum + Math.max(0, toNumber(row.expectedDemand, 0)), 0) / totalDemand
        : NaN;
    let interpretation = '고객이 다시 돌아오는 구조를 아직 충분히 읽지 못하고 있어요.';
    if (Number.isFinite(repeatRate)) {
        if (repeatRate < 0.2) interpretation = '고객이 다시 돌아오는 구조가 아직 약합니다.';
        else if (repeatRate < 0.35) interpretation = '고객이 다시 돌아오지만 아직 충분히 두껍지는 않아요.';
        else interpretation = '고객이 다시 돌아오는 구조가 비교적 안정적으로 만들어지고 있습니다.';
    } else if (Number.isFinite(returnDemandShare)) {
        if (returnDemandShare < 0.2) interpretation = '고객이 돌아오는 제품이 아직 뚜렷하지 않습니다.';
        else if (returnDemandShare < 0.4) interpretation = '고객이 돌아오는 제품은 보이지만 아직 중심이 약합니다.';
        else interpretation = '고객이 돌아오는 제품이 비교적 뚜렷하게 형성되어 있습니다.';
    }

    const metrics = [];
    if (Number.isFinite(repeatRate)) {
        metrics.push({ label: '고객이 다시 돌아온 비율', value: brandFormatRatioPercent(repeatRate, 0) });
    } else if (Number.isFinite(returnDemandShare)) {
        metrics.push({ label: '고객이 돌아오는 제품 수요 비중', value: brandFormatRatioPercent(returnDemandShare, 0) });
    }

    return {
        key: 'return',
        label: '고객이 돌아오는 제품',
        basisLabel: Number.isFinite(toNumber(currentImpactRow?.repeat_rate_t, NaN)) ? `최근 ${formatNumber(selectedWindowDays, 0)}일 기준` : '현재 구조 기준',
        score,
        indicatorLevel: brandGetStructureIndicatorLevel(score),
        metrics,
        interpretation
    };
}

function brandBuildBasketStructureCard(structureRow, productRows, totalExpectedDemand, currentImpactRow, weightedAttach, selectedWindowDays) {
    const snapshotItemsPerOrder = toNumber(structureRow?.basket_items_per_order, NaN);
    const snapshotAttachRate = toNumber(structureRow?.basket_attach_rate, NaN);
    if (Number.isFinite(snapshotItemsPerOrder)) {
        const score = brandClamp((snapshotItemsPerOrder - 1) / 1.5, 0, 1) * 100;
        let interpretation = '함께 사는 구조가 비교적 잘 형성되어 있습니다.';
        if (snapshotItemsPerOrder < 1.5) interpretation = '동시 구매 구조가 약합니다.';
        else if (snapshotItemsPerOrder < 2) interpretation = '장바구니 확장이 일부 제품 중심으로 나타납니다.';
        const metrics = [{
            label: '평균 구매 제품 수',
            value: formatNumber(snapshotItemsPerOrder, 2)
        }];
        if (Number.isFinite(snapshotAttachRate)) {
            metrics.push({
                label: '장바구니 확장도',
                value: formatNumber(snapshotAttachRate, 2)
            });
        }
        return {
            key: 'basket',
            label: '장바구니 확장',
            basisLabel: `최근 ${formatNumber(selectedWindowDays, 0)}일 기준`,
            score,
            indicatorLevel: brandGetStructureIndicatorLevel(score),
            metrics,
            interpretation
        };
    }

    const weightedItemsPerOrder = totalExpectedDemand > 0
        ? brandSafeDivide(
            (productRows || []).reduce((sum, row) => {
                const cartSize = toNumber(row.medianCartSize, NaN);
                if (!Number.isFinite(cartSize) || cartSize <= 0) return sum;
                return sum + (cartSize * row.expectedDemand);
            }, 0),
            (productRows || []).reduce((sum, row) => {
                const cartSize = toNumber(row.medianCartSize, NaN);
                if (!Number.isFinite(cartSize) || cartSize <= 0) return sum;
                return sum + row.expectedDemand;
            }, 0),
            NaN
        )
        : NaN;
    const attachRate = toNumber(firstDefinedValue(currentImpactRow?.attach_rate_t, weightedAttach), NaN);
    const hasItemsPerOrder = Number.isFinite(weightedItemsPerOrder);
    const score = hasItemsPerOrder
        ? brandClamp((weightedItemsPerOrder - 1) / 1.5, 0, 1) * 100
        : brandClamp(brandSafeDivide(attachRate, 0.4, 0), 0, 1) * 100;

    let interpretation = '동시 구매 구조를 아직 충분히 읽지 못하고 있어요.';
    if (hasItemsPerOrder) {
        if (weightedItemsPerOrder < 1.5) interpretation = '동시 구매 구조가 약합니다.';
        else if (weightedItemsPerOrder < 2) interpretation = '장바구니 확장이 일부 제품 중심으로 나타납니다.';
        else interpretation = '함께 사는 구조가 비교적 잘 형성되어 있습니다.';
    } else if (Number.isFinite(attachRate)) {
        if (attachRate < 0.15) interpretation = '동시 구매 구조가 약합니다.';
        else if (attachRate < 0.3) interpretation = '장바구니 확장이 일부 제품 중심으로 나타납니다.';
        else interpretation = '함께 사는 구조가 비교적 잘 형성되어 있습니다.';
    }

    const metrics = hasItemsPerOrder
        ? [{ label: '평균 구매 제품 수', value: formatNumber(weightedItemsPerOrder, 2) }]
        : [{ label: '장바구니 확장도', value: formatNumber(attachRate, 2) }];

    if (!hasItemsPerOrder && Number.isFinite(attachRate)) {
        metrics.push({ label: 'Attach Rate', value: brandFormatRatioPercent(attachRate, 0) });
    }

    return {
        key: 'basket',
        label: '장바구니 확장',
        basisLabel: Number.isFinite(attachRate) ? `최근 ${formatNumber(selectedWindowDays, 0)}일 기준` : '현재 구조 기준',
        score,
        indicatorLevel: brandGetStructureIndicatorLevel(score),
        metrics,
        interpretation
    };
}

function brandBuildStructureCards(structureRow, productRows, edgeRows, totalFirstCustomers, totalExpectedDemand, brandRow, currentImpactRow, componentScores, weightedRepurchase, weightedAttach, selectedWindowDays) {
    return [
        brandBuildEntryStructureCard(structureRow, productRows, totalFirstCustomers, brandRow, selectedWindowDays, componentScores.entryDiversity),
        brandBuildFlowStructureCard(structureRow, edgeRows, totalFirstCustomers, selectedWindowDays, componentScores.expansionLadder),
        brandBuildReturnStructureCard(structureRow, productRows, currentImpactRow, weightedRepurchase, componentScores.loopStability, selectedWindowDays),
        brandBuildBasketStructureCard(structureRow, productRows, totalExpectedDemand, currentImpactRow, weightedAttach, selectedWindowDays)
    ];
}

function brandBuildStructureOverview(cards, healthCurrentScore, healthDelta, healthInterpretation, roleBalanceScore) {
    const ordered = [...(cards || [])].sort((a, b) => toNumber(b.score, 0) - toNumber(a.score, 0));
    const strongest = ordered[0] || null;
    const weakOrdered = [...(cards || [])].sort((a, b) => toNumber(a.score, 0) - toNumber(b.score, 0));
    const weakCandidates = weakOrdered.filter((card) => toNumber(card.score, 0) < 55).slice(0, 2);
    const weakItems = weakCandidates.length ? weakCandidates : weakOrdered.slice(0, 1);

    let summary = healthInterpretation;
    if (weakItems.length >= 2) {
        summary = `${weakItems[0].label}과 ${weakItems[1].label} 쪽 구조 보강이 필요해요.`;
    } else if (weakItems.length === 1 && toNumber(weakItems[0].score, 0) < 60) {
        summary = `${weakItems[0].label}이 어디서 막히는지 먼저 확인해 보세요.`;
    } else if (strongest) {
        summary = `${strongest.label} 구조가 비교적 강하고 전반도 안정적인 편이에요.`;
    }

    if (Number.isFinite(roleBalanceScore) && roleBalanceScore < 45) {
        summary += ' 제품 역할 균형도 함께 점검할 필요가 있어요.';
    }

    return {
        label: '판매 구조',
        score: healthCurrentScore,
        delta: healthDelta,
        strongest: strongest?.label || '-',
        weakest: weakItems.map((item) => item.label),
        summary
    };
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
            loopStability: '고객이 다시 돌아오는 구조가 비교적 안정적으로 유지되고 있어요.',
            roleBalance: '제품 역할이 한쪽으로 심하게 기울지 않았어요.',
            coreInfluence: '핵심 제품이 구매 활성도의 중심을 잡고 있어요.',
            graphStrength: '제품 사이 연결이 살아 있어 구매 활성도가 이어지고 있어요.',
            returnPower: '고객이 다시 돌아오는 힘이 구매 활성도로 이어지고 있어요.',
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
            loopStability: '고객이 돌아오는 구조가 꾸준히 만들어지지 않았어요.',
            roleBalance: '제품 역할이 한쪽에 몰려 있어 균형이 부족해요.',
            coreInfluence: '핵심 제품이 구매 활성도를 끌어당기는 힘이 아직 약해요.',
            graphStrength: '제품 사이 연결이 얇아 구매 활성도가 길게 이어지지 않아요.',
            returnPower: '고객이 돌아오는 힘이 약해서 구매 활성도로 충분히 이어지지 않아요.',
            hubConcentration: '구매 활성도가 소수 제품에 몰려 의존도가 높아요.'
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
            note: '최근 기간 기준 구매 활성도 데이터가 없어 이 비교는 아직 표시하지 않아요.'
        };
    }

    const preferredWindow = availableWindows.includes(impactWindowDays)
        ? impactWindowDays
        : availableWindows[0];

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
            note: '일별 구매 활성도 데이터가 없어 아직 표시하지 않아요.',
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
            note: '선택한 기간 또는 365일 기준 구매 활성도 데이터가 없어 이 진단은 아직 표시하지 않아요.'
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
            note: '매출 진단 데이터가 없어 매출과 구매 활성도 비교는 아직 표시하지 않아요.'
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
            note: '같은 날짜 기준 매출과 구매 활성도 데이터를 함께 읽을 수 없어 이 비교는 아직 표시하지 않아요.'
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
    const driverTimeseries = AppState.data.brandPurchaseDriverTimeseries || [];
    const structureTimeseries = AppState.data.brandStructureTimeseries || [];
    const { selectedWindowDays, availableWindows } = brandSelectPageWindow(
        driverTimeseries,
        AppState.data.biiWindow || [],
        AppState.data.brandImpactTimeseries || [],
        structureTimeseries
    );
    const defaultImpactRow = brandSelectCurrentImpactRow(AppState.data.biiWindow || [], selectedWindowDays);
    const timeseries = AppState.data.brandImpactTimeseries || [];
    const dailyPulseRows = AppState.data.brandImpactDailyPulse || [];
    const revenueTimeseries = AppState.data.brandRevenueTimeseries || [];
    const currentStructureRow = brandGetLatestRowForWindow(structureTimeseries, selectedWindowDays);
    const currentImpactRow = (AppState.data.biiWindow || []).find((row) => toNumber(row.window_days, null) === selectedWindowDays) || defaultImpactRow;

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
        const medianCartSize = toNumber(cart.median_cart_size, NaN);
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
            medianCartSize,
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
    const timeline = brandBuildTimelineModel(timeseries, toNumber(brandRow.BHI, 0), selectedWindowDays);
    const dailyPulse = brandBuildDailyPulseModel(dailyPulseRows);
    const diagnosticSeriesRows = (timeseries || [])
        .filter((row) => toNumber(row.window_days, null) === selectedWindowDays)
        .sort((a, b) => (a.as_of_date < b.as_of_date ? -1 : 1));
    const diagnosticLatestRow = diagnosticSeriesRows[diagnosticSeriesRows.length - 1] || null;
    const diagnosticHealthDelta = brandFindDelta(timeseries, 'bhi', selectedWindowDays, diagnosticLatestRow);
    const diagnosticImpactDelta = brandFindDelta(timeseries, 'bii', selectedWindowDays, diagnosticLatestRow);

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
                loop: '고객이 다시 돌아오는 구조를 만드는 핵심 축이에요.',
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
                demand: '기대 수요량이 커서 구매 활성도가 넓게 퍼져요.',
                core: '유입과 확장 강도가 함께 높아 구매 활성도의 중심을 만듭니다.'
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
        ? '구매 활성도는 소수 핵심 제품이 끌고 있고, 그만큼 특정 제품 의존도도 높아요.'
        : componentScores[weakestImpactKey] < 50
            ? brandGetComponentInterpretation('impact', weakestImpactKey, BRAND_COMPONENT_META[strongestImpactKey].label, BRAND_COMPONENT_META[weakestImpactKey].label)
            : '핵심 제품과 제품 연결이 살아 있어 구조가 구매 활성도로 이어지고 있어요.';
    const healthPrimaryReason = componentScores[weakestHealthKey] < 55
        ? brandGetPrimaryReason('health', weakestHealthKey)
        : `주된 원인: ${BRAND_COMPONENT_META[strongestHealthKey].label}. 구조의 약한 고리가 두드러지지 않아요.`;
    const impactPrimaryReason = componentScores.hubConcentration >= 70 && componentScores.coreInfluence >= 60
        ? '주된 원인: 허브 집중도. 구매 활성도가 소수 핵심 제품에 많이 몰려 있어요.'
        : componentScores[weakestImpactKey] < 50
            ? brandGetPrimaryReason('impact', weakestImpactKey)
            : `주된 원인: ${BRAND_COMPONENT_META[strongestImpactKey].label}. 핵심 연결축이 구매 활성도를 받고 있어요.`;
    const healthStrengthPoint = brandGetPointSummary('strength', strongestHealthKey, componentScores[strongestHealthKey]);
    const healthWeakPoint = brandGetPointSummary('weakness', weakestHealthKey, componentScores[weakestHealthKey]);
    const impactStrengthPoint = brandGetPointSummary('strength', strongestImpactKey, componentScores[strongestImpactKey]);
    const impactWeakPoint = brandGetPointSummary('weakness', weakestImpactKey, componentScores[weakestImpactKey]);
    const hero = brandBuildHeroModel(
        driverTimeseries,
        currentImpactRow,
        AppState.data.biiWindow || [],
        timeseries,
        componentScores,
        impactInterpretation,
        selectedWindowDays
    );
    const heroMetrics = brandBuildHeroMetrics(selectedWindowDays, revenueTimeseries);
    const purchaseDrivers = brandBuildDriversModel(driverTimeseries, selectedWindowDays);
    const structureCards = brandBuildStructureCards(
        currentStructureRow,
        productRows,
        edgeRows,
        totalFirstCustomers,
        totalExpectedDemand,
        brandRow,
        currentImpactRow,
        componentScores,
        weightedRepurchase,
        weightedAttach,
        selectedWindowDays
    );
    const structureOverview = brandBuildStructureOverview(
        structureCards,
        Number.isFinite(toNumber(currentStructureRow?.ps_static, NaN))
            ? toNumber(currentStructureRow.ps_static, 0) * 100
            : healthCurrentScore,
        diagnosticHealthDelta,
        healthInterpretation,
        componentScores.roleBalance
    );
    const structureBasisNote = currentStructureRow
        ? ''
        : (structureTimeseries.length
            ? '선택한 기간 구조 스냅샷이 없어 현재 구조 기준으로 보여줘요.'
            : '구조 스냅샷 파일이 없어 현재 구조 기준으로 보여줘요.');
    const diagnosticMatrices = {
        availableWindows,
        selectedWindowDays,
        structure: brandBuildStructureMatrix(healthCurrentScore, currentImpactRow, timeseries, selectedWindowDays),
        revenue: brandBuildRevenueMatrix(revenueTimeseries, timeseries, selectedWindowDays)
    };

    return {
        brandRow,
        availableWindows,
        selectedWindowDays,
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
        impactWindowDays: toNumber(currentImpactRow?.window_days, selectedWindowDays || timeline.selectedWindowDays || defaultImpactRow?.window_days || 30),
        roleCounts,
        productRows,
        edgeRows,
        maps,
        hero,
        heroMetrics,
        purchaseDrivers,
        structureCards,
        structureOverview,
        structureBasisNote,
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

function renderBrandStateCard(title, summary, score, delta, interpretation, primaryReason, strengthPoint, weakPoint, metaLabel, metaValue, tone, radarCanvasId, detailSectionHtml = '', options = {}) {
    const showReason = options.showReason !== false;
    const showPoints = options.showPoints !== false;
    const showRadar = options.showRadar !== false;
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
                    ${showReason ? `<p class="brand-state-reason">${escapeHtml(primaryReason)}</p>` : ''}
                    ${showPoints ? `
                        <div class="brand-state-points">
                            <p class="brand-state-point brand-state-point-strong">${escapeHtml(strengthPoint)}</p>
                            <p class="brand-state-point brand-state-point-weak">${escapeHtml(weakPoint)}</p>
                        </div>
                    ` : ''}
                </div>
                ${showRadar ? `
                    <div class="brand-state-mini-radar">
                        <canvas id="${escapeHtml(radarCanvasId)}"></canvas>
                    </div>
                ` : ''}
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

function renderBrandHeroSection(hero, selectedWindowDays, heroMetrics = []) {
    if (!hero || hero.empty) {
        return `
            <section class="brand-hero-card brand-hero-card-empty">
                <div class="brand-hero-eyebrow">구매 활성도 상태</div>
                <div class="brand-empty-state">
                    <p>${escapeHtml(hero?.note || '구매 활성도 상태를 아직 계산하지 못하고 있어요.')}</p>
                </div>
            </section>
        `;
    }

    const revenueMetric = heroMetrics[0] || null;
    return `
        <section class="brand-hero-card">
            <div class="brand-hero-main">
                <div class="brand-hero-bar">
                    <p class="brand-hero-eyebrow">${escapeHtml(hero.eyebrow)}</p>
                    <div class="brand-hero-bar-main">
                        <div class="brand-hero-state-block">
                            <div class="brand-hero-state-line">
                                <div class="brand-hero-state-group">
                                    <span class="brand-hero-state-basis">최근 흐름 기준</span>
                                    <h2 class="brand-hero-state">${escapeHtml(hero.stateLabel)}</h2>
                                </div>
                                <div class="brand-hero-direction-wrap">
                                    <span class="brand-hero-direction-label">최근 1년 기준</span>
                                    <span class="brand-hero-direction">${escapeHtml(hero.directionLabel)}</span>
                                </div>
                            </div>
                            ${revenueMetric ? `
                                <div class="brand-hero-revenue-inline" title="${escapeHtml(`${revenueMetric.label} ${revenueMetric.value} · ${revenueMetric.meta}`)}">
                                    <span class="brand-hero-revenue-inline-label">${escapeHtml(revenueMetric.label)}</span>
                                    <strong class="brand-hero-revenue-inline-value">${escapeHtml(revenueMetric.value)}</strong>
                                    <span class="brand-hero-revenue-inline-meta">${escapeHtml(revenueMetric.meta)}</span>
                                </div>
                            ` : ''}
                        </div>
                        <div class="brand-hero-bar-copy">
                            <div class="brand-hero-sparkline-block">
                                <div class="brand-hero-sparkline-head">
                                    <span>최근 ${escapeHtml(formatNumber(selectedWindowDays, 0))}일 흐름</span>
                                    <button class="brand-hero-sparkline-cta" type="button" onclick="scrollToBrandTrendSection()">자세히 보기</button>
                                </div>
                                <div class="brand-hero-sparkline-wrap" role="button" tabindex="0" aria-label="하단 추이 섹션으로 이동" onclick="scrollToBrandTrendSection()" onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();scrollToBrandTrendSection();}">
                                    <canvas id="brand-hero-sparkline"></canvas>
                                </div>
                            </div>
                            <p class="brand-hero-direction-copy">${escapeHtml(brandNormalizeUiCopy(hero.directionSentence))}</p>
                            <p class="brand-hero-flow-copy">${escapeHtml(brandNormalizeUiCopy(brandBuildHeroFlowSentence(hero.stateLabel)))}</p>
                            <p class="brand-hero-summary">${escapeHtml(brandNormalizeUiCopy(hero.summary))}</p>
                            ${hero.helper ? `<p class="brand-hero-helper">${escapeHtml(brandNormalizeUiCopy(hero.helper))}</p>` : ''}
                        </div>
                        <div class="brand-hero-bar-side">
                            <div class="brand-hero-bar-cause">
                                <div class="brand-hero-cause-head">
                                    <p class="brand-hero-cause-label">주요 원인</p>
                                </div>
                                <ul class="brand-hero-cause-list">
                                    ${(hero.causes || []).slice(0, 2).map((cause) => `<li>${escapeHtml(brandNormalizeUiCopy(cause))}</li>`).join('') || '<li>원인 요약 데이터가 아직 부족해요.</li>'}
                                </ul>
                            </div>
                        </div>
                    </div>
                    <p class="brand-hero-cause-note">상태는 최근 흐름, 수치는 최근 1년 평균과 비교한 현재 수준이에요.</p>
                </div>
            </div>
        </section>
    `;
}

function renderBrandStructureOverviewCard(overview) {
    return `
        <div class="brand-structure-overview-card">
            <div class="brand-structure-overview-main">
                <p class="brand-structure-overview-label">${overview.label}</p>
                <p class="brand-structure-overview-summary">${escapeHtml(overview.summary)}</p>
            </div>
            <div class="brand-structure-overview-aside">
                <div class="brand-structure-overview-group brand-structure-overview-group-strong">
                    <span class="brand-structure-overview-group-label">강한 구조</span>
                    <strong>${escapeHtml(overview.strongest)}</strong>
                </div>
                <div class="brand-structure-overview-group brand-structure-overview-group-weak">
                    <span class="brand-structure-overview-group-label">약한 구조</span>
                    <div class="brand-structure-overview-weak-list">
                        ${(overview.weakest || []).map((label) => `<strong>${escapeHtml(label)}</strong>`).join('')}
                    </div>
                </div>
            </div>
        </div>
    `;
}

function renderBrandStructureAxisCards(cards) {
    const getCtaHref = (cardKey) => {
        if (cardKey === 'entry') return '../products/?core=entry';
        if (cardKey === 'flow') return '../products/?core=expansion';
        if (cardKey === 'return') return '../products/?core=return';
        if (cardKey === 'basket') return '../products/?chart=demand-graph&demandTab=basket';
        return '../products/';
    };
    return `
        <div class="brand-structure-axis-grid">
            ${(cards || []).map((card) => `
                <article class="brand-structure-axis-card">
                    <div class="brand-structure-axis-head">
                        <div class="brand-structure-axis-title-wrap">
                            <p class="brand-structure-axis-label">${escapeHtml(card.label)}</p>
                            ${card.basisLabel ? `<span class="brand-structure-axis-basis">${escapeHtml(card.basisLabel)}</span>` : ''}
                        </div>
                        <div class="brand-structure-axis-indicator" aria-label="${escapeHtml(`${card.indicatorLevel}/5`)}">
                            ${brandBuildStructureIndicator(card.indicatorLevel)}
                        </div>
                    </div>
                    <div class="brand-structure-axis-metrics">
                        ${(card.metrics || []).map((metric) => `
                            <div class="brand-structure-axis-metric">
                                <span class="brand-structure-axis-metric-label">${escapeHtml(metric.label)}</span>
                                <strong class="brand-structure-axis-metric-value">${escapeHtml(metric.value)}</strong>
                            </div>
                        `).join('')}
                    </div>
                    <p class="brand-structure-axis-copy">${escapeHtml(card.interpretation)}</p>
                    <div class="brand-structure-axis-cta-wrap">
                        <a class="brand-structure-axis-cta" href="${escapeHtml(getCtaHref(card.key))}">제품 관계 분석에서 보기</a>
                    </div>
                </article>
            `).join('')}
        </div>
    `;
}

function renderBrandStructureSection(model) {
    return `
        <section class="card brand-structure-card">
            <div class="brand-section-head">
                <div>
                    <h2>판매 구조</h2>
                    <p>구매 활성도의 바탕이 되는 구조 건강도를 봐요.</p>
                </div>
            </div>
            ${model.structureBasisNote ? `<p class="brand-structure-basis-note">${escapeHtml(model.structureBasisNote)}</p>` : ''}
            ${renderBrandStructureOverviewCard(model.structureOverview)}
            ${renderBrandStructureAxisCards(model.structureCards)}
        </section>
    `;
}

function renderBrandPurchaseEngine(cards) {
    const orderedCards = cards || [];
    const weakestCard = [...orderedCards].sort((a, b) => toNumber(a.contribution, 0) - toNumber(b.contribution, 0))[0] || null;
    const strongestCard = [...orderedCards].sort((a, b) => toNumber(b.contribution, 0) - toNumber(a.contribution, 0))[0] || null;
    const weakestValue = toNumber(weakestCard?.contribution, 0);
    const strongestValue = toNumber(strongestCard?.contribution, 0);

    const getTone = (card) => {
        if (weakestCard && card.key === weakestCard.key && weakestValue < -0.01) return 'is-critical';
        if (strongestCard && card.key === strongestCard.key && strongestValue > 0.01) return 'is-supporting';
        return 'is-neutral';
    };

    const getStatusLabel = (card) => {
        const value = toNumber(card.contribution, 0);
        if (weakestCard && card.key === weakestCard.key && weakestValue < -0.01) return '가장 약함';
        if (strongestCard && card.key === strongestCard.key && strongestValue > 0.01) return '안정';
        if (value < -0.01) return '약함';
        if (value > 0.01) return '보통 이상';
        return '보통';
    };

    const summary = weakestCard && weakestValue < -0.01
        ? `지금은 ${weakestCard.label} 단계가 가장 약합니다.`
        : strongestCard && strongestValue > 0.01
            ? `지금은 ${strongestCard.label} 단계가 전체 흐름을 가장 잘 받치고 있어요.`
            : '지금은 4개 신호가 크게 갈리지 않아요.';

    return `
        <div class="brand-purchase-engine-wrap">
            <div class="brand-purchase-engine-flow" aria-label="구매 활성도 흐름">
            ${orderedCards.map((card, index) => `
                <div class="brand-purchase-engine-step ${getTone(card)}">
                    <span class="brand-purchase-engine-step-label">${escapeHtml(card.label)}</span>
                    <span class="brand-purchase-engine-step-state">${escapeHtml(getStatusLabel(card))}</span>
                </div>
                ${index < orderedCards.length - 1 ? '<span class="brand-purchase-engine-arrow">→</span>' : ''}
            `).join('')}
            </div>
            <p class="brand-purchase-engine-summary">${escapeHtml(summary)}</p>
        </div>
    `;
}

function renderBrandPurchaseDriversSection(drivers) {
    if (!drivers || drivers.empty) {
        return `
            <section class="brand-purchase-drivers-card">
                <div class="brand-section-head">
                    <div>
                        <h2>왜 이런 상태인지</h2>
                        <p>구매 활성도가 왜 좋아지거나 약해졌는지, 아래 4개 신호에서 먼저 확인하세요.</p>
                    </div>
                </div>
                <p class="brand-purchase-engine">어디서 약해지는지 먼저 보세요.</p>
                <div class="brand-empty-state">
                    <p>${escapeHtml(drivers.note)}</p>
                    <p class="brand-empty-state-sub">${escapeHtml(drivers.helper)}</p>
                </div>
            </section>
        `;
    }

    return `
        <section class="brand-purchase-drivers-card">
            <div class="brand-section-head">
                <div>
                    <h2>왜 이런 상태인지</h2>
                    <p>구매 활성도가 왜 좋아지거나 약해졌는지, 아래 4개 신호에서 먼저 확인하세요.</p>
                </div>
            </div>
            ${renderBrandPurchaseEngine(drivers.cards)}
            <div class="brand-purchase-drivers-grid">
                ${drivers.cards.map((card) => {
                    return `
                        <article class="brand-purchase-driver-item">
                            <div class="brand-purchase-driver-top">
                                <div>
                                    <p class="brand-purchase-driver-label">${escapeHtml(card.label)}</p>
                                    <div class="brand-purchase-driver-value">${escapeHtml(card.displayValue)}</div>
                                </div>
                                <span class="brand-purchase-driver-delta">${escapeHtml(card.deltaLabel)}</span>
                            </div>
                            <p class="brand-purchase-driver-copy">${escapeHtml(card.interpretation)}</p>
                        </article>
                    `;
                }).join('')}
            </div>
        </section>
    `;
}

function renderBrandPurchaseOverviewSection(model) {
    return `
        <section class="card brand-purchase-overview-card">
            <div class="brand-section-head brand-section-head-overview">
                <div>
                    <h2>지금 구매 활성도는 어떤 상태인가?</h2>
                    <p>판매 구조가 실제 구매까지 얼마나 이어지고 있는지 보여줘요.</p>
                </div>
            </div>
            <div class="brand-purchase-overview-stack">
                ${renderBrandHeroSection(model.hero, model.selectedWindowDays, model.heroMetrics)}
                ${renderBrandPurchaseDriversSection(model.purchaseDrivers)}
            </div>
        </section>
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
    const stageToneClass = `brand-matrix-stage-${escapeHtml(matrix.stage.tone)}`;

    return `
        <article class="brand-matrix-card">
            <div class="brand-matrix-head">
                <div>
                    <h3>${escapeHtml(matrix.title)}</h3>
                </div>
                <span class="brand-matrix-stage ${stageToneClass}">${escapeHtml(matrix.stage.label)}</span>
            </div>
            <p class="brand-matrix-summary">${escapeHtml(matrix.stage.summary)}</p>
            <p class="brand-matrix-note">${escapeHtml(matrix.note)}</p>
            <div class="brand-matrix-visual">
                <div class="brand-matrix-axis-label brand-matrix-axis-label-x">${escapeHtml(matrix.xAxisLabel)}</div>
                <div class="brand-matrix-grid brand-matrix-grid-${escapeHtml(matrix.kind)}" style="--matrix-threshold-x:${brandScaleMatrixPoint(matrix.thresholdX, matrix.xRange)}%; --matrix-threshold-y:${brandScaleMatrixPoint(matrix.thresholdY, matrix.yRange)}%;">
                    <div class="brand-matrix-cell brand-matrix-cell-q1"><strong>${escapeHtml(matrix.kind === 'structure' ? '단기 펌핑' : '장기 개선 신호')}</strong></div>
                    <div class="brand-matrix-cell brand-matrix-cell-q2"><strong>${escapeHtml(matrix.kind === 'structure' ? '최우수' : '건강한 성장')}</strong></div>
                    <div class="brand-matrix-cell brand-matrix-cell-q3"><strong>${escapeHtml(matrix.kind === 'structure' ? '전면 개선 필요' : '동반 침체')}</strong></div>
                    <div class="brand-matrix-cell brand-matrix-cell-q4"><strong>${escapeHtml(matrix.kind === 'structure' ? '실행력 저하' : '구조 리스크')}</strong></div>
                    <div
                        class="brand-matrix-overlay"
                        data-current-left="${currentLeft}"
                        data-current-bottom="${currentBottom}"
                        ${matrix.previousPoint ? `data-previous-left="${previousLeft}" data-previous-bottom="${previousBottom}"` : ''}
                    >
                        ${matrix.previousPoint ? `
                            <div class="brand-matrix-arrow brand-matrix-arrow-animated"></div>
                            <span class="brand-matrix-point brand-matrix-point-prev" title="4주 전"></span>
                            <span class="brand-matrix-point-label brand-matrix-point-label-prev">4주 전</span>
                        ` : ''}
                        <span class="brand-matrix-point brand-matrix-point-current" title="현재"></span>
                        <span class="brand-matrix-point-label brand-matrix-point-label-current">현재</span>
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
                    <p>판매 구조, 구매 활성도, 매출을 함께 보면 지금 문제의 종류를 더 빨리 읽을 수 있어요.</p>
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
                    <h3>일별 구매 활성도 흐름</h3>
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
                <h3>일별 구매 활성도 흐름</h3>
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
    const selectedWindowGuide = BRAND_TIMELINE_WINDOW_GUIDE[timeline.selectedWindowDays] || '선택한 기간 기준으로 구매 활성도를 해석해요.';
    if (!timeline.availableWindows.length) {
        return `
            <article class="brand-trend-panel brand-trend-panel-secondary">
                <div class="brand-mini-chart-head brand-mini-chart-head-stack">
                    <div>
                        <h3>최근 기간 기준으로 보면 구매 활성도는 어떤가?</h3>
                        <p>최근 7일, 30일처럼 기간을 묶어 최근 흐름이 얼마나 이어지는지 봐요.</p>
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
                    <h3>최근 기간 기준으로 보면 구매 활성도는 어떤가?</h3>
                    <p>최근 ${escapeHtml(selectedWindowLabel)}을 묶어, 지금 구매 활성도가 얼마나 살아 있는지 봐요.</p>
                </div>
            </div>
            <p class="brand-panel-compare"><strong>이 차트는 이런 때 봐요.</strong> 하루 흐름보다 조금 더 길게, 최근 운영이 실제 구매로 이어지고 있는지 볼 때 좋아요.</p>
            <div class="brand-explainer-card">
                <p><strong>구매 활성도</strong>는 최근 고객 반응이 실제 구매로 얼마나 이어지고 있는지 보여줘요.</p>
                <p>위 차트가 하루 흐름이라면, 이 차트는 최근 ${escapeHtml(selectedWindowLabel)}을 한 번에 묶어 읽는 그림이에요. 각 점은 하루 값이 아니라 그날까지의 최근 ${escapeHtml(selectedWindowLabel)}을 묶어 계산한 값이에요.</p>
            </div>
            <p class="brand-timeline-context"><strong>${escapeHtml(selectedWindowLabel)} 기준은 이렇게 보면 돼요.</strong> ${escapeHtml(selectedWindowGuide)}</p>
            <div class="brand-mini-chart-head">
                <h3>최근 ${escapeHtml(selectedWindowLabel)} 기준 구매 활성도</h3>
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
                    <h2>요즘 구매 활성도 흐름은 어떤가?</h2>
                    <p>하루 흐름은 위에서 보고, 조금 더 길게 본 흐름은 아래에서 같이 확인해요.</p>
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

    if (topBarActions) {
        topBarActions.innerHTML = `
            <div class="brand-topbar-window">
                <div class="brand-topbar-window-copy">
                    <div class="brand-topbar-window-head">
                        <span class="brand-topbar-window-label">구매 활성도 묶음 기준</span>
                        <span class="brand-topbar-window-helper">조회 90일 고정</span>
                    </div>
                </div>
                <div class="brand-timeline-toggle brand-window-toggle">
                    ${BRAND_DIAGNOSTIC_WINDOW_ORDER.map((windowDays) => `
                        <button
                            class="btn-primary ${model.selectedWindowDays === windowDays ? 'is-active' : ''}"
                            type="button"
                            onclick="setBrandWindow(${windowDays})"
                            ${model.availableWindows?.includes(windowDays) ? '' : 'disabled'}
                        >${formatNumber(windowDays, 0)}일 묶음</button>
                    `).join('')}
                </div>
            </div>
        `;
    }

    container.innerHTML = `
        <div class="brand-dashboard animate-fade-in">
            ${renderBrandPurchaseOverviewSection(model)}

            ${renderBrandStructureSection(model)}

            ${renderBrandDiagnosticSection(model)}

            ${renderBrandTimelineSection(model)}

            <section class="card brand-next-cta-card">
                <div class="brand-section-head">
                    <div>
                        <h2>제품별 기여를 더 자세히 보려면</h2>
                        <p>제품 단위 구조 기여, 구매 활성도 기여, 수요 흐름과 역할 맵은 제품 관계 분석 화면에서 이어서 볼 수 있어요.</p>
                    </div>
                    <a class="btn-primary brand-next-cta-link" href="../products/">제품 관계 분석으로 이동</a>
                </div>
                <p class="brand-next-cta-note">브랜드 페이지에서는 현재 브랜드 상태와 진단에 집중하고, 제품별 drill-down과 구조 맵은 제품 관계 분석 화면으로 분리했어요.</p>
            </section>
        </div>
    `;

    renderBrandRoleMapChart(model);
    renderBrandTimelineCharts(model);
    syncBrandMatrixOverlays();
    ensureBrandMatrixOverlayResizeBinding();
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
    const impactCanvas = document.getElementById('brand-impact-radar-inline');

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

function syncBrandMatrixOverlays() {
    document.querySelectorAll('.brand-matrix-grid').forEach((grid) => {
        const overlay = grid.querySelector('.brand-matrix-overlay');
        if (!overlay) return;

        const width = grid.clientWidth;
        const height = grid.clientHeight;
        if (!width || !height) return;

        const currentLeftPct = toNumber(overlay.dataset.currentLeft, NaN);
        const currentBottomPct = toNumber(overlay.dataset.currentBottom, NaN);
        if (!Number.isFinite(currentLeftPct) || !Number.isFinite(currentBottomPct)) return;

        const currentX = (currentLeftPct / 100) * width;
        const currentY = (currentBottomPct / 100) * height;
        const previousLeftPct = toNumber(overlay.dataset.previousLeft, NaN);
        const previousBottomPct = toNumber(overlay.dataset.previousBottom, NaN);
        const hasPrevious = Number.isFinite(previousLeftPct) && Number.isFinite(previousBottomPct);
        const previousX = hasPrevious ? (previousLeftPct / 100) * width : currentX;
        const previousY = hasPrevious ? (previousBottomPct / 100) * height : currentY;

        const currentPoint = overlay.querySelector('.brand-matrix-point-current');
        const currentLabel = overlay.querySelector('.brand-matrix-point-label-current');
        const previousPoint = overlay.querySelector('.brand-matrix-point-prev');
        const previousLabel = overlay.querySelector('.brand-matrix-point-label-prev');
        const arrow = overlay.querySelector('.brand-matrix-arrow');

        if (currentPoint) {
            currentPoint.style.left = `${currentX}px`;
            currentPoint.style.bottom = `${currentY}px`;
        }

        if (hasPrevious && previousPoint) {
            previousPoint.style.left = `${previousX}px`;
            previousPoint.style.bottom = `${previousY}px`;
        }

        const deltaXPx = currentX - previousX;
        const deltaYPx = currentY - previousY;
        const vectorLength = Math.max(Math.sqrt((deltaXPx ** 2) + (deltaYPx ** 2)), 0.0001);
        const unitX = deltaXPx / vectorLength;
        const unitY = deltaYPx / vectorLength;
        const previousLabelOffsetX = brandClamp(-unitX * 18, -24, 24);
        const previousLabelOffsetY = brandClamp(unitY * 18 + 18, 12, 28);
        const currentLabelOffsetX = brandClamp(unitX * 18, -24, 24);
        const currentLabelOffsetY = brandClamp(-unitY * 18 - 22, -30, -14);

        if (currentLabel) {
            currentLabel.style.left = `${currentX}px`;
            currentLabel.style.bottom = `${currentY}px`;
            currentLabel.style.setProperty('--brand-point-label-x', `${currentLabelOffsetX}px`);
            currentLabel.style.setProperty('--brand-point-label-y', `${currentLabelOffsetY}px`);
        }

        if (hasPrevious && previousLabel) {
            previousLabel.style.left = `${previousX}px`;
            previousLabel.style.bottom = `${previousY}px`;
            previousLabel.style.setProperty('--brand-point-label-x', `${previousLabelOffsetX}px`);
            previousLabel.style.setProperty('--brand-point-label-y', `${previousLabelOffsetY}px`);
        }

        if (hasPrevious && arrow) {
            const arrowAngle = Math.atan2(-deltaYPx, deltaXPx) * (180 / Math.PI);
            const arrowLength = Math.sqrt((deltaXPx ** 2) + (deltaYPx ** 2));
            arrow.style.left = `${previousX}px`;
            arrow.style.bottom = `${previousY}px`;
            arrow.style.width = `${arrowLength}px`;
            arrow.style.setProperty('--brand-matrix-angle', `${arrowAngle}deg`);
        }
    });
}

function ensureBrandMatrixOverlayResizeBinding() {
    if (AppState.helpers.brandMatrixResizeBound) return;
    const sync = () => window.requestAnimationFrame(syncBrandMatrixOverlays);
    AppState.helpers.brandMatrixResizeBound = sync;
    window.addEventListener('resize', sync);
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
    const heroSparkCanvas = document.getElementById('brand-hero-sparkline');
    if (heroSparkCanvas && model.timeline?.impactRows?.length) {
        const heroSparkRows = model.timeline.impactRows.slice(-24);
        const heroSparkValues = heroSparkRows.map((row) => toNumber(row.bii, 0) * 100);
        const lastHighlightCount = Math.min(6, heroSparkValues.length);
        const lastHighlightStart = Math.max(0, heroSparkValues.length - lastHighlightCount);
        const lowIndex = heroSparkValues.reduce((bestIndex, value, index, arr) => (
            bestIndex === -1 || value < arr[bestIndex] ? index : bestIndex
        ), -1);
        const heroSparkMin = Math.min(...heroSparkValues);
        const heroSparkMax = Math.max(...heroSparkValues);
        const heroSparkSpan = Math.max(heroSparkMax - heroSparkMin, 1);
        const heroSparkPad = Math.max(heroSparkSpan * 0.22, 0.8);
        AppState.charts.brandHeroSparkline = new Chart(heroSparkCanvas, {
            type: 'line',
            data: {
                labels: heroSparkRows.map((row) => String(row.as_of_date || '').slice(5)),
                datasets: [{
                    label: '최근 흐름',
                    data: heroSparkValues,
                    borderColor: 'rgba(99, 102, 241, 0.38)',
                    backgroundColor: 'rgba(99, 102, 241, 0.07)',
                    borderWidth: 1.8,
                    tension: 0.32,
                    pointRadius: 0,
                    pointHoverRadius: 0,
                    fill: true
                }, {
                    label: '최근 구간',
                    data: heroSparkValues.map((value, index) => (index >= lastHighlightStart ? value : null)),
                    borderColor: '#4f46e5',
                    backgroundColor: 'rgba(0, 0, 0, 0)',
                    borderWidth: 2.7,
                    tension: 0.32,
                    pointRadius: 0,
                    pointHoverRadius: 0,
                    fill: false
                }, {
                    label: '저점',
                    data: heroSparkValues.map((value, index) => (index === lowIndex ? value : null)),
                    borderColor: 'rgba(0,0,0,0)',
                    backgroundColor: 'rgba(0,0,0,0)',
                    pointRadius: 3.4,
                    pointHoverRadius: 0,
                    pointBackgroundColor: '#ffffff',
                    pointBorderColor: '#818cf8',
                    pointBorderWidth: 2,
                    showLine: false
                }, {
                    label: '현재',
                    data: heroSparkValues.map((value, index) => (index === heroSparkValues.length - 1 ? value : null)),
                    borderColor: 'rgba(0,0,0,0)',
                    backgroundColor: 'rgba(0,0,0,0)',
                    pointRadius: 3.6,
                    pointHoverRadius: 0,
                    pointBackgroundColor: '#4338ca',
                    pointBorderColor: '#c7d2fe',
                    pointBorderWidth: 2,
                    showLine: false
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: false,
                plugins: {
                    legend: { display: false },
                    tooltip: { enabled: false }
                },
                scales: {
                    x: {
                        display: false,
                        grid: { display: false },
                        border: { display: false }
                    },
                    y: {
                        display: false,
                        min: heroSparkMin - heroSparkPad,
                        max: heroSparkMax + heroSparkPad,
                        grid: { display: false },
                        border: { display: false }
                    }
                },
                elements: {
                    line: {
                        capBezierPoints: true
                    }
                }
            }
        });
    }

    const pulseCanvas = document.getElementById('brand-daily-pulse-timeline');
    if (pulseCanvas && model.dailyPulse.rows.length) {
        const pulseValues = model.dailyPulse.rows.map((row) => toNumber(row.daily_bii_pulse, 0) * 100);
        AppState.charts.brandDailyPulseTimeline = new Chart(pulseCanvas, {
            type: 'line',
            data: {
                labels: model.dailyPulse.rows.map((row) => String(row.as_of_date || '').slice(5)),
                datasets: [{
                    label: '일별 구매 활성도 흐름',
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
                label: '최근 기간 기준 구매 활성도',
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

window.scrollToBrandTrendSection = () => {
    const section = document.querySelector('.brand-timeline-card');
    if (!section) return;
    section.scrollIntoView({ behavior: 'smooth', block: 'start' });
    section.classList.add('is-highlighted');
    window.setTimeout(() => section.classList.remove('is-highlighted'), 1400);
};

window.renderBrandDashboard = renderBrandDashboard;
