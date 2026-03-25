// Products page logic

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

function withAlpha(color, alpha) {
    const normalizedAlpha = Math.max(0, Math.min(1, toNumber(alpha, 1)));
    const normalizedColor = String(color || '').trim();
    if (!normalizedColor) return `rgba(148, 163, 184, ${normalizedAlpha})`;

    const hexMatch = normalizedColor.match(/^#([0-9a-f]{3}|[0-9a-f]{6})$/i);
    if (hexMatch) {
        const hex = hexMatch[1];
        const expanded = hex.length === 3
            ? hex.split('').map((char) => char + char).join('')
            : hex;
        const r = parseInt(expanded.slice(0, 2), 16);
        const g = parseInt(expanded.slice(2, 4), 16);
        const b = parseInt(expanded.slice(4, 6), 16);
        return `rgba(${r}, ${g}, ${b}, ${normalizedAlpha})`;
    }

    const rgbMatch = normalizedColor.match(/^rgba?\(\s*([^\)]+)\s*\)$/i);
    if (rgbMatch) {
        const channels = rgbMatch[1].split(',').map((part) => part.trim()).slice(0, 3);
        if (channels.length === 3) {
            return `rgba(${channels[0]}, ${channels[1]}, ${channels[2]}, ${normalizedAlpha})`;
        }
    }

    return normalizedColor;
}

const QUADRANT_CONVERGENCE_EDGE_TOP_N = 8;
const QUADRANT_RETURN_LOOP_TOP_N = 4;
const QUADRANT_THUMBNAIL_SRC = '../../assets/thumbnail.png';

const QUADRANT_EDGE_MODE_META = {
    representative: {
        label: '전이 흐름',
        guide: '이 제품에서 이어지거나 이 제품으로 넘어오는 전이 흐름을 보여줘요.',
        emptyGuide: '이 제품과 이어지는 전이 흐름이 아직 많지 않아요.',
        unavailableGuide: '전이 흐름을 보여줄 데이터가 아직 준비되지 않았어요.'
    },
    convergence: {
        label: '도착 흐름',
        guide: '이 제품이 다른 제품 다음에 자주 선택되는지 보여줘요.',
        emptyGuide: '이 제품이 다음 구매로 자주 이어지는 흐름은 아직 많지 않아요.',
        unavailableGuide: '도착 흐름을 보여줄 상세 전이 데이터가 아직 준비되지 않았어요.'
    }
};

const DEMAND_GRAPH_TAB_META = {
    transition: {
        label: '구매 전이',
        guide: '이 제품 전후로 자주 이어지는 구매 관계를 보여줘요.',
        emptyGuide: '이 제품과 자주 이어지는 연결은 아직 많지 않아요.',
        unavailableGuide: '주변 연결 흐름을 보여줄 상세 패턴 데이터가 아직 준비되지 않았어요.',
        summaryTitle: '자주 이어지는 제품'
    },
    basket: {
        label: '장바구니 조합',
        guide: '이 제품과 함께 담기는 관계를 보여줘요.',
        emptyGuide: '이 제품과 자주 함께 담기는 조합은 아직 많지 않아요.',
        unavailableGuide: '장바구니 조합을 보여줄 패턴 데이터가 아직 준비되지 않았어요.',
        summaryTitle: '자주 함께 담기는 제품'
    }
};

const DEMAND_GRAPH_TRANSITION_LIMIT = 5;
const DEMAND_GRAPH_BASKET_LIMIT = 6;
const DEMAND_GRAPH_SUMMARY_LIMIT = 5;
const QUADRANT_SVG_FRAME = Object.freeze({ width: 708, height: 585 });
const QUADRANT_MIN_RENDER_WIDTH = 360;

function normalizeDemandGraphTab(tab) {
    return String(tab || '').trim().toLowerCase() === 'basket' ? 'basket' : 'transition';
}

function normalizeQuadrantEdgeMode(mode) {
    const normalized = String(mode || '').trim().toLowerCase();
    if (normalized === 'convergence') return 'convergence';
    return 'representative';
}

function getQuadrantEdgeModeAvailability() {
    const hasTransitionEdge = Array.isArray(AppState.data.productTransitionEdge) && AppState.data.productTransitionEdge.length > 0;
    const hasAnchorTransition = Array.isArray(AppState.data.anchorTransition) && AppState.data.anchorTransition.length > 0;
    return {
        representative: hasTransitionEdge || hasAnchorTransition,
        convergence: hasTransitionEdge || hasAnchorTransition
    };
}

function getQuadrantStatus(entry, expansion, centerEntry, centerExpansion) {
    const highEntry = toNumber(entry, 0) >= toNumber(centerEntry, 0);
    const highExpansion = toNumber(expansion, 0) >= toNumber(centerExpansion, 0);
    if (highEntry && highExpansion) {
        return {
            key: 'hero',
            label: '우선 확대 대상',
            color: '#1e85fb',
            summary: '현재 화면에서 신규 유입 강점과 재구매 강점이 모두 상대적으로 높아 우선 확대를 검토할 수 있는 제품이에요.',
            guide: '현재 화면에서 신규 유입 강점과 재구매 강점이 모두 상대적으로 높은 구간이에요. 우선적으로 노출·예산·재고 확대를 검토할 수 있어요.',
            actions: [
                '핵심 지면과 캠페인에서 상시 노출해 성장 모멘텀을 키워요.',
                '재고와 배송 가용성을 우선 보호해 품절 손실을 줄여요.'
            ]
        };
    }
    if (!highEntry && !highExpansion) {
        return {
            key: 'phaseout',
            label: '개선 필요',
            color: '#f45151',
            summary: '현재 화면에서 신규 유입 강점과 재구매 강점이 모두 상대적으로 낮아 개선 여부를 점검할 필요가 있는 상태예요.',
            guide: '현재 화면에서 신규 유입 강점과 재구매 강점이 모두 상대적으로 낮은 구간이에요. 개선 실험 후 유지 여부를 판단할 수 있어요.',
            actions: [
                '가격·구성·메시지 개선 실험으로 반응 회복 가능성을 먼저 확인해요.',
                '개선 반응이 낮으면 축소 또는 대체 제품으로 전환해요.'
            ]
        };
    }
    if (highEntry && !highExpansion) {
        return {
            key: 'entry-only',
            label: '첫구매 강점 제품',
            color: '#6bba25',
            summary: '현재 화면에서 첫구매 강점은 상대적으로 높지만 재구매 강점은 상대적으로 낮아 후속 전환 보강이 필요해요.',
            guide: '현재 화면에서 신규 유입 강점은 상대적으로 높지만, 재구매 강점은 상대적으로 낮은 구간이에요. 재구매 전환 장치 보강이 필요해요.',
            actions: [
                '첫구매 직후 재구매 유도 번들/세트를 전면 배치해 연결을 강화해요.',
                '첫 구매 후 3~7일 CRM 리마인드로 다음 구매 전환을 높여요.'
            ]
        };
    }
    return {
        key: 'expansion-only',
        label: '재구매 강점 제품',
        color: '#a765fb',
        summary: '현재 화면에서 재구매 강점은 상대적으로 높지만 신규 유입 강점은 상대적으로 낮아 유입 확대가 필요해요.',
        guide: '현재 화면에서 재구매 강점은 상대적으로 높지만, 신규 유입 강점은 상대적으로 낮은 구간이에요. 신규 유입 채널 보강이 필요해요.',
        actions: [
            '신규 유입 채널과 크리에이티브를 확장해 첫구매 모수를 늘려요.',
            '첫구매 강점 제품과의 동시 노출로 유입 구간을 보강해요.'
        ]
    };
}

function getQuadrantStatusCompactCopy(status) {
    switch (String(status?.key || '').trim()) {
    case 'hero':
        return '신규 유입과 재구매 강점이 모두 높아 우선 확대를 검토할 수 있어요.';
    case 'entry-only':
        return '신규 유입 강점은 높지만 재구매 강점은 낮아 전환 보강이 필요해요.';
    case 'phaseout':
        return '신규 유입과 재구매 강점이 모두 낮아 개선 여부 점검이 필요해요.';
    case 'expansion-only':
    default:
        return '재구매 강점은 높지만 신규 유입 강점은 낮아 유입 확대가 필요해요.';
    }
}

function getPointByIdMap(points) {
    return new Map((points || []).map((point) => [String(point.id || '').trim(), point]));
}

function expandRangeToInclude(range, point, xPad, yPad) {
    if (!point) return range;
    return {
        xMin: Math.min(range.xMin, point.entry - xPad),
        xMax: Math.max(range.xMax, point.entry + xPad),
        yMin: Math.min(range.yMin, point.expansion - yPad),
        yMax: Math.max(range.yMax, point.expansion + yPad)
    };
}

function compactFocusRange(range, points, selected, xPad, yPad) {
    const sourcePoints = (points || []).filter(Boolean);
    if (!sourcePoints.length) return range;

    const entries = sourcePoints.map((point) => point.entry).filter((value) => Number.isFinite(value));
    const expansions = sourcePoints.map((point) => point.expansion).filter((value) => Number.isFinite(value));
    if (!entries.length || !expansions.length) return range;

    const dataMinX = Math.min(...entries);
    const dataMaxX = Math.max(...entries);
    const dataMinY = Math.min(...expansions);
    const dataMaxY = Math.max(...expansions);
    const spanX = Math.max(range.xMax - range.xMin, 0.02);
    const spanY = Math.max(range.yMax - range.yMin, 0.02);
    const leftGapRatio = (dataMinX - range.xMin) / spanX;
    const rightGapRatio = (range.xMax - dataMaxX) / spanX;
    const bottomGapRatio = (dataMinY - range.yMin) / spanY;
    const topGapRatio = (range.yMax - dataMaxY) / spanY;
    const xCompactPad = Math.max(xPad * 1.15, 0.02);
    const yCompactPad = Math.max(yPad * 1.15, 0.02);

    if (leftGapRatio > 0.18) {
        range.xMin = Math.max(range.xMin, dataMinX - xCompactPad);
    }
    if (rightGapRatio > 0.18) {
        range.xMax = Math.min(range.xMax, dataMaxX + xCompactPad);
    }
    if (bottomGapRatio > 0.18) {
        range.yMin = Math.max(range.yMin, dataMinY - yCompactPad);
    }
    if (topGapRatio > 0.18) {
        range.yMax = Math.min(range.yMax, dataMaxY + yCompactPad);
    }

    if (selected) {
        range = expandRangeToInclude(range, selected, xCompactPad * 0.8, yCompactPad * 0.8);
    }
    if (range.xMin === range.xMax) range.xMax = range.xMin + 0.02;
    if (range.yMin === range.yMax) range.yMax = range.yMin + 0.02;
    return range;
}

function getFocusPrimaryPoints(relatedPoints, selected) {
    if (!selected || !Array.isArray(relatedPoints) || !relatedPoints.length) return [];
    const others = relatedPoints
        .filter((point) => String(point.id || '').trim() !== String(selected.id || '').trim())
        .map((point) => ({
            point,
            distance: Math.hypot(point.entry - selected.entry, point.expansion - selected.expansion)
        }))
        .sort((a, b) => a.distance - b.distance);

    if (!others.length) return [selected];
    const distances = others.map((item) => item.distance);
    const threshold = percentile(distances, 0.7);
    const capped = others
        .filter((item, index) => index < 5 || item.distance <= threshold)
        .map((item) => item.point);
    const primary = [selected, ...capped];
    const unique = new Map(primary.map((point) => [String(point.id || '').trim(), point]));
    return Array.from(unique.values());
}

function buildConnectedFocusRange(points, selected, visibleEdges) {
    if (!selected) return null;
    const pointMap = getPointByIdMap(points);
    const relatedIds = new Set([String(selected.id || '').trim()]);
    (visibleEdges || []).forEach((edge) => {
        if (edge.from) relatedIds.add(String(edge.from).trim());
        if (edge.to) relatedIds.add(String(edge.to).trim());
    });
    const relatedPoints = Array.from(relatedIds)
        .map((id) => pointMap.get(id))
        .filter(Boolean);
    if (!relatedPoints.length) return null;

    const primaryPoints = getFocusPrimaryPoints(relatedPoints, selected);
    const entries = primaryPoints.map((point) => point.entry);
    const expansions = primaryPoints.map((point) => point.expansion);
    const meanEntry = entries.reduce((sum, value) => sum + value, 0) / entries.length;
    const meanExpansion = expansions.reduce((sum, value) => sum + value, 0) / expansions.length;
    const rawWidth = Math.max(...entries) - Math.min(...entries);
    const rawHeight = Math.max(...expansions) - Math.min(...expansions);
    const zoomMultiplier = primaryPoints.length <= 2 ? 1.1 : primaryPoints.length <= 4 ? 1.22 : primaryPoints.length <= 7 ? 1.35 : 1.45;
    const width = Math.max(rawWidth * zoomMultiplier, primaryPoints.length <= 2 ? 0.08 : 0.12);
    const height = Math.max(rawHeight * zoomMultiplier, primaryPoints.length <= 2 ? 0.08 : 0.12);
    const xPad = Math.max(width * 0.08, 0.02);
    const yPad = Math.max(height * 0.08, 0.02);
    const xThreshold = Math.max(width * 0.08, 0.015);
    const yThreshold = Math.max(height * 0.08, 0.015);
    const leftRatio = meanEntry > selected.entry + xThreshold
        ? 0.38
        : meanEntry < selected.entry - xThreshold
            ? 0.62
            : 0.5;
    const bottomRatio = meanExpansion > selected.expansion + yThreshold
        ? 0.38
        : meanExpansion < selected.expansion - yThreshold
            ? 0.62
            : 0.5;
    let range = {
        xMin: selected.entry - (width * leftRatio),
        xMax: selected.entry + (width * (1 - leftRatio)),
        yMin: selected.expansion - (height * bottomRatio),
        yMax: selected.expansion + (height * (1 - bottomRatio))
    };

    primaryPoints.forEach((point) => {
        range = expandRangeToInclude(range, point, xPad, yPad);
    });
    range = expandRangeToInclude(range, selected, xPad, yPad);
    range = compactFocusRange(range, primaryPoints, selected, xPad, yPad);

    return {
        ...range,
        xPad,
        yPad,
        relatedCount: relatedPoints.length,
        primaryCount: primaryPoints.length
    };
}

function getFocusRange(points, selected, visibleEdges = []) {
    const connectedRange = buildConnectedFocusRange(points, selected, visibleEdges);
    if (connectedRange) {
        if (connectedRange.xMin === connectedRange.xMax) connectedRange.xMax = connectedRange.xMin + 0.02;
        if (connectedRange.yMin === connectedRange.yMax) connectedRange.yMax = connectedRange.yMin + 0.02;
        return connectedRange;
    }

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
    let range = compactFocusRange({ xMin, xMax, yMin, yMax }, points, selected, xPad, yPad);
    if (range.xMin === range.xMax) range.xMax = range.xMin + 0.02;
    if (range.yMin === range.yMax) range.yMax = range.yMin + 0.02;
    return { ...range, xPad, yPad, relatedCount: selected ? 1 : 0 };
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

function buildQuadrantScaleModel(points, selected, scaleMode, visibleEdges = []) {
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
    const focusRange = getFocusRange(points, selected, visibleEdges);
    const activeRange = scaleMode === 'raw' ? rawRange : focusRange;
    return {
        rawRange,
        focusRange,
        activeRange,
        mode: scaleMode
    };
}

function getQuadrantRenderRange(range) {
    const baseRange = range || { xMin: 0, xMax: 1, yMin: 0, yMax: 1 };
    return {
        xMin: toNumber(baseRange.xMin, 0),
        xMax: toNumber(baseRange.xMax, 1),
        yMin: toNumber(baseRange.yMin, 0),
        yMax: toNumber(baseRange.yMax, 1)
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

function buildRepresentativeQuadrantEdges(pointIdSet, selectedId) {
    const edgeMap = new Map();
    const sourceRows = Array.isArray(AppState.data.productTransitionEdge) && AppState.data.productTransitionEdge.length
        ? AppState.data.productTransitionEdge
        : (AppState.data.anchorTransition || []);

    const upsertEdge = (from, to, direction, row) => {
        if (!from || !to || from === to) return;
        if (!pointIdSet.has(from) || !pointIdSet.has(to)) return;
        const customers = Math.max(0, toNumber(firstDefinedValue(row.transition_customer_cnt, row.transition_customers, 0), 0));
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
        const avgDays = toNumber(firstDefinedValue(
            row.avg_days_to_target,
            row.avg_days_to_pca,
            row.avg_days_to_expansion,
            NaN
        ), NaN);
        if (Number.isFinite(avgDays)) {
            acc.avgDaysNum += avgDays * customers;
            acc.avgDaysDen += customers;
        }
        acc.peakRate = Math.max(acc.peakRate, toNumber(firstDefinedValue(row.transition_rate, row.edge_rate, 0), 0));
    };

    sourceRows.forEach((row) => {
        const from = String(firstDefinedValue(row.source_product_id, row.aa_product_id, row.entry_product_id, '')).trim();
        const to = String(firstDefinedValue(row.target_product_id, row.pca_product_id, row.expansion_product_id, '')).trim();
        if (!from || !to || from === to) return;
        if (from === selectedId) upsertEdge(from, to, 'outbound', row);
        if (to === selectedId) upsertEdge(from, to, 'inbound', row);
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

function buildConvergenceQuadrantEdges(pointIdSet, selectedId) {
    const sourceRows = Array.isArray(AppState.data.productTransitionEdge) && AppState.data.productTransitionEdge.length
        ? AppState.data.productTransitionEdge
        : (AppState.data.anchorTransition || []);

    return sourceRows
        .map((row) => ({
            from: String(firstDefinedValue(row.source_product_id, row.aa_product_id, row.entry_product_id, '')).trim(),
            to: String(firstDefinedValue(row.target_product_id, row.pca_product_id, row.expansion_product_id, '')).trim(),
            direction: 'convergence',
            transitionCustomers: Math.max(0, toNumber(firstDefinedValue(row.transition_customer_cnt, row.transition_customers, 0), 0)),
            peakRate: Math.max(0, toNumber(firstDefinedValue(row.transition_rate, row.edge_rate, 0), 0))
        }))
        .filter((edge) => edge.from && edge.to && edge.from !== edge.to)
        .filter((edge) => edge.to === selectedId && edge.from !== selectedId)
        .filter((edge) => pointIdSet.has(edge.from) && pointIdSet.has(edge.to))
        .sort((a, b) => {
            const byCustomers = toNumber(b.transitionCustomers, 0) - toNumber(a.transitionCustomers, 0);
            if (byCustomers !== 0) return byCustomers;
            return toNumber(b.peakRate, 0) - toNumber(a.peakRate, 0);
        })
        .slice(0, QUADRANT_CONVERGENCE_EDGE_TOP_N);
}

function buildReturnQuadrantEdges(pointIdSet, selectedId) {
    return (AppState.data.returnGravityLoopDetail || [])
        .filter((row) => String(firstDefinedValue(row.product_id, row.anchor_product_id, '')).trim() === selectedId)
        .filter((row) => {
            const lastVia = String(firstDefinedValue(row.last_via_product_id, row.via_product_id, row.intermediate_product_id, '')).trim();
            const usableLast = lastVia && lastVia !== selectedId && pointIdSet.has(lastVia);
            return usableLast;
        })
        .sort((a, b) => toNumber(b.return_loop_customer_cnt, 0) - toNumber(a.return_loop_customer_cnt, 0))
        .slice(0, QUADRANT_RETURN_LOOP_TOP_N)
        .map((row) => {
            const lastVia = String(firstDefinedValue(row.last_via_product_id, row.via_product_id, row.intermediate_product_id, '')).trim();
            const customers = Math.max(0, toNumber(row.return_loop_customer_cnt, 0));
            return {
                from: lastVia,
                to: selectedId,
                direction: 'loop-return',
                transitionCustomers: customers,
                peakRate: 0
            };
        });
}

function getReturnPatternSummary(selectedId) {
    const targetId = String(selectedId || '').trim();
    if (!targetId) return [];
    return (AppState.data.returnGravityLoopDetail || [])
        .filter((row) => String(firstDefinedValue(row.product_id, row.anchor_product_id, '')).trim() === targetId)
        .map((row) => ({
            id: String(firstDefinedValue(row.last_via_product_id, row.via_product_id, row.intermediate_product_id, '')).trim(),
            name: getProductName(String(firstDefinedValue(row.last_via_product_id, row.via_product_id, row.intermediate_product_id, '')).trim()),
            count: Math.max(0, toNumber(row.return_loop_customer_cnt, 0))
        }))
        .filter((item) => item.id && item.name && item.count > 0)
        .sort((a, b) => b.count - a.count)
        .reduce((acc, item) => {
            if (acc.some((existing) => existing.id === item.id)) return acc;
            acc.push(item);
            return acc;
        }, [])
        .slice(0, 3);
}

function getDemandGraphNodeLookup() {
    return new Map(
        (AppState.data.insightDemandGraphNodes || []).map((row) => [String(row.product_id || '').trim(), row])
    );
}

function getDemandGraphNodeInfo(id, nodeLookup = getDemandGraphNodeLookup()) {
    const targetId = String(id || '').trim();
    const node = nodeLookup.get(targetId) || {};
    return {
        id: targetId,
        name: node.product_name_latest || getProductName(targetId),
        sizeScore: toNumber(node.node_size_score, 0)
    };
}

function parseDemandGraphPatternIds(row) {
    const related = String(row.related_product_ids || '').trim();
    if (related) {
        return related.split(/[|,>]/).map((part) => String(part || '').trim()).filter(Boolean);
    }
    return String(row.product_path || '').trim().split(/[|>]/).map((part) => String(part || '').trim()).filter(Boolean);
}

function buildTransitionDemandGraphModel(selectedId) {
    const nodeLookup = getDemandGraphNodeLookup();
    const selected = getDemandGraphNodeInfo(selectedId, nodeLookup);
    const edges = (AppState.data.insightDemandGraphEdges || [])
        .map((row) => ({
            source: String(row.source_product_id || '').trim(),
            target: String(row.target_product_id || '').trim(),
            customers: Math.max(0, toNumber(row.transition_customer_cnt, 0)),
            rate: toNumber(row.transition_rate, 0)
        }))
        .filter((row) => row.source && row.target && row.customers > 0);

    const relevant = edges.filter((edge) => edge.source === selectedId || edge.target === selectedId);
    const incoming = relevant
        .filter((edge) => edge.target === selectedId && edge.source !== selectedId)
        .sort((a, b) => b.customers - a.customers || b.rate - a.rate)
        .slice(0, DEMAND_GRAPH_TRANSITION_LIMIT)
        .map((edge) => ({
            ...getDemandGraphNodeInfo(edge.source, nodeLookup),
            count: edge.customers,
            subtitle: `이전 전이 고객 ${formatNumber(edge.customers, 0)}명`
        }));
    const outgoing = relevant
        .filter((edge) => edge.source === selectedId && edge.target !== selectedId)
        .sort((a, b) => b.customers - a.customers || b.rate - a.rate)
        .slice(0, DEMAND_GRAPH_TRANSITION_LIMIT)
        .map((edge) => ({
            ...getDemandGraphNodeInfo(edge.target, nodeLookup),
            count: edge.customers,
            subtitle: `다음 전이 고객 ${formatNumber(edge.customers, 0)}명`
        }));

    const highlights = [...incoming, ...outgoing]
        .sort((a, b) => toNumber(b.count, 0) - toNumber(a.count, 0))
        .slice(0, DEMAND_GRAPH_SUMMARY_LIMIT);

    return {
        selected: {
            ...selected,
            subtitle: '이전 제품에서 들어오고 다음 제품으로 이어져요'
        },
        incoming,
        outgoing,
        highlights,
        hasData: relevant.length > 0,
        hasDataset: edges.length > 0
    };
}

function buildBasketDemandGraphModel(selectedId) {
    const nodeLookup = getDemandGraphNodeLookup();
    const selected = getDemandGraphNodeInfo(selectedId, nodeLookup);
    const grouped = new Map();
    (AppState.data.insightDemandGraphPatterns || [])
        .filter((row) => String(row.pattern_type || '').trim() === 'basket_pair')
        .forEach((row) => {
            const anchorId = String(row.anchor_product_id || '').trim();
            const ids = parseDemandGraphPatternIds(row);
            const relatedIds = Array.from(new Set([anchorId, ...ids].filter(Boolean)));
            if (!relatedIds.includes(selectedId)) return;
            relatedIds.filter((id) => id && id !== selectedId).forEach((id) => {
                if (!grouped.has(id)) {
                    grouped.set(id, {
                        ...getDemandGraphNodeInfo(id, nodeLookup),
                        support: 0
                    });
                }
                grouped.get(id).support += Math.max(0, toNumber(row.support_value, 0));
            });
        });

    const companions = Array.from(grouped.values())
        .sort((a, b) => b.support - a.support)
        .slice(0, DEMAND_GRAPH_BASKET_LIMIT)
        .map((item) => ({
            ...item,
            count: item.support,
            subtitle: `함께 담긴 주문 ${formatNumber(item.support, 0)}건`
        }));

    return {
        selected: {
            ...selected,
            subtitle: '장바구니 조합 중심'
        },
        left: companions.filter((_, index) => index % 2 === 0),
        right: companions.filter((_, index) => index % 2 === 1),
        highlights: companions.slice(0, DEMAND_GRAPH_SUMMARY_LIMIT),
        hasData: companions.length > 0,
        hasDataset: Array.isArray(AppState.data.insightDemandGraphPatterns) && AppState.data.insightDemandGraphPatterns.some((row) => String(row.pattern_type || '').trim() === 'basket_pair')
    };
}

function buildDemandGraphModel(selectedId) {
    const normalizedSelectedId = String(selectedId || '').trim();
    const tab = normalizeDemandGraphTab(AppState.viewState.products?.demandGraphTab);
    const meta = DEMAND_GRAPH_TAB_META[tab];
    if (!normalizedSelectedId) {
        return { tab, meta, hasAnyDataset: false, hasData: false, selected: null, highlights: [] };
    }
    const tabModel = tab === 'basket'
        ? buildBasketDemandGraphModel(normalizedSelectedId)
        : buildTransitionDemandGraphModel(normalizedSelectedId);
    const hasAnyDataset = (AppState.data.insightDemandGraphEdges || []).length > 0 || (AppState.data.insightDemandGraphPatterns || []).length > 0;
    return {
        tab,
        meta,
        ...tabModel,
        hasAnyDataset
    };
}

function getDemandGraphNodeToneLabel(tone, tab = 'transition') {
    if (tone === 'anchor') return '선택 제품';
    if (tone === 'incoming') return '이전 구매';
    if (tone === 'outgoing') return '다음 구매';
    if (tone === 'basket') return '함께 구매';
    return tab === 'basket' ? '연결 제품' : '관계 제품';
}

function renderDemandGraphNodeSubtitleMarkup(text = '') {
    const subtitle = String(text || '').trim();
    if (!subtitle) return '';
    const pairedMatch = subtitle.match(/^(이전 전이 고객|다음 전이 고객|함께 담긴 주문)\s+(.+)$/);
    if (!pairedMatch) return `<span class="demand-graph-node-subtitle">${escapeHtml(subtitle)}</span>`;
    return `
        <span class="demand-graph-node-subtitle">
            <span class="demand-graph-node-subtitle-label">${escapeHtml(pairedMatch[1])}</span>
            <span class="demand-graph-node-subtitle-value">${escapeHtml(pairedMatch[2])}</span>
        </span>
    `;
}

function renderDemandGraphChevron(role = 'incoming') {
    const normalizedRole = role === 'outgoing' ? 'outgoing' : 'incoming';
    return `
        <span class="demand-graph-anchor-chevron is-${normalizedRole}" data-role="${normalizedRole}-chevron" aria-hidden="true">
            <svg viewBox="0 0 7 14" width="7" height="14" focusable="false" aria-hidden="true">
                <path d="M 1 1 L 6 7 L 1 13" />
            </svg>
        </span>
    `;
}

function renderDemandGraphNodeButton(node, style = '', tab = 'transition') {
    if (!node?.id) return '';
    const composedStyle = `${style}${style && !style.trim().endsWith(';') ? ';' : ''} --node-scale:${toNumber(node.sizeScale, 1).toFixed(3)};${Number.isFinite(node.maxWidth) ? ` --node-max-width:${svgNum(node.maxWidth)}px;` : ''}${Number.isFinite(node.fixedWidth) ? ` --anchor-card-width:${svgNum(node.fixedWidth)}px;` : ''}`;
    const toneLabel = getDemandGraphNodeToneLabel(node.tone, tab);
    const dragAttrs = node.allowDrag === false
        ? 'data-drag-disabled="true"'
        : 'onpointerdown="startDemandGraphNodeDrag(event)"';
    const isAnchorCluster = node.tone === 'anchor' && node.positionMode === 'fixed' && tab === 'transition';
    const cardMarkup = `
        <span
            class="demand-graph-node-card"
            data-graph-key="${escapeHtml(node.key)}"
            data-graph-tone="${escapeHtml(node.tone)}"
        >
            <span class="demand-graph-node-eyebrow">${escapeHtml(toneLabel)}</span>
            <strong>${escapeHtml(node.name)}</strong>
            ${renderDemandGraphNodeSubtitleMarkup(node.subtitle)}
        </span>
    `;
    return `
        <button
            class="demand-graph-node ${node.tone ? `is-${node.tone}` : ''} ${node.tone === 'anchor' ? 'is-anchor' : ''} ${node.positionMode === 'fixed' ? 'is-fixed-layout' : ''} ${isAnchorCluster ? 'is-anchor-cluster is-center-cluster' : ''}"
            type="button"
            data-graph-key="${escapeHtml(node.key)}"
            data-graph-tone="${escapeHtml(node.tone)}"
            data-product-id="${escapeHtml(node.id)}"
            data-strength-level="${escapeHtml(node.strengthLevel || 'mid')}"
            style="${composedStyle}"
            ${dragAttrs}
            onclick="handleDemandGraphNodeClick(event, '${escapeJs(node.id)}')"
            title="${escapeHtml(node.name)}"
        >
            ${isAnchorCluster ? `
                <span class="demand-graph-center-cluster">
                    ${node.hasIncoming ? renderDemandGraphChevron('incoming') : ''}
                    <span class="demand-graph-anchor-cluster">
                        ${cardMarkup}
                    </span>
                    ${node.hasOutgoing ? renderDemandGraphChevron('outgoing') : ''}
                </span>
            ` : cardMarkup}
        </button>
    `;
}

function distributeGraphY(index, total) {
    const presets = {
        1: [34],
        2: [30, 70],
        3: [24, 38, 76],
        4: [22, 36, 64, 78],
        5: [18, 32, 68, 82, 92],
        6: [16, 28, 40, 60, 74, 88]
    };
    const normalizedTotal = Math.max(1, Number(total) || 1);
    const lane = presets[normalizedTotal];
    if (lane) {
        return lane[Math.min(Math.max(index, 0), lane.length - 1)];
    }
    const upperCount = Math.ceil(normalizedTotal / 2);
    const lowerCount = normalizedTotal - upperCount;
    const upperStep = upperCount > 1 ? (40 - 16) / (upperCount - 1) : 0;
    const lowerStep = lowerCount > 1 ? (88 - 60) / (lowerCount - 1) : 0;
    if (index < upperCount) {
        return 16 + (upperStep * index);
    }
    return 60 + (lowerStep * (index - upperCount));
}

function getGraphLaneY(index, total, lane = 'center') {
    const y = distributeGraphY(index, total);
    return lane === 'right' ? 100 - y : y;
}

function createDemandGraphSceneNode(item, key, x, y, tone = 'default') {
    return {
        ...item,
        key,
        x,
        y,
        tone
    };
}

function getDemandGraphNodeScale(count, maxCount, tone = 'default') {
    if (tone === 'anchor') return 1;
    const safeCount = Math.max(0, toNumber(count, 0));
    const safeMax = Math.max(1, toNumber(maxCount, 1));
    const normalized = Math.log1p(safeCount) / Math.log1p(safeMax);
    return 0.9 + (normalized * 0.34);
}

function getDemandGraphStrengthLevel(index, total, tone = 'default') {
    if (tone === 'anchor') return 'anchor';
    if (index === 0) return 'high';
    if (index <= Math.min(2, Math.max(total - 1, 0))) return 'mid';
    return 'low';
}

function getBasketGraphPosition(index, total) {
    const count = Math.max(1, Number(total) || 1);
    const positions = {
        1: [{ x: 50, y: 26 }],
        2: [{ x: 50, y: 24 }, { x: 50, y: 76 }],
        3: [{ x: 26, y: 34 }, { x: 74, y: 34 }, { x: 50, y: 74 }],
        4: [{ x: 26, y: 33 }, { x: 74, y: 33 }, { x: 26, y: 67 }, { x: 74, y: 67 }],
        5: [{ x: 50, y: 22 }, { x: 24, y: 38 }, { x: 76, y: 38 }, { x: 28, y: 72 }, { x: 72, y: 72 }],
        6: [{ x: 50, y: 20 }, { x: 26, y: 34 }, { x: 74, y: 34 }, { x: 22, y: 66 }, { x: 78, y: 66 }, { x: 50, y: 80 }]
    };
    const preset = positions[count];
    if (preset) return preset[Math.min(Math.max(index, 0), preset.length - 1)];
    const angleStart = -90;
    const angle = angleStart + ((360 / count) * index);
    const radians = (angle * Math.PI) / 180;
    return {
        x: 50 + Math.cos(radians) * 28,
        y: 50 + Math.sin(radians) * 28
    };
}

const DEMAND_GRAPH_TRANSITION_FIGMA_LAYOUT = {
    viewportWidth: 708,
    baseHeight: 611,
    scenePaddingX: 20,
    scenePaddingRight: 20,
    centerGap: 94,
    chevronWidth: 7,
    chevronGap: 7,
    mergeOffset: 8,
    incomingRows: [89, 188, 287, 386, 485],
    outgoingRows: [89, 188, 287, 386, 485],
    anchor: { y: 283, width: 156, height: 45 }
};

const DEMAND_GRAPH_BASKET_LAYOUT = {
    viewportWidth: 708,
    baseHeight: 611,
    safeInsetX: 132,
    safeInsetTop: 96,
    safeInsetBottom: 96,
    anchor: { width: 156, height: 45 }
};

const DEMAND_GRAPH_TRANSITION_COMPACT_BREAKPOINT = 960;
const DEMAND_GRAPH_TRANSITION_INCOMING_MAX_WIDTHS = [348, 260, 272, 268, 240];
const DEMAND_GRAPH_TRANSITION_OUTGOING_MAX_WIDTHS = [240, 252, 244, 240, 228];

function normalizeDemandGraphBasketViewportWidth(viewportWidth) {
    const width = Math.round(toNumber(viewportWidth, DEMAND_GRAPH_BASKET_LAYOUT.viewportWidth));
    return width > 0 ? width : DEMAND_GRAPH_BASKET_LAYOUT.viewportWidth;
}

function buildBasketLayout(viewportWidth = DEMAND_GRAPH_BASKET_LAYOUT.viewportWidth) {
    const baseLayout = DEMAND_GRAPH_BASKET_LAYOUT;
    const sceneWidth = normalizeDemandGraphBasketViewportWidth(viewportWidth);
    const safeInsetX = Math.min(
        toNumber(baseLayout.safeInsetX, 132),
        Math.max(56, Math.floor(((sceneWidth - toNumber(baseLayout.anchor?.width, 156)) / 2) - 40))
    );
    return {
        ...baseLayout,
        viewportWidth: sceneWidth,
        sceneWidth,
        sceneHeight: toNumber(baseLayout.baseHeight, 611),
        safeInsetX
    };
}

function getBasketGraphSafeArea(layout = DEMAND_GRAPH_BASKET_LAYOUT) {
    const sceneWidth = Math.max(1, toNumber(layout?.sceneWidth, layout?.viewportWidth || 708));
    const sceneHeight = Math.max(1, toNumber(layout?.sceneHeight, layout?.baseHeight || 611));
    const width = Math.max(0, sceneWidth - (toNumber(layout?.safeInsetX, 0) * 2));
    const height = Math.max(0, sceneHeight - toNumber(layout?.safeInsetTop, 0) - toNumber(layout?.safeInsetBottom, 0));
    const left = Math.max(0, toNumber(layout?.safeInsetX, 0));
    const top = Math.max(0, toNumber(layout?.safeInsetTop, 0));
    return {
        left,
        top,
        width,
        height,
        centerX: left + (width / 2),
        centerY: top + (height / 2)
    };
}

function mapBasketGraphPercentToScene(position, layout = DEMAND_GRAPH_BASKET_LAYOUT) {
    const safeArea = getBasketGraphSafeArea(layout);
    const sceneWidth = Math.max(1, toNumber(layout?.sceneWidth, layout?.viewportWidth || 708));
    const sceneHeight = Math.max(1, toNumber(layout?.sceneHeight, layout?.baseHeight || 611));
    const x = safeArea.left + (safeArea.width * (toNumber(position?.x, 50) / 100));
    const y = safeArea.top + (safeArea.height * (toNumber(position?.y, 50) / 100));
    return {
        x: (x / sceneWidth) * 100,
        y: (y / sceneHeight) * 100
    };
}

function getBasketGraphAnchorPosition(layout = DEMAND_GRAPH_BASKET_LAYOUT) {
    const safeArea = getBasketGraphSafeArea(layout);
    const sceneWidth = Math.max(1, toNumber(layout?.sceneWidth, layout?.viewportWidth || 708));
    const sceneHeight = Math.max(1, toNumber(layout?.sceneHeight, layout?.baseHeight || 611));
    return {
        x: (safeArea.centerX / sceneWidth) * 100,
        y: (safeArea.centerY / sceneHeight) * 100
    };
}

function getTransitionFixedRowY(index, side = 'incoming') {
    const rows = side === 'outgoing'
        ? DEMAND_GRAPH_TRANSITION_FIGMA_LAYOUT.outgoingRows
        : DEMAND_GRAPH_TRANSITION_FIGMA_LAYOUT.incomingRows;
    return rows[Math.min(Math.max(index, 0), rows.length - 1)] ?? rows[rows.length - 1];
}

function getTransitionFixedCardMaxWidth(index, side = 'incoming') {
    const widths = side === 'outgoing'
        ? DEMAND_GRAPH_TRANSITION_OUTGOING_MAX_WIDTHS
        : DEMAND_GRAPH_TRANSITION_INCOMING_MAX_WIDTHS;
    return widths[Math.min(Math.max(index, 0), widths.length - 1)] ?? widths[widths.length - 1];
}

function estimateDemandGraphAnchorWidth(name = '', viewportWidth = DEMAND_GRAPH_TRANSITION_FIGMA_LAYOUT.viewportWidth, layoutMode = 'transition-compact') {
    const text = String(name || '').trim();
    const responsiveBoost = Math.max(0, toNumber(viewportWidth, DEMAND_GRAPH_TRANSITION_FIGMA_LAYOUT.viewportWidth) - DEMAND_GRAPH_TRANSITION_COMPACT_BREAKPOINT);
    const minWidth = layoutMode === 'transition-fluid'
        ? Math.min(188, 156 + Math.round(responsiveBoost * 0.05))
        : 156;
    const maxWidth = layoutMode === 'transition-fluid'
        ? Math.min(360, 260 + Math.round(responsiveBoost * 0.16))
        : 260;
    if (!text) return minWidth;
    const estimated = Math.round((text.length * 9.2) + 42);
    return Math.max(minWidth, Math.min(maxWidth, estimated));
}

function estimateDemandGraphSideCardWidth(item, side = 'incoming', index = 0) {
    const maxWidth = getTransitionFixedCardMaxWidth(index, side);
    const title = String(item?.name || '').trim();
    const subtitle = String(item?.subtitle || '').trim();
    const titleWidth = title.length * 8.4;
    const subtitleWidth = subtitle.length * 6.8;
    const estimated = Math.round(Math.max(titleWidth, subtitleWidth) + 30);
    return Math.max(132, Math.min(maxWidth, estimated));
}

function getDemandGraphMeasuredLayoutKey(model) {
    const selectedId = String(model?.selected?.id || '').trim();
    const tab = normalizeDemandGraphTab(model?.tab);
    return selectedId ? `${tab}:${selectedId}` : '';
}

function getDemandGraphMeasuredLayoutCache() {
    if (!AppState.helpers.demandGraphMeasuredLayouts) {
        AppState.helpers.demandGraphMeasuredLayouts = {};
    }
    return AppState.helpers.demandGraphMeasuredLayouts;
}

function getDemandGraphMeasuredMetrics(model) {
    const key = getDemandGraphMeasuredLayoutKey(model);
    if (!key) return null;
    return getDemandGraphMeasuredLayoutCache()[key] || null;
}

function setDemandGraphMeasuredMetrics(model, metrics) {
    const key = getDemandGraphMeasuredLayoutKey(model);
    if (!key || !metrics) return;
    getDemandGraphMeasuredLayoutCache()[key] = {
        maxIncomingCardWidth: Math.max(0, toNumber(metrics.maxIncomingCardWidth, 0)),
        anchorCardWidth: Math.max(0, toNumber(metrics.anchorCardWidth, 0)),
        maxOutgoingCardWidth: Math.max(0, toNumber(metrics.maxOutgoingCardWidth, 0))
    };
}

function areDemandGraphMeasuredMetricsEqual(a, b, tolerance = 1) {
    if (!a || !b) return false;
    return ['maxIncomingCardWidth', 'anchorCardWidth', 'maxOutgoingCardWidth']
        .every((key) => Math.abs(toNumber(a[key], 0) - toNumber(b[key], 0)) <= tolerance);
}

function getDemandGraphViewportWidth(sceneElement = null) {
    const directViewport = sceneElement?.closest?.('.demand-graph-scene-viewport');
    const parentViewport = sceneElement?.parentElement?.classList?.contains('demand-graph-scene-viewport')
        ? sceneElement.parentElement
        : null;
    const viewport = directViewport || parentViewport || document.querySelector('.demand-graph-scene-viewport');
    const width = Math.round(toNumber(viewport?.clientWidth, 0));
    return width > 0 ? width : DEMAND_GRAPH_TRANSITION_FIGMA_LAYOUT.viewportWidth;
}

function getDemandGraphTransitionLayoutMode(viewportWidth) {
    return toNumber(viewportWidth, DEMAND_GRAPH_TRANSITION_FIGMA_LAYOUT.viewportWidth) <= DEMAND_GRAPH_TRANSITION_COMPACT_BREAKPOINT
        ? 'transition-compact'
        : 'transition-fluid';
}

function normalizeDemandGraphTransitionViewportWidth(viewportWidth) {
    const rawWidth = Math.max(
        DEMAND_GRAPH_TRANSITION_FIGMA_LAYOUT.viewportWidth,
        Math.round(toNumber(viewportWidth, DEMAND_GRAPH_TRANSITION_FIGMA_LAYOUT.viewportWidth))
    );
    if (getDemandGraphTransitionLayoutMode(rawWidth) !== 'transition-fluid') {
        return rawWidth;
    }
    return Math.max(
        DEMAND_GRAPH_TRANSITION_COMPACT_BREAKPOINT + 24,
        Math.round(rawWidth / 24) * 24
    );
}

function buildTransitionFixedLayout(model, viewportWidth = DEMAND_GRAPH_TRANSITION_FIGMA_LAYOUT.viewportWidth, measuredMetrics = null) {
    const baseLayout = DEMAND_GRAPH_TRANSITION_FIGMA_LAYOUT;
    const normalizedViewportWidth = normalizeDemandGraphTransitionViewportWidth(viewportWidth);
    const layoutMode = getDemandGraphTransitionLayoutMode(normalizedViewportWidth);
    const estimatedAnchorWidth = estimateDemandGraphAnchorWidth(model?.selected?.name, normalizedViewportWidth, layoutMode);
    const incomingWidths = (model?.incoming || []).map((item, index) => estimateDemandGraphSideCardWidth(item, 'incoming', index));
    const outgoingWidths = (model?.outgoing || []).map((item, index) => estimateDemandGraphSideCardWidth(item, 'outgoing', index));
    const estimatedLeftColumnWidth = incomingWidths.length ? Math.max(...incomingWidths) : 0;
    const estimatedRightColumnWidth = outgoingWidths.length ? Math.max(...outgoingWidths) : 0;
    const measuredAnchorWidth = toNumber(measuredMetrics?.anchorCardWidth, NaN);
    const measuredLeftColumnWidth = toNumber(measuredMetrics?.maxIncomingCardWidth, NaN);
    const measuredRightColumnWidth = toNumber(measuredMetrics?.maxOutgoingCardWidth, NaN);
    const anchorWidth = Number.isFinite(measuredAnchorWidth) && measuredAnchorWidth > 0
        ? measuredAnchorWidth
        : estimatedAnchorWidth;
    const leftColumnWidth = Number.isFinite(measuredLeftColumnWidth) && measuredLeftColumnWidth > 0
        ? measuredLeftColumnWidth
        : estimatedLeftColumnWidth;
    const rightColumnWidth = Number.isFinite(measuredRightColumnWidth) && measuredRightColumnWidth > 0
        ? measuredRightColumnWidth
        : estimatedRightColumnWidth;
    const hasIncoming = incomingWidths.length > 0;
    const hasOutgoing = outgoingWidths.length > 0;
    const incomingLeadWidth = hasIncoming ? baseLayout.chevronWidth + baseLayout.chevronGap : 0;
    const outgoingTrailWidth = hasOutgoing ? baseLayout.chevronGap + baseLayout.chevronWidth : 0;
    let boxGap = baseLayout.centerGap;
    let viewportCenterX = Math.round(normalizedViewportWidth / 2);
    let anchorCardX;
    let leftColumnX;
    let rightColumnX;
    let rightColumnRight;
    let sceneWidth;
    let initialScrollLeft;

    if (layoutMode === 'transition-fluid') {
        anchorCardX = viewportCenterX - Math.round(anchorWidth / 2);
        const availableLeftGap = Math.max(baseLayout.centerGap, anchorCardX - baseLayout.scenePaddingX - leftColumnWidth);
        const availableRightGap = Math.max(
            baseLayout.centerGap,
            normalizedViewportWidth - baseLayout.scenePaddingRight - (anchorCardX + anchorWidth) - rightColumnWidth
        );
        const maxFluidGap = Math.max(baseLayout.centerGap, Math.min(availableLeftGap, availableRightGap));
        boxGap = Math.round(baseLayout.centerGap + ((maxFluidGap - baseLayout.centerGap) * 0.7));
        leftColumnX = anchorCardX - boxGap - leftColumnWidth;
        rightColumnX = anchorCardX + anchorWidth + boxGap;
        rightColumnRight = rightColumnX + rightColumnWidth;
        sceneWidth = Math.max(
            normalizedViewportWidth,
            rightColumnRight + baseLayout.scenePaddingRight
        );
        initialScrollLeft = 0;
    } else {
        leftColumnX = baseLayout.scenePaddingX;
        anchorCardX = leftColumnX + leftColumnWidth + boxGap;
        rightColumnX = anchorCardX + anchorWidth + boxGap;
        rightColumnRight = rightColumnX + rightColumnWidth;
        sceneWidth = Math.max(
            normalizedViewportWidth,
            baseLayout.viewportWidth,
            rightColumnRight + baseLayout.scenePaddingRight
        );
        const rightPreviewWidth = Math.min(rightColumnWidth, 96);
        const preferredVisibleWidth = rightColumnX + rightPreviewWidth + baseLayout.scenePaddingRight;
        initialScrollLeft = Math.max(
            0,
            Math.min(
                sceneWidth - normalizedViewportWidth,
                preferredVisibleWidth - normalizedViewportWidth
            )
        );
    }

    const anchorCardRight = anchorCardX + anchorWidth;
    const leftColumnInnerEdge = leftColumnX + leftColumnWidth;
    const rightColumnInnerEdge = rightColumnX;
    const centerClusterX = anchorCardX - incomingLeadWidth;
    const centerClusterWidth = incomingLeadWidth + anchorWidth + outgoingTrailWidth;
    return {
        ...baseLayout,
        layoutMode,
        measured: Boolean(measuredMetrics),
        viewportWidth: normalizedViewportWidth,
        viewportCenterX,
        boxGap,
        anchorCardX,
        anchorCardWidth: anchorWidth,
        anchorCardRight,
        leftColumnInnerEdge,
        leftColumnX,
        leftColumnWidth,
        rightColumnInnerEdge,
        rightColumnWidth,
        centerClusterWidth,
        centerClusterX,
        rightColumnX,
        rightColumnRight,
        sceneWidth,
        initialScrollLeft,
        anchor: {
            ...baseLayout.anchor,
            width: anchorWidth
        },
        hasIncoming,
        hasOutgoing
    };
}

function buildDemandGraphScene(model) {
    const nodes = [];
    const edges = [];
    const maxCount = Math.max(1, ...(model.highlights || []).map((item) => toNumber(item.count, 0)));

    if (model.tab === 'basket') {
        const layout = buildBasketLayout(getDemandGraphViewportWidth());
        const anchorPosition = getBasketGraphAnchorPosition(layout);
        const anchorWidth = estimateDemandGraphAnchorWidth(
            model?.selected?.name,
            layout.sceneWidth,
            getDemandGraphTransitionLayoutMode(layout.sceneWidth)
        );
        nodes.push({
            ...createDemandGraphSceneNode(model.selected, 'anchor', anchorPosition.x, anchorPosition.y, 'anchor'),
            sizeScale: 1,
            strengthLevel: 'anchor',
            positionMode: 'center',
            allowDrag: true,
            fixedWidth: anchorWidth
        });
        const companions = model.highlights || [];
        companions.forEach((item, index, arr) => {
            const key = `basket-${index}`;
            const position = mapBasketGraphPercentToScene(getBasketGraphPosition(index, arr.length), layout);
            nodes.push({
                ...createDemandGraphSceneNode(item, key, position.x, position.y, 'basket'),
                sizeScale: 1,
                strengthLevel: getDemandGraphStrengthLevel(index, arr.length, 'basket'),
                positionMode: 'center',
                allowDrag: true
            });
            edges.push({
                fromKey: 'anchor',
                toKey: key,
                kind: 'basket',
                fromSide: position.x >= 50 ? 'right' : 'left',
                toSide: position.x >= 50 ? 'left' : 'right',
                offsetIndex: index,
                offsetCount: arr.length,
                strokeWidth: 1.6
            });
        });
        return {
            layout: 'basket',
            canvasWidth: layout.sceneWidth,
            canvasHeight: layout.sceneHeight,
            layoutData: layout,
            nodes,
            edges
        };
    }

    const layout = buildTransitionFixedLayout(model, getDemandGraphViewportWidth(), getDemandGraphMeasuredMetrics(model));
    nodes.push({
        ...createDemandGraphSceneNode(model.selected, 'anchor', layout.centerClusterX, layout.anchor.y, 'anchor'),
        sizeScale: 1,
        strengthLevel: 'anchor',
        positionMode: 'fixed',
        allowDrag: false,
        fixedWidth: layout.anchor.width,
        hasIncoming: layout.hasIncoming,
        hasOutgoing: layout.hasOutgoing
    });

    (model.incoming || []).forEach((item, index, arr) => {
        const key = `incoming-${index}`;
        nodes.push({
            ...createDemandGraphSceneNode(item, key, layout.leftColumnX, getTransitionFixedRowY(index, 'incoming'), 'incoming'),
            sizeScale: 1,
            strengthLevel: getDemandGraphStrengthLevel(index, arr.length, 'incoming'),
            positionMode: 'fixed',
            allowDrag: false,
            maxWidth: getTransitionFixedCardMaxWidth(index, 'incoming')
        });
        edges.push({
            fromKey: key,
            toKey: 'anchor',
            kind: 'incoming',
            fromSide: 'right',
            toSide: 'left',
            offsetIndex: index,
            offsetCount: arr.length,
            strokeWidth: 1.6
        });
    });

    (model.outgoing || []).forEach((item, index, arr) => {
        const key = `outgoing-${index}`;
        nodes.push({
            ...createDemandGraphSceneNode(item, key, layout.rightColumnRight, getTransitionFixedRowY(index, 'outgoing'), 'outgoing'),
            sizeScale: 1,
            strengthLevel: getDemandGraphStrengthLevel(index, arr.length, 'outgoing'),
            positionMode: 'fixed',
            positionEdge: 'right',
            allowDrag: false,
            maxWidth: getTransitionFixedCardMaxWidth(index, 'outgoing')
        });
        edges.push({
            fromKey: 'anchor',
            toKey: key,
            kind: 'outgoing',
            fromSide: 'right',
            toSide: 'left',
            offsetIndex: index,
            offsetCount: arr.length,
            strokeWidth: 1.6
        });
    });

    return {
        layout: 'transition-fixed',
        canvasWidth: layout.sceneWidth,
        canvasHeight: layout.baseHeight,
        layoutData: layout,
        nodes,
        edges
    };
}

function getDemandGraphNodePort(rect, sceneRect, side = 'right', offsetPx = 0) {
    const left = rect.left - sceneRect.left;
    const right = rect.right - sceneRect.left;
    const top = rect.top - sceneRect.top;
    const bottom = rect.bottom - sceneRect.top;
    const centerY = (top + bottom) / 2;
    const y = Math.min(bottom - 12, Math.max(top + 12, centerY + offsetPx));
    return {
        x: side === 'left' ? left : right,
        y
    };
}

function getDemandGraphRadialPort(rect, sceneRect, targetX, targetY) {
    const left = rect.left - sceneRect.left;
    const right = rect.right - sceneRect.left;
    const top = rect.top - sceneRect.top;
    const bottom = rect.bottom - sceneRect.top;
    const cx = (left + right) / 2;
    const cy = (top + bottom) / 2;
    const dx = targetX - cx;
    const dy = targetY - cy;
    const halfW = Math.max((right - left) / 2, 1);
    const halfH = Math.max((bottom - top) / 2, 1);
    const scale = 1 / Math.max(Math.abs(dx) / halfW || 0, Math.abs(dy) / halfH || 0, 1);
    return {
        x: cx + (dx * scale),
        y: cy + (dy * scale)
    };
}

function getDemandGraphOffsetPx(index, count) {
    if (!count || count <= 1) return 0;
    return (index - ((count - 1) / 2)) * 12;
}

function buildMeasuredDemandGraphPath(edge, rectMap, sceneRect) {
    const fromRect = rectMap.get(edge.fromKey);
    const toRect = rectMap.get(edge.toKey);
    if (!fromRect || !toRect) return null;

    const offsetPx = getDemandGraphOffsetPx(edge.offsetIndex, edge.offsetCount);
    const from = getDemandGraphNodePort(
        fromRect,
        sceneRect,
        edge.fromSide,
        edge.fromKey === 'anchor' ? offsetPx : 0
    );
    const to = getDemandGraphNodePort(
        toRect,
        sceneRect,
        edge.toSide,
        edge.toKey === 'anchor' ? offsetPx : 0
    );
    const horizontalGap = Math.abs(to.x - from.x);
    const curve = Math.max(36, horizontalGap * 0.28);
    const fromCurveX = edge.fromSide === 'right' ? from.x + curve : from.x - curve;
    const toCurveX = edge.toSide === 'left' ? to.x - curve : to.x + curve;
    return {
        from,
        to,
        path: `M ${from.x} ${from.y} C ${fromCurveX} ${from.y}, ${toCurveX} ${to.y}, ${to.x} ${to.y}`
    };
}

function getDemandGraphArrowTip(from, to, inset = 8) {
    const dx = to.x - from.x;
    const dy = to.y - from.y;
    const distance = Math.hypot(dx, dy) || 1;
    if (distance <= inset) return { ...to };
    const ratio = (distance - inset) / distance;
    return {
        x: from.x + (dx * ratio),
        y: from.y + (dy * ratio)
    };
}

function buildDemandGraphArrowMarkup(kind, control, tip) {
    if (!tip || !control) return '';
    const dx = tip.x - control.x;
    const dy = tip.y - control.y;
    const distance = Math.hypot(dx, dy) || 1;
    const unitX = dx / distance;
    const unitY = dy / distance;
    const normalX = -unitY;
    const normalY = unitX;
    const arrowLength = 7.5;
    const arrowHalfWidth = 4.5;
    const baseX = tip.x - (unitX * arrowLength);
    const baseY = tip.y - (unitY * arrowLength);
    const leftX = baseX + (normalX * arrowHalfWidth);
    const leftY = baseY + (normalY * arrowHalfWidth);
    const rightX = baseX - (normalX * arrowHalfWidth);
    const rightY = baseY - (normalY * arrowHalfWidth);
    return `
        <path
            class="demand-graph-arrow is-${kind}"
            d="M ${svgNum(leftX)} ${svgNum(leftY)} L ${svgNum(tip.x)} ${svgNum(tip.y)} L ${svgNum(rightX)} ${svgNum(rightY)}"
        />
    `;
}

function buildTransitionFixedCurve(from, to, direction = 'incoming') {
    const deltaX = Math.abs(to.x - from.x);
    const c1Offset = Math.min(132, Math.max(72, deltaX * 0.42));
    const c2Offset = Math.min(120, Math.max(56, deltaX * 0.2));
    if (direction === 'incoming') {
        return `M ${from.x} ${from.y} C ${from.x + c1Offset} ${from.y}, ${to.x - c2Offset} ${to.y}, ${to.x} ${to.y}`;
    }
    return `M ${from.x} ${from.y} C ${from.x + c2Offset} ${from.y}, ${to.x - c1Offset} ${to.y}, ${to.x} ${to.y}`;
}

function getDemandGraphScenePointFromRect(rect, sceneRect, xFactor = 0, yFactor = 0.5) {
    return {
        x: (rect.left - sceneRect.left) + (rect.width * xFactor),
        y: (rect.top - sceneRect.top) + (rect.height * yFactor)
    };
}

function getDemandGraphChevronGeometry(sceneElement, sceneRect, layout, role = 'incoming') {
    const normalizedRole = role === 'outgoing' ? 'outgoing' : 'incoming';
    const chevronElement = sceneElement.querySelector(`.demand-graph-anchor-chevron[data-role="${normalizedRole}-chevron"]`);
    const anchorCenterY = layout.anchor.y + (layout.anchor.height / 2);
    const fallbackX = normalizedRole === 'incoming'
        ? layout.centerClusterX
        : layout.centerClusterX + layout.centerClusterWidth;
    if (!chevronElement) {
        return {
            chevronPoint: { x: fallbackX, y: anchorCenterY },
            mergePoint: {
                x: fallbackX + (normalizedRole === 'incoming' ? -layout.mergeOffset : layout.mergeOffset),
                y: anchorCenterY
            }
        };
    }
    const chevronRect = chevronElement.getBoundingClientRect();
    const chevronPoint = getDemandGraphScenePointFromRect(
        chevronRect,
        sceneRect,
        normalizedRole === 'incoming' ? 0 : 1,
        0.5
    );
    return {
        chevronPoint,
        mergePoint: {
            x: chevronPoint.x + (normalizedRole === 'incoming' ? -layout.mergeOffset : layout.mergeOffset),
            y: chevronPoint.y
        }
    };
}

function buildDemandGraphTransitionFixedEdgeMarkup(sceneElement, scene, rectMap, sceneRect) {
    const layout = scene.layoutData || DEMAND_GRAPH_TRANSITION_FIGMA_LAYOUT;
    const incomingGeometry = getDemandGraphChevronGeometry(sceneElement, sceneRect, layout, 'incoming');
    const outgoingGeometry = getDemandGraphChevronGeometry(sceneElement, sceneRect, layout, 'outgoing');
    const markup = [];

    scene.edges.filter((edge) => edge.kind === 'incoming').forEach((edge) => {
        const fromRect = rectMap.get(edge.fromKey);
        if (!fromRect) return;
        const from = getDemandGraphNodePort(fromRect, sceneRect, 'right');
        const path = buildTransitionFixedCurve(from, incomingGeometry.mergePoint, 'incoming');
        markup.push(`
            <path class="demand-graph-line-glow is-incoming" d="${path}" stroke-width="4.8" />
            <path class="demand-graph-line is-incoming" d="${path}" stroke-width="1.6" />
        `);
    });

    if (scene.edges.some((edge) => edge.kind === 'incoming')) {
        markup.push(`
            <path class="demand-graph-line-glow is-incoming" d="M ${svgNum(incomingGeometry.mergePoint.x)} ${svgNum(incomingGeometry.mergePoint.y)} L ${svgNum(incomingGeometry.chevronPoint.x)} ${svgNum(incomingGeometry.chevronPoint.y)}" stroke-width="4.8" />
            <path class="demand-graph-line is-incoming" d="M ${svgNum(incomingGeometry.mergePoint.x)} ${svgNum(incomingGeometry.mergePoint.y)} L ${svgNum(incomingGeometry.chevronPoint.x)} ${svgNum(incomingGeometry.chevronPoint.y)}" stroke-width="1.6" />
        `);
    }

    if (scene.edges.some((edge) => edge.kind === 'outgoing')) {
        markup.push(`
            <path class="demand-graph-line-glow is-outgoing" d="M ${svgNum(outgoingGeometry.chevronPoint.x)} ${svgNum(outgoingGeometry.chevronPoint.y)} L ${svgNum(outgoingGeometry.mergePoint.x)} ${svgNum(outgoingGeometry.mergePoint.y)}" stroke-width="4.8" />
            <path class="demand-graph-line is-outgoing" d="M ${svgNum(outgoingGeometry.chevronPoint.x)} ${svgNum(outgoingGeometry.chevronPoint.y)} L ${svgNum(outgoingGeometry.mergePoint.x)} ${svgNum(outgoingGeometry.mergePoint.y)}" stroke-width="1.6" />
        `);
    }

    scene.edges.filter((edge) => edge.kind === 'outgoing').forEach((edge) => {
        const toRect = rectMap.get(edge.toKey);
        if (!toRect) return;
        const to = getDemandGraphNodePort(toRect, sceneRect, 'left');
        const path = buildTransitionFixedCurve(outgoingGeometry.mergePoint, to, 'outgoing');
        markup.push(`
            <path class="demand-graph-line-glow is-outgoing" d="${path}" stroke-width="4.8" />
            <path class="demand-graph-line is-outgoing" d="${path}" stroke-width="1.6" />
        `);
    });

    return markup.join('');
}

function buildDemandGraphEdgeMarkup(scene) {
    const sceneElement = document.querySelector('.demand-graph-scene');
    if (!sceneElement) return '';
    const sceneRect = sceneElement.getBoundingClientRect();
    if (!sceneRect.width || !sceneRect.height) return '';
    const rectMap = new Map();
    sceneElement.querySelectorAll('.demand-graph-node-card[data-graph-key]').forEach((element) => {
        rectMap.set(String(element.dataset.graphKey || '').trim(), element.getBoundingClientRect());
    });
    const anchorRect = rectMap.get('anchor');
    if (!anchorRect) return '';
    if (scene.layout === 'transition-fixed') {
        return buildDemandGraphTransitionFixedEdgeMarkup(sceneElement, scene, rectMap, sceneRect);
    }
    const anchorCenterY = ((anchorRect.top - sceneRect.top) + (anchorRect.bottom - sceneRect.top)) / 2;
    const leftHub = { x: anchorRect.left - sceneRect.left - 3, y: anchorCenterY };
    const rightHub = { x: anchorRect.right - sceneRect.left + 3, y: anchorCenterY };
    const edgeMarkup = scene.edges.map((edge) => {
        const geometry = buildMeasuredDemandGraphPath(edge, rectMap, sceneRect);
        if (!geometry) return '';
        let { from, to } = geometry;
        if (edge.kind !== 'basket' && edge.toKey === 'anchor') {
            to = leftHub;
        } else if (edge.kind !== 'basket' && edge.fromKey === 'anchor') {
            from = rightHub;
        }
        const renderTo = edge.kind === 'basket' ? to : getDemandGraphArrowTip(from, to, 8);
        const horizontalGap = Math.abs(to.x - from.x);
        const curve = Math.max(24, horizontalGap * 0.28);
        const fromCurveX = edge.fromSide === 'right' ? from.x + curve : from.x - curve;
        const toCurveX = edge.toSide === 'left' ? renderTo.x - curve : renderTo.x + curve;
        const path = `M ${from.x} ${from.y} C ${fromCurveX} ${from.y}, ${toCurveX} ${renderTo.y}, ${renderTo.x} ${renderTo.y}`;
        return `
            <path
                class="demand-graph-line-glow is-${edge.kind}"
                d="${path}"
                stroke-width="4.8"
            />
            <path
                class="demand-graph-line is-${edge.kind}"
                d="${path}"
                stroke-width="${edge.strokeWidth}"
            />
        `;
    }).join('');
    const connectorPaths = [];
    if (scene.edges.some((edge) => edge.kind !== 'basket' && edge.toKey === 'anchor')) {
        const incomingTip = getDemandGraphArrowTip(leftHub, { x: anchorRect.left - sceneRect.left, y: anchorCenterY }, 8);
        connectorPaths.push(`
            <path class="demand-graph-line-glow is-incoming" d="M ${leftHub.x} ${leftHub.y} L ${incomingTip.x} ${incomingTip.y}" stroke-width="5.2" />
            <path class="demand-graph-line is-incoming" d="M ${leftHub.x} ${leftHub.y} L ${incomingTip.x} ${incomingTip.y}" stroke-width="1.6" />
            ${buildDemandGraphArrowMarkup('incoming', leftHub, incomingTip)}
        `);
    }
    if (scene.edges.some((edge) => edge.kind !== 'basket' && edge.fromKey === 'anchor')) {
        const outgoingTip = getDemandGraphArrowTip(
            { x: anchorRect.right - sceneRect.left, y: anchorCenterY },
            rightHub,
            2
        );
        connectorPaths.push(`
            <path class="demand-graph-line-glow is-outgoing" d="M ${anchorRect.right - sceneRect.left} ${anchorCenterY} L ${outgoingTip.x} ${outgoingTip.y}" stroke-width="5.2" />
            <path class="demand-graph-line is-outgoing" d="M ${anchorRect.right - sceneRect.left} ${anchorCenterY} L ${outgoingTip.x} ${outgoingTip.y}" stroke-width="1.6" />
            ${buildDemandGraphArrowMarkup('outgoing', { x: anchorRect.right - sceneRect.left, y: anchorCenterY }, outgoingTip)}
        `);
    }
    return `${edgeMarkup}${connectorPaths.join('')}`;
}

function measureDemandGraphTransitionLayout(sceneElement) {
    if (!sceneElement?.classList.contains('is-transition-fixed')) return null;
    const anchorCard = sceneElement.querySelector('.demand-graph-node.is-anchor .demand-graph-node-card');
    if (!anchorCard) return null;
    const getMaxWidth = (selector) => {
        const widths = Array.from(sceneElement.querySelectorAll(selector))
            .map((element) => element.getBoundingClientRect().width)
            .filter((width) => width > 0);
        return widths.length ? Math.max(...widths) : 0;
    };
    return {
        maxIncomingCardWidth: getMaxWidth('.demand-graph-node.is-incoming .demand-graph-node-card'),
        anchorCardWidth: anchorCard.getBoundingClientRect().width,
        maxOutgoingCardWidth: getMaxWidth('.demand-graph-node.is-outgoing .demand-graph-node-card')
    };
}

function getDemandGraphSceneLayoutMetrics(sceneElement) {
    if (!sceneElement) return null;
    return {
        maxIncomingCardWidth: Math.max(0, toNumber(sceneElement.dataset.layoutIncomingWidth, 0)),
        anchorCardWidth: Math.max(0, toNumber(sceneElement.dataset.layoutAnchorWidth, 0)),
        maxOutgoingCardWidth: Math.max(0, toNumber(sceneElement.dataset.layoutOutgoingWidth, 0)),
        viewportWidth: Math.max(0, toNumber(sceneElement.dataset.layoutViewportWidth, 0)),
        layoutMode: String(sceneElement.dataset.layoutMode || '').trim()
    };
}

function syncDemandGraphMeasuredLayout(model, sceneElement) {
    if (!sceneElement?.classList.contains('is-transition-fixed')) return false;
    if (normalizeDemandGraphTab(model?.tab) !== 'transition') return false;
    const viewportWidth = getDemandGraphViewportWidth(sceneElement);
    const normalizedViewportWidth = normalizeDemandGraphTransitionViewportWidth(viewportWidth);
    const expectedLayoutMode = getDemandGraphTransitionLayoutMode(viewportWidth);
    const currentMetrics = getDemandGraphSceneLayoutMetrics(sceneElement);
    if (
        !currentMetrics
        || Math.abs(normalizedViewportWidth - toNumber(currentMetrics.viewportWidth, 0)) > 12
        || currentMetrics.layoutMode !== expectedLayoutMode
    ) {
        const rerender = AppState.helpers.rerenderProductsSelection;
        if (typeof rerender === 'function') {
            const pendingScrollTarget = String(AppState.helpers.productsPendingScrollTarget || '').trim();
            rerender(!pendingScrollTarget, pendingScrollTarget);
            return true;
        }
        return false;
    }
    const measuredMetrics = measureDemandGraphTransitionLayout(sceneElement);
    if (!measuredMetrics) return false;
    if (areDemandGraphMeasuredMetricsEqual(measuredMetrics, currentMetrics, 1)) {
        return false;
    }
    setDemandGraphMeasuredMetrics(model, measuredMetrics);
    const rerender = AppState.helpers.rerenderProductsSelection;
    if (typeof rerender === 'function') {
        const pendingScrollTarget = String(AppState.helpers.productsPendingScrollTarget || '').trim();
        rerender(!pendingScrollTarget, pendingScrollTarget);
        return true;
    }
    return false;
}

function syncDemandGraphBasketLayout(model, sceneElement) {
    if (!sceneElement?.classList.contains('is-basket-layout')) return false;
    if (normalizeDemandGraphTab(model?.tab) !== 'basket') return false;
    const viewportWidth = normalizeDemandGraphBasketViewportWidth(getDemandGraphViewportWidth(sceneElement));
    const currentViewportWidth = Math.max(0, toNumber(sceneElement.dataset.layoutViewportWidth, 0));
    if (Math.abs(viewportWidth - currentViewportWidth) <= 1) {
        return false;
    }
    const rerender = AppState.helpers.rerenderProductsSelection;
    if (typeof rerender === 'function') {
        const pendingScrollTarget = String(AppState.helpers.productsPendingScrollTarget || '').trim();
        rerender(!pendingScrollTarget, pendingScrollTarget);
        return true;
    }
    return false;
}

function scheduleDemandGraphEdgeLayout() {
    if (AppState.viewState.products?.chartView !== 'demand-graph') return;
    const selectedId = String(AppState.viewState.products?.quadrant?.selectedId || '').trim();
    if (!selectedId) return;
    const run = () => {
        const sceneElement = document.querySelector('.demand-graph-scene');
        if (!sceneElement) return;
        const model = buildDemandGraphModel(selectedId);
        if (syncDemandGraphMeasuredLayout(model, sceneElement)) return;
        if (syncDemandGraphBasketLayout(model, sceneElement)) return;
        applyDemandGraphInitialFraming();
        syncDemandGraphViewportState();
        const svg = sceneElement.querySelector('.demand-graph-svg');
        const edgeLayer = sceneElement.querySelector('.demand-graph-edge-layer');
        if (!svg || !edgeLayer) return;
        const sceneRect = sceneElement.getBoundingClientRect();
        svg.setAttribute('viewBox', `0 0 ${Math.max(sceneRect.width, 1)} ${Math.max(sceneRect.height, 1)}`);
        const scene = buildDemandGraphScene(model);
        edgeLayer.innerHTML = buildDemandGraphEdgeMarkup(scene);
    };
    requestAnimationFrame(() => {
        run();
        requestAnimationFrame(run);
    });
}

function ensureDemandGraphResizeHandler() {
    if (AppState.helpers.demandGraphResizeHandlerAttached) return;
    AppState.helpers.demandGraphResizeHandlerAttached = true;
    window.addEventListener('resize', scheduleDemandGraphEdgeLayout);
}

function applyDemandGraphInitialFraming() {
    const viewport = document.querySelector('.demand-graph-scene-viewport');
    const scene = viewport?.querySelector('.demand-graph-scene');
    if (!viewport || !scene) return;
    const frameKey = String(scene.dataset.frameKey || '').trim();
    if (!frameKey) return;
    if (viewport.dataset.frameKeyApplied === frameKey) return;
    const initialScrollLeft = Math.max(0, toNumber(scene.dataset.initialScrollLeft, 0));
    viewport.scrollLeft = Math.min(initialScrollLeft, Math.max(0, viewport.scrollWidth - viewport.clientWidth));
    viewport.dataset.frameKeyApplied = frameKey;
}

function syncDemandGraphViewportState() {
    const viewport = document.querySelector('.demand-graph-scene-viewport');
    if (!viewport) return;
    const canPan = viewport.scrollWidth > viewport.clientWidth + 2;
    viewport.classList.toggle('is-pan-enabled', canPan);
    if (!canPan) {
        viewport.classList.remove('is-panning');
    }
}

function startDemandGraphPan(event) {
    const viewport = event.target.closest('.demand-graph-scene-viewport');
    const scene = viewport?.querySelector('.demand-graph-scene');
    const targetNode = event.target.closest('.demand-graph-node');
    const startedOnNode = Boolean(targetNode);
    const allowNodePan = Boolean(scene?.classList.contains('is-transition-fixed'));
    if (!viewport || (startedOnNode && !allowNodePan)) return;
    if (viewport.scrollWidth <= viewport.clientWidth + 2) return;
    AppState.helpers.demandGraphPan = {
        viewport,
        pointerId: event.pointerId,
        startX: event.clientX,
        startScrollLeft: viewport.scrollLeft,
        startedOnNode,
        targetProductId: startedOnNode ? String(targetNode?.dataset.productId || '').trim() : '',
        moved: false
    };
    viewport.classList.add('is-panning');
    viewport.setPointerCapture?.(event.pointerId);
    event.preventDefault();
}

function startDemandGraphNodeDrag(event) {
    const node = event.target.closest('.demand-graph-node');
    const scene = event.target.closest('.demand-graph-scene');
    if (!node || !scene) return;
    if (node.dataset.dragDisabled === 'true') return;
    const sceneRect = scene.getBoundingClientRect();
    const nodeRect = node.getBoundingClientRect();
    const centerX = nodeRect.left + (nodeRect.width / 2);
    const centerY = nodeRect.top + (nodeRect.height / 2);
    AppState.helpers.demandGraphDrag = {
        node,
        scene,
        pointerId: event.pointerId,
        startX: event.clientX,
        startY: event.clientY,
        pointerDx: event.clientX - centerX,
        pointerDy: event.clientY - centerY,
        moved: false
    };
    node.classList.add('is-dragging');
    node.setPointerCapture?.(event.pointerId);
    event.preventDefault();
}

function handleDemandGraphNodeClick(event, id) {
    event.stopPropagation();
    if (AppState.helpers.demandGraphPanSuppressClick) {
        AppState.helpers.demandGraphPanSuppressClick = false;
        return;
    }
    if (AppState.helpers.demandGraphDrag?.suppressClick) {
        AppState.helpers.demandGraphDrag.suppressClick = false;
        return;
    }
    focusQuadrantFromDemandDriver(id);
}

function handleDemandGraphPointerMove(event) {
    const pan = AppState.helpers.demandGraphPan;
    if (pan && pan.pointerId === event.pointerId) {
        if (Math.abs(event.clientX - pan.startX) > 4) {
            pan.moved = true;
        }
        pan.viewport.scrollLeft = pan.startScrollLeft - (event.clientX - pan.startX);
        return;
    }
    const drag = AppState.helpers.demandGraphDrag;
    if (!drag || drag.pointerId !== event.pointerId) return;
    const sceneRect = drag.scene.getBoundingClientRect();
    const cardRect = drag.node.querySelector('.demand-graph-node-card')?.getBoundingClientRect();
    const halfWidth = Math.max(24, (cardRect?.width || drag.node.getBoundingClientRect().width || 48) / 2);
    const halfHeight = Math.max(24, (cardRect?.height || drag.node.getBoundingClientRect().height || 48) / 2);
    const edgeInset = 12;
    const nextX = Math.min(
        Math.max(halfWidth + edgeInset, sceneRect.width - halfWidth - edgeInset),
        Math.max(halfWidth + edgeInset, Math.min(sceneRect.width - halfWidth - edgeInset, event.clientX - sceneRect.left - drag.pointerDx))
    );
    const nextY = Math.min(
        Math.max(halfHeight + edgeInset, sceneRect.height - halfHeight - edgeInset),
        Math.max(halfHeight + edgeInset, Math.min(sceneRect.height - halfHeight - edgeInset, event.clientY - sceneRect.top - drag.pointerDy))
    );
    drag.node.style.left = `${nextX}px`;
    drag.node.style.top = `${nextY}px`;
    drag.node.style.transform = 'translate(-50%, -50%)';
    if (Math.abs(event.clientX - drag.startX) > 4 || Math.abs(event.clientY - drag.startY) > 4) {
        drag.moved = true;
        drag.suppressClick = true;
    }
    scheduleDemandGraphEdgeLayout();
}

function handleDemandGraphPointerEnd(event) {
    const pan = AppState.helpers.demandGraphPan;
    if (pan && pan.pointerId === event.pointerId) {
        pan.viewport.classList.remove('is-panning');
        pan.viewport.releasePointerCapture?.(event.pointerId);
        if (pan.startedOnNode && !pan.moved && pan.targetProductId) {
            focusQuadrantFromDemandDriver(pan.targetProductId);
        }
        AppState.helpers.demandGraphPanSuppressClick = pan.startedOnNode && pan.moved;
        AppState.helpers.demandGraphPan = null;
        return;
    }
    const drag = AppState.helpers.demandGraphDrag;
    if (!drag || drag.pointerId !== event.pointerId) return;
    drag.node.classList.remove('is-dragging');
    drag.node.releasePointerCapture?.(event.pointerId);
    AppState.helpers.demandGraphDrag = drag.moved ? { suppressClick: true } : null;
}

window.startDemandGraphPan = startDemandGraphPan;
window.startDemandGraphNodeDrag = startDemandGraphNodeDrag;
window.handleDemandGraphNodeClick = handleDemandGraphNodeClick;
window.addEventListener('pointermove', handleDemandGraphPointerMove);
window.addEventListener('pointerup', handleDemandGraphPointerEnd);
window.addEventListener('pointercancel', handleDemandGraphPointerEnd);

function renderDemandGraphNetwork(model) {
    const scene = buildDemandGraphScene(model);
    const viewClass = model.tab === 'basket' ? 'is-basket-view' : 'is-transition-view';
    const nodeButtons = scene.nodes.map((node) => {
        const positionStyle = node.positionMode === 'fixed'
            ? (node.positionEdge === 'right'
                ? `right:${svgNum(Math.max(0, (scene.canvasWidth || 0) - toNumber(node.x, 0)))}px; top:${svgNum(node.y)}px;`
                : `left:${svgNum(node.x)}px; top:${svgNum(node.y)}px;`)
            : `left:${node.x}%; top:${node.y}%;`;
        return renderDemandGraphNodeButton(node, positionStyle, model.tab);
    }).join('');
    const sceneStyle = `width:${svgNum(scene.canvasWidth || DEMAND_GRAPH_TRANSITION_FIGMA_LAYOUT.viewportWidth)}px; height:${svgNum(scene.canvasHeight || DEMAND_GRAPH_TRANSITION_FIGMA_LAYOUT.baseHeight)}px;`;
    const layoutSignature = scene.layout === 'transition-fixed'
        ? `${String(scene.layoutData?.layoutMode || 'transition-compact')}:${svgNum(scene.layoutData?.viewportWidth || 0)}:${svgNum(scene.layoutData?.leftColumnWidth || 0)}:${svgNum(scene.layoutData?.anchorCardWidth || 0)}:${svgNum(scene.layoutData?.rightColumnWidth || 0)}:${scene.layoutData?.measured ? 'm' : 'e'}`
        : 'default';
    const sceneFrameKey = `${String(model.tab || 'transition')}:${String(model.selected?.id || '')}:${String(scene.layout || 'default')}:${svgNum(scene.canvasWidth || 0)}:${svgNum(scene.canvasHeight || 0)}:${layoutSignature}`;
    const initialScrollLeft = scene.layout === 'transition-fixed'
        ? toNumber(scene.layoutData?.initialScrollLeft, 0)
        : 0;
    const transitionLayoutClass = scene.layout === 'transition-fixed'
        ? ` is-${String(scene.layoutData?.layoutMode || 'transition-compact')}`
        : '';

    return `
        <p class="demand-graph-network-helper">${escapeHtml(model.meta.guide)}</p>
        <div class="demand-graph-scene-viewport ${viewClass}" onpointerdown="startDemandGraphPan(event)">
            <div class="demand-graph-scene ${scene.layout === 'transition-fixed' ? `is-transition-fixed${transitionLayoutClass}` : 'is-basket-layout'}" data-frame-key="${escapeHtml(sceneFrameKey)}" data-initial-scroll-left="${escapeHtml(initialScrollLeft)}" data-layout-incoming-width="${escapeHtml(svgNum(scene.layoutData?.leftColumnWidth || 0))}" data-layout-anchor-width="${escapeHtml(svgNum(scene.layoutData?.anchorCardWidth || 0))}" data-layout-outgoing-width="${escapeHtml(svgNum(scene.layoutData?.rightColumnWidth || 0))}" data-layout-viewport-width="${escapeHtml(svgNum(scene.layoutData?.viewportWidth || 0))}" data-layout-mode="${escapeHtml(String(scene.layoutData?.layoutMode || ''))}" data-layout-measured="${scene.layoutData?.measured ? 'true' : 'false'}" style="${sceneStyle}">
                <svg class="demand-graph-svg" preserveAspectRatio="none" aria-hidden="true">
                    <defs>
                        <linearGradient id="demandFlowIncomingGradient" x1="0%" y1="0%" x2="100%" y2="0%">
                            <stop offset="0%" stop-color="#5b93ea" />
                            <stop offset="100%" stop-color="#8da7cf" />
                        </linearGradient>
                        <linearGradient id="demandFlowOutgoingGradient" x1="0%" y1="0%" x2="100%" y2="0%">
                            <stop offset="0%" stop-color="#8da7cf" />
                            <stop offset="100%" stop-color="#3eb7a6" />
                        </linearGradient>
                    </defs>
                    <g class="demand-graph-edge-layer"></g>
                </svg>
                <div class="demand-graph-node-layer">${nodeButtons}</div>
            </div>
        </div>
    `;
}

function renderDemandGraphInline(quadrantModel, demandGraphModel = null) {
    const selectedId = String(quadrantModel?.selected?.id || AppState.viewState.products?.quadrant?.selectedId || '').trim();
    const model = demandGraphModel || buildDemandGraphModel(selectedId);
    return renderDemandGraphInlineContent(model);
}

function renderDemandGraphHeaderCopy() {
    return `
        <div class="pgm-quadrant-controls-summary-wrap pgm-demand-graph-head-copy">
            <h3>제품 관계 구조</h3>
            <p>이 제품이 어떤 제품과 어떤 관계로 이어지는지 보여줘요.</p>
        </div>
    `;
}

function renderProductsChartViewTabs(chartView) {
    return `
        <div class="pgm-chart-tab-group" role="tablist" aria-label="차트 보기">
            <button
                class="pgm-chart-tab ${chartView === 'quadrant' ? 'is-active' : ''}"
                type="button"
                role="tab"
                aria-selected="${chartView === 'quadrant' ? 'true' : 'false'}"
                onclick="setProductsChartView('quadrant')"
            >제품 수요 포지션</button>
            <button
                class="pgm-chart-tab ${chartView === 'demand-graph' ? 'is-active' : ''}"
                type="button"
                role="tab"
                aria-selected="${chartView === 'demand-graph' ? 'true' : 'false'}"
                onclick="setProductsChartView('demand-graph')"
            >제품 관계 구조</button>
        </div>
    `;
}

function renderDemandGraphTabControls(model) {
    return `
        <div class="pgm-seg-group pgm-demand-graph-tab-toggle" role="tablist" aria-label="선택 제품 주변 흐름 탭">
            ${Object.entries(DEMAND_GRAPH_TAB_META).map(([key, meta]) => `
                <button
                    class="pgm-seg-btn ${model?.tab === key ? 'is-active' : ''}"
                    type="button"
                    role="tab"
                    aria-selected="${model?.tab === key ? 'true' : 'false'}"
                    onclick="setProductsDemandGraphTab('${key}')"
                >${meta.label}</button>
            `).join('')}
        </div>
    `;
}

function renderDemandGraphInlineContent(model) {
    return `
        <div class="demand-graph-wrap is-inline">
            ${!model.hasAnyDataset ? `
                <div class="demand-graph-empty">빈발 패턴 데이터가 준비되면 선택 제품 주변 흐름을 보여드릴게요.</div>
            ` : !model.hasDataset ? `
                <div class="demand-graph-empty">${escapeHtml(model.meta.unavailableGuide)}</div>
            ` : !model.hasData ? `
                <div class="demand-graph-empty">${escapeHtml(model.meta.emptyGuide)}</div>
            ` : `
                <div class="demand-graph-network-panel is-inline ${model.tab === 'basket' ? 'is-basket-view' : 'is-transition-view'}">
                    ${renderDemandGraphNetwork(model)}
                </div>
            `}
        </div>
    `;
}

function renderProductQuadrantHeaderCopy(chartView, quadrantControlsSummary, hasModel) {
    if (chartView === 'demand-graph') {
        return hasModel
            ? renderDemandGraphHeaderCopy()
            : `
                <div class="pgm-quadrant-controls-summary-wrap pgm-demand-graph-head-copy">
                    <h3>제품 관계 구조</h3>
                    <p>선택한 제품이 없어서 관계 구조를 아직 보여줄 수 없어요.</p>
                </div>
            `;
    }
    if (hasModel) {
        return `
            <div class="pgm-quadrant-controls-summary-wrap pgm-demand-graph-head-copy">
                <h3>제품 수요 포지션</h3>
                <p>${escapeHtml(quadrantControlsSummary)} 각 제품이 어떤 역할을 하는지 확인해보세요.</p>
            </div>
        `;
    }
    return `
        <div class="pgm-quadrant-controls-summary-wrap pgm-demand-graph-head-copy">
            <h3>제품 수요 포지션</h3>
            <p>표시할 제품이 없어서 수요 포지션을 아직 보여줄 수 없어요.</p>
        </div>
    `;
}

function renderProductChartContextControls(model, chartView, demandGraphModel, representativeAvailable, convergenceAvailable) {
    if (!model) return '';
    if (chartView === 'demand-graph') {
        return renderDemandGraphTabControls(demandGraphModel);
    }
    return `
        <div class="pgm-seg-group pgm-quadrant-scale-toggle" role="tablist" aria-label="사분면 보기 범위">
            <button
                class="pgm-seg-btn ${model.scaleMode === 'raw' ? 'is-active' : ''}"
                type="button"
                role="tab"
                aria-selected="${model.scaleMode === 'raw' ? 'true' : 'false'}"
                title="전체 제품 범위를 기준으로 보여줘요."
                onclick="setQuadrantScaleMode('raw')"
            >전체뷰</button>
            <button
                class="pgm-seg-btn ${model.scaleMode !== 'raw' ? 'is-active' : ''}"
                type="button"
                role="tab"
                aria-selected="${model.scaleMode !== 'raw' ? 'true' : 'false'}"
                title="선택 제품과 주요 연결 범위를 중심으로 확대해 보여줘요."
                onclick="setQuadrantScaleMode('focus')"
            >집중뷰</button>
        </div>
        <div class="pgm-seg-group pgm-quadrant-edge-toggle" role="tablist" aria-label="사분면 연결 모드">
            <button
                class="pgm-seg-btn ${model.edgeMode === 'representative' ? 'is-active' : ''}"
                type="button"
                role="tab"
                aria-selected="${model.edgeMode === 'representative' ? 'true' : 'false'}"
                title="${escapeHtml(representativeAvailable ? QUADRANT_EDGE_MODE_META.representative.guide : QUADRANT_EDGE_MODE_META.representative.unavailableGuide)}"
                ${representativeAvailable ? '' : 'disabled'}
                onclick="setQuadrantEdgeMode('representative')"
            >${escapeHtml(QUADRANT_EDGE_MODE_META.representative.label)}</button>
            <button
                class="pgm-seg-btn ${model.edgeMode === 'convergence' ? 'is-active' : ''}"
                type="button"
                role="tab"
                aria-selected="${model.edgeMode === 'convergence' ? 'true' : 'false'}"
                title="${escapeHtml(convergenceAvailable ? QUADRANT_EDGE_MODE_META.convergence.guide : QUADRANT_EDGE_MODE_META.convergence.unavailableGuide)}"
                ${convergenceAvailable ? '' : 'disabled'}
                onclick="setQuadrantEdgeMode('convergence')"
            >${escapeHtml(QUADRANT_EDGE_MODE_META.convergence.label)}</button>
        </div>
    `;
}

function buildVisibleQuadrantEdges(points, selectedId, edgeMode = 'representative') {
    const selected = String(selectedId || '').trim();
    const normalizedEdgeMode = normalizeQuadrantEdgeMode(edgeMode);
    const config = QUADRANT_EDGE_MODE_META[normalizedEdgeMode];
    if (!selected || !Array.isArray(points) || !points.length) {
        return {
            mode: normalizedEdgeMode,
            label: config.label,
            guide: config.emptyGuide,
            edges: [],
            isAvailable: false,
            availability: getQuadrantEdgeModeAvailability()
        };
    }

    const pointIdSet = new Set(points.map((point) => String(point.id || '').trim()).filter(Boolean));
    if (!pointIdSet.has(selected)) {
        return {
            mode: normalizedEdgeMode,
            label: config.label,
            guide: config.emptyGuide,
            edges: [],
            isAvailable: false,
            availability: getQuadrantEdgeModeAvailability()
        };
    }

    const availability = getQuadrantEdgeModeAvailability();
    const isAvailable = Boolean(availability[normalizedEdgeMode]);
    if (!isAvailable) {
        return {
            mode: normalizedEdgeMode,
            label: config.label,
            guide: config.unavailableGuide,
            edges: [],
            isAvailable,
            availability
        };
    }

    let edges = [];
    if (normalizedEdgeMode === 'convergence') {
        edges = buildConvergenceQuadrantEdges(pointIdSet, selected);
    } else {
        edges = buildRepresentativeQuadrantEdges(pointIdSet, selected);
    }

    return {
        mode: normalizedEdgeMode,
        label: config.label,
        guide: edges.length ? config.guide : config.emptyGuide,
        edges,
        isAvailable,
        availability
    };
}

function buildQuadrantModel(rows, selectedId, scaleMode = 'focus', scope = 'retention-emphasis', edgeMode = 'representative') {
    const transitionEntitySet = buildTransitionEntitySet();
    const demandGravityMap = new Map(
        (AppState.data.productDemandGravity || [])
            .map((row) => [String(row.product_id || '').trim(), row])
            .filter(([id]) => id)
    );
    const normalizedScope = String(scope || '').toLowerCase() === 'all' ? 'all' : 'retention-emphasis';
    const normalizedEdgeMode = normalizeQuadrantEdgeMode(edgeMode);
    const allPoints = (rows || [])
        .map((row) => {
            const id = String(row.product_id || '').trim();
            const entry = toNumber(firstDefinedValue(row.AA_Score, row.Entry_Gravity_Score), NaN);
            const expansion = toNumber(firstDefinedValue(row.PCA_Score, row.Expansion_Gravity_Score), NaN);
            if (!id || !Number.isFinite(entry) || !Number.isFinite(expansion)) return null;
            const weeklyForecast = Math.max(0, toNumber(row.product_order_cnt_1y, 0) / 52);
            const entityMeta = getEntityMeta(id);
            const memberCount = Math.max(1, toNumber(row.member_count, 1), toNumber(entityMeta.memberCount, 1));
            const gravityRow = demandGravityMap.get(id) || {};
            return {
                id,
                name: getProductName(id),
                entry,
                expansion,
                returnScore: toNumber(gravityRow.Return_Gravity_Score, 0),
                convergenceScore: toNumber(gravityRow.Convergence_Gravity_Score, 0),
                weeklyForecast,
                firstCustomerCnt: toNumber(row.first_customer_cnt, 0),
                repurchaseCustomerCnt90d: toNumber(row.repurchase_customer_cnt_90d, 0),
                repurchaseRate90d: toNumber(row.first_customer_cnt, 0) > 0
                    ? toNumber(row.repurchase_customer_cnt_90d, 0) / toNumber(row.first_customer_cnt, 0)
                    : 0,
                revenue90d: toNumber(row.revenue_90d, 0),
                memberCount,
                groupEntityId: entityMeta.entityId || id,
                hasTransition: transitionEntitySet.has(id)
            };
        })
        .filter(Boolean)
        .sort((a, b) => b.revenue90d - a.revenue90d);

    const points = allPoints;

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
    const totalFirstCustomerCnt = points.reduce((sum, point) => sum + toNumber(point.firstCustomerCnt, 0), 0);
    const totalRepurchaseCustomerCnt90d = points.reduce((sum, point) => sum + toNumber(point.repurchaseCustomerCnt90d, 0), 0);
    const pointsWithDemandShare = points.map((point) => ({
        ...point,
        entryDemandShare: totalFirstCustomerCnt > 0 ? toNumber(point.firstCustomerCnt, 0) / totalFirstCustomerCnt : 0,
        expansionDemandShare: totalRepurchaseCustomerCnt90d > 0 ? toNumber(point.repurchaseCustomerCnt90d, 0) / totalRepurchaseCustomerCnt90d : 0
    }));

    let activeId = selectedId && pointsWithDemandShare.some((p) => p.id === selectedId) ? selectedId : '';
    if (!activeId && AppState.helpers.focusEntityId && pointsWithDemandShare.some((p) => p.id === AppState.helpers.focusEntityId)) {
        activeId = AppState.helpers.focusEntityId;
    }
    if (!activeId) activeId = pointsWithDemandShare[0].id;
    const selected = pointsWithDemandShare.find((p) => p.id === activeId) || pointsWithDemandShare[0];
    const status = getQuadrantStatus(selected.entry, selected.expansion, centerEntry, centerExpansion);
    const shouldHideEdgesForRetentionMode = normalizedScope === 'retention-emphasis' && !selected.hasTransition;
    const edgeDisplay = shouldHideEdgesForRetentionMode
        ? {
            mode: normalizedEdgeMode,
            label: QUADRANT_EDGE_MODE_META[normalizedEdgeMode].label,
            guide: '이 제품은 90일 내 전이 데이터가 없어 흐름이 표시되지 않아요.',
            edges: [],
            isAvailable: true,
            availability: getQuadrantEdgeModeAvailability()
        }
        : buildVisibleQuadrantEdges(pointsWithDemandShare, selected.id, normalizedEdgeMode);
    const scale = buildQuadrantScaleModel(points, selected, scaleMode, edgeDisplay.edges);

    return {
        points: pointsWithDemandShare,
        selected,
        visibleEdges: edgeDisplay.edges,
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
        edgeMode: edgeDisplay.mode,
        edgeModeLabel: edgeDisplay.label,
        edgeGuide: edgeDisplay.guide,
        edgeAvailability: edgeDisplay.availability,
        edgeModeAvailable: edgeDisplay.isAvailable,
        scaleRange: scale.activeRange,
        focusRange: scale.focusRange,
        rawRange: scale.rawRange
    };
}

function getRelativeLevelFromRows(value, rows, field) {
    const values = (rows || []).map((row) => toNumber(row?.[field], NaN)).filter((v) => Number.isFinite(v));
    if (!values.length) return '보통';
    return getLevelText(value, percentile(values, 0.33), percentile(values, 0.66));
}

function buildQuadrantStrategyModel(model) {
    if (!model?.selected) return null;
    const selected = model.selected;
    const selectedId = String(selected.id || '').trim();
    const scopedPoints = model.points || [];
    const pointIdSet = new Set(scopedPoints.map((point) => String(point.id || '').trim()).filter(Boolean));
    const scopedCaRows = (AppState.data.caProfile || []).filter((row) => pointIdSet.has(String(row.product_id || '').trim()));
    const selectedCa = scopedCaRows.find((row) => String(row.product_id || '').trim() === selectedId) || null;
    const cartRowsForLevel = selectedCa ? scopedCaRows : [];

    const levels = {
        weeklyForecast: getRelativeLevelFromRows(selected.weeklyForecast, scopedPoints, 'weeklyForecast'),
        repurchaseRate90d: getRelativeLevelFromRows(selected.repurchaseRate90d, scopedPoints, 'repurchaseRate90d'),
        entryDemandShare: getRelativeLevelFromRows(selected.entryDemandShare, scopedPoints, 'entryDemandShare'),
        expansionDemandShare: getRelativeLevelFromRows(selected.expansionDemandShare, scopedPoints, 'expansionDemandShare'),
        returnRole: getRelativeLevelFromRows(selected.returnScore, scopedPoints, 'returnScore'),
        convergenceRole: getRelativeLevelFromRows(selected.convergenceScore, scopedPoints, 'convergenceScore'),
        attachRate: selectedCa ? getRelativeLevelFromRows(selectedCa.attach_rate, cartRowsForLevel, 'attach_rate') : '-',
        breadthLift: selectedCa ? getRelativeLevelFromRows(selectedCa.breadth_lift, cartRowsForLevel, 'breadth_lift') : '-',
        top1Share: selectedCa ? getRelativeLevelFromRows(selectedCa.top1_share, cartRowsForLevel, 'top1_share') : '-'
    };

    const statusKey = String(model.status?.key || '');
    const highDemand = levels.weeklyForecast === '높음';
    const mediumDemand = levels.weeklyForecast === '보통';
    const lowDemand = levels.weeklyForecast === '낮음';
    const highContinuity = levels.repurchaseRate90d === '높음';
    const lowContinuity = levels.repurchaseRate90d === '낮음';
    const highEntryContribution = levels.entryDemandShare === '높음';
    const highExpansionContribution = levels.expansionDemandShare === '높음';
    const highReturnRole = levels.returnRole === '높음';
    const highConvergenceRole = levels.convergenceRole === '높음';
    const lowContribution = levels.entryDemandShare === '낮음' && levels.expansionDemandShare === '낮음';
    const hasCartSignal = Boolean(selectedCa);
    const highCartExpansion = hasCartSignal && (levels.attachRate === '높음' || levels.breadthLift === '높음');
    const highCartConcentration = hasCartSignal && levels.top1Share === '높음';
    const cartType = String(selectedCa?.ca_type || '').toLowerCase();

    let goalText = '현재 강점을 유지하면서 다음 성장 포인트를 확인해요.';
    if (highDemand && (highEntryContribution || highExpansionContribution) && lowContinuity) {
        goalText = '유입된 수요가 다음 구매로 이어지도록 유지 흐름을 보강해요.';
    } else if (highReturnRole) {
        goalText = '다시 돌아오는 구매 흐름이 끊기지 않도록 유지 반응을 지켜요.';
    } else if (highConvergenceRole) {
        goalText = '여러 흐름이 모이는 대표 제품 역할을 더 분명하게 만들어요.';
    } else if (highContinuity && !highEntryContribution) {
        goalText = '안정적인 재구매 반응을 바탕으로 신규 유입 모수를 넓혀요.';
    } else if (highCartExpansion && (mediumDemand || highDemand)) {
        goalText = '장바구니 연계 노출로 보조 수요를 더 크게 만들어봐요.';
    } else if (highEntryContribution && !highExpansionContribution) {
        goalText = '첫 구매 이후 다음 구매 전환을 높이는 구조를 만들어요.';
    } else if (highExpansionContribution && !highEntryContribution) {
        goalText = '재구매 강점을 유지하면서 신규 유입을 보강해요.';
    } else if (lowDemand && lowContribution) {
        goalText = '작게 검증하면서 반응 회복 가능성을 확인해요.';
    }

    let actionText = '지금 반응이 좋은 지점은 유지하고 약한 신호 한 가지를 정해 보강 여부를 확인해봐요.';
    if (highDemand && (highEntryContribution || highExpansionContribution) && lowContinuity) {
        actionText = '상세·CRM·재구매 혜택을 묶어 첫 구매 후 3~7일 전환 장치를 우선 보강해봐요.';
    } else if (highReturnRole) {
        actionText = '재구매 리마인드 CRM과 루틴 구매 혜택을 묶어 다시 찾는 흐름을 안정적으로 유지해봐요.';
    } else if (highConvergenceRole) {
        actionText = '대표 상세와 세트 연결을 정리해 여러 흐름의 도착점 역할이 더 잘 보이게 만들어봐요.';
    } else if (highContinuity && !highEntryContribution) {
        actionText = '구매 지속 반응이 좋은 만큼 대표 진입 지면과 신규 유입 캠페인에서 노출 확대를 검토해봐요.';
    } else if (highCartExpansion) {
        if (highCartConcentration) {
            actionText = '상위 조합은 유지하되 한 조합 쏠림을 점검하면서 함께 구매·세트 노출을 넓혀봐요.';
        } else if (cartType === 'set' || levels.breadthLift === '높음') {
            actionText = '세트 제안과 함께 구매 영역을 보강해 장바구니 확장을 검토해봐요.';
        } else {
            actionText = '상세 교차노출과 장바구니 추천 영역을 보강해 연관 구매 연결을 키워봐요.';
        }
    } else if (lowDemand && lowContribution) {
        actionText = '가격·구성·메시지를 작게 바꿔보며 반응 개선 여부를 먼저 확인해봐요.';
    } else if (highEntryContribution && !highExpansionContribution) {
        actionText = '첫 구매 직후 번들 제안과 리마인드 CRM을 붙여 다음 구매 연결을 강화해봐요.';
    } else if (highExpansionContribution && !highEntryContribution) {
        actionText = '재구매 강점은 유지하면서 신규 유입 채널과 대표 소재 확장을 함께 검토해봐요.';
    } else if (highDemand) {
        actionText = '핵심 지면 노출과 재고 운영을 우선 점검해 현재 수요 흐름이 꺾이지 않게 관리해봐요.';
    }

    return {
        goalText,
        actionText,
        reasonTags: levels,
        selectedCa
    };
}

function getRoleLevelLabel(level) {
    if (level === '높음') return '강함';
    if (level === '낮음') return '약함';
    return '보통';
}

function getAdditionalRoleGuide(key, level) {
    if (key === 'returnRole') {
        if (level === '높음') return '다른 구매를 거친 뒤 다시 돌아오는 흐름이 잘 보여요.';
        if (level === '낮음') return '다시 찾는 구매 흐름은 아직 약하게 보여요.';
        return '다시 찾는 구매 흐름이 일부 보여요.';
    }
    if (level === '높음') return '다른 제품 다음에 이 제품이 자주 선택되는 흐름이 뚜렷해요.';
    if (level === '낮음') return '다음 구매로 이 제품이 이어지는 흐름은 아직 약해요.';
    return '다음 구매로 이 제품이 이어지는 흐름이 일부 보여요.';
}

// Status tag style map (Figma)
const QUADRANT_STATUS_TAG_STYLE = {
    hero:             { bg: '#ddedff', border: '#a8c8f5', alertBorder: '#93c5fd', text: '#1e85fb' },
    'entry-only':     { bg: '#e9f8e0', border: '#8dcb57', alertBorder: '#bfe4a2', text: '#6bba25' },
    'expansion-only': { bg: '#efe4ff', border: '#c4a8f5', alertBorder: '#d8b4fe', text: '#a765fb' },
    phaseout:         { bg: '#ffeded', border: '#f5a8a8', alertBorder: '#fca5a5', text: '#f45151' }
};

function renderQuadrantPanel(model) {
    if (!model) {
        return '<p class="empty-state">4분면 계산 대상 제품이 없습니다.</p>';
    }
    const { selected, status } = model;
    const strategy = buildQuadrantStrategyModel(model);
    const memberMeta = selected.memberCount > 1 ? `그룹 제품 (${selected.memberCount}개 SKU)` : '단일 제품';
    const hasHistory = (AppState.viewState.products.quadrant.history || []).length > 0;
    const tagStyle = QUADRANT_STATUS_TAG_STYLE[status.key] || QUADRANT_STATUS_TAG_STYLE.hero;
    const transitionCta = selected.hasTransition
        ? `<button class="btn-primary pgm-side-cta-btn" type="button" onclick="openRetentionFlowModal('${escapeJs(selected.id)}')">추가구매 제품 보기</button>`
        : `
            <button class="btn-primary pgm-side-cta-btn" type="button" disabled title="90일 추가구매 데이터가 없어 이동할 수 없음">추가구매 제품 보기</button>
            <p class="pgm-link-help">구매 후 90일 내 추가구매 데이터가 없어 이동할 수 없어요.</p>
        `;

    return `
        <div class="pgm-side-summary">

            <!-- 1. 제품 헤더: 태그 + 이름 -->
            <div class="pgm-side-hero">
                <span class="pgm-badge" style="background:${tagStyle.bg}; color:${tagStyle.text}; border-color:${tagStyle.border};">${status.label}</span>
                <div class="pgm-side-hero-body">
                    <div class="pgm-side-hero-thumb">
                        <img src="${QUADRANT_THUMBNAIL_SRC}" alt="" class="pgm-side-hero-thumb-image" loading="lazy" decoding="async">
                    </div>
                    <div class="pgm-side-hero-copy">
                        <div class="pgm-selected-head">
                            <h3 title="${escapeHtml(selected.name)}">${escapeHtml(selected.name)}</h3>
                        </div>
                        <p class="pgm-panel-helper">${escapeHtml(getQuadrantStatusCompactCopy(status))}</p>
                    </div>
                </div>
            </div>

            <!-- 2. 수요 인사이트 -->
            <section class="pgm-side-section-card pgm-side-section-card--insight">
                <h4>수요 인사이트</h4>
                <div class="pgm-metrics pgm-insight-metrics">
                    <div>
                        <label>주간 예상 수요량</label>
                        <strong>${formatNumber(selected.weeklyForecast, 1)}</strong>
                        <span>${memberMeta}</span>
                    </div>
                    <div>
                        <label>구매 지속 가능성</label>
                        <strong>${formatPercent(selected.repurchaseRate90d, 1)}</strong>
                        <span>구매 후 90일 기준</span>
                    </div>
                </div>
                <div class="pgm-insight-actions">
                    ${transitionCta}
                </div>
            </section>

            <!-- 3. 구매 기여 비중 -->
            <section class="pgm-side-section-card">
                <h4>구매 기여 비중</h4>
                <div class="pgm-demand-share-grid">
                    <div class="pgm-demand-share-card">
                        <label>첫구매 기여 비중</label>
                        <strong>${formatPercent(selected.entryDemandShare, 1)}</strong>
                    </div>
                    <div class="pgm-demand-share-card">
                        <label>재구매 기여 비중</label>
                        <strong>${formatPercent(selected.expansionDemandShare, 1)}</strong>
                    </div>
                </div>
            </section>

            <!-- 4. 운영 전략 -->
            <section class="pgm-side-section-card">
                <h4>운영 전략</h4>
                <div class="pgm-action-list">
                    <div class="pgm-action-row">
                        <label>전략 목표</label>
                        <p>${escapeHtml(strategy?.goalText || '-')}</p>
                    </div>
                    <div class="pgm-action-row">
                        <label>실행 방법</label>
                        <p>${escapeHtml(strategy?.actionText || '-')}</p>
                    </div>
                </div>
            </section>

            <!-- 5. 장바구니 확장 힌트 -->
            <section class="pgm-side-section-card">
                <div class="pgm-side-section-head">
                    <h4>장바구니 확장 힌트</h4>
                    <p class="pgm-basket-helper">이 제품이 포함될 때 함께 담기는 제품 구조를 보여줘요.</p>
                </div>
                <button class="btn-primary pgm-side-cta-btn" type="button" onclick="openCartFlowModal('${escapeJs(selected.id)}')">함께 구매되는 제품 보기</button>
            </section>
            <button class="btn-primary pgm-prev-btn" type="button"
                onclick="selectPreviousQuadrantItem()"
                ${hasHistory ? '' : 'disabled'}
            >이전 제품으로</button>

        </div>
    `;
}

function renderProductQuadrant(model, coreDemandModel = null) {
    const chartView = AppState.viewState.products.chartView === 'demand-graph' ? 'demand-graph' : 'quadrant';
    const isCompactSidePanel = isCompactProductsSidePanelViewport();
    const isSidePanelOpen = getProductsSidePanelOpenState(isCompactSidePanel);
    const emptyChartMessage = '표시할 제품이 없습니다.';
    const coreCount = Math.max(0, toNumber(coreDemandModel?.rows?.length, 0));
    const quadrantControlsSummary = coreCount
        ? `${formatNumber(coreCount, 0)}개 제품이 핵심 수요를 만들고 있어요.`
        : '핵심 수요를 만드는 제품을 보여줘요.';
    const representativeAvailable = Boolean(model?.edgeAvailability?.representative);
    const convergenceAvailable = Boolean(model?.edgeAvailability?.convergence);
    const demandGraphModel = chartView === 'demand-graph' && model
        ? buildDemandGraphModel(String(model?.selected?.id || '').trim())
        : null;
    return `
        <div class="pgm-quadrant-wrap animate-fade-in">
            <div class="pgm-quadrant-head ${chartView === 'demand-graph' ? 'is-demand-graph-view' : 'is-quadrant-view'}">
                ${renderProductQuadrantHeaderCopy(chartView, quadrantControlsSummary, Boolean(model))}
                <div class="pgm-chart-view-switch">
                    ${renderProductsChartViewTabs(chartView)}
                </div>
            </div>
            <div class="pgm-quadrant-body ${isSidePanelOpen ? '' : 'is-side-panel-closed'}">
                <div class="pgm-chart chart-card ${chartView === 'demand-graph' ? 'is-demand-graph-view' : 'is-quadrant-view'}">
                    <div class="pgm-chart-frame ${chartView === 'demand-graph' ? 'is-demand-graph-view' : 'is-quadrant-view'}">
                        <div class="pgm-chart-header-row">
                            <div class="pgm-quadrant-controls-actions is-chart-header ${model ? '' : 'is-empty'}">
                                ${renderProductChartContextControls(model, chartView, demandGraphModel, representativeAvailable, convergenceAvailable)}
                            </div>
                            <div class="pgm-chart-header-actions">
                                ${model ? `
                                    <button
                                        class="pgm-panel-toggle-btn ${isSidePanelOpen ? 'is-active' : ''}"
                                        type="button"
                                        aria-label="${isSidePanelOpen ? '인사이트 패널 닫기' : '인사이트 패널 열기'}"
                                        title="${isSidePanelOpen ? '인사이트 패널 닫기' : '인사이트 패널 열기'}"
                                        onclick="toggleProductsSidePanel()"
                                    >
                                        <img src="../../assets/BiDockRight.svg" alt="" class="pgm-panel-toggle-icon" aria-hidden="true">
                                    </button>
                                ` : ''}
                            </div>
                        </div>
                        <div class="pgm-chart-stage ${chartView === 'demand-graph' ? 'is-demand-graph-view' : 'is-quadrant-view'}">
                            ${chartView === 'quadrant'
            ? (model
                ? renderQuadrantStageFrame(model)
                : `<div class="quadrant-chart-empty"><p>${emptyChartMessage}</p></div>`)
            : renderDemandGraphInline(model, demandGraphModel)}
                        </div>
                    </div>
                </div>
                ${model ? `
                    ${isCompactSidePanel && isSidePanelOpen ? '<button class="pgm-side-backdrop" type="button" aria-label="인사이트 패널 닫기" onclick="closeProductsSidePanel()"></button>' : ''}
                    <div class="pgm-side card ${isCompactSidePanel ? 'is-compact-drawer' : ''} ${isSidePanelOpen ? 'is-open' : ''}">
                        <button
                            class="pgm-side-close-btn"
                            type="button"
                            aria-label="인사이트 패널 닫기"
                            title="인사이트 패널 닫기"
                            onclick="closeProductsSidePanel()"
                        ><i class="ph ph-x"></i></button>
                        ${renderQuadrantPanel(model)}
                    </div>
                ` : ''}
                <div id="products-summary-card" class="animate-fade-in"></div>
            </div>
        </div>
    `;
}

function svgNum(value) {
    return Number.isFinite(value) ? value.toFixed(2) : '0';
}

function getQuadrantVisualConfig(frame = QUADRANT_SVG_FRAME) {
    const widthScale = Math.max(0.72, toNumber(frame?.width, QUADRANT_SVG_FRAME.width) / QUADRANT_SVG_FRAME.width);
    const heightScale = Math.max(0.72, toNumber(frame?.height, QUADRANT_SVG_FRAME.height) / QUADRANT_SVG_FRAME.height);
    const bubbleScale = Math.min(1.72, Math.max(0.94, Math.sqrt(widthScale * heightScale)));
    return {
        baseRadius: 7 * bubbleScale,
        radiusSpread: 24 * bubbleScale,
        maxRadius: 42,
        selectedScale: 1.08,
        selectedRingGap: 5,
        selectedRingWidth: 3,
        pulseExtra: 8,
        pulseWidth: 2,
        edgeStartPad: 3,
        edgeEndPad: 5,
        edgeArrowPad: 12,
        edgeArrowLenBase: 6,
        edgeArrowLenSpread: 2.8,
        edgeArrowWidthBase: 3.2,
        edgeArrowWidthSpread: 1.4
    };
}

function getQuadrantBubbleRadius(point, maxWeekly, visualConfig) {
    const weekly = Math.max(0, toNumber(point?.weeklyForecast, 0));
    const maxValue = Math.max(1, toNumber(maxWeekly, 1));
    const baseRadius = visualConfig.baseRadius + visualConfig.radiusSpread * Math.sqrt(weekly / maxValue);
    const scaledRadius = point?.isSelected ? baseRadius * visualConfig.selectedScale : baseRadius;
    return Math.min(visualConfig.maxRadius, Math.max(visualConfig.baseRadius, scaledRadius));
}

function getQuadrantBubbleOuterRadius(point, maxWeekly, visualConfig) {
    const radius = getQuadrantBubbleRadius(point, maxWeekly, visualConfig);
    if (!point?.isSelected) return radius + 2;
    return radius + visualConfig.selectedRingGap + visualConfig.selectedRingWidth + visualConfig.pulseExtra + visualConfig.pulseWidth;
}

function buildQuadrantPixelInsets(model, frame = QUADRANT_SVG_FRAME, visualConfig = getQuadrantVisualConfig(frame), rangeOverride = null) {
    const range = getQuadrantRenderRange(rangeOverride || model?.scaleRange || {});
    const points = Array.isArray(model?.points) ? model.points : [];
    const selectedId = String(model?.selected?.id || '').trim();
    const spanX = Math.max(0.02, range.xMax - range.xMin);
    const spanY = Math.max(0.02, range.yMax - range.yMin);

    let left = 0;
    let right = 0;
    let top = 0;
    let bottom = 0;

    for (let i = 0; i < 4; i += 1) {
        const usableWidth = Math.max(80, frame.width - left - right);
        const usableHeight = Math.max(80, frame.height - top - bottom);
        const mapX = (value) => left + (((value - range.xMin) / spanX) * usableWidth);
        const mapY = (value) => top + ((1 - ((value - range.yMin) / spanY)) * usableHeight);

        let nextLeft = 0;
        let nextRight = 0;
        let nextTop = 0;
        let nextBottom = 0;

        points.forEach((point) => {
            const isSelected = String(point.id || '').trim() === selectedId;
            const projected = model?.scaleMode === 'focus'
                ? projectOutlierPoint(point, range)
                : { x: point.entry, y: point.expansion, marker: '' };
            const x = mapX(projected.x);
            const y = mapY(projected.y);
            const outerRadius = getQuadrantBubbleOuterRadius({ ...point, isSelected }, model?.maxWeekly, visualConfig);
            const topExtra = projected.marker && projected.marker.includes('↑') ? 14 : 0;
            const bottomExtra = projected.marker && projected.marker.includes('↓') ? 10 : 0;
            nextLeft = Math.max(nextLeft, outerRadius + 4 - x);
            nextRight = Math.max(nextRight, outerRadius + 4 - (frame.width - x));
            nextTop = Math.max(nextTop, outerRadius + 4 + topExtra - y);
            nextBottom = Math.max(nextBottom, outerRadius + 4 + bottomExtra - (frame.height - y));
        });

        left = Math.max(0, nextLeft);
        right = Math.max(0, nextRight);
        top = Math.max(0, nextTop);
        bottom = Math.max(0, nextBottom);
    }

    return { left, right, top, bottom, range };
}

function buildQuadrantSummaryBuckets(model) {
    const labels = {
        'expansion-only': '재구매 강점 제품',
        hero: '우선 확대 대상',
        phaseout: '개선 필요',
        'entry-only': '첫구매 강점 제품'
    };
    const totals = (model?.points || []).reduce((acc, point) => {
        const status = getQuadrantStatus(point.entry, point.expansion, model.centerEntry, model.centerExpansion);
        const key = String(status?.key || '');
        const revenue = Math.max(0, toNumber(point.revenue90d, 0));
        if (!acc[key]) acc[key] = { count: 0, revenue: 0 };
        acc[key].count += 1;
        acc[key].revenue += revenue;
        acc.totalRevenue += revenue;
        return acc;
    }, { totalRevenue: 0 });

    const formatSummary = (key) => {
        const bucket = totals[key] || { count: 0, revenue: 0 };
        const share = totals.totalRevenue > 0 ? (bucket.revenue / totals.totalRevenue) * 100 : 0;
        return {
            label: labels[key],
            summary: `총 ${formatNumber(bucket.count, 0)}개 제품 / 매출 비중 ${formatNumber(share, 1)}%`
        };
    };

    return {
        axisDescription: 'X축: 구매전환율  |  Y축: 매출기여도',
        legend: [
            { key: 'expansion-only', label: labels['expansion-only'] },
            { key: 'hero', label: labels.hero },
            { key: 'phaseout', label: labels.phaseout },
            { key: 'entry-only', label: labels['entry-only'] }
        ],
        corners: {
            topLeft: formatSummary('expansion-only'),
            topRight: formatSummary('hero'),
            bottomLeft: formatSummary('phaseout'),
            bottomRight: formatSummary('entry-only')
        }
    };
}

function buildQuadrantSvgModel(model, frame = QUADRANT_SVG_FRAME, rangeOverride = null) {
    const visualConfig = getQuadrantVisualConfig(frame);
    const pixelInsets = buildQuadrantPixelInsets(model, frame, visualConfig, rangeOverride);
    const summary = buildQuadrantSummaryBuckets(model);
    const baseRange = pixelInsets.range;
    const spanX = Math.max(0.02, baseRange.xMax - baseRange.xMin);
    const spanY = Math.max(0.02, baseRange.yMax - baseRange.yMin);
    const selectedId = String(model?.selected?.id || '').trim();
    const mutedFillPalette = {
        hero: 'rgba(147, 197, 253, 0.34)',
        'entry-only': 'rgba(153, 246, 228, 0.30)',
        'expansion-only': 'rgba(216, 180, 254, 0.32)',
        phaseout: 'rgba(253, 186, 186, 0.30)'
    };
    const mutedBorderPalette = {
        hero: 'rgba(96, 165, 250, 0.54)',
        'entry-only': 'rgba(45, 212, 191, 0.50)',
        'expansion-only': 'rgba(196, 181, 253, 0.56)',
        phaseout: 'rgba(252, 165, 165, 0.50)'
    };

    const usableWidth = Math.max(80, frame.width - pixelInsets.left - pixelInsets.right);
    const usableHeight = Math.max(80, frame.height - pixelInsets.top - pixelInsets.bottom);
    const mapX = (value) => pixelInsets.left + (((value - baseRange.xMin) / spanX) * usableWidth);
    const mapY = (value) => pixelInsets.top + ((1 - ((value - baseRange.yMin) / spanY)) * usableHeight);
    const centerPx = {
        x: mapX(toNumber(model?.centerEntry, 0)),
        y: mapY(toNumber(model?.centerExpansion, 0))
    };

    const points = (model?.points || []).map((point) => {
        const isSelected = String(point.id || '').trim() === selectedId;
        const status = getQuadrantStatus(point.entry, point.expansion, model.centerEntry, model.centerExpansion);
        const projected = model?.scaleMode === 'focus'
            ? projectOutlierPoint(point, baseRange)
            : { x: point.entry, y: point.expansion, marker: '' };
        const radius = getQuadrantBubbleRadius({ ...point, isSelected }, model?.maxWeekly, visualConfig);
        const outerRadius = getQuadrantBubbleOuterRadius({ ...point, isSelected }, model?.maxWeekly, visualConfig);
        const muted = model?.scope === 'retention-emphasis' && !point.hasTransition;
        const baseFocusColor = muted
            ? (mutedBorderPalette[status.key] || 'rgba(148, 163, 184, 0.42)')
            : status.color;
        return {
            ...point,
            isSelected,
            status,
            outlierMarker: projected.marker,
            x: mapX(projected.x),
            y: mapY(projected.y),
            radius,
            outerRadius,
            fill: muted ? (mutedFillPalette[status.key] || 'rgba(148, 163, 184, 0.24)') : status.color,
            fillOpacity: muted || isSelected ? 1 : 0.86,
            stroke: muted ? (mutedBorderPalette[status.key] || 'rgba(148, 163, 184, 0.42)') : '#ffffff',
            strokeWidth: isSelected ? 2.6 : (muted ? 1 : 1.6),
            focusRingColor: withAlpha(baseFocusColor, 0.28),
            focusPulseColor: withAlpha(baseFocusColor, 0.22)
        };
    }).sort((a, b) => {
        if (a.isSelected && !b.isSelected) return 1;
        if (!a.isSelected && b.isSelected) return -1;
        return a.radius - b.radius;
    });

    const pointById = new Map(points.map((point) => [String(point.id || '').trim(), point]));
    const maxCustomers = Math.max(...(model?.visibleEdges || []).map((edge) => toNumber(edge.transitionCustomers, 0)), 1);
    const edges = (model?.visibleEdges || []).map((edge) => {
        const from = pointById.get(String(edge.from || '').trim());
        const to = pointById.get(String(edge.to || '').trim());
        if (!from || !to) return null;
        const dx = to.x - from.x;
        const dy = to.y - from.y;
        const dist = Math.hypot(dx, dy);
        if (!Number.isFinite(dist) || dist < 6) return null;

        const ux = dx / dist;
        const uy = dy / dist;
        const startX = from.x + ux * (from.radius + visualConfig.edgeStartPad);
        const startY = from.y + uy * (from.radius + visualConfig.edgeStartPad);
        const endX = to.x - ux * (to.radius + visualConfig.edgeEndPad);
        const endY = to.y - uy * (to.radius + visualConfig.edgeEndPad);
        const isInbound = edge.direction === 'inbound' || edge.direction === 'convergence' || edge.direction === 'loop-return';
        const bend = Math.min(38, Math.max(12, dist * 0.18)) * (isInbound ? -1 : 1);
        const controlX = (startX + endX) / 2 + (-uy * bend);
        const controlY = (startY + endY) / 2 + (ux * bend);
        const weight = toNumber(edge.transitionCustomers, 0) / maxCustomers;
        const color = isInbound ? [78, 99, 132] : [55, 103, 110];
        const alpha = 0.14 + (weight * 0.24);
        const arrowLen = visualConfig.edgeArrowLenBase + (weight * visualConfig.edgeArrowLenSpread);
        const arrowWidth = visualConfig.edgeArrowWidthBase + (weight * visualConfig.edgeArrowWidthSpread);
        const tangentX = endX - controlX;
        const tangentY = endY - controlY;
        const tangentLen = Math.max(Math.hypot(tangentX, tangentY), 0.0001);
        const tux = tangentX / tangentLen;
        const tuy = tangentY / tangentLen;
        const leftX = endX - (tux * arrowLen) + (-tuy * arrowWidth);
        const leftY = endY - (tuy * arrowLen) + (tux * arrowWidth);
        const rightX = endX - (tux * arrowLen) - (-tuy * arrowWidth);
        const rightY = endY - (tuy * arrowLen) - (tux * arrowWidth);
        return {
            path: `M ${svgNum(startX)} ${svgNum(startY)} Q ${svgNum(controlX)} ${svgNum(controlY)} ${svgNum(endX)} ${svgNum(endY)}`,
            arrowPoints: `${svgNum(endX)},${svgNum(endY)} ${svgNum(leftX)},${svgNum(leftY)} ${svgNum(rightX)},${svgNum(rightY)}`,
            color: `rgba(${color[0]}, ${color[1]}, ${color[2]}, ${alpha})`,
            arrowColor: `rgba(${color[0]}, ${color[1]}, ${color[2]}, ${Math.min(0.5, alpha + 0.12)})`,
            strokeWidth: 1.25 + (weight * 1.75),
            dashArray: isInbound ? '3 5' : ''
        };
    }).filter(Boolean);

    return {
        axisDescription: summary.axisDescription,
        legend: summary.legend,
        corners: summary.corners,
        frame,
        pixelInsets,
        centerPx,
        points,
        edges
    };
}

function buildQuadrantStageMeta(svgModel) {
    return {
        axisDescription: svgModel.axisDescription,
        legend: svgModel.legend
    };
}

function renderQuadrantTooltipThumb(point) {
    return `
        <div class="pgm-quadrant-tooltip-thumb">
            <img src="${QUADRANT_THUMBNAIL_SRC}" alt="" class="pgm-quadrant-tooltip-thumb-image" loading="lazy" decoding="async">
        </div>
    `;
}

function renderQuadrantTooltipHtml(point) {
    const title = escapeHtml(point.name || '-');
    const lines = [
        `상태 : ${point.status?.label || '-'}`,
        `첫구매 유입 점수 : ${formatNumber(point.entry, 3)}`,
        `재구매 점수 : ${formatNumber(point.expansion, 3)}`,
        point.outlierMarker ? `집중뷰 경계 표시 : ${point.outlierMarker}` : '',
        `주간 예상 수요량 : ${formatNumber(point.weeklyForecast, 1)}`,
        `리텐션 상태 : ${point.hasTransition ? '발생' : '없음'}`,
        `SKU 수 : ${formatNumber(point.memberCount, 0)}`
    ].filter(Boolean);

    return `
        <div class="pgm-quadrant-tooltip-header">
            ${renderQuadrantTooltipThumb(point)}
            <div class="pgm-quadrant-tooltip-title">${title}</div>
        </div>
        <div class="pgm-quadrant-tooltip-divider" aria-hidden="true"></div>
        <div class="pgm-quadrant-tooltip-body">
            ${lines.map((line) => `<div class="pgm-quadrant-tooltip-line">${escapeHtml(line)}</div>`).join('')}
        </div>
    `;
}

function renderQuadrantSvgMarkup(svgModel) {
    const { centerPx, frame } = svgModel;

    return `
        <svg class="pgm-quadrant-svg" viewBox="0 0 ${frame.width} ${frame.height}" preserveAspectRatio="none" aria-label="제품 수요 포지션 사분면">
            <g class="pgm-quadrant-svg-lines">
                <line class="pgm-quadrant-svg-center-line" x1="${svgNum(centerPx.x)}" y1="0" x2="${svgNum(centerPx.x)}" y2="${svgNum(frame.height)}"></line>
                <line class="pgm-quadrant-svg-center-line" x1="0" y1="${svgNum(centerPx.y)}" x2="${svgNum(frame.width)}" y2="${svgNum(centerPx.y)}"></line>
            </g>
            <g class="pgm-quadrant-svg-edges">
                ${svgModel.edges.map((edge) => `
                    <g class="pgm-quadrant-svg-edge">
                        <path d="${edge.path}" fill="none" stroke="${edge.color}" stroke-width="${svgNum(edge.strokeWidth)}" stroke-linecap="round" ${edge.dashArray ? `stroke-dasharray="${edge.dashArray}"` : ''}></path>
                        <polygon points="${edge.arrowPoints}" fill="${edge.arrowColor}"></polygon>
                    </g>
                `).join('')}
            </g>
            <g class="pgm-quadrant-svg-points">
                ${svgModel.points.map((point) => `
                    <g class="pgm-quadrant-svg-point ${point.isSelected ? 'is-selected' : ''}">
                        ${point.isSelected ? `
                            <circle class="pgm-quadrant-selected-ring" cx="${svgNum(point.x)}" cy="${svgNum(point.y)}" r="${svgNum(point.radius + 6)}" stroke="${point.focusRingColor}"></circle>
                            <circle class="pgm-quadrant-selected-pulse" cx="${svgNum(point.x)}" cy="${svgNum(point.y)}" r="${svgNum(point.radius + 6)}" stroke="${point.focusPulseColor}">
                                <animate attributeName="r" values="${svgNum(point.radius + 6)};${svgNum(point.radius + 14)};${svgNum(point.radius + 6)}" dur="1.6s" repeatCount="indefinite"></animate>
                                <animate attributeName="opacity" values="0.28;0.08;0.28" dur="1.6s" repeatCount="indefinite"></animate>
                            </circle>
                        ` : ''}
                        <circle class="pgm-quadrant-bubble-circle" cx="${svgNum(point.x)}" cy="${svgNum(point.y)}" r="${svgNum(point.radius)}" fill="${point.fill}" fill-opacity="${svgNum(point.fillOpacity)}" stroke="${point.stroke}" stroke-width="${svgNum(point.strokeWidth)}"></circle>
                        ${point.outlierMarker ? `<text class="pgm-quadrant-outlier-marker" x="${svgNum(point.x)}" y="${svgNum(point.y - point.radius - 10)}" text-anchor="middle">${escapeHtml(point.outlierMarker)}</text>` : ''}
                        <circle class="pgm-quadrant-bubble-hitbox" cx="${svgNum(point.x)}" cy="${svgNum(point.y)}" r="${svgNum(Math.max(14, point.outerRadius))}" fill="transparent" tabindex="0" role="button" aria-label="${escapeHtml(`${point.name} ${point.status?.label || ''}`)}" data-product-id="${escapeHtml(point.id)}"></circle>
                    </g>
                `).join('')}
            </g>
        </svg>
    `;
}

function renderQuadrantCornerOverlays(corners) {
    if (!corners) return '';
    return `
        <div class="pgm-quadrant-corner-overlay is-top-left" data-quadrant-corner="top-left" aria-hidden="true">
            <div class="pgm-quadrant-corner-copy">
                <div class="pgm-quadrant-corner-title">${escapeHtml(corners.topLeft.label)}</div>
                <div class="pgm-quadrant-corner-summary">${escapeHtml(corners.topLeft.summary)}</div>
            </div>
        </div>
        <div class="pgm-quadrant-corner-overlay is-top-right" data-quadrant-corner="top-right" aria-hidden="true">
            <div class="pgm-quadrant-corner-copy">
                <div class="pgm-quadrant-corner-title">${escapeHtml(corners.topRight.label)}</div>
                <div class="pgm-quadrant-corner-summary">${escapeHtml(corners.topRight.summary)}</div>
            </div>
        </div>
        <div class="pgm-quadrant-corner-overlay is-bottom-left" data-quadrant-corner="bottom-left" aria-hidden="true">
            <div class="pgm-quadrant-corner-copy">
                <div class="pgm-quadrant-corner-title">${escapeHtml(corners.bottomLeft.label)}</div>
                <div class="pgm-quadrant-corner-summary">${escapeHtml(corners.bottomLeft.summary)}</div>
            </div>
        </div>
        <div class="pgm-quadrant-corner-overlay is-bottom-right" data-quadrant-corner="bottom-right" aria-hidden="true">
            <div class="pgm-quadrant-corner-copy">
                <div class="pgm-quadrant-corner-title">${escapeHtml(corners.bottomRight.label)}</div>
                <div class="pgm-quadrant-corner-summary">${escapeHtml(corners.bottomRight.summary)}</div>
            </div>
        </div>
    `;
}

function getQuadrantViewportFrame(wrap) {
    const fallback = QUADRANT_SVG_FRAME;
    if (!wrap) return fallback;
    const measuredWidth = Math.round(toNumber(wrap.clientWidth, fallback.width));
    const measuredHeight = Math.round(toNumber(wrap.clientHeight, fallback.height));
    return {
        width: Math.max(QUADRANT_MIN_RENDER_WIDTH, measuredWidth || fallback.width),
        height: Math.max(260, measuredHeight || fallback.height)
    };
}

function getQuadrantSceneFrame(viewportFrame, model) {
    const frame = {
        width: Math.max(QUADRANT_MIN_RENDER_WIDTH, toNumber(viewportFrame?.width, QUADRANT_SVG_FRAME.width)),
        height: Math.max(260, toNumber(viewportFrame?.height, QUADRANT_SVG_FRAME.height))
    };
    if (String(model?.scaleMode || '').trim() !== 'focus') return frame;

    const widthFactor = frame.width < 640 ? 1.42 : frame.width < 760 ? 1.28 : 1.16;
    const heightFactor = frame.width < 640 ? 1.2 : 1.12;
    return {
        width: Math.round(Math.max(frame.width + 120, frame.width * widthFactor)),
        height: Math.round(Math.max(frame.height + 56, frame.height * heightFactor))
    };
}

function clampQuadrantRangeToBounds(range, bounds) {
    const next = { ...range };
    const boundWidth = Math.max(0.02, bounds.xMax - bounds.xMin);
    const boundHeight = Math.max(0.02, bounds.yMax - bounds.yMin);
    const width = Math.max(0.02, next.xMax - next.xMin);
    const height = Math.max(0.02, next.yMax - next.yMin);

    if (width >= boundWidth) {
        next.xMin = bounds.xMin;
        next.xMax = bounds.xMax;
    } else {
        if (next.xMin < bounds.xMin) {
            next.xMax += bounds.xMin - next.xMin;
            next.xMin = bounds.xMin;
        }
        if (next.xMax > bounds.xMax) {
            next.xMin -= next.xMax - bounds.xMax;
            next.xMax = bounds.xMax;
        }
    }

    if (height >= boundHeight) {
        next.yMin = bounds.yMin;
        next.yMax = bounds.yMax;
    } else {
        if (next.yMin < bounds.yMin) {
            next.yMax += bounds.yMin - next.yMin;
            next.yMin = bounds.yMin;
        }
        if (next.yMax > bounds.yMax) {
            next.yMin -= next.yMax - bounds.yMax;
            next.yMax = bounds.yMax;
        }
    }

    return next;
}

function getQuadrantSceneRange(model, sceneFrame, viewportFrame) {
    const activeRange = getQuadrantRenderRange(model?.scaleRange || {});
    if (String(model?.scaleMode || '').trim() !== 'focus') return activeRange;

    const focusRange = getQuadrantRenderRange(model?.focusRange || activeRange);
    const rawRange = getQuadrantRenderRange(model?.rawRange || activeRange);
    const xPad = Math.max(0.02, toNumber(model?.focusRange?.xPad, 0.02));
    const yPad = Math.max(0.02, toNumber(model?.focusRange?.yPad, 0.02));
    const bounds = {
        xMin: rawRange.xMin - (xPad * 0.45),
        xMax: rawRange.xMax + (xPad * 0.45),
        yMin: rawRange.yMin - (yPad * 0.45),
        yMax: rawRange.yMax + (yPad * 0.45)
    };

    const focusWidth = Math.max(0.02, focusRange.xMax - focusRange.xMin);
    const focusHeight = Math.max(0.02, focusRange.yMax - focusRange.yMin);
    const widthFactor = Math.max(1, toNumber(sceneFrame?.width, viewportFrame?.width) / Math.max(1, toNumber(viewportFrame?.width, 1)));
    const heightFactor = Math.max(1, toNumber(sceneFrame?.height, viewportFrame?.height) / Math.max(1, toNumber(viewportFrame?.height, 1)));
    const visibleRelaxX = 1 + ((widthFactor - 1) * 0.38);
    const visibleRelaxY = 1 + ((heightFactor - 1) * 0.32);
    const expandedWidth = Math.min(bounds.xMax - bounds.xMin, focusWidth * widthFactor * visibleRelaxX);
    const expandedHeight = Math.min(bounds.yMax - bounds.yMin, focusHeight * heightFactor * visibleRelaxY);
    const centerX = (focusRange.xMin + focusRange.xMax) / 2;
    const centerY = (focusRange.yMin + focusRange.yMax) / 2;

    return clampQuadrantRangeToBounds({
        xMin: centerX - (expandedWidth / 2),
        xMax: centerX + (expandedWidth / 2),
        yMin: centerY - (expandedHeight / 2),
        yMax: centerY + (expandedHeight / 2)
    }, bounds);
}

function renderQuadrantCanvasMarkup(svgModel, model) {
    const canPan = String(model?.scaleMode || '').trim() === 'focus'
        && (svgModel.frame.width > QUADRANT_SVG_FRAME.width || svgModel.frame.height > QUADRANT_SVG_FRAME.height);
    return `
        <div class="quadrant-chart-canvas-viewport ${canPan ? 'is-pan-enabled' : ''}" onpointerdown="startQuadrantCanvasPan(event)" onscroll="syncQuadrantCornerOverlayVisibility()">
            <div class="quadrant-chart-canvas-scene" style="width:${svgNum(svgModel.frame.width)}px;height:${svgNum(svgModel.frame.height)}px;">
                ${renderQuadrantSvgMarkup(svgModel)}
            </div>
        </div>
        ${renderQuadrantCornerOverlays(svgModel.corners)}
        <div class="pgm-quadrant-tooltip" hidden></div>
    `;
}

function renderQuadrantStageFrame(model) {
    const sceneFrame = getQuadrantSceneFrame(QUADRANT_SVG_FRAME, model);
    const sceneRange = getQuadrantSceneRange(model, sceneFrame, QUADRANT_SVG_FRAME);
    const svgModel = buildQuadrantSvgModel(model, sceneFrame, sceneRange);
    const meta = buildQuadrantStageMeta(svgModel);
    AppState.helpers.productsQuadrantModel = model;
    AppState.helpers.productsQuadrantSvgModel = svgModel;
    return `
        <div class="pgm-quadrant-stage-meta">
            <p class="pgm-quadrant-axis-desc">${escapeHtml(meta.axisDescription)}</p>
            <div class="pgm-quadrant-legend">
                ${meta.legend.map((item) => `
                    <span class="pgm-quadrant-legend-item is-${item.key}">
                        <span class="pgm-quadrant-legend-dot" aria-hidden="true"></span>
                        <span>${escapeHtml(item.label)}</span>
                    </span>
                `).join('')}
            </div>
        </div>
        <div class="quadrant-chart-canvas-wrap">
            ${renderQuadrantCanvasMarkup(svgModel, model)}
        </div>
    `;
}

function syncQuadrantCanvasViewportState(wrap) {
    const viewport = wrap?.querySelector('.quadrant-chart-canvas-viewport');
    if (!viewport) return;
    const canPan = viewport.scrollWidth > viewport.clientWidth + 2 || viewport.scrollHeight > viewport.clientHeight + 2;
    viewport.classList.toggle('is-pan-enabled', canPan);
    if (!canPan) {
        viewport.classList.remove('is-panning');
        viewport.scrollLeft = 0;
        viewport.scrollTop = 0;
    }
}

function syncQuadrantCornerOverlayVisibility() {
    const wrap = document.querySelector('.quadrant-chart-canvas-wrap');
    const viewport = wrap?.querySelector('.quadrant-chart-canvas-viewport');
    const svgModel = AppState.helpers.productsQuadrantSvgModel;
    if (!wrap || !viewport || !svgModel?.centerPx || !svgModel?.frame) return;

    if (!viewport.classList.contains('is-pan-enabled')) {
        wrap.querySelectorAll('.pgm-quadrant-corner-overlay[data-quadrant-corner]').forEach((node) => {
            node.classList.remove('is-hidden');
        });
        return;
    }

    const sceneWidth = Math.max(1, toNumber(svgModel.frame.width, 1));
    const sceneHeight = Math.max(1, toNumber(svgModel.frame.height, 1));
    const viewLeft = Math.max(0, viewport.scrollLeft);
    const viewRight = Math.min(sceneWidth, viewLeft + viewport.clientWidth);
    const viewTop = Math.max(0, viewport.scrollTop);
    const viewBottom = Math.min(sceneHeight, viewTop + viewport.clientHeight);
    const centerX = toNumber(svgModel.centerPx.x, sceneWidth / 2);
    const centerY = toNumber(svgModel.centerPx.y, sceneHeight / 2);
    const minVisibleWidth = 24;
    const minVisibleHeight = 24;
    const intersectSize = (aStart, aEnd, bStart, bEnd) => Math.max(0, Math.min(aEnd, bEnd) - Math.max(aStart, bStart));
    const visibleLeft = intersectSize(viewLeft, viewRight, 0, centerX);
    const visibleRight = intersectSize(viewLeft, viewRight, centerX, sceneWidth);
    const visibleTop = intersectSize(viewTop, viewBottom, 0, centerY);
    const visibleBottom = intersectSize(viewTop, viewBottom, centerY, sceneHeight);
    const visibility = {
        'top-left': visibleLeft > minVisibleWidth && visibleTop > minVisibleHeight,
        'top-right': visibleRight > minVisibleWidth && visibleTop > minVisibleHeight,
        'bottom-left': visibleLeft > minVisibleWidth && visibleBottom > minVisibleHeight,
        'bottom-right': visibleRight > minVisibleWidth && visibleBottom > minVisibleHeight
    };

    wrap.querySelectorAll('.pgm-quadrant-corner-overlay[data-quadrant-corner]').forEach((node) => {
        const key = String(node.getAttribute('data-quadrant-corner') || '').trim();
        node.classList.toggle('is-hidden', !visibility[key]);
    });
}

function centerQuadrantViewportOnSelected(wrap, svgModel) {
    const viewport = wrap?.querySelector('.quadrant-chart-canvas-viewport');
    if (!viewport || !svgModel) return;
    const selected = (svgModel.points || []).find((point) => point.isSelected) || null;
    if (!selected) return;
    const targetLeft = Math.max(0, selected.x - (viewport.clientWidth / 2));
    const targetTop = Math.max(0, selected.y - (viewport.clientHeight / 2));
    viewport.scrollLeft = Math.min(targetLeft, Math.max(0, viewport.scrollWidth - viewport.clientWidth));
    viewport.scrollTop = Math.min(targetTop, Math.max(0, viewport.scrollHeight - viewport.clientHeight));
}

function renderQuadrantSvgIntoWrap(wrap, model) {
    if (!wrap || !model) return null;
    const viewportFrame = getQuadrantViewportFrame(wrap);
    const sceneFrame = getQuadrantSceneFrame(viewportFrame, model);
    const sceneRange = getQuadrantSceneRange(model, sceneFrame, viewportFrame);
    const svgModel = buildQuadrantSvgModel(model, sceneFrame, sceneRange);
    AppState.helpers.productsQuadrantSvgModel = svgModel;
    wrap.innerHTML = renderQuadrantCanvasMarkup(svgModel, model);
    syncQuadrantCanvasViewportState(wrap);
    centerQuadrantViewportOnSelected(wrap, svgModel);
    syncQuadrantCornerOverlayVisibility();
    return svgModel;
}

function isCompactProductsSidePanelViewport() {
    return typeof window !== 'undefined' && window.innerWidth < 1280;
}

function getProductsSidePanelOpenState(isCompact = isCompactProductsSidePanelViewport()) {
    const savedSidePanelOpen = AppState.viewState.products?.sidePanelOpen;
    return typeof savedSidePanelOpen === 'boolean' ? savedSidePanelOpen : !isCompact;
}

function syncQuadrantCanvasWrapHeight() {
    const wrap = document.querySelector('.quadrant-chart-canvas-wrap');
    const stage = wrap?.closest('.pgm-chart-stage.is-quadrant-view');
    if (!wrap || !stage) return 0;
    const meta = stage.querySelector('.pgm-quadrant-stage-meta');
    const metaHeight = meta ? meta.getBoundingClientRect().height : 0;
    const available = Math.max(260, stage.clientHeight - metaHeight - 12);
    wrap.style.height = `${available}px`;
    wrap.style.flex = '0 0 auto';
    return available;
}

function syncQuadrantPanelHeights() {
    const body = document.querySelector('.pgm-quadrant-body');
    const chartCard = body?.querySelector('.pgm-chart.card.chart-card');
    const sideCard = body?.querySelector('.pgm-side.card');
    if (!body || !chartCard || !sideCard) return 0;

    chartCard.style.height = '';
    chartCard.style.minHeight = '';
    return sideCard.getBoundingClientRect().height || 0;
}

function startQuadrantCanvasPan(event) {
    const viewport = event.target.closest('.quadrant-chart-canvas-viewport');
    if (!viewport || !viewport.classList.contains('is-pan-enabled')) return;
    if (event.target.closest('.pgm-quadrant-bubble-hitbox')) return;
    AppState.helpers.productsQuadrantPan = {
        viewport,
        pointerId: event.pointerId,
        startX: event.clientX,
        startY: event.clientY,
        startScrollLeft: viewport.scrollLeft,
        startScrollTop: viewport.scrollTop
    };
    viewport.classList.add('is-panning');
    viewport.setPointerCapture?.(event.pointerId);
    event.preventDefault();
}

function handleQuadrantCanvasPointerMove(event) {
    const pan = AppState.helpers.productsQuadrantPan;
    if (!pan || pan.pointerId !== event.pointerId) return;
    pan.viewport.scrollLeft = pan.startScrollLeft - (event.clientX - pan.startX);
    pan.viewport.scrollTop = pan.startScrollTop - (event.clientY - pan.startY);
}

function handleQuadrantCanvasPointerEnd(event) {
    const pan = AppState.helpers.productsQuadrantPan;
    if (!pan || pan.pointerId !== event.pointerId) return;
    pan.viewport.classList.remove('is-panning');
    pan.viewport.releasePointerCapture?.(event.pointerId);
    AppState.helpers.productsQuadrantPan = null;
}

window.startQuadrantCanvasPan = startQuadrantCanvasPan;
window.syncQuadrantCornerOverlayVisibility = syncQuadrantCornerOverlayVisibility;
window.addEventListener('pointermove', handleQuadrantCanvasPointerMove);
window.addEventListener('pointerup', handleQuadrantCanvasPointerEnd);
window.addEventListener('pointercancel', handleQuadrantCanvasPointerEnd);

function ensureQuadrantResizeHandler() {
    if (AppState.helpers.productsQuadrantResizeBound) return;
    window.addEventListener('resize', () => {
        const isCompact = isCompactProductsSidePanelViewport();
        if (AppState.helpers.productsSidePanelCompact !== isCompact) {
            AppState.helpers.productsSidePanelCompact = isCompact;
            AppState.viewState.products.sidePanelOpen = isCompact ? false : true;
            renderProducts();
            return;
        }
        syncQuadrantPanelHeights();
        if (AppState.viewState?.products?.chartView === 'quadrant') {
            syncQuadrantCanvasWrapHeight();
            if (AppState.helpers.productsQuadrantModel) {
                bindQuadrantSvg(AppState.helpers.productsQuadrantModel);
            }
        }
        scheduleDemandGraphEdgeLayout();
    });
    AppState.helpers.productsQuadrantResizeBound = true;
}

function positionQuadrantTooltip(wrap, tooltip, clientX, clientY) {
    if (!wrap || !tooltip) return;
    const wrapRect = wrap.getBoundingClientRect();
    const tooltipWidth = tooltip.offsetWidth;
    const tooltipHeight = tooltip.offsetHeight;
    const offset = 14;
    let left = clientX - wrapRect.left + offset;
    let top = clientY - wrapRect.top + offset;
    if (left + tooltipWidth > wrapRect.width - 8) left = wrapRect.width - tooltipWidth - 8;
    if (top + tooltipHeight > wrapRect.height - 8) top = wrapRect.height - tooltipHeight - 8;
    if (left < 8) left = 8;
    if (top < 8) top = 8;
    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
}

function bindQuadrantSvg(model) {
    const wrap = document.querySelector('.quadrant-chart-canvas-wrap');
    if (!wrap || !model) return;
    ensureQuadrantResizeHandler();
    syncQuadrantPanelHeights();
    syncQuadrantCanvasWrapHeight();
    const svgModel = renderQuadrantSvgIntoWrap(wrap, model);
    const tooltip = wrap.querySelector('.pgm-quadrant-tooltip');
    if (!tooltip || !svgModel) return;
    const hideTooltip = () => {
        tooltip.hidden = true;
        tooltip.innerHTML = '';
    };

    wrap.querySelectorAll('.pgm-quadrant-bubble-hitbox').forEach((node) => {
        const targetId = String(node.getAttribute('data-product-id') || '').trim();
        const point = svgModel.points.find((item) => String(item.id || '').trim() === targetId);
        if (!point) return;

        const showTooltip = (clientX, clientY) => {
            tooltip.innerHTML = renderQuadrantTooltipHtml(point);
            tooltip.hidden = false;
            positionQuadrantTooltip(wrap, tooltip, clientX, clientY);
        };

        node.addEventListener('mouseenter', (event) => {
            showTooltip(event.clientX, event.clientY);
        });
        node.addEventListener('mousemove', (event) => {
            if (tooltip.hidden) return;
            positionQuadrantTooltip(wrap, tooltip, event.clientX, event.clientY);
        });
        node.addEventListener('mouseleave', hideTooltip);
        node.addEventListener('focus', () => {
            const wrapRect = wrap.getBoundingClientRect();
            showTooltip(wrapRect.left + point.x, wrapRect.top + point.y);
        });
        node.addEventListener('blur', hideTooltip);
        node.addEventListener('click', () => {
            if (typeof window.selectQuadrantItem === 'function') {
                window.selectQuadrantItem(point.id);
            }
        });
        node.addEventListener('keydown', (event) => {
            if (event.key !== 'Enter' && event.key !== ' ') return;
            event.preventDefault();
            if (typeof window.selectQuadrantItem === 'function') {
                window.selectQuadrantItem(point.id);
            }
        });
    });

    wrap.addEventListener('mouseleave', hideTooltip);
}

function getProductsScopeMode() {
    return 'retention-emphasis';
}

function getScopedProductsData() {
    return [...(AppState.data.anchorScored || [])];
}

const CORE_GRAVITY_ORDER = ['entry', 'expansion'];
const CORE_GRAVITY_CONFIG = {
    entry: {
        label: '첫구매 유입',
        scoreKey: 'Entry_Gravity_Score',
        helper: '신규 고객의 첫 구매를 많이 만들고 있는 제품부터 보여줘요.'
    },
    expansion: {
        label: '재구매 확장',
        scoreKey: 'Expansion_Gravity_Score',
        helper: '첫 구매 뒤 다음 구매로 자연스럽게 이어지는 제품부터 보여줘요.'
    },
    return: {
        label: '다시 찾는 구매',
        scoreKey: 'Return_Gravity_Score',
        helper: '고객이 다른 구매를 거친 뒤 다시 돌아오게 만드는 제품부터 보여줘요.'
    },
    convergence: {
        label: '도착 흐름',
        scoreKey: 'Convergence_Gravity_Score',
        helper: '다른 제품 다음에 이 제품으로 자주 이어지는 제품부터 보여줘요.'
    }
};

function normalizeCoreSortKey(value) {
    const key = String(value || '').toLowerCase();
    return CORE_GRAVITY_ORDER.includes(key) ? key : 'entry';
}

function getProductsCoreSortKey() {
    return normalizeCoreSortKey(AppState.viewState.products?.coreSortKey || 'entry');
}

function getMergedCoreDemandRows() {
    const gravityMap = new Map(
        (AppState.data.productDemandGravity || [])
            .map((row) => [String(row.product_id || '').trim(), row])
            .filter(([id]) => id)
    );

    return getScopedProductsData().map((row) => {
        const id = String(row.product_id || '').trim();
        const gravity = gravityMap.get(id) || {};
        return {
            ...row,
            Entry_Gravity_Score: toNumber(gravity.Entry_Gravity_Score, toNumber(row.Entry_Gravity_Score, 0)),
            Expansion_Gravity_Score: toNumber(gravity.Expansion_Gravity_Score, toNumber(row.Expansion_Gravity_Score, 0)),
            Convergence_Gravity_Score: toNumber(gravity.Convergence_Gravity_Score, 0),
            Return_Gravity_Score: toNumber(gravity.Return_Gravity_Score, 0),
            Entry_Gravity_Primary_Type: gravity.Entry_Gravity_Primary_Type || row.Entry_Gravity_Primary_Type || row.AA_Primary_Type || '-',
            Expansion_Gravity_Primary_Type: gravity.Expansion_Gravity_Primary_Type || row.Expansion_Gravity_Primary_Type || row.PCA_Primary_Type || '-',
            incoming_transition_rate_sum_90d: toNumber(gravity.incoming_transition_rate_sum_90d, 0),
            distinct_source_product_cnt_90d: toNumber(gravity.distinct_source_product_cnt_90d, 0),
            converged_customer_cnt_90d: toNumber(gravity.converged_customer_cnt_90d, 0),
            return_customer_rate_90d: toNumber(gravity.return_customer_rate_90d, 0),
            return_loop_rate_90d: toNumber(gravity.return_loop_rate_90d, 0),
            simple_repeat_rate_90d: toNumber(gravity.simple_repeat_rate_90d, 0)
        };
    });
}

function buildGravityContributionSet(rows, sortKey, threshold = 0.8) {
    const { scoreKey } = CORE_GRAVITY_CONFIG[sortKey] || CORE_GRAVITY_CONFIG.entry;
    const sorted = [...(rows || [])]
        .filter((row) => toNumber(row[scoreKey], 0) > 0)
        .sort((a, b) => toNumber(b[scoreKey], 0) - toNumber(a[scoreKey], 0));
    const total = sorted.reduce((acc, row) => acc + toNumber(row[scoreKey], 0), 0);
    if (total <= 0) {
        return { total: 0, items: [], achievedShare: 0 };
    }

    let cumulativeValue = 0;
    const items = [];
    sorted.forEach((row) => {
        if (items.length && cumulativeValue / total >= threshold) return;
        const value = toNumber(row[scoreKey], 0);
        cumulativeValue += value;
        const share = value / total;
        const cumulativeShare = cumulativeValue / total;
        items.push({
            product_id: String(row.product_id || '').trim(),
            product_name_latest: row.product_name_latest || getProductName(row.product_id),
            first_customer_cnt: toNumber(row.first_customer_cnt, 0),
            repurchase_customer_cnt_90d: toNumber(row.repurchase_customer_cnt_90d, 0),
            repurchase_rate_90d: toNumber(row.repurchase_rate_90d, 0),
            revenue_90d: toNumber(row.revenue_90d, 0),
            metricKey: scoreKey,
            metricValue: value,
            share,
            cumulativeShare,
            gravitySortKey: sortKey
        });
    });

    return {
        total,
        items,
        achievedShare: items.length ? items[items.length - 1].cumulativeShare : 0
    };
}

function buildCoreDemandTableModel() {
    const scopedRows = getMergedCoreDemandRows();
    const transitionEntitySet = buildTransitionEntitySet();
    const firstCustomerTotal = scopedRows.reduce((acc, row) => acc + toNumber(row.first_customer_cnt, 0), 0);
    const repurchaseCustomerTotal = scopedRows.reduce((acc, row) => acc + toNumber(row.repurchase_customer_cnt_90d, 0), 0);
    const sets = {};
    const itemMaps = {};
    const idSets = {};
    CORE_GRAVITY_ORDER.forEach((key) => {
        const contributionSet = buildGravityContributionSet(scopedRows, key, 0.8);
        sets[key] = contributionSet;
        itemMaps[key] = new Map();
        idSets[key] = new Set();
        contributionSet.items.forEach((item, index) => {
            const enriched = { ...item, rank: index + 1 };
            itemMaps[key].set(item.product_id, enriched);
            idSets[key].add(item.product_id);
        });
    });

    const rowMap = new Map(scopedRows.map((row) => [String(row.product_id || '').trim(), row]));
    const unionIds = new Set(CORE_GRAVITY_ORDER.flatMap((key) => sets[key].items.map((item) => item.product_id)));
    const rows = Array.from(unionIds).map((id) => {
        const source = rowMap.get(id) || {};
        const axisMembership = CORE_GRAVITY_ORDER.filter((key) => idSets[key].has(id));
        const gravityShares = CORE_GRAVITY_ORDER.reduce((acc, key) => {
            acc[key] = itemMaps[key].get(id)?.share ?? null;
            return acc;
        }, {});
        const gravityRanks = CORE_GRAVITY_ORDER.reduce((acc, key) => {
            acc[key] = itemMaps[key].get(id)?.rank ?? null;
            return acc;
        }, {});
        return {
            product_id: id,
            product_name_latest: source.product_name_latest || getProductName(id),
            member_ids: source.member_ids || '',
            revenue_90d: toNumber(source.revenue_90d, 0),
            first_customer_cnt: toNumber(source.first_customer_cnt, 0),
            repurchase_customer_cnt_90d: toNumber(source.repurchase_customer_cnt_90d, 0),
            repurchase_rate_90d: toNumber(source.repurchase_rate_90d, 0),
            hasTransition: transitionEntitySet.has(id),
            axisMembership,
            axisMembershipCount: axisMembership.length,
            gravityShares,
            gravityRanks,
            firstCustomerShare: firstCustomerTotal > 0 ? toNumber(source.first_customer_cnt, 0) / firstCustomerTotal : 0,
            repurchaseCustomerShare: repurchaseCustomerTotal > 0 ? toNumber(source.repurchase_customer_cnt_90d, 0) / repurchaseCustomerTotal : 0
        };
    });

    const currentSortKey = getProductsCoreSortKey();
    const searchQuery = String(AppState.viewState.products?.searchQuery || '').toLowerCase().trim();
    const filteredRows = searchQuery
        ? rows.filter((row) =>
            String(row.product_id || '').toLowerCase().includes(searchQuery) ||
            String(row.member_ids || '').toLowerCase().includes(searchQuery) ||
            String(row.product_name_latest || '').toLowerCase().includes(searchQuery)
        )
        : rows;

    filteredRows.sort((a, b) => {
        const aRank = Number.isFinite(a.gravityRanks[currentSortKey]) ? a.gravityRanks[currentSortKey] : Number.POSITIVE_INFINITY;
        const bRank = Number.isFinite(b.gravityRanks[currentSortKey]) ? b.gravityRanks[currentSortKey] : Number.POSITIVE_INFINITY;
        if (aRank !== bRank) return aRank - bRank;
        const shareDiff = toNumber(b.gravityShares[currentSortKey], 0) - toNumber(a.gravityShares[currentSortKey], 0);
        if (shareDiff !== 0) return shareDiff;
        const membershipDiff = toNumber(b.axisMembershipCount, 0) - toNumber(a.axisMembershipCount, 0);
        if (membershipDiff !== 0) return membershipDiff;
        return toNumber(b.revenue_90d, 0) - toNumber(a.revenue_90d, 0);
    });

    return {
        scopeMode: getProductsScopeMode(),
        scopedProductCount: scopedRows.length,
        currentSortKey,
        currentSortLabel: CORE_GRAVITY_CONFIG[currentSortKey].label,
        sets,
        rows: filteredRows,
        totalCoreCount: rows.length,
        sharedCoreCount: rows.filter((row) => row.axisMembershipCount > 1).length
    };
}

function buildCoreDemandRowsHtml(displayData, focusEntityId, emphasisMode = 'retention-emphasis', currentSortKey = 'entry') {
    return displayData.map((row) => {
        const isFocused = focusEntityId && String(row.product_id) === focusEntityId;
        const currentRank = row.gravityRanks[currentSortKey];
        return `
            <tr class="clickable ${isFocused ? 'row-focused' : ''}" onclick="focusQuadrantFromTable('${escapeHtml(row.product_id)}')">
                <td>
                    ${renderProductsTableNameCell(row)}
                </td>
                <td class="core-demand-table-cell is-centered">${Number.isFinite(currentRank) ? formatNumber(currentRank, 0) : '-'}</td>
                <td class="core-demand-table-cell is-centered">${formatPercent(row.firstCustomerShare, 1)}</td>
                <td class="core-demand-table-cell is-centered">${formatPercent(row.repurchaseCustomerShare, 1)}</td>
                <td class="core-demand-table-cell is-centered">${formatNumber(row.revenue_90d)}</td>
                <td class="core-demand-table-cell is-centered">${row.hasTransition ? '발생' : '없음'}</td>
            </tr>
        `;
    }).join('');
}

function renderProductsTableNameCell(row) {
    const productName = escapeHtml(row?.product_name_latest || '-');
    const productId = escapeHtml(row?.product_id || '-');
    return `
        <div class="pgm-product-table-name-cell">
            <div class="pgm-product-table-name" title="${productName}">${productName}</div>
            <div class="pgm-product-table-id">${productId}</div>
        </div>
    `;
}

function renderProductsTableSearch() {
    const query = String(AppState.viewState.products?.searchQuery || '');
    return `
        <div class="pgm-product-table-search animate-fade-in">
            <div class="pgm-product-table-search-wrap">
                <i class="ph ph-magnifying-glass" aria-hidden="true"></i>
                <input
                    id="search-input-products"
                    type="text"
                    class="pgm-product-table-search-input"
                    placeholder="제품명을 검색해 주세요."
                    value="${escapeHtml(query)}"
                    oncompositionstart="handleSearchCompositionStart('products')"
                    oncompositionend="handleSearchCompositionEnd('products', this)"
                    oninput="handleGlobalSearch('products', this.value, this.selectionStart, this.selectionEnd)"
                >
            </div>
        </div>
    `;
}

function getProductsCoreRankColumnLabel(sortKey) {
    return sortKey === 'expansion' ? '재구매 확장 순위' : '첫구매 유입 순위';
}

function renderCoreDemandSortTabs(currentSortKey) {
    return `
        <div class="core-demand-tabs-wrap">
            <div class="core-demand-tabs" role="tablist" aria-label="핵심 축 정렬 기준">
                ${CORE_GRAVITY_ORDER.map((key) => `
                    <button
                        class="core-demand-tab ${key === currentSortKey ? 'is-active' : ''}"
                        type="button"
                        role="tab"
                        aria-selected="${key === currentSortKey ? 'true' : 'false'}"
                        onclick="setProductsCoreSortKey('${key}')"
                    >${CORE_GRAVITY_CONFIG[key].label}</button>
                `).join('')}
            </div>
        </div>
    `;
}

function renderProductsTableOnly(model = null) {
    if (document.body.id !== 'page-products') return;
    const summaryCard = document.getElementById('products-summary-card');
    if (!summaryCard) return;

    const resolvedModel = model || buildCoreDemandTableModel();
    const qSelectedId = String(AppState.viewState.products?.quadrant?.selectedId || '').trim();
    const focusEntityId = String(AppState.helpers.focusEntityId || qSelectedId).trim();
    const emphasisMode = getProductsScopeMode();
    const currentSortKey = resolvedModel.currentSortKey;
    const rows = buildCoreDemandRowsHtml(resolvedModel.rows, focusEntityId, emphasisMode, currentSortKey);
    const rankColumnLabel = getProductsCoreRankColumnLabel(currentSortKey);

    summaryCard.innerHTML = `
        <div class="pgm-product-table-card">
            <div class="pgm-product-table-top">
                <div class="pgm-core-demand-head">
                    <h3>최근 90일 핵심 제품</h3>
                    <p class="pgm-core-desc">최근 1년 내 판매 이력이 있고, 최근 90일에도 실제 판매가 이어진 핵심 제품이에요.</p>
                </div>
                <div class="pgm-product-table-filter-row">
                    ${renderCoreDemandSortTabs(currentSortKey)}
                    ${renderProductsTableSearch()}
                </div>
            </div>
            <div class="table-container">
                <table class="data-table core-demand-table">
                    <colgroup>
                        <col class="core-demand-col-name">
                        <col class="core-demand-col-rank">
                        <col class="core-demand-col-share">
                        <col class="core-demand-col-share">
                        <col class="core-demand-col-revenue">
                        <col class="core-demand-col-retention">
                    </colgroup>
                    <thead><tr>
                        <th>제품명</th>
                        <th>${escapeHtml(rankColumnLabel)}</th>
                        <th>첫구매 고객 비중</th>
                        <th>재구매 고객 비중</th>
                        <th>최근 90일 매출</th>
                        <th>리텐션 상태</th>
                    </tr></thead>
                    <tbody>${rows || `<tr><td colspan="6" class="core-demand-empty">지금 범위에서는 표시할 핵심 제품이 없어요.</td></tr>`}</tbody>
                </table>
            </div>
        </div>
    `;
    applyFriendlyUi(summaryCard);
}

function renderProducts() {
    destroyCarts();
    const container = document.getElementById('content-area');
    const qState = AppState.viewState.products.quadrant;
    qState.scope = 'retention-emphasis';
    if (!['focus', 'raw'].includes(qState.scaleMode)) qState.scaleMode = 'focus';
    qState.edgeMode = normalizeQuadrantEdgeMode(qState.edgeMode);

    const quadrantModel = buildQuadrantModel(
        getScopedProductsData(),
        qState.selectedId,
        qState.scaleMode || 'focus',
        qState.scope || 'retention-emphasis',
        qState.edgeMode || 'representative'
    );
    const coreDemandModel = buildCoreDemandTableModel();
    if (quadrantModel) {
        qState.selectedId = quadrantModel.selected.id;
    }

    container.innerHTML = renderProductQuadrant(quadrantModel, coreDemandModel);
    applyFriendlyUi(container);

    if (AppState.viewState.products.chartView !== 'demand-graph') {
        bindQuadrantSvg(quadrantModel);
    } else {
        ensureQuadrantResizeHandler();
        syncQuadrantPanelHeights();
        ensureDemandGraphResizeHandler();
        scheduleDemandGraphEdgeLayout();
    }
    renderProductsTableOnly(coreDemandModel);

    const restoreProductsScrollPosition = (scrollX, scrollY) => {
        const restore = () => window.scrollTo(scrollX, scrollY);
        restore();
        requestAnimationFrame(() => {
            restore();
            requestAnimationFrame(restore);
        });
        setTimeout(restore, 0);
        setTimeout(restore, 80);
    };

    const scrollProductsChartIntoView = (scrollTarget = 'chart-head') => {
        const chartHead = container.querySelector('.pgm-quadrant-head');
        const chartWrap = container.querySelector('.pgm-chart-wrap');
        const target = scrollTarget === 'chart-head'
            ? (chartHead || chartWrap || container.querySelector('.pgm-quadrant-wrap'))
            : (chartWrap || chartHead || container.querySelector('.pgm-quadrant-wrap'));
        if (!target) return;
        const top = Math.max(0, window.scrollY + target.getBoundingClientRect().top - 16);
        window.scrollTo({ top, left: 0, behavior: 'smooth' });
    };

    const scheduleProductsScrollIntent = (scrollTarget, intentToken, renderToken) => {
        if (!scrollTarget || !intentToken || !renderToken) return;
        const run = () => {
            const currentIntent = AppState.helpers.productsScrollIntent;
            const currentRenderToken = AppState.helpers.productsRenderToken;
            if (!currentIntent || currentIntent.token !== intentToken) return;
            if (currentRenderToken !== renderToken) return;
            scrollProductsChartIntoView(scrollTarget);
            AppState.helpers.productsScrollIntent = null;
            AppState.helpers.productsPendingScrollTarget = null;
        };
        requestAnimationFrame(() => {
            requestAnimationFrame(run);
        });
        setTimeout(run, 80);
    };

    const rerenderProductsSelection = (preserveScroll = false, scrollTarget = '') => {
        const requestedScrollTarget = String(scrollTarget || AppState.helpers.productsPendingScrollTarget || '').trim();
        const scrollY = preserveScroll ? window.scrollY : 0;
        const scrollX = preserveScroll ? window.scrollX : 0;
        const renderToken = (toNumber(AppState.helpers.productsRenderToken, 0) || 0) + 1;
        AppState.helpers.productsRenderToken = renderToken;
        const nextQuadrantModel = buildQuadrantModel(
            getScopedProductsData(),
            qState.selectedId,
            qState.scaleMode || 'focus',
            qState.scope || 'retention-emphasis',
            qState.edgeMode || 'representative'
        );
        const nextCoreDemandModel = buildCoreDemandTableModel();
        if (nextQuadrantModel) {
            qState.selectedId = nextQuadrantModel.selected.id;
        }
        const quadrantWrap = container.querySelector('.pgm-quadrant-wrap');
        if (quadrantWrap) {
            quadrantWrap.outerHTML = renderProductQuadrant(nextQuadrantModel, nextCoreDemandModel);
            applyFriendlyUi(container.querySelector('.pgm-quadrant-wrap') || container);
        }
        if (AppState.viewState.products.chartView !== 'demand-graph') {
            bindQuadrantSvg(nextQuadrantModel);
        } else {
            ensureQuadrantResizeHandler();
            syncQuadrantPanelHeights();
            ensureDemandGraphResizeHandler();
            scheduleDemandGraphEdgeLayout();
        }
        renderProductsTableOnly(nextCoreDemandModel);
        if (preserveScroll) {
            restoreProductsScrollPosition(scrollX, scrollY);
        }
        if (requestedScrollTarget) {
            const activeIntent = AppState.helpers.productsScrollIntent;
            if (activeIntent?.target === requestedScrollTarget) {
                activeIntent.renderToken = renderToken;
            } else {
                const nextIntentToken = (toNumber(AppState.helpers.productsScrollIntentSeq, 0) || 0) + 1;
                AppState.helpers.productsScrollIntentSeq = nextIntentToken;
                AppState.helpers.productsScrollIntent = {
                    token: nextIntentToken,
                    target: requestedScrollTarget,
                    renderToken
                };
            }
            const finalIntent = AppState.helpers.productsScrollIntent;
            scheduleProductsScrollIntent(finalIntent?.target, finalIntent?.token, renderToken);
        } else {
            AppState.helpers.productsScrollIntent = null;
        }
    };
    AppState.helpers.rerenderProductsSelection = rerenderProductsSelection;

    window.selectQuadrantItem = (entityId, options = {}) => {
        const targetId = String(entityId || '').trim();
        if (!targetId) return;
        const preserveScroll = Boolean(options?.preserveScroll);
        const scrollTarget = String(options?.scrollTarget || (options?.scrollToChart ? 'chart-head' : '')).trim();
        AppState.helpers.productsPendingScrollTarget = preserveScroll ? null : scrollTarget;
        if (!scrollTarget) {
            AppState.helpers.productsScrollIntent = null;
        }
        if (preserveScroll && document.activeElement instanceof HTMLElement) {
            document.activeElement.blur();
        }
        const qState = AppState.viewState.products.quadrant;
        if (qState.selectedId && qState.selectedId !== targetId) {
            qState.history.push(qState.selectedId);
            if (qState.history.length > 30) qState.history = qState.history.slice(-30);
        }
        qState.selectedId = targetId;
        AppState.helpers.focusEntityId = targetId;
        rerenderProductsSelection(preserveScroll, scrollTarget);
    };

    window.focusQuadrantFromTable = (entityId) => {
        const targetId = String(entityId || '').trim();
        if (!targetId) return;
        if (typeof window.selectQuadrantItem === 'function') {
            window.selectQuadrantItem(targetId, { preserveScroll: false, scrollTarget: 'chart-head' });
        }
    };

    window.focusQuadrantFromDemandDriver = (entityId) => {
        const targetId = String(entityId || '').trim();
        if (!targetId) return;
        if (typeof window.selectQuadrantItem === 'function') {
            window.selectQuadrantItem(targetId, { preserveScroll: true });
        }
    };

    window.setProductsCoreSortKey = (key) => {
        AppState.viewState.products.coreSortKey = normalizeCoreSortKey(key);
        renderProductsTableOnly();
    };

    window.setProductsDemandGraphTab = (tab) => {
        AppState.viewState.products.demandGraphTab = normalizeDemandGraphTab(tab);
        rerenderProductsSelection(true);
    };

    window.setProductsChartView = (view) => {
        AppState.viewState.products.chartView = view === 'demand-graph' ? 'demand-graph' : 'quadrant';
        rerenderProductsSelection(true);
    };

    window.toggleProductsSidePanel = () => {
        AppState.viewState.products.sidePanelOpen = !getProductsSidePanelOpenState();
        rerenderProductsSelection(true);
    };

    window.closeProductsSidePanel = () => {
        if (!getProductsSidePanelOpenState()) return;
        AppState.viewState.products.sidePanelOpen = false;
        rerenderProductsSelection(true);
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
        if (AppState.viewState.products.quadrant.scaleMode === next) return;
        AppState.viewState.products.quadrant.scaleMode = next;
        rerenderProductsSelection(true);
    };

    window.setQuadrantScopeMode = (mode) => {
        AppState.viewState.products.quadrant.scope = 'retention-emphasis';
        renderProducts();
    };

    window.setQuadrantEdgeMode = (mode) => {
        const next = normalizeQuadrantEdgeMode(mode);
        const availability = getQuadrantEdgeModeAvailability();
        if (!availability[next]) return;
        if (AppState.viewState.products.quadrant.edgeMode === next) return;
        AppState.viewState.products.quadrant.edgeMode = next;
        rerenderProductsSelection(true);
    };

}
