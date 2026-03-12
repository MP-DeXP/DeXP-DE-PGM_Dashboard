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

function getQuadrantStatus(entry, expansion, centerEntry, centerExpansion) {
    const highEntry = toNumber(entry, 0) >= toNumber(centerEntry, 0);
    const highExpansion = toNumber(expansion, 0) >= toNumber(centerExpansion, 0);
    if (highEntry && highExpansion) {
        return {
            key: 'hero',
            label: '우선 확대 대상',
            color: '#3b82f6',
            summary: '현재 화면에서 신규 유입 강점과 재구매 강점이 모두 상대적으로 높아 우선 확대를 검토할 수 있는 상품이에요.',
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
            color: '#ef4444',
            summary: '현재 화면에서 신규 유입 강점과 재구매 강점이 모두 상대적으로 낮아 개선 여부를 점검할 필요가 있는 상태예요.',
            guide: '현재 화면에서 신규 유입 강점과 재구매 강점이 모두 상대적으로 낮은 구간이에요. 개선 실험 후 유지 여부를 판단할 수 있어요.',
            actions: [
                '가격·구성·메시지 개선 실험으로 반응 회복 가능성을 먼저 확인해요.',
                '개선 반응이 낮으면 축소 또는 대체 상품으로 전환해요.'
            ]
        };
    }
    if (highEntry && !highExpansion) {
        return {
            key: 'entry-only',
            label: '첫구매 강점 상품',
            color: '#14b8a6',
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
        label: '재구매 강점 상품',
        color: '#8b5cf6',
        summary: '현재 화면에서 재구매 강점은 상대적으로 높지만 신규 유입 강점은 상대적으로 낮아 유입 확대가 필요해요.',
        guide: '현재 화면에서 재구매 강점은 상대적으로 높지만, 신규 유입 강점은 상대적으로 낮은 구간이에요. 신규 유입 채널 보강이 필요해요.',
        actions: [
            '신규 유입 채널과 크리에이티브를 확장해 첫구매 모수를 늘려요.',
            '첫구매 강점 상품과의 동시 노출로 유입 구간을 보강해요.'
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

function buildQuadrantModel(rows, selectedId, scaleMode = 'focus', scope = 'retention-emphasis', edgeMode = 'both') {
    const transitionEntitySet = buildTransitionEntitySet();
    const normalizedScope = String(scope || '').toLowerCase() === 'all' ? 'all' : 'retention-emphasis';
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
    const scale = buildQuadrantScaleModel(points, selected, scaleMode);
    const visibleEdges = buildVisibleQuadrantEdges(pointsWithDemandShare, selected.id, normalizedEdgeMode);

    return {
        points: pointsWithDemandShare,
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
    const lowContribution = levels.entryDemandShare === '낮음' && levels.expansionDemandShare === '낮음';
    const hasCartSignal = Boolean(selectedCa);
    const highCartExpansion = hasCartSignal && (levels.attachRate === '높음' || levels.breadthLift === '높음');
    const highCartConcentration = hasCartSignal && levels.top1Share === '높음';
    const cartType = String(selectedCa?.ca_type || '').toLowerCase();

    let roleText = '현재 반응을 점검할 필요가 있는 상품';
    if (highEntryContribution && highExpansionContribution) {
        roleText = '신규 유입과 재구매를 함께 만드는 핵심 상품';
    } else if (highExpansionContribution && (highContinuity || statusKey === 'expansion-only')) {
        roleText = '재구매를 지키는 유지형 핵심 상품';
    } else if (highEntryContribution && (highDemand || statusKey === 'entry-only')) {
        roleText = '신규 유입을 크게 만드는 확장 후보';
    } else if (highCartExpansion) {
        roleText = '장바구니 연결력이 있는 보조 상품';
    } else if (highDemand || mediumDemand) {
        roleText = '수요 반응을 확인할 가치가 있는 상품';
    }

    let goalText = '현재 강점을 유지하면서 다음 성장 포인트를 확인해요.';
    if (highDemand && (highEntryContribution || highExpansionContribution) && lowContinuity) {
        goalText = '유입된 수요가 다음 구매로 이어지도록 유지 흐름을 보강해요.';
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
        roleText,
        goalText,
        actionText,
        reasonTags: levels,
        selectedCa
    };
}

function renderQuadrantPanel(model) {
    if (!model) {
        return '<p class="empty-state">4분면 계산 대상 상품이 없습니다.</p>';
    }
    const { selected, status } = model;
    const strategy = buildQuadrantStrategyModel(model);
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
        { key: 'hero', label: '우선 확대 대상', color: '#3b82f6', guide: '현재 화면에서 신규 유입 강점과 재구매 강점이 모두 상대적으로 높은 구간이에요. 우선적으로 노출·예산·재고 확대를 검토할 수 있어요.' },
        { key: 'phaseout', label: '개선 필요', color: '#ef4444', guide: '현재 화면에서 신규 유입 강점과 재구매 강점이 모두 상대적으로 낮은 구간이에요. 개선 실험 후 유지 여부를 판단할 수 있어요.' },
        { key: 'entry-only', label: '첫구매 강점 상품', color: '#14b8a6', guide: '현재 화면에서 신규 유입 강점은 상대적으로 높지만, 재구매 강점은 상대적으로 낮은 구간이에요. 재구매 전환 장치 보강이 필요해요.' },
        { key: 'expansion-only', label: '재구매 강점 상품', color: '#8b5cf6', guide: '현재 화면에서 재구매 강점은 상대적으로 높지만, 신규 유입 강점은 상대적으로 낮은 구간이에요. 신규 유입 채널 보강이 필요해요.' }
    ];
    const transitionCta = selected.hasTransition
        ? `<button class="btn-primary" type="button" onclick="openRetentionFlowModal('${escapeJs(selected.id)}')">90일 추가구매 상품 보기</button>`
        : `
            <button class="btn-primary" type="button" disabled title="90일 추가구매 데이터가 없어 이동할 수 없음">90일 추가구매 상품 보기</button>
            <p class="pgm-link-help">구매 후 90일 내 추가구매 데이터가 없어 이동할 수 없어요.</p>
        `;
    const edgeModeLabel = model.edgeMode === 'both' ? '양방향 흐름' : '다음 재구매 흐름';
    const edgeGuide = model.visibleEdges?.length
        ? `이 상품 기준 상위 ${formatNumber(model.visibleEdges.length)}개 ${edgeModeLabel}을 시각화했어요.`
        : '이 상품은 90일 내 재구매 상품 연결이 없어 선이 표시되지 않아요.';
    return `
        <div class="pgm-side-summary">
            <span class="pgm-badge" style="background:${status.color}1f; color:${status.color}; border-color:${status.color}55;">${status.label}</span>
            <div class="pgm-selected-head">
                <h3 title="${escapeHtml(selected.name)}">${escapeHtml(selected.name)}</h3>
                ${groupedLabel}
            </div>
            <p class="pgm-panel-helper">최근 1년 내 판매 이력이 있고, 최근 90일에도 실제 판매가 이어진 핵심 수요 상품 기준이에요.</p>
            <div class="pgm-insight-section">
                <h4>수요 인사이트</h4>
                <div class="pgm-metrics pgm-insight-metrics">
                    <div><label>주간 예상 수요량</label><strong>${formatNumber(selected.weeklyForecast, 1)}</strong><span>${memberMeta}</span></div>
                    <div><label>구매 지속 가능성</label><strong>${formatPercent(selected.repurchaseRate90d, 1)}</strong><span>구매 후 90일 기준</span></div>
                </div>
                <div class="pgm-insight-actions">
                    ${transitionCta}
                </div>
            </div>
            <div class="pgm-demand-share-section">
                <h4>수요 기여 비중</h4>
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
            </div>
            <div class="pgm-actions">
                <h4>운영 전략</h4>
                <div class="pgm-action-list">
                    <div class="pgm-action-row">
                        <label>상품 역할</label>
                        <p>${escapeHtml(strategy?.roleText || '-')}</p>
                    </div>
                    <div class="pgm-action-row">
                        <label>전략 목표</label>
                        <p>${escapeHtml(strategy?.goalText || '-')}</p>
                    </div>
                    <div class="pgm-action-row">
                        <label>실행 방법</label>
                        <p>${escapeHtml(strategy?.actionText || '-')}</p>
                    </div>
                </div>
            </div>
            <div class="pgm-basket-section">
                <h4>장바구니 확장 힌트</h4>
                <p class="pgm-basket-helper">이 상품이 포함될 때 함께 담기는 상품 구조를 보여줘요.</p>
                <button class="btn-primary" type="button" onclick="openCartFlowModal('${escapeJs(selected.id)}')">함께 구매되는 상품 보기</button>
            </div>
            <div class="pgm-status-guide">
                <h4>상태 정의</h4>
                <p class="pgm-status-helper">이 상태는 현재 화면의 상품들끼리 비교한 상대 위치 기준이에요.</p>
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
            <button class="btn-primary pgm-prev-btn" type="button" onclick="selectPreviousQuadrantItem()" ${hasHistory ? '' : 'disabled'}>이전 상품으로</button>
        </div>
    `;
}

function renderProductQuadrant(model, coreDemandModel = null) {
    const qState = AppState.viewState.products.quadrant || {};
    const scaleMode = qState.scaleMode || 'focus';
    const scopeMode = qState.scope === 'all' ? 'all' : 'retention-emphasis';
    const emptyChartMessage = '표시할 상품이 없습니다.';
    const demandHeadline = coreDemandModel && coreDemandModel.totalCoreCount > 0
        ? `${formatNumber(coreDemandModel.totalCoreCount, 0)}개 상품이 핵심 수요를 만들고 있어요.`
        : '';
    return `
        <div class="card pgm-quadrant-wrap animate-fade-in">
            <div class="pgm-quadrant-head">
                <div>
                    <h3>상품 상태 4분면</h3>
                    <p>첫구매 강점과 재구매 강점을 한눈에 비교해요.</p>
                    ${demandHeadline ? `<span class="pgm-head-highlight">${escapeHtml(demandHeadline)}</span>` : ''}
                </div>
                <div class="quadrant-head-controls">
                    <div class="quadrant-scope-toggle">
                        <button
                            class="btn-primary metric-tooltip-target ${scopeMode === 'retention-emphasis' ? 'is-active' : ''}"
                            type="button"
                            data-metric-tooltip="전체 상품을 보여주되 리텐션 발생 상품을 더 선명하게 강조해요."
                            aria-label="전체 상품을 보여주되 리텐션 발생 상품을 더 선명하게 강조해요."
                            onclick="setQuadrantScopeMode('retention-emphasis')"
                        >리텐션 발생 상품 강조</button>
                        <button
                            class="btn-primary metric-tooltip-target ${scopeMode === 'all' ? 'is-active' : ''}"
                            type="button"
                            data-metric-tooltip="전체 상품을 동일한 강조 수준으로 보여줘요."
                            aria-label="전체 상품을 동일한 강조 수준으로 보여줘요."
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
            <p
                class="quadrant-bubble-note metric-tooltip-target"
                title="버블 크기는 주간 예상 수요량(최근 1년 주문수 ÷ 52)의 상대 크기예요."
                data-metric-tooltip="버블 크기는 주간 예상 수요량(최근 1년 주문수 ÷ 52)의 상대 크기예요."
                aria-label="버블 크기는 주간 예상 수요량(최근 1년 주문수 ÷ 52)의 상대 크기예요."
            >버블 크기 기준: 주간 예상 수요량(최근 1년 주문수 ÷ 52)</p>
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
    const canvasHeight = Math.max(220, toNumber(canvas.clientHeight, 0));
    const bubbleScale = Math.min(1.55, Math.max(1, canvasHeight / 560));
    const baseRadius = 7 * bubbleScale;
    const radiusSpread = 24 * bubbleScale;
    const maxRadius = 42;
    const mutedFillPalette = {
        hero: 'rgba(147, 197, 253, 0.34)',
        'entry-only': 'rgba(153, 246, 228, 0.3)',
        'expansion-only': 'rgba(216, 180, 254, 0.32)',
        phaseout: 'rgba(253, 186, 186, 0.3)'
    };
    const mutedBorderPalette = {
        hero: 'rgba(96, 165, 250, 0.54)',
        'entry-only': 'rgba(45, 212, 191, 0.5)',
        'expansion-only': 'rgba(196, 181, 253, 0.56)',
        phaseout: 'rgba(252, 165, 165, 0.5)'
    };

    const range = model.scaleRange;
    const chartPoints = model.points.map((p) => {
        const status = getQuadrantStatus(p.entry, p.expansion, centerX, centerY);
        const radius = baseRadius + radiusSpread * Math.sqrt((p.weeklyForecast || 0) / (model.maxWeekly || 1));
        const isSelected = selectedId && selectedId === p.id;
        const projected = model.scaleMode === 'focus' ? projectOutlierPoint(p, range) : { x: p.entry, y: p.expansion, marker: '' };
        return {
            x: projected.x,
            y: projected.y,
            r: Math.min(maxRadius, Math.max(baseRadius, isSelected ? radius * 1.15 : radius)),
            productId: p.id,
            productName: p.name,
            weeklyForecast: p.weeklyForecast,
            rawEntry: p.entry,
            rawExpansion: p.expansion,
            outlierMarker: projected.marker,
            status,
            memberCount: p.memberCount,
            hasTransition: p.hasTransition,
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
                        if (model.scope === 'retention-emphasis' && !raw.hasTransition) {
                            return raw.isSelected ? base : (mutedFillPalette[raw.status?.key] || 'rgba(148, 163, 184, 0.22)');
                        }
                        return raw.isSelected ? base : `${base}d9`;
                    },
                    borderColor: (ctx2) => {
                        const raw = ctx2.raw || {};
                        if (model.scope === 'retention-emphasis' && !raw.hasTransition) {
                            return raw.isSelected ? '#ffffff' : (mutedBorderPalette[raw.status?.key] || 'rgba(148, 163, 184, 0.42)');
                        }
                        return model.scope === 'retention-emphasis' ? 'rgba(255, 255, 255, 0.96)' : '#ffffff';
                    },
                    borderWidth: (ctx2) => {
                        const raw = ctx2.raw || {};
                        if (raw.isSelected) return 2.6;
                        if (model.scope === 'retention-emphasis' && !raw.hasTransition) return 0.9;
                        return 1.4;
                    },
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
                                `주간 예상 수요량: ${formatNumber(raw.weeklyForecast, 1)}`,
                                `리텐션 상태: ${raw.hasTransition ? '발생' : '없음'}`,
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
                    title: { display: true, text: '첫구매 강점' },
                    ticks: { display: false },
                    grid: { display: false, drawBorder: false }
                },
                y: {
                    min: range.yMin,
                    max: range.yMax,
                    title: { display: true, text: '재구매 강점' },
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
                    { text: '재구매 강점 상품', helper: '상대적으로 재구매 강점 높음', x: chartArea.left + 12, y: chartArea.top + 10, align: 'left', helperOffset: 12 },
                    { text: '우선 확대 대상', helper: '상대적으로 두 강점 모두 높음', x: chartArea.right - 12, y: chartArea.top + 10, align: 'right', helperOffset: 12 },
                    { text: '개선 필요', helper: '상대적으로 두 강점 모두 낮음', x: chartArea.left + 12, y: chartArea.bottom - 22, align: 'left', helperOffset: 12 },
                    { text: '첫구매 강점 상품', helper: '상대적으로 첫구매 강점 높음', x: chartArea.right - 12, y: chartArea.bottom - 22, align: 'right', helperOffset: 12 }
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
                chartCtx.textBaseline = 'middle';
                labels.forEach((label) => {
                    chartCtx.textAlign = label.align;
                    chartCtx.font = '700 10px Inter, sans-serif';
                    chartCtx.fillStyle = '#334155';
                    chartCtx.fillText(label.text, label.x, label.y);
                    chartCtx.font = '500 9px Inter, sans-serif';
                    chartCtx.fillStyle = 'rgba(51, 65, 85, 0.78)';
                    chartCtx.fillText(label.helper, label.x, label.y + label.helperOffset);
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

function getProductsScopeMode() {
    return String(AppState.viewState.products?.quadrant?.scope || 'retention-emphasis').toLowerCase() === 'all' ? 'all' : 'retention-emphasis';
}

function getScopedProductsData() {
    return [...(AppState.data.anchorScored || [])];
}

const CORE_GRAVITY_ORDER = ['entry', 'expansion', 'return', 'convergence'];
const CORE_GRAVITY_CONFIG = {
    entry: {
        label: '첫구매 유입',
        scoreKey: 'Entry_Gravity_Score',
        helper: '신규 고객의 첫 구매를 많이 만들고 있는 상품부터 보여줘요.'
    },
    expansion: {
        label: '재구매 확장',
        scoreKey: 'Expansion_Gravity_Score',
        helper: '첫 구매 뒤 다음 구매로 자연스럽게 이어지는 상품부터 보여줘요.'
    },
    return: {
        label: '다시 찾는 구매',
        scoreKey: 'Return_Gravity_Score',
        helper: '고객이 다른 구매를 거친 뒤 다시 돌아오게 만드는 상품부터 보여줘요.'
    },
    convergence: {
        label: '수요 집중',
        scoreKey: 'Convergence_Gravity_Score',
        helper: '여러 상품 흐름의 끝에서 수요가 모이는 상품부터 보여줘요.'
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

function renderCoreAxisSummary(axisMembership, currentSortKey) {
    const chips = axisMembership.map((key) => {
        const isActive = key === currentSortKey;
        return `<span class="core-axis-chip ${isActive ? 'is-active' : ''}">${escapeHtml(CORE_GRAVITY_CONFIG[key].label)}</span>`;
    }).join('');
    const summary = axisMembership.length > 1
        ? `<span class="core-axis-summary">${formatNumber(axisMembership.length, 0)}축 핵심</span>`
        : '';
    return `
        <div class="core-axis-cell">
            <div class="core-axis-group">${chips}</div>
            ${summary}
        </div>
    `;
}

function buildCoreDemandRowsHtml(displayData, focusEntityId, emphasisMode = 'retention-emphasis', currentSortKey = 'entry') {
    return displayData.map((row) => {
        const isFocused = focusEntityId && String(row.product_id) === focusEntityId;
        const isMuted = emphasisMode === 'retention-emphasis' && !row.hasTransition;
        const meta = getEntityMeta(row.product_id);
        const groupedChip = meta.memberCount > 1
            ? `<button class="group-chip-trigger" type="button" onclick="event.stopPropagation();openGroupEditorWizard({focusEntityId:'${escapeJs(meta.entityId)}'})">그룹 ${formatNumber(meta.memberCount, 0)}개</button>`
            : '';
        const currentShare = row.gravityShares[currentSortKey];
        const currentRank = row.gravityRanks[currentSortKey];
        return `
            <tr class="clickable ${isFocused ? 'row-focused' : ''} ${isMuted ? 'row-retention-muted' : ''}" onclick="focusQuadrantFromTable('${escapeHtml(row.product_id)}')">
                <td>
                    <div class="core-product-cell">
                        ${renderProductCell(row.product_name_latest || '-', row.product_id, 36, { nameClickMode: 'focus-quadrant' })}
                        ${groupedChip}
                    </div>
                </td>
                <td>${renderCoreAxisSummary(row.axisMembership, currentSortKey)}</td>
                <td>${currentShare !== null ? formatPercent(currentShare, 1) : '-'}</td>
                <td>${Number.isFinite(currentRank) ? `#${formatNumber(currentRank, 0)}` : '-'}</td>
                <td>${formatPercent(row.firstCustomerShare, 1)}</td>
                <td>${formatPercent(row.repurchaseCustomerShare, 1)}</td>
                <td>${formatNumber(row.revenue_90d)}</td>
                <td><span class="retention-status-chip ${row.hasTransition ? 'is-retained' : 'is-muted'}">${row.hasTransition ? '리텐션 발생' : '리텐션 없음'}</span></td>
            </tr>
        `;
    }).join('');
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
            <p class="core-demand-tab-helper">${escapeHtml(CORE_GRAVITY_CONFIG[currentSortKey].helper)}</p>
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
    const currentSortLabel = resolvedModel.currentSortLabel;
    const currentSet = resolvedModel.sets[currentSortKey];
    const scopeLabel = resolvedModel.scopeMode === 'retention-emphasis' ? '현재 화면: 리텐션 발생 상품 강조' : '현재 화면: 전체 상품 보기';
    const rows = buildCoreDemandRowsHtml(resolvedModel.rows, focusEntityId, emphasisMode, currentSortKey);

    summaryCard.innerHTML = `
        <div class="core-demand-wrap">
            <div class="core-demand-head">
                <div>
                    <h3>최근 90일 핵심 수요 상품</h3>
                    <p>최근 1년 내 판매 이력이 있고, 최근 90일에도 실제 판매가 이어진 핵심 수요 상품을 보여줘요.</p>
                    <div class="core-demand-summary">
                        <span>${escapeHtml(currentSortLabel)} 기준 80% 핵심 상품 ${formatNumber(currentSet.items.length, 0)}개</span>
                        <span>4개 축 전체 핵심 상품 ${formatNumber(resolvedModel.totalCoreCount, 0)}개</span>
                        ${resolvedModel.sharedCoreCount > 0 ? `<span>여러 축 핵심 상품 ${formatNumber(resolvedModel.sharedCoreCount, 0)}개</span>` : ''}
                    </div>
                </div>
                <span class="demand-driver-scope">${scopeLabel}</span>
            </div>
            <div class="core-demand-controls">
                ${renderCoreDemandSortTabs(currentSortKey)}
                ${renderSearchUI('products', '핵심 상품 검색')}
            </div>
            <div class="table-container">
                <table class="data-table core-demand-table">
                    <thead><tr>
                        <th>상품명</th>
                        <th>핵심 축</th>
                        <th>${escapeHtml(currentSortLabel)} 비중</th>
                        <th>${escapeHtml(currentSortLabel)} 순위</th>
                        <th>첫구매 고객 비중</th>
                        <th>재구매 고객 비중</th>
                        <th>90일 매출</th>
                        <th>리텐션 상태</th>
                    </tr></thead>
                    <tbody>${rows || `<tr><td colspan="8" class="core-demand-empty">지금 범위에서는 표시할 핵심 상품이 없어요.</td></tr>`}</tbody>
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
    if (!['retention-emphasis', 'all'].includes(qState.scope)) qState.scope = 'retention-emphasis';
    if (!['focus', 'raw'].includes(qState.scaleMode)) qState.scaleMode = 'focus';

    const quadrantModel = buildQuadrantModel(
        getScopedProductsData(),
        qState.selectedId,
        qState.scaleMode || 'focus',
        qState.scope || 'retention-emphasis',
        'both'
    );
    const coreDemandModel = buildCoreDemandTableModel();
    if (quadrantModel) {
        qState.selectedId = quadrantModel.selected.id;
    }

    container.innerHTML = `
        ${renderProductQuadrant(quadrantModel, coreDemandModel)}
        <div id="products-summary-card" class="card animate-fade-in"></div>
    `;
    applyFriendlyUi(container);

    renderQuadrantChart(quadrantModel);
    renderProductsTableOnly(coreDemandModel);

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

    window.focusQuadrantFromTable = (entityId) => {
        const targetId = String(entityId || '').trim();
        if (!targetId) return;
        if (typeof window.selectQuadrantItem === 'function') {
            window.selectQuadrantItem(targetId);
        }
        setTimeout(() => {
            document.querySelector('.pgm-quadrant-wrap')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }, 0);
    };

    window.focusQuadrantFromDemandDriver = (entityId) => {
        const targetId = String(entityId || '').trim();
        if (!targetId) return;
        if (typeof window.selectQuadrantItem === 'function') {
            window.selectQuadrantItem(targetId);
        }
    };

    window.setProductsCoreSortKey = (key) => {
        AppState.viewState.products.coreSortKey = normalizeCoreSortKey(key);
        renderProductsTableOnly();
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
        const next = String(mode || '').toLowerCase() === 'all' ? 'all' : 'retention-emphasis';
        AppState.viewState.products.quadrant.scope = next;
        renderProducts();
    };

}
