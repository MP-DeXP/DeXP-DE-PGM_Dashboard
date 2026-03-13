// Insights page logic

function renderMissingSection(title, desc) {
    return `
        <section class="insight-section card animate-fade-in">
            <h3>${title}</h3>
            <p class="empty-state">${desc}</p>
        </section>
    `;
}

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
            title: '대량 유입형 제품의 재구매 강화',
            action: '첫구매 유입 후 7일 이내 단골의 시작점 제품으로 이어지도록 CRM/리타게팅을 우선 배치합니다.',
            impact: '재구매 도달률 개선 및 유입 낭비 축소',
            evidence: `${TERM_LABELS.AA}-${AA_TYPE_LABELS.BROAD} 비중 ${formatPercent(m.aa_broad_ratio, 1)} / 90일 재구매 도달률 ${formatPercent(m.pca_transition_90d_rate, 1)}`
        });
    }

    if (m.transition_top3_share > 0.65) {
        cards.push({
            domain: 'marketing',
            priority: 2,
            title: '전이 경로 과집중 완화 실험',
            action: '상위 재구매 제품 편중 경로를 유지하되 대체 제품 노출 A/B 테스트를 병행합니다.',
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
            action: '상위 조합 제품을 고정 번들로 구성하고 교차추천 슬롯을 상단에 고정합니다.',
            impact: '장바구니 확장 확장률 향상',
            evidence: `최대 top1_share ${formatPercent(m.ca_pair_top1_share_max, 1)}`
        });
    }

    if (m.ca_set_breadth_lift_avg > 1.5) {
        cards.push({
            domain: 'md',
            priority: 2,
            title: '장바구니 세트형 랜딩 강화',
            action: '세트형 제품군을 랜딩/기획전으로 분리하고 구성 SKU 재고 안정성을 우선 확보합니다.',
            impact: 'AOV 상승 및 이탈 감소',
            evidence: `${TERM_LABELS.CA}-${CA_TYPE_LABELS.SET} 평균 카테고리 확장 지수 ${formatNumber(m.ca_set_breadth_lift_avg, 2)}`
        });
    }

    if (m.pca_scale_concentration > 0.55) {
        cards.push({
            domain: 'md',
            priority: 1,
            title: '효자 제품 재고 방어',
            action: '효자 제품군의 안전재고 기준을 상향하고 품절 알림 자동화를 적용합니다.',
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
        action: '재구매 제품 노출과 핵심 재고 방어를 최우선으로 전환하세요.',
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
        return renderMissingSection('90일 리텐션 흐름', `${REQUIRED_FILES.aaTransitionPath.filename} 데이터가 없어 첫 구매 후 90일 리텐션 흐름을 표시할 수 없습니다.`);
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
                <h2>90일 리텐션 흐름</h2>
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
            <p class="chart-hint">차트 라벨은 제품명 기준이며, 마우스를 올리면 전체 제품명과 ID를 확인할 수 있습니다.</p>
            <div class="card chart-card"><canvas id="transitionChart"></canvas></div>
            <div class="table-container" style="margin-top:1rem;">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>첫구매 유입 제품</th>
                            <th>재구매 제품</th>
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
            <h4>선택한 첫구매 유입 제품 기준 장바구니 확장</h4>
            <p><strong title="${escapeHtml(getProductName(model.selectedCa.product_id))}">${escapeHtml(truncateText(getProductName(model.selectedCa.product_id), 42))}</strong> (${escapeHtml(model.selectedCa.product_id)})</p>
            <div class="selected-ca-grid">
                <span>확장 유형: ${escapeHtml(toCaTypeLabel(withFallback(model.selectedCa.ca_type, 'None')))}</span>
                <span>동반구매 비율: ${formatPercent(model.selectedCa.attach_rate, 1)}</span>
                <span>중간 장바구니 크기: ${formatNumber(model.selectedCa.median_cart_size, 2)}</span>
                <span>상위 1개 집중도: ${formatPercent(model.selectedCa.top1_share, 1)}</span>
            </div>
        </div>
        `
        : '<div class="selected-ca-panel"><h4>선택한 첫구매 유입 제품 기준 장바구니 확장</h4><p>첫구매 유입 제품 필터를 선택하면 해당 제품의 장바구니 확장 신호를 표시합니다.</p></div>';

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
                            <th>제품</th>
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
        const label = id === 'ALL' ? '전체 Entry 제품' : `${getProductName(id)} (${id})`;
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
                        <span>Entry 제품</span>
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
