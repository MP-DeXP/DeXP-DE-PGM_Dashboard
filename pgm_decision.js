const PGM_DECISION_PURPOSES = [
    { key: 'entry-growth', label: '신규 유입 / 첫 구매 확대', shortLabel: '신규 유입' },
    { key: 'next-purchase', label: '다음 구매 연결 강화', shortLabel: '다음 구매' },
    { key: 'basket-expansion', label: '함께 담기 확장', shortLabel: '함께 담기' },
    { key: 'return-strength', label: '다시 찾는 구매 강화', shortLabel: '재방문 구매' }
];

const PGM_DECISION_SCOPE_ORDER = {
    broad_rollout: 4,
    limited_rollout: 3,
    small_test: 2,
    not_recommended: 1
};

const PGM_DECISION_LEVEL_ORDER = {
    high: 3,
    medium: 2,
    low: 1
};

const PGM_DECISION_SCOPE_LABELS = {
    broad_rollout: '넓은 적용 검토',
    limited_rollout: '제한 적용 검토',
    small_test: '작은 실험 검토',
    not_recommended: '현재 검토 제외'
};

const PGM_DECISION_STATUS_LABELS = {
    operational_candidate: '운영 검토 후보',
    testable: '실험 검토 후보',
    hypothesis_only: '가설 수준',
    insufficient: '근거 부족',
    not_applicable: '비대상'
};

const PGM_DECISION_OPERATIONAL_SAFETY_LABELS = {
    safe: '운영 안전',
    guarded: '주의 필요',
    fragile: '취약'
};

const PGM_DECISION_REQUIRED_COLUMNS = [
    'product_id',
    'product_name_latest',
    'purpose_key',
    'effect_status',
    'effect_strength',
    'effect_confidence',
    'effect_scope',
    'execution_readiness',
    'operational_safety',
    'effect_precondition_flag',
    'effect_risk_flag'
];

function pgmDecisionState() {
    if (!AppState.viewState.pgmDecision) {
        AppState.viewState.pgmDecision = { selectedPurposeKey: PGM_DECISION_PURPOSES[0].key };
    }
    if (!AppState.viewState.pgmDecision.selectedPurposeKey) {
        AppState.viewState.pgmDecision.selectedPurposeKey = PGM_DECISION_PURPOSES[0].key;
    }
    return AppState.viewState.pgmDecision;
}

function pgmDecisionEscape(value) {
    return typeof escapeHtml === 'function' ? escapeHtml(value) : String(value ?? '');
}

function pgmDecisionNormalizeKey(value) {
    return String(value || '').trim();
}

function pgmDecisionBoolean(value) {
    const normalized = String(value ?? '').trim().toLowerCase();
    return ['true', '1', 'yes', 'y'].includes(normalized);
}

function pgmDecisionRows() {
    const source = AppState?.data?.productPurposeEffects || AppState?.rawData?.productPurposeEffects || [];
    return (source || []).map((row) => {
        const normalized = typeof normalizeCsvRows === 'function' ? normalizeCsvRows([row])[0] : row;
        return {
            snapshotName: pgmDecisionNormalizeKey(normalized.snapshot_name),
            productId: pgmDecisionNormalizeKey(normalized.product_id),
            productName: pgmDecisionNormalizeKey(normalized.product_name_latest) || pgmDecisionNormalizeKey(normalized.product_name) || pgmDecisionNormalizeKey(normalized.product_id),
            purposeKey: pgmDecisionNormalizeKey(normalized.purpose_key),
            effectStatus: pgmDecisionNormalizeKey(normalized.effect_status),
            effectStrength: pgmDecisionNormalizeKey(normalized.effect_strength),
            effectConfidence: pgmDecisionNormalizeKey(normalized.effect_confidence),
            effectScope: pgmDecisionNormalizeKey(normalized.effect_scope),
            executionReadiness: pgmDecisionNormalizeKey(normalized.execution_readiness),
            operationalSafety: pgmDecisionNormalizeKey(normalized.operational_safety),
            preconditionFlag: pgmDecisionBoolean(normalized.effect_precondition_flag),
            riskFlag: pgmDecisionBoolean(normalized.effect_risk_flag),
            primaryMetric: pgmDecisionNormalizeKey(normalized.effect_primary_metric),
            primaryMetricValue: normalized.effect_primary_metric_value,
            secondaryMetric: pgmDecisionNormalizeKey(normalized.effect_secondary_metric),
            secondaryMetricValue: normalized.effect_secondary_metric_value,
            relationSummary: pgmDecisionNormalizeKey(normalized.relation_summary_ko),
            relationRiskSummary: pgmDecisionNormalizeKey(normalized.relation_risk_summary_ko)
        };
    }).filter((row) => row.productId && row.purposeKey);
}

function pgmDecisionSchemaStatus(rawRows) {
    if (!rawRows.length) return { ok: true, missing: [] };
    const sample = typeof normalizeCsvRows === 'function' ? normalizeCsvRows([rawRows[0]])[0] : rawRows[0];
    const missing = PGM_DECISION_REQUIRED_COLUMNS.filter((key) => !Object.prototype.hasOwnProperty.call(sample || {}, key));
    return { ok: missing.length === 0, missing };
}

function pgmDecisionCompare(a, b) {
    const scopeDiff = (PGM_DECISION_SCOPE_ORDER[b.effectScope] || 0) - (PGM_DECISION_SCOPE_ORDER[a.effectScope] || 0);
    if (scopeDiff) return scopeDiff;
    const confidenceDiff = (PGM_DECISION_LEVEL_ORDER[b.effectConfidence] || 0) - (PGM_DECISION_LEVEL_ORDER[a.effectConfidence] || 0);
    if (confidenceDiff) return confidenceDiff;
    const strengthDiff = (PGM_DECISION_LEVEL_ORDER[b.effectStrength] || 0) - (PGM_DECISION_LEVEL_ORDER[a.effectStrength] || 0);
    if (strengthDiff) return strengthDiff;
    return String(a.productName || a.productId).localeCompare(String(b.productName || b.productId), 'ko');
}

function pgmDecisionIsEligible(row) {
    return !['not_applicable', 'insufficient'].includes(row.effectStatus)
        && row.executionReadiness !== 'not_ready';
}

function pgmDecisionIsCautionHeavy(row) {
    return row.operationalSafety === 'fragile' || row.riskFlag || row.preconditionFlag;
}

function pgmDecisionPurposeMeta(key) {
    return PGM_DECISION_PURPOSES.find((purpose) => purpose.key === key) || { key, label: key, shortLabel: key };
}

function pgmDecisionMetricText(metric, value) {
    if (!metric) return '-';
    const text = value === null || typeof value === 'undefined' || value === ''
        ? metric
        : `${metric} ${typeof formatNumber === 'function' && Number.isFinite(Number(value)) ? formatNumber(Number(value), Number(value) % 1 === 0 ? 0 : 2) : value}`;
    return text;
}

function pgmDecisionBuildModel() {
    const state = pgmDecisionState();
    const rawRows = AppState?.rawData?.productPurposeEffects || AppState?.data?.productPurposeEffects || [];
    const schema = pgmDecisionSchemaStatus(rawRows || []);
    const rows = schema.ok ? pgmDecisionRows() : [];
    const purposeOptions = PGM_DECISION_PURPOSES.filter((purpose) => rows.some((row) => row.purposeKey === purpose.key));
    if (!purposeOptions.some((purpose) => purpose.key === state.selectedPurposeKey)) {
        state.selectedPurposeKey = (purposeOptions[0] || PGM_DECISION_PURPOSES[0]).key;
    }
    const purposeRows = rows.filter((row) => row.purposeKey === state.selectedPurposeKey);
    const eligible = purposeRows.filter(pgmDecisionIsEligible).sort(pgmDecisionCompare);
    const topCandidates = eligible.slice(0, 3);
    const top = topCandidates[0] || null;
    const second = topCandidates[1] || null;
    const clearGap = Boolean(top && (!second || top.effectScope !== second.effectScope || (top.effectScope === second.effectScope && (PGM_DECISION_LEVEL_ORDER[top.effectConfidence] || 0) > (PGM_DECISION_LEVEL_ORDER[second.effectConfidence] || 0))));
    const focusSelected = Boolean(top
        && top.operationalSafety !== 'fragile'
        && ['broad_rollout', 'limited_rollout'].includes(top.effectScope)
        && clearGap);
    const decisionType = top ? (focusSelected ? 'focus_selected' : 'comparison_hold') : 'no_candidate';
    const alternatives = focusSelected ? topCandidates.slice(1, 3) : topCandidates.slice(0, 3);
    const snapshotName = rows.find((row) => row.snapshotName)?.snapshotName || '';
    return { state, schema, rows, purposeRows, purposeOptions, eligible, topCandidates, top, second, focusSelected, decisionType, alternatives, snapshotName };
}

function pgmDecisionEvidenceItems(model) {
    const top = model.top;
    if (!top) return [];
    return [
        {
            title: '검토 범위',
            value: PGM_DECISION_SCOPE_LABELS[top.effectScope] || top.effectScope || '-',
            desc: '가장 보수적인 우선순위 기준이에요.'
        },
        {
            title: '근거 수준',
            value: top.effectConfidence || '-',
            desc: model.second
                ? `2순위 대비 ${top.effectScope !== model.second.effectScope ? '범위 차이' : '근거 수준 우위'}가 있는지 봤어요.`
                : '비교 대상이 적어 단독으로 판단했어요.'
        },
        {
            title: '주의 신호',
            value: pgmDecisionIsCautionHeavy(top) ? '주의 있음' : '주의 낮음',
            desc: `운영 안전성 ${PGM_DECISION_OPERATIONAL_SAFETY_LABELS[top.operationalSafety] || top.operationalSafety || '-'}${top.preconditionFlag || top.riskFlag ? ' · 전제/리스크 포함' : ''}`
        }
    ];
}

function pgmDecisionSummaryText(model) {
    const purposeLabel = pgmDecisionPurposeMeta(model.state.selectedPurposeKey).shortLabel;
    if (!model.top) return `${purposeLabel} 목적에서는 지금 바로 집중할 상품을 고르기보다 추가 근거 확보가 먼저예요.`;
    if (model.focusSelected) {
        return `${model.top.productName} 중심으로 메시지, 예산, 채널 실행을 먼저 정리해도 되는 날이에요. 다만 ${model.top.relationRiskSummary || '운영 제약은 계속 확인하세요.'}`;
    }
    return `${purposeLabel} 목적에서는 아직 한 상품으로 몰지 말고 상위 ${model.alternatives.length}개 후보를 나란히 비교하는 편이 안전해요. 범위와 근거 차이는 작고, 주의 신호를 함께 봐야 해요.`;
}

function pgmDecisionMetaLine(row) {
    const parts = [];
    if (row.effectScope) parts.push(PGM_DECISION_SCOPE_LABELS[row.effectScope] || row.effectScope);
    if (row.effectConfidence) parts.push(`근거 ${row.effectConfidence}`);
    return parts.join(' · ');
}

function pgmDecisionBuildPros(row) {
    const items = [];
    if (row.effectScope && row.effectScope !== 'not_recommended') {
        items.push(PGM_DECISION_SCOPE_LABELS[row.effectScope] || row.effectScope);
    }
    if (row.effectConfidence && items.length < 2) {
        items.push(`근거 ${row.effectConfidence}`);
    }
    if (row.effectStrength && items.length < 2) {
        items.push(`효과 강도 ${row.effectStrength}`);
    }
    if (row.executionReadiness && row.executionReadiness !== 'not_ready' && items.length < 2) {
        items.push(`실행 준비 ${row.executionReadiness}`);
    }
    if (row.relationSummary && items.length < 2) {
        items.push(row.relationSummary);
    }
    return items.filter(Boolean).slice(0, 2);
}

function pgmDecisionBuildCons(row) {
    const items = [];
    if (row.operationalSafety && row.operationalSafety !== 'safe') {
        items.push(PGM_DECISION_OPERATIONAL_SAFETY_LABELS[row.operationalSafety] || row.operationalSafety);
    }
    if (row.preconditionFlag && items.length < 2) {
        items.push('선행 조건 확인');
    }
    if (row.riskFlag && items.length < 2) {
        items.push('리스크 신호 있음');
    }
    if (row.relationRiskSummary && items.length < 2) {
        items.push(row.relationRiskSummary);
    }
    return items.filter(Boolean).slice(0, 2);
}

function pgmDecisionCandidateRows(model) {
    if (!model.top) return [];
    return model.focusSelected
        ? [model.top, ...(model.alternatives || [])].slice(0, 3)
        : (model.alternatives || []).slice(0, 3);
}

function pgmDecisionCandidateBlock(row) {
    const pros = pgmDecisionBuildPros(row);
    const cons = pgmDecisionBuildCons(row);
    const metaLine = pgmDecisionMetaLine(row);
    return `
        <article class="pgm-decision-candidate-block">
            <div class="pgm-decision-candidate-head">
                <strong>${pgmDecisionEscape(row.productName)}</strong>
                ${metaLine ? `<span>${pgmDecisionEscape(metaLine)}</span>` : ''}
            </div>
            ${pros.length ? `
                <div class="pgm-decision-candidate-group">
                    <span class="pgm-decision-candidate-label">지금 밀 이유</span>
                    <ul class="pgm-decision-candidate-points">
                        ${pros.map((item) => `<li>${pgmDecisionEscape(item)}</li>`).join('')}
                    </ul>
                </div>
            ` : ''}
            ${cons.length ? `
                <div class="pgm-decision-candidate-group">
                    <span class="pgm-decision-candidate-label">망설일 이유</span>
                    <ul class="pgm-decision-candidate-points">
                        ${cons.map((item) => `<li>${pgmDecisionEscape(item)}</li>`).join('')}
                    </ul>
                </div>
            ` : ''}
        </article>
    `;
}

function pgmDecisionDecisionCard(model) {
    if (!model.top) {
        return `<section class="pgm-decision-card is-empty"><h2>오늘 결정</h2><p>조건을 통과한 후보가 아직 없어요. 실행보다 데이터 확인이 먼저예요.</p></section>`;
    }
    const candidateRows = pgmDecisionCandidateRows(model);
    if (model.focusSelected) {
        return `
            <section class="pgm-decision-card is-focus">
                <div class="pgm-decision-card-head">
                    <div>
                        <span class="pgm-decision-eyebrow">오늘 결정</span>
                        <h2>${pgmDecisionEscape(model.top.productName)}</h2>
                        <p>${pgmDecisionEscape(model.top.productId)} · ${pgmDecisionEscape(pgmDecisionPurposeMeta(model.state.selectedPurposeKey).label)}</p>
                    </div>
                    <span class="pgm-decision-badge is-strong">집중 1개</span>
                </div>
                <p class="pgm-decision-main-copy">오늘은 이 상품에 집중해서 마케팅 전략을 세우는 쪽이 가장 보수적으로도 설명돼요.</p>
                <div class="pgm-decision-chip-row">
                    <span class="pgm-decision-chip">${pgmDecisionEscape(PGM_DECISION_SCOPE_LABELS[model.top.effectScope] || model.top.effectScope)}</span>
                    <span class="pgm-decision-chip">근거 ${pgmDecisionEscape(model.top.effectConfidence || '-')}</span>
                    <span class="pgm-decision-chip">강도 ${pgmDecisionEscape(model.top.effectStrength || '-')}</span>
                </div>
                <div class="pgm-decision-candidate-grid">
                    ${candidateRows.map((row) => pgmDecisionCandidateBlock(row)).join('')}
                </div>
            </section>
        `;
    }
    return `
        <section class="pgm-decision-card is-hold">
            <div class="pgm-decision-card-head">
                <div>
                    <span class="pgm-decision-eyebrow">오늘 결정</span>
                    <h2>비교 보류</h2>
                    <p>${pgmDecisionEscape(pgmDecisionPurposeMeta(model.state.selectedPurposeKey).label)} 기준 상위 후보를 더 나란히 봐야 해요.</p>
                </div>
                <span class="pgm-decision-badge is-caution">2~3개 비교</span>
            </div>
            <p class="pgm-decision-main-copy">1위 후보가 있더라도 범위 차이나 근거 우위가 충분히 선명하지 않아서, 바로 한 상품으로 고정하지 않는 편이 안전해요.</p>
            <div class="pgm-decision-candidate-grid">
                ${candidateRows.map((row) => pgmDecisionCandidateBlock(row)).join('')}
            </div>
        </section>
    `;
}

function pgmDecisionEvidenceCards(model) {
    const items = pgmDecisionEvidenceItems(model);
    return `
        <section class="pgm-decision-evidence-grid">
            ${items.map((item) => `
                <article class="pgm-decision-evidence-card">
                    <span>${pgmDecisionEscape(item.title)}</span>
                    <strong>${pgmDecisionEscape(item.value)}</strong>
                    <p>${pgmDecisionEscape(item.desc)}</p>
                </article>
            `).join('')}
        </section>
    `;
}

function pgmDecisionDirectionSection(model) {
    const top = model.top;
    if (!top) {
        return `<section class="pgm-decision-panel"><h3>오늘 마케팅 방향</h3><p>후보 부재로 방향 요약을 만들지 않았어요. 목적별 산출 행과 실행 준비 상태를 먼저 확인하세요.</p></section>`;
    }
    const metric1 = pgmDecisionMetricText(top.primaryMetric, top.primaryMetricValue);
    const metric2 = pgmDecisionMetricText(top.secondaryMetric, top.secondaryMetricValue);
    return `
        <section class="pgm-decision-panel">
            <h3>오늘 마케팅 방향</h3>
            <p>${pgmDecisionEscape(pgmDecisionSummaryText(model))}</p>
            <div class="pgm-decision-direction-list">
                <div><strong>핵심 근거</strong><span>${pgmDecisionEscape(metric1)}</span></div>
                <div><strong>보조 근거</strong><span>${pgmDecisionEscape(metric2)}</span></div>
                <div><strong>관계 해석</strong><span>${pgmDecisionEscape(top.relationSummary || top.relationRiskSummary || '관계 요약 없음')}</span></div>
            </div>
        </section>
    `;
}

function pgmDecisionAlternativesSection(model) {
    const rows = model.alternatives || [];
    return `
        <section class="pgm-decision-panel">
            <div class="pgm-decision-panel-head">
                <div>
                    <h3>대안 후보</h3>
                    <p>최대 2~3개만 유지해 오늘 비교 대상을 작게 잡아요.</p>
                </div>
                <a href="../pgm_usage/" class="pgm-decision-link">pgm_usage에서 자세히 보기</a>
            </div>
            <div class="pgm-decision-alt-list">
                ${rows.length ? rows.map((row) => `
                    <article class="pgm-decision-alt-item">
                        <div>
                            <strong>${pgmDecisionEscape(row.productName)}</strong>
                            <span>${pgmDecisionEscape(row.productId)}</span>
                        </div>
                        <div class="pgm-decision-alt-meta">
                            <span>${pgmDecisionEscape(PGM_DECISION_SCOPE_LABELS[row.effectScope] || row.effectScope)}</span>
                            <span>근거 ${pgmDecisionEscape(row.effectConfidence || '-')}</span>
                            ${pgmDecisionIsCautionHeavy(row) ? '<span>주의 포함</span>' : '<span>주의 낮음</span>'}
                        </div>
                    </article>
                `).join('') : '<p class="pgm-decision-muted">표시할 대안 후보가 없어요.</p>'}
            </div>
        </section>
    `;
}

function pgmDecisionPurposeBar(model) {
    return `
        <section class="pgm-decision-purpose-bar">
            ${PGM_DECISION_PURPOSES.map((purpose) => `
                <button
                    type="button"
                    class="pgm-decision-purpose-btn ${model.state.selectedPurposeKey === purpose.key ? 'is-active' : ''}"
                    onclick="selectPgmDecisionPurpose('${purpose.key}')"
                >
                    <strong>${pgmDecisionEscape(purpose.shortLabel)}</strong>
                    <span>${pgmDecisionEscape(purpose.label)}</span>
                </button>
            `).join('')}
        </section>
    `;
}

function pgmDecisionRenderSchemaError(missing) {
    return `
        <div class="pgm-decision-state-card is-error">
            <h2>스키마 오류</h2>
            <p>pgm_product_purpose_effects.csv에 필요한 컬럼이 빠져 있어요.</p>
            <p>누락 컬럼: <strong>${pgmDecisionEscape(missing.join(', '))}</strong></p>
        </div>
    `;
}

function pgmDecisionRenderEmpty() {
    return `
        <div class="pgm-decision-state-card">
            <h2>데이터 없음</h2>
            <p>pgm_product_purpose_effects.csv를 업로드하면 오늘 집중 상품 결정을 볼 수 있어요.</p>
            <button class="btn-primary" type="button" onclick="showUploadModal()">CSV 업로드</button>
        </div>
    `;
}

function renderPgmDecision() {
    const container = document.getElementById('content-area');
    if (!container) return;
    const model = pgmDecisionBuildModel();
    if (!model.schema.ok) {
        container.innerHTML = pgmDecisionRenderSchemaError(model.schema.missing);
        if (typeof applyFriendlyUi === 'function') applyFriendlyUi(container);
        return;
    }
    if (!model.rows.length) {
        container.innerHTML = pgmDecisionRenderEmpty();
        if (typeof applyFriendlyUi === 'function') applyFriendlyUi(container);
        return;
    }
    container.innerHTML = `
        <div class="pgm-decision-page">
            <div class="pgm-decision-header-meta">
                <span>질문: 오늘 내 목적 하에 무슨 상품에 집중할까</span>
                <span>${pgmDecisionEscape(model.snapshotName || '스냅샷 없음')}</span>
                <span>대상 ${pgmDecisionEscape(String(model.purposeRows.length))}개 행</span>
            </div>
            ${pgmDecisionPurposeBar(model)}
            ${pgmDecisionDecisionCard(model)}
            ${model.top ? pgmDecisionEvidenceCards(model) : ''}
            ${pgmDecisionDirectionSection(model)}
            ${pgmDecisionAlternativesSection(model)}
        </div>
    `;
    if (typeof applyFriendlyUi === 'function') applyFriendlyUi(container);
}

window.selectPgmDecisionPurpose = (purposeKey) => {
    const state = pgmDecisionState();
    state.selectedPurposeKey = String(purposeKey || '').trim() || PGM_DECISION_PURPOSES[0].key;
    renderPgmDecision();
};
