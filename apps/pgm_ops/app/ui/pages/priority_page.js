function cleanDisplayText(value) {
    const text = String(value ?? '').trim();
    if (!text) {
        return '데이터 없음';
    }

    return text
        .replace(/same-date snapshot/gi, '당일 기준')
        .replace(/same-date role snapshot/gi, '당일 기준')
        .replace(/same-date role state/gi, '당일 상태')
        .replace(/role-state/gi, '상태')
        .replace(/runtime mode/gi, '상태')
        .replace(/artifact-backed/gi, '실데이터 연결')
        .replace(/sample fallback/gi, '예시 데이터')
        .replace(/latest role fallback/gi, '보정')
        .replace(/\bblank\b/gi, '데이터 없음')
        .replace(/PGM 미관측/gi, '상태 미확인')
        .replace(/Deferred:/gi, '')
        .replace(/\s+/g, ' ')
        .trim();
}

function formatPercent(value) {
    if (value == null || value === '') {
        return '데이터 없음';
    }

    const numeric = Number(value);
    if (Number.isNaN(numeric)) {
        return cleanDisplayText(value);
    }

    return `${(numeric * 100).toFixed(1)}%`;
}

function renderPrioritySupport(transitionRows, returnLoopRows) {
    const topTransitions = transitionRows.slice(0, 3);
    const topReturnLoops = returnLoopRows.slice(0, 3);

    return `
        <section class="ops-panel ops-section">
            <div class="ops-section-head">
                <div>
                    <h3>보조 근거</h3>
                    <p>선택한 우선 점검 항목을 이동 신호와 복귀 흐름으로 다시 확인합니다.</p>
                </div>
            </div>
            <div class="ops-support-grid">
                <div class="ops-support-card">
                    <h4>이동이 많은 상품</h4>
                    <div class="ops-support-list">
                        ${topTransitions.length ? topTransitions.map((row) => `
                            <article class="ops-support-item">
                                <strong>${cleanDisplayText(`${row.product_name} → ${row.target_product_name}`)}</strong>
                                <p>${cleanDisplayText(`전환율 ${formatPercent(row.transition_rate)} · 평균 ${Number(row.avg_days_to_transition ?? 0).toFixed(1)}일`)}</p>
                            </article>
                        `).join('') : '<div class="ops-empty compact"><strong>표시할 이동 신호가 없습니다.</strong></div>'}
                    </div>
                </div>
                <div class="ops-support-card">
                    <h4>복귀가 잦은 상품</h4>
                    <div class="ops-support-list">
                        ${topReturnLoops.length ? topReturnLoops.map((row) => `
                            <article class="ops-support-item">
                                <strong>${cleanDisplayText(row.product_name)}</strong>
                                <p>${cleanDisplayText(`복귀율 ${formatPercent(row.qualified_return_rate)} · 루프율 ${formatPercent(row.return_loop_rate)} · 반복율 ${formatPercent(row.simple_repeat_rate)}`)}</p>
                            </article>
                        `).join('') : '<div class="ops-empty compact"><strong>표시할 복귀 신호가 없습니다.</strong></div>'}
                    </div>
                </div>
            </div>
        </section>
    `;
}

export function renderPriorityPage(rows, transitionRows = [], returnLoopRows = []) {
    const prioritySection = !rows.length
        ? `
            <section class="ops-panel ops-section">
                <div class="ops-section-head">
                    <div>
                        <h3>우선 점검 대상</h3>
                        <p>지금은 앞에서 볼 항목이 없습니다.</p>
                    </div>
                </div>
                <div class="ops-empty">
                    <strong>표시할 우선 점검 항목이 없습니다.</strong>
                </div>
            </section>
        `
        : `
            <section class="ops-panel ops-section">
                <div class="ops-section-head">
                    <div>
                        <h3>우선 점검 대상</h3>
                        <p>매출 구조와 역할 신호를 함께 보고 먼저 확인할 항목을 좁힌 목록입니다.</p>
                    </div>
                </div>
                <div class="ops-priority-list">
                    ${rows.map((row) => `
                        <article class="ops-priority-item is-${row.priority}">
                            <div class="ops-priority-head">
                                <div>
                                    <strong class="ops-priority-title">${cleanDisplayText(row.label)}</strong>
                                    <p class="ops-priority-meta">${cleanDisplayText(`${row.priority.toUpperCase()} · ${row.entity_type} · ${row.entity_id}`)}</p>
                                </div>
                                <span class="ops-pill">${cleanDisplayText(row.priority)}</span>
                            </div>
                            <p class="ops-priority-reason">${cleanDisplayText(row.reason)}</p>
                            <p class="ops-priority-evidence"><strong>확인 포인트:</strong> ${cleanDisplayText(row.suggested_check)}</p>
                            <p class="ops-priority-evidence"><strong>근거:</strong> ${cleanDisplayText(row.evidence)}</p>
                            ${row.rule_source ? `<p class="ops-priority-evidence"><strong>규칙 출처:</strong> ${cleanDisplayText(row.rule_source)}</p>` : ''}
                        </article>
                    `).join('')}
                </div>
            </section>
        `;

    return `${prioritySection}${renderPrioritySupport(transitionRows, returnLoopRows)}`;
}
