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
        .replace(/\bproduct\b/gi, '상품')
        .replace(/\bbrand\b/gi, '브랜드')
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
                    <h3>함께 볼 보조 신호</h3>
                    <p>선택한 상품이나 점검 항목을 볼 때 같이 확인하면 좋은 이동·복귀 흐름입니다.</p>
                </div>
            </div>
            <div class="ops-support-grid">
                <div class="ops-support-card">
                    <h4>이동 신호가 큰 흐름</h4>
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
                    <h4>복귀 신호가 큰 상품</h4>
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
                        <h3>함께 볼 점검 포인트</h3>
                        <p>지금은 따로 덧붙여 볼 항목이 없습니다.</p>
                    </div>
                </div>
                <div class="ops-empty">
                    <strong>표시할 점검 포인트가 없습니다.</strong>
                </div>
            </section>
        `
        : `
            <section class="ops-panel ops-section">
                <div class="ops-section-head">
                    <div>
                        <h3>함께 볼 점검 포인트</h3>
                        <p>선택한 상품과 구조 변화를 읽을 때 같이 확인하면 좋은 보조 판단 항목입니다.</p>
                    </div>
                </div>
                <div class="ops-priority-list">
                    ${rows.map((row) => `
                        <article class="ops-priority-item is-${row.priority}">
                            <div class="ops-priority-head">
                                <div>
                                    <strong class="ops-priority-title">${cleanDisplayText(row.label)}</strong>
                                    <p class="ops-priority-meta">${cleanDisplayText(`우선도 ${row.priority} · ${row.entity_type} · ${row.entity_id}`)}</p>
                                </div>
                                <span class="ops-pill">${cleanDisplayText(`우선도 ${row.priority}`)}</span>
                            </div>
                            <p class="ops-priority-reason">${cleanDisplayText(row.reason)}</p>
                            <p class="ops-priority-evidence"><strong>먼저 볼 내용:</strong> ${cleanDisplayText(row.suggested_check)}</p>
                            <p class="ops-priority-evidence"><strong>이렇게 보입니다:</strong> ${cleanDisplayText(row.evidence)}</p>
                            ${row.rule_source ? `<p class="ops-priority-evidence"><strong>판단 기준:</strong> ${cleanDisplayText(row.rule_source)}</p>` : ''}
                        </article>
                    `).join('')}
                </div>
            </section>
        `;

    return `${prioritySection}${renderPrioritySupport(transitionRows, returnLoopRows)}`;
}
