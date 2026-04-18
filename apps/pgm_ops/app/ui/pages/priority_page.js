export function renderPriorityPage(rows) {
    return `
        <section class="ops-panel ops-section">
            <div class="ops-section-head">
                <div>
                    <h3>우선 점검 대상</h3>
                    <p>latest `brand_operating_status_daily`와 `revenue_structure_daily` 기준으로 무엇을 먼저 볼지 좁힌 목록입니다.</p>
                </div>
            </div>
            <div class="ops-priority-list">
                ${rows.map((row) => `
                    <article class="ops-priority-item is-${row.priority}">
                        <div class="ops-priority-head">
                            <div>
                                <strong class="ops-priority-title">${row.label}</strong>
                                <p class="ops-priority-meta">${row.priority.toUpperCase()} · ${row.entity_type} · ${row.entity_id}</p>
                            </div>
                            <span class="ops-pill">${row.priority}</span>
                        </div>
                        <p class="ops-priority-reason">${row.reason}</p>
                        <p class="ops-priority-evidence"><strong>Check:</strong> ${row.suggested_check}</p>
                        <p class="ops-priority-evidence"><strong>Evidence:</strong> ${row.evidence}</p>
                        ${row.rule_source ? `<p class="ops-priority-evidence"><strong>Rule Source:</strong> ${row.rule_source}</p>` : ''}
                    </article>
                `).join('')}
            </div>
        </section>
    `;
}
