function cleanDisplayText(value) {
    const text = String(value ?? '').trim();
    if (!text) {
        return '데이터 없음';
    }

    return text
        .replace(/same[-_ ]date snapshot/gi, '기준일 스냅샷')
        .replace(/same[-_ ]date role snapshot/gi, '기준일 스냅샷')
        .replace(/same-date role state/gi, '기준일 관측 상태')
        .replace(/role-state/gi, '상태')
        .replace(/\bproduct\b/gi, '상품')
        .replace(/\bbrand\b/gi, '브랜드')
        .replace(/runtime mode/gi, '상태')
        .replace(/artifact-backed/gi, '실데이터 연결')
        .replace(/sample fallback/gi, '예시 데이터')
        .replace(/latest role fallback/gi, '보정')
        .replace(/\bblank\b/gi, '관측 상태 없음')
        .replace(/PGM 미관측/gi, '관측 상태 없음')
        .replace(/Deferred:/gi, '')
        .replace(/\s+/g, ' ')
        .trim();
}

export function renderPriorityPage(rows = [], transitionRows = [], returnLoopRows = []) {
    const supportCount = rows.length + transitionRows.length + returnLoopRows.length;

    return `
        <section class="ops-panel ops-section pgm-product-table-card card">
            <div class="pgm-product-table-top">
                <div>
                    <h3>우선 점검 block</h3>
                    <p class="chart-hint">priority는 독립 대형 섹션이 아니라 overview의 "지금 먼저 볼 점검" block을 다시 여는 보조 진입점입니다.</p>
                </div>
                <span class="ops-pill badge">${cleanDisplayText(`${supportCount}개 보조 신호`)}</span>
            </div>
            <p class="insight-note">핵심 읽기 흐름은 overview에서 기간 선택과 매출 요약, 역할별 기여 변화, compact 점검 block, 우측 pgm-side drilldown 순서로 이어집니다.</p>
        </section>
    `;
}
