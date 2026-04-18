import { renderCards, renderKpiStack } from '../components/cards.js';

export function renderOverviewPage(cards, latestCardDate, meta) {
    const summaryRows = [
        {
            label: '데이터 기준일',
            value: latestCardDate ?? 'n/a',
            delta: null
        },
        {
            label: '로딩 소스',
            value: meta.sourceLabel,
            delta: null
        },
        {
            label: '런타임 모드',
            value: meta.mode,
            delta: null
        }
    ];
    const loadDetails = [
        meta.fallbackKeys?.length ? `sample fallback 파일: ${meta.fallbackKeys.join(', ')}` : null,
        meta.emptyArtifactKeys?.length ? `빈 artifact 유지: ${meta.emptyArtifactKeys.join(', ')}` : null,
        'Deferred: BHI primary metric, member/UTM drill-down, transition/return-loop experience'
    ].filter(Boolean);

    return `
        <section class="ops-grid hero">
            <article class="ops-panel hero">
                <div class="ops-hero-head">
                    <div>
                        <h2>운영 리듬별 상태판</h2>
                        <p>Daily / Weekly / Monthly 카드는 artifact-backed일 때만 현재 구현 결과를 의미합니다. sample fallback 값은 화면 동작 확인용 예시이며 운영 판단 근거가 아닙니다.</p>
                    </div>
                </div>
                ${renderCards(cards)}
            </article>
            <aside class="ops-panel side">
                <div class="ops-section-head">
                    <div>
                        <h2>운영 메모</h2>
                        <p>이 앱은 raw/silver/gold를 직접 조립하지 않고 `view_model/*.csv`만 읽습니다. same-date role state blank는 blank 그대로 두고, UI에서만 `PGM 미관측`으로 라벨링합니다.</p>
                    </div>
                </div>
                ${renderKpiStack(summaryRows)}
                <div class="ops-note">${meta.note}</div>
                <div class="ops-note">
                    ${loadDetails.map((detail) => `<div>${detail}</div>`).join('')}
                </div>
            </aside>
        </section>
    `;
}
