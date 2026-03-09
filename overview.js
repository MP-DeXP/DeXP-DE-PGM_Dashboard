// Overview page logic

function renderOverview() {
    destroyCarts();
    const container = document.getElementById('content-area');
    const data = AppState.data.brandScore ? AppState.data.brandScore[0] : {};

    const bhiRaw = data.Brand_Health_Index || data.BHI || data.brand_health_index || data.Brand_Health_Score;
    const bhi = (bhiRaw !== undefined && bhiRaw !== null && !Number.isNaN(toNumber(bhiRaw, NaN)))
        ? (toNumber(bhiRaw) * 100).toFixed(2)
        : '-';

    const concentration = data.AA_Concentration_Index ? `${(toNumber(data.AA_Concentration_Index) * 100).toFixed(1)}%` : '-';
    const balance = data.Chain_Balance_Index ? formatNumber(data.Chain_Balance_Index, 2) : '-';
    const confidence = data.Confidence_Index || '-';

    container.innerHTML = `
        <div class="animate-fade-in" style="margin-bottom: 2rem;">
            <div class="card" style="text-align: center; background: linear-gradient(135deg, white 0%, var(--primary-light) 100%); border: 1px solid var(--primary); border-width: 2px;">
                <h3 style="color: var(--primary); font-size: 1rem; margin-bottom: 0.5rem; font-weight: 700;">브랜드 구조 건강도</h3>
                <div class="value" style="font-size: 4.5rem; color: var(--primary);">${bhi}</div>
            </div>
        </div>

        <div class="stats-grid animate-fade-in">
            <div class="card">
                <h3>축 1: 첫구매 유입 집중도</h3>
                <div class="value">${concentration}</div>
            </div>
            <div class="card">
                <h3>축 2: 재구매 사슬 균형</h3>
                <div class="value">${balance}</div>
            </div>
            <div class="card">
                <h3>축 3: 신뢰도</h3>
                <div class="value" style="color: var(--accent);">${confidence}</div>
            </div>
        </div>

        <div class="card animate-fade-in" style="margin-top: 2rem; border-left: 4px solid var(--primary);">
            <h3 style="color: var(--primary); text-transform: none; letter-spacing: normal; font-size: 1.1rem;">화면 해석 가이드</h3>
            <p style="color: var(--text-muted); margin-top: 1rem; line-height: 1.6; font-size: 0.95rem;">
                브랜드 구조 건강도는 유입 균형, 재구매 균형, 가치 준비도를 함께 보여줘요. 매출 규모보다 구조 균형이 유지되는지 먼저 보면 좋아요.
            </p>
        </div>
    `;
    applyFriendlyUi(container);
}
