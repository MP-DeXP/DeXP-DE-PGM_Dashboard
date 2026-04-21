const PERIODS = {
    q1: 'Q1 매출·주문 기준 2026.01.01 ~ 2026.03.31',
    march: '3월 매출 기준 2026.03.01 ~ 2026.03.31',
    latest: '최신 가용 관계·참여 스냅샷 2026.04.08 ~ 2026.04.20',
    extracted: '관계 스냅샷은 2026.04.20 추출'
};

const PRODUCT_SALES_Q1 = [
    { sku: '2711', name: '듀오락 골드 하루 한 포 2개 (60일분)', revenue: 226324000, orders: 1408 },
    { sku: '2675', name: '[블프 페스타] 듀오락 골드캡슐 30일분 2개 + (퀘르세틴5일+하루 한 포 10일분)', revenue: 73500000, orders: 649 },
    { sku: '274', name: '듀오락 골드캡슐 1개 (30일분)', revenue: 49400000, orders: 782 },
    { sku: '2674', name: '듀오락 골드세트 1개 (65일분)', revenue: 46410000, orders: 326 },
    { sku: '2720', name: '[블프 페스타] 듀오락 얌얌플러스 2개 (80일분) + 얌얌플러스 10일분 증정', revenue: 38548000, orders: 399 },
    { sku: '2612', name: '듀오락 얌얌 40일분(80정) 2개', revenue: 38400000, orders: 423 },
    { sku: '2620', name: '듀오락 골드캡슐 3개 (90일분)', revenue: 33900000, orders: 206 },
    { sku: '2666', name: '듀오락 듀오 디 드롭스 7.5ml 2개', revenue: 33880000, orders: 473 },
    { sku: '2559', name: '[블프 페스타] 듀오락 바이오가드 2개 (60일분)', revenue: 27296800, orders: 331 },
    { sku: '2477', name: '듀오락 골드세트 2개 (130일분)', revenue: 26000000, orders: 95 },
    { sku: '2688', name: '듀오락 에이티피 2개 (60일분)', revenue: 25440000, orders: 168 },
    { sku: '2693', name: '듀오락 베이비 2개 (60일분)', revenue: 19368000, orders: 230 }
];

const PRODUCT_SALES_MARCH = [
    { sku: '2711', name: '듀오락 골드 하루 한 포 2개 (60일분)', revenue: 54870000, orders: 402 },
    { sku: '2675', name: '[블프 페스타] 듀오락 골드캡슐 30일분 2개 + (퀘르세틴5일+하루 한 포 10일분)', revenue: 17900000, orders: 156 },
    { sku: '274', name: '듀오락 골드캡슐 1개 (30일분)', revenue: 17650000, orders: 281 },
    { sku: '2612', name: '듀오락 얌얌 40일분(80정) 2개', revenue: 12560000, orders: 151 },
    { sku: '2620', name: '듀오락 골드캡슐 3개 (90일분)', revenue: 12000000, orders: 71 },
    { sku: '2720', name: '[블프 페스타] 듀오락 얌얌플러스 2개 (80일분) + 얌얌플러스 10일분 증정', revenue: 10396000, orders: 107 },
    { sku: '2688', name: '듀오락 에이티피 2개 (60일분)', revenue: 8880000, orders: 59 },
    { sku: '2674', name: '듀오락 골드세트 1개 (65일분)', revenue: 8450000, orders: 59 }
];

const RELATION_CORE = [
    { from: '듀오락 골드캡슐 1개 (30일분)', to: '듀오락 골드캡슐 30일분 2개', customers: 129, rate: 5.9, days: 42.7 },
    { from: '[50%체험] 듀오락 골드캡슐 10일', to: '듀오락 골드캡슐 1개', customers: 103, rate: 5.1, days: 26.9 },
    { from: '듀오락 골드 하루 한 포 1개', to: '[집중케어] 듀오락 골드 하루 한 포 2개 +10일', customers: 80, rate: 15.6, days: 38.9 },
    { from: '[50%체험] 듀오락 골드캡슐 10일', to: '듀오락 골드캡슐 30일분 2개', customers: 78, rate: 3.9, days: 35.3 },
    { from: '[50%체험] 듀오락 골드 10일', to: '[집중케어] 듀오락 골드 하루 한 포 2개 +10일', customers: 69, rate: 2.7, days: 26.0 },
    { from: '듀오락 골드캡슐 1개', to: '듀오락 골드캡슐 3개', customers: 56, rate: 2.6, days: 40.1 }
];

const RELATION_LIFECYCLE = [
    { from: '[50%체험] 듀오락 얌얌플러스 10일', to: '듀오락 얌얌플러스 2개', customers: 55, rate: 4.0, days: 25.2 },
    { from: '듀오락 얌얌플러스 1개', to: '듀오락 얌얌플러스 2개', customers: 47, rate: 5.2, days: 38.4 },
    { from: '[50%체험] 듀오락 베이비 10일분', to: '[봄맞이] 듀오락 베이비 2개 + 얌얌플러스 5일분', customers: 25, rate: 3.7, days: 34.3 },
    { from: '듀오락 베이비 1개', to: '[봄맞이] 듀오락 베이비 2개 + 얌얌플러스 5일분', customers: 24, rate: 8.7, days: 40.4 },
    { from: '듀오락 듀오 디 드롭스 1개', to: '[봄맞이] 듀오락 듀오 디 드롭스 2개 + 베이비 10일분', customers: 23, rate: 4.4, days: 39.4 }
];

const PARTICIPATION = [
    { name: '[가정의달] 듀오락 골드세트 1개 + 10일분 증정', views: 27395, validViews: 787, carts: 77, purchaseClicks: 128 },
    { name: '[가정의달] 듀오락 비타면역꾸미 2개', views: 22407, validViews: 859, carts: 18, purchaseClicks: 12 },
    { name: '[가정의달] 듀오락 더 퍼스트 클래스 2개', views: 9151, validViews: 208, carts: 9, purchaseClicks: 26 },
    { name: '[가정의달] 듀오락 베이비 2개', views: 8113, validViews: 444, carts: 55, purchaseClicks: 60 },
    { name: '듀오락 듀오 디 드롭스 1개', views: 7555, validViews: 844, carts: 38, purchaseClicks: 59 },
    { name: '듀오락 맘스 1개', views: 7196, validViews: 128, carts: 4, purchaseClicks: 4 },
    { name: '듀오락 비타면역꾸미 1개', views: 6971, validViews: 190, carts: 4, purchaseClicks: 0 },
    { name: '[가정의달] 듀오락 듀오 디 드롭스 2개', views: 5606, validViews: 899, carts: 37, purchaseClicks: 140 },
    { name: '듀오락 얌얌플러스 1개', views: 4875, validViews: 437, carts: 54, purchaseClicks: 51 }
];

const Q1_DIRECT = [
    { month: '2026-01', orders: 2981, revenue: 279991096 },
    { month: '2026-02', orders: 2671, revenue: 250636638 },
    { month: '2026-03', orders: 2476, revenue: 221206623 }
];

const Q1_DIRECT_TOTAL = { orders: 8128, revenue: 751834357, aov: 92499 };

const INFLOW = [
    { channel: 'naver / cpc', sessions: 5165, purchaseAmount: 59386100, rps: 11497.79 },
    { channel: 'naver / organic', sessions: 876, purchaseAmount: 6348400, rps: 7247.03 },
    { channel: 'google / cpc', sessions: 226, purchaseAmount: 1295400, rps: 5731.86 },
    { channel: '(direct) / (none)', sessions: 5995, purchaseAmount: 30446450, rps: 5078.64 },
    { channel: 'facebook / social', sessions: 1636, purchaseAmount: 1065450, rps: 651.25 },
    { channel: 'fbig / da', sessions: 8997, purchaseAmount: 354000, rps: 39.35 },
    { channel: 'criteo / display', sessions: 6529, purchaseAmount: 224500, rps: 34.39 }
];

const CHANNEL_AGGREGATE = [
    { channel: 'naver', sessions: 6129, purchaseAmount: 66162550 },
    { channel: 'direct', sessions: 22727, purchaseAmount: 20148750 },
    { channel: 'self', sessions: 3213, purchaseAmount: 13834600 },
    { channel: 'google', sessions: 424, purchaseAmount: 2000400 },
    { channel: 'instagram', sessions: 2164, purchaseAmount: 1568900 }
];

const PORTFOLIO_SKU_GROUPS = {
    gold: ['2711', '2675', '274', '2674', '2620', '2477'],
    lifecycle: ['2720', '2612', '2666', '2693'],
    problem: ['2559', '2688']
};

function formatCount(value) {
    return Number(value).toLocaleString('ko-KR');
}

function formatCurrency(value) {
    return `${Number(value).toLocaleString('ko-KR')}원`;
}

function formatCompactCurrency(value) {
    if (value >= 100000000) {
        return `${(value / 100000000).toFixed(2)}억 원`;
    }

    return `${Math.round(value / 10000).toLocaleString('ko-KR')}만 원`;
}

function formatDays(value) {
    return `${value.toFixed(1)}일`;
}

function formatRate(value, digits = 1) {
    return `${Number(value).toLocaleString('ko-KR', {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits
    })}%`;
}

function formatRatio(value, digits = 1) {
    return `${Number(value).toLocaleString('ko-KR', {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits
    })}배`;
}

function percentage(numerator, denominator) {
    if (!denominator) {
        return 0;
    }

    return (numerator / denominator) * 100;
}

function sumBy(list, key) {
    return list.reduce((total, item) => total + item[key], 0);
}

function metric(label, value, note) {
    return { label, value, note };
}

function buildPeriodPills(periods) {
    return periods.map((period) => `<span class="slide-pill slide-pill--period">${period}</span>`).join('');
}

function renderMetricCard(item) {
    return `
        <div class="slide-metric-card">
            <span class="slide-metric-card__label">${item.label}</span>
            <strong class="slide-metric-card__value">${item.value}</strong>
            <p class="slide-metric-card__note">${item.note}</p>
        </div>
    `;
}

function renderRankRows(items) {
    const maxRevenue = Math.max(...items.map((item) => item.revenue));

    return items.map((item, index) => `
        <div class="rank-row">
            <div class="rank-row__head">
                <span class="rank-row__order">${String(index + 1).padStart(2, '0')}</span>
                <div>
                    <strong>${item.name}</strong>
                    <p>구매금액 ${formatCurrency(item.revenue)} · 구매건수 ${formatCount(item.orders)}건</p>
                </div>
            </div>
            <div class="rank-row__bar">
                <span style="width: ${(item.revenue / maxRevenue) * 100}%;"></span>
            </div>
        </div>
    `).join('');
}

function renderMarchCards(items) {
    return items.map((item) => `
        <article class="mini-card">
            <p class="mini-card__eyebrow">3월 상위 SKU</p>
            <strong>${item.name}</strong>
            <p>${formatCompactCurrency(item.revenue)} · ${formatCount(item.orders)}건</p>
        </article>
    `).join('');
}

function renderTransitionCard(edge) {
    return `
        <article class="transition-card">
            <div class="transition-card__names">
                <span>${edge.from}</span>
                <strong>${edge.to}</strong>
            </div>
            <div class="transition-card__stats">
                <span>전환고객 ${formatCount(edge.customers)}명</span>
                <span>전환율 ${formatRate(edge.rate)}</span>
                <span>평균 ${formatDays(edge.days)}</span>
            </div>
        </article>
    `;
}

function renderLifecycleLane(title, note, edges) {
    return `
        <article class="journey-lane">
            <header class="journey-lane__header">
                <p class="journey-lane__eyebrow">${title}</p>
                <strong>${note}</strong>
            </header>
            <div class="journey-lane__steps">
                ${edges.map((edge, index) => `
                    ${renderTransitionCard(edge)}
                    ${index < edges.length - 1 ? '<div class="journey-arrow">다음 계단</div>' : ''}
                `).join('')}
            </div>
        </article>
    `;
}

function renderFlowNode(title, emphasis) {
    return `
        <div class="flow-node">
            <p class="flow-node__eyebrow">${emphasis}</p>
            <strong>${title}</strong>
        </div>
    `;
}

function renderFlowLink(label) {
    return `<div class="flow-link"><span>${label}</span></div>`;
}

function renderChannelRows(items) {
    const maxRps = Math.max(...items.map((item) => item.rps));

    return items.map((item) => `
        <div class="channel-row">
            <div>
                <strong>${item.channel}</strong>
                <p>세션 ${formatCount(item.sessions)} · 구매금액 ${formatCurrency(item.purchaseAmount)}</p>
            </div>
            <div class="channel-row__meter">
                <span style="width: ${(item.rps / maxRps) * 100}%;"></span>
            </div>
            <div class="channel-row__value">세션당 매출 ${item.rps.toLocaleString('ko-KR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
        </div>
    `).join('');
}

function renderAggregateRows(items) {
    const maxAmount = Math.max(...items.map((item) => item.purchaseAmount));

    return items.map((item) => `
        <div class="aggregate-row">
            <div>
                <strong>${item.channel}</strong>
                <p>세션 ${formatCount(item.sessions)}</p>
            </div>
            <div class="aggregate-row__bar">
                <span style="width: ${(item.purchaseAmount / maxAmount) * 100}%;"></span>
            </div>
            <div class="aggregate-row__value">${formatCompactCurrency(item.purchaseAmount)}</div>
        </div>
    `).join('');
}

function renderParticipationHighlights(items) {
    return items.map((item) => `
        <article class="spotlight-card">
            <p class="spotlight-card__eyebrow">${item.kicker}</p>
            <strong>${item.title}</strong>
            <p>${item.body}</p>
            <div class="spotlight-card__stats">
                ${item.stats.map((stat) => `<span>${stat}</span>`).join('')}
            </div>
        </article>
    `).join('');
}

function renderInsightStatCards(items, gridClass = '') {
    return `
        <div class="insight-stat-grid${gridClass ? ` ${gridClass}` : ''}">
            ${items.map((item) => `
                <article class="insight-stat-card${item.tone ? ` insight-stat-card--${item.tone}` : ''}">
                    <p class="insight-stat-card__eyebrow">${item.eyebrow}</p>
                    <strong class="insight-stat-card__value">${item.value}</strong>
                    ${item.title ? `<p class="insight-stat-card__title">${item.title}</p>` : ''}
                    <p>${item.note}</p>
                </article>
            `).join('')}
        </div>
    `;
}

function renderParticipationRows(items) {
    const maxClickRate = Math.max(...items.map((item) => percentage(item.purchaseClicks, item.views)));

    return items.map((item) => {
        const clickRate = percentage(item.purchaseClicks, item.views);
        const cartRate = percentage(item.carts, item.views);

        return `
            <div class="participation-row">
                <div>
                    <strong>${item.name}</strong>
                    <p>조회 ${formatCount(item.views)} · 유효조회 ${formatCount(item.validViews)}</p>
                </div>
                <div class="participation-row__stats">
                    <span>장바구니 ${formatCount(item.carts)} (조회 대비 ${formatRate(cartRate, 2)})</span>
                    <span>구매클릭 ${formatCount(item.purchaseClicks)} (조회 대비 ${formatRate(clickRate, 2)})</span>
                </div>
                <div class="participation-row__bar">
                    <span style="width: ${(clickRate / maxClickRate) * 100}%;"></span>
                </div>
            </div>
        `;
    }).join('');
}

function renderActionCards(items) {
    return items.map((item, index) => `
        <article class="action-card">
            <div class="action-card__order">${index + 1}</div>
            <div>
                <p class="action-card__eyebrow">${item.kicker}</p>
                <strong>${item.title}</strong>
                <p>${item.body}</p>
            </div>
        </article>
    `).join('');
}

function buildSlides() {
    const marchTopRevenue = PRODUCT_SALES_MARCH[0];
    const lifecycleFastest = RELATION_LIFECYCLE.reduce((best, item) => (item.days < best.days ? item : best), RELATION_LIFECYCLE[0]);
    const lifecycleHighestRate = RELATION_LIFECYCLE.reduce((best, item) => (item.rate > best.rate ? item : best), RELATION_LIFECYCLE[0]);
    const coreHighestRate = RELATION_CORE.reduce((best, item) => (item.rate > best.rate ? item : best), RELATION_CORE[0]);
    const coreMaxCustomers = RELATION_CORE.reduce((best, item) => (item.customers > best.customers ? item : best), RELATION_CORE[0]);
    const q1Top12Revenue = sumBy(PRODUCT_SALES_Q1, 'revenue');
    const q1Top12Orders = sumBy(PRODUCT_SALES_Q1, 'orders');
    const goldRevenue = sumBy(PRODUCT_SALES_Q1.filter((item) => PORTFOLIO_SKU_GROUPS.gold.includes(item.sku)), 'revenue');
    const lifecycleRevenue = sumBy(PRODUCT_SALES_Q1.filter((item) => PORTFOLIO_SKU_GROUPS.lifecycle.includes(item.sku)), 'revenue');
    const problemRevenue = sumBy(PRODUCT_SALES_Q1.filter((item) => PORTFOLIO_SKU_GROUPS.problem.includes(item.sku)), 'revenue');
    const goldShare = percentage(goldRevenue, q1Top12Revenue);
    const lifecycleShare = percentage(lifecycleRevenue, q1Top12Revenue);
    const problemShare = percentage(problemRevenue, q1Top12Revenue);
    const latestTopViews = PARTICIPATION.reduce((best, item) => (item.views > best.views ? item : best), PARTICIPATION[0]);
    const bestInflow = INFLOW.reduce((best, item) => (item.rps > best.rps ? item : best), INFLOW[0]);
    const naverCpc = INFLOW.find((item) => item.channel === 'naver / cpc');
    const directInflow = INFLOW.find((item) => item.channel === '(direct) / (none)');
    const fbigDa = INFLOW.find((item) => item.channel === 'fbig / da');
    const criteoDisplay = INFLOW.find((item) => item.channel === 'criteo / display');
    const directRevenueChange = percentage(Q1_DIRECT[2].revenue - Q1_DIRECT[0].revenue, Q1_DIRECT[0].revenue);
    const naverVsDirect = naverCpc.rps / directInflow.rps;
    const naverVsFbig = naverCpc.rps / fbigDa.rps;
    const naverVsCriteo = naverCpc.rps / criteoDisplay.rps;
    const participationByName = Object.fromEntries(PARTICIPATION.map((item) => [item.name, item]));
    const weakVitamin = participationByName['[가정의달] 듀오락 비타면역꾸미 2개'];
    const weakMoms = participationByName['듀오락 맘스 1개'];
    const strongDropsBundle = participationByName['[가정의달] 듀오락 듀오 디 드롭스 2개'];
    const strongYumyumSingle = participationByName['듀오락 얌얌플러스 1개'];

    const participationHighlights = [
        {
            kicker: '규모 앵커',
            title: '[가정의달] 듀오락 골드세트 1개 + 10일분 증정',
            body: '최다 조회와 강한 구매클릭을 동시에 확보해 프로모션 대표 진열면으로 기능했습니다.',
            stats: ['조회 27,395', '구매클릭 128', `조회 대비 구매클릭률 ${formatRate(percentage(128, 27395), 2)}`]
        },
        {
            kicker: '행동 밀도',
            title: '[가정의달] 듀오락 듀오 디 드롭스 2개',
            body: '조회 규모는 상대적으로 작지만 구매클릭 140건으로 실제 행동이 가장 크게 모였습니다.',
            stats: [`장바구니율 ${formatRate(percentage(strongDropsBundle.carts, strongDropsBundle.views), 2)}`, `구매클릭률 ${formatRate(percentage(strongDropsBundle.purchaseClicks, strongDropsBundle.views), 2)}`, '구매클릭 140']
        },
        {
            kicker: '균형형',
            title: '[가정의달] 듀오락 베이비 2개 / 듀오락 얌얌플러스 1개',
            body: '장바구니와 구매클릭이 함께 살아 있어 생애주기 가족군 육성 후보로 읽힙니다.',
            stats: [`얌얌플러스 구매클릭률 ${formatRate(percentage(strongYumyumSingle.purchaseClicks, strongYumyumSingle.views), 2)}`, '베이비 2개 장바구니 55', '얌얌플러스 1개 장바구니 54']
        },
        {
            kicker: '이탈 신호',
            title: '[가정의달] 듀오락 비타면역꾸미 2개',
            body: '조회와 유효조회는 큰 편이지만 장바구니 18, 구매클릭 12에 머물러 메시지와 오퍼 재점검이 필요합니다.',
            stats: [`장바구니율 ${formatRate(percentage(weakVitamin.carts, weakVitamin.views), 2)}`, `구매클릭률 ${formatRate(percentage(weakVitamin.purchaseClicks, weakVitamin.views), 2)}`, '구매클릭 12']
        }
    ];

    const actionCards = [
        {
            kicker: '우선순위 1',
            title: '상품 관계 정교화',
            body: '골드는 체험 → 1개 → 2개/3개, 하루 한 포는 1개 → 집중케어 번들 흐름을 26~43일 윈도우 중심으로 CRM과 상세페이지에 맞춥니다.'
        },
        {
            kicker: '우선순위 2',
            title: '상품 인사이트 확장',
            body: '골드 계열은 코어·리텐션, 베이비·키즈는 생애주기 성장, ATP·바이오가드는 문제해결형 육성으로 묶고 듀오랩·락토클리어는 크로스셀 후보로만 관리합니다.'
        },
        {
            kicker: '우선순위 3',
            title: 'UTM / 유입 해석 보완',
            body: 'Q1 direct/none 주문 성과는 결과 기준으로 보고, 최신 유입 스냅샷에서는 naver/cpc의 높은 세션당 매출을 신규 효율 점검 레인으로 별도 운영합니다.'
        },
        {
            kicker: '우선순위 4',
            title: '상품 참여 신호 활용',
            body: '골드세트와 듀오 디 드롭스 2개처럼 클릭 밀도가 확인된 진열 패턴을 우선 재사용하고, 조회 대비 행동이 약한 상품은 후순위 실험으로 둡니다.'
        }
    ];

    return [
        {
            id: 'slide-01',
            order: '01',
            title: '듀오락 Q1 2026 마케팅 전략',
            kicker: '표지',
            navNote: '핵심 방향과 데이터 기준',
            summary: `Q1 상위 12개 매출 ${formatCompactCurrency(q1Top12Revenue)} 중 골드가 ${formatRate(goldShare)}를 차지하고, direct / none 주문 매출은 1월 대비 3월 ${formatRate(directRevenueChange)}로 둔화된 상황을 상품 관계 중심으로 재해석한 8장 발표형 리포트입니다.`,
            periods: [PERIODS.q1, PERIODS.latest],
            metrics: [
                metric('포트폴리오 중심', `골드 ${formatRate(goldShare)}`, `Q1 상위 12개 ${formatCurrency(q1Top12Revenue)} 중 ${formatCurrency(goldRevenue)}`),
                metric('유입 경고 신호', `direct ${formatRate(directRevenueChange)}`, `1월 ${formatCompactCurrency(Q1_DIRECT[0].revenue)} → 3월 ${formatCompactCurrency(Q1_DIRECT[2].revenue)}`),
                metric('출력 규칙', '슬라이드 1장 = PDF 1페이지', '인쇄 시 목차와 개요는 숨기고 슬라이드만 페이지로 출력합니다.')
            ],
            layoutClass: 'is-cover',
            body: `
                <section class="cover-grid">
                    <article class="hero-card hero-card--feature">
                        <p class="hero-card__eyebrow">이번 리포트가 답하는 질문</p>
                        <h4>Q1 매출 성과 위에 어떤 상품 관계를 얹어야 다음 분기 재구매와 확장을 동시에 만들 수 있는가</h4>
                        <p>
                            골드 계열은 코어·리텐션 축으로, 베이비·키즈는 생애주기 성장 축으로,
                            문제해결형 SKU는 육성 축으로 배치합니다.
                        </p>
                    </article>
                    <div class="stack-grid">
                        <article class="hero-card">
                            <p class="hero-card__eyebrow">관계 중심 재정렬</p>
                            <strong>3장과 4장에서 생애주기 계단과 체험 → 본품 → 대용량 흐름을 가장 강하게 노출합니다.</strong>
                        </article>
                        <article class="hero-card">
                            <p class="hero-card__eyebrow">데이터 기준 분리</p>
                            <strong>Q1 결과 지표와 4/8~4/20 관계·참여 스냅샷은 같은 페이지 안에서도 구간을 나눠 표기합니다.</strong>
                        </article>
                        <article class="hero-card">
                            <p class="hero-card__eyebrow">실행 관점</p>
                            <strong>장표는 도구 설명이 아니라 우선순위, 전환 경로, 오퍼 구조를 바로 논의할 수 있게 설계했습니다.</strong>
                        </article>
                    </div>
                </section>
                <section class="cover-axis-grid">
                    <article class="axis-card">
                        <p class="axis-card__eyebrow">코어 / 리텐션</p>
                        <strong>골드 계열</strong>
                        <p>Q1 상위 12개 매출 ${formatCurrency(q1Top12Revenue)} 중 ${formatCurrency(goldRevenue)}, 비중 ${formatRate(goldShare)}로 코어 매출과 관계 스냅샷이 함께 골드 업셀 경로를 지지합니다.</p>
                    </article>
                    <article class="axis-card">
                        <p class="axis-card__eyebrow">생애주기 성장</p>
                        <strong>베이비 · 키즈</strong>
                        <p>얌얌·얌얌플러스·베이비·듀오 디 드롭스가 ${formatCurrency(lifecycleRevenue)}, 비중 ${formatRate(lifecycleShare)}로 2순위 성장 축을 형성합니다.</p>
                    </article>
                    <article class="axis-card">
                        <p class="axis-card__eyebrow">육성 / 크로스셀</p>
                        <strong>문제해결형 SKU</strong>
                        <p>ATP·바이오가드는 ${formatCurrency(problemRevenue)}, 비중 ${formatRate(problemShare)}의 육성 트랙으로 두고 듀오랩·락토클리어는 크로스셀 후보로만 관리합니다.</p>
                    </article>
                </section>
            `,
            footer: '모든 숫자는 제공된 정적 데이터만 사용했고, 슬라이드마다 기간 차이를 따로 표시했습니다.'
        },
        {
            id: 'slide-02',
            order: '02',
            title: '핵심 요약',
            kicker: '핵심 요약',
            navNote: '이번 루프에서 읽어야 할 4가지',
            summary: 'Q1 실적, 최신 관계 스냅샷, UTM 흐름, 상품 참여 신호를 한 장에서 묶어 우선순위를 정리했습니다.',
            periods: [PERIODS.q1, PERIODS.latest, PERIODS.march],
            metrics: [
                metric('골드 비중', `${formatRate(goldShare)}`, `Q1 상위 12개 ${formatCompactCurrency(q1Top12Revenue)} 중 ${formatCompactCurrency(goldRevenue)}`),
                metric('관계 최고 전환율', `${formatRate(coreHighestRate.rate)}`, `${coreHighestRate.from} → ${coreHighestRate.to}`),
                metric('최신 효율 우위', `naver / cpc ${formatRatio(naverVsDirect)}`, `direct 대비 · fbig/da ${formatRatio(naverVsFbig, 0)} · criteo/display ${formatRatio(naverVsCriteo, 0)}`)
            ],
            layoutClass: 'is-summary',
            body: `
                <section class="panel-grid panel-grid--2">
                    <div class="summary-grid">
                        <article class="summary-card summary-card--accent">
                            <p class="summary-card__eyebrow">1. 상품 관계</p>
                            <strong>관계 데이터는 골드 업셀 계단을 가장 선명하게 보여줍니다.</strong>
                            <p>골드캡슐 1개 → 2개는 전환고객 129명, 5.9%, 평균 42.7일이며, 하루 한 포 1개 → 집중케어 번들은 15.6%로 전환율이 가장 높습니다.</p>
                        </article>
                        <article class="summary-card">
                            <p class="summary-card__eyebrow">2. 상품 인사이트</p>
                            <strong>Q1 상위 12개 매출 ${formatCompactCurrency(q1Top12Revenue)} 중 골드 계열이 ${formatRate(goldShare)}를 차지합니다.</strong>
                            <p>생애주기 성장군은 ${formatRate(lifecycleShare)}, 문제해결형 육성군은 ${formatRate(problemShare)}로 분리돼 포트폴리오의 주력과 후보가 선명합니다.</p>
                        </article>
                        <article class="summary-card">
                            <p class="summary-card__eyebrow">3. UTM / 유입</p>
                            <strong>Q1 direct / none 주문 매출은 1월 대비 3월 ${formatRate(directRevenueChange)}이고, 최신 효율 1위는 naver / cpc입니다.</strong>
                            <p>세션당 매출은 direct 대비 ${formatRatio(naverVsDirect)}, fbig / da 대비 ${formatRatio(naverVsFbig, 0)}, criteo / display 대비 ${formatRatio(naverVsCriteo, 0)}로 벌어집니다.</p>
                        </article>
                        <article class="summary-card">
                            <p class="summary-card__eyebrow">4. 상품 참여</p>
                            <strong>조회 대비 구매클릭률은 듀오 디 드롭스 2개 ${formatRate(percentage(strongDropsBundle.purchaseClicks, strongDropsBundle.views), 2)}와 비타면역꾸미 2개 ${formatRate(percentage(weakVitamin.purchaseClicks, weakVitamin.views), 2)}의 격차가 가장 선명합니다.</strong>
                            <p>얌얌플러스 1개는 ${formatRate(percentage(strongYumyumSingle.purchaseClicks, strongYumyumSingle.views), 2)}, 맘스 1개는 ${formatRate(percentage(weakMoms.purchaseClicks, weakMoms.views), 2)}로 강약을 바로 구분할 수 있습니다.</p>
                        </article>
                    </div>
                    <div class="stack-grid">
                        <article class="note-card note-card--contrast">
                            <p class="note-card__eyebrow">이번 루프 메시지</p>
                            <strong>상품 관계를 먼저 고정하고 그 위에 상품 메시지와 유입 운영을 얹는 순서가 맞습니다.</strong>
                        </article>
                        <article class="note-card">
                            <p class="note-card__eyebrow">기간 주의</p>
                            <strong>Q1 매출·주문 기준</strong>
                            <p>2026.01.01 ~ 2026.03.31</p>
                            <strong>최신 가용 관계·참여 스냅샷</strong>
                            <p>2026.04.08 ~ 2026.04.20</p>
                        </article>
                        <article class="note-card">
                            <p class="note-card__eyebrow">3월 보조 해석</p>
                            <strong>${marchTopRevenue.name}</strong>
                            <p>${formatCompactCurrency(marchTopRevenue.revenue)} · ${formatCount(marchTopRevenue.orders)}건으로 월간 1위를 유지했습니다.</p>
                        </article>
                    </div>
                </section>
            `,
            footer: '핵심 요약 장표는 전체 장표의 읽는 순서를 미리 고정하는 페이지로 설계했습니다.'
        },
        {
            id: 'slide-03',
            order: '03',
            title: '상품 관계 인사이트 A: 생애주기 계단',
            kicker: '상품 관계',
            navNote: '키즈·베이비·드롭스 생애주기 이동',
            summary: '생애주기 관계 스냅샷은 체험 또는 단품에서 2개 번들로 올라가는 계단 구조를 보여줍니다.',
            periods: [PERIODS.latest, PERIODS.extracted],
            metrics: [
                metric('가장 빠른 이동', `${formatDays(lifecycleFastest.days)}`, `${lifecycleFastest.from} → ${lifecycleFastest.to}`),
                metric('가장 높은 전환율', `${formatRate(lifecycleHighestRate.rate)}`, `${lifecycleHighestRate.from} → ${lifecycleHighestRate.to}`),
                metric('해석 포인트', '체험/단품 → 2개 번들', '생애주기 가족군은 대부분 업셀 종착점이 2개 번들에 모입니다.')
            ],
            layoutClass: 'is-relation',
            body: `
                <section class="relation-stage">
                    <div class="journey-board">
                        ${renderLifecycleLane('키즈 성장', '얌얌플러스는 체험과 1개 모두 2개 번들로 수렴', [
                            RELATION_LIFECYCLE[0],
                            RELATION_LIFECYCLE[1]
                        ])}
                        ${renderLifecycleLane('베이비 성장', '베이비는 단품 출발의 전환율이 더 높습니다', [
                            RELATION_LIFECYCLE[2],
                            RELATION_LIFECYCLE[3]
                        ])}
                        ${renderLifecycleLane('영유아 크로스 번들', '드롭스는 베이비 동반 번들로 연결됩니다', [
                            RELATION_LIFECYCLE[4]
                        ])}
                    </div>
                    <div class="stack-grid">
                        <article class="note-card note-card--contrast">
                            <p class="note-card__eyebrow">핵심 해석</p>
                            <strong>생애주기 축에서는 대용량보다 2개 번들이 먼저 잠재 수요를 흡수합니다.</strong>
                            <p>키즈와 베이비는 체험·단품에서 2개 번들로 이동하는 사례가 반복되므로, 초기 구매 다음 오퍼는 대용량보다 가족용 번들이 더 자연스럽습니다.</p>
                        </article>
                        <article class="note-card">
                            <p class="note-card__eyebrow">신호 강도</p>
                            <strong>베이비 1개 → 봄맞이 2개 번들</strong>
                            <p>전환율 8.7%, 평균 40.4일로 제공된 생애주기 관계 중 가장 높은 전환율입니다.</p>
                        </article>
                        <article class="note-card">
                            <p class="note-card__eyebrow">운영 시사점</p>
                            <strong>체험 직후 25일 안팎, 단품 이후 38~40일 안팎의 두 개 리마인드 구간이 필요합니다.</strong>
                        </article>
                    </div>
                </section>
            `,
            footer: '생애주기 관계 데이터는 최신 가용 스냅샷으로만 존재하므로, Q1 실적과 직접 합산하지 않고 별도 장표로 분리했습니다.'
        },
        {
            id: 'slide-04',
            order: '04',
            title: '상품 관계 인사이트 B: 체험 → 본품 → 대용량',
            kicker: '상품 관계',
            navNote: '골드 업셀 계단과 집중케어 번들',
            summary: '골드 계열 관계 스냅샷은 캡슐 업셀 계단과 하루 한 포 집중케어 번들의 두 개 성장 경로를 동시에 보여줍니다.',
            periods: [PERIODS.latest, PERIODS.extracted],
            metrics: [
                metric('최대 전환 고객수', `${formatCount(coreMaxCustomers.customers)}명`, `${coreMaxCustomers.from} → ${coreMaxCustomers.to}`),
                metric('최고 전환율', `${formatRate(coreHighestRate.rate)}`, `${coreHighestRate.from} → ${coreHighestRate.to}`),
                metric('관찰 윈도우', '26.0 ~ 42.7일', '체험 직후와 본품 구매 후 업셀 타이밍이 모두 한 달 안팎에서 포착됩니다.')
            ],
            layoutClass: 'is-relation is-relation-strong',
            body: `
                <section class="panel-grid panel-grid--2 panel-grid--wide">
                    <article class="flow-board">
                        <p class="flow-board__eyebrow">골드 캡슐 업셀 계단</p>
                        <div class="flow-board__track">
                            ${renderFlowNode('[50%체험] 듀오락 골드캡슐 10일', '체험')}
                            ${renderFlowLink('103명 · 5.1% · 26.9일')}
                            ${renderFlowNode('듀오락 골드캡슐 1개', '본품')}
                            ${renderFlowLink('129명 · 5.9% · 42.7일')}
                            ${renderFlowNode('듀오락 골드캡슐 30일분 2개', '확장')}
                            ${renderFlowLink('56명 · 2.6% · 40.1일')}
                            ${renderFlowNode('듀오락 골드캡슐 3개', '대용량')}
                        </div>
                        <div class="flow-board__foot">
                            <span>직접 점프: 체험 10일 → 30일분 2개 = 78명 · 3.9% · 35.3일</span>
                        </div>
                    </article>
                    <div class="stack-grid">
                        <article class="feature-card feature-card--accent">
                            <p class="feature-card__eyebrow">별도 성장 레인</p>
                            <strong>하루 한 포는 집중케어 번들 전환율이 15.6%로 가장 강합니다.</strong>
                            <p>듀오락 골드 하루 한 포 1개 → [집중케어] 2개 + 10일은 전환고객 80명, 평균 38.9일입니다.</p>
                        </article>
                        <article class="feature-card">
                            <p class="feature-card__eyebrow">체험 번들 직결</p>
                            <strong>[50%체험] 듀오락 골드 10일 → 집중케어 번들</strong>
                            <p>전환고객 69명, 2.7%, 평균 26.0일로 체험 직후 번들 진입 가능성도 확인됩니다.</p>
                        </article>
                        <article class="feature-card">
                            <p class="feature-card__eyebrow">실행 문장</p>
                            <strong>골드는 첫 본품에서 끝나지 않도록 30일 1개 후속 오퍼와 집중케어 번들을 동시에 준비해야 합니다.</strong>
                        </article>
                    </div>
                </section>
            `,
            footer: '가장 강한 관계 장표로 보이도록 체험, 본품, 대용량, 번들의 순서를 시각적으로 한눈에 보이게 구성했습니다.'
        },
        {
            id: 'slide-05',
            order: '05',
            title: '상품 인사이트',
            kicker: '상품 포트폴리오',
            navNote: 'Q1 상위 SKU와 3월 유지력',
            summary: `Q1 상위 12개 매출 ${formatCurrency(q1Top12Revenue)}은 골드 ${formatRate(goldShare)}, 생애주기 성장군 ${formatRate(lifecycleShare)}, 문제해결형 육성군 ${formatRate(problemShare)}로 나뉘며 포트폴리오 우선순위가 분명합니다.`,
            periods: [PERIODS.q1, PERIODS.march],
            metrics: [
                metric('제공된 상위 12개 합계', `${formatCurrency(q1Top12Revenue)}`, `구매건수 ${formatCount(q1Top12Orders)}건 기준`),
                metric('골드 계열 비중', `${formatRate(goldShare)}`, `${formatCurrency(goldRevenue)} · 코어 / 리텐션 축`),
                metric('성장·육성 잔여축', `${formatRate(lifecycleShare + problemShare)}`, `생애주기 ${formatRate(lifecycleShare)} · 문제해결형 ${formatRate(problemShare)}`)
            ],
            layoutClass: 'is-portfolio',
            body: `
                <section class="panel-grid panel-grid--2">
                    <article class="data-card">
                        <p class="data-card__eyebrow">Q1 상품 매출 상위</p>
                        <div class="rank-grid">
                            ${renderRankRows(PRODUCT_SALES_Q1.slice(0, 8))}
                        </div>
                    </article>
                    <div class="stack-grid">
                        <article class="portfolio-card">
                            <p class="portfolio-card__eyebrow">코어 / 리텐션</p>
                            <span class="portfolio-card__value">${formatRate(goldShare)}</span>
                            <strong>골드 계열</strong>
                            <p>Q1 상위 12개 매출 중 ${formatCurrency(goldRevenue)}으로 가장 큰 비중을 차지하며, 1위 SKU와 3월 1위 SKU도 모두 골드 코어군입니다.</p>
                        </article>
                        <article class="portfolio-card">
                            <p class="portfolio-card__eyebrow">생애주기 성장</p>
                            <span class="portfolio-card__value">${formatRate(lifecycleShare)}</span>
                            <strong>얌얌 · 얌얌플러스 · 베이비 · 듀오 디 드롭스</strong>
                            <p>${formatCurrency(lifecycleRevenue)}으로 2순위 볼륨을 형성하며, 가족군 번들과 생애주기 메시지 확장 여지가 확인됩니다.</p>
                        </article>
                        <article class="portfolio-card">
                            <p class="portfolio-card__eyebrow">육성 / 후보군</p>
                            <span class="portfolio-card__value">${formatRate(problemShare)}</span>
                            <strong>ATP · 바이오가드 / 듀오랩 · 락토클리어</strong>
                            <p>ATP와 바이오가드는 ${formatCurrency(problemRevenue)} 규모의 문제해결형 육성 트랙으로 두고, 듀오랩·락토클리어는 크로스셀 후보로만 관리합니다.</p>
                        </article>
                        <div class="mini-card-grid">
                            ${renderMarchCards(PRODUCT_SALES_MARCH.slice(0, 4))}
                        </div>
                    </div>
                </section>
            `,
            footer: '상품 인사이트 장표는 이번 루프에서 포트폴리오 비중이 먼저 보이도록 숫자 중심으로 다시 정리했습니다.'
        },
        {
            id: 'slide-06',
            order: '06',
            title: 'UTM / 유입분석',
            kicker: '유입 해석',
            navNote: 'Q1 direct 주문과 최신 유입 분리',
            summary: `Q1 주문 기준 direct / none 매출은 1월 ${formatCurrency(Q1_DIRECT[0].revenue)}에서 3월 ${formatCurrency(Q1_DIRECT[2].revenue)}으로 ${formatRate(directRevenueChange)} 감소했고, 최신 유입 효율은 naver / cpc가 direct 대비 ${formatRatio(naverVsDirect)}로 앞섭니다.`,
            periods: [PERIODS.q1, PERIODS.latest],
            metrics: [
                metric('Q1 direct / none 감소', `${formatRate(directRevenueChange)}`, `1월 ${formatCurrency(Q1_DIRECT[0].revenue)} → 3월 ${formatCurrency(Q1_DIRECT[2].revenue)}`),
                metric('최신 세션당 매출 1위', `${bestInflow.channel}`, `${bestInflow.rps.toLocaleString('ko-KR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`),
                metric('효율 격차', `${formatRatio(naverVsFbig, 0)} / ${formatRatio(naverVsCriteo, 0)}`, 'fbig / da 및 criteo / display 대비')
            ],
            layoutClass: 'is-sky',
            body: `
                <section class="panel-grid panel-grid--2">
                    <div class="stack-grid">
                        <article class="data-card">
                            <p class="data-card__eyebrow">Q1 주문 기준 direct / none</p>
                            <div class="month-grid">
                                ${Q1_DIRECT.map((item) => `
                                    <div class="month-card">
                                        <p class="month-card__eyebrow">${item.month}</p>
                                        <strong>${formatCount(item.orders)}건</strong>
                                        <p>구매금액 ${formatCurrency(item.revenue)}</p>
                                    </div>
                                `).join('')}
                            </div>
                            ${renderInsightStatCards([
                                {
                                    eyebrow: 'Q1 direct 감소폭',
                                    value: formatRate(directRevenueChange),
                                    title: '주문 매출 기준 1월 → 3월',
                                    note: `${formatCurrency(Q1_DIRECT[0].revenue)}에서 ${formatCurrency(Q1_DIRECT[2].revenue)}으로 축소`
                                }
                            ])}
                        </article>
                        <article class="note-card">
                            <p class="note-card__eyebrow">해석 기준</p>
                            <strong>Q1의 direct / none은 결과 규모 확인용입니다.</strong>
                            <p>1월 ${formatCurrency(Q1_DIRECT[0].revenue)}에서 3월 ${formatCurrency(Q1_DIRECT[2].revenue)}으로 ${formatRate(directRevenueChange)} 감소했지만, 여전히 Q1 합계 ${formatCount(Q1_DIRECT_TOTAL.orders)}건과 ${formatCurrency(Q1_DIRECT_TOTAL.revenue)} 규모를 형성합니다.</p>
                        </article>
                    </div>
                    <div class="stack-grid">
                        ${renderInsightStatCards([
                            {
                                eyebrow: 'direct 대비',
                                value: formatRatio(naverVsDirect),
                                title: 'naver / cpc 세션당 매출',
                                note: `${directInflow.channel} ${directInflow.rps.toLocaleString('ko-KR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} 대비`
                            },
                            {
                                eyebrow: 'fbig / da 대비',
                                value: formatRatio(naverVsFbig, 0),
                                title: 'naver / cpc 세션당 매출',
                                note: `${fbigDa.rps.toLocaleString('ko-KR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} 대비 압도적 격차`
                            },
                            {
                                eyebrow: 'criteo / display 대비',
                                value: formatRatio(naverVsCriteo, 0),
                                title: 'naver / cpc 세션당 매출',
                                note: `${criteoDisplay.rps.toLocaleString('ko-KR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} 대비 압도적 격차`
                            }
                        ], 'insight-stat-grid--triple')}
                        <article class="data-card">
                            <p class="data-card__eyebrow">최신 유입 스냅샷</p>
                            <div class="channel-board">
                                ${renderChannelRows(INFLOW)}
                            </div>
                        </article>
                        <article class="data-card">
                            <p class="data-card__eyebrow">채널 묶음 참고값</p>
                            <div class="aggregate-board">
                                ${renderAggregateRows(CHANNEL_AGGREGATE)}
                            </div>
                        </article>
                    </div>
                </section>
            `,
            footer: 'UTM 장표는 이번 루프에서 결과 규모와 최신 유입 효율 해석에만 집중했습니다.'
        },
        {
            id: 'slide-07',
            order: '07',
            title: '상품 참여 인사이트',
            kicker: '상품 참여',
            navNote: '조회 규모와 행동 밀도 분리',
            summary: `최신 참여 스냅샷에서는 조회 규모보다 조회 대비 행동률이 더 중요하며, 듀오 디 드롭스 2개의 구매클릭률 ${formatRate(percentage(strongDropsBundle.purchaseClicks, strongDropsBundle.views), 2)}와 비타면역꾸미 2개의 ${formatRate(percentage(weakVitamin.purchaseClicks, weakVitamin.views), 2)} 차이가 이를 가장 선명하게 보여줍니다.`,
            periods: [PERIODS.latest],
            metrics: [
                metric('최다 조회', `${formatCount(latestTopViews.views)}`, latestTopViews.name),
                metric('강한 참여', `${formatRate(percentage(strongDropsBundle.purchaseClicks, strongDropsBundle.views), 2)}`, `${strongDropsBundle.name} 구매클릭률`),
                metric('약한 참여', `${formatRate(percentage(weakVitamin.purchaseClicks, weakVitamin.views), 2)}`, `${weakVitamin.name} 구매클릭률`)
            ],
            layoutClass: 'is-highlight',
            body: `
                <section class="panel-grid panel-grid--2">
                    <div class="stack-grid">
                        <article class="data-card">
                            <p class="data-card__eyebrow">참여 신호 요약</p>
                            <div class="spotlight-grid">
                                ${renderParticipationHighlights(participationHighlights)}
                            </div>
                        </article>
                        <article class="data-card">
                            <p class="data-card__eyebrow">조회 대비 행동 격차</p>
                            ${renderInsightStatCards([
                                {
                                    eyebrow: '약한 참여',
                                    value: formatRate(percentage(weakVitamin.purchaseClicks, weakVitamin.views), 2),
                                    title: '비타면역꾸미 2개 구매클릭률',
                                    note: `장바구니율 ${formatRate(percentage(weakVitamin.carts, weakVitamin.views), 2)}`,
                                    tone: 'muted'
                                },
                                {
                                    eyebrow: '약한 참여',
                                    value: formatRate(percentage(weakMoms.purchaseClicks, weakMoms.views), 2),
                                    title: '맘스 1개 구매클릭률',
                                    note: `장바구니율 ${formatRate(percentage(weakMoms.carts, weakMoms.views), 2)}`,
                                    tone: 'muted'
                                },
                                {
                                    eyebrow: '강한 참여',
                                    value: formatRate(percentage(strongDropsBundle.purchaseClicks, strongDropsBundle.views), 2),
                                    title: '듀오 디 드롭스 2개 구매클릭률',
                                    note: `장바구니율 ${formatRate(percentage(strongDropsBundle.carts, strongDropsBundle.views), 2)}`,
                                    tone: 'accent'
                                },
                                {
                                    eyebrow: '강한 참여',
                                    value: formatRate(percentage(strongYumyumSingle.purchaseClicks, strongYumyumSingle.views), 2),
                                    title: '얌얌플러스 1개 구매클릭률',
                                    note: `장바구니율 ${formatRate(percentage(strongYumyumSingle.carts, strongYumyumSingle.views), 2)}`,
                                    tone: 'accent'
                                }
                            ], 'insight-stat-grid--double')}
                        </article>
                    </div>
                    <article class="data-card">
                        <p class="data-card__eyebrow">상품별 참여 현황</p>
                        <div class="participation-board">
                            ${renderParticipationRows(PARTICIPATION)}
                        </div>
                    </article>
                </section>
            `,
            footer: '참여 장표는 2026.04.08 ~ 04.20 합산 스냅샷이며, 이번 루프에서는 조회 규모보다 조회 대비 행동률 비교를 더 전면에 두었습니다.'
        },
        {
            id: 'slide-08',
            order: '08',
            title: '실행안 + 가드레일',
            kicker: '실행 제안',
            navNote: '우선 실행 항목과 해석 규칙',
            summary: '데이터 범위 안에서 바로 실행할 수 있는 우선순위와, 구간 차이 때문에 반드시 지켜야 할 해석 규칙을 한 장에 묶었습니다.',
            periods: [PERIODS.q1, PERIODS.latest],
            metrics: [
                metric('첫 번째 실행 축', '상품 관계 정교화', '골드 업셀과 생애주기 번들 리듬부터 고정합니다.'),
                metric('두 번째 실행 축', '상품 인사이트 확장', '포트폴리오 역할을 메시지와 오퍼 구조로 연결합니다.'),
                metric('가드레일', '구간 혼합 인과 금지', 'Q1과 최신 스냅샷은 나란히 보되 하나의 추세선처럼 해석하지 않습니다.')
            ],
            layoutClass: 'is-closing',
            body: `
                <section class="panel-grid panel-grid--2">
                    <article class="data-card">
                        <p class="data-card__eyebrow">실행 우선순위</p>
                        <div class="action-stack">
                            ${renderActionCards(actionCards)}
                        </div>
                    </article>
                    <div class="stack-grid">
                        <article class="guardrail-card">
                            <p class="guardrail-card__eyebrow">가드레일 1</p>
                            <strong>장표 안에서 Q1 기준과 최신 가용 스냅샷을 명시적으로 분리 표기합니다.</strong>
                        </article>
                        <article class="guardrail-card">
                            <p class="guardrail-card__eyebrow">가드레일 2</p>
                            <strong>관계·참여 데이터는 2026.04.08 ~ 04.20 가용 구간만 반영하며, Q1 매출과 인과로 단정하지 않습니다.</strong>
                        </article>
                        <article class="guardrail-card">
                            <p class="guardrail-card__eyebrow">가드레일 3</p>
                            <strong>이번 실행안은 제공된 관계·상품·유입·참여 데이터 범위 안에서만 판단합니다.</strong>
                        </article>
                        <article class="guardrail-card">
                            <p class="guardrail-card__eyebrow">가드레일 4</p>
                            <strong>듀오랩·락토클리어는 현재 데이터 공백을 전제로 크로스셀 후보로만 다루고 과도한 매출 기여 가정을 두지 않습니다.</strong>
                        </article>
                        <article class="guardrail-card">
                            <p class="guardrail-card__eyebrow">가드레일 5</p>
                            <strong>외부 가이드 URL은 인증 오류로 본문 확인이 불가해 이번 실행안 근거에서는 제외합니다.</strong>
                        </article>
                    </div>
                </section>
            `,
            footer: '실행안은 관계 기반 전환 경로를 먼저 확정하고, 상품 메시지와 유입 운영은 그 다음에 정교화하는 순서를 제안합니다.'
        }
    ];
}

const SLIDES = buildSlides();

function renderSlide(slide, total) {
    return `
        <article class="report-slide ${slide.layoutClass}" id="${slide.id}" data-slide-order="${slide.order}">
            <div class="slide-card">
                <header class="slide-card__header">
                    <div>
                        <p class="slide-card__eyebrow">${slide.kicker}</p>
                        <h3>${slide.title}</h3>
                        <p class="slide-card__summary">${slide.summary}</p>
                    </div>
                    <div class="slide-card__meta" aria-label="슬라이드 메타 정보">
                        ${buildPeriodPills(slide.periods)}
                    </div>
                </header>

                <section class="slide-card__metric-grid" aria-label="핵심 지표">
                    ${slide.metrics.map(renderMetricCard).join('')}
                </section>

                <section class="slide-card__body" aria-label="슬라이드 본문 콘텐츠">
                    ${slide.body}
                </section>

                <footer class="slide-card__footer-row">
                    <p class="slide-card__footer">${slide.footer}</p>
                    <div class="slide-page-marker">${slide.order} / ${String(total).padStart(2, '0')}</div>
                </footer>
            </div>
        </article>
    `;
}

function renderNavItem(slide) {
    return `
        <button class="slide-nav-list__item" type="button" data-target="${slide.id}">
            <span class="slide-nav-list__order">${slide.order}</span>
            <span class="slide-nav-list__title">${slide.title}</span>
            <span class="slide-nav-list__note">${slide.navNote}</span>
        </button>
    `;
}

function setActiveSlide(slideId) {
    const navButtons = document.querySelectorAll('.slide-nav-list__item');
    const progressNode = document.getElementById('slide-progress');
    const activeIndex = SLIDES.findIndex((slide) => slide.id === slideId);

    navButtons.forEach((button) => {
        button.classList.toggle('is-active', button.dataset.target === slideId);
    });

    if (activeIndex >= 0 && progressNode) {
        progressNode.textContent = `${String(activeIndex + 1).padStart(2, '0')} / ${String(SLIDES.length).padStart(2, '0')}`;
    }
}

function bindNavigation() {
    const navButtons = document.querySelectorAll('.slide-nav-list__item');
    const navRoot = document.getElementById('report-nav');
    const mobileButton = document.getElementById('mobile-nav-button');
    const desktopToggleButton = document.getElementById('nav-toggle-button');
    const printButton = document.getElementById('print-button');

    navButtons.forEach((button) => {
        button.addEventListener('click', () => {
            const target = document.getElementById(button.dataset.target);
            if (!target) {
                return;
            }

            target.scrollIntoView({ behavior: 'smooth', block: 'start' });
            setActiveSlide(button.dataset.target);

            if (window.innerWidth <= 1180 && navRoot) {
                navRoot.classList.remove('is-open');
                mobileButton?.setAttribute('aria-expanded', 'false');
                if (mobileButton) {
                    mobileButton.textContent = '목차 보기';
                }
            }
        });
    });

    mobileButton?.addEventListener('click', () => {
        if (!navRoot) {
            return;
        }

        const nextState = !navRoot.classList.contains('is-open');
        navRoot.classList.toggle('is-open', nextState);
        mobileButton.setAttribute('aria-expanded', String(nextState));
        mobileButton.textContent = nextState ? '목차 닫기' : '목차 보기';
    });

    desktopToggleButton?.addEventListener('click', () => {
        if (!navRoot || window.innerWidth <= 1180) {
            return;
        }

        const nextState = !navRoot.classList.contains('is-collapsed');
        navRoot.classList.toggle('is-collapsed', nextState);
        desktopToggleButton.textContent = nextState ? '목차 펼치기' : '목차 접기';
        desktopToggleButton.setAttribute('aria-expanded', String(!nextState));
    });

    printButton?.addEventListener('click', () => {
        window.print();
    });
}

function observeSlides() {
    const slides = document.querySelectorAll('.report-slide');
    const observer = new IntersectionObserver(
        (entries) => {
            const visibleEntries = entries
                .filter((entry) => entry.isIntersecting)
                .sort((left, right) => right.intersectionRatio - left.intersectionRatio);

            if (!visibleEntries.length) {
                return;
            }

            setActiveSlide(visibleEntries[0].target.id);
        },
        {
            rootMargin: '-10% 0px -25% 0px',
            threshold: [0.2, 0.35, 0.55, 0.75]
        }
    );

    slides.forEach((slide) => observer.observe(slide));
}

function renderReport() {
    const deck = document.getElementById('slide-deck');
    const nav = document.getElementById('slide-nav-list');

    if (!deck || !nav) {
        return;
    }

    deck.innerHTML = SLIDES.map((slide) => renderSlide(slide, SLIDES.length)).join('');
    nav.innerHTML = SLIDES.map(renderNavItem).join('');
    setActiveSlide(SLIDES[0].id);
    bindNavigation();
    observeSlides();
}

document.addEventListener('DOMContentLoaded', renderReport);
