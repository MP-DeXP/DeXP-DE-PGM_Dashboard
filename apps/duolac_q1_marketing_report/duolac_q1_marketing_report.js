const PERIODS = {
    q1: 'Q1 누적 결과 2026.01.01 ~ 2026.03.31',
    march: '3월 보조 단면 2026.03.01 ~ 2026.03.31',
    latest: '최신 스냅샷 2026.04.08 ~ 2026.04.20'
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

const INFLOW = [
    { channel: 'naver / cpc', sessions: 5165, purchaseAmount: 59386100, rps: 11497.79 },
    { channel: 'naver / organic', sessions: 876, purchaseAmount: 6348400, rps: 7247.03 },
    { channel: 'google / cpc', sessions: 226, purchaseAmount: 1295400, rps: 5731.86 },
    { channel: '(direct) / (none)', sessions: 5995, purchaseAmount: 30446450, rps: 5078.64 },
    { channel: 'facebook / social', sessions: 1636, purchaseAmount: 1065450, rps: 651.25 },
    { channel: 'fbig / da', sessions: 8997, purchaseAmount: 354000, rps: 39.35 },
    { channel: 'criteo / display', sessions: 6529, purchaseAmount: 224500, rps: 34.39 }
];

const SITE_OBSERVATIONS = [
    '듀오락 홈페이지는 생애 맞춤 케어, 브랜드 스토어, 후기/상품 노출 중심이다.',
    '사이트 레벨에서는 생애주기 분류와 브랜드 분류가 전면에 보인다.'
];

function formatCount(value) {
    return Number(value).toLocaleString('ko-KR');
}

function formatCurrency(value) {
    return `${Number(value).toLocaleString('ko-KR')}원`;
}

function formatCompactCurrency(value) {
    if (Math.abs(value) >= 100000000) {
        return `${(value / 100000000).toFixed(2)}억 원`;
    }

    return `${Math.round(value / 10000).toLocaleString('ko-KR')}만 원`;
}

function formatRate(value, digits = 1) {
    return `${Number(value).toLocaleString('ko-KR', {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits
    })}%`;
}

function formatRatio(value, digits = 2) {
    return `${Number(value).toLocaleString('ko-KR', {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits
    })}배`;
}

function formatDays(value) {
    return `${value.toFixed(1)}일`;
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

function renderDiagnosticSection(slide) {
    return `
        <section class="diagnosis-grid" aria-label="진단 기준">
            <article class="diagnosis-panel">
                <p class="diagnosis-panel__label">판단</p>
                <p class="diagnosis-panel__value">${slide.judgment}</p>
            </article>
            <article class="diagnosis-panel">
                <p class="diagnosis-panel__label">사용 데이터</p>
                <p class="diagnosis-panel__value">${slide.dataScope}</p>
            </article>
            <article class="diagnosis-panel">
                <p class="diagnosis-panel__label">강한 판단</p>
                <p class="diagnosis-panel__value">${slide.strongJudgment}</p>
            </article>
        </section>
        <section class="prose-block" aria-label="진단 본문">
            ${slide.paragraphs.map((paragraph) => `<p>${paragraph}</p>`).join('')}
        </section>
        ${slide.reframeItems ? renderReframeSection(slide.reframeItems) : ''}
    `;
}

function renderReframeSection(items) {
    return `
        <section class="reframe-section" aria-label="기존 보고서와의 차이">
            <p class="reframe-section__eyebrow">이 진단서가 기존 보고서를 어떻게 뒤집는가</p>
            <div class="reframe-list">
                ${items.map((item, index) => `
                    <article class="reframe-item">
                        <span class="reframe-item__order">${index + 1}</span>
                        <p>${item}</p>
                    </article>
                `).join('')}
            </div>
        </section>
    `;
}

function buildSlides() {
    const q1Revenue = sumBy(PRODUCT_SALES_Q1, 'revenue');
    const q1Orders = sumBy(PRODUCT_SALES_Q1, 'orders');
    const goldItems = PRODUCT_SALES_Q1.filter((item) => item.name.includes('골드'));
    const goldRevenue = sumBy(goldItems, 'revenue');
    const goldOrders = sumBy(goldItems, 'orders');
    const goldRevenueShare = percentage(goldRevenue, q1Revenue);
    const goldOrderShare = percentage(goldOrders, q1Orders);
    const marchRevenueTotal = sumBy(PRODUCT_SALES_MARCH, 'revenue');
    const marchTop3Revenue = sumBy(PRODUCT_SALES_MARCH.slice(0, 3), 'revenue');
    const marchTop3Share = percentage(marchTop3Revenue, marchRevenueTotal);
    const coreLargestCustomers = RELATION_CORE.reduce((best, item) => (item.customers > best.customers ? item : best), RELATION_CORE[0]);
    const strongestTransition = [...RELATION_CORE, ...RELATION_LIFECYCLE].reduce((best, item) => (item.rate > best.rate ? item : best), RELATION_CORE[0]);
    const fastestTransition = [...RELATION_CORE, ...RELATION_LIFECYCLE].reduce((best, item) => (item.days < best.days ? item : best), RELATION_CORE[0]);
    const lifecycleStrongest = RELATION_LIFECYCLE.reduce((best, item) => (item.rate > best.rate ? item : best), RELATION_LIFECYCLE[0]);
    const participationByName = Object.fromEntries(PARTICIPATION.map((item) => [item.name, item]));
    const goldSetExposure = participationByName['[가정의달] 듀오락 골드세트 1개 + 10일분 증정'];
    const vitaminBundle = participationByName['[가정의달] 듀오락 비타면역꾸미 2개'];
    const dropsBundle = participationByName['[가정의달] 듀오락 듀오 디 드롭스 2개'];
    const yumyumSingle = participationByName['듀오락 얌얌플러스 1개'];
    const directJanuary = Q1_DIRECT[0];
    const directMarch = Q1_DIRECT[2];
    const directRevenueChange = percentage(directMarch.revenue - directJanuary.revenue, directJanuary.revenue);
    const directOrderChange = percentage(directMarch.orders - directJanuary.orders, directJanuary.orders);
    const naverCpc = INFLOW.find((item) => item.channel === 'naver / cpc');
    const directInflow = INFLOW.find((item) => item.channel === '(direct) / (none)');
    const fbigDa = INFLOW.find((item) => item.channel === 'fbig / da');
    const criteoDisplay = INFLOW.find((item) => item.channel === 'criteo / display');
    const naverVsDirect = naverCpc.rps / directInflow.rps;
    const naverVsFbig = naverCpc.rps / fbigDa.rps;
    const naverVsCriteo = naverCpc.rps / criteoDisplay.rps;
    const dropsClickRate = percentage(dropsBundle.purchaseClicks, dropsBundle.views);
    const vitaminClickRate = percentage(vitaminBundle.purchaseClicks, vitaminBundle.views);
    const goldSetClickRate = percentage(goldSetExposure.purchaseClicks, goldSetExposure.views);
    const yumyumClickRate = percentage(yumyumSingle.purchaseClicks, yumyumSingle.views);

    return [
        {
            id: 'slide-01',
            order: '01',
            title: '진단 목적과 범위',
            kicker: '진단서 초안',
            navNote: '무엇을 판정하고 어디까지 말할지',
            summary: '이 문서는 사이트 전면 분류와 실제 판매·전환 구조의 일치 여부만 판정하는 초안이며, Q1 누적 결과와 2026.04.08~04.20 최신 스냅샷을 분리해서 읽습니다.',
            periods: [PERIODS.q1, PERIODS.latest],
            metrics: [
                metric('문서 성격', '진단서 초안', '실행 제안서가 아니라 구조 일치 여부를 먼저 판정합니다.'),
                metric('Q1 결과 구간', '2026.01.01 ~ 03.31', '매출과 주문은 누적 결과로만 읽습니다.'),
                metric('최신 스냅샷 구간', '2026.04.08 ~ 04.20', '관계·참여·유입은 최신 단면으로만 읽습니다.')
            ],
            judgment: '사이트가 전면에서 보여주는 분류 언어와 실제로 데이터에 남는 판매·전환 언어가 같은지 판단하는 장이다.',
            dataScope: 'PRODUCT_SALES_Q1, PRODUCT_SALES_MARCH, RELATION_CORE, RELATION_LIFECYCLE, PARTICIPATION, Q1_DIRECT, INFLOW, 사용자 관찰 2문장만 사용한다.',
            strongJudgment: '이 초안은 Q1 누적 결과와 2026.04.08~04.20 최신 스냅샷을 하나의 흐름으로 합치지 않으며, 사이트 구조의 정합성 판정에만 범위를 한정한다.',
            paragraphs: [
                '핵심 쟁점은 단순히 어떤 상품이 많이 팔렸는지가 아니다. 사이트 전면에서는 생애주기 분류와 브랜드 분류가 동시에 보이는데, 실제 구매와 이동 데이터가 그 병렬 구조를 뒷받침하는지를 먼저 확인해야 한다.',
                '판단의 층위는 둘로 나뉜다. PRODUCT_SALES_Q1과 PRODUCT_SALES_MARCH는 2026년 1분기 누적 결과와 3월 보조 단면을 보여주고, RELATION_CORE·RELATION_LIFECYCLE·PARTICIPATION·INFLOW는 2026년 4월 8일부터 4월 20일까지의 최신 스냅샷만 보여준다.',
                '따라서 이 문서에서는 Q1 결과를 최신 관계·참여·유입의 원인으로 단정하지 않는다. 반대로 최신 스냅샷을 Q1 전체의 대표 구조처럼 확대하지도 않는다.'
            ],
            footer: '8장은 모두 진단 문법으로 작성했고, 원인 판정 없이 실행 문장을 앞세우지 않도록 재구성했습니다.',
            layoutClass: 'is-cover'
        },
        {
            id: 'slide-02',
            order: '02',
            title: '사이트 전면 구조에 대한 관찰',
            kicker: '사이트 관찰',
            navNote: '전면에서 무엇을 먼저 보여주는가',
            summary: '사용자 관찰 2문장만 놓고 보면 사이트 전면은 생애 맞춤 케어와 브랜드 스토어, 후기·상품 노출을 함께 밀고 있으며, 생애주기 분류와 브랜드 분류가 동시에 앞에 서 있다.',
            periods: ['사용자 관찰 기준'],
            metrics: [
                metric('관찰 1', '생애 맞춤 케어', '브랜드 스토어와 후기/상품 노출 중심이라는 진술과 함께 제시됐습니다.'),
                metric('관찰 2', '브랜드 스토어', '생애주기 분류와 병렬로 전면에 놓인 축으로 읽힙니다.'),
                metric('핵심 쟁점', '병렬 분류', '어느 축이 실제 전환의 주축인지 관찰만으로는 드러나지 않습니다.')
            ],
            judgment: '사용자가 사이트 첫 화면에서 어떤 분류 논리와 마주하는지를 판단하는 장이다.',
            dataScope: '사용자 제공 관찰 2문장만 사용한다.',
            strongJudgment: '현재 전면 구조는 생애주기와 브랜드를 병렬로 내세우지만, 이 관찰만으로는 실제 구매 이동을 설명하는 중심 축이 무엇인지 확인되지 않는다.',
            paragraphs: [
                `${SITE_OBSERVATIONS[0]} ${SITE_OBSERVATIONS[1]}`,
                '이 두 문장을 함께 읽으면 사이트는 하나의 축으로 몰아주기보다, 생애주기와 브랜드를 동시에 전면 언어로 쓰고 있다고 볼 수 있다. 즉 사용자는 처음부터 병렬 분류를 통과해 상품으로 들어가게 된다.',
                '문제는 병렬 분류가 있다는 사실 자체가 아니라, 그 병렬 구조가 실제 판매 집중과 상품 간 이동 구조를 설명하느냐이다. 이 장에서는 아직 그 일치 여부를 말하지 않고, 사이트 전면이 무엇을 먼저 보여주는지만 고정한다.'
            ],
            footer: '이 장은 관찰을 고정하는 페이지이며, 데이터 판정은 다음 장부터 시작합니다.',
            layoutClass: 'is-summary'
        },
        {
            id: 'slide-03',
            order: '03',
            title: 'Q1 판매 구조 진단',
            kicker: '판매 구조',
            navNote: 'Q1 누적 결과와 3월 보조 단면',
            summary: `Q1 상위 12개 기준 매출 ${formatCompactCurrency(q1Revenue)}, 주문 ${formatCount(q1Orders)}건 가운데 이름상 골드가 포함된 상품이 매출 ${formatRate(goldRevenueShare)}, 주문 ${formatRate(goldOrderShare)}를 차지해 전면의 병렬 분류보다 훨씬 좁은 수렴 구조를 보입니다.`,
            periods: [PERIODS.q1, PERIODS.march],
            metrics: [
                metric('Q1 상위 12개', formatCompactCurrency(q1Revenue), `주문 ${formatCount(q1Orders)}건`),
                metric('골드 포함 비중', formatRate(goldRevenueShare), `주문 비중 ${formatRate(goldOrderShare)}`),
                metric('3월 상위 3개 집중', formatRate(marchTop3Share), '3월 매출 합계 안에서 상위 3개가 차지한 비중입니다.')
            ],
            judgment: 'Q1 누적 매출과 주문이 실제로 어느 쪽에 몰려 있는지 판단하는 장이다.',
            dataScope: 'PRODUCT_SALES_Q1과 PRODUCT_SALES_MARCH만 사용한다.',
            strongJudgment: 'Q1 판매 구조는 브랜드와 생애주기의 병렬 분산이라기보다 골드 계열 집중에 가깝고, 3월 단면도 그 수렴을 크게 벗어나지 않는다.',
            paragraphs: [
                `PRODUCT_SALES_Q1 상위 12개를 보면 매출 ${formatCurrency(q1Revenue)}, 주문 ${formatCount(q1Orders)}건 중 골드가 이름에 들어간 상품이 매출 ${formatCurrency(goldRevenue)}, 주문 ${formatCount(goldOrders)}건을 만든다. 이는 매출 ${formatRate(goldRevenueShare)}, 주문 ${formatRate(goldOrderShare)}에 해당한다.`,
                `즉 사이트 전면에서는 생애주기 분류와 브랜드 분류가 함께 보이더라도, 실제 Q1 결과는 넓게 퍼진 병렬 구조보다 한 축으로 강하게 수렴한다. 얌얌 2개나 듀오 디 드롭스 2개 같은 다른 계열이 존재해도 누적 결과의 중심은 골드 축에서 크게 벗어나지 않는다.`,
                `PRODUCT_SALES_MARCH에서도 상위 3개 매출 비중이 ${formatRate(marchTop3Share)}로 잡혀 있어, 3월 보조 단면 역시 분산보다 집중에 가깝다. 따라서 사이트 전면의 병렬 분류를 그대로 판매 구조로 읽으면 실제 집중도를 과소평가하게 된다.`
            ],
            footer: '이 장은 판매 결과만 본 판정이며, 전환 허브와 동일시하지 않습니다.',
            layoutClass: 'is-portfolio'
        },
        {
            id: 'slide-04',
            order: '04',
            title: '상품 간 전환 구조 진단',
            kicker: '전환 구조',
            navNote: '판매 상위와 전환 허브의 분리',
            summary: `최신 관계 스냅샷에서는 ${coreLargestCustomers.from}에서 ${coreLargestCustomers.to}로 ${formatCount(coreLargestCustomers.customers)}명이 이동했고, 가장 높은 전환율은 ${strongestTransition.from}에서 ${strongestTransition.to}로 ${formatRate(strongestTransition.rate)}입니다. 판매 상위와 전환 허브는 같은 의미가 아닙니다.`,
            periods: [PERIODS.latest],
            metrics: [
                metric('최대 전환 고객수', `${formatCount(coreLargestCustomers.customers)}명`, `${coreLargestCustomers.from} → ${coreLargestCustomers.to}`),
                metric('최고 전환율', formatRate(strongestTransition.rate), `${strongestTransition.from} → ${strongestTransition.to}`),
                metric('가장 빠른 이동', formatDays(fastestTransition.days), `${fastestTransition.from} → ${fastestTransition.to}`)
            ],
            judgment: '실제 상품 간 이동이 어떤 결절을 중심으로 생기는지 판단하는 장이다.',
            dataScope: 'RELATION_CORE와 RELATION_LIFECYCLE만 사용하며, 기간은 2026.04.08 ~ 2026.04.20 최신 스냅샷으로 한정한다.',
            strongJudgment: '판매 상위 상품과 전환 허브 상품은 같지 않다. 최신 관계 스냅샷에서 허브는 체험 상품과 1개 상품이며, 이들이 2개 번들·본품·대용량으로 이동시키는 중간 결절로 작동한다.',
            paragraphs: [
                `RELATION_CORE에서는 [50%체험] 듀오락 골드캡슐 10일이 듀오락 골드캡슐 1개로 ${formatCount(RELATION_CORE[1].customers)}명, ${formatRate(RELATION_CORE[1].rate)} 이동하고, 듀오락 골드캡슐 1개는 30일분 2개로 ${formatCount(RELATION_CORE[0].customers)}명, ${formatRate(RELATION_CORE[0].rate)} 이동한다. 같은 1개 상품이 3개 구성으로도 ${formatCount(RELATION_CORE[5].customers)}명 이동시키므로, 허브는 최종 대용량이 아니라 중간 단계에 있다.`,
                `하루 한 포 계열에서도 가장 높은 전환율은 듀오락 골드 하루 한 포 1개에서 [집중케어] 2개 + 10일로 가는 ${formatRate(RELATION_CORE[2].rate)}다. 이 수치는 판매 순위가 아니라 후속 이동을 만드는 경로의 두께를 말한다.`,
                `RELATION_LIFECYCLE 역시 같은 문법을 보인다. 얌얌플러스와 베이비는 체험 또는 1개에서 2개 번들로 이어지고, 가장 높은 생애주기 전환율도 ${lifecycleStrongest.from}에서 ${lifecycleStrongest.to}로 가는 ${formatRate(lifecycleStrongest.rate)}다. 즉 최신 전환 구조의 언어는 브랜드 분류보다 체험·1개·2개 같은 단계명에 더 가깝다.`
            ],
            footer: '이 장의 허브는 전환 결절을 뜻하며, 판매 상위 상품과 같은 말로 쓰지 않았습니다.',
            layoutClass: 'is-relation is-relation-strong'
        },
        {
            id: 'slide-05',
            order: '05',
            title: '참여 단계 마찰 진단',
            kicker: '참여 단계',
            navNote: '노출 전면성과 행동 전면성의 차이',
            summary: `최신 참여 스냅샷에서는 ${goldSetExposure.name}이 조회 ${formatCount(goldSetExposure.views)}로 가장 크게 노출되지만 구매클릭률은 ${formatRate(goldSetClickRate, 2)}이고, ${dropsBundle.name}은 조회 ${formatCount(dropsBundle.views)}에도 구매클릭률 ${formatRate(dropsClickRate, 2)}를 보여 노출 규모와 행동 밀도가 분리됩니다.`,
            periods: [PERIODS.latest],
            metrics: [
                metric('최다 노출', formatCount(goldSetExposure.views), goldSetExposure.name),
                metric('최고 구매클릭률', formatRate(dropsClickRate, 2), dropsBundle.name),
                metric('낮은 행동률', formatRate(vitaminClickRate, 2), vitaminBundle.name)
            ],
            judgment: '전면 노출 이후 어떤 상품에서 행동이 끊기고, 어떤 상품에서 행동이 몰리는지 판단하는 장이다.',
            dataScope: 'PARTICIPATION만 사용하며, 기간은 2026.04.08 ~ 2026.04.20 최신 스냅샷이다.',
            strongJudgment: '현재 노출 전면성과 행동 전면성은 같지 않다. 많이 본 상품이 아니라 구매클릭률이 높은 상품이 따로 있고, 이 차이가 상세 진입 직전의 마찰을 보여준다.',
            paragraphs: [
                `${goldSetExposure.name}은 조회 ${formatCount(goldSetExposure.views)}로 가장 크게 노출되지만 구매클릭은 ${formatCount(goldSetExposure.purchaseClicks)}건, 조회 대비 ${formatRate(goldSetClickRate, 2)}에 그친다. 큰 노출이 곧바로 강한 행동으로 이어진다고 보기는 어렵다.`,
                `${vitaminBundle.name}은 조회 ${formatCount(vitaminBundle.views)}, 유효조회 ${formatCount(vitaminBundle.validViews)}로 전면 노출이 적지 않지만 장바구니 ${formatCount(vitaminBundle.carts)}, 구매클릭 ${formatCount(vitaminBundle.purchaseClicks)}에 머문다. 조회 대비 구매클릭률은 ${formatRate(vitaminClickRate, 2)}로 매우 낮아, 전면 노출 이후 설명력이나 후속 선택 유도가 약하다는 신호로 읽힌다.`,
                `${dropsBundle.name}은 조회 ${formatCount(dropsBundle.views)}에도 구매클릭 ${formatCount(dropsBundle.purchaseClicks)}건, 조회 대비 ${formatRate(dropsClickRate, 2)}를 보인다. ${yumyumSingle.name}도 구매클릭률 ${formatRate(yumyumClickRate, 2)}로 비교적 높다. 즉 현재 사이트 전면은 많이 보이는 상품과 실제 행동을 끌어내는 상품을 같은 위치에 놓고 있지 않을 가능성이 크다.`
            ],
            footer: '이 장은 판매 결과가 아니라 참여 단계의 마찰만 다룹니다.',
            layoutClass: 'is-highlight'
        },
        {
            id: 'slide-06',
            order: '06',
            title: '유입 및 direct 성과의 해석 범위',
            kicker: '유입 해석',
            navNote: '결과 지표와 최신 효율 신호의 분리',
            summary: `Q1 direct 주문 매출은 2026년 1월 ${formatCompactCurrency(directJanuary.revenue)}에서 2026년 3월 ${formatCompactCurrency(directMarch.revenue)}으로 ${formatRate(directRevenueChange)} 변했고, 최신 스냅샷에서는 naver / cpc의 세션당 매출이 direct 대비 ${formatRatio(naverVsDirect)}입니다.`,
            periods: [PERIODS.q1, PERIODS.latest],
            metrics: [
                metric('direct 매출 변화', formatRate(directRevenueChange), '2026-01 대비 2026-03'),
                metric('direct 주문 변화', formatRate(directOrderChange), '2026-01 대비 2026-03'),
                metric('naver / cpc 우위', formatRatio(naverVsDirect), '최신 세션당 매출 기준 direct 대비')
            ],
            judgment: 'Q1 direct 실적과 최신 inflow 효율을 어디까지 연결할 수 있는지 판단하는 장이다.',
            dataScope: 'Q1_DIRECT와 INFLOW만 사용한다.',
            strongJudgment: 'Q1 direct와 2026.04.08~04.20 inflow는 서로 다른 층위다. direct 감소는 결과 변화이고, naver / cpc 우위는 최신 세션 효율 신호일 뿐이며 둘을 하나의 원인 사슬로 잇기 어렵다.',
            paragraphs: [
                `Q1_DIRECT를 보면 direct 주문 매출은 2026년 1월 ${formatCurrency(directJanuary.revenue)}에서 2026년 3월 ${formatCurrency(directMarch.revenue)}으로 ${formatRate(directRevenueChange)} 변했고, 주문 수 역시 ${formatRate(directOrderChange)} 변했다. 이 값은 분기 결과의 변화량이지, 사이트 분류 구조의 원인을 직접 말해주지는 않는다.`,
                `INFLOW의 최신 스냅샷에서는 naver / cpc의 세션당 매출이 ${naverCpc.rps.toLocaleString('ko-KR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}이고, direct는 ${directInflow.rps.toLocaleString('ko-KR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}다. 최신 시점에서는 naver / cpc가 direct 대비 ${formatRatio(naverVsDirect)}, fbig / da 대비 ${formatRatio(naverVsFbig, 0)}, criteo / display 대비 ${formatRatio(naverVsCriteo, 0)}로 앞선다.`,
                '따라서 유입 데이터는 사이트 구조의 정합성을 판정하는 보조 신호로만 써야 한다. 결과 집계인 Q1 direct와 최신 효율 스냅샷을 섞어 하나의 개선 방향으로 묶으면, 어떤 문제가 사이트 구조에서 발생했고 어떤 문제가 유입 믹스에서 발생했는지 분리하기 어렵다.'
            ],
            footer: '이 장은 유입을 구조 진단의 보조값으로만 취급합니다.',
            layoutClass: 'is-sky'
        },
        {
            id: 'slide-07',
            order: '07',
            title: '사이트 구조와 데이터 구조의 불일치 진단',
            kicker: '핵심 쟁점',
            navNote: '병렬 분류와 실제 이동 언어의 차이',
            summary: `사이트 전면은 생애주기 분류와 브랜드 분류를 함께 보여주지만, Q1 판매는 골드 포함 상품 매출 ${formatRate(goldRevenueShare)}로 수렴하고 최신 전환은 체험·1개·2개 중심의 단계 구조를 보입니다. 전면 분류의 언어와 실제 이동의 언어가 다릅니다.`,
            periods: [PERIODS.q1, PERIODS.latest],
            metrics: [
                metric('사이트 전면 축', '2개', '생애주기 분류와 브랜드 분류가 동시에 앞에 섭니다.'),
                metric('Q1 판매 수렴', formatRate(goldRevenueShare), '골드가 이름에 들어간 상품의 매출 비중입니다.'),
                metric('최신 이동 언어', '체험 · 1개 · 2개', '분류명보다 단계명이 반복됩니다.')
            ],
            judgment: '사이트 전면 분류가 실제 판매 구조와 전환 구조를 설명하는지 판단하는 장이다.',
            dataScope: '사용자 관찰 2문장, PRODUCT_SALES_Q1, PRODUCT_SALES_MARCH, RELATION_CORE, RELATION_LIFECYCLE, PARTICIPATION만 사용한다.',
            strongJudgment: '사이트는 브랜드와 생애주기를 병렬 분류로 보여주지만, 데이터는 골드 집중과 단계형 이동을 더 강하게 보여준다. 즉 전면 분류의 언어와 실제 전환의 언어가 서로 다르다.',
            paragraphs: [
                '사이트 전면 관찰만 놓고 보면 사용자는 생애주기와 브랜드라는 두 개의 병렬 축을 따라 상품을 찾게 된다. 그러나 Q1 판매 구조는 그 병렬성을 그대로 재현하지 않고 한쪽으로 더 강하게 수렴한다.',
                `판매 측면에서는 골드가 이름에 들어간 상품이 매출 ${formatRate(goldRevenueShare)}를 차지한다. 전환 측면에서는 체험 상품과 1개 상품이 2개 번들·본품·대용량으로 사람을 이동시키는 결절로 반복 등장한다. 즉 실제 데이터의 중심 언어는 브랜드나 생애주기 자체보다 체험, 1개, 2개, 번들 같은 단계 표시에 더 가깝다.`,
                `참여 단계에서도 불일치가 이어진다. 전면 노출이 큰 ${goldSetExposure.name}과 ${vitaminBundle.name}이 항상 강한 행동을 만들지는 않고, 오히려 ${dropsBundle.name}처럼 더 작은 노출에서 높은 클릭 밀도가 나온다. 이는 현재 전면 구조가 전환 허브를 앞에 세우는 방식으로 조직돼 있지 않을 가능성을 보여준다.`
            ],
            footer: '이 문서의 핵심 판정은 분류 축의 개수보다 전환 언어의 정합성에 있습니다.',
            layoutClass: 'is-summary'
        },
        {
            id: 'slide-08',
            order: '08',
            title: '우선 재설계 과제',
            kicker: '재설계 과제',
            navNote: '무엇부터 다시 그려야 하는가',
            summary: '우선 과제는 분류를 더 늘리는 일이 아니라, 사이트 전면과 상품 상세와 후속 진입 지점이 같은 전환 단계 언어를 쓰도록 다시 맞추는 것입니다.',
            periods: [PERIODS.q1, PERIODS.latest],
            metrics: [
                metric('재설계 시작점', '전환 단계 정렬', '분류 추가보다 먼저 맞춰야 하는 기준입니다.'),
                metric('구분 원칙', '판매 상위 ≠ 전환 허브', '행동 상위도 별도 층위로 분리합니다.'),
                metric('판독 원칙', 'Q1 결과와 최신 스냅샷 분리', '한 추세선으로 묶지 않습니다.')
            ],
            judgment: '앞선 진단을 바탕으로 무엇을 먼저 다시 그려야 하는지 판단하는 장이다.',
            dataScope: '1~7장에서 사용한 동일 데이터만 다시 묶어 말한다.',
            strongJudgment: '우선 과제는 더 많은 분류를 추가하는 일이 아니라, 사이트 전면·상품 상세·후속 진입 지점이 같은 전환 단계 언어를 쓰게 만드는 일이다.',
            paragraphs: [
                '첫째, 사이트 전면에서 브랜드와 생애주기를 병렬로 나열하는 구조를 유지할지보다, 실제 이동이 확인된 체험 → 1개 → 2개/번들 → 대용량 단계를 어디에서 드러낼지부터 다시 정의해야 한다. 현재 데이터는 분류명보다 단계명이 실제 이동을 더 잘 설명한다.',
                '둘째, 판매 상위 상품과 전환 허브 상품과 행동 밀도 상위 상품을 같은 기준으로 다루지 말아야 한다. Q1 판매 상단에 있는 상품, 최신 관계에서 다음 상품으로 사람을 보내는 허브, 최신 참여에서 클릭이 붙는 상품이 서로 다르기 때문이다.',
                '셋째, 판독 체계도 분리해야 한다. Q1 누적은 결과 구조를 읽는 층위로 두고, 2026년 4월 8일부터 4월 20일까지의 최신 관계·참여·유입은 이동과 마찰을 읽는 층위로 남겨야 한다.',
                '넷째, 유입은 구조 판정의 보조값으로만 두는 것이 맞다. direct 변화와 최신 inflow 효율 차이는 중요하지만, 그것만으로 사이트 분류 구조의 적합성을 대신 설명할 수는 없다.'
            ],
            reframeItems: [
                '기존 보고서는 상품 역할과 실행 우선순위를 먼저 말했다. 이번 초안은 사이트 분류와 데이터 구조의 일치 여부를 먼저 판정한다.',
                '기존 보고서는 판매 상위 상품을 중심으로 읽었다. 이번 초안은 판매 상위, 전환 허브, 행동 밀도 상위를 서로 다른 층위로 분리한다.',
                '기존 보고서는 Q1 결과와 최신 스냅샷을 한 흐름처럼 읽기 쉬웠다. 이번 초안은 2026.01.01~03.31과 2026.04.08~04.20을 명시적으로 나눈다.',
                '기존 보고서는 실행 문장이 앞에 있었다. 이번 초안은 원인 진단 뒤에만 재설계 과제를 남긴다.',
                '기존 보고서는 상품 관계를 바로 제안 근거로 사용했다. 이번 초안은 전면 분류의 언어와 실제 이동의 언어가 다르다는 점을 핵심 문제로 둔다.'
            ],
            footer: '우선 재설계 과제는 모두 앞선 진단에서 확인된 원인 위에만 올렸습니다.',
            layoutClass: 'is-closing'
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

                <section class="slide-card__metric-grid" aria-label="핵심 판단 보조 지표">
                    ${slide.metrics.map(renderMetricCard).join('')}
                </section>

                <section class="slide-card__body" aria-label="슬라이드 본문 콘텐츠">
                    ${renderDiagnosticSection(slide)}
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
