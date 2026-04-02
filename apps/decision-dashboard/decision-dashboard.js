const REPRESENTATIVE_ISSUE_ID = 'long-aged-inventory';
const SIDEBAR_STORAGE_KEY = 'pgm_sidebar_collapsed';
const INBOX_DEPARTMENT_FILTERS = Object.freeze([
    { value: 'all', label: '전체' },
    { value: 'operations', label: '운영' },
    { value: 'md', label: 'MD' },
    { value: 'marketing', label: '마케팅' },
    { value: 'sc', label: 'SC' }
]);
const DEFAULT_FILTERS = Object.freeze({
    store: 'all',
    brand: 'all',
    category: 'all',
    thresholdDays: '21'
});

const DASHBOARD_STATE = {
    screen: 'inbox',
    inboxDepartment: 'all',
    selectedIssueId: REPRESENTATIVE_ISSUE_ID,
    selectedSkuId: 'MX-OW-214',
    filters: { ...DEFAULT_FILTERS },
    sidebarCollapsed: false
};

const ISSUE_CARDS = Object.freeze([
    {
        id: REPRESENTATIVE_ISSUE_ID,
        featured: true,
        title: '장기 체류 / 무판매 재고 집중',
        kicker: '가장 먼저 볼 문제',
        severity: 'High',
        signalLead: '21일 넘게 안 팔린 SKU 9개',
        signalDetail: '재고 575개가 6개 매장에 묶여 있습니다',
        departments: ['MD', '운영'],
        metricLabel: '잠재 손실 재고',
        metricValue: '575개',
        metricNote: '6개 매장 동시 영향',
        metricTone: 'critical',
        ctaLabel: '지금 바로 보기',
        deepDiveEnabled: true
    },
    {
        id: 'store-imbalance',
        title: '특정 매장 판매 저조 / 재고 불균형',
        kicker: '우선 판단 필요',
        severity: 'High',
        signalLead: '핵심 SKU 14개가 4개 매장에 쏠림',
        signalDetail: '반응 낮은 매장에 재고가 묶여 판매 기회를 놓치고 있습니다',
        departments: ['MD', '운영'],
        metricLabel: '편중 매장',
        metricValue: '4개',
        metricNote: '우선 판단 필요',
        metricTone: 'critical',
        ctaLabel: '우선 판단 필요',
        deepDiveEnabled: false
    },
    {
        id: 'dispatch-drop',
        title: '출고율 저하로 판매 기회 손실 위험',
        kicker: '즉시 확인 필요',
        severity: 'High',
        signalLead: '출고 지연 18건, 판매 기회 손실 확산',
        signalDetail: '반응 있는 SKU도 제때 매장에 못 들어가고 있습니다',
        departments: ['운영', 'SC'],
        metricLabel: '출고 지연',
        metricValue: '18건',
        metricNote: '즉시 확인 필요',
        metricTone: 'critical',
        ctaLabel: '즉시 확인 필요',
        deepDiveEnabled: false
    },
    {
        id: 'season-progress',
        title: '시즌 판매 진척 이상징후',
        kicker: '추가 확인 필요',
        severity: 'Medium',
        signalLead: '시즌 3개 카테고리, 목표 대비 -12%',
        signalDetail: '회복이 늦어지면 시즌 재고 부담이 빠르게 커집니다',
        departments: ['마케팅', '운영'],
        metricLabel: '진척 차이',
        metricValue: '-12%',
        metricNote: '추가 확인 필요',
        metricTone: 'warning',
        ctaLabel: '추가 확인 필요',
        deepDiveEnabled: false
    },
    {
        id: 'reorder-warning',
        title: '발주 기준 경고 / 안전재고 미감지',
        kicker: '조치 후보',
        severity: 'Medium',
        signalLead: '안전재고 미감지 SKU 7개',
        signalDetail: '판매가 이어지는 상품이 경고 없이 기준 아래로 내려가고 있습니다',
        departments: ['SC', 'MD'],
        metricLabel: '경고 SKU',
        metricValue: '7개',
        metricNote: '조치 후보',
        metricTone: 'warning',
        ctaLabel: '조치 후보',
        deepDiveEnabled: false
    },
    {
        id: 'price-resistance',
        title: '가격 저항 구간 의심 SKU',
        kicker: '추가 확인 필요',
        severity: 'Medium',
        signalLead: '전환 하락 -9.4%, 가격 저항 의심',
        signalDetail: '유입은 유지되지만 구매 직전 이탈이 커지고 있습니다',
        departments: ['MD', '마케팅'],
        metricLabel: '전환 하락',
        metricValue: '-9.4%',
        metricNote: '추가 확인 필요',
        metricTone: 'warning',
        ctaLabel: '추가 확인 필요',
        deepDiveEnabled: false
    }
]);

const SKU_CANDIDATES = Object.freeze([
    {
        skuId: 'MX-OW-214',
        productName: '에어리 라이트 재킷',
        brand: 'MERCURY ESSENTIAL',
        category: '아우터',
        receivedDate: '2026-02-24',
        daysInStock: 38,
        totalSales: 0,
        currentStock: 124,
        priority: 'High',
        recommendedAction: '우선 할인 검토',
        rationale: '입고 후 38일이 지났는데도 판매가 없고, 5개 매장에 재고가 넓게 퍼져 있어 가격 반응을 먼저 확인해야 해요.',
        recentSignal: '최근 14일 판매 0건, 주요 매장 피팅 문의도 함께 줄었어요.',
        categoryBaselineDays: 17,
        actionReason: '카테고리 평균 체류일보다 21일 길고 초기 진열 반응도 없어서 짧은 할인 실험이 가장 빠른 판단 옵션이에요.',
        storeBreakdown: [
            { store: '강남 플래그십', stock: 34, recentSales: 0, note: '주말 유입은 있었지만 구매 전환이 없어요.' },
            { store: '성수 라운지', stock: 27, recentSales: 0, note: '신규 유입 대비 피팅 후 이탈이 반복돼요.' },
            { store: '잠실 롯데월드몰', stock: 22, recentSales: 0, note: '유사 가격대 대비 반응이 약해요.' },
            { store: '판교 현대백화점', stock: 18, recentSales: 0, note: '메인 진열에서도 판매가 안 나와요.' },
            { store: '부산 센텀시티', stock: 23, recentSales: 0, note: '봄 아우터군 평균보다 체류가 길어요.' }
        ],
        actionOptions: [
            { label: '우선 할인 검토', reason: '28일 이상 판매 0 상태가 이어져 짧은 가격 테스트로 반응을 먼저 확인해요.', tone: 'primary' },
            { label: '타 매장 이동 검토', reason: '현재 재고가 5개 매장에 퍼져 있어 반응이 상대적으로 나은 매장 중심으로 먼저 재배치해요.', tone: 'secondary' },
            { label: '추가 입고 중단 검토', reason: '현재 재고만으로도 4주 이상 커버가 가능해 당장 추가 물량을 멈춰도 돼요.', tone: 'secondary' }
        ]
    },
    {
        skuId: 'MX-TP-118',
        productName: '소프트 레이어드 셔츠',
        brand: 'MERCURY ESSENTIAL',
        category: '상의',
        receivedDate: '2026-02-27',
        daysInStock: 35,
        totalSales: 0,
        currentStock: 89,
        priority: 'High',
        recommendedAction: '진열 위치 변경 필요',
        rationale: '유입은 유지되는데 상위 동선에서 반응이 붙지 않아, 진열 위치와 코디 제안부터 손보는 게 좋아요.',
        recentSignal: '최근 7일 판매 0건, 동일 브랜드 셔츠군 평균 체류 대비 15일 더 길어요.',
        categoryBaselineDays: 20,
        actionReason: '가격보다 노출 위치 문제 가능성이 커 보여서 진열 위치와 착장 제안을 먼저 조정해보는 흐름이 적합해요.',
        storeBreakdown: [
            { store: '강남 플래그십', stock: 25, recentSales: 0, note: '보조 진열 존에만 걸려 있어 노출이 낮아요.' },
            { store: '잠실 롯데월드몰', stock: 18, recentSales: 0, note: '피팅 후 상위 가격대 셔츠로 이탈해요.' },
            { store: '판교 현대백화점', stock: 24, recentSales: 0, note: '신규 고객 유입 대비 체류가 길어요.' },
            { store: '대전 타임월드', stock: 22, recentSales: 0, note: '추천 코디 제안 없이 단독 노출돼요.' }
        ],
        actionOptions: [
            { label: '진열 위치 변경 필요', reason: '현재는 보조 동선에만 걸려 있어 메인 셔츠 존으로 노출을 끌어올려야 해요.', tone: 'primary' },
            { label: '타 매장 이동 검토', reason: '판교·대전보다 강남 반응이 상대적으로 좋아 유입 많은 매장으로 물량을 더 모아봐요.', tone: 'secondary' },
            { label: '우선 할인 검토', reason: '진열 변경 후에도 1주 반응이 없으면 다음 단계로 가격 테스트를 붙여요.', tone: 'secondary' }
        ]
    },
    {
        skuId: 'MX-BT-302',
        productName: '릴랙스 테이퍼드 팬츠',
        brand: 'MERCURY STUDIO',
        category: '하의',
        receivedDate: '2026-03-01',
        daysInStock: 33,
        totalSales: 0,
        currentStock: 76,
        priority: 'High',
        recommendedAction: '타 매장 이동 검토',
        rationale: '재고가 반응이 약한 3개 매장에 몰려 있어, 판매 기회가 있는 매장으로 먼저 재배치하는 판단이 필요해요.',
        recentSignal: '최근 14일 판매 0건, 하의 카테고리 평균 대비 체류가 14일 길어요.',
        categoryBaselineDays: 19,
        actionReason: '같은 핏군 팬츠 중 반응 좋은 매장이 분명해 보여서 가격보다 재배치가 먼저예요.',
        storeBreakdown: [
            { store: '성수 라운지', stock: 29, recentSales: 0, note: '피팅은 있으나 구매 전환이 거의 없어요.' },
            { store: '잠실 롯데월드몰', stock: 21, recentSales: 0, note: '동일 가격대 팬츠와 경쟁 중이에요.' },
            { store: '부산 센텀시티', stock: 26, recentSales: 0, note: '주말 객수 대비 팬츠 전환이 낮아요.' }
        ],
        actionOptions: [
            { label: '타 매장 이동 검토', reason: '현재 보유가 3개 매장에만 몰려 있어 반응 좋은 매장으로 재배치해 빠르게 테스트해요.', tone: 'primary' },
            { label: '진열 위치 변경 필요', reason: '재배치 전후로 하의 메인 존에 다시 배치해 비교하면 원인 판단이 쉬워져요.', tone: 'secondary' },
            { label: '추가 입고 중단 검토', reason: '현재 재고가 충분해 재배치 결과가 나오기 전까지는 추가 발주를 멈춰도 돼요.', tone: 'secondary' }
        ]
    },
    {
        skuId: 'MX-OW-251',
        productName: '스프링 하프 트렌치',
        brand: 'MERCURY STUDIO',
        category: '아우터',
        receivedDate: '2026-03-03',
        daysInStock: 31,
        totalSales: 0,
        currentStock: 68,
        priority: 'High',
        recommendedAction: '우선 할인 검토',
        rationale: '시즌 핵심 아우터인데 초기 반응이 전혀 없어서, 더 늦기 전에 가격 반응과 메시지를 같이 점검해야 해요.',
        recentSignal: '최근 7일 판매 0건, 동일 시즌 아우터 대비 반응 하락폭이 커요.',
        categoryBaselineDays: 18,
        actionReason: '시즌성이 있는 상품이라 대기보다 빠른 반응 확인이 중요해요.',
        storeBreakdown: [
            { store: '강남 플래그십', stock: 18, recentSales: 0, note: '메인 쇼윈도 노출 후에도 판매가 없어요.' },
            { store: '성수 라운지', stock: 14, recentSales: 0, note: '비슷한 가격대 경량 아우터로 수요가 이동해요.' },
            { store: '판교 현대백화점', stock: 17, recentSales: 0, note: '피팅 문의가 빠르게 줄었어요.' },
            { store: '대전 타임월드', stock: 19, recentSales: 0, note: '할인 문의만 있고 정상가 전환은 없어요.' }
        ],
        actionOptions: [
            { label: '우선 할인 검토', reason: '시즌 중반 전까지 짧은 가격 실험으로 반응 회복 여부를 먼저 확인해요.', tone: 'primary' },
            { label: '진열 위치 변경 필요', reason: '외투 메인 존에서 메시지를 다시 정리해 보조 할인과 함께 테스트해요.', tone: 'secondary' },
            { label: '추가 입고 중단 검토', reason: '시즌 반응이 불확실하니 현재 보유분으로만 운영해요.', tone: 'secondary' }
        ]
    },
    {
        skuId: 'MX-SH-090',
        productName: '데일리 러너 스니커즈',
        brand: 'MERCURY ACTIVE',
        category: '신발',
        receivedDate: '2026-03-05',
        daysInStock: 29,
        totalSales: 0,
        currentStock: 57,
        priority: 'High',
        recommendedAction: '진열 위치 변경 필요',
        rationale: '핵심 사이즈가 아직 남아 있는데도 반응이 없어서, 동선과 착화 유도 메시지를 먼저 손봐야 해요.',
        recentSignal: '최근 14일 판매 0건, 신발군 평균 체류일보다 12일 길어요.',
        categoryBaselineDays: 17,
        actionReason: '재고는 충분하지만 가격보다 체험 유도가 부족한 상황으로 보여요.',
        storeBreakdown: [
            { store: '잠실 롯데월드몰', stock: 29, recentSales: 0, note: '착화 유도 POP 없이 벽면에만 노출돼요.' },
            { store: '부산 센텀시티', stock: 28, recentSales: 0, note: '신상 러닝화군 내부 경쟁이 커요.' }
        ],
        actionOptions: [
            { label: '진열 위치 변경 필요', reason: '착화 유도가 가능한 존으로 옮기고 러닝 카테고리 메인에 다시 붙여봐요.', tone: 'primary' },
            { label: '우선 할인 검토', reason: '진열 변경 1주 뒤에도 반응이 없으면 빠른 가격 실험으로 이어가요.', tone: 'secondary' },
            { label: '타 매장 이동 검토', reason: '러닝 카테고리 강점이 큰 매장으로 일부 물량을 이동해 반응을 비교해요.', tone: 'secondary' }
        ]
    },
    {
        skuId: 'MX-TP-084',
        productName: '시어 니트 풀오버',
        brand: 'MERCURY ESSENTIAL',
        category: '상의',
        receivedDate: '2026-03-07',
        daysInStock: 27,
        totalSales: 0,
        currentStock: 51,
        priority: 'Medium',
        recommendedAction: '우선 할인 검토',
        rationale: '노출은 충분했지만 3주 넘게 판매가 없어, 가벼운 가격 조정으로 반응을 확인해볼 시점이에요.',
        recentSignal: '최근 7일 판매 0건, 카테고리 평균보다 체류가 9일 길어요.',
        categoryBaselineDays: 18,
        actionReason: '초기 관심이 빠르게 꺾여서 메시지보다 가격 민감도 확인이 더 시급해 보여요.',
        storeBreakdown: [
            { store: '강남 플래그십', stock: 17, recentSales: 0, note: '동일 소재군보다 체류가 길어요.' },
            { store: '성수 라운지', stock: 16, recentSales: 0, note: '컬러 반응이 예상보다 낮아요.' },
            { store: '판교 현대백화점', stock: 18, recentSales: 0, note: '상위 가격 니트로 이탈해요.' }
        ],
        actionOptions: [
            { label: '우선 할인 검토', reason: '3주 이상 무판매 상태라 짧은 할인 실험으로 반응 구간을 먼저 확인해요.', tone: 'primary' },
            { label: '진열 위치 변경 필요', reason: '니트 메인 존에서 색상별 착장 제안을 강화해요.', tone: 'secondary' },
            { label: '추가 입고 중단 검토', reason: '반응 확인 전까지는 현재 재고로만 운영해도 돼요.', tone: 'secondary' }
        ]
    },
    {
        skuId: 'MX-BG-411',
        productName: '라이트 유틸 크로스백',
        brand: 'MERCURY ACTIVE',
        category: '잡화',
        receivedDate: '2026-03-08',
        daysInStock: 26,
        totalSales: 0,
        currentStock: 43,
        priority: 'Medium',
        recommendedAction: '타 매장 이동 검토',
        rationale: '반응이 약한 매장 중심으로 묶여 있어, 액세서리 반응이 좋은 채널로 일부 이동이 필요해 보여요.',
        recentSignal: '최근 14일 판매 0건, 액세서리군 평균 대비 체류가 8일 길어요.',
        categoryBaselineDays: 18,
        actionReason: '제품 자체보다 매장 믹스 차이가 커 보여서 재배치 판단이 먼저예요.',
        storeBreakdown: [
            { store: '잠실 롯데월드몰', stock: 9, recentSales: 0, note: '피팅 없이 지나치는 비중이 높아요.' },
            { store: '판교 현대백화점', stock: 12, recentSales: 0, note: '동일 가격대 백팩으로 수요가 분산돼요.' },
            { store: '부산 센텀시티', stock: 11, recentSales: 0, note: '액세서리 존 체류 시간이 짧아요.' },
            { store: '대전 타임월드', stock: 11, recentSales: 0, note: '보조 집기존에만 배치돼 있어요.' }
        ],
        actionOptions: [
            { label: '타 매장 이동 검토', reason: '액세서리 반응이 강한 매장으로 일부만 옮겨 반응 차이를 먼저 봐요.', tone: 'primary' },
            { label: '진열 위치 변경 필요', reason: '보조 진열이 아닌 계산대 전면 존으로 위치를 조정해 비교해요.', tone: 'secondary' },
            { label: '우선 할인 검토', reason: '재배치 후에도 변화가 없으면 소폭 할인으로 저항 구간을 체크해요.', tone: 'secondary' }
        ]
    },
    {
        skuId: 'MX-DR-220',
        productName: '릴렉스 셔츠 원피스',
        brand: 'MERCURY WOMAN',
        category: '원피스',
        receivedDate: '2026-03-10',
        daysInStock: 24,
        totalSales: 0,
        currentStock: 39,
        priority: 'Medium',
        recommendedAction: '진열 위치 변경 필요',
        rationale: '매장별 체류는 길지만 피팅 이후 구매로 잘 이어지지 않아, 스타일 제안과 위치를 같이 조정해야 해요.',
        recentSignal: '최근 7일 판매 0건, 원피스군 평균 대비 체류가 6일 길어요.',
        categoryBaselineDays: 18,
        actionReason: '코디 제안과 메인 마네킹 노출이 부족해 보여 가격보다 진열/메시지 보강이 먼저예요.',
        storeBreakdown: [
            { store: '강남 플래그십', stock: 19, recentSales: 0, note: '피팅 수 대비 구매 전환이 낮아요.' },
            { store: '대전 타임월드', stock: 20, recentSales: 0, note: '원피스 집중 존이 아닌 보조 존에 걸려 있어요.' }
        ],
        actionOptions: [
            { label: '진열 위치 변경 필요', reason: '메인 마네킹과 함께 코디 제안을 붙이면 판단이 빨라져요.', tone: 'primary' },
            { label: '우선 할인 검토', reason: '진열 보정 후 반응이 그대로면 가격 민감도를 짧게 확인해요.', tone: 'secondary' },
            { label: '추가 입고 중단 검토', reason: '당장 판매 반응이 없으니 추가 입고는 보류해도 괜찮아요.', tone: 'secondary' }
        ]
    },
    {
        skuId: 'MX-AC-052',
        productName: '클래식 버킷햇',
        brand: 'MERCURY ACTIVE',
        category: '잡화',
        receivedDate: '2026-03-13',
        daysInStock: 21,
        totalSales: 0,
        currentStock: 28,
        priority: 'Medium',
        recommendedAction: '우선 할인 검토',
        rationale: '시즌 잡화인데 초기 반응이 없어, 빠르게 할인 여부를 검토하며 체류를 늘리지 않는 편이 좋아요.',
        recentSignal: '최근 7일 판매 0건, 잡화군 평균보다 체류가 5일 길어요.',
        categoryBaselineDays: 16,
        actionReason: '시즌성이 강해서 대기보다 빠른 가격 판단이 유리해요.',
        storeBreakdown: [
            { store: '성수 라운지', stock: 9, recentSales: 0, note: '동일 가격대 캡 상품으로 수요가 옮겨가요.' },
            { store: '부산 센텀시티', stock: 11, recentSales: 0, note: '계절성 홍보 없이 일반 잡화존에만 배치돼요.' },
            { store: '대전 타임월드', stock: 8, recentSales: 0, note: '주말 유입 대비 반응이 정체예요.' }
        ],
        actionOptions: [
            { label: '우선 할인 검토', reason: '시즌 잡화라 빠르게 반응 구간을 확인하는 편이 리스크가 작아요.', tone: 'primary' },
            { label: '진열 위치 변경 필요', reason: '캡/햇 통합 존에서 시즈널 강조를 붙여 반응을 다시 봐요.', tone: 'secondary' },
            { label: '추가 입고 중단 검토', reason: '현재 재고만 소진해도 충분해 당장 추가 발주는 멈춰도 돼요.', tone: 'secondary' }
        ]
    }
]);

function escapeHtml(value) {
    return String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
}

function formatNumber(value) {
    return new Intl.NumberFormat('ko-KR').format(Number(value) || 0);
}

function formatDate(value) {
    const [year, month, day] = String(value || '').split('-');
    if (!year || !month || !day) return '-';
    return `${year}.${month}.${day}`;
}

function getSeverityClass(severity) {
    return String(severity || '').toLowerCase() === 'high' ? 'is-high' : 'is-medium';
}

function getPriorityRank(priority) {
    return String(priority || '').toLowerCase() === 'high' ? 2 : 1;
}

function getAllStores() {
    const stores = new Set();
    SKU_CANDIDATES.forEach((candidate) => {
        candidate.storeBreakdown.forEach((store) => stores.add(store.store));
    });
    return Array.from(stores).sort((a, b) => a.localeCompare(b, 'ko'));
}

function getAllBrands() {
    return Array.from(new Set(SKU_CANDIDATES.map((candidate) => candidate.brand))).sort((a, b) => a.localeCompare(b, 'ko'));
}

function getAllCategories() {
    return Array.from(new Set(SKU_CANDIDATES.map((candidate) => candidate.category))).sort((a, b) => a.localeCompare(b, 'ko'));
}

function getVisibleStores(candidate, filters = DASHBOARD_STATE.filters) {
    if (!candidate) return [];
    if (!filters || filters.store === 'all') return candidate.storeBreakdown;
    return candidate.storeBreakdown.filter((store) => store.store === filters.store);
}

function getVisibleStock(candidate, filters = DASHBOARD_STATE.filters) {
    return getVisibleStores(candidate, filters).reduce((sum, store) => sum + store.stock, 0);
}

function getVisibleStoreCount(candidate, filters = DASHBOARD_STATE.filters) {
    return getVisibleStores(candidate, filters).length;
}

function getFilteredCandidates() {
    const thresholdDays = Number.parseInt(DASHBOARD_STATE.filters.thresholdDays, 10) || 21;
    return SKU_CANDIDATES
        .filter((candidate) => candidate.daysInStock >= thresholdDays)
        .filter((candidate) => DASHBOARD_STATE.filters.brand === 'all' || candidate.brand === DASHBOARD_STATE.filters.brand)
        .filter((candidate) => DASHBOARD_STATE.filters.category === 'all' || candidate.category === DASHBOARD_STATE.filters.category)
        .filter((candidate) => DASHBOARD_STATE.filters.store === 'all' || candidate.storeBreakdown.some((store) => store.store === DASHBOARD_STATE.filters.store))
        .sort((left, right) => {
            const priorityGap = getPriorityRank(right.priority) - getPriorityRank(left.priority);
            if (priorityGap !== 0) return priorityGap;
            const stockGap = getVisibleStock(right) - getVisibleStock(left);
            if (stockGap !== 0) return stockGap;
            return right.daysInStock - left.daysInStock;
        });
}

function normalizeSelection(filteredCandidates) {
    if (!Array.isArray(filteredCandidates) || !filteredCandidates.length) {
        DASHBOARD_STATE.selectedSkuId = '';
        return null;
    }

    const current = filteredCandidates.find((candidate) => candidate.skuId === DASHBOARD_STATE.selectedSkuId);
    if (current) return current;

    DASHBOARD_STATE.selectedSkuId = filteredCandidates[0].skuId;
    return filteredCandidates[0];
}

function buildSummary(filteredCandidates) {
    const visibleStoreNames = new Set();
    let inventoryUnits = 0;

    filteredCandidates.forEach((candidate) => {
        inventoryUnits += getVisibleStock(candidate);
        getVisibleStores(candidate).forEach((store) => visibleStoreNames.add(store.store));
    });

    return {
        skuCount: filteredCandidates.length,
        inventoryUnits,
        storeCount: visibleStoreNames.size
    };
}

function renderMetaPill(iconClass, label) {
    return `
        <span class="decision-meta-pill">
            <i class="ph ${escapeHtml(iconClass)}"></i>
            ${escapeHtml(label)}
        </span>
    `;
}

function getFilteredIssues() {
    if (DASHBOARD_STATE.inboxDepartment === 'all') return ISSUE_CARDS;

    const selectedDepartment = INBOX_DEPARTMENT_FILTERS.find((option) => option.value === DASHBOARD_STATE.inboxDepartment)?.label;
    if (!selectedDepartment) return ISSUE_CARDS;

    return ISSUE_CARDS.filter((issue) => issue.departments.includes(selectedDepartment));
}

function normalizeInboxSelection(filteredIssues) {
    if (!filteredIssues.length) {
        DASHBOARD_STATE.selectedIssueId = REPRESENTATIVE_ISSUE_ID;
        return;
    }

    if (filteredIssues.some((issue) => issue.id === DASHBOARD_STATE.selectedIssueId)) return;

    const representativeIssue = filteredIssues.find((issue) => issue.id === REPRESENTATIVE_ISSUE_ID);
    DASHBOARD_STATE.selectedIssueId = representativeIssue ? representativeIssue.id : filteredIssues[0].id;
}

function renderInboxDepartmentFilter() {
    return `
        <div class="decision-inbox-toolbar" aria-label="유관부서 관점 필터">
            <span class="decision-label">관점 필터</span>
            <div class="decision-chip-row">
                ${INBOX_DEPARTMENT_FILTERS.map((option) => {
                    const activeClass = option.value === DASHBOARD_STATE.inboxDepartment ? ' is-active' : '';
                    const ariaPressed = option.value === DASHBOARD_STATE.inboxDepartment ? 'true' : 'false';
                    return `
                        <button
                            class="decision-filter-chip${activeClass}"
                            type="button"
                            data-action="set-inbox-department"
                            data-department-filter="${escapeHtml(option.value)}"
                            aria-pressed="${ariaPressed}"
                        >
                            ${escapeHtml(option.label)}
                        </button>
                    `;
                }).join('')}
            </div>
        </div>
    `;
}

function renderIssueCards(filteredIssues) {
    if (!filteredIssues.length) {
        return `
            <article class="decision-empty-state card">
                <strong>해당 관점에서 보이는 이슈가 아직 없습니다.</strong>
                <p>다른 부서 필터를 선택하거나 전체 보기로 돌아가 전사 이슈를 다시 확인해보세요.</p>
            </article>
        `;
    }

    return filteredIssues.map((issue) => {
        const featuredClass = issue.featured ? 'is-featured' : '';
        const selectedClass = !issue.featured && issue.id === DASHBOARD_STATE.selectedIssueId ? 'is-subselected' : '';
        const severityClass = getSeverityClass(issue.severity);
        const metricToneClass = issue.metricTone ? `is-${issue.metricTone}` : 'is-neutral';
        const ctaMarkup = issue.deepDiveEnabled
            ? `<button class="btn-primary" type="button" data-action="open-decision" data-issue-id="${escapeHtml(issue.id)}">${escapeHtml(issue.ctaLabel)}</button>`
            : `<button class="decision-secondary-btn is-judgment" type="button" data-action="preview-issue" data-issue-id="${escapeHtml(issue.id)}">${escapeHtml(issue.ctaLabel)}</button>`;

        return `
            <article class="decision-issue-card ${featuredClass} ${selectedClass}">
                <div class="decision-card-top">
                    <span class="decision-kicker">${escapeHtml(issue.kicker)}</span>
                    <span class="decision-status-badge ${severityClass}">${escapeHtml(issue.severity)}</span>
                </div>
                <div class="decision-card-signal">
                    <strong>${escapeHtml(issue.signalLead)}</strong>
                    <p>${escapeHtml(issue.signalDetail)}</p>
                </div>
                <div class="decision-card-title">
                    <span class="decision-card-name-label">문제 유형</span>
                    <h4>${escapeHtml(issue.title)}</h4>
                </div>
                <div class="decision-tag-row">
                    ${issue.departments.map((department) => `<span class="decision-tag">${escapeHtml(department)}</span>`).join('')}
                </div>
                <div class="decision-card-body">
                    <div class="decision-card-metric ${metricToneClass}">
                        <div>
                            <label>${escapeHtml(issue.metricLabel)}</label>
                            <strong>${escapeHtml(issue.metricValue)}</strong>
                        </div>
                        <span>${escapeHtml(issue.metricNote)}</span>
                    </div>
                </div>
                <div class="decision-card-actions">
                    ${ctaMarkup}
                </div>
            </article>
        `;
    }).join('');
}

function renderInboxScreen() {
    const filteredIssues = getFilteredIssues();
    normalizeInboxSelection(filteredIssues);

    return `
        <section class="decision-hero-card card animate-fade-in">
            <div class="decision-hero-top">
                <div class="decision-copy-wrap">
                    <h2>지금 먼저 해결해야 할 문제를 보여드립니다</h2>
                    <p>문제를 찾는 데 쓰던 시간을 줄이고, 판단이 필요한 이슈부터 빠르게 확인할 수 있습니다.</p>
                </div>
                <div class="decision-meta-stack">
                    ${renderMetaPill('ph-calendar-blank', '최근 7일 기준 · 전체 매장 기준')}
                    ${renderMetaPill('ph-clock', '기준 시점 2026.04.02 09:00')}
                </div>
            </div>
        </section>

        <section class="card animate-fade-in">
            <div class="decision-section-head">
                <div>
                    <h3>핵심 문제 목록</h3>
                    <p>지금 손실이 커질 문제부터 먼저 좁혀서, 바로 판단이 필요한 이슈를 우선 보여줘요.</p>
                </div>
                ${renderMetaPill('ph-warning-circle', `우선 검토 이슈 ${filteredIssues.length}개`)}
            </div>
            ${renderInboxDepartmentFilter()}
            <div class="decision-issue-grid">
                ${renderIssueCards(filteredIssues)}
            </div>
        </section>
    `;
}

function renderFilterOption(value, label, selectedValue) {
    const selectedAttr = value === selectedValue ? ' selected' : '';
    return `<option value="${escapeHtml(value)}"${selectedAttr}>${escapeHtml(label)}</option>`;
}

function renderDecisionFilters(filteredCandidates) {
    const brands = getAllBrands();
    const categories = getAllCategories();
    const stores = getAllStores();

    return `
        <section class="decision-filter-card card animate-fade-in">
            <div class="decision-section-head">
                <div>
                    <h3>판단 범위 조정</h3>
                    <p>필터는 최소화하고, 클릭 몇 번 안에 문제 발견에서 액션 판단까지 이어지게 구성했어요.</p>
                </div>
            </div>
            <div class="decision-filter-grid">
                <div class="decision-filter-field">
                    <label for="decision-filter-store">매장</label>
                    <select id="decision-filter-store" class="search-select" data-filter-key="store">
                        ${renderFilterOption('all', '전체 매장', DASHBOARD_STATE.filters.store)}
                        ${stores.map((store) => renderFilterOption(store, store, DASHBOARD_STATE.filters.store)).join('')}
                    </select>
                </div>
                <div class="decision-filter-field">
                    <label for="decision-filter-brand">브랜드</label>
                    <select id="decision-filter-brand" class="search-select" data-filter-key="brand">
                        ${renderFilterOption('all', '전체 브랜드', DASHBOARD_STATE.filters.brand)}
                        ${brands.map((brand) => renderFilterOption(brand, brand, DASHBOARD_STATE.filters.brand)).join('')}
                    </select>
                </div>
                <div class="decision-filter-field">
                    <label for="decision-filter-category">카테고리</label>
                    <select id="decision-filter-category" class="search-select" data-filter-key="category">
                        ${renderFilterOption('all', '전체 카테고리', DASHBOARD_STATE.filters.category)}
                        ${categories.map((category) => renderFilterOption(category, category, DASHBOARD_STATE.filters.category)).join('')}
                    </select>
                </div>
                <div class="decision-filter-field">
                    <label for="decision-filter-threshold">체류기간 기준</label>
                    <select id="decision-filter-threshold" class="search-select" data-filter-key="thresholdDays">
                        ${renderFilterOption('21', '21일 이상', DASHBOARD_STATE.filters.thresholdDays)}
                        ${renderFilterOption('28', '28일 이상', DASHBOARD_STATE.filters.thresholdDays)}
                    </select>
                </div>
            </div>
            <div class="decision-filter-actions">
                <span class="decision-filter-helper">현재 조건에서 ${formatNumber(filteredCandidates.length)}개 SKU가 대표 검토 후보로 남아 있어요.</span>
                <button class="decision-secondary-btn" type="button" data-action="reset-filters">기본 조건으로 되돌리기</button>
            </div>
        </section>
    `;
}

function renderTableRows(filteredCandidates) {
    if (!filteredCandidates.length) {
        return `
            <tr>
                <td colspan="10">
                    <div class="decision-empty-state">
                        <strong>지금 조건에서는 검토 후보가 없어요.</strong>
                        <p>필터를 조금 넓혀서 다시 확인하면, 대표 이슈 후보를 바로 이어서 볼 수 있어요.</p>
                    </div>
                </td>
            </tr>
        `;
    }

    return filteredCandidates.map((candidate) => {
        const selectedClass = candidate.skuId === DASHBOARD_STATE.selectedSkuId ? 'is-selected' : '';
        const visibleStock = getVisibleStock(candidate);
        const visibleStoreCount = getVisibleStoreCount(candidate);
        return `
            <tr class="${selectedClass}" data-action="select-sku" data-sku-id="${escapeHtml(candidate.skuId)}">
                <td>
                    <div class="decision-product-name">
                        <strong>${escapeHtml(candidate.productName)}</strong>
                        <span>${escapeHtml(candidate.skuId)}</span>
                    </div>
                </td>
                <td>
                    <div class="decision-product-name">
                        <strong>${escapeHtml(candidate.brand)}</strong>
                        <span>${escapeHtml(candidate.category)}</span>
                    </div>
                </td>
                <td>${escapeHtml(formatDate(candidate.receivedDate))}</td>
                <td>${formatNumber(candidate.daysInStock)}일</td>
                <td>${formatNumber(candidate.totalSales)}</td>
                <td>${formatNumber(visibleStock)}</td>
                <td>${formatNumber(visibleStoreCount)}</td>
                <td><span class="decision-priority-badge ${getSeverityClass(candidate.priority)}">${escapeHtml(candidate.priority)}</span></td>
                <td>${escapeHtml(candidate.recommendedAction)}</td>
            </tr>
        `;
    }).join('');
}

function renderDecisionTable(filteredCandidates) {
    return `
        <section class="decision-table-card card animate-fade-in">
            <div class="decision-table-header">
                <h3>우선 검토 후보</h3>
                <p>기본 정렬은 우선순위가 높은 순이고, 과한 BI 느낌보다 실제 판단에 필요한 컬럼만 남겼어요.</p>
            </div>
            <div class="table-container decision-table-wrap">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>상품명 / SKU</th>
                            <th>브랜드 / 카테고리</th>
                            <th>입고일</th>
                            <th>경과일수</th>
                            <th>누적 판매수량</th>
                            <th>현재 재고수량</th>
                            <th>보유 매장 수</th>
                            <th>우선순위</th>
                            <th>권장 액션</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${renderTableRows(filteredCandidates)}
                    </tbody>
                </table>
            </div>
        </section>
    `;
}

function renderStoreRows(selectedCandidate) {
    const stores = getVisibleStores(selectedCandidate);
    if (!stores.length) {
        return '<p class="decision-detail-note">현재 필터 조건에 맞는 매장 분포가 없어요.</p>';
    }

    return `
        <div class="decision-store-list">
            ${stores.map((store) => `
                <div class="decision-store-row">
                    <div class="decision-store-head">
                        <strong>${escapeHtml(store.store)}</strong>
                        <span class="decision-store-note">${escapeHtml(store.note)}</span>
                    </div>
                    <span class="decision-store-stock">재고 ${formatNumber(store.stock)}개</span>
                </div>
            `).join('')}
        </div>
    `;
}

function renderActionRows(selectedCandidate) {
    return `
        <div class="decision-action-list">
            ${selectedCandidate.actionOptions.map((action) => `
                <div class="decision-action-block ${action.tone === 'primary' ? 'is-primary' : ''}">
                    <div class="decision-action-head">
                        <strong>${escapeHtml(action.label)}</strong>
                        <span class="decision-priority-badge ${action.tone === 'primary' ? 'is-high' : 'is-medium'}">${action.tone === 'primary' ? '우선' : '보조'}</span>
                    </div>
                    <p class="decision-detail-copy">${escapeHtml(action.reason)}</p>
                </div>
            `).join('')}
        </div>
    `;
}

function renderDecisionDetail(selectedCandidate) {
    if (!selectedCandidate) {
        return `
            <section class="decision-detail-card card animate-fade-in">
                <div class="decision-detail-header">
                    <h3>상세 근거</h3>
                    <p>선택한 후보가 있으면 근거와 권장 액션을 같이 보여줘요.</p>
                </div>
                <div class="decision-empty-state">
                    <strong>현재 조건에서는 대표 후보가 비어 있어요.</strong>
                    <p>필터를 완화하면 다시 근거와 액션을 이어서 볼 수 있어요.</p>
                    <button class="btn-primary" type="button" data-action="reset-filters">필터 초기화</button>
                </div>
            </section>
        `;
    }

    return `
        <section class="decision-detail-card card animate-fade-in">
            <div class="decision-detail-header">
                <h3>상세 근거와 액션</h3>
                <p>단순 조회가 아니라, 지금 왜 이 SKU를 먼저 봐야 하는지와 어떤 판단 옵션이 적절한지 같이 보여줘요.</p>
            </div>
            <div class="decision-detail-panel">
                <div class="decision-detail-hero">
                    <div class="decision-detail-main">
                        <h4>${escapeHtml(selectedCandidate.productName)}</h4>
                        <p>${escapeHtml(selectedCandidate.skuId)} · ${escapeHtml(selectedCandidate.brand)} · ${escapeHtml(selectedCandidate.category)}</p>
                    </div>
                    <span class="decision-priority-badge ${getSeverityClass(selectedCandidate.priority)}">${escapeHtml(selectedCandidate.priority)}</span>
                </div>

                <div class="decision-detail-grid">
                    <div class="decision-detail-metric">
                        <label>왜 상위 후보인가</label>
                        <strong>${escapeHtml(selectedCandidate.recommendedAction)}</strong>
                        <span>${escapeHtml(selectedCandidate.actionReason)}</span>
                    </div>
                    <div class="decision-detail-metric">
                        <label>카테고리 평균 대비 체류</label>
                        <strong>${formatNumber(selectedCandidate.daysInStock - selectedCandidate.categoryBaselineDays)}일 더 김</strong>
                        <span>평균 ${formatNumber(selectedCandidate.categoryBaselineDays)}일 대비 길게 머물고 있어요.</span>
                    </div>
                </div>

                <div class="decision-detail-block">
                    <h5>왜 지금 이 SKU를 우선 검토해야 하나요?</h5>
                    <p class="decision-detail-copy">${escapeHtml(selectedCandidate.rationale)}</p>
                </div>

                <div class="decision-detail-cells">
                    <div class="decision-detail-block">
                        <h5>최근 판매 부재 신호</h5>
                        <p class="decision-detail-copy">${escapeHtml(selectedCandidate.recentSignal)}</p>
                    </div>
                    <div class="decision-detail-block">
                        <h5>운영 판단 메모</h5>
                        <p class="decision-detail-copy">${escapeHtml(selectedCandidate.actionReason)}</p>
                    </div>
                </div>

                <div class="decision-store-block">
                    <h5>매장별 재고 분포</h5>
                    ${renderStoreRows(selectedCandidate)}
                </div>

                <div class="decision-action-group">
                    <h5>권장 액션과 그 이유</h5>
                    ${renderActionRows(selectedCandidate)}
                </div>

                <div class="decision-difference-card">
                    <div class="decision-difference-grid">
                        <div class="decision-bridge-cell">
                            <strong>ERP</strong>
                            <p>어느 매장에 얼마나 있는지는 보여주지만, 왜 지금 이 SKU를 먼저 봐야 하는지는 사람이 다시 판단해야 해요.</p>
                        </div>
                        <div class="decision-bridge-cell is-accent">
                            <strong>MERCURY X</strong>
                            <p>지금 볼 문제를 먼저 좁히고, 근거와 다음 판단 옵션을 한 화면에서 이어줘요.</p>
                        </div>
                    </div>
                </div>
            </div>
        </section>
    `;
}

function renderDecisionScreen() {
    const filteredCandidates = getFilteredCandidates();
    const selectedCandidate = normalizeSelection(filteredCandidates);
    const summary = buildSummary(filteredCandidates);

    return `
        <div class="decision-breadcrumb-row animate-fade-in">
            <button class="decision-breadcrumb" type="button" data-action="go-inbox">
                <i class="ph ph-arrow-left"></i>
                Problem Discovery Inbox로 돌아가기
            </button>
            ${renderMetaPill('ph-path', '문제 발견 → 대표 이슈 판단')}
        </div>

        <section class="decision-summary-card card animate-fade-in">
            <div class="decision-summary-top">
                <div class="decision-summary-head">
                    <div class="decision-summary-tags">
                        <span class="decision-kicker">Representative issue drill-down</span>
                        <span class="decision-status-badge is-high">High</span>
                    </div>
                    <h2>장기 체류 / 무판매 재고 집중</h2>
                    <p>입고 후 21일 이상 경과했고, 누적 판매가 0이며, 전 매장 기준으로 재고가 묶여 있는 SKU를 먼저 보여줘요.</p>
                </div>
                <div class="decision-meta-stack">
                    ${renderMetaPill('ph-funnel', '입고 후 21일 이상')}
                    ${renderMetaPill('ph-chart-line-down', '누적 판매 0')}
                    ${renderMetaPill('ph-buildings', '전 매장 기준')}
                </div>
            </div>
            <div class="decision-summary-metrics">
                <div class="decision-summary-metric">
                    <label>영향 SKU 수</label>
                    <strong>${formatNumber(summary.skuCount)}개</strong>
                    <span>현재 조건에서 바로 검토할 후보예요.</span>
                </div>
                <div class="decision-summary-metric">
                    <label>재고 수량</label>
                    <strong>${formatNumber(summary.inventoryUnits)}개</strong>
                    <span>선택 조건에서 묶여 있는 재고예요.</span>
                </div>
                <div class="decision-summary-metric">
                    <label>영향 매장 수</label>
                    <strong>${formatNumber(summary.storeCount)}개</strong>
                    <span>지금 판단이 필요한 운영 범위예요.</span>
                </div>
            </div>
        </section>

        ${renderDecisionFilters(filteredCandidates)}

        <div class="decision-screen-layout">
            ${renderDecisionTable(filteredCandidates)}
            ${renderDecisionDetail(selectedCandidate)}
        </div>
    `;
}

function renderDashboard() {
    const contentArea = document.getElementById('content-area');
    if (!contentArea) return;

    const screenMarkup = DASHBOARD_STATE.screen === 'decision'
        ? renderDecisionScreen()
        : renderInboxScreen();

    contentArea.innerHTML = `
        <div class="decision-shell">
            ${screenMarkup}
        </div>
    `;
}

function applySidebarCollapsedState(isCollapsed) {
    const appContainer = document.querySelector('.app-container');
    const collapseButton = document.querySelector('.sidebar-collapse-btn');
    if (!appContainer || !collapseButton) return;

    appContainer.classList.toggle('is-sidebar-collapsed', Boolean(isCollapsed));
    collapseButton.title = isCollapsed ? '사이드바 펼치기' : '사이드바 접기';
    collapseButton.setAttribute('aria-label', isCollapsed ? '사이드바 펼치기' : '사이드바 접기');
    collapseButton.innerHTML = `<i class="ph ${isCollapsed ? 'ph-arrow-line-right' : 'ph-arrow-line-left'}"></i>`;
}

function initSidebarCollapse() {
    const collapseButton = document.querySelector('.sidebar-collapse-btn');
    if (!collapseButton) return;

    const saved = window.localStorage?.getItem(SIDEBAR_STORAGE_KEY) === 'true';
    DASHBOARD_STATE.sidebarCollapsed = saved;
    applySidebarCollapsedState(saved);

    collapseButton.addEventListener('click', () => {
        DASHBOARD_STATE.sidebarCollapsed = !DASHBOARD_STATE.sidebarCollapsed;
        applySidebarCollapsedState(DASHBOARD_STATE.sidebarCollapsed);
        try {
            window.localStorage?.setItem(SIDEBAR_STORAGE_KEY, String(DASHBOARD_STATE.sidebarCollapsed));
        } catch (_) {
            // noop
        }
    });
}

function handleContentClick(event) {
    const actionTarget = event.target.closest('[data-action]');
    if (!actionTarget) return;

    const action = actionTarget.dataset.action;
    if (action === 'set-inbox-department') {
        DASHBOARD_STATE.inboxDepartment = actionTarget.dataset.departmentFilter || 'all';
        renderDashboard();
        return;
    }

    if (action === 'preview-issue') {
        DASHBOARD_STATE.selectedIssueId = actionTarget.dataset.issueId || REPRESENTATIVE_ISSUE_ID;
        renderDashboard();
        return;
    }

    if (action === 'open-decision') {
        DASHBOARD_STATE.selectedIssueId = actionTarget.dataset.issueId || REPRESENTATIVE_ISSUE_ID;
        DASHBOARD_STATE.screen = 'decision';
        renderDashboard();
        return;
    }

    if (action === 'go-inbox') {
        DASHBOARD_STATE.screen = 'inbox';
        renderDashboard();
        return;
    }

    if (action === 'select-sku') {
        DASHBOARD_STATE.selectedSkuId = actionTarget.dataset.skuId || '';
        renderDashboard();
        return;
    }

    if (action === 'reset-filters') {
        DASHBOARD_STATE.filters = { ...DEFAULT_FILTERS };
        renderDashboard();
    }
}

function handleContentChange(event) {
    const target = event.target;
    if (!(target instanceof HTMLSelectElement)) return;

    const filterKey = target.dataset.filterKey;
    if (!filterKey || !(filterKey in DASHBOARD_STATE.filters)) return;

    DASHBOARD_STATE.filters[filterKey] = target.value;
    renderDashboard();
}

function initDecisionDashboard() {
    initSidebarCollapse();

    const contentArea = document.getElementById('content-area');
    if (!contentArea) return;

    contentArea.addEventListener('click', handleContentClick);
    contentArea.addEventListener('change', handleContentChange);

    renderDashboard();
}

document.addEventListener('DOMContentLoaded', initDecisionDashboard);
