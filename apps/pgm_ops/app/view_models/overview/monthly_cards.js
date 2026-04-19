export function buildMonthlyOverviewCards(brandRows, windowSnapshot) {
    const latest = brandRows[brandRows.length - 1];

    if (!latest) {
        return [];
    }

    return [
        {
            period: 'monthly',
            card_key: 'revenue_30d',
            label: '직전 30일 매출',
            value: windowSnapshot.monthly.current,
            delta: windowSnapshot.monthly.deltaRate,
            reason: '기준일 당일을 제외한 직전 30일 누적 매출입니다.',
            as_of_date: latest.date,
            support_window_days: 30
        },
        {
            period: 'monthly',
            card_key: 'revenue_90d_reference',
            label: '직전 90일 참조 매출',
            value: windowSnapshot.quarterly.current,
            delta: windowSnapshot.quarterly.deltaRate,
            reason: '월간 해석은 30일을 본체로 보고, 90일은 구조 비교용 보조 참조 구간으로 사용합니다.',
            as_of_date: latest.date,
            support_window_days: 90
        },
        {
            period: 'monthly',
            card_key: 'top_share',
            label: '상위 상품 집중도',
            value: latest.top_product_revenue_share,
            delta: null,
            reason: '집중도가 높으면 단기 매출이 좋아도 구조 리스크로 해석합니다.',
            as_of_date: latest.date,
            support_window_days: 30
        },
        {
            period: 'monthly',
            card_key: 'coverage',
            label: '역할 관측 안정도',
            value: latest.pgm_observed_coverage,
            delta: null,
            reason: '관측 상태 없음 유지 원칙은 구조 해석 공백을 그대로 보여 주기 위한 안전장치입니다.',
            as_of_date: latest.date,
            support_window_days: 30
        }
    ];
}
