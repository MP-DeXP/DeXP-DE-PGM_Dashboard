function buildCard({ period, cardKey, label, value, delta, reason, asOfDate, supportWindowDays }) {
    return {
        period,
        card_key: cardKey,
        label,
        value,
        delta,
        reason,
        as_of_date: asOfDate,
        support_window_days: supportWindowDays
    };
}

export function buildDailyOverviewCards(brandRows) {
    const latest = brandRows[brandRows.length - 1];

    if (!latest) {
        return [];
    }

    return [
        buildCard({
            period: 'daily',
            cardKey: 'brand_revenue',
            label: '어제(최근 확정일) 브랜드 매출',
            value: latest.brand_revenue,
            delta: latest.brand_revenue_day_over_day_change_rate,
            reason: latest.status_reason,
            asOfDate: latest.date,
            supportWindowDays: 1
        }),
        buildCard({
            period: 'daily',
            cardKey: 'active_products',
            label: '활성 상품 수',
            value: latest.active_product_count,
            delta: null,
            reason: '활성 정의는 판매 발생 상품 기준입니다. 노출/판매중 상태와 혼용하지 않습니다.',
            asOfDate: latest.date,
            supportWindowDays: 1
        }),
        buildCard({
            period: 'daily',
            cardKey: 'pgm_coverage',
            label: 'PGM 관측 커버리지',
            value: latest.pgm_observed_coverage,
            delta: null,
            reason: '동일 일자 역할 스냅샷이 없는 상품은 관측 상태 없음으로 유지하고 관측 공백을 그대로 드러냅니다.',
            asOfDate: latest.date,
            supportWindowDays: 1
        }),
        buildCard({
            period: 'daily',
            cardKey: 'top_share',
            label: '상위 상품 매출 비중',
            value: latest.top_product_revenue_share,
            delta: null,
            reason: '매출 집중도는 일별 운영 리스크를 읽는 최소 지표입니다.',
            asOfDate: latest.date,
            supportWindowDays: 1
        })
    ];
}
