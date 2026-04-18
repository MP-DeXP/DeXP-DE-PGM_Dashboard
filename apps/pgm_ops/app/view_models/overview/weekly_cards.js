export function buildWeeklyOverviewCards(brandRows, windowSnapshot) {
    const latest = brandRows[brandRows.length - 1];

    if (!latest) {
        return [];
    }

    return [
        {
            period: 'weekly',
            card_key: 'revenue_7d',
            label: '직전 7일 매출',
            value: windowSnapshot.weekly.current,
            delta: windowSnapshot.weekly.deltaRate,
            reason: '기준일 당일을 제외한 직전 7일 rolling 매출입니다.',
            as_of_date: latest.date,
            support_window_days: 7
        },
        {
            period: 'weekly',
            card_key: 'revenue_30d_reference',
            label: '직전 30일 참조 매출',
            value: latest.revenue_30d,
            delta: null,
            reason: 'Daily 판단은 7일 중심, 30일은 보조 reference로 사용합니다.',
            as_of_date: latest.date,
            support_window_days: 30
        },
        {
            period: 'weekly',
            card_key: 'dominant_role',
            label: '매출 기여 주 역할',
            value: latest.dominant_role_state_in_revenue,
            delta: null,
            reason: 'Revenue 기준 dominant role을 우선 보고, 구조 리스크는 집중도로 보정합니다.',
            as_of_date: latest.date,
            support_window_days: 7
        },
        {
            period: 'weekly',
            card_key: 'status_label',
            label: '주간 상태 요약',
            value: latest.status_summary_label,
            delta: null,
            reason: latest.status_reason,
            as_of_date: latest.date,
            support_window_days: 7
        }
    ];
}
