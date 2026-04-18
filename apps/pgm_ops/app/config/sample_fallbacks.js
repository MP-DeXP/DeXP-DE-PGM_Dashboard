export const SAMPLE_FALLBACKS = {
    overview_daily_cards: [
        {
            period: 'daily',
            card_key: 'brand_revenue',
            label: '오늘 브랜드 매출',
            value: '914000',
            delta: '0.01668520578420467',
            reason: 'Revenue 변화가 크지 않으므로 role-state와 집중도에서 우선 점검 대상을 좁히는 편이 안전합니다.',
            as_of_date: '2024-05-10',
            support_window_days: '1'
        },
        {
            period: 'daily',
            card_key: 'active_products',
            label: '활성 상품 수',
            value: '4',
            delta: '',
            reason: '활성 정의는 판매 발생 상품 기준입니다. 노출/판매중 상태와 혼용하지 않습니다.',
            as_of_date: '2024-05-10',
            support_window_days: '1'
        },
        {
            period: 'daily',
            card_key: 'pgm_coverage',
            label: 'PGM 관측 커버리지',
            value: '0.5',
            delta: '',
            reason: 'same-date role snapshot이 없는 상품은 blank로 유지하고 coverage gap으로 드러냅니다.',
            as_of_date: '2024-05-10',
            support_window_days: '1'
        },
        {
            period: 'daily',
            card_key: 'top_share',
            label: '상위 상품 매출 비중',
            value: '0.47702407002188185',
            delta: '',
            reason: 'Revenue 집중도는 Daily 운영 리스크를 읽는 최소 지표입니다.',
            as_of_date: '2024-05-10',
            support_window_days: '1'
        }
    ],
    overview_weekly_cards: [
        {
            period: 'weekly',
            card_key: 'revenue_7d',
            label: '직전 7일 매출',
            value: '6142000',
            delta: '2.772727272727273',
            reason: '기준일 당일을 제외한 직전 7일 rolling 매출입니다.',
            as_of_date: '2024-05-10',
            support_window_days: '7'
        },
        {
            period: 'weekly',
            card_key: 'revenue_30d_reference',
            label: '직전 30일 참조 매출',
            value: '7770000',
            delta: '',
            reason: 'Daily 판단은 7일 중심, 30일은 보조 reference로 사용합니다.',
            as_of_date: '2024-05-10',
            support_window_days: '30'
        },
        {
            period: 'weekly',
            card_key: 'dominant_role',
            label: '매출 기여 주 역할',
            value: 'expansion',
            delta: '',
            reason: 'Revenue 기준 dominant role을 우선 보고, 구조 리스크는 집중도로 보정합니다.',
            as_of_date: '2024-05-10',
            support_window_days: '7'
        },
        {
            period: 'weekly',
            card_key: 'status_label',
            label: '주간 상태 요약',
            value: '매출은 유지 중이고 구조 확인이 필요한 상태입니다.',
            delta: '',
            reason: 'Revenue 변화가 크지 않으므로 role-state와 집중도에서 우선 점검 대상을 좁히는 편이 안전합니다.',
            as_of_date: '2024-05-10',
            support_window_days: '7'
        }
    ],
    overview_monthly_cards: [
        {
            period: 'monthly',
            card_key: 'revenue_30d',
            label: '직전 30일 매출',
            value: '7770000',
            delta: '',
            reason: '기준일 당일 미포함 30일 rolling 매출입니다.',
            as_of_date: '2024-05-10',
            support_window_days: '30'
        },
        {
            period: 'monthly',
            card_key: 'revenue_90d_reference',
            label: '직전 90일 참조 매출',
            value: '7770000',
            delta: '',
            reason: 'Monthly는 30일을 본체로 보고 90일을 구조 비교 reference로 사용합니다.',
            as_of_date: '2024-05-10',
            support_window_days: '90'
        },
        {
            period: 'monthly',
            card_key: 'top_share',
            label: '상위 상품 집중도',
            value: '0.47702407002188185',
            delta: '',
            reason: '집중도가 높으면 단기 매출이 좋아도 구조 리스크로 해석합니다.',
            as_of_date: '2024-05-10',
            support_window_days: '30'
        },
        {
            period: 'monthly',
            card_key: 'coverage',
            label: '역할 관측 안정도',
            value: '0.5',
            delta: '',
            reason: 'Role state blank rule은 구조적 해석 공백을 그대로 보여 주기 위한 안전장치입니다.',
            as_of_date: '2024-05-10',
            support_window_days: '30'
        }
    ],
    product_table: [
        {
            product_id: 'p-101',
            product_name: 'Hydra Serum',
            product_name_source: 'product_master',
            profile_role_primary: 'entry',
            profile_role_secondary: 'expansion',
            profile_confidence: '0.92',
            role_state_primary: 'expansion',
            role_state_confidence: '0.86',
            pgm_observed_flag: 'true',
            role_state_source: 'same_date_snapshot',
            revenue: '436000',
            order_count: '7',
            quantity: '7',
            revenue_share_in_brand_day: '0.47702407002188185',
            revenue_rank_in_brand_day: '1',
            revenue_7d: '2517000',
            revenue_30d: '3089000',
            revenue_90d: '3089000',
            revenue_day_over_day_change_rate: '0.0430622009569378',
            as_of_date: '2024-05-10'
        },
        {
            product_id: 'p-102',
            product_name: 'Barrier Toner',
            product_name_source: 'product_master',
            profile_role_primary: 'expansion',
            profile_role_secondary: 'entry',
            profile_confidence: '0.84',
            role_state_primary: 'PGM 미관측',
            role_state_confidence: '',
            pgm_observed_flag: 'false',
            role_state_source: 'blank',
            revenue: '296000',
            order_count: '4',
            quantity: '4',
            revenue_share_in_brand_day: '0.3238512035010941',
            revenue_rank_in_brand_day: '2',
            revenue_7d: '2241000',
            revenue_30d: '2855000',
            revenue_90d: '2855000',
            revenue_day_over_day_change_rate: '-0.019867549668874173',
            as_of_date: '2024-05-10'
        },
        {
            product_id: 'p-103',
            product_name: 'Calm Cleanser',
            product_name_source: 'product_master',
            profile_role_primary: 'retention',
            profile_role_secondary: 'return',
            profile_confidence: '0.74',
            role_state_primary: 'retention',
            role_state_confidence: '0.72',
            pgm_observed_flag: 'true',
            role_state_source: 'same_date_snapshot',
            revenue: '126000',
            order_count: '2',
            quantity: '2',
            revenue_share_in_brand_day: '0.13785557986870897',
            revenue_rank_in_brand_day: '3',
            revenue_7d: '1037000',
            revenue_30d: '1393000',
            revenue_90d: '1393000',
            revenue_day_over_day_change_rate: '-0.03816793893129771',
            as_of_date: '2024-05-10'
        },
        {
            product_id: 'p-104',
            product_name: 'Night Repair Mask',
            product_name_source: 'order_item_fallback',
            profile_role_primary: 'PGM 미관측',
            profile_role_secondary: '',
            profile_confidence: '',
            role_state_primary: 'PGM 미관측',
            role_state_confidence: '',
            pgm_observed_flag: 'false',
            role_state_source: 'blank',
            revenue: '56000',
            order_count: '1',
            quantity: '1',
            revenue_share_in_brand_day: '0.061269146608315096',
            revenue_rank_in_brand_day: '4',
            revenue_7d: '347000',
            revenue_30d: '433000',
            revenue_90d: '433000',
            revenue_day_over_day_change_rate: '0.16666666666666666',
            as_of_date: '2024-05-10'
        }
    ],
    product_detail_header: [
        {
            product_id: 'p-101',
            headline: 'Hydra Serum 운영 요약',
            summary: 'entry profile을 가진 상품이며 same-date role state는 expansion입니다.',
            priority_hint: 'same-date role snapshot과 revenue 구조를 함께 점검하세요.'
        },
        {
            product_id: 'p-102',
            headline: 'Barrier Toner 운영 요약',
            summary: 'expansion profile을 가진 상품입니다. same-date role state는 blank이며 latest role로 보정하지 않았습니다.',
            priority_hint: 'same-date role snapshot이 없어 blank로 유지했습니다. latest role fallback 없이 revenue와 관측 누락 여부를 먼저 확인하세요.'
        },
        {
            product_id: 'p-103',
            headline: 'Calm Cleanser 운영 요약',
            summary: 'retention profile을 가진 상품이며 same-date role state는 retention입니다.',
            priority_hint: 'same-date role snapshot과 revenue 구조를 함께 점검하세요.'
        },
        {
            product_id: 'p-104',
            headline: 'Night Repair Mask 운영 요약',
            summary: 'PGM 미관측 profile을 가진 상품입니다. same-date role state는 blank이며 latest role로 보정하지 않았습니다.',
            priority_hint: 'same-date role snapshot이 없어 blank로 유지했습니다. latest role fallback 없이 revenue와 관측 누락 여부를 먼저 확인하세요.'
        }
    ],
    role_structure_chart: [
        {
            role_state_primary: 'expansion',
            revenue: '436000',
            product_count: '1',
            revenue_share: '0.47702407002188185'
        },
        {
            role_state_primary: 'PGM 미관측',
            revenue: '352000',
            product_count: '2',
            revenue_share: '0.3851203501094092'
        },
        {
            role_state_primary: 'retention',
            revenue: '126000',
            product_count: '1',
            revenue_share: '0.13785557986870897'
        }
    ],
    revenue_structure_chart: [
        {
            product_id: 'p-101',
            product_name: 'Hydra Serum',
            revenue: '436000',
            revenue_share_in_brand_day: '0.47702407002188185',
            role_state_primary: 'expansion'
        },
        {
            product_id: 'p-102',
            product_name: 'Barrier Toner',
            revenue: '296000',
            revenue_share_in_brand_day: '0.3238512035010941',
            role_state_primary: 'PGM 미관측'
        },
        {
            product_id: 'p-103',
            product_name: 'Calm Cleanser',
            revenue: '126000',
            revenue_share_in_brand_day: '0.13785557986870897',
            role_state_primary: 'retention'
        },
        {
            product_id: 'p-104',
            product_name: 'Night Repair Mask',
            revenue: '56000',
            revenue_share_in_brand_day: '0.061269146608315096',
            role_state_primary: 'PGM 미관측'
        }
    ],
    priority_checks: [
        {
            priority_rank: '1',
            priority: 'medium',
            entity_type: 'product',
            entity_id: 'p-102',
            label: 'Barrier Toner role-state 공백',
            reason: 'revenue_structure_daily 상위 기여 상품인데 product_role_state_daily same-date snapshot은 blank입니다.',
            suggested_check: 'PGM 관측 누락인지 실제 구조 변화인지 먼저 구분하세요.',
            evidence: 'revenue.rank=2; revenue.share=0.3238512035010941; role_state_source=blank',
            rule_source: 'revenue_structure_daily.revenue_rank_in_brand_day + product_role_state_daily.role_state_source'
        }
    ]
};
