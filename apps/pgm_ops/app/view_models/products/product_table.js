import { ROLE_LABEL_FALLBACK } from '../../config/constants.js';
import { getLatestDate } from '../../transforms/base/date_windows.js';

export function buildProductTable(productDailyMetrics, productRoleProfile, productRoleStateDaily, revenueStructureDaily, productTransitionSummary = [], productReturnLoopSummary = []) {
    const latestDate = getLatestDate(productDailyMetrics);
    const profileLookup = new Map(productRoleProfile.map((row) => [row.product_id, row]));
    const stateLookup = new Map(productRoleStateDaily.map((row) => [`${row.date}|${row.product_id}`, row]));
    const revenueLookup = new Map(revenueStructureDaily.map((row) => [`${row.date}|${row.product_id}`, row]));
    const latestTransitionLookup = new Map(
        productTransitionSummary
            .filter((row) => row.date === latestDate && Number(row.transition_rank ?? 999) === 1)
            .map((row) => [row.product_id, row])
    );
    const latestReturnLoopLookup = new Map(
        productReturnLoopSummary
            .filter((row) => row.date === latestDate)
            .map((row) => [row.product_id, row])
    );

    return productDailyMetrics
        .filter((row) => row.date === latestDate)
        .map((row) => {
            const profile = profileLookup.get(row.product_id);
            const state = stateLookup.get(`${row.date}|${row.product_id}`);
            const revenue = revenueLookup.get(`${row.date}|${row.product_id}`);
            const transition = latestTransitionLookup.get(row.product_id);
            const returnLoop = latestReturnLoopLookup.get(row.product_id);

            return {
                product_id: row.product_id,
                product_name: row.product_name,
                product_name_source: row.product_name_source,
                image_url: row.image_url ?? '',
                detail_url: row.detail_url ?? '',
                product_status: row.product_status ?? '',
                profile_role_primary: profile?.profile_role_primary ?? ROLE_LABEL_FALLBACK,
                profile_role_secondary: profile?.profile_role_secondary ?? '',
                profile_confidence: profile?.profile_confidence ?? '',
                // 빈 역할 상태는 UI에서 자연어로만 풀어 보여 줍니다.
                role_state_primary: state?.role_state_primary || ROLE_LABEL_FALLBACK,
                role_state_confidence: state?.role_state_confidence ?? '',
                pgm_observed_flag: state?.pgm_observed_flag ?? 'false',
                role_state_source: state?.role_state_source ?? 'unobserved',
                revenue: row.revenue,
                order_count: row.order_count,
                quantity: row.quantity,
                revenue_share_in_brand_day: revenue?.revenue_share_in_brand_day ?? 0,
                revenue_rank_in_brand_day: revenue?.revenue_rank_in_brand_day ?? '',
                revenue_7d: row.revenue_7d ?? 0,
                revenue_30d: row.revenue_30d ?? 0,
                revenue_90d: row.revenue_90d ?? 0,
                revenue_day_over_day_change_rate: row.revenue_day_over_day_change_rate,
                top_transition_target_id: transition?.target_product_id ?? '',
                top_transition_target_name: transition?.target_product_name ?? '',
                top_transition_rate: transition?.transition_rate ?? '',
                qualified_return_rate: returnLoop?.qualified_return_rate ?? '',
                return_loop_rate: returnLoop?.return_loop_rate ?? '',
                simple_repeat_rate: returnLoop?.simple_repeat_rate ?? '',
                as_of_date: latestDate
            };
        })
        .sort((left, right) => Number(right.revenue ?? 0) - Number(left.revenue ?? 0));
}
