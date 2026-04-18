import { ROLE_LABEL_FALLBACK } from '../../config/constants.js';
import { safeDivide } from '../base/null_handling.js';

function buildStatusSummary({ revenueDelta, topShare, coverage }) {
    if (topShare > 0.5) {
        return {
            label: '상위 상품 쏠림이 큰 상태입니다.',
            reason: 'Revenue 집중도가 높아 단기 성과와 별도로 구조 리스크를 점검해야 합니다.'
        };
    }

    if ((revenueDelta ?? 0) < -0.08 && coverage < 0.75) {
        return {
            label: '매출 둔화와 역할 관측 공백이 동시에 보입니다.',
            reason: 'Revenue 신호가 약해지는 날에는 role-state 공백이 판단 지연으로 이어질 수 있습니다.'
        };
    }

    if ((revenueDelta ?? 0) > 0.05) {
        return {
            label: '매출이 개선되고 있습니다.',
            reason: 'Revenue 신호를 우선 보되, 구조 쏠림은 낮은 편이라 운영 확장 여지가 있습니다.'
        };
    }

    return {
        label: '매출은 유지 중이고 구조 확인이 필요한 상태입니다.',
        reason: 'Revenue 변화가 크지 않으므로 role-state와 집중도에서 우선 점검 대상을 좁히는 편이 안전합니다.'
    };
}

export function buildBrandOperatingStatusDaily(productDailyMetrics, roleStateDaily, revenueStructureDaily) {
    const roleLookup = new Map(roleStateDaily.map((row) => [`${row.date}|${row.product_id}`, row]));
    const revenueLookup = new Map(revenueStructureDaily.map((row) => [`${row.date}|${row.product_id}`, row]));
    const grouped = productDailyMetrics.reduce((map, row) => {
        if (!map.has(row.date)) {
            map.set(row.date, []);
        }
        map.get(row.date).push(row);
        return map;
    }, new Map());
    const dates = [...grouped.keys()].sort((left, right) => left.localeCompare(right));
    const rows = [];

    dates.forEach((date, index) => {
        const dayRows = grouped.get(date);
        const brandRevenue = dayRows.reduce((sum, row) => sum + Number(row.revenue ?? 0), 0);
        const brandOrderCount = dayRows.reduce((sum, row) => sum + Number(row.order_count ?? 0), 0);
        const activeProductCount = dayRows.filter((row) => Number(row.revenue ?? 0) > 0 || Number(row.order_count ?? 0) > 0).length;
        const observedRows = dayRows.filter((row) => roleLookup.get(`${row.date}|${row.product_id}`)?.pgm_observed_flag === 'true');
        const pgmObservedProductCount = observedRows.length;
        const topProductRevenueShare = Math.max(...dayRows.map((row) => Number(revenueLookup.get(`${row.date}|${row.product_id}`)?.revenue_share_in_brand_day ?? 0)));
        const roleRevenue = new Map();
        const previousBrandRevenue = index > 0 ? rows[index - 1].brand_revenue : null;
        const revenueDelta = previousBrandRevenue == null || previousBrandRevenue === 0
            ? null
            : (brandRevenue - previousBrandRevenue) / previousBrandRevenue;

        dayRows.forEach((row) => {
            // Blank daily states are grouped under a display label for brand-level summaries only.
            const state = roleLookup.get(`${row.date}|${row.product_id}`)?.role_state_primary || ROLE_LABEL_FALLBACK;
            roleRevenue.set(state, (roleRevenue.get(state) ?? 0) + Number(row.revenue ?? 0));
        });

        const dominantRoleState = [...roleRevenue.entries()].sort((left, right) => right[1] - left[1])[0]?.[0] ?? ROLE_LABEL_FALLBACK;
        const coverage = safeDivide(pgmObservedProductCount, activeProductCount) ?? 0;
        const summary = buildStatusSummary({
            revenueDelta,
            topShare: topProductRevenueShare,
            coverage
        });

        rows.push({
            date,
            brand_revenue: brandRevenue,
            brand_order_count: brandOrderCount,
            active_product_count: activeProductCount,
            pgm_observed_product_count: pgmObservedProductCount,
            pgm_observed_coverage: coverage,
            top_product_revenue_share: topProductRevenueShare,
            dominant_role_state_in_revenue: dominantRoleState,
            status_summary_label: summary.label,
            status_reason: summary.reason
        });
    });

    return rows;
}
