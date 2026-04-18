import { getLatestDate } from '../../transforms/base/date_windows.js';

function pushCheck(checks, row) {
    checks.push({
        priority_rank: checks.length + 1,
        ...row
    });
}

export function buildPriorityChecks(brandRows, revenueStructureRows, roleStateDaily, productTableRows) {
    const latestDate = getLatestDate(brandRows);
    const latestBrand = brandRows.find((row) => row.date === latestDate);
    const latestRevenueRows = revenueStructureRows
        .filter((row) => row.date === latestDate)
        .sort((left, right) => Number(left.revenue_rank_in_brand_day ?? 999) - Number(right.revenue_rank_in_brand_day ?? 999));
    const latestRoleLookup = new Map(
        roleStateDaily
            .filter((row) => row.date === latestDate)
            .map((row) => [row.product_id, row])
    );
    const productLookup = new Map(productTableRows.map((row) => [row.product_id, row]));
    const checks = [];

    if (!latestBrand) {
        return checks;
    }

    if (latestBrand.top_product_revenue_share > 0.5) {
        const topRevenueRow = latestRevenueRows[0];
        const topProduct = productLookup.get(topRevenueRow?.product_id);

        pushCheck(checks, {
            priority: 'high',
            entity_type: 'brand',
            entity_id: 'brand',
            label: '상위 상품 쏠림 점검',
            reason: 'brand_operating_status_daily의 top share와 revenue_structure_daily의 1위 매출 구성이 함께 쏠림을 보여 줍니다.',
            suggested_check: '재고, 프로모션, 대체 상품 전개 가능성을 먼저 확인하세요.',
            evidence: `brand.top_product_revenue_share=${latestBrand.top_product_revenue_share}; revenue.rank1=${topProduct?.product_name ?? topRevenueRow?.product_id ?? 'n/a'}`,
            rule_source: 'brand_operating_status_daily.top_product_revenue_share + revenue_structure_daily.revenue_rank_in_brand_day'
        });
    }

    latestRevenueRows
        .filter((row) => Number(row.revenue_rank_in_brand_day ?? 999) <= 2)
        .filter((row) => latestRoleLookup.get(row.product_id)?.pgm_observed_flag !== 'true')
        .forEach((row) => {
            const product = productLookup.get(row.product_id);
            const roleState = latestRoleLookup.get(row.product_id);

            pushCheck(checks, {
                priority: 'medium',
                entity_type: 'product',
                entity_id: row.product_id,
                label: `${product?.product_name ?? row.product_id} role-state 공백`,
                reason: 'revenue_structure_daily 상위 기여 상품인데 product_role_state_daily same-date snapshot은 blank입니다.',
                suggested_check: 'PGM 관측 누락인지 실제 구조 변화인지 먼저 구분하세요.',
                evidence: `revenue.rank=${row.revenue_rank_in_brand_day}; revenue.share=${row.revenue_share_in_brand_day}; role_state_source=${roleState?.role_state_source ?? 'blank'}`,
                rule_source: 'revenue_structure_daily.revenue_rank_in_brand_day + product_role_state_daily.role_state_source'
            });
        });

    if ((latestBrand.brand_revenue_day_over_day_change_rate ?? 0) < -0.08) {
        const focusProducts = latestRevenueRows
            .slice(0, 2)
            .map((row) => productLookup.get(row.product_id)?.product_name ?? row.product_id)
            .join(', ');

        pushCheck(checks, {
            priority: 'medium',
            entity_type: 'brand',
            entity_id: 'brand',
            label: '브랜드 매출 둔화 확인',
            reason: 'brand_operating_status_daily의 day-over-day 하락이 운영 리듬 경계선을 넘었습니다.',
            suggested_check: '상위 매출 상품과 재고/프로모션 변경점을 함께 대조하세요.',
            evidence: `brand.revenue_day_over_day_change_rate=${latestBrand.brand_revenue_day_over_day_change_rate}; focus_products=${focusProducts || 'n/a'}`,
            rule_source: 'brand_operating_status_daily.brand_revenue_day_over_day_change_rate + revenue_structure_daily.revenue_rank_in_brand_day'
        });
    }

    if (!checks.length) {
        pushCheck(checks, {
            priority: 'low',
            entity_type: 'brand',
            entity_id: 'brand',
            label: '즉시 긴급 이슈 없음',
            reason: latestBrand.status_reason,
            suggested_check: '정기 점검 기준으로 coverage와 집중도만 유지 확인하세요.',
            evidence: latestBrand.status_summary_label,
            rule_source: 'brand_operating_status_daily.status_summary_label + brand_operating_status_daily.status_reason'
        });
    }

    return checks.slice(0, 5);
}
