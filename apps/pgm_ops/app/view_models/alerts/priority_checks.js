import { getLatestDate } from '../../transforms/base/date_windows.js';

function pushCheck(checks, row) {
    checks.push({
        priority_rank: checks.length + 1,
        ...row
    });
}

function asListEvidence(pairs) {
    return pairs
        .filter(([, value]) => value !== '' && value != null)
        .map(([label, value]) => `${label}=${value}`)
        .join('; ');
}

function getTriggeredLoopMetrics(product) {
    const qualifiedReturnRate = Number(product.qualified_return_rate ?? 0);
    const returnLoopRate = Number(product.return_loop_rate ?? 0);
    const simpleRepeatRate = Number(product.simple_repeat_rate ?? 0);
    const triggeredMetrics = [];

    if (qualifiedReturnRate > 0) {
        triggeredMetrics.push('qualified_return_rate');
    }

    if (returnLoopRate >= 0.08) {
        triggeredMetrics.push('return_loop_rate');
    }

    if (simpleRepeatRate >= 0.2) {
        triggeredMetrics.push('simple_repeat_rate');
    }

    return triggeredMetrics;
}

function getLoopTriggerCopy(product) {
    const triggeredMetrics = getTriggeredLoopMetrics(product);

    if (triggeredMetrics.length >= 2) {
        return {
            reason: '왜: 여러 반복 구매 지표가 함께 점화되면 재방문, 반복 연결, 익숙한 재구매가 한꺼번에 섞였는지 같이 봐야 합니다.',
            suggested_check: '다음 확인: 재방문 간격, 반복 구매 패턴, 함께 확인할 전환 상품을 한 흐름으로 비교하세요.'
        };
    }

    if (triggeredMetrics.includes('simple_repeat_rate')) {
        return {
            reason: '왜: 반복 구매율 중심으로만 신호가 올라오면 익숙한 재구매 흐름인지, 특정 상황에만 반복된 구매인지 먼저 구분해야 합니다.',
            suggested_check: '다음 확인: 반복 구매가 몰린 기간, 함께 팔린 상품, 프로모션 영향 여부를 순서대로 확인하세요.'
        };
    }

    if (triggeredMetrics.includes('qualified_return_rate')) {
        return {
            reason: '왜: 재방문율 중심으로 신호가 올라오면 한 번 이탈한 고객이 다시 돌아온 이유를 먼저 확인해야 합니다.',
            suggested_check: '다음 확인: 재방문 간격, 직전 구매 이후 변화, 복귀 전에 많이 연결된 상품을 차례로 보세요.'
        };
    }

    return {
        reason: '왜: 반복 연결 비중 중심으로 신호가 올라오면 특정 상품 묶음이나 순환 구매 흐름이 강화됐는지 확인해야 합니다.',
        suggested_check: '다음 확인: 반복 연결된 상품 조합, 연결 직전 주문 흐름, 함께 유지된 프로모션을 먼저 보세요.'
    };
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
            reason: '왜: 상위 1개 상품 매출 비중이 50%를 넘으면 브랜드 운영 신호가 한 상품에 과도하게 묶일 수 있습니다.',
            suggested_check: '다음 확인: 1위 상품 재고, 당일 프로모션, 대체 상품 전개 가능성을 순서대로 확인하세요.',
            evidence: `근거: ${asListEvidence([
                ['상위 1개 상품 매출 비중', latestBrand.top_product_revenue_share],
                ['매출 1위 상품', topProduct?.product_name ?? topRevenueRow?.product_id ?? '데이터 없음']
            ])}`,
            rule_source: 'brand_operating_status_daily.top_product_revenue_share + revenue_structure_daily.revenue_rank_in_brand_day'
        });
    }

    latestRevenueRows
        .filter((row) => Number(row.revenue_rank_in_brand_day ?? 999) <= 2)
        .filter((row) => latestRoleLookup.get(row.product_id)?.pgm_observed_flag !== 'true')
        .forEach((row) => {
            const product = productLookup.get(row.product_id);
            const roleState = latestRoleLookup.get(row.product_id);
            const transitionEvidence = product?.top_transition_target_name
                ? [
                    ['연결 상품', product.top_transition_target_name],
                    ['연결 비중', product.top_transition_rate ?? '데이터 없음']
                ]
                : [];

            pushCheck(checks, {
                priority: 'medium',
                entity_type: 'product',
                entity_id: row.product_id,
                label: `${product?.product_name ?? row.product_id} 당일 관측 상태 없음`,
                reason: '왜: 매출 상위 상품인데 기준일 역할 관측 상태가 비어 있어 최근 확정일 해석을 확정할 근거가 부족합니다.',
                suggested_check: '다음 확인: PGM 관측 누락 여부, 기준일 매출 기여도, 연결 상품 전환 신호를 차례로 확인하세요.',
                evidence: `근거: ${asListEvidence([
                    ['매출 순위', row.revenue_rank_in_brand_day],
                    ['브랜드 내 매출 비중', row.revenue_share_in_brand_day],
                    ['관측 상태 근거', roleState?.role_state_source ?? '관측 상태 없음'],
                    ...transitionEvidence
                ])}`,
                rule_source: 'revenue_structure_daily.revenue_rank_in_brand_day + product_role_state_daily.role_state_source + product_transition_summary.transition_rate'
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
            reason: '왜: 전일 대비 매출 하락 폭이 경계선을 넘으면 상위 상품 구조와 운영 변경점을 같이 봐야 합니다.',
            suggested_check: '다음 확인: 상위 매출 상품의 재고, 프로모션, 유입 차이가 동시에 있었는지 대조하세요.',
            evidence: `근거: ${asListEvidence([
                ['전일 대비 매출 증감률', latestBrand.brand_revenue_day_over_day_change_rate],
                ['중점 상품', focusProducts || '데이터 없음']
            ])}`,
            rule_source: 'brand_operating_status_daily.brand_revenue_day_over_day_change_rate + revenue_structure_daily.revenue_rank_in_brand_day'
        });
    }

    latestRevenueRows
        .filter((row) => Number(row.revenue_rank_in_brand_day ?? 999) <= 3)
        .map((row) => productLookup.get(row.product_id))
        .filter(Boolean)
        .filter((row) => getTriggeredLoopMetrics(row).length > 0)
        .forEach((product) => {
            const loopTriggerCopy = getLoopTriggerCopy(product);

            pushCheck(checks, {
                priority: 'medium',
                entity_type: 'product',
                entity_id: product.product_id,
                label: `${product.product_name} 복귀/반복 루프 점검`,
                reason: loopTriggerCopy.reason,
                suggested_check: loopTriggerCopy.suggested_check,
                evidence: `근거: ${asListEvidence([
                    ['재방문율', product.qualified_return_rate ?? '데이터 없음'],
                    ['반복 연결 비중', product.return_loop_rate ?? '데이터 없음'],
                    ['반복 구매율', product.simple_repeat_rate ?? '데이터 없음'],
                    ['함께 확인할 전환 상품', product.top_transition_target_name || '데이터 없음']
                ])}`,
                rule_source: 'product_return_loop_summary.return_loop_rate + product_return_loop_summary.simple_repeat_rate + product_transition_summary.transition_rate'
            });
        });

    if (!checks.length) {
        pushCheck(checks, {
            priority: 'low',
            entity_type: 'brand',
            entity_id: 'brand',
            label: '즉시 긴급 이슈 없음',
            reason: `왜: ${latestBrand.status_reason}`,
            suggested_check: '다음 확인: 정기 점검 기준으로 관측 범위와 집중도만 유지 확인하세요.',
            evidence: `근거: ${latestBrand.status_summary_label}`,
            rule_source: 'brand_operating_status_daily.status_summary_label + brand_operating_status_daily.status_reason'
        });
    }

    return checks.slice(0, 5);
}
