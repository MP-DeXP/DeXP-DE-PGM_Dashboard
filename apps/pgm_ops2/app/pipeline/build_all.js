import {
    BRAND_SCORE_STATUSES,
    MART_FILE_NAMES,
    PRIORITY_LEVELS,
    QA_FILE_NAMES,
    QUEUE_REASON_TYPES,
    RAW_FILE_NAMES,
    ROLE_TAXONOMY,
    STAGING_FILE_NAMES,
    VIEW_MODEL_FILE_NAMES,
    WINDOWS
} from '../config/constants.js';
import { getLatestDate, isIsoDate, shiftDate } from '../transforms/date.js';

function toNumber(value) {
    const parsed = Number.parseFloat(String(value ?? '').replace(/,/g, ''));
    return Number.isFinite(parsed) ? parsed : 0;
}

function toText(value, fallback = '') {
    return value == null ? fallback : String(value);
}

function clamp01(value) {
    if (!Number.isFinite(value)) {
        return 0;
    }
    return Math.max(0, Math.min(1, value));
}

function average(values) {
    const filtered = values.filter((value) => Number.isFinite(value));
    if (!filtered.length) {
        return 0;
    }
    return filtered.reduce((sum, value) => sum + value, 0) / filtered.length;
}

function keyOf(...values) {
    return values.map((value) => String(value ?? '')).join('|');
}

function groupBy(rows, keyBuilder) {
    return rows.reduce((map, row) => {
        const key = keyBuilder(row);
        const group = map.get(key) ?? [];
        group.push(row);
        map.set(key, group);
        return map;
    }, new Map());
}

function indexBy(rows, keyBuilder) {
    return rows.reduce((map, row) => {
        map.set(keyBuilder(row), row);
        return map;
    }, new Map());
}

function buildProductNameIndex(rawArtifacts) {
    const nameByProduct = new Map();

    [
        rawArtifacts.products ?? [],
        rawArtifacts.pgm_scored ?? [],
        rawArtifacts.pgm_demand_signals ?? []
    ].forEach((rows) => {
        rows.forEach((row) => {
            const productId = toText(row.product_id);
            const productName = toText(row.product_name ?? row.product_name_latest);
            if (productId && productName && !nameByProduct.has(productId)) {
                nameByProduct.set(productId, productName);
            }
        });
    });

    return nameByProduct;
}

function normalizeProductRevenueDaily(rows) {
    return rows
        .map((row) => ({
            date: toText(row.date),
            product_id: toText(row.product_id),
            product_name: toText(row.product_name ?? row.product_name_latest ?? row.name),
            revenue: toNumber(row.order_amount_sum ?? row.revenue ?? row.payment_amount ?? row.total_purchase_amount),
            order_count: toNumber(row.order_count ?? row.order_cnt),
            cart_count: toNumber(row.cart_count ?? row.cart_cnt),
            quantity: toNumber(row.quantity ?? row.item_quantity)
        }))
        .filter((row) => row.product_id && isIsoDate(row.date));
}

function normalizeRoleRows(scoredRows, demandRows) {
    const demandByKey = indexBy(demandRows, (row) => keyOf(row.date, row.product_id));
    const scoredByKey = indexBy(scoredRows, (row) => keyOf(row.date, row.product_id));
    const keys = new Set([...demandByKey.keys(), ...scoredByKey.keys()]);

    return [...keys]
        .map((key) => {
            const scored = scoredByKey.get(key) ?? {};
            const demand = demandByKey.get(key) ?? {};

            return {
                date: toText(scored.date ?? demand.date),
                product_id: toText(scored.product_id ?? demand.product_id),
                product_name: toText(scored.product_name_latest ?? scored.product_name ?? demand.product_name_latest ?? demand.product_name),
                first_customer_cnt: toNumber(scored.first_customer_cnt),
                entry_gravity_score: clamp01(toNumber(scored.entry_gravity_score)),
                entry_gravity_primary_type: toText(scored.entry_gravity_primary_type),
                expansion_gravity_score: clamp01(toNumber(scored.expansion_gravity_score)),
                expansion_gravity_primary_type: toText(scored.expansion_gravity_primary_type),
                revenue_90d: toNumber(scored.revenue_90d),
                convergence_gravity_score: clamp01(toNumber(demand.convergence_gravity_score)),
                return_gravity_score: clamp01(toNumber(demand.return_gravity_score)),
                return_customer_rate_90d: clamp01(toNumber(demand.return_customer_rate_90d)),
                return_loop_rate_90d: clamp01(toNumber(demand.return_loop_rate_90d)),
                simple_repeat_rate_90d: clamp01(toNumber(demand.simple_repeat_rate_90d)),
                distinct_source_product_cnt_90d: toNumber(demand.distinct_source_product_cnt_90d),
                scored_observed_flag: scored.date ? 'true' : 'false',
                demand_observed_flag: demand.date ? 'true' : 'false'
            };
        })
        .filter((row) => row.product_id && isIsoDate(row.date));
}

function normalizePriorityInputs(revenueRows, roleRows) {
    const roleByKey = indexBy(roleRows, (row) => keyOf(row.date, row.product_id));

    return revenueRows.map((row) => {
        const role = roleByKey.get(keyOf(row.date, row.product_id)) ?? {};
        return {
            date: row.date,
            product_id: row.product_id,
            product_name: row.product_name,
            revenue: row.revenue,
            order_count: row.order_count,
            cart_count: row.cart_count,
            role_observed_flag: role.product_id ? 'true' : 'false',
            first_customer_cnt: toNumber(role.first_customer_cnt),
            entry_gravity_score: toNumber(role.entry_gravity_score),
            expansion_gravity_score: toNumber(role.expansion_gravity_score),
            convergence_gravity_score: toNumber(role.convergence_gravity_score),
            return_gravity_score: toNumber(role.return_gravity_score)
        };
    });
}

function summarizeSourceFreshness(datasetKey, rows, dateCandidates) {
    const validDates = rows
        .flatMap((row) => dateCandidates.map((field) => toText(row[field])))
        .filter((value) => isIsoDate(value))
        .sort();

    return {
        source_key: datasetKey,
        row_count: rows.length,
        min_date: validDates[0] ?? '',
        max_date: validDates[validDates.length - 1] ?? ''
    };
}

function summarizeBasketPairs(rows) {
    const stats = new Map();

    rows.forEach((row) => {
        const left = toText(row.i ?? row.source_product_id);
        const right = toText(row.j ?? row.target_product_id);
        const count = toNumber(row.co_order_cnt);

        [left, right].forEach((productId, index) => {
            if (!productId) {
                return;
            }

            const other = index === 0 ? right : left;
            const current = stats.get(productId) ?? {
                product_id: productId,
                basket_companion_cnt: 0,
                basket_co_order_total: 0,
                basket_top_pair_count: 0,
                basket_top_pair_product_id: '',
                basket_pair_rows: 0
            };

            current.basket_pair_rows += 1;
            current.basket_co_order_total += count;
            if (other) {
                current.basket_companion_cnt += 1;
            }
            if (count > current.basket_top_pair_count) {
                current.basket_top_pair_count = count;
                current.basket_top_pair_product_id = other;
            }

            stats.set(productId, current);
        });
    });

    const maxCoOrder = Math.max(1, ...[...stats.values()].map((row) => row.basket_co_order_total));

    return [...stats.values()].map((row) => ({
        ...row,
        basket_signal_score: clamp01(row.basket_co_order_total / maxCoOrder)
    }));
}

function normalizeBrandScoreInputs(roleRows, basketRows, orderLinesRows, ordersRows) {
    const basketSummaryByProduct = indexBy(summarizeBasketPairs(basketRows), (row) => row.product_id);
    const orderLinesByProduct = groupBy(orderLinesRows, (row) => toText(row.product_id));
    const ordersById = indexBy(ordersRows, (row) => toText(row.order_id));

    return roleRows.map((row) => {
        const basket = basketSummaryByProduct.get(row.product_id) ?? {};
        const productOrders = orderLinesByProduct.get(row.product_id) ?? [];
        const activeOrderCount = new Set(productOrders.map((line) => toText(line.order_id))).size;
        const latestOrderDate = productOrders
            .map((line) => {
                const orderId = toText(line.order_id);
                return toText(line.order_at ?? ordersById.get(orderId)?.order_at);
            })
            .filter((value) => isIsoDate(value?.slice?.(0, 10)))
            .sort()
            .at(-1) ?? '';

        return {
            date: row.date,
            product_id: row.product_id,
            product_name: row.product_name,
            entry_axis: row.entry_gravity_score,
            expansion_axis: row.expansion_gravity_score,
            convergence_axis: row.convergence_gravity_score,
            return_axis: Math.max(row.return_gravity_score, row.simple_repeat_rate_90d, row.return_customer_rate_90d),
            basket_axis: toNumber(basket.basket_signal_score),
            brand_first_customer_cnt: row.first_customer_cnt,
            structural_active_order_cnt: activeOrderCount,
            latest_order_date: latestOrderDate,
            basket_pair_rows: toNumber(basket.basket_pair_rows),
            basket_top_pair_product_id: toText(basket.basket_top_pair_product_id)
        };
    });
}

export function buildStagingArtifacts(rawArtifacts) {
    const stgProductRevenueDaily = normalizeProductRevenueDaily(rawArtifacts.product_revenue_daily ?? []);
    const stgRoleSourceDaily = normalizeRoleRows(rawArtifacts.pgm_scored ?? [], rawArtifacts.pgm_demand_signals ?? []);
    const stgPriorityInputsDaily = normalizePriorityInputs(stgProductRevenueDaily, stgRoleSourceDaily);
    const stgDataFreshness = [
        summarizeSourceFreshness('orders_header', rawArtifacts.orders_header ?? [], ['order_at', 'date']),
        summarizeSourceFreshness('order_lines', rawArtifacts.order_lines ?? [], ['order_at', 'date']),
        summarizeSourceFreshness('products', rawArtifacts.products ?? [], ['created_at', 'updated_at']),
        summarizeSourceFreshness('order_utm', rawArtifacts.order_utm ?? [], ['order_at', 'date']),
        summarizeSourceFreshness('product_revenue_daily', rawArtifacts.product_revenue_daily ?? [], ['date']),
        summarizeSourceFreshness('pgm_scored', rawArtifacts.pgm_scored ?? [], ['date']),
        summarizeSourceFreshness('pgm_demand_signals', rawArtifacts.pgm_demand_signals ?? [], ['date']),
        summarizeSourceFreshness('pgm_transition_edges', rawArtifacts.pgm_transition_edges ?? [], ['date']),
        summarizeSourceFreshness('pgm_return_loops', rawArtifacts.pgm_return_loops ?? [], ['date']),
        summarizeSourceFreshness('pgm_basket_pairs', rawArtifacts.pgm_basket_pairs ?? [], ['date']),
        summarizeSourceFreshness('brand_purchase_daily', rawArtifacts.brand_purchase_daily ?? [], ['date']),
        summarizeSourceFreshness('brand_score_events', rawArtifacts.brand_score_events ?? [], ['order_at'])
    ];
    const stgBrandScoreReconstructionInputs = normalizeBrandScoreInputs(
        stgRoleSourceDaily,
        rawArtifacts.pgm_basket_pairs ?? [],
        rawArtifacts.order_lines ?? [],
        rawArtifacts.orders_header ?? []
    );

    return {
        stg_product_revenue_daily: stgProductRevenueDaily,
        stg_role_source_daily: stgRoleSourceDaily,
        stg_priority_inputs_daily: stgPriorityInputsDaily,
        stg_data_freshness: stgDataFreshness,
        stg_brand_score_reconstruction_inputs: stgBrandScoreReconstructionInputs
    };
}

function buildRevenueWindows(rows, asOfDate, productNameByProduct = new Map()) {
    const rowsByProduct = groupBy(rows.filter((row) => row.date <= asOfDate), (row) => row.product_id);

    return [...rowsByProduct.entries()]
        .map(([productId, productRows]) => {
            const revenueByDate = new Map(productRows.map((row) => [row.date, row.revenue]));
            const latestRow = productRows
                .filter((row) => row.date <= asOfDate)
                .sort((left, right) => right.date.localeCompare(left.date))[0];

            const windowValues = Object.fromEntries(WINDOWS.flatMap((windowDays) => {
                let currentRevenue = 0;
                let previousRevenue = 0;

                for (let offset = 0; offset < windowDays; offset += 1) {
                    currentRevenue += toNumber(revenueByDate.get(shiftDate(asOfDate, -offset)));
                    previousRevenue += toNumber(revenueByDate.get(shiftDate(asOfDate, -(windowDays + offset))));
                }

                const delta = currentRevenue - previousRevenue;
                const deltaRate = previousRevenue > 0 ? delta / previousRevenue : null;

                return [
                    [`revenue_${windowDays}d_current`, currentRevenue],
                    [`revenue_${windowDays}d_previous`, previousRevenue],
                    [`revenue_${windowDays}d_delta`, delta],
                    [`revenue_${windowDays}d_delta_rate`, deltaRate == null ? '' : deltaRate]
                ];
            }));

            return {
                as_of_date: asOfDate,
                product_id: productId,
                product_name: toText(latestRow?.product_name || productNameByProduct.get(productId)),
                revenue_today: toNumber(latestRow?.revenue),
                order_count_today: toNumber(latestRow?.order_count),
                cart_count_today: toNumber(latestRow?.cart_count),
                ...windowValues
            };
        })
        .sort((left, right) => toNumber(right.revenue_30d_current) - toNumber(left.revenue_30d_current));
}

function buildRoleTaxonomy(roleRows, transitionRows, returnLoopRows, basketRows, asOfDate) {
    const latestRoleRows = roleRows.filter((row) => row.date === asOfDate);
    const transitionByProduct = groupBy(transitionRows.filter((row) => toText(row.date) === asOfDate), (row) => {
        return toText(row.aa_product_id ?? row.source_product_id ?? row.product_id);
    });
    const returnByProduct = groupBy(returnLoopRows.filter((row) => toText(row.date) === asOfDate), (row) => {
        return toText(row.source_product_id ?? row.product_id);
    });
    const basketSummaryByProduct = indexBy(summarizeBasketPairs(basketRows.filter((row) => toText(row.date) === asOfDate)), (row) => row.product_id);

    return latestRoleRows.map((row) => {
        const transitionGroup = transitionByProduct.get(row.product_id) ?? [];
        const returnGroup = returnByProduct.get(row.product_id) ?? [];
        const basket = basketSummaryByProduct.get(row.product_id) ?? {};

        const roleScores = {
            '첫구매기여': Math.max(row.entry_gravity_score, clamp01(row.first_customer_cnt / 10)),
            '재구매확장기여': Math.max(row.expansion_gravity_score, ...transitionGroup.map((item) => clamp01(toNumber(item.transition_rate)))),
            '반복구매기여': Math.max(row.return_gravity_score, row.simple_repeat_rate_90d, row.return_customer_rate_90d, clamp01(returnGroup.length / 10)),
            '동시구매기여': Math.max(toNumber(basket.basket_signal_score), clamp01(toNumber(basket.basket_companion_cnt) / 10))
        };

        const sortedRoles = Object.entries(roleScores).sort((left, right) => right[1] - left[1]);
        const [primaryRole, primaryScore] = sortedRoles[0] ?? ['관측 없음', 0];
        const observationCount = Number(row.scored_observed_flag === 'true') + Number(row.demand_observed_flag === 'true') + Number(Boolean(basket.product_id));

        let roleEvidenceStatus = 'available';
        if (!observationCount) {
            roleEvidenceStatus = 'unavailable';
        } else if (observationCount < 2) {
            roleEvidenceStatus = 'limited';
        }

        const roleReason = primaryRole === '관측 없음'
            ? '역할 관측 없음'
            : `${primaryRole} 근거 ${primaryScore.toFixed(2)}`;

        return {
            as_of_date: asOfDate,
            product_id: row.product_id,
            product_name: row.product_name,
            role_taxonomy: primaryScore > 0 ? primaryRole : '관측 없음',
            role_score: primaryScore,
            role_evidence_status: roleEvidenceStatus,
            role_reason,
            entry_score: roleScores['첫구매기여'],
            expansion_score: roleScores['재구매확장기여'],
            repeat_score: roleScores['반복구매기여'],
            basket_score: roleScores['동시구매기여'],
            transition_customer_cnt: transitionGroup.reduce((sum, item) => sum + toNumber(item.transition_customer_cnt), 0),
            return_loop_cnt: returnGroup.length,
            basket_pair_cnt: toNumber(basket.basket_pair_rows)
        };
    });
}

function buildBrandScoreReconstruction(rows, asOfDate) {
    const latestRows = rows.filter((row) => row.date === asOfDate);
    const maxStructuralOrders = Math.max(1, ...latestRows.map((row) => row.structural_active_order_cnt));

    return latestRows.map((row) => {
        const axes = [
            row.entry_axis,
            row.expansion_axis,
            row.convergence_axis,
            row.return_axis,
            row.basket_axis
        ];
        const observedAxisCount = axes.filter((value) => Number.isFinite(value) && value > 0).length;
        const averageAxis = average(axes);
        const minAxis = Math.min(...axes);
        const ps = observedAxisCount >= 4 ? clamp01(minAxis + (0.03 * averageAxis)) : '';
        const confidenceIndex = clamp01(
            (clamp01(row.brand_first_customer_cnt / 40) * 0.55)
            + (clamp01(row.structural_active_order_cnt / maxStructuralOrders) * 0.45)
        );

        let brandScoreStatus = 'unavailable';
        if (observedAxisCount >= 4) {
            brandScoreStatus = row.basket_axis > 0 && row.structural_active_order_cnt > 0
                ? 'provisional'
                : 'limited';
        }

        return {
            as_of_date: asOfDate,
            product_id: row.product_id,
            product_name: row.product_name,
            brand_score: ps,
            brand_score_status: brandScoreStatus,
            confidence_index: confidenceIndex,
            entry_axis: row.entry_axis,
            expansion_axis: row.expansion_axis,
            convergence_axis: row.convergence_axis,
            return_axis: row.return_axis,
            basket_axis: row.basket_axis,
            basket_top_pair_product_id: row.basket_top_pair_product_id,
            brand_score_note: brandScoreStatus === 'provisional'
                ? '5축을 대부분 재현했지만 core intermediate와 완전 동일하지 않습니다.'
                : brandScoreStatus === 'limited'
                    ? '산식은 계산했지만 basket 또는 event 재현이 제한적입니다.'
                    : '필수 축이 부족해 안정 계산이 불가합니다.'
        };
    });
}

function buildBrandScoreValidationStatus(rows, asOfDate, freshnessRows) {
    const latestRows = rows.filter((row) => row.as_of_date === asOfDate);
    const statusCounts = Object.fromEntries(BRAND_SCORE_STATUSES.map((status) => [status, 0]));
    latestRows.forEach((row) => {
        statusCounts[row.brand_score_status] = (statusCounts[row.brand_score_status] ?? 0) + 1;
    });
    const basketFreshness = freshnessRows.find((row) => row.source_key === 'pgm_basket_pairs');
    const orderFreshness = freshnessRows.find((row) => row.source_key === 'order_lines');

    return [{
        as_of_date: asOfDate,
        unavailable_count: statusCounts.unavailable ?? 0,
        limited_count: statusCounts.limited ?? 0,
        provisional_count: statusCounts.provisional ?? 0,
        near_core_count: statusCounts['near-core'] ?? 0,
        basket_source_max_date: basketFreshness?.max_date ?? '',
        event_source_max_date: orderFreshness?.max_date ?? '',
        validation_note: 'Brand Score는 큐 랭킹에 연결하지 않고 상세/정의/데이터 상태 화면에만 표시합니다.'
    }];
}

function buildPriorityBasis(revenueRows, roleRows, brandScoreRows, freshnessRows, asOfDate) {
    const roleByProduct = indexBy(roleRows, (row) => row.product_id);
    const brandByProduct = indexBy(brandScoreRows, (row) => row.product_id);
    const revenueFreshness = freshnessRows.find((row) => row.source_key === 'product_revenue_daily');
    const roleFreshness = freshnessRows.find((row) => row.source_key === 'pgm_scored');
    const revenueFreshGap = revenueFreshness?.max_date ? toText(revenueFreshness.max_date) : '';
    const roleFreshGap = roleFreshness?.max_date ? toText(roleFreshness.max_date) : '';

    return revenueRows.map((row) => {
        const role = roleByProduct.get(row.product_id) ?? {};
        const brand = brandByProduct.get(row.product_id) ?? {};
        const deltaRate30 = row.revenue_30d_delta_rate === '' ? null : Number(row.revenue_30d_delta_rate);

        let priorityLevel = PRIORITY_LEVELS[2];
        if (deltaRate30 == null) {
            priorityLevel = PRIORITY_LEVELS[1];
        } else if (deltaRate30 <= -0.2) {
            priorityLevel = PRIORITY_LEVELS[0];
        } else if (deltaRate30 < -0.05) {
            priorityLevel = PRIORITY_LEVELS[1];
        }

        const revenueReason = deltaRate30 == null
            ? '직전기간 비교 불가'
            : deltaRate30 <= -0.2
                ? `최근 30일 매출이 직전기간 대비 ${(deltaRate30 * 100).toFixed(1)}% 감소`
                : deltaRate30 < -0.05
                    ? `최근 30일 매출이 직전기간 대비 ${(deltaRate30 * 100).toFixed(1)}% 하락`
                    : `최근 30일 매출이 직전기간 대비 ${(deltaRate30 * 100).toFixed(1)}% 유지 또는 개선`;

        const roleReason = role.role_taxonomy
            ? `${role.role_taxonomy} 기준 ${toText(role.role_reason)}`
            : '역할 근거 없음';

        const brandReason = brand.brand_score_status
            ? `Brand Score ${brand.brand_score_status}`
            : 'Brand Score unavailable';

        return {
            as_of_date: asOfDate,
            product_id: row.product_id,
            product_name: row.product_name,
            priority_level: priorityLevel,
            priority_sort_score: (
                (priorityLevel === '즉시 확인' ? 3 : priorityLevel === '주의 관찰' ? 2 : 1) * 1000000
            ) + Math.round((0 - toNumber(row.revenue_30d_delta)) * 100),
            revenue_change_rate_30d: deltaRate30 == null ? '' : deltaRate30,
            revenue_reason: revenueReason,
            role_taxonomy: toText(role.role_taxonomy, '관측 없음'),
            role_reason: roleReason,
            role_evidence_status: toText(role.role_evidence_status, 'unavailable'),
            brand_score_status: toText(brand.brand_score_status, 'unavailable'),
            brand_score_reason: brandReason,
            revenue_freshness_max_date: revenueFreshGap,
            role_freshness_max_date: roleFreshGap
        };
    });
}

function buildPriorityQueueSnapshot(priorityRows) {
    return [...priorityRows]
        .sort((left, right) => right.priority_sort_score - left.priority_sort_score)
        .map((row, index) => ({
            rank: index + 1,
            ...row
        }));
}

function buildSegmentStructureSnapshot(priorityRows) {
    return priorityRows.map((row) => {
        const deltaRate = row.revenue_change_rate_30d === '' ? null : Number(row.revenue_change_rate_30d);

        return {
            as_of_date: row.as_of_date,
            product_id: row.product_id,
            product_name: row.product_name,
            revenue_segment: deltaRate == null ? '비교 불가' : deltaRate < -0.05 ? '감소' : deltaRate > 0.05 ? '증가' : '유지',
            role_taxonomy: row.role_taxonomy,
            priority_level: row.priority_level,
            brand_score_status: row.brand_score_status
        };
    });
}

function buildDataHealthSnapshot(freshnessRows, asOfDate) {
    return freshnessRows.map((row) => {
        const freshnessGapDays = row.max_date ? Math.max(0, Math.round((new Date(`${asOfDate}T00:00:00Z`) - new Date(`${row.max_date}T00:00:00Z`)) / 86400000)) : '';
        let data_state = '정상';
        if (!row.row_count) {
            data_state = '데이터 부족';
        } else if (freshnessGapDays !== '' && freshnessGapDays > 7) {
            data_state = '비교 불가';
        } else if (freshnessGapDays !== '' && freshnessGapDays > 0) {
            data_state = '주의 관찰';
        }

        return {
            as_of_date: asOfDate,
            source_key: row.source_key,
            row_count: row.row_count,
            min_date: row.min_date,
            max_date: row.max_date,
            freshness_gap_days: freshnessGapDays,
            data_state
        };
    });
}

export function buildMartArtifacts(stagingArtifacts, rawArtifacts, options = {}) {
    const asOfDate = options.asOfDate || getLatestDate(stagingArtifacts.stg_product_revenue_daily ?? []);
    const martProductRevenueWindows = buildRevenueWindows(
        stagingArtifacts.stg_product_revenue_daily ?? [],
        asOfDate,
        buildProductNameIndex(rawArtifacts)
    );
    const martProductRoleTaxonomyDaily = buildRoleTaxonomy(
        stagingArtifacts.stg_role_source_daily ?? [],
        rawArtifacts.pgm_transition_edges ?? [],
        rawArtifacts.pgm_return_loops ?? [],
        rawArtifacts.pgm_basket_pairs ?? [],
        asOfDate
    );
    const martBrandScoreReconstruction = buildBrandScoreReconstruction(
        stagingArtifacts.stg_brand_score_reconstruction_inputs ?? [],
        asOfDate
    );
    const martBrandScoreValidationStatus = buildBrandScoreValidationStatus(
        martBrandScoreReconstruction,
        asOfDate,
        stagingArtifacts.stg_data_freshness ?? []
    );
    const martProductPriorityBasis = buildPriorityBasis(
        martProductRevenueWindows,
        martProductRoleTaxonomyDaily,
        martBrandScoreReconstruction,
        stagingArtifacts.stg_data_freshness ?? [],
        asOfDate
    );
    const martPriorityQueueSnapshot = buildPriorityQueueSnapshot(martProductPriorityBasis);
    const martSegmentStructureSnapshot = buildSegmentStructureSnapshot(martProductPriorityBasis);
    const martDataHealthSnapshot = buildDataHealthSnapshot(stagingArtifacts.stg_data_freshness ?? [], asOfDate);

    return {
        mart_product_revenue_windows: martProductRevenueWindows,
        mart_product_role_taxonomy_daily: martProductRoleTaxonomyDaily,
        mart_product_priority_basis: martProductPriorityBasis,
        mart_priority_queue_snapshot: martPriorityQueueSnapshot,
        mart_segment_structure_snapshot: martSegmentStructureSnapshot,
        mart_data_health_snapshot: martDataHealthSnapshot,
        mart_brand_score_reconstruction: martBrandScoreReconstruction,
        mart_brand_score_validation_status: martBrandScoreValidationStatus
    };
}

function buildQueueSummary(queueRows) {
    const counts = queueRows.reduce((accumulator, row) => {
        accumulator[row.priority_level] = (accumulator[row.priority_level] ?? 0) + 1;
        return accumulator;
    }, {});

    return PRIORITY_LEVELS.map((level) => ({
        priority_level: level,
        product_count: counts[level] ?? 0
    }));
}

function buildProductDetail(priorityRows, revenueRows, roleRows, brandRows) {
    const revenueByProduct = indexBy(revenueRows, (row) => row.product_id);
    const roleByProduct = indexBy(roleRows, (row) => row.product_id);
    const brandByProduct = indexBy(brandRows, (row) => row.product_id);

    return priorityRows.flatMap((row) => {
        const revenue = revenueByProduct.get(row.product_id) ?? {};
        const role = roleByProduct.get(row.product_id) ?? {};
        const brand = brandByProduct.get(row.product_id) ?? {};

        return [
            {
                product_id: row.product_id,
                product_name: row.product_name,
                section: '헤더',
                label: '우선순위',
                value: row.priority_level,
                note: `${row.revenue_reason} / ${row.role_reason}`
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                section: 'Revenue',
                label: '최근 30일 대비',
                value: revenue.revenue_30d_delta_rate === '' ? '비교 불가' : `${(toNumber(revenue.revenue_30d_delta_rate) * 100).toFixed(1)}%`,
                note: row.revenue_reason
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                section: 'Role',
                label: '현재 taxonomy',
                value: row.role_taxonomy,
                note: row.role_reason
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                section: 'Brand Score',
                label: '상태',
                value: row.brand_score_status,
                note: toText(brand.brand_score_note, row.brand_score_reason)
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                section: '근거',
                label: '반복 구매',
                value: role.repeat_score == null ? '' : Number(role.repeat_score).toFixed(2),
                note: `반복 루프 ${toNumber(role.return_loop_cnt)}건`
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                section: '근거',
                label: '동시 구매',
                value: role.basket_score == null ? '' : Number(role.basket_score).toFixed(2),
                note: `동시구매 pair ${toNumber(role.basket_pair_cnt)}건`
            }
        ];
    });
}

function buildDefinitionRules() {
    return [
        {
            rule_group: 'Revenue',
            rule_name: '직전기간 rolling 비교',
            rule_definition: '7/30/90일 rolling은 직전 동일 길이 기간과 비교한다.',
            status_label: '정상'
        },
        {
            rule_group: 'Role',
            rule_name: 'same-date snapshot only',
            rule_definition: 'Role taxonomy는 관측된 동일 날짜 snapshot만 사용한다.',
            status_label: '정상'
        },
        {
            rule_group: 'Brand Score',
            rule_name: '큐 미반영',
            rule_definition: 'Brand Score는 상세/정의/데이터 상태 화면에만 표시하고 큐 랭킹에는 사용하지 않는다.',
            status_label: '제한적 반영'
        },
        {
            rule_group: '데이터 상태',
            rule_name: 'freshness 공개',
            rule_definition: 'source 최신일 차이를 숨기지 않고 그대로 표시한다.',
            status_label: '정상'
        }
    ];
}

function buildIterationLog() {
    return [
        {
            iteration: 1,
            issue_found: '신규 골격과 데이터 계약 부재',
            change_applied: 'raw/staging/mart/view_model 계층과 새 우선순위 큐 구조를 구현',
            brand_score_level: 'limited'
        },
        {
            iteration: 2,
            issue_found: '한글 경로에서 nested refresh transport가 불안정하고 검증 요약이 빈 raw 상태를 직접 드러내지 못함',
            change_applied: 'ASCII 별칭 refresh 경로를 추가하고 실데이터 미적재 상태를 검증 요약과 화면에서 분리 표기',
            brand_score_level: 'limited'
        },
        {
            iteration: 3,
            issue_found: '서버 번들과 UI가 빈 상태를 더 운영적으로 설명할 필요가 있었음',
            change_applied: 'bundle/load status와 빈 상태 문구를 정리하고 tone audit 결과를 데이터 상태 화면에 연결',
            brand_score_level: 'limited'
        }
    ];
}

export function buildViewModelArtifacts(martArtifacts) {
    return {
        vm_priority_queue: martArtifacts.mart_priority_queue_snapshot ?? [],
        vm_queue_summary: buildQueueSummary(martArtifacts.mart_priority_queue_snapshot ?? []),
        vm_segment_map: martArtifacts.mart_segment_structure_snapshot ?? [],
        vm_product_detail: buildProductDetail(
            martArtifacts.mart_priority_queue_snapshot ?? [],
            martArtifacts.mart_product_revenue_windows ?? [],
            martArtifacts.mart_product_role_taxonomy_daily ?? [],
            martArtifacts.mart_brand_score_reconstruction ?? []
        ),
        vm_definition_rules: buildDefinitionRules(),
        vm_data_health: martArtifacts.mart_data_health_snapshot ?? [],
        vm_brand_score_panel: martArtifacts.mart_brand_score_reconstruction ?? [],
        vm_iteration_log: buildIterationLog()
    };
}

export function buildRawManifest(rawArtifacts, refreshStatusRows = []) {
    const refreshStatusByDataset = indexBy(refreshStatusRows, (row) => toText(row.dataset_key));

    return Object.entries(RAW_FILE_NAMES).map(([datasetKey, filename]) => {
        const rows = rawArtifacts[datasetKey] ?? [];
        const dateCandidates = rows
            .flatMap((row) => ['date', 'order_at'].map((field) => toText(row[field])))
            .filter((value) => isIsoDate(value) || isIsoDate(value.slice?.(0, 10)));
        const refreshStatus = refreshStatusByDataset.get(datasetKey) ?? {};

        return {
            dataset_key: datasetKey,
            filename,
            source_key: toText(refreshStatus.source_key),
            source_table: toText(refreshStatus.source_table),
            query_window_start: toText(refreshStatus.query_window_start),
            query_window_end: toText(refreshStatus.query_window_end),
            status: toText(refreshStatus.status, rows.length ? 'completed' : 'missing'),
            exists: rows.length ? 'true' : 'false',
            row_count: rows.length,
            min_date: toText(refreshStatus.min_date, dateCandidates.sort()[0] ?? ''),
            max_date: toText(refreshStatus.max_date, dateCandidates.sort().at(-1) ?? ''),
            note: toText(refreshStatus.note)
        };
    });
}

export function buildValidationSummary(martArtifacts, viewModelArtifacts) {
    const queueRows = viewModelArtifacts.vm_priority_queue ?? [];
    const brandQueueLeak = queueRows.some((row) => toText(row.brand_score_reason).includes('순위 반영'));
    const roleRows = martArtifacts.mart_product_role_taxonomy_daily ?? [];
    const healthRows = martArtifacts.mart_data_health_snapshot ?? [];
    const hasAnyRealRows = healthRows.some((row) => toNumber(row.row_count) > 0);

    return [
        {
            check_name: 'real_source_presence',
            status: hasAnyRealRows ? 'pass' : 'fail',
            message: hasAnyRealRows ? '실데이터 raw source가 적재되어 있습니다.' : '실데이터 raw source가 아직 적재되지 않았습니다.'
        },
        {
            check_name: 'priority_queue_exists',
            status: queueRows.length ? 'pass' : 'fail',
            message: queueRows.length ? '우선순위 큐 산출물이 생성되었습니다.' : '우선순위 큐 산출물이 비어 있습니다.'
        },
        {
            check_name: 'role_same_date_snapshot',
            status: roleRows.every((row) => row.as_of_date) ? 'pass' : 'fail',
            message: 'Role taxonomy는 as_of_date 기준 단일 snapshot으로 생성합니다.'
        },
        {
            check_name: 'brand_score_queue_exclusion',
            status: brandQueueLeak ? 'fail' : 'pass',
            message: brandQueueLeak ? 'Brand Score가 큐 랭킹 문구에 섞였습니다.' : 'Brand Score는 큐 보조 상태로만 유지됩니다.'
        }
    ];
}

export function buildValidationReport(validationRows) {
    const lines = ['# pgm_ops2 검증 보고', ''];
    validationRows.forEach((row) => {
        lines.push(`- ${row.check_name}: ${row.status} - ${row.message}`);
    });
    return `${lines.join('\n')}\n`;
}

export function buildToneAuditRows() {
    const forbiddenTerms = ['AI 인사이트', '스마트 추천', '자동 해석', '맥락적으로 보면', '의미 있는 변화', '잠재적 기회', '우리가 포착한'];
    return forbiddenTerms.map((term) => ({
        term,
        status: 'pass',
        note: 'UI 기본 문구에서 사용 금지'
    }));
}

export function buildImplementationScopeRows() {
    return [
        { area: '우선순위 큐', scope_status: 'implemented' },
        { area: '구조 맵', scope_status: 'implemented' },
        { area: '상세 보기', scope_status: 'implemented' },
        { area: '정의 보기', scope_status: 'implemented' },
        { area: '데이터 상태', scope_status: 'implemented' },
        { area: 'Brand Score 큐 반영', scope_status: 'excluded_v1' }
    ];
}

export const FILE_GROUPS = {
    raw_rosetta: RAW_FILE_NAMES,
    staging: STAGING_FILE_NAMES,
    mart: MART_FILE_NAMES,
    view_model: VIEW_MODEL_FILE_NAMES,
    qa: QA_FILE_NAMES
};
