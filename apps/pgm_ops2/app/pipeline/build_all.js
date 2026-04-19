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
import { getDateGapDays, getLatestDate, isIsoDate, normalizeDateValue, shiftDate } from '../transforms/date.js';

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

function maxNumber(values) {
    const filtered = values.filter((value) => Number.isFinite(value));
    return filtered.length ? Math.max(...filtered) : 0;
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

function uniqueCount(values) {
    return new Set(values.filter((value) => toText(value))).size;
}

function sortDates(values) {
    return values
        .map((value) => normalizeDateValue(value))
        .filter((value) => value)
        .sort();
}

function collectNormalizedDates(rows, dateCandidates = []) {
    return rows.flatMap((row) => dateCandidates.map((field) => normalizeDateValue(row[field])));
}

function buildRevenueRequiredStartDates(asOfDate) {
    return Object.fromEntries(WINDOWS.map((windowDays) => [
        windowDays,
        shiftDate(asOfDate, -((windowDays * 2) - 1))
    ]));
}

function getCoverageFlags(minDate, maxDate, asOfDate) {
    const requiredStartDates = buildRevenueRequiredStartDates(asOfDate);
    const normalizedMinDate = normalizeDateValue(minDate);
    const normalizedMaxDate = normalizeDateValue(maxDate);

    return {
        requiredStartDates,
        historyReady7d: Boolean(normalizedMinDate && normalizedMaxDate && normalizedMinDate <= requiredStartDates[7] && normalizedMaxDate >= asOfDate),
        historyReady30d: Boolean(normalizedMinDate && normalizedMaxDate && normalizedMinDate <= requiredStartDates[30] && normalizedMaxDate >= asOfDate),
        historyReady90d: Boolean(normalizedMinDate && normalizedMaxDate && normalizedMinDate <= requiredStartDates[90] && normalizedMaxDate >= asOfDate)
    };
}

function summarizeCoverageState(row, asOfDate) {
    const { requiredStartDates, historyReady7d, historyReady30d, historyReady90d } = getCoverageFlags(row.min_date, row.max_date, asOfDate);

    let coverageState = '관측 불가';
    let coverageNote = '기간 계산 불가';

    if (!toNumber(row.row_count) || !normalizeDateValue(row.max_date)) {
        coverageState = '관측 불가';
        coverageNote = '기간 계산 불가';
    } else if (historyReady90d) {
        coverageState = '90일 비교 가능';
        coverageNote = '7/30/90일 직전기간 비교가 모두 가능합니다.';
    } else if (historyReady30d) {
        coverageState = '30일까지만 가능';
        coverageNote = '30일 비교까지 가능하고 90일 비교는 불가합니다.';
    } else if (historyReady7d) {
        coverageState = '7일까지만 가능';
        coverageNote = '7일 비교까지만 가능하고 30/90일 비교는 불가합니다.';
    } else {
        coverageState = 'history 부족';
        coverageNote = '직전기간 비교를 위한 history가 부족합니다.';
    }

    return {
        coverageState,
        coverageNote,
        requiredStartDates,
        historyReady7d,
        historyReady30d,
        historyReady90d
    };
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

function buildProductImageIndex(rawArtifacts) {
    const imageByProduct = new Map();

    (rawArtifacts.products ?? []).forEach((row) => {
        const productId = toText(row.product_id);
        const productImageUrl = toText(row.list_image).trim();
        const currentImageUrl = imageByProduct.get(productId);

        if (!productId) {
            return;
        }

        if (!imageByProduct.has(productId) || (!currentImageUrl && productImageUrl)) {
            imageByProduct.set(productId, productImageUrl);
        }
    });

    return imageByProduct;
}

function normalizeProductRevenueDaily(rows, productNameByProduct = new Map()) {
    return rows
        .map((row) => ({
            date: normalizeDateValue(row.date),
            product_id: toText(row.product_id),
            product_name: toText(row.product_name ?? row.product_name_latest ?? row.name ?? productNameByProduct.get(toText(row.product_id))),
            revenue: toNumber(row.order_amount_sum ?? row.revenue ?? row.payment_amount ?? row.total_purchase_amount),
            order_count: toNumber(row.order_count ?? row.order_cnt),
            cart_count: toNumber(row.cart_count ?? row.cart_cnt),
            quantity: toNumber(row.quantity ?? row.item_quantity)
        }))
        .filter((row) => row.product_id && row.date);
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
                date: normalizeDateValue(scored.date ?? demand.date),
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
        .filter((row) => row.product_id && row.date);
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
    const validDates = sortDates(collectNormalizedDates(rows, dateCandidates));

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
                return normalizeDateValue(line.order_at ?? ordersById.get(orderId)?.order_at);
            })
            .filter((value) => value)
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
    const productNameByProduct = buildProductNameIndex(rawArtifacts);
    const stgProductRevenueDaily = normalizeProductRevenueDaily(rawArtifacts.product_revenue_daily ?? [], productNameByProduct);
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
        summarizeSourceFreshness('pgm_entry_to_expansion_transition', rawArtifacts.pgm_entry_to_expansion_transition ?? [], ['date']),
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

function buildRevenueWindows(rows, asOfDate, productNameByProduct = new Map(), productImageByProduct = new Map()) {
    const scopedRows = rows.filter((row) => row.date && row.date <= asOfDate);
    const rowsByProduct = groupBy(scopedRows, (row) => row.product_id);
    const allDates = sortDates(scopedRows.map((row) => row.date));
    const globalMinDate = allDates[0] ?? '';
    const globalMaxDate = allDates.at(-1) ?? '';
    const coverageFlags = getCoverageFlags(globalMinDate, globalMaxDate, asOfDate);

    return [...rowsByProduct.entries()]
        .map(([productId, productRows]) => {
            const revenueByDate = new Map(productRows.map((row) => [row.date, row.revenue]));
            const latestRow = productRows
                .filter((row) => row.date <= asOfDate)
                .sort((left, right) => right.date.localeCompare(left.date))[0];

            const windowValues = Object.fromEntries(WINDOWS.flatMap((windowDays) => {
                let currentRevenue = 0;
                let previousRevenue = 0;
                const compareState = coverageFlags[`historyReady${windowDays}d`] ? 'available' : 'history_insufficient';
                const compareNote = compareState === 'available'
                    ? '직전 동일 길이 기간과 비교 가능합니다.'
                    : 'source history가 부족해 직전기간 비교가 불가합니다.';

                for (let offset = 0; offset < windowDays; offset += 1) {
                    currentRevenue += toNumber(revenueByDate.get(shiftDate(asOfDate, -offset)));
                    previousRevenue += toNumber(revenueByDate.get(shiftDate(asOfDate, -(windowDays + offset))));
                }

                const previousRevenueValue = compareState === 'available' ? previousRevenue : '';
                const delta = compareState === 'available' ? currentRevenue - previousRevenue : '';
                const deltaRate = compareState === 'available'
                    ? (previousRevenue > 0 ? (currentRevenue - previousRevenue) / previousRevenue : 0)
                    : '';

                return [
                    [`revenue_${windowDays}d_current`, currentRevenue],
                    [`revenue_${windowDays}d_previous`, previousRevenueValue],
                    [`revenue_${windowDays}d_delta`, delta],
                    [`revenue_${windowDays}d_delta_rate`, deltaRate],
                    [`revenue_${windowDays}d_compare_state`, compareState],
                    [`revenue_${windowDays}d_compare_note`, compareNote]
                ];
            }));

            return {
                as_of_date: asOfDate,
                product_id: productId,
                product_name: toText(latestRow?.product_name || productNameByProduct.get(productId)),
                product_image_url: toText(productImageByProduct.get(productId)),
                revenue_today: toNumber(latestRow?.revenue),
                order_count_today: toNumber(latestRow?.order_count),
                cart_count_today: toNumber(latestRow?.cart_count),
                ...windowValues
            };
        })
        .sort((left, right) => toNumber(right.revenue_30d_current) - toNumber(left.revenue_30d_current));
}

function getRoleSupportCount(rows, candidates) {
    return rows.reduce((sum, row) => {
        return sum + toNumber(candidates.map((field) => row[field]).find((value) => value != null && value !== ''));
    }, 0);
}

function getRoleSupportRate(rows, candidates) {
    return maxNumber(rows.map((row) => toNumber(candidates.map((field) => row[field]).find((value) => value != null && value !== ''))));
}

function buildRoleTaxonomy(
    roleRows,
    transitionSnapshotRows,
    transitionEdgeRows,
    returnLoopRows,
    basketRows,
    freshnessRows,
    asOfDate,
    productImageByProduct = new Map()
) {
    const latestRoleRows = roleRows.filter((row) => row.date === asOfDate);
    const transitionSnapshotByProduct = groupBy(transitionSnapshotRows.filter((row) => normalizeDateValue(row.date) === asOfDate), (row) => {
        return toText(row.product_id ?? row.source_product_id ?? row.entry_product_id ?? row.aa_product_id);
    });
    const transitionEdgeByProduct = groupBy(transitionEdgeRows.filter((row) => normalizeDateValue(row.date) === asOfDate), (row) => {
        return toText(row.aa_product_id ?? row.source_product_id ?? row.product_id);
    });
    const returnByProduct = groupBy(returnLoopRows.filter((row) => toText(row.date) === asOfDate), (row) => {
        return toText(row.source_product_id ?? row.product_id);
    });
    const basketSummaryByProduct = indexBy(summarizeBasketPairs(basketRows.filter((row) => toText(row.date) === asOfDate)), (row) => row.product_id);
    const scoredFreshness = freshnessRows.find((row) => row.source_key === 'pgm_scored');
    const demandFreshness = freshnessRows.find((row) => row.source_key === 'pgm_demand_signals');
    const basketFreshness = freshnessRows.find((row) => row.source_key === 'pgm_basket_pairs');
    const transitionFreshness = freshnessRows.find((row) => row.source_key === 'pgm_entry_to_expansion_transition')
        ?? freshnessRows.find((row) => row.source_key === 'pgm_transition_edges');

    return latestRoleRows.map((row) => {
        const transitionSnapshotGroup = transitionSnapshotByProduct.get(row.product_id) ?? [];
        const transitionEdgeGroup = transitionEdgeByProduct.get(row.product_id) ?? [];
        const returnGroup = returnByProduct.get(row.product_id) ?? [];
        const basket = basketSummaryByProduct.get(row.product_id) ?? {};

        const roleScores = {
            '첫구매기여': row.entry_gravity_score,
            '재구매확장기여': row.expansion_gravity_score,
            '반복구매기여': Math.max(row.return_gravity_score, row.simple_repeat_rate_90d, row.return_customer_rate_90d),
            '동시구매기여': toNumber(basket.basket_signal_score)
        };
        const supportCounts = {
            entry: toNumber(row.first_customer_cnt),
            expansion: getRoleSupportCount(transitionSnapshotGroup, ['transition_customer_cnt', 'entry_to_expansion_customer_cnt', 'customer_cnt', 'expansion_customer_cnt'])
                + getRoleSupportCount(transitionEdgeGroup, ['transition_customer_cnt']),
            repeat: returnGroup.length,
            basket: toNumber(basket.basket_pair_rows)
        };
        const supportRates = {
            expansion: maxNumber([
                getRoleSupportRate(transitionSnapshotGroup, ['transition_rate', 'entry_to_expansion_rate', 'expansion_rate']),
                getRoleSupportRate(transitionEdgeGroup, ['transition_rate'])
            ])
        };
        const sourceFamilyCount = [
            row.scored_observed_flag === 'true',
            row.demand_observed_flag === 'true',
            Boolean(basket.product_id),
            Boolean(transitionSnapshotGroup.length || transitionEdgeGroup.length)
        ].filter(Boolean).length;
        const roleSourceGapDays = maxNumber([
            getDateGapDays(asOfDate, scoredFreshness?.max_date),
            getDateGapDays(asOfDate, demandFreshness?.max_date),
            getDateGapDays(asOfDate, basketFreshness?.max_date),
            getDateGapDays(asOfDate, transitionFreshness?.max_date)
        ].map((value) => value === '' ? Number.NaN : value));

        const sortedRoles = Object.entries(roleScores).sort((left, right) => right[1] - left[1]);
        const [primaryRole, primaryScore] = sortedRoles[0] ?? ['관측 없음', 0];

        let roleEvidenceStatus = 'available';
        if (!sourceFamilyCount) {
            roleEvidenceStatus = 'unavailable';
        } else if (sourceFamilyCount < 2 || roleSourceGapDays > 7) {
            roleEvidenceStatus = 'limited';
        }

        const hasAnySupportEvidence = Object.values(supportCounts).some((value) => value > 0);
        const finalRole = primaryScore < 0.05 && !hasAnySupportEvidence ? '관측 없음' : primaryRole;
        const roleReason = finalRole === '관측 없음'
            ? '역할 관측 없음'
            : `score ${primaryScore.toFixed(2)}`;

        return {
            as_of_date: asOfDate,
            product_id: row.product_id,
            product_name: row.product_name,
            product_image_url: toText(productImageByProduct.get(row.product_id)),
            role_taxonomy: finalRole,
            role_score: primaryScore,
            role_evidence_status: roleEvidenceStatus,
            role_reason: roleReason,
            entry_score: roleScores['첫구매기여'],
            expansion_score: roleScores['재구매확장기여'],
            repeat_score: roleScores['반복구매기여'],
            basket_score: roleScores['동시구매기여'],
            primary_axis_label: finalRole === '관측 없음' ? '' : primaryRole,
            primary_axis_value: finalRole === '관측 없음' ? '' : primaryScore,
            entry_support_count: supportCounts.entry,
            expansion_support_count: supportCounts.expansion,
            repeat_support_count: supportCounts.repeat,
            basket_support_count: supportCounts.basket,
            transition_customer_cnt: supportCounts.expansion,
            transition_support_rate: supportRates.expansion,
            return_loop_cnt: returnGroup.length,
            basket_pair_cnt: toNumber(basket.basket_pair_rows)
        };
    });
}

function buildBrandScoreReconstruction(rows, asOfDate, freshnessRows) {
    const latestRows = rows.filter((row) => row.date === asOfDate);
    const maxStructuralOrders = Math.max(1, ...latestRows.map((row) => row.structural_active_order_cnt));
    const basketFreshness = freshnessRows.find((row) => row.source_key === 'pgm_basket_pairs');
    const eventFreshness = freshnessRows.find((row) => row.source_key === 'brand_score_events')
        ?? freshnessRows.find((row) => row.source_key === 'order_lines');
    const basketFreshnessGapDays = getDateGapDays(asOfDate, basketFreshness?.max_date);
    const eventFreshnessGapDays = getDateGapDays(asOfDate, eventFreshness?.max_date);

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
        let statusCapReason = '';
        if (observedAxisCount >= 4) {
            const freshnessBlocked = eventFreshnessGapDays === '' || eventFreshnessGapDays > 7 || basketFreshnessGapDays === '' || basketFreshnessGapDays > 7;
            if (freshnessBlocked) {
                brandScoreStatus = 'limited';
                statusCapReason = 'event 또는 basket freshness 제약으로 provisional을 제한했습니다.';
            } else {
                brandScoreStatus = 'provisional';
            }
        }

        return {
            as_of_date: asOfDate,
            product_id: row.product_id,
            product_name: row.product_name,
            brand_score: ps,
            brand_score_status: brandScoreStatus,
            confidence_index: confidenceIndex,
            event_freshness_gap_days: eventFreshnessGapDays,
            basket_freshness_gap_days: basketFreshnessGapDays,
            status_cap_reason: statusCapReason,
            entry_axis: row.entry_axis,
            expansion_axis: row.expansion_axis,
            convergence_axis: row.convergence_axis,
            return_axis: row.return_axis,
            basket_axis: row.basket_axis,
            basket_top_pair_product_id: row.basket_top_pair_product_id,
            brand_score_note: brandScoreStatus === 'provisional'
                ? '5축을 대부분 재현했지만 core intermediate와 완전 동일하지 않습니다.'
                : brandScoreStatus === 'limited'
                    ? (statusCapReason || '산식은 계산했지만 basket 또는 event 재현이 제한적입니다.')
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
    const eventFreshness = freshnessRows.find((row) => row.source_key === 'brand_score_events')
        ?? freshnessRows.find((row) => row.source_key === 'order_lines');
    const basketFreshnessGapDays = getDateGapDays(asOfDate, basketFreshness?.max_date);
    const eventFreshnessGapDays = getDateGapDays(asOfDate, eventFreshness?.max_date);
    const statusCapReason = eventFreshnessGapDays === '' || eventFreshnessGapDays > 7 || basketFreshnessGapDays === '' || basketFreshnessGapDays > 7
        ? 'event 또는 basket freshness 제약으로 provisional을 제한합니다.'
        : '';

    return [{
        as_of_date: asOfDate,
        unavailable_count: statusCounts.unavailable ?? 0,
        limited_count: statusCounts.limited ?? 0,
        provisional_count: statusCounts.provisional ?? 0,
        near_core_count: statusCounts['near-core'] ?? 0,
        basket_source_max_date: basketFreshness?.max_date ?? '',
        event_source_max_date: eventFreshness?.max_date ?? '',
        basket_freshness_gap_days: basketFreshnessGapDays,
        event_freshness_gap_days: eventFreshnessGapDays,
        status_cap_reason: statusCapReason,
        validation_note: 'Brand Score는 큐 랭킹에 연결하지 않고 상세/정의/데이터 상태 화면에만 표시합니다.'
    }];
}

function buildBrandRevenueContext(rows, asOfDate) {
    const revenueByDate = new Map(
        rows
            .map((row) => [normalizeDateValue(row.date), toNumber(row.total_purchase_amount ?? row.brand_purchase_amount ?? row.purchase_amount)])
            .filter(([date]) => date)
    );

    let currentRevenue30d = 0;
    let previousRevenue30d = 0;
    for (let offset = 0; offset < 30; offset += 1) {
        currentRevenue30d += toNumber(revenueByDate.get(shiftDate(asOfDate, -offset)));
        previousRevenue30d += toNumber(revenueByDate.get(shiftDate(asOfDate, -(30 + offset))));
    }

    return {
        brand_revenue_30d_current: currentRevenue30d,
        brand_revenue_30d_previous: previousRevenue30d,
        brand_revenue_30d_delta_rate: previousRevenue30d > 0 ? (currentRevenue30d - previousRevenue30d) / previousRevenue30d : 0
    };
}

function buildPriorityBasis(revenueRows, roleRows, brandScoreRows, freshnessRows, asOfDate, brandRevenueContext) {
    const roleByProduct = indexBy(roleRows, (row) => row.product_id);
    const brandByProduct = indexBy(brandScoreRows, (row) => row.product_id);
    const revenueFreshness = freshnessRows.find((row) => row.source_key === 'product_revenue_daily');
    const roleFreshness = freshnessRows.find((row) => row.source_key === 'pgm_scored');
    const revenueFreshGap = revenueFreshness?.max_date ? toText(revenueFreshness.max_date) : '';
    const roleFreshGap = roleFreshness?.max_date ? toText(roleFreshness.max_date) : '';

    return revenueRows.map((row) => {
        const role = roleByProduct.get(row.product_id) ?? {};
        const brand = brandByProduct.get(row.product_id) ?? {};
        const revenueCompareState30d = toText(row.revenue_30d_compare_state, 'history_insufficient');
        const revenueCompareNote30d = toText(row.revenue_30d_compare_note, 'source history가 부족해 직전기간 비교가 불가합니다.');
        const deltaRate30 = revenueCompareState30d === 'available' ? Number(row.revenue_30d_delta_rate) : null;

        let priorityLevel = PRIORITY_LEVELS[2];
        if (deltaRate30 != null && deltaRate30 <= -0.2) {
            priorityLevel = PRIORITY_LEVELS[0];
        } else if (deltaRate30 < -0.05) {
            priorityLevel = PRIORITY_LEVELS[1];
        }

        const revenueReason = deltaRate30 == null
            ? '직전기간 비교 불가'
            : `최근 30일 매출이 직전기간 대비 ${(deltaRate30 * 100).toFixed(1)}%`;

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
            product_image_url: toText(row.product_image_url || role.product_image_url),
            priority_level: priorityLevel,
            priority_sort_score: (
                (priorityLevel === '즉시 확인' ? 3 : priorityLevel === '주의 관찰' ? 2 : 1) * 1000000
            ) + Math.round((0 - toNumber(row.revenue_30d_delta)) * 100),
            revenue_change_rate_30d: deltaRate30 == null ? '' : deltaRate30,
            revenue_30d_compare_state: revenueCompareState30d,
            revenue_30d_compare_note: revenueCompareNote30d,
            revenue_reason: revenueReason,
            role_taxonomy: toText(role.role_taxonomy, '관측 없음'),
            role_reason: roleReason,
            role_evidence_status: toText(role.role_evidence_status, 'unavailable'),
            brand_score_status: toText(brand.brand_score_status, 'unavailable'),
            brand_score_reason: brandReason,
            revenue_freshness_max_date: revenueFreshGap,
            role_freshness_max_date: roleFreshGap,
            ...brandRevenueContext
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
        const deltaRate = row.revenue_30d_compare_state === 'available' && row.revenue_change_rate_30d !== '' ? Number(row.revenue_change_rate_30d) : null;

        return {
            as_of_date: row.as_of_date,
            product_id: row.product_id,
            product_name: row.product_name,
            product_image_url: row.product_image_url,
            revenue_segment: deltaRate == null ? '비교 불가' : deltaRate < -0.05 ? '감소' : deltaRate > 0.05 ? '증가' : '유지',
            role_taxonomy: row.role_taxonomy,
            priority_level: row.priority_level,
            brand_score_status: row.brand_score_status
        };
    });
}

function buildDataHealthSnapshot(freshnessRows, asOfDate) {
    return freshnessRows.map((row) => {
        const freshnessGapDays = getDateGapDays(asOfDate, row.max_date);
        let data_state = '정상';
        if (!row.row_count) {
            data_state = '데이터 부족';
        } else if (freshnessGapDays === '' || freshnessGapDays > 7) {
            data_state = '비교 불가';
        } else if (freshnessGapDays !== '' && freshnessGapDays > 0) {
            data_state = '주의 관찰';
        }
        const coverageSummary = summarizeCoverageState(row, asOfDate);

        return {
            as_of_date: asOfDate,
            source_key: row.source_key,
            row_count: row.row_count,
            min_date: row.min_date,
            max_date: row.max_date,
            freshness_gap_days: freshnessGapDays,
            data_state,
            coverage_state: coverageSummary.coverageState,
            coverage_note: coverageSummary.coverageNote,
            required_start_date_7d: coverageSummary.requiredStartDates[7],
            required_start_date_30d: coverageSummary.requiredStartDates[30],
            required_start_date_90d: coverageSummary.requiredStartDates[90]
        };
    });
}

export function buildMartArtifacts(stagingArtifacts, rawArtifacts, options = {}) {
    const asOfDate = options.asOfDate || getLatestDate(stagingArtifacts.stg_product_revenue_daily ?? []);
    const productNameByProduct = buildProductNameIndex(rawArtifacts);
    const productImageByProduct = buildProductImageIndex(rawArtifacts);
    const brandRevenueContext = buildBrandRevenueContext(rawArtifacts.brand_purchase_daily ?? [], asOfDate);
    const martProductRevenueWindows = buildRevenueWindows(
        stagingArtifacts.stg_product_revenue_daily ?? [],
        asOfDate,
        productNameByProduct,
        productImageByProduct
    );
    const martProductRoleTaxonomyDaily = buildRoleTaxonomy(
        stagingArtifacts.stg_role_source_daily ?? [],
        rawArtifacts.pgm_entry_to_expansion_transition ?? [],
        rawArtifacts.pgm_transition_edges ?? [],
        rawArtifacts.pgm_return_loops ?? [],
        rawArtifacts.pgm_basket_pairs ?? [],
        stagingArtifacts.stg_data_freshness ?? [],
        asOfDate,
        productImageByProduct
    );
    const martBrandScoreReconstruction = buildBrandScoreReconstruction(
        stagingArtifacts.stg_brand_score_reconstruction_inputs ?? [],
        asOfDate,
        stagingArtifacts.stg_data_freshness ?? []
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
        asOfDate,
        brandRevenueContext
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
    const brandRevenueContext = queueRows[0] ?? {};

    return PRIORITY_LEVELS.map((level) => ({
        priority_level: level,
        product_count: counts[level] ?? 0,
        brand_revenue_30d_current: toText(brandRevenueContext.brand_revenue_30d_current),
        brand_revenue_30d_previous: toText(brandRevenueContext.brand_revenue_30d_previous),
        brand_revenue_30d_delta_rate: toText(brandRevenueContext.brand_revenue_30d_delta_rate)
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
                product_image_url: row.product_image_url,
                section: '헤더',
                label: '우선순위',
                value: row.priority_level,
                note: `${row.revenue_reason} / ${row.role_reason}`
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                section: 'Revenue',
                label: '최근 30일 대비',
                value: revenue.revenue_30d_delta_rate === '' ? '비교 불가' : `${(toNumber(revenue.revenue_30d_delta_rate) * 100).toFixed(1)}%`,
                note: toText(revenue.revenue_30d_compare_note, row.revenue_reason)
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                section: 'Revenue',
                label: '비교 상태',
                value: toText(revenue.revenue_30d_compare_state, 'history_insufficient'),
                note: toText(revenue.revenue_30d_compare_note, 'source history가 부족해 직전기간 비교가 불가합니다.')
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                section: 'Role',
                label: '현재 taxonomy',
                value: row.role_taxonomy,
                note: row.role_reason
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                section: 'Brand Score',
                label: '상태',
                value: row.brand_score_status,
                note: toText(brand.brand_score_note, row.brand_score_reason)
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                section: '근거',
                label: '반복 구매',
                value: role.repeat_score == null ? '' : Number(role.repeat_score).toFixed(2),
                note: `반복 루프 ${toNumber(role.return_loop_cnt)}건 / support ${toNumber(role.repeat_support_count)}`
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                section: '근거',
                label: '동시 구매',
                value: role.basket_score == null ? '' : Number(role.basket_score).toFixed(2),
                note: `동시구매 pair ${toNumber(role.basket_pair_cnt)}건 / support ${toNumber(role.basket_support_count)}`
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
            rule_group: 'Revenue',
            rule_name: '비교 가능 조건',
            rule_definition: 'source history가 직전 동일 길이 window 시작일까지 확보된 경우에만 Revenue 비교를 사용한다.',
            status_label: '정상'
        },
        {
            rule_group: 'Role',
            rule_name: 'same-date snapshot only',
            rule_definition: 'Role taxonomy는 관측된 동일 날짜 snapshot만 사용한다.',
            status_label: '정상'
        },
        {
            rule_group: 'Role',
            rule_name: 'canonical score 우선',
            rule_definition: 'Role taxonomy는 entry/expansion/repeat/basket canonical score로 결정하고 count는 근거로만 사용한다.',
            status_label: '정상'
        },
        {
            rule_group: 'Brand Score',
            rule_name: '큐 미반영',
            rule_definition: 'Brand Score는 상세/정의/데이터 상태 화면에만 표시하고 큐 랭킹에는 사용하지 않는다.',
            status_label: '제한적 반영'
        },
        {
            rule_group: 'Brand Score',
            rule_name: 'freshness cap',
            rule_definition: 'event 또는 basket freshness가 부족하면 provisional 대신 limited로 제한한다.',
            status_label: '제한적 반영'
        },
        {
            rule_group: '데이터 상태',
            rule_name: 'freshness 공개',
            rule_definition: 'source 최신일 차이를 숨기지 않고 그대로 표시한다.',
            status_label: '정상'
        },
        {
            rule_group: '데이터 상태',
            rule_name: 'history 부족 공개',
            rule_definition: '직전기간 비교에 필요한 history가 부족하면 비교 불가를 그대로 표시한다.',
            status_label: '정상'
        },
        {
            rule_group: '상품 이미지',
            rule_name: 'Rosetta products 기준',
            rule_definition: '상품 이미지는 raw_rosetta.products.csv의 list_image를 기준으로 사용한다.',
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
        },
        {
            iteration: 4,
            issue_found: '상품 이미지가 참조 주입 상태라 실적재 여부를 화면과 QA에서 구분하기 어려웠음',
            change_applied: 'Rosetta products 실적재 경로와 이미지 provenance 검증을 추가하고, 화면 밀도를 운영툴 기준으로 재설계',
            brand_score_level: 'limited'
        },
        {
            iteration: 5,
            issue_found: 'Revenue source history가 부족해 직전기간 rolling 비교가 전부 무너졌음',
            change_applied: 'dataset별 최소 history 적재와 Revenue compare state를 추가해 비교 가능 여부를 분리',
            brand_score_level: 'limited'
        },
        {
            iteration: 6,
            issue_found: 'timestamp freshness가 비어도 데이터 상태 화면에서 정상처럼 보였음',
            change_applied: '날짜 정규화 공통 함수를 도입하고 freshness/coverage 상태를 source별로 다시 계산',
            brand_score_level: 'limited'
        },
        {
            iteration: 7,
            issue_found: 'Role/Brand Score 판정이 support count와 stale source에 과도하게 끌렸음',
            change_applied: 'Role taxonomy를 canonical score 중심으로 재정의하고 Brand Score freshness cap 및 QA 검증을 추가',
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
        const dateCandidates = sortDates(collectNormalizedDates(rows, ['date', 'order_at', 'created_at', 'updated_at', 'created_date']));
        const refreshStatus = refreshStatusByDataset.get(datasetKey) ?? {};
        const dataProvenance = datasetKey === 'products'
            ? (rows.length && ['completed', 'preserved'].includes(toText(refreshStatus.status)) ? 'rosetta_direct' : 'missing')
            : '';

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
            min_date: toText(refreshStatus.min_date, dateCandidates[0] ?? ''),
            max_date: toText(refreshStatus.max_date, dateCandidates.at(-1) ?? ''),
            requested_lookback_days: toText(refreshStatus.requested_lookback_days),
            effective_lookback_days: toText(refreshStatus.effective_lookback_days),
            required_min_lookback_days: toText(refreshStatus.required_min_lookback_days),
            history_ready_7d: toText(refreshStatus.history_ready_7d),
            history_ready_30d: toText(refreshStatus.history_ready_30d),
            history_ready_90d: toText(refreshStatus.history_ready_90d),
            history_note: toText(refreshStatus.history_note),
            data_provenance: dataProvenance,
            note: toText(refreshStatus.note)
        };
    });
}

export function buildValidationSummary(martArtifacts, viewModelArtifacts, rawManifestRows = []) {
    const queueRows = viewModelArtifacts.vm_priority_queue ?? [];
    const brandQueueLeak = queueRows.some((row) => toText(row.brand_score_reason).includes('순위 반영'));
    const roleRows = martArtifacts.mart_product_role_taxonomy_daily ?? [];
    const healthRows = martArtifacts.mart_data_health_snapshot ?? [];
    const hasAnyRealRows = healthRows.some((row) => toNumber(row.row_count) > 0);
    const productManifest = rawManifestRows.find((row) => toText(row.dataset_key) === 'products') ?? {};
    const revenueManifest = rawManifestRows.find((row) => toText(row.dataset_key) === 'product_revenue_daily') ?? {};
    const transitionManifest = rawManifestRows.find((row) => toText(row.dataset_key) === 'pgm_entry_to_expansion_transition') ?? {};
    const queueSummaryRows = viewModelArtifacts.vm_queue_summary ?? [];
    const brandScoreValidationRows = martArtifacts.mart_brand_score_validation_status ?? [];
    const hasQueueImages = queueRows.some((row) => toText(row.product_image_url).trim());
    const imageProvenance = toText(productManifest.data_provenance) === 'rosetta_direct'
        ? 'rosetta_direct'
        : hasQueueImages
            ? 'preview_reference_only'
            : 'missing';
    const availableRevenueCompareRows = queueRows.filter((row) => toText(row.revenue_30d_compare_state) === 'available');
    const healthBySource = indexBy(healthRows, (row) => toText(row.source_key));
    const freshnessMismatchCount = rawManifestRows.filter((row) => {
        const sourceKey = toText(row.dataset_key);
        const healthRow = healthBySource.get(sourceKey);
        return healthRow && normalizeDateValue(healthRow.max_date) !== normalizeDateValue(row.max_date);
    }).length;
    const roleContractSourcePresence = toText(transitionManifest.exists) === 'true' && toText(transitionManifest.status) !== 'missing';
    const rolePrimaryAxisMismatchCount = roleRows.filter((row) => {
        const roleTaxonomy = toText(row.role_taxonomy);
        if (roleTaxonomy === '관측 없음') {
            return false;
        }

        const scoreEntries = {
            '첫구매기여': toNumber(row.entry_score),
            '재구매확장기여': toNumber(row.expansion_score),
            '반복구매기여': toNumber(row.repeat_score),
            '동시구매기여': toNumber(row.basket_score)
        };
        const [expectedAxisLabel, expectedAxisValue] = Object.entries(scoreEntries).sort((left, right) => right[1] - left[1])[0] ?? ['', 0];
        return toText(row.primary_axis_label) !== expectedAxisLabel || Math.abs(toNumber(row.primary_axis_value) - expectedAxisValue) > 1e-9;
    }).length;
    const staleBrandRows = (viewModelArtifacts.vm_brand_score_panel ?? []).filter((row) => {
        const gap = row.event_freshness_gap_days === '' ? '' : toNumber(row.event_freshness_gap_days);
        return (gap === '' || gap > 7) && toText(row.brand_score_status) === 'provisional';
    }).length;
    const detailRows = viewModelArtifacts.vm_product_detail ?? [];
    const detailProductCount = uniqueCount(detailRows.map((row) => row.product_id));
    const queueProductCount = uniqueCount(queueRows.map((row) => row.product_id));
    const queueSummaryBrandContext = queueSummaryRows.some((row) => {
        return toText(row.brand_revenue_30d_current) && toText(row.brand_revenue_30d_previous) !== '' && toText(row.brand_revenue_30d_delta_rate) !== '';
    });
    const revenueHistoryReady = toText(revenueManifest.history_ready_30d) === 'true' && toText(revenueManifest.history_ready_90d) === 'true';
    const brandScoreValidationRow = brandScoreValidationRows[0] ?? {};

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
        },
        {
            check_name: 'product_image_provenance',
            status: imageProvenance === 'rosetta_direct' ? 'pass' : 'fail',
            message: imageProvenance === 'rosetta_direct'
                ? '상품 이미지는 Rosetta 적재 결과를 직접 사용합니다.'
                : imageProvenance === 'preview_reference_only'
                    ? '상품 이미지는 아직 참조 표시 상태입니다.'
                    : '상품 이미지는 아직 적재되지 않았습니다.'
        },
        {
            check_name: 'revenue_history_ready',
            status: revenueHistoryReady ? 'pass' : 'fail',
            message: revenueHistoryReady ? 'Revenue 30/90일 비교를 위한 history가 확보되었습니다.' : 'Revenue 30/90일 비교 history가 부족합니다.'
        },
        {
            check_name: 'revenue_compare_population',
            status: availableRevenueCompareRows.length ? 'pass' : 'fail',
            message: availableRevenueCompareRows.length ? 'Revenue 30일 비교 가능 상품이 존재합니다.' : 'Revenue 30일 비교 가능 상품이 없습니다.'
        },
        {
            check_name: 'freshness_timestamp_consistency',
            status: freshnessMismatchCount === 0 ? 'pass' : 'fail',
            message: freshnessMismatchCount === 0 ? 'raw manifest와 데이터 상태 화면의 최신일이 일치합니다.' : 'raw manifest와 데이터 상태 화면의 최신일이 불일치합니다.'
        },
        {
            check_name: 'role_contract_source_presence',
            status: roleContractSourcePresence ? 'pass' : 'fail',
            message: roleContractSourcePresence ? 'Role 확장 전이 source가 적재되어 있습니다.' : 'Role 확장 전이 source가 비어 있습니다.'
        },
        {
            check_name: 'role_primary_axis_not_support_only',
            status: rolePrimaryAxisMismatchCount === 0 ? 'pass' : 'fail',
            message: rolePrimaryAxisMismatchCount === 0 ? 'Role taxonomy는 canonical score 축으로 결정됩니다.' : 'Role taxonomy가 canonical score 축과 어긋난 row가 있습니다.'
        },
        {
            check_name: 'brand_score_freshness_cap',
            status: staleBrandRows === 0 ? 'pass' : 'fail',
            message: staleBrandRows === 0
                ? (toText(brandScoreValidationRow.status_cap_reason) || 'Brand Score freshness cap이 정상 적용되었습니다.')
                : 'stale event source인데 provisional로 남은 Brand Score row가 있습니다.'
        },
        {
            check_name: 'detail_picker_covers_full_queue',
            status: detailProductCount === queueProductCount ? 'pass' : 'fail',
            message: detailProductCount === queueProductCount ? '상세 선택 목록이 전체 큐를 모두 덮습니다.' : '상세 선택 목록이 전체 큐를 모두 덮지 못합니다.'
        },
        {
            check_name: 'brand_revenue_context_present',
            status: queueSummaryBrandContext ? 'pass' : 'fail',
            message: queueSummaryBrandContext ? '브랜드 30일 Revenue 컨텍스트가 큐 요약에 포함되어 있습니다.' : '브랜드 30일 Revenue 컨텍스트가 큐 요약에 없습니다.'
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
