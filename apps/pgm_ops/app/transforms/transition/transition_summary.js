import { buildFallbackProductLabelMap } from '../joins/canonical_joins.js';

function getProductLabel(labelLookup, productId) {
    return labelLookup.get(productId)?.product_name ?? productId ?? 'n/a';
}

export function buildProductTransitionSummary(stgPgmTransitionEdge, stgProducts, stgOrderItems) {
    const labelLookup = buildFallbackProductLabelMap(stgProducts, stgOrderItems);
    const grouped = stgPgmTransitionEdge.reduce((map, row) => {
        const key = `${row.date}|${row.source_product_id}`;
        if (!map.has(key)) {
            map.set(key, []);
        }
        map.get(key).push(row);
        return map;
    }, new Map());

    const rows = [];

    grouped.forEach((groupRows, key) => {
        const [date, productId] = key.split('|');
        const productName = getProductLabel(labelLookup, productId);
        const rankedRows = [...groupRows].sort((left, right) => {
            const rateGap = Number(right.transition_rate ?? 0) - Number(left.transition_rate ?? 0);
            if (rateGap !== 0) {
                return rateGap;
            }

            const customerGap = Number(right.transition_customer_cnt ?? 0) - Number(left.transition_customer_cnt ?? 0);
            if (customerGap !== 0) {
                return customerGap;
            }

            return String(left.target_product_id ?? '').localeCompare(String(right.target_product_id ?? ''));
        });

        rankedRows.forEach((row, index) => {
            rows.push({
                date,
                product_id: productId,
                product_name: productName,
                target_product_id: row.target_product_id,
                target_product_name: getProductLabel(labelLookup, row.target_product_id),
                transition_rank: index + 1,
                transition_customer_cnt: Number(row.transition_customer_cnt ?? 0),
                source_cohort_customer_cnt: Number(row.source_cohort_customer_cnt ?? 0),
                transition_rate: Number(row.transition_rate ?? 0),
                avg_days_to_transition: row.avg_days_to_transition == null ? null : Number(row.avg_days_to_transition)
            });
        });
    });

    return rows.sort((left, right) => {
        const keyCompare = `${left.date}|${left.product_id}`.localeCompare(`${right.date}|${right.product_id}`);
        if (keyCompare !== 0) {
            return keyCompare;
        }

        return Number(left.transition_rank ?? 0) - Number(right.transition_rank ?? 0);
    });
}
