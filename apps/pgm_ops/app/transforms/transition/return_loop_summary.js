import { asBooleanString, safeDivide } from '../base/null_handling.js';
import { buildFallbackProductLabelMap } from '../joins/canonical_joins.js';

function average(values) {
    if (!values.length) {
        return null;
    }

    return values.reduce((sum, value) => sum + value, 0) / values.length;
}

export function buildProductReturnLoopSummary(stgPgmLoopDetail, stgProducts, stgOrderItems) {
    const labelLookup = buildFallbackProductLabelMap(stgProducts, stgOrderItems);
    const grouped = stgPgmLoopDetail.reduce((map, row) => {
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
        const returnCaseCount = groupRows.length;
        const qualifiedReturnCount = groupRows.filter((row) => asBooleanString(row.qualified_return_flag) === 'true').length;
        const simpleRepeatCount = groupRows.filter((row) => asBooleanString(row.simple_repeat_comparison_flag) === 'true').length;
        const returnLoopCaseCount = groupRows.filter((row) => asBooleanString(row.qualified_return_flag) === 'true' && Number(row.intermediate_step_cnt ?? 0) > 0).length;
        const returnDays = groupRows
            .map((row) => row.return_days == null ? null : Number(row.return_days))
            .filter((value) => Number.isFinite(value));

        rows.push({
            date,
            product_id: productId,
            product_name: labelLookup.get(productId)?.product_name ?? productId ?? 'n/a',
            return_case_count: returnCaseCount,
            qualified_return_count: qualifiedReturnCount,
            qualified_return_rate: safeDivide(qualifiedReturnCount, returnCaseCount),
            simple_repeat_rate: safeDivide(simpleRepeatCount, returnCaseCount),
            return_loop_rate: safeDivide(returnLoopCaseCount, returnCaseCount),
            avg_return_days: average(returnDays)
        });
    });

    return rows.sort((left, right) => `${left.date}|${left.product_id}`.localeCompare(`${right.date}|${right.product_id}`));
}
