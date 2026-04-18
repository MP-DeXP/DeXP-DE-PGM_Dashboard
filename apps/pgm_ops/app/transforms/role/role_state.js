import { createLookup } from '../joins/canonical_joins.js';

export function buildProductRoleStateDaily(stg_pgm_scored, productDailyMetrics) {
    const lookup = createLookup(stg_pgm_scored, ['date', 'product_id']);

    return productDailyMetrics.map((row) => {
        const matched = lookup.get(`${row.date}|${row.product_id}`);

        return {
            date: row.date,
            product_id: row.product_id,
            // Daily role state is same-date only. Missing same-date snapshots stay blank.
            role_state_primary: matched?.role_state_primary ?? '',
            role_state_confidence: matched?.role_state_confidence ?? '',
            pgm_observed_flag: matched?.pgm_observed_flag ?? 'false',
            role_state_source: matched ? 'same_date_snapshot' : 'blank'
        };
    });
}
