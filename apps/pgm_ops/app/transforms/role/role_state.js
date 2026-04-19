import { createLookup } from '../joins/canonical_joins.js';
import { DEFAULT_ROLE_HISTORY_MODE } from '../../config/constants.js';

function buildLatestLookup(stg_pgm_scored) {
    const rowsByProduct = stg_pgm_scored.reduce((grouped, row) => {
        if (!grouped.has(row.product_id)) {
            grouped.set(row.product_id, []);
        }

        grouped.get(row.product_id).push(row);
        return grouped;
    }, new Map());

    rowsByProduct.forEach((rows) => {
        rows.sort((left, right) => String(left.date ?? '').localeCompare(String(right.date ?? '')));
    });

    return rowsByProduct;
}

function findLatestRow(rows, date) {
    let latest = null;

    rows.forEach((row) => {
        if (row.date <= date && (!latest || row.date > latest.date)) {
            latest = row;
        }
    });

    return latest;
}

export function buildProductRoleStateDaily(stg_pgm_scored, productDailyMetrics, options = {}) {
    const mode = options.roleHistoryMode ?? DEFAULT_ROLE_HISTORY_MODE;
    const lookup = createLookup(stg_pgm_scored, ['date', 'product_id']);
    const latestLookup = mode === 'latest_available' ? buildLatestLookup(stg_pgm_scored) : null;

    return productDailyMetrics.map((row) => {
        const matched = mode === 'latest_available'
            ? findLatestRow(latestLookup.get(row.product_id) ?? [], row.date)
            : lookup.get(`${row.date}|${row.product_id}`);
        const isSameDateMatch = matched?.date === row.date;

        return {
            date: row.date,
            product_id: row.product_id,
            // Default behavior keeps the strict same-date contract; latest_available is opt-in.
            role_state_primary: matched?.role_state_primary ?? '',
            role_state_confidence: matched?.role_state_confidence ?? '',
            pgm_observed_flag: matched?.pgm_observed_flag ?? 'false',
            role_state_source: !matched
                ? 'blank'
                : isSameDateMatch
                    ? 'same_date_snapshot'
                    : 'latest_available_snapshot'
        };
    });
}
