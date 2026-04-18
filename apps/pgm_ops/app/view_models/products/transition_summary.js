import { getLatestDate } from '../../transforms/base/date_windows.js';

function getProductImageUrl(row) {
    return row?.image_url || row?.list_image || row?.detail_image || row?.additional_image_1 || '';
}

export function buildTransitionSummaryView(productTransitionSummaryRows, productRows = []) {
    const latestDate = getLatestDate(productTransitionSummaryRows);
    const productLookup = new Map(productRows.map((row) => [row.product_id, row]));

    return productTransitionSummaryRows
        .filter((row) => row.date === latestDate && Number(row.transition_rank ?? 999) <= 3)
        .map((row) => ({
            ...row,
            as_of_date: latestDate,
            product_image_url: getProductImageUrl(productLookup.get(row.product_id)),
            target_product_image_url: getProductImageUrl(productLookup.get(row.target_product_id))
        }))
        .sort((left, right) => {
            const productCompare = String(left.product_name ?? '').localeCompare(String(right.product_name ?? ''));
            if (productCompare !== 0) {
                return productCompare;
            }

            return Number(left.transition_rank ?? 0) - Number(right.transition_rank ?? 0);
        });
}
