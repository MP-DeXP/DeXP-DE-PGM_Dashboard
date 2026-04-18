import { getLatestDate } from '../../transforms/base/date_windows.js';

function getProductImageUrl(row) {
    return row?.image_url || row?.list_image || row?.detail_image || row?.additional_image_1 || '';
}

export function buildReturnLoopSummaryView(productReturnLoopSummaryRows, productRows = []) {
    const latestDate = getLatestDate(productReturnLoopSummaryRows);
    const productLookup = new Map(productRows.map((row) => [row.product_id, row]));

    return productReturnLoopSummaryRows
        .filter((row) => row.date === latestDate)
        .map((row) => ({
            ...row,
            as_of_date: latestDate,
            product_image_url: getProductImageUrl(productLookup.get(row.product_id))
        }))
        .sort((left, right) => Number(right.return_loop_rate ?? 0) - Number(left.return_loop_rate ?? 0));
}
