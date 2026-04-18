import { buildFallbackProductLabelMap } from '../joins/canonical_joins.js';

export function buildProductDailyMetrics({ stg_product_daily, stg_products, stg_order_items }) {
    const labelLookup = buildFallbackProductLabelMap(stg_products, stg_order_items);
    const productMetaLookup = new Map(
        stg_products.map((row) => [
            row.product_id,
            {
                image_url: row.list_image || row.detail_image || row.additional_image_1 || '',
                detail_url: row.detail_url || '',
                product_status: row.product_status || ''
            }
        ])
    );

    return [...stg_product_daily]
        .sort((left, right) => `${left.date}|${left.product_id}`.localeCompare(`${right.date}|${right.product_id}`))
        .map((row) => {
            const label = labelLookup.get(row.product_id) ?? {
                product_name: row.product_id,
                product_name_source: 'unmapped'
            };
            const productMeta = productMetaLookup.get(row.product_id) ?? {
                image_url: '',
                detail_url: '',
                product_status: ''
            };

            return {
                date: row.date,
                product_id: row.product_id,
                product_name: label.product_name,
                product_name_source: label.product_name_source,
                image_url: productMeta.image_url,
                detail_url: productMeta.detail_url,
                product_status: productMeta.product_status,
                order_count: Number(row.order_count ?? 0),
                quantity: Number(row.quantity ?? 0),
                revenue: Number(row.revenue ?? 0)
            };
        });
}
