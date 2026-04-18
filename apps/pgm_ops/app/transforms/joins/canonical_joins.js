export const FORBIDDEN_JOINS = [
    'product_daily_fact↔order_item',
    'product_daily_fact↔order_with_utm',
    'order_item.customer_id↔member.member_id'
];

export function assertJoinAllowed(joinName) {
    if (FORBIDDEN_JOINS.includes(joinName)) {
        throw new Error(`Forbidden join attempted: ${joinName}`);
    }
}

export function createLookup(rows, keyFields) {
    return rows.reduce((lookup, row) => {
        const key = keyFields.map((field) => row[field] ?? '').join('|');
        lookup.set(key, row);
        return lookup;
    }, new Map());
}

export function buildFallbackProductLabelMap(products, orderItems) {
    const labels = new Map();

    products.forEach((row) => {
        labels.set(row.product_id, {
            product_name: row.product_name,
            product_name_source: 'product_master'
        });
    });

    orderItems.forEach((row) => {
        if (!labels.has(row.product_id) && row.product_name) {
            labels.set(row.product_id, {
                product_name: row.product_name,
                product_name_source: 'order_item_fallback'
            });
        }
    });

    return labels;
}
