const COLUMN_ALIASES = {
    orders: {
        order_date: 'date'
    },
    order_items: {},
    products: {},
    product_daily: {},
    pgm_scored: {
        snapshot_date: 'date'
    }
};

export function standardizeColumns(rows, datasetKey) {
    const aliases = COLUMN_ALIASES[datasetKey] ?? {};

    return rows.map((row) => {
        const nextRow = {};

        Object.entries(row).forEach(([key, value]) => {
            nextRow[aliases[key] ?? key] = value;
        });

        return nextRow;
    });
}
