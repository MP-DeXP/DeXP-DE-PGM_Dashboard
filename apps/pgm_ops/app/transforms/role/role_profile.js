export function buildProductRoleProfile(stg_pgm_scored) {
    const grouped = stg_pgm_scored.reduce((map, row) => {
        if (!map.has(row.product_id)) {
            map.set(row.product_id, []);
        }
        map.get(row.product_id).push(row);
        return map;
    }, new Map());

    const rows = [];

    grouped.forEach((productRows, productId) => {
        // Profile is intentionally product-grain and uses the latest scored snapshot.
        // This is not a fallback path for daily role state blanks.
        const latest = [...productRows].sort((left, right) => right.date.localeCompare(left.date))[0];

        rows.push({
            product_id: productId,
            profile_role_primary: latest.profile_role_primary ?? '',
            profile_role_secondary: latest.profile_role_secondary ?? '',
            profile_confidence: latest.profile_confidence ?? ''
        });
    });

    return rows.sort((left, right) => left.product_id.localeCompare(right.product_id));
}
