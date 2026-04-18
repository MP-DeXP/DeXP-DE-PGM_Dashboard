export function buildProductDetailHeader(productTableRows) {
    return productTableRows.map((row) => ({
        product_id: row.product_id,
        headline: `${row.product_name} 운영 요약`,
        summary: row.role_state_source === 'same_date_snapshot'
            ? `${row.profile_role_primary} profile을 가진 상품이며 same-date role state는 ${row.role_state_primary}입니다.`
            : `${row.profile_role_primary} profile을 가진 상품입니다. same-date role state는 blank이며 latest role로 보정하지 않았습니다.`,
        priority_hint: row.pgm_observed_flag === 'true'
            ? 'same-date role snapshot과 revenue 구조를 함께 점검하세요.'
            : 'same-date role snapshot이 없어 blank로 유지했습니다. latest role fallback 없이 revenue와 관측 누락 여부를 먼저 확인하세요.'
    }));
}
