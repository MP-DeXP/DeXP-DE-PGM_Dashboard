export function buildProductDetailHeader(productTableRows) {
    return productTableRows.map((row) => ({
        product_id: row.product_id,
        headline: `${row.product_name} 운영 요약`,
        summary: row.pgm_observed_flag === 'true'
            ? `${row.profile_role_primary} 흐름이 강하게 보이고 현재 상태는 ${row.role_state_primary}로 읽힙니다.${row.top_transition_target_name ? ` 함께 자주 움직이는 상품은 ${row.top_transition_target_name}입니다.` : ''}`
            : `${row.profile_role_primary} 흐름은 보이지만 오늘 상태를 단정할 데이터는 더 필요합니다.${row.top_transition_target_name ? ` 대신 함께 보는 상품으로는 ${row.top_transition_target_name}이 잡힙니다.` : ''}`,
        priority_hint: row.pgm_observed_flag === 'true'
            ? `오늘 매출과 최근 흐름을 함께 보고, 필요하면 복귀율도 같이 확인하세요.${row.return_loop_rate ? ` 복귀율은 ${Number(row.return_loop_rate * 100).toFixed(1)}%입니다.` : ''}`
            : `오늘 상태가 비어 있어도 최근 매출 흐름과 연결 상품을 먼저 보면 다음 액션을 정하기 쉽습니다.`
    }));
}
