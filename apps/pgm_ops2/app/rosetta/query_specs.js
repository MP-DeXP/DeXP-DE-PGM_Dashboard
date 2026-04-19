const CORE_CONNECTION_ID = '085fb59e-abe8-4071-9ad1-3d406309f8fb';
const DMA_CONNECTION_ID = '8bf973ba-50e2-4279-a183-2c1791038bbb';

function buildDateWindowClause(fieldName, startDate, asOfDate) {
    return `CAST(${fieldName} AS DATE) BETWEEN DATE '${startDate}' AND DATE '${asOfDate}'`;
}

function buildAsOfDateClause(fieldName, asOfDate) {
    return `CAST(${fieldName} AS DATE) = DATE '${asOfDate}'`;
}

function buildPagedQuery({ selectSql, fromSql, whereSql, orderBySql, limit, offset }) {
    return [
        'SELECT *',
        'FROM (',
        `    ${selectSql}`,
        `    ${fromSql}`,
        `    WHERE ${whereSql}`,
        ') t',
        `ORDER BY ${orderBySql}`,
        `LIMIT ${limit}`,
        `OFFSET ${offset}`
    ].join('\n');
}

export function getRawDatasetColumns() {
    return {
        orders_header: ['order_id', 'order_at', 'member_id', 'order_amount', 'created_date', 'mx_channel_id', 'mx_platform'],
        order_lines: ['order_id', 'order_at', 'product_id', 'product_name', 'quantity', 'order_status', 'customer_id', 'discount_amount', 'payment_amount', 'mx_channel_id', 'mx_platform'],
        products: ['product_id', 'product_name', 'price', 'retail_price', 'category_id_list', 'detail_url', 'list_image', 'is_display', 'is_selling', 'is_sold_out', 'created_at', 'updated_at', 'mx_channel_id', 'mx_platform'],
        order_utm: ['order_id', 'order_at', 'purchase_amount', 'utm_source', 'utm_medium', 'utm_campaign', 'session_count', 'valid_session_count', 'created_date', 'mx_channel_id', 'mx_platform'],
        product_revenue_daily: ['date', 'product_id', 'product_name', 'order_count', 'quantity', 'order_amount_sum', 'cart_count', 'cart_amount_sum', 'cart_quantity_sum', 'mx_channel_id', 'mx_platform'],
        pgm_scored: ['date', 'product_id', 'product_name_latest', 'first_customer_cnt', 'revenue_90d', 'entry_gravity_primary_type', 'entry_gravity_score', 'expansion_gravity_primary_type', 'expansion_gravity_score'],
        pgm_demand_signals: ['date', 'product_id', 'product_name_latest', 'convergence_gravity_score', 'return_gravity_score', 'distinct_source_product_cnt_90d', 'return_customer_rate_90d', 'return_loop_rate_90d', 'simple_repeat_rate_90d'],
        pgm_transition_edges: ['date', 'source_product_id', 'target_product_id', 'transition_customer_cnt', 'source_cohort_customer_cnt', 'transition_rate', 'avg_days_to_transition'],
        pgm_return_loops: ['date', 'customer_id', 'source_product_id', 'return_product_id', 'source_order_id', 'return_order_id', 'return_days', 'intermediate_step_cnt', 'intermediate_distinct_product_cnt', 'intermediate_path', 'qualified_return_flag', 'simple_repeat_comparison_flag'],
        pgm_basket_pairs: ['date', 'i', 'j', 'co_order_cnt', 'rn'],
        brand_purchase_daily: ['date', 'total_purchase_amount', 'new_purchase_amount', 're_purchase_amount'],
        brand_score_events: ['order_id', 'order_at', 'product_id', 'member_id', 'event_type', 'quantity', 'payment_amount']
    };
}

export function getRosettaQuerySpecs({ asOfDate, lookbackStart }) {
    const windowedOrderAt = buildDateWindowClause('order_at', lookbackStart, asOfDate);
    const windowedDate = buildDateWindowClause('date', lookbackStart, asOfDate);
    const latestPgmDate = buildAsOfDateClause('date', asOfDate);

    return [
        {
            datasetKey: 'orders_header',
            sourceKey: 'core_order_header',
            sourceTable: 'silver_meta_order',
            connectionId: CORE_CONNECTION_ID,
            columns: getRawDatasetColumns().orders_header,
            dateFields: ['order_at'],
            pageSize: 500,
            buildSql: ({ limit, offset }) => buildPagedQuery({
                selectSql: [
                    'SELECT',
                    '    CAST(order_id AS VARCHAR) AS order_id,',
                    '    CAST(order_at AS VARCHAR) AS order_at,',
                    '    CAST(member_id AS VARCHAR) AS member_id,',
                    '    CAST(order_amount_info.order_amount AS VARCHAR) AS order_amount,',
                    '    CAST(created_date AS VARCHAR) AS created_date,',
                    '    CAST(mx_channel_id AS VARCHAR) AS mx_channel_id,',
                    '    CAST(mx_platform AS VARCHAR) AS mx_platform'
                ].join('\n'),
                fromSql: 'FROM silver_meta_order',
                whereSql: windowedOrderAt,
                orderBySql: 'order_at, order_id',
                limit,
                offset
            })
        },
        {
            datasetKey: 'order_lines',
            sourceKey: 'core_order_line',
            sourceTable: 'silver_meta_order_item',
            connectionId: CORE_CONNECTION_ID,
            columns: getRawDatasetColumns().order_lines,
            dateFields: ['order_at'],
            pageSize: 500,
            buildSql: ({ limit, offset }) => buildPagedQuery({
                selectSql: [
                    'SELECT',
                    '    CAST(order_id AS VARCHAR) AS order_id,',
                    '    CAST(order_at AS VARCHAR) AS order_at,',
                    '    CAST(product_id AS VARCHAR) AS product_id,',
                    '    CAST(product_name AS VARCHAR) AS product_name,',
                    '    CAST(quantity AS VARCHAR) AS quantity,',
                    '    CAST(order_status AS VARCHAR) AS order_status,',
                    '    CAST(customer_id AS VARCHAR) AS customer_id,',
                    '    CAST(discount_amount AS VARCHAR) AS discount_amount,',
                    '    CAST(payment_amount AS VARCHAR) AS payment_amount,',
                    '    CAST(mx_channel_id AS VARCHAR) AS mx_channel_id,',
                    '    CAST(mx_platform AS VARCHAR) AS mx_platform'
                ].join('\n'),
                fromSql: 'FROM silver_meta_order_item',
                whereSql: windowedOrderAt,
                orderBySql: 'order_at, order_id, product_id',
                limit,
                offset
            })
        },
        {
            datasetKey: 'products',
            sourceKey: 'core_product',
            sourceTable: 'silver_meta_product',
            connectionId: CORE_CONNECTION_ID,
            columns: getRawDatasetColumns().products,
            dateFields: ['created_at', 'updated_at'],
            pageSize: 500,
            buildSql: ({ limit, offset }) => buildPagedQuery({
                selectSql: [
                    'SELECT',
                    '    CAST(product_id AS VARCHAR) AS product_id,',
                    '    CAST(product_name AS VARCHAR) AS product_name,',
                    '    CAST(price AS VARCHAR) AS price,',
                    '    CAST(retail_price AS VARCHAR) AS retail_price,',
                    '    CAST(category_id_list AS VARCHAR) AS category_id_list,',
                    '    CAST(detail_url AS VARCHAR) AS detail_url,',
                    '    CAST(list_image AS VARCHAR) AS list_image,',
                    '    CAST(is_display AS VARCHAR) AS is_display,',
                    '    CAST(is_selling AS VARCHAR) AS is_selling,',
                    '    CAST(is_sold_out AS VARCHAR) AS is_sold_out,',
                    '    CAST(created_at AS VARCHAR) AS created_at,',
                    '    CAST(updated_at AS VARCHAR) AS updated_at,',
                    '    CAST(mx_channel_id AS VARCHAR) AS mx_channel_id,',
                    '    CAST(mx_platform AS VARCHAR) AS mx_platform'
                ].join('\n'),
                fromSql: 'FROM silver_meta_product',
                whereSql: 'TRUE',
                orderBySql: 'product_id',
                limit,
                offset
            })
        },
        {
            datasetKey: 'order_utm',
            sourceKey: 'dma_order_utm',
            sourceTable: 'silver_order_with_utm',
            connectionId: DMA_CONNECTION_ID,
            columns: getRawDatasetColumns().order_utm,
            dateFields: ['order_at'],
            pageSize: 500,
            buildSql: ({ limit, offset }) => buildPagedQuery({
                selectSql: [
                    'SELECT',
                    '    CAST(order_id AS VARCHAR) AS order_id,',
                    '    CAST(order_at AS VARCHAR) AS order_at,',
                    '    CAST(purchase_amount AS VARCHAR) AS purchase_amount,',
                    '    CAST(utm_source AS VARCHAR) AS utm_source,',
                    '    CAST(utm_medium AS VARCHAR) AS utm_medium,',
                    '    CAST(utm_campaign AS VARCHAR) AS utm_campaign,',
                    '    CAST(session_count AS VARCHAR) AS session_count,',
                    '    CAST(valid_session_count AS VARCHAR) AS valid_session_count,',
                    '    CAST(created_date AS VARCHAR) AS created_date,',
                    '    CAST(mx_channel_id AS VARCHAR) AS mx_channel_id,',
                    '    CAST(mx_platform AS VARCHAR) AS mx_platform'
                ].join('\n'),
                fromSql: 'FROM silver_order_with_utm',
                whereSql: windowedOrderAt,
                orderBySql: 'order_at, order_id',
                limit,
                offset
            })
        },
        {
            datasetKey: 'product_revenue_daily',
            sourceKey: 'dma_product_revenue_daily',
            sourceTable: 'silver_fact_product',
            connectionId: DMA_CONNECTION_ID,
            columns: getRawDatasetColumns().product_revenue_daily,
            dateFields: ['date'],
            pageSize: 500,
            buildSql: ({ limit, offset }) => buildPagedQuery({
                selectSql: [
                    'SELECT',
                    '    CAST(date AS VARCHAR) AS date,',
                    '    CAST(product_id AS VARCHAR) AS product_id,',
                    "    '' AS product_name,",
                    '    CAST(order_count AS VARCHAR) AS order_count,',
                    '    CAST(order_quantity_sum AS VARCHAR) AS quantity,',
                    '    CAST(order_amount_sum AS VARCHAR) AS order_amount_sum,',
                    '    CAST(cart_count AS VARCHAR) AS cart_count,',
                    '    CAST(cart_amount_sum AS VARCHAR) AS cart_amount_sum,',
                    '    CAST(cart_quantity_sum AS VARCHAR) AS cart_quantity_sum,',
                    '    CAST(mx_channel_id AS VARCHAR) AS mx_channel_id,',
                    '    CAST(mx_platform AS VARCHAR) AS mx_platform'
                ].join('\n'),
                fromSql: 'FROM silver_fact_product',
                whereSql: windowedDate,
                orderBySql: 'date, product_id',
                limit,
                offset
            })
        },
        {
            datasetKey: 'pgm_scored',
            sourceKey: 'dma_pgm_scored',
            sourceTable: 'gold_pgm_scored',
            connectionId: DMA_CONNECTION_ID,
            columns: getRawDatasetColumns().pgm_scored,
            dateFields: ['date'],
            pageSize: 500,
            buildSql: ({ limit, offset }) => buildPagedQuery({
                selectSql: [
                    'SELECT',
                    '    CAST(date AS VARCHAR) AS date,',
                    '    CAST(product_id AS VARCHAR) AS product_id,',
                    '    CAST(product_name_latest AS VARCHAR) AS product_name_latest,',
                    '    CAST(first_customer_cnt AS VARCHAR) AS first_customer_cnt,',
                    '    CAST(revenue_90d AS VARCHAR) AS revenue_90d,',
                    '    CAST(entry_gravity_primary_type AS VARCHAR) AS entry_gravity_primary_type,',
                    '    CAST(entry_gravity_score AS VARCHAR) AS entry_gravity_score,',
                    '    CAST(expansion_gravity_primary_type AS VARCHAR) AS expansion_gravity_primary_type,',
                    '    CAST(expansion_gravity_score AS VARCHAR) AS expansion_gravity_score'
                ].join('\n'),
                fromSql: 'FROM gold_pgm_scored',
                whereSql: windowedDate,
                orderBySql: 'date, product_id',
                limit,
                offset
            })
        },
        {
            datasetKey: 'pgm_demand_signals',
            sourceKey: 'dma_pgm_demand_signals',
            sourceTable: 'gold_pgm_product_demand_gravity',
            connectionId: DMA_CONNECTION_ID,
            columns: getRawDatasetColumns().pgm_demand_signals,
            dateFields: ['date'],
            pageSize: 500,
            buildSql: ({ limit, offset }) => buildPagedQuery({
                selectSql: [
                    'SELECT',
                    '    CAST(date AS VARCHAR) AS date,',
                    '    CAST(product_id AS VARCHAR) AS product_id,',
                    '    CAST(product_name_latest AS VARCHAR) AS product_name_latest,',
                    '    CAST(convergence_gravity_score AS VARCHAR) AS convergence_gravity_score,',
                    '    CAST(return_gravity_score AS VARCHAR) AS return_gravity_score,',
                    '    CAST(distinct_source_product_cnt_90d AS VARCHAR) AS distinct_source_product_cnt_90d,',
                    '    CAST(return_customer_rate_90d AS VARCHAR) AS return_customer_rate_90d,',
                    '    CAST(return_loop_rate_90d AS VARCHAR) AS return_loop_rate_90d,',
                    '    CAST(simple_repeat_rate_90d AS VARCHAR) AS simple_repeat_rate_90d'
                ].join('\n'),
                fromSql: 'FROM gold_pgm_product_demand_gravity',
                whereSql: windowedDate,
                orderBySql: 'date, product_id',
                limit,
                offset
            })
        },
        {
            datasetKey: 'pgm_transition_edges',
            sourceKey: 'dma_pgm_transition_edges',
            sourceTable: 'gold_pgm_product_transition_edge',
            connectionId: DMA_CONNECTION_ID,
            columns: getRawDatasetColumns().pgm_transition_edges,
            dateFields: ['date'],
            pageSize: 500,
            buildSql: ({ limit, offset }) => buildPagedQuery({
                selectSql: [
                    'SELECT',
                    '    CAST(date AS VARCHAR) AS date,',
                    '    CAST(source_product_id AS VARCHAR) AS source_product_id,',
                    '    CAST(target_product_id AS VARCHAR) AS target_product_id,',
                    '    CAST(transition_customer_cnt AS VARCHAR) AS transition_customer_cnt,',
                    '    CAST(source_cohort_customer_cnt AS VARCHAR) AS source_cohort_customer_cnt,',
                    '    CAST(transition_rate AS VARCHAR) AS transition_rate,',
                    '    CAST(avg_days_to_transition AS VARCHAR) AS avg_days_to_transition'
                ].join('\n'),
                fromSql: 'FROM gold_pgm_product_transition_edge',
                whereSql: latestPgmDate,
                orderBySql: 'date, source_product_id, target_product_id',
                limit,
                offset
            })
        },
        {
            datasetKey: 'pgm_return_loops',
            sourceKey: 'dma_pgm_return_loops',
            sourceTable: 'gold_pgm_return_gravity_loop_detail',
            connectionId: DMA_CONNECTION_ID,
            columns: getRawDatasetColumns().pgm_return_loops,
            dateFields: ['date'],
            pageSize: 500,
            buildSql: ({ limit, offset }) => buildPagedQuery({
                selectSql: [
                    'SELECT',
                    '    CAST(date AS VARCHAR) AS date,',
                    '    CAST(customer_id AS VARCHAR) AS customer_id,',
                    '    CAST(source_product_id AS VARCHAR) AS source_product_id,',
                    '    CAST(return_product_id AS VARCHAR) AS return_product_id,',
                    '    CAST(source_order_id AS VARCHAR) AS source_order_id,',
                    '    CAST(return_order_id AS VARCHAR) AS return_order_id,',
                    '    CAST(return_days AS VARCHAR) AS return_days,',
                    '    CAST(intermediate_step_cnt AS VARCHAR) AS intermediate_step_cnt,',
                    '    CAST(intermediate_distinct_product_cnt AS VARCHAR) AS intermediate_distinct_product_cnt,',
                    '    CAST(intermediate_path AS VARCHAR) AS intermediate_path,',
                    '    CAST(qualified_return_flag AS VARCHAR) AS qualified_return_flag,',
                    '    CAST(simple_repeat_comparison_flag AS VARCHAR) AS simple_repeat_comparison_flag'
                ].join('\n'),
                fromSql: 'FROM gold_pgm_return_gravity_loop_detail',
                whereSql: latestPgmDate,
                orderBySql: 'date, source_product_id, return_product_id, customer_id',
                limit,
                offset
            })
        },
        {
            datasetKey: 'pgm_basket_pairs',
            sourceKey: 'dma_pgm_basket_pairs',
            sourceTable: 'gold_pgm_basket_gravity_detail',
            connectionId: DMA_CONNECTION_ID,
            columns: getRawDatasetColumns().pgm_basket_pairs,
            dateFields: ['date'],
            pageSize: 500,
            buildSql: ({ limit, offset }) => buildPagedQuery({
                selectSql: [
                    'SELECT',
                    '    CAST(date AS VARCHAR) AS date,',
                    '    CAST(i AS VARCHAR) AS i,',
                    '    CAST(j AS VARCHAR) AS j,',
                    '    CAST(co_order_cnt AS VARCHAR) AS co_order_cnt,',
                    '    CAST(rn AS VARCHAR) AS rn'
                ].join('\n'),
                fromSql: 'FROM gold_pgm_basket_gravity_detail',
                whereSql: latestPgmDate,
                orderBySql: 'date, i, j, rn',
                limit,
                offset
            })
        },
        {
            datasetKey: 'brand_purchase_daily',
            sourceKey: 'dma_brand_purchase_daily',
            sourceTable: 'gold_purchase_analysis_daily',
            connectionId: DMA_CONNECTION_ID,
            columns: getRawDatasetColumns().brand_purchase_daily,
            dateFields: ['date'],
            pageSize: 500,
            buildSql: ({ limit, offset }) => buildPagedQuery({
                selectSql: [
                    'SELECT',
                    '    CAST(date AS VARCHAR) AS date,',
                    '    CAST(total_purchase_amount AS VARCHAR) AS total_purchase_amount,',
                    '    CAST(new_purchase_amount AS VARCHAR) AS new_purchase_amount,',
                    '    CAST(re_purchase_amount AS VARCHAR) AS re_purchase_amount'
                ].join('\n'),
                fromSql: 'FROM gold_purchase_analysis_daily',
                whereSql: windowedDate,
                orderBySql: 'date',
                limit,
                offset
            })
        }
    ];
}

