# Demand Graph Insight Output v0.1

Demand Graph Insight Output v0.1  
`04_PGM_Insight_CSV_Generator.ipynb`에서 생성하는 UI용 Demand Graph database 파생 산출물 정의

## 1. 목적

이 산출물은 PGM core score를 다시 계산하기 위한 것이 아니다.  
목적은 이미 계산된 공식 산출물을 UI/insight 계층에서 재가공해
프론트엔드가 바로 사용할 수 있는 `nodes / edges / patterns` 형태의 CSV를 제공하는 것이다.

즉 `04_PGM_Insight_CSV_Generator.ipynb`는:

- core metric notebook이 아니다
- presentation / interaction layer를 위한 파생 산출 단계다
- Demand Graph UI가 프론트엔드에서 무거운 join / pruning / pattern 가공을 하지 않도록 돕는다

## 2. 입력 의존성

Demand Graph insight output은 아래 공식 산출물을 입력으로 사용한다.

- `pgm_product_transition_edge.csv`
- `pgm_product_demand_gravity.csv`
- `pgm_basket_gravity_detail.csv`
- `pgm_scored.csv`

원칙:

- core score 산식은 변경하지 않는다
- `04`는 입력 CSV를 읽어 UI용으로만 재조합한다

## 3. 출력 파일

`04_PGM_Insight_CSV_Generator.ipynb`는 아래 3개 Demand Graph CSV를 생성한다.

- `_insight_demand_graph_nodes.csv`
- `_insight_demand_graph_edges.csv`
- `_insight_demand_graph_patterns.csv`

### 3.1 `_insight_demand_graph_nodes.csv`

역할:

- 그래프 노드 메타데이터
- 노드 라벨/크기/주요 role 결정용 테이블

기준 유니버스:

- `pgm_product_demand_gravity.csv`의 전체 `product_id`

주요 컬럼:

- `product_id`
- `product_name_latest`
- `entry_score`
- `expansion_score`
- `convergence_score`
- `return_score`
- `entry_type`
- `expansion_type`
- `distinct_source_product_cnt_90d`
- `incoming_transition_rate_sum_90d`
- `return_customer_rate_90d`
- `return_loop_rate_90d`
- `node_role_primary`
- `node_size_score`

규칙:

- `product_name_latest`는 `pgm_scored.csv`를 우선 참조해 보강한다
- 이름 누락은 허용하되, product row는 드롭하지 않는다
- `node_role_primary`는 네 gravity score 중 최대값의 축이다
- 동률 우선순위는 `Entry > Expansion > Convergence > Return`
- `node_size_score`는 네 score의 최대값이다

### 3.2 `_insight_demand_graph_edges.csv`

역할:

- UI가 바로 그릴 curated transition edge 목록

입력:

- `pgm_product_transition_edge.csv`

출력 컬럼:

- `source_product_id`
- `target_product_id`
- `source_product_name`
- `target_product_name`
- `transition_customer_cnt`
- `source_cohort_customer_cnt`
- `transition_rate`
- `avg_days_to_transition`
- `edge_rank_outgoing`
- `edge_rank_incoming`
- `edge_type`
- `is_core_edge`

규칙:

- self-loop (`source_product_id == target_product_id`)는 제외한다
- `edge_type = transition`
- 이 파일은 curated edge만 저장하므로 `is_core_edge = 1`

curation 규칙:

- source 기준 `transition_rate desc`, `transition_customer_cnt desc`, `target_product_id asc` 정렬 상위 5개
- target 기준 `transition_customer_cnt desc`, `transition_rate desc`, `source_product_id asc` 정렬 상위 5개
- 또는 `transition_rate >= 0.03`
- 또는 `transition_customer_cnt >= 3`

즉 최종 edge는 위 조건들의 합집합이다.

### 3.3 `_insight_demand_graph_patterns.csv`

역할:

- Demand Graph UI가 함께 사용할 빈발패턴 테이블

출력 컬럼:

- `pattern_id`
- `pattern_type`
- `anchor_product_id`
- `related_product_ids`
- `product_path`
- `support_value`
- `support_unit`
- `confidence_value`
- `rank_within_type`
- `window_days`

`pattern_type` 종류:

- `transition_path`
- `basket_pair`

#### `transition_path`

입력:

- `pgm_product_transition_edge.csv`의 non-self curated edge

규칙:

- `product_path = source_product_id > target_product_id`
- `support_value = transition_customer_cnt`
- `support_unit = customer`
- `confidence_value = transition_rate`
- `window_days = 90`

#### `basket_pair`

입력:

- `pgm_basket_gravity_detail.csv`

규칙:

- `i != j`만 사용한다
- `(i, j)`와 `(j, i)`는 canonical pair 하나로 합친다
- canonical pair는 `product_a = min(i, j)`, `product_b = max(i, j)`다
- 최소 기준은 `co_order_cnt >= 3`
- `product_path = product_a | product_b`
- `support_value = co_order_cnt`
- `support_unit = order`
- `confidence_value`는 비운다
- `window_days`는 비운다

## 4. Self-loop와 raw edge에 대한 원칙

- `pgm_product_transition_edge.csv`의 self-loop는 Convergence 진단용 raw signal로는 의미가 있다
- 그러나 Demand Graph UI edge 목록에서는 제외한다
- self-loop 관련 진단 값은 `pgm_product_demand_gravity.csv`를 통해 node 메타에서만 간접적으로 사용한다

또한:

- `_insight_demand_graph_edges.csv`는 raw edge 전체를 복제하지 않는다
- UI에 적합한 curated edge만 저장한다

## 5. Sanity 규칙

`04_PGM_Insight_CSV_Generator.ipynb`는 아래 기본 sanity를 수행해야 한다.

- `demand_graph_nodes`의 `product_id` 수는 `pgm_product_demand_gravity.csv`의 전체 `product_id` 수와 같아야 한다
- `demand_graph_edges`의 source/target은 모두 `demand_graph_nodes`에 존재해야 한다
- `demand_graph_edges`에 self-loop가 있으면 안 된다
- `demand_graph_patterns`의 `anchor_product_id`, `related_product_ids`는 모두 `demand_graph_nodes`에 존재해야 한다
- 출력 파일 3종이 실제로 생성되어야 한다

## 6. 구현 레이어 원칙

Demand Graph insight output은 아래 레이어 원칙을 따른다.

- `01`: Entry / Expansion / Basket 계산
- `02`: Convergence / Return 및 공식 transition edge 계산
- `03`: Brand Score / BII 계산
- `04`: UI/insight layer CSV 생성

따라서 Demand Graph CSV는 `02`에 넣지 않고 `04`에 둔다.
