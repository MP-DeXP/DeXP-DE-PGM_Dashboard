# Anchor Product Scoring & Transition Technical Spec

Anchor Product Scoring & Transition Technical Spec
Version: APF v1.0 (Notebook implementation)
Artifact: AnchorProduct.ipynb
Scope: 상품 역할(AA/PCA) 점수 산출 + AA→PCA 전이(Transition) 분석

1) 문제 정의 및 산출물
목표
브랜드 내 product_id를 기준으로 다음을 산출한다.
	•	AA (Acquisition Anchor) 점수
	•	신규 유입(첫 구매)을 많이 만들고, 유입 고객이 90일 동안 남긴 총 결제금액(누적 매출)까지 반영해 “유입 엔진” 상품을 식별한다.
	•	PCA (Purchase Chain Anchor) 점수
	•	상품 구매를 기점으로 “구매사슬이 열리는지(재구매 전환 + 추가 주문)”를 기준으로 “사슬 개시 엔진” 상품을 식별한다.
	•	AA → PCA 전이 테이블
	•	AA 후보 상품으로 유입된 고객이 이후 90일 안에 어떤 PCA 후보 상품을 “처음” 구매하는지(전이율, 평균 소요일)를 계산한다.
핵심 산출 DF/테이블
	•	anchor_scored (상품별 지표 + AA/PCA/Primary 점수 포함)
	•	aa_pca_transition (AA→PCA 전이 테이블)

2) 데이터 소스 및 전처리 계약
입력 데이터
	•	core.silver_meta_order (Spark SQL)
	•	필터:
	•	mx_channel_id == CHANNEL_ID
	•	order_at >= current_date - 365 (최근 1년)
주문/아이템 단위 테이블
	•	주문의 item_list, cancel_list, return_list, exchange_list, refund_list를 explode해 아이템 단위로 정규화
정제 원칙
	•	member_id IS NOT NULL
	•	취소/반품/교환/환불 아이템을 아이템 단위로 제거
	•	키: (order_id, item_id)
	•	exclude_keys = cancel_keys ∪ return_keys ∪ exchange_origin_keys ∪ refund_keys
	•	order_items_clean = base_items LEFT_ANTI exclude_keys
Candidate 상품군 제한 (정확도/비용 최적화)
	•	CANDIDATE_DAYS = 30
	•	최근 30일 주문(item_list)에서 등장한 product_id를 후보군으로 정의:
	•	candidate_product_ids_df
	•	중요: order_items_clean 단계에서 candidate_product_ids_df로 left_semi 조인하여 이후 모든 계산이 후보 상품군으로 제한됨

3) 핵심 파라미터(전역 상수)
	•	CHURN_DURATION = 90
	•	대부분의 90일 창(window)에 사용됨
	•	R90(재구매 전환), Revenue_90d, x2(addl_order_cnt) 계산의 시간 범위로 재사용됨
	•	CANDIDATE_DAYS = 30
	•	점수 산출 대상 product 후보군 생성 기간
	•	점수 가중치(현재 노트북 구현)
	•	AA_ScoreBase = 0.4 * fcr_norm + 0.6 * rev_norm
	•	PCA_ScoreBase = 0.6 * r90_norm + 0.4 * x2_norm
	•	volume_weight = minmax(log1p(first_customer_cnt))
	•	AA_Score = volume_weight^alpha_volume * AA_ScoreBase (alpha_volume=1.0)
	•	PCA_Score = volume_weight^alpha_volume * PCA_ScoreBase
	•	PrimaryAnchorScoreBase는 레거시/참고용 혼합 점수 (현재 노트북에서 유지)

4) 주요 중간 데이터 구조(스키마 요약)
order_items_raw
	•	주문 레벨에서 explode 후 필요한 필드만 유지
	•	포함:
	•	member_id, order_id, order_at
	•	order_payment_amount (order_amount_info.payment_amount)
	•	product_id (string cast)
	•	item(struct), cancel/return/exchange/refund struct들
주의(안정성): explode 후 alias 참조 문제 방지를 위해 withColumn(explode) → select(item.*) 구조 사용됨.
order_items_clean
	•	base_items에서 exclude_keys를 left_anti하여 정상 아이템만 남김
	•	candidate 상품군으로 left_semi 조인 적용
order_item_options
	•	옵션을 explode하여 option_name, option_value를 정규화
	•	현재는 간이 base_product_key = concat(product_id, option_name, option_value) 생성 (참고용)

5) Cohort 정의
Product-based Cohort
	•	단위: (member_id, product_id)
	•	cohort_order_at = 해당 고객이 해당 상품을 처음 구매한 시점
	•	cohort_order_id도 함께 기록 (후속 주문 계산 시 동일 주문 제외)
이 정의는 “고객 생애 첫 구매”가 아니라, 상품 기준 첫 구매를 허용하므로 고객은 여러 cohort에 속할 수 있다.

6) 지표 정의(수식/의도)
모든 지표는 기본적으로 cohort_order_at을 기준으로 0~90일 창에서 계산한다(일관성 유지).
6.1 first_customer_cnt, first_customer_ratio
	•	first_customer_cnt(product_id) = product_cohort에서 distinct member_id count
	•	first_customer_ratio = first_customer_cnt / total_first_customers
	•	total_first_customers = product_cohort에서 distinct member_id count
의도: 해당 상품이 “처음 구매된 비중”을 통해 유입력을 측정 (AA의 핵심 입력)

6.2 R90:
repurchase_rate_90d
	•	cohort 이후 90일 내 추가 주문(어떤 상품이든) 존재 여부
	•	계산:
	•	cohort_orders = product_cohort join member_orders
	•	days_from_cohort = datediff(order_at, cohort_order_at)
	•	order_id != cohort_order_id
	•	0 <= days_from_cohort <= 90
	•	repurchase_customer_cnt_90d(product_id) = distinct member_id count in cohort_orders
	•	cohort_customer_cnt(product_id) = distinct member_id count in product_cohort
	•	repurchase_rate_90d = repurchase_customer_cnt_90d / cohort_customer_cnt
의도: 구매사슬이 “열리는지”를 가장 단순하게 측정하는 트리거 (PCA의 핵심 입력)

6.3 Revenue_90d:
revenue_90d
	•	cohort 이후 90일 내 고객이 발생시킨 주문 전체 결제금액 합
	•	중복 방지:
	•	아이템 단위로 같은 order_payment_amount가 반복되므로
	•	member_orders_pay = (member_id, order_id, order_at) groupBy 후 max(payment_amount)
	•	revenue_orders_90d = product_cohort join member_orders_pay + 0~90일 필터
	•	revenue_90d(product_id) = sum(order_payment_amount)
의도: AA에서 “유입이 만든 총 가치(90일)”를 반영.
PCA에는 직접 넣지 않고(정의 보존), 분석/시각화에서 ‘중력’으로 활용 가능.

6.4 x2: 추가 주문 구조 (90일)
고객별로 cohort 이후 90일 내 주문 수를 세고, “추가 주문 수”로 변환한다.
	•	고객-상품 단위:
	•	order_cnt_90d = countDistinct(order_id) within 0~90d window
	•	addl_order_cnt_90d = max(order_cnt_90d - 1, 0) (cohort 주문 1회 제외)
	•	상품별 분포 요약:
	•	p50_addl_order_cnt_90d
	•	p75_addl_order_cnt_90d (PCA score input)
	•	p90_addl_order_cnt_90d
	•	addl_order_rate_90d = avg( addl_order_cnt_90d > 0 )
의도:
	•	p75는 “사슬이 열림”의 보수적 경계값 역할(0/1로 수렴할 수 있음)
	•	p90과 addl_order_rate는 PCA 구조 해석(소수 강자 vs 광범위 사슬)에 도움

6.5 retention_days (참고 지표)
	•	고객 생애 기준 retention_days(first_order_date ~ churn_date)
	•	상품별 p75_retention_days로 집계
의도: v1.0에서는 AA/PCA 핵심 입력이 아니라 장기 가치 해석/레거시 Primary용 보조 지표로 사용.

7) 점수 정의(AA / PCA / Primary)
7.1 Normalization
모든 입력을 candidate 상품군 내에서 min-max 정규화한다(분모 0 방지).
	•	r90_norm = minmax(repurchase_rate_90d)
	•	x2_norm = minmax(p75_addl_order_cnt_90d)
	•	rev_norm = minmax(revenue_90d)
	•	fcr_norm = minmax(first_customer_ratio)
	•	(참고) x3_norm = minmax(p75_retention_days)
7.2 Base Scores
	•	AA_ScoreBase   = 0.4 * fcr_norm + 0.6 * rev_norm 
	•	PCA_ScoreBase   = 0.6 * r90_norm + 0.4 * x2_norm 
7.3 Sample Size Weighting
	•	volume_raw = log1p(first_customer_cnt)
	•	volume_weight = minmax(volume_raw)
	•	최종:
	•	AA_Score = volume_weight^alpha_volume * AA_ScoreBase
	•	PCA_Score = volume_weight^alpha_volume * PCA_ScoreBase
	•	(참고) PrimaryAnchorScore = volume_weight^alpha_volume * PrimaryAnchorScoreBase
의도: 소표본 과대평가를 완화.
필요시 MIN_FIRST_CUSTOMERS 하드컷을 사용할 수 있으나, 기본은 가중치 방식.

8) AA → PCA 전이(Transition) 정의
후보 집합
	•	aa_candidates: AA_Score 상위 N개 (현재 TOP_AA=10)
	•	pca_candidates: PCA_Score 상위 N개 (현재 TOP_PCA=30)
전이 이벤트 정의
	•	기준점: aa_cohort_order_at (AA 후보 상품의 cohort 시점)
	•	기간: 0~90일
	•	대상 이벤트: PCA 후보 상품 구매 이벤트
	•	“전이 대상” 선택:
	•	고객-AA상품 단위로 가장 먼저 발생한 PCA 구매 이벤트를 선택 (row_number by order_at, order_id)
전이 집계
	•	분모: aa_cohort_customer_cnt (AA cohort 고객수)
	•	분자: transition_customer_cnt (AA→PCA 최초 도달 고객수)
	•	전이율: transition_rate = transition_customer_cnt / aa_cohort_customer_cnt
	•	속도: avg_days_to_pca

9) 운영/성능 고려사항 (실제 장애 경험 기반)
9.1 SparkContext shutdown / stage materialization 오류
	•	전이 분석은 조인 + 윈도우 + 집계로 DAG가 무거움
	•	toPandas()는 작은 limit이라도 full DAG를 실행하므로 드라이버/클러스터 불안정 유발 가능
권장:
	•	결과 확인은 우선 show()로 제한
	•	중간 DF는 필요시 persist(MEMORY_AND_DISK) + count()로 물리화
	•	후보군(AA/PCA TOP_N)을 작게 유지하고, broadcast 조인을 적극 사용
9.2 candidate 필터링 위치
	•	order_items_clean 단계에서 후보군으로 제한하는 방식이 비용/정확도 모두 유리
	•	후보군 제한이 누락되면 이후 모든 집계 비용이 폭발함

10) 해석 가이드(개발/DS 관점)
AA 해석
	•	높은 AA는 “유입 규모 + 90일 가치”의 결합
	•	무료체험/프로모션 상품이 올라오는 것은 정상
	•	AA의 진짜 가치는 **전이 구조(AA→PCA)**에서 평가 가능
PCA 해석
	•	높은 PCA는 “사슬 개시 신호”(R90 + x2)
	•	p75가 0/1에 수렴하는 것은 브랜드 구조적 특성일 수 있음
	•	p90_addl_order_cnt_90d와 addl_order_rate_90d로 구조 해석(소수 강자 vs 광범위) 가능
	•	revenue는 PCA 산식에 넣지 않고, **시각화에서 원 크기(size)**로 취급 권장

11) 개선 로드맵(코드 변경 없이 가능한 순서)
단기(해석/가시화)
	•	AA vs PCA scatter plot + bubble size = revenue_90d
	•	PCA 상위 상품의 “x2 분포(p50/p75/p90) + addl_order_rate” 해석 템플릿
중기(정제)
	•	promo/사은품/회원전용 상품 태그(이름 패턴 기반) → PCA 후보에서 제외/감점 (정의가 아니라 후보 제한 레이어로)
장기(확장)
	•	base_product(기초상품) 레이어를 “해석/번들/CRM 액션 단위”로 추가 (Anchor 단위는 product_id 유지)

부록: 수식 요약
	•	AA_ScoreBase = 0.4minmax(first_customer_ratio) + 0.6minmax(revenue_90d)
	•	PCA_ScoreBase = 0.6minmax(repurchase_rate_90d) + 0.4minmax(p75_addl_order_cnt_90d)
	•	volume_weight = minmax(log1p(first_customer_cnt))
	•	AA_Score = volume_weight * AA_ScoreBase
	•	PCA_Score = volume_weight * PCA_ScoreBase
 
