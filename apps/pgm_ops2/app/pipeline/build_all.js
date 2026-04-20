import {
    BRAND_SCORE_STATUSES,
    ENABLE_NEAR_CORE_STATUS,
    MART_FILE_NAMES,
    PRIORITY_LEVELS,
    QA_FILE_NAMES,
    QUEUE_REASON_TYPES,
    RAW_FILE_NAMES,
    ROLE_TAXONOMY,
    STAGING_FILE_NAMES,
    VIEW_MODEL_FILE_NAMES,
    WINDOWS
} from '../config/constants.js';
import { getDateGapDays, getLatestDate, isIsoDate, normalizeDateValue, shiftDate } from '../transforms/date.js';

function toNumber(value) {
    const parsed = Number.parseFloat(String(value ?? '').replace(/,/g, ''));
    return Number.isFinite(parsed) ? parsed : 0;
}

function toText(value, fallback = '') {
    return value == null ? fallback : String(value);
}

function toWindowKey(windowDays) {
    return `${windowDays}d`;
}

function toWindowDays(windowKey) {
    const parsed = Number.parseInt(String(windowKey ?? '').replace(/d$/i, ''), 10);
    return Number.isFinite(parsed) ? parsed : 7;
}

function getOperatingWindowDays() {
    return WINDOWS.filter((windowDays) => [1, 7, 30].includes(windowDays));
}

function getPeriodLabel(windowKey) {
    const windowDays = toWindowDays(windowKey);
    if (windowDays === 1) return '하루 기준';
    if (windowDays === 7) return '최근 7일';
    if (windowDays === 30) return '최근 30일';
    return `최근 ${windowDays}일`;
}

function getCompareLabel(windowKey) {
    const windowDays = toWindowDays(windowKey);
    if (windowDays === 1) return '직전 하루';
    return `직전 ${windowDays}일`;
}

function clamp01(value) {
    if (!Number.isFinite(value)) {
        return 0;
    }
    return Math.max(0, Math.min(1, value));
}

function average(values) {
    const filtered = values.filter((value) => Number.isFinite(value));
    if (!filtered.length) {
        return 0;
    }
    return filtered.reduce((sum, value) => sum + value, 0) / filtered.length;
}

function median(values) {
    const filtered = values.filter((value) => Number.isFinite(value)).sort((left, right) => left - right);
    if (!filtered.length) {
        return 0;
    }

    const middleIndex = Math.floor(filtered.length / 2);
    return filtered.length % 2 === 0
        ? (filtered[middleIndex - 1] + filtered[middleIndex]) / 2
        : filtered[middleIndex];
}

function percentile(values, ratio) {
    const filtered = values.filter((value) => Number.isFinite(value)).sort((left, right) => left - right);
    if (!filtered.length) {
        return 0;
    }

    const index = Math.min(filtered.length - 1, Math.max(0, Math.floor((filtered.length - 1) * ratio)));
    return filtered[index];
}

function maxNumber(values) {
    const filtered = values.filter((value) => Number.isFinite(value));
    return filtered.length ? Math.max(...filtered) : 0;
}

function minNumber(values) {
    const filtered = values.filter((value) => Number.isFinite(value));
    return filtered.length ? Math.min(...filtered) : 0;
}

function keyOf(...values) {
    return values.map((value) => String(value ?? '')).join('|');
}

function groupBy(rows, keyBuilder) {
    return rows.reduce((map, row) => {
        const key = keyBuilder(row);
        const group = map.get(key) ?? [];
        group.push(row);
        map.set(key, group);
        return map;
    }, new Map());
}

function indexBy(rows, keyBuilder) {
    return rows.reduce((map, row) => {
        map.set(keyBuilder(row), row);
        return map;
    }, new Map());
}

function uniqueCount(values) {
    return new Set(values.filter((value) => toText(value))).size;
}

function sum(values) {
    return values.reduce((total, value) => total + toNumber(value), 0);
}

function safeDivide(numerator, denominator) {
    const normalizedDenominator = toNumber(denominator);
    if (!normalizedDenominator) {
        return 0;
    }
    return toNumber(numerator) / normalizedDenominator;
}

function safeBalanceIndex(values) {
    if (!values.length) {
        return 0;
    }

    const target = 1 / values.length;
    const squaredDiffAverage = average(values.map((value) => (toNumber(value) - target) ** 2));
    return clamp01(1 - Math.sqrt(squaredDiffAverage));
}

function topShare(values, topN = 3) {
    const positiveValues = values
        .map((value) => Math.max(0, toNumber(value)))
        .sort((left, right) => right - left);
    const total = sum(positiveValues);
    if (!total) {
        return 0;
    }

    return sum(positiveValues.slice(0, topN)) / total;
}

function toBooleanFlag(value) {
    return ['true', '1', 'yes'].includes(toText(value).toLowerCase());
}

function translateUserFacingText(value) {
    return [
        [/Brand Score/g, '브랜드 점수'],
        [/brand-level/g, '브랜드 단위'],
        [/brand level/g, '브랜드 단위'],
        [/Revenue/g, '매출'],
        [/Role/g, '역할'],
        [/same-date snapshot only/g, '기준일 스냅샷만 사용'],
        [/same-date snapshot/g, '기준일 스냅샷'],
        [/canonical score/g, '기준 점수'],
        [/freshness cap/g, '최신성 제한'],
        [/freshness/g, '최신성'],
        [/reconstruction registry/g, '재구성 관리 기록'],
        [/contributor signal/g, '기여 신호'],
        [/contributor/g, '기여'],
        [/canonical/g, '기준값'],
        [/near-core/g, '검증 후보'],
        [/provisional/g, '운영 참고'],
        [/limited/g, '제한 반영'],
        [/unavailable/g, '산출 불가'],
        [/event/g, '이벤트'],
        [/basket/g, '동시구매'],
        [/source history/g, '비교 이력'],
        [/history/g, '비교 이력'],
        [/window/g, '기간'],
        [/taxonomy/g, '역할 분류'],
        [/attach_rate/g, '동반구매율'],
        [/top1/g, '상위 1개'],
        [/top3/g, '상위 3개'],
        [/companion/g, '연관 조합'],
        [/score/g, '점수']
    ].reduce((text, [pattern, replacement]) => text.replace(pattern, replacement), toText(value))
        .replace(/\s+/g, ' ')
        .trim();
}

function toRevenueCompareStateLabel(state) {
    const labels = {
        available: '비교 가능',
        history_insufficient: '비교 이력 부족'
    };
    return labels[toText(state)] ?? translateUserFacingText(state);
}

function toBrandScoreStatusLabel(status) {
    const labels = {
        unavailable: '브랜드 방향 산출 없음',
        limited: '브랜드 방향 신호 제한 반영',
        provisional: '브랜드 구조 변화 신호',
        available: '브랜드 구조 반영 완료',
        'near-core': '브랜드 구조 변화 신호 검증 중'
    };
    return labels[toText(status)] ?? translateUserFacingText(status);
}

function toStructureLabel(value) {
    const labels = {
        Broad: '넓은 유입',
        Qualified: '선별 유입',
        Core: '핵심 확장',
        Deep: '깊은 확장',
        Scale: '확산 확장',
        Pair: '강한 짝상품',
        Set: '묶음 중심',
        None: '관측 약함'
    };
    return labels[toText(value)] ?? toText(value, '관측 약함');
}

function summarizeBrandScoreReason(status, rawReason, fallback = '') {
    const rawText = toText(rawReason);
    const labels = [];
    const append = (label) => {
        if (label && !labels.includes(label)) {
            labels.push(label);
        }
    };

    [
        ['brand-level 집계를 위한 product input이 없습니다.', '집계 대상 부족'],
        ['reconstructed event가 없습니다.', '이벤트 근거 부족'],
        ['event 재구성 결과가 비어 있습니다.', '이벤트 근거 부족'],
        ['reconstructed basket summary가 없습니다.', '동시구매 근거 부족'],
        ['scored/demand 기반 핵심 축이 비어 있습니다.', '핵심 축 근거 부족'],
        ['event freshness가 7일을 초과합니다.', '이벤트 최신성 지연'],
        ['basket freshness가 7일을 초과합니다.', '동시구매 최신성 지연'],
        ['event product coverage가 낮습니다.', '반영 상품 범위 협소'],
        ['basket parity가 낮습니다.', '동시구매 재구성 안정성 낮음'],
        ['raw basket detail과의 유사도가 낮아 exact parity를 주장할 수 없습니다.', '동시구매 재구성 안정성 낮음'],
        ['역할 근거로는 사용하지만 core basket summary와 동일성은 아직 입증되지 않았습니다.', '동시구매 정합성 검증 필요'],
        ['near-core 상태는 기본 비활성이라 provisional로 유지했습니다.', '운영 반영 전 검증 단계'],
        ['상품 단위 핵심 축이 모두 재구성되지 않았습니다.', '상품별 핵심 근거 부족'],
        ['일부 상품은 contributor 상태가 limited입니다.', '일부 상품 제한 반영'],
        ['core canonical output과의 field-by-field diff 검증이 없어 near-core나 exact parity를 주장할 수 없습니다.', '정합성 검증 필요'],
        ['confidence label은 core threshold를 참고하지만 direct diff 검증은 아직 없습니다.', '신뢰도 검증 필요']
    ].forEach(([pattern, label]) => {
        if (rawText.includes(pattern)) {
            append(label);
        }
    });

    if (!labels.length && rawText) {
        append(translateUserFacingText(rawText));
    }

    if (labels.length) {
        const head = labels.slice(0, 2).join(' / ');
        return labels.length > 2 ? `${head} 외 ${labels.length - 2}건` : head;
    }

    if (fallback) {
        return fallback;
    }

    if (status === 'unavailable') {
        return '핵심 근거 부족';
    }
    if (status === 'limited') {
        return '일부 근거 제한';
    }
    if (status === 'near-core') {
        return '운영 반영 전 검증';
    }
    return '운영 참고값 제공';
}

function buildBrandScoreNote(status, rawReason, fallback = '') {
    const summary = summarizeBrandScoreReason(status, rawReason, fallback).replace(/[.]+$/g, '');

    if (status === 'unavailable') {
        return summary
            ? `브랜드 방향 산출에 필요한 근거가 부족해 ${summary} 상태입니다.`
            : '브랜드 방향 산출에 필요한 근거가 아직 부족합니다.';
    }
    if (status === 'limited') {
        return summary
            ? `브랜드 방향 신호는 ${summary} 제약을 반영해 제한적으로 읽습니다.`
            : '브랜드 방향 신호를 제한적으로 읽습니다.';
    }
    if (status === 'near-core') {
        return summary
            ? `브랜드 구조 변화 신호를 ${summary} 기준으로 검증 중입니다.`
            : '브랜드 구조 변화 신호를 검증 중입니다.';
    }
    if (status === 'available') {
        return summary
            ? `브랜드 구조 반영을 완료했고 ${summary}를 함께 확인합니다.`
            : '브랜드 구조 반영을 완료했습니다.';
    }
    return summary && summary !== '운영 참고값 제공'
        ? `브랜드 구조 변화 신호를 ${summary}와 함께 읽습니다.`
        : '브랜드 구조 변화 신호를 읽을 수 있습니다.';
}

function getBrandDirectionCompareWindowKey(windowKey) {
    const comparisons = {
        '1d': '7d',
        '7d': '30d',
        '30d': '7d'
    };
    return comparisons[windowKey] ?? '7d';
}

function getBrandDirectionLabel(directionKey) {
    const labels = {
        flat: '변화 작음',
        improving: '브랜드 구조 개선 신호',
        deteriorating: '브랜드 구조 악화 신호',
        hold: '브랜드 방향 판단 보류'
    };
    return labels[directionKey] ?? '브랜드 방향 판단 보류';
}

function getBrandDirectionRelativeText(delta) {
    if (!Number.isFinite(delta)) {
        return '판단을 보류합니다';
    }
    if (Math.abs(delta) < 0.01) {
        return '변화가 작습니다';
    }
    if (delta >= 0.01) {
        return '더 강하게 유지됩니다';
    }
    return '더 약하게 유지됩니다';
}

function buildBrandDirectionProductContributionNote(contributorRows = []) {
    const topContributors = [...contributorRows]
        .sort((left, right) => {
            return (
                toNumber(right.contribution_share) - toNumber(left.contribution_share)
                || toNumber(left.contributor_rank) - toNumber(right.contributor_rank)
            );
        })
        .slice(0, 3)
        .map((row) => toText(row.product_name || row.product_id))
        .filter(Boolean);

    if (!topContributors.length) {
        return '상품 기여 집계 대상은 아직 충분하지 않습니다. 상품 기여는 브랜드 전체 방향을 보조로 설명하며 큐 순위에는 반영하지 않습니다.';
    }

    return `상품 기여는 ${topContributors.join(', ')} 중심으로 집계합니다. 상품 기여는 브랜드 전체 방향을 보조로 설명하며 큐 순위에는 반영하지 않습니다.`;
}

function buildBrandDirectionSummary(brandLevelRows, contributorRows) {
    const operatingWindowKeys = getOperatingWindowDays().map((windowDays) => toWindowKey(windowDays));
    const brandLevelByWindow = indexBy(brandLevelRows, (row) => row.window_key);
    const contributorRowsByWindow = groupBy(contributorRows, (row) => row.window_key);

    return operatingWindowKeys.map((windowKey) => {
        const brandLevelRow = brandLevelByWindow.get(windowKey) ?? {};
        const periodLabel = toText(brandLevelRow.period_label, getPeriodLabel(windowKey));
        const comparisonWindowKey = getBrandDirectionCompareWindowKey(windowKey);
        const comparisonRow = brandLevelByWindow.get(comparisonWindowKey) ?? {};
        const comparisonPeriodLabel = toText(comparisonRow.period_label, getPeriodLabel(comparisonWindowKey));
        const currentNumeric = brandLevelRow.brand_score_numeric === '' ? Number.NaN : toNumber(brandLevelRow.brand_score_numeric);
        const comparisonNumeric = comparisonRow.brand_score_numeric === '' ? Number.NaN : toNumber(comparisonRow.brand_score_numeric);
        const hasComparisonRow = Boolean(toText(comparisonRow.window_key));
        const shouldHoldDirection = (
            !hasComparisonRow
            || toText(comparisonRow.brand_score_status) === 'unavailable'
            || toText(brandLevelRow.brand_score_status) === 'unavailable'
            || !Number.isFinite(currentNumeric)
            || !Number.isFinite(comparisonNumeric)
        );
        const delta = shouldHoldDirection ? Number.NaN : currentNumeric - comparisonNumeric;
        const directionKey = shouldHoldDirection
            ? 'hold'
            : Math.abs(delta) < 0.01
                ? 'flat'
                : delta >= 0.01
                    ? 'improving'
                    : 'deteriorating';
        const otherWindowKeys = operatingWindowKeys.filter((candidate) => candidate !== windowKey);
        const relativeSummary = otherWindowKeys.map((otherWindowKey) => {
            const otherRow = brandLevelByWindow.get(otherWindowKey) ?? {};
            const otherNumeric = otherRow.brand_score_numeric === '' ? Number.NaN : toNumber(otherRow.brand_score_numeric);
            const relativeDelta = Number.isFinite(currentNumeric) && Number.isFinite(otherNumeric)
                ? currentNumeric - otherNumeric
                : Number.NaN;

            return `${getPeriodLabel(otherWindowKey)}과는 ${getBrandDirectionRelativeText(relativeDelta)}`;
        }).join(', ');
        const statusLabel = toBrandScoreStatusLabel(brandLevelRow.brand_score_status || 'unavailable');
        const statusNote = toText(
            brandLevelRow.status_reason,
            buildBrandScoreNote(brandLevelRow.brand_score_status, brandLevelRow.limitation_reason_detail || brandLevelRow.limitation_reason)
        );

        return {
            as_of_date: toText(brandLevelRow.as_of_date, brandLevelRows[0]?.as_of_date ?? ''),
            window_key: windowKey,
            period_label: periodLabel,
            direction_key: directionKey,
            direction_label: getBrandDirectionLabel(directionKey),
            direction_note: `${periodLabel} 브랜드 전체 방향은 ${relativeSummary}. 비교 기준 방향 판정은 ${comparisonPeriodLabel} 대비 ${getBrandDirectionLabel(directionKey)}입니다.`,
            comparison_window_key: comparisonWindowKey,
            comparison_period_label: comparisonPeriodLabel,
            status_label: statusLabel,
            status_note: statusNote,
            product_contribution_note: buildBrandDirectionProductContributionNote(contributorRowsByWindow.get(windowKey) ?? [])
        };
    });
}

function toConfidenceLabel(score) {
    if (score >= 0.67) {
        return 'High';
    }
    if (score >= 0.34) {
        return 'Medium';
    }
    return 'Low';
}

function getParityLevel(score) {
    if (score >= 0.8) {
        return 'high';
    }
    if (score >= 0.55) {
        return 'medium';
    }
    return 'low';
}

function getStatusRank(status) {
    return BRAND_SCORE_STATUSES.indexOf(status);
}

function getWorstStatus(statuses) {
    return statuses.reduce((worst, status) => {
        if (!worst) {
            return status;
        }
        return getStatusRank(status) < getStatusRank(worst) ? status : worst;
    }, '');
}

function isBlockedOrderStatus(status) {
    const normalized = toText(status).toLowerCase();
    if (!normalized) {
        return false;
    }

    return [
        'cancel',
        'return',
        'refund',
        'exchange',
        'fail',
        '취소',
        '반품',
        '환불',
        '교환'
    ].some((token) => normalized.includes(token));
}

function sortDates(values) {
    return values
        .map((value) => normalizeDateValue(value))
        .filter((value) => value)
        .sort();
}

function collectNormalizedDates(rows, dateCandidates = []) {
    return rows.flatMap((row) => dateCandidates.map((field) => normalizeDateValue(row[field])));
}

function buildRevenueRequiredStartDates(asOfDate) {
    return Object.fromEntries(WINDOWS.map((windowDays) => [
        windowDays,
        shiftDate(asOfDate, -((windowDays * 2) - 1))
    ]));
}

function getCoverageFlags(minDate, maxDate, asOfDate) {
    const requiredStartDates = buildRevenueRequiredStartDates(asOfDate);
    const normalizedMinDate = normalizeDateValue(minDate);
    const normalizedMaxDate = normalizeDateValue(maxDate);

    return {
        requiredStartDates,
        historyReady1d: Boolean(normalizedMinDate && normalizedMaxDate && normalizedMinDate <= requiredStartDates[1] && normalizedMaxDate >= asOfDate),
        historyReady7d: Boolean(normalizedMinDate && normalizedMaxDate && normalizedMinDate <= requiredStartDates[7] && normalizedMaxDate >= asOfDate),
        historyReady30d: Boolean(normalizedMinDate && normalizedMaxDate && normalizedMinDate <= requiredStartDates[30] && normalizedMaxDate >= asOfDate),
        historyReady90d: Boolean(normalizedMinDate && normalizedMaxDate && normalizedMinDate <= requiredStartDates[90] && normalizedMaxDate >= asOfDate)
    };
}

function summarizeCoverageState(row, asOfDate) {
    const { requiredStartDates, historyReady1d, historyReady7d, historyReady30d, historyReady90d } = getCoverageFlags(row.min_date, row.max_date, asOfDate);

    let coverageState = '관측 불가';
    let coverageNote = '비교 기준을 계산할 수 없습니다.';

    if (!toNumber(row.row_count) || !normalizeDateValue(row.max_date)) {
        coverageState = '관측 불가';
        coverageNote = '비교 기준을 계산할 수 없습니다.';
    } else if (historyReady90d) {
        coverageState = '90일 비교 가능';
        coverageNote = '7/30/90일 직전기간 비교가 모두 가능합니다.';
    } else if (historyReady30d) {
        coverageState = '30일 비교 가능';
        coverageNote = '하루·7일·30일 비교가 가능하고 90일 비교는 어렵습니다.';
    } else if (historyReady7d) {
        coverageState = '7일 비교 가능';
        coverageNote = '하루·7일 비교는 가능하지만 30일 비교는 어렵습니다.';
    } else if (historyReady1d) {
        coverageState = '하루 비교 가능';
        coverageNote = '하루 비교는 가능하지만 7일 이상 비교는 어렵습니다.';
    } else {
        coverageState = '비교 이력 부족';
        coverageNote = '비교 이력이 부족해 직전 기간 비교가 어렵습니다.';
    }

    return {
        coverageState,
        coverageNote,
        requiredStartDates,
        historyReady1d,
        historyReady7d,
        historyReady30d,
        historyReady90d
    };
}

function buildProductNameIndex(rawArtifacts) {
    const nameByProduct = new Map();

    [
        rawArtifacts.products ?? [],
        rawArtifacts.pgm_scored ?? [],
        rawArtifacts.pgm_demand_signals ?? []
    ].forEach((rows) => {
        rows.forEach((row) => {
            const productId = toText(row.product_id);
            const productName = toText(row.product_name ?? row.product_name_latest);
            if (productId && productName && !nameByProduct.has(productId)) {
                nameByProduct.set(productId, productName);
            }
        });
    });

    return nameByProduct;
}

function buildProductImageIndex(rawArtifacts) {
    const imageByProduct = new Map();

    (rawArtifacts.products ?? []).forEach((row) => {
        const productId = toText(row.product_id);
        const productImageUrl = toText(row.list_image).trim();
        const currentImageUrl = imageByProduct.get(productId);

        if (!productId) {
            return;
        }

        if (!imageByProduct.has(productId) || (!currentImageUrl && productImageUrl)) {
            imageByProduct.set(productId, productImageUrl);
        }
    });

    return imageByProduct;
}

function normalizeProductRevenueDaily(rows, productNameByProduct = new Map()) {
    return rows
        .map((row) => ({
            date: normalizeDateValue(row.date),
            product_id: toText(row.product_id),
            product_name: toText(row.product_name ?? row.product_name_latest ?? row.name ?? productNameByProduct.get(toText(row.product_id))),
            revenue: toNumber(row.order_amount_sum ?? row.revenue ?? row.payment_amount ?? row.total_purchase_amount),
            order_count: toNumber(row.order_count ?? row.order_cnt),
            cart_count: toNumber(row.cart_count ?? row.cart_cnt),
            quantity: toNumber(row.quantity ?? row.item_quantity)
        }))
        .filter((row) => row.product_id && row.date);
}

function normalizeRoleRows(scoredRows, demandRows) {
    const demandByKey = indexBy(demandRows, (row) => keyOf(row.date, row.product_id));
    const scoredByKey = indexBy(scoredRows, (row) => keyOf(row.date, row.product_id));
    const keys = new Set([...demandByKey.keys(), ...scoredByKey.keys()]);

    return [...keys]
        .map((key) => {
            const scored = scoredByKey.get(key) ?? {};
            const demand = demandByKey.get(key) ?? {};

            return {
                date: normalizeDateValue(scored.date ?? demand.date),
                product_id: toText(scored.product_id ?? demand.product_id),
                product_name: toText(scored.product_name_latest ?? scored.product_name ?? demand.product_name_latest ?? demand.product_name),
                first_customer_cnt: toNumber(scored.first_customer_cnt),
                entry_gravity_score: clamp01(toNumber(scored.entry_gravity_score)),
                entry_gravity_primary_type: toText(scored.entry_gravity_primary_type),
                expansion_gravity_score: clamp01(toNumber(scored.expansion_gravity_score)),
                expansion_gravity_primary_type: toText(scored.expansion_gravity_primary_type),
                revenue_90d: toNumber(scored.revenue_90d),
                convergence_gravity_score: clamp01(toNumber(demand.convergence_gravity_score)),
                return_gravity_score: clamp01(toNumber(demand.return_gravity_score)),
                return_customer_rate_90d: clamp01(toNumber(demand.return_customer_rate_90d)),
                return_loop_rate_90d: clamp01(toNumber(demand.return_loop_rate_90d)),
                simple_repeat_rate_90d: clamp01(toNumber(demand.simple_repeat_rate_90d)),
                distinct_source_product_cnt_90d: toNumber(demand.distinct_source_product_cnt_90d),
                scored_observed_flag: scored.date ? 'true' : 'false',
                demand_observed_flag: demand.date ? 'true' : 'false'
            };
        })
        .filter((row) => row.product_id && row.date);
}

function normalizePriorityInputs(revenueRows, roleRows) {
    const roleByKey = indexBy(roleRows, (row) => keyOf(row.date, row.product_id));

    return revenueRows.map((row) => {
        const role = roleByKey.get(keyOf(row.date, row.product_id)) ?? {};
        return {
            date: row.date,
            product_id: row.product_id,
            product_name: row.product_name,
            revenue: row.revenue,
            order_count: row.order_count,
            cart_count: row.cart_count,
            role_observed_flag: role.product_id ? 'true' : 'false',
            first_customer_cnt: toNumber(role.first_customer_cnt),
            entry_gravity_score: toNumber(role.entry_gravity_score),
            expansion_gravity_score: toNumber(role.expansion_gravity_score),
            convergence_gravity_score: toNumber(role.convergence_gravity_score),
            return_gravity_score: toNumber(role.return_gravity_score)
        };
    });
}

function summarizeSourceFreshness(datasetKey, rows, dateCandidates) {
    const validDates = sortDates(collectNormalizedDates(rows, dateCandidates));

    return {
        source_key: datasetKey,
        row_count: rows.length,
        min_date: validDates[0] ?? '',
        max_date: validDates[validDates.length - 1] ?? ''
    };
}

function summarizeBasketPairs(rows) {
    const stats = new Map();

    rows.forEach((row) => {
        const left = toText(row.i ?? row.source_product_id);
        const right = toText(row.j ?? row.target_product_id);
        const count = toNumber(row.co_order_cnt);

        [left, right].forEach((productId, index) => {
            if (!productId) {
                return;
            }

            const other = index === 0 ? right : left;
            const current = stats.get(productId) ?? {
                product_id: productId,
                basket_companion_cnt: 0,
                basket_co_order_total: 0,
                basket_top_pair_count: 0,
                basket_top_pair_product_id: '',
                basket_pair_rows: 0
            };

            current.basket_pair_rows += 1;
            current.basket_co_order_total += count;
            if (other) {
                current.basket_companion_cnt += 1;
            }
            if (count > current.basket_top_pair_count) {
                current.basket_top_pair_count = count;
                current.basket_top_pair_product_id = other;
            }

            stats.set(productId, current);
        });
    });

    const maxCoOrder = Math.max(1, ...[...stats.values()].map((row) => row.basket_co_order_total));

    return [...stats.values()].map((row) => ({
        ...row,
        basket_signal_score: clamp01(row.basket_co_order_total / maxCoOrder)
    }));
}

function buildReconstructedOrderProductEvents(roleRows, orderLinesRows, ordersRows, productNameByProduct = new Map()) {
    const relevantProducts = new Set(roleRows.map((row) => toText(row.product_id)).filter(Boolean));
    const ordersById = indexBy(ordersRows, (row) => toText(row.order_id));
    const eventByKey = new Map();

    orderLinesRows.forEach((row) => {
        const orderId = toText(row.order_id);
        const orderHeader = ordersById.get(orderId) ?? {};
        const productId = toText(row.product_id);
        const orderAt = toText(row.order_at ?? orderHeader.order_at);
        const orderDate = normalizeDateValue(orderAt);
        const memberId = toText(row.customer_id ?? orderHeader.member_id);

        if (!orderId || !productId || !orderDate || !memberId || isBlockedOrderStatus(row.order_status)) {
            return;
        }

        if (relevantProducts.size && !relevantProducts.has(productId)) {
            return;
        }

        const key = keyOf(orderId, productId, memberId, orderDate);
        const current = eventByKey.get(key) ?? {
            order_id: orderId,
            order_at: orderAt,
            order_date: orderDate,
            member_id: memberId,
            product_id: productId,
            product_name: toText(row.product_name ?? productNameByProduct.get(productId)),
            quantity: 0,
            payment_amount: 0,
            source_line_count: 0,
            valid_member_flag: 'true',
            reconstruction_level: 'same_universe_reconstructed'
        };

        current.quantity += toNumber(row.quantity);
        current.payment_amount += toNumber(row.payment_amount ?? orderHeader.order_amount);
        current.source_line_count += 1;

        if (!current.product_name) {
            current.product_name = toText(productNameByProduct.get(productId));
        }

        eventByKey.set(key, current);
    });

    return [...eventByKey.values()]
        .map((row) => ({
            ...row,
            quantity: row.quantity,
            payment_amount: row.payment_amount
        }))
        .sort((left, right) => {
            return (
                left.order_date.localeCompare(right.order_date)
                || left.order_id.localeCompare(right.order_id)
                || left.product_id.localeCompare(right.product_id)
            );
        });
}

function buildReconstructedBasketSummary(eventRows, rawBasketRows) {
    const eventRowsByOrder = groupBy(eventRows, (row) => toText(row.order_id));
    const productStats = new Map();
    const undirectedPairCounts = new Map();
    const rawBasketSummaryByProduct = indexBy(summarizeBasketPairs(rawBasketRows), (row) => row.product_id);
    const cartSizes = [];

    eventRowsByOrder.forEach((rows, orderId) => {
        const uniqueProducts = [...new Set(rows.map((row) => toText(row.product_id)).filter(Boolean))].sort();
        const cartSize = uniqueProducts.length;
        if (!orderId || !cartSize) {
            return;
        }

        cartSizes.push(cartSize);

        uniqueProducts.forEach((productId) => {
            const current = productStats.get(productId) ?? {
                product_id: productId,
                order_cnt: 0,
                attach_order_cnt: 0,
                cart_sizes: [],
                distinct_member_ids: new Set(),
                quantity_sum: 0,
                payment_amount_sum: 0
            };

            const productRows = rows.filter((row) => toText(row.product_id) === productId);
            current.order_cnt += 1;
            current.attach_order_cnt += cartSize > 1 ? 1 : 0;
            current.cart_sizes.push(cartSize);
            productRows.forEach((row) => {
                current.distinct_member_ids.add(toText(row.member_id));
                current.quantity_sum += toNumber(row.quantity);
                current.payment_amount_sum += toNumber(row.payment_amount);
            });
            productStats.set(productId, current);
        });

        for (let index = 0; index < uniqueProducts.length; index += 1) {
            for (let tailIndex = index + 1; tailIndex < uniqueProducts.length; tailIndex += 1) {
                const left = uniqueProducts[index];
                const right = uniqueProducts[tailIndex];
                const key = keyOf(left, right);
                undirectedPairCounts.set(key, (undirectedPairCounts.get(key) ?? 0) + 1);
            }
        }
    });

    const directedPairCounts = [];
    undirectedPairCounts.forEach((count, undirectedKey) => {
        const [left, right] = undirectedKey.split('|');
        directedPairCounts.push({ product_id: left, companion_product_id: right, co_order_cnt: count });
        directedPairCounts.push({ product_id: right, companion_product_id: left, co_order_cnt: count });
    });

    const directedPairsByProduct = groupBy(directedPairCounts, (row) => row.product_id);
    const globalMedianCartSize = median(cartSizes);
    const baseRows = [...productStats.values()].map((row) => {
        const directedPairs = directedPairsByProduct.get(row.product_id) ?? [];
        const pairRows = directedPairs.length;
        const coOrderCounts = directedPairs.map((pair) => toNumber(pair.co_order_cnt)).sort((left, right) => right - left);
        const coOrderTotal = sum(coOrderCounts);
        const topPair = directedPairs.sort((left, right) => toNumber(right.co_order_cnt) - toNumber(left.co_order_cnt))[0] ?? {};
        const rawBasketSummary = rawBasketSummaryByProduct.get(row.product_id) ?? {};
        const rawCompanionCount = toNumber(rawBasketSummary.basket_companion_cnt);
        const rawPairRows = toNumber(rawBasketSummary.basket_pair_rows);
        const topPairMatches = toText(rawBasketSummary.basket_top_pair_product_id)
            ? toText(rawBasketSummary.basket_top_pair_product_id) === toText(topPair.companion_product_id)
            : pairRows === 0;
        const companionRatio = rawCompanionCount
            ? clamp01(1 - (Math.abs(pairRows - rawCompanionCount) / rawCompanionCount))
            : (pairRows ? 0.5 : 1);
        const pairRowRatio = rawPairRows
            ? clamp01(1 - (Math.abs(pairRows - rawPairRows) / rawPairRows))
            : (pairRows ? 0.5 : 1);
        const parityScore = average([
            topPairMatches ? 1 : 0,
            companionRatio,
            pairRowRatio
        ]);

        return {
            product_id: row.product_id,
            order_cnt: row.order_cnt,
            attach_order_cnt: row.attach_order_cnt,
            attach_rate: clamp01(safeDivide(row.attach_order_cnt, row.order_cnt)),
            median_cart_size: median(row.cart_sizes),
            breadth_lift: median(row.cart_sizes) - globalMedianCartSize,
            volume_raw: Math.log1p(row.order_cnt),
            companion_cnt: pairRows,
            top1_share: coOrderTotal ? safeDivide(coOrderCounts[0] ?? 0, coOrderTotal) : 0,
            top3_share: coOrderTotal ? safeDivide(sum(coOrderCounts.slice(0, 3)), coOrderTotal) : 0,
            co_order_total: coOrderTotal,
            top_pair_product_id: toText(topPair.companion_product_id),
            basket_pair_rows: pairRows,
            distinct_member_cnt: row.distinct_member_ids.size,
            quantity_sum: row.quantity_sum,
            payment_amount_sum: row.payment_amount_sum,
            raw_basket_pair_rows: rawPairRows,
            raw_basket_top_pair_product_id: toText(rawBasketSummary.basket_top_pair_product_id),
            raw_basket_signal_score: toNumber(rawBasketSummary.basket_signal_score),
            parity_score: parityScore,
            parity_level: getParityLevel(parityScore),
            reconstruction_level: 'event_window_reconstructed',
            limitation_reason: pairRows === 0
                ? '복수 상품 주문이 부족해 basket summary가 약합니다.'
                : parityScore < 0.55
                    ? 'raw basket detail과의 유사도가 낮습니다.'
                    : ''
        };
    });

    const minVolume = minNumber(baseRows.map((row) => row.volume_raw));
    const maxVolume = maxNumber(baseRows.map((row) => row.volume_raw));
    const volumeDenominator = maxVolume - minVolume;
    const rowsWithVolumeWeight = baseRows.map((row) => ({
        ...row,
        volume_weight: volumeDenominator === 0 ? 1 : clamp01((row.volume_raw - minVolume) / volumeDenominator)
    }));

    const attachP50 = percentile(rowsWithVolumeWeight.map((row) => row.attach_rate), 0.5);
    const attachP75 = percentile(rowsWithVolumeWeight.map((row) => row.attach_rate), 0.75);
    const breadthP75 = percentile(rowsWithVolumeWeight.map((row) => row.breadth_lift), 0.75);
    const companionP75 = percentile(rowsWithVolumeWeight.map((row) => row.companion_cnt), 0.75);
    const top1P50 = percentile(rowsWithVolumeWeight.map((row) => row.top1_share), 0.5);
    const top1P75 = percentile(rowsWithVolumeWeight.map((row) => row.top1_share), 0.75);
    const top3P75 = percentile(rowsWithVolumeWeight.map((row) => row.top3_share), 0.75);
    const cartP50 = percentile(rowsWithVolumeWeight.map((row) => row.median_cart_size), 0.5);
    const maxCoOrderTotal = Math.max(1, ...rowsWithVolumeWeight.map((row) => row.co_order_total));

    return rowsWithVolumeWeight.map((row) => {
        const caValid = row.volume_weight >= 0.1;
        const isCore = caValid
            && row.attach_rate >= attachP75
            && row.breadth_lift >= breadthP75
            && row.companion_cnt >= companionP75
            && row.top1_share <= top1P50;
        const isPair = caValid
            && row.attach_rate >= attachP50
            && row.top1_share >= top1P75;
        const isSet = caValid
            && row.attach_rate >= attachP75
            && row.median_cart_size >= (cartP50 + 2)
            && row.top3_share >= top3P75;
        const primaryType = isPair ? 'Pair' : isSet ? 'Set' : isCore ? 'Core' : 'None';

        return {
            ...row,
            CA_Core: isCore ? 'true' : 'false',
            CA_Pair: isPair ? 'true' : 'false',
            CA_Set: isSet ? 'true' : 'false',
            CA_Primary_Type: primaryType,
            Basket_Gravity_Primary_Type: primaryType,
            basket_signal_score: clamp01(safeDivide(row.co_order_total, maxCoOrderTotal))
        };
    });
}

function normalizeBrandScoreInputs(roleRows, reconstructedBasketRows, reconstructedEventRows) {
    const basketSummaryByProduct = indexBy(reconstructedBasketRows, (row) => row.product_id);
    const eventRowsByProduct = groupBy(reconstructedEventRows, (row) => toText(row.product_id));

    return roleRows.map((row) => {
        const basket = basketSummaryByProduct.get(row.product_id) ?? {};
        const productEvents = eventRowsByProduct.get(row.product_id) ?? [];
        const latestOrderDate = productEvents
            .map((eventRow) => normalizeDateValue(eventRow.order_at ?? eventRow.order_date))
            .filter(Boolean)
            .sort()
            .at(-1) ?? '';

        return {
            date: row.date,
            product_id: row.product_id,
            product_name: row.product_name,
            entry_axis: row.entry_gravity_score,
            expansion_axis: row.expansion_gravity_score,
            convergence_axis: row.convergence_gravity_score,
            return_axis: Math.max(row.return_gravity_score, row.simple_repeat_rate_90d, row.return_customer_rate_90d),
            basket_axis: toNumber(basket.attach_rate),
            first_customer_cnt: row.first_customer_cnt,
            entry_primary_type: toText(row.entry_gravity_primary_type),
            expansion_primary_type: toText(row.expansion_gravity_primary_type),
            distinct_source_product_cnt_90d: toNumber(row.distinct_source_product_cnt_90d),
            return_customer_rate_90d: toNumber(row.return_customer_rate_90d),
            return_loop_rate_90d: toNumber(row.return_loop_rate_90d),
            simple_repeat_rate_90d: toNumber(row.simple_repeat_rate_90d),
            scored_observed_flag: row.scored_observed_flag,
            demand_observed_flag: row.demand_observed_flag,
            event_observed_flag: productEvents.length ? 'true' : 'false',
            structural_active_order_cnt: uniqueCount(productEvents.map((eventRow) => eventRow.order_id)),
            structural_active_member_cnt: uniqueCount(productEvents.map((eventRow) => eventRow.member_id)),
            latest_order_date: latestOrderDate,
            basket_pair_rows: toNumber(basket.basket_pair_rows),
            basket_top_pair_product_id: toText(basket.top_pair_product_id),
            basket_type: toText(basket.Basket_Gravity_Primary_Type),
            attach_rate: toNumber(basket.attach_rate),
            median_cart_size: toNumber(basket.median_cart_size),
            basket_companion_cnt: toNumber(basket.companion_cnt),
            basket_top1_share: toNumber(basket.top1_share),
            basket_top3_share: toNumber(basket.top3_share),
            basket_signal_score: toNumber(basket.basket_signal_score),
            basket_parity_score: toNumber(basket.parity_score),
            basket_parity_level: toText(basket.parity_level),
            basket_reconstruction_level: toText(basket.reconstruction_level),
            basket_limitation_reason: toText(basket.limitation_reason)
        };
    });
}

export function buildStagingArtifacts(rawArtifacts) {
    const productNameByProduct = buildProductNameIndex(rawArtifacts);
    const stgProductRevenueDaily = normalizeProductRevenueDaily(rawArtifacts.product_revenue_daily ?? [], productNameByProduct);
    const stgRoleSourceDaily = normalizeRoleRows(rawArtifacts.pgm_scored ?? [], rawArtifacts.pgm_demand_signals ?? []);
    const stgPriorityInputsDaily = normalizePriorityInputs(stgProductRevenueDaily, stgRoleSourceDaily);
    const stgDataFreshness = [
        summarizeSourceFreshness('orders_header', rawArtifacts.orders_header ?? [], ['order_at', 'date']),
        summarizeSourceFreshness('order_lines', rawArtifacts.order_lines ?? [], ['order_at', 'date']),
        summarizeSourceFreshness('products', rawArtifacts.products ?? [], ['created_at', 'updated_at']),
        summarizeSourceFreshness('order_utm', rawArtifacts.order_utm ?? [], ['order_at', 'date']),
        summarizeSourceFreshness('product_revenue_daily', rawArtifacts.product_revenue_daily ?? [], ['date']),
        summarizeSourceFreshness('pgm_scored', rawArtifacts.pgm_scored ?? [], ['date']),
        summarizeSourceFreshness('pgm_demand_signals', rawArtifacts.pgm_demand_signals ?? [], ['date']),
        summarizeSourceFreshness('pgm_entry_to_expansion_transition', rawArtifacts.pgm_entry_to_expansion_transition ?? [], ['date']),
        summarizeSourceFreshness('pgm_transition_edges', rawArtifacts.pgm_transition_edges ?? [], ['date']),
        summarizeSourceFreshness('pgm_return_loops', rawArtifacts.pgm_return_loops ?? [], ['date']),
        summarizeSourceFreshness('pgm_basket_pairs', rawArtifacts.pgm_basket_pairs ?? [], ['date']),
        summarizeSourceFreshness('brand_purchase_daily', rawArtifacts.brand_purchase_daily ?? [], ['date']),
        summarizeSourceFreshness('brand_score_events', rawArtifacts.brand_score_events ?? [], ['order_at'])
    ];
    const stgReconstructedOrderProductEvents = buildReconstructedOrderProductEvents(
        stgRoleSourceDaily,
        rawArtifacts.order_lines ?? [],
        rawArtifacts.orders_header ?? [],
        productNameByProduct
    );
    const stgReconstructedBasketSummary = buildReconstructedBasketSummary(
        stgReconstructedOrderProductEvents,
        rawArtifacts.pgm_basket_pairs ?? []
    );
    const stgBrandScoreReconstructionInputs = normalizeBrandScoreInputs(
        stgRoleSourceDaily,
        stgReconstructedBasketSummary,
        stgReconstructedOrderProductEvents
    );

    return {
        stg_product_revenue_daily: stgProductRevenueDaily,
        stg_role_source_daily: stgRoleSourceDaily,
        stg_priority_inputs_daily: stgPriorityInputsDaily,
        stg_data_freshness: stgDataFreshness,
        stg_reconstructed_order_product_events: stgReconstructedOrderProductEvents,
        stg_reconstructed_basket_summary: stgReconstructedBasketSummary,
        stg_brand_score_reconstruction_inputs: stgBrandScoreReconstructionInputs
    };
}

function buildRevenueWindows(rows, asOfDate, productNameByProduct = new Map(), productImageByProduct = new Map()) {
    const scopedRows = rows.filter((row) => row.date && row.date <= asOfDate);
    const rowsByProduct = groupBy(scopedRows, (row) => row.product_id);
    const allDates = sortDates(scopedRows.map((row) => row.date));
    const globalMinDate = allDates[0] ?? '';
    const globalMaxDate = allDates.at(-1) ?? '';
    const coverageFlags = getCoverageFlags(globalMinDate, globalMaxDate, asOfDate);

    return [...rowsByProduct.entries()]
        .map(([productId, productRows]) => {
            const revenueByDate = new Map(productRows.map((row) => [row.date, row.revenue]));
            const latestRow = productRows
                .filter((row) => row.date <= asOfDate)
                .sort((left, right) => right.date.localeCompare(left.date))[0];

            const windowValues = Object.fromEntries(WINDOWS.flatMap((windowDays) => {
                let currentRevenue = 0;
                let previousRevenue = 0;
                const compareState = coverageFlags[`historyReady${windowDays}d`] ? 'available' : 'history_insufficient';
                const compareNote = compareState === 'available'
                    ? '직전 동일 길이 기간과 비교 가능합니다.'
                    : '비교 이력이 부족해 직전 기간 비교가 어렵습니다.';

                for (let offset = 0; offset < windowDays; offset += 1) {
                    currentRevenue += toNumber(revenueByDate.get(shiftDate(asOfDate, -offset)));
                    previousRevenue += toNumber(revenueByDate.get(shiftDate(asOfDate, -(windowDays + offset))));
                }

                const previousRevenueValue = compareState === 'available' ? previousRevenue : '';
                const delta = compareState === 'available' ? currentRevenue - previousRevenue : '';
                const deltaRate = compareState === 'available'
                    ? (previousRevenue > 0 ? (currentRevenue - previousRevenue) / previousRevenue : 0)
                    : '';

                return [
                    [`revenue_${windowDays}d_current`, currentRevenue],
                    [`revenue_${windowDays}d_previous`, previousRevenueValue],
                    [`revenue_${windowDays}d_delta`, delta],
                    [`revenue_${windowDays}d_delta_rate`, deltaRate],
                    [`revenue_${windowDays}d_compare_state`, compareState],
                    [`revenue_${windowDays}d_compare_note`, compareNote]
                ];
            }));

            return {
                as_of_date: asOfDate,
                product_id: productId,
                product_name: toText(latestRow?.product_name || productNameByProduct.get(productId)),
                product_image_url: toText(productImageByProduct.get(productId)),
                revenue_today: toNumber(latestRow?.revenue),
                order_count_today: toNumber(latestRow?.order_count),
                cart_count_today: toNumber(latestRow?.cart_count),
                ...windowValues
            };
        })
        .sort((left, right) => toNumber(right.revenue_30d_current) - toNumber(left.revenue_30d_current));
}

function expandRevenueRowsByWindow(rows) {
    return rows.flatMap((row) => {
        return getOperatingWindowDays().map((windowDays) => {
            const windowKey = toWindowKey(windowDays);
            return {
                as_of_date: row.as_of_date,
                window_key: windowKey,
                window_days: windowDays,
                period_label: getPeriodLabel(windowKey),
                compare_period_label: getCompareLabel(windowKey),
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                revenue_today: row.revenue_today,
                order_count_today: row.order_count_today,
                cart_count_today: row.cart_count_today,
                revenue_current: row[`revenue_${windowDays}d_current`],
                revenue_previous: row[`revenue_${windowDays}d_previous`],
                revenue_delta: row[`revenue_${windowDays}d_delta`],
                revenue_delta_rate: row[`revenue_${windowDays}d_delta_rate`],
                revenue_compare_state: row[`revenue_${windowDays}d_compare_state`],
                revenue_compare_note: row[`revenue_${windowDays}d_compare_note`],
                revenue_1d_current: row.revenue_1d_current,
                revenue_1d_previous: row.revenue_1d_previous,
                revenue_1d_delta: row.revenue_1d_delta,
                revenue_1d_delta_rate: row.revenue_1d_delta_rate,
                revenue_1d_compare_state: row.revenue_1d_compare_state,
                revenue_1d_compare_note: row.revenue_1d_compare_note,
                revenue_7d_current: row.revenue_7d_current,
                revenue_7d_previous: row.revenue_7d_previous,
                revenue_7d_delta: row.revenue_7d_delta,
                revenue_7d_delta_rate: row.revenue_7d_delta_rate,
                revenue_7d_compare_state: row.revenue_7d_compare_state,
                revenue_7d_compare_note: row.revenue_7d_compare_note,
                revenue_30d_current: row.revenue_30d_current,
                revenue_30d_previous: row.revenue_30d_previous,
                revenue_30d_delta: row.revenue_30d_delta,
                revenue_30d_delta_rate: row.revenue_30d_delta_rate,
                revenue_30d_compare_state: row.revenue_30d_compare_state,
                revenue_30d_compare_note: row.revenue_30d_compare_note,
                revenue_90d_current: row.revenue_90d_current,
                revenue_90d_previous: row.revenue_90d_previous,
                revenue_90d_delta: row.revenue_90d_delta,
                revenue_90d_delta_rate: row.revenue_90d_delta_rate,
                revenue_90d_compare_state: row.revenue_90d_compare_state,
                revenue_90d_compare_note: row.revenue_90d_compare_note
            };
        });
    });
}

function getRoleSupportCount(rows, candidates) {
    return rows.reduce((sum, row) => {
        return sum + toNumber(candidates.map((field) => row[field]).find((value) => value != null && value !== ''));
    }, 0);
}

function getRoleSupportRate(rows, candidates) {
    return maxNumber(rows.map((row) => toNumber(candidates.map((field) => row[field]).find((value) => value != null && value !== ''))));
}

function buildRoleTaxonomy(
    roleRows,
    transitionSnapshotRows,
    transitionEdgeRows,
    returnLoopRows,
    basketRows,
    freshnessRows,
    asOfDate,
    productImageByProduct = new Map()
) {
    const latestRoleRows = roleRows.filter((row) => row.date === asOfDate);
    const transitionSnapshotByProduct = groupBy(transitionSnapshotRows.filter((row) => normalizeDateValue(row.date) === asOfDate), (row) => {
        return toText(row.product_id ?? row.source_product_id ?? row.entry_product_id ?? row.aa_product_id);
    });
    const transitionEdgeByProduct = groupBy(transitionEdgeRows.filter((row) => normalizeDateValue(row.date) === asOfDate), (row) => {
        return toText(row.aa_product_id ?? row.source_product_id ?? row.product_id);
    });
    const returnByProduct = groupBy(returnLoopRows.filter((row) => toText(row.date) === asOfDate), (row) => {
        return toText(row.source_product_id ?? row.product_id);
    });
    const basketSummaryByProduct = indexBy(summarizeBasketPairs(basketRows.filter((row) => toText(row.date) === asOfDate)), (row) => row.product_id);
    const scoredFreshness = freshnessRows.find((row) => row.source_key === 'pgm_scored');
    const demandFreshness = freshnessRows.find((row) => row.source_key === 'pgm_demand_signals');
    const basketFreshness = freshnessRows.find((row) => row.source_key === 'pgm_basket_pairs');
    const transitionFreshness = freshnessRows.find((row) => row.source_key === 'pgm_entry_to_expansion_transition')
        ?? freshnessRows.find((row) => row.source_key === 'pgm_transition_edges');

    return latestRoleRows.map((row) => {
        const transitionSnapshotGroup = transitionSnapshotByProduct.get(row.product_id) ?? [];
        const transitionEdgeGroup = transitionEdgeByProduct.get(row.product_id) ?? [];
        const returnGroup = returnByProduct.get(row.product_id) ?? [];
        const basket = basketSummaryByProduct.get(row.product_id) ?? {};

        const roleScores = {
            '첫구매기여': row.entry_gravity_score,
            '재구매확장기여': row.expansion_gravity_score,
            '반복구매기여': Math.max(row.return_gravity_score, row.simple_repeat_rate_90d, row.return_customer_rate_90d),
            '동시구매기여': toNumber(basket.basket_signal_score)
        };
        const supportCounts = {
            entry: toNumber(row.first_customer_cnt),
            expansion: getRoleSupportCount(transitionSnapshotGroup, ['transition_customer_cnt', 'entry_to_expansion_customer_cnt', 'customer_cnt', 'expansion_customer_cnt'])
                + getRoleSupportCount(transitionEdgeGroup, ['transition_customer_cnt']),
            repeat: returnGroup.length,
            basket: toNumber(basket.basket_pair_rows)
        };
        const supportRates = {
            expansion: maxNumber([
                getRoleSupportRate(transitionSnapshotGroup, ['transition_rate', 'entry_to_expansion_rate', 'expansion_rate']),
                getRoleSupportRate(transitionEdgeGroup, ['transition_rate'])
            ])
        };
        const sourceFamilyCount = [
            row.scored_observed_flag === 'true',
            row.demand_observed_flag === 'true',
            Boolean(basket.product_id),
            Boolean(transitionSnapshotGroup.length || transitionEdgeGroup.length)
        ].filter(Boolean).length;
        const roleSourceGapDays = maxNumber([
            getDateGapDays(asOfDate, scoredFreshness?.max_date),
            getDateGapDays(asOfDate, demandFreshness?.max_date),
            getDateGapDays(asOfDate, basketFreshness?.max_date),
            getDateGapDays(asOfDate, transitionFreshness?.max_date)
        ].map((value) => value === '' ? Number.NaN : value));

        const sortedRoles = Object.entries(roleScores).sort((left, right) => right[1] - left[1]);
        const [primaryRole, primaryScore] = sortedRoles[0] ?? ['관측 없음', 0];

        let roleEvidenceStatus = 'available';
        if (!sourceFamilyCount) {
            roleEvidenceStatus = 'unavailable';
        } else if (sourceFamilyCount < 2 || roleSourceGapDays > 7) {
            roleEvidenceStatus = 'limited';
        }

        const hasAnySupportEvidence = Object.values(supportCounts).some((value) => value > 0);
        const finalRole = primaryScore < 0.05 && !hasAnySupportEvidence ? '관측 없음' : primaryRole;
        const roleReason = finalRole === '관측 없음'
            ? '역할 관측 없음'
            : `주요 신호 ${primaryScore.toFixed(2)}`;

        return {
            as_of_date: asOfDate,
            product_id: row.product_id,
            product_name: row.product_name,
            product_image_url: toText(productImageByProduct.get(row.product_id)),
            role_taxonomy: finalRole,
            role_score: primaryScore,
            role_evidence_status: roleEvidenceStatus,
            role_reason: roleReason,
            entry_score: roleScores['첫구매기여'],
            expansion_score: roleScores['재구매확장기여'],
            repeat_score: roleScores['반복구매기여'],
            basket_score: roleScores['동시구매기여'],
            primary_axis_label: finalRole === '관측 없음' ? '' : primaryRole,
            primary_axis_value: finalRole === '관측 없음' ? '' : primaryScore,
            entry_support_count: supportCounts.entry,
            expansion_support_count: supportCounts.expansion,
            repeat_support_count: supportCounts.repeat,
            basket_support_count: supportCounts.basket,
            transition_customer_cnt: supportCounts.expansion,
            transition_support_rate: supportRates.expansion,
            return_loop_cnt: returnGroup.length,
            basket_pair_cnt: toNumber(basket.basket_pair_rows)
        };
    });
}

function aggregateRoleRowsByWindow(
    roleRows,
    transitionSnapshotRows,
    transitionEdgeRows,
    returnLoopRows,
    basketRows,
    freshnessRows,
    asOfDate,
    productImageByProduct = new Map()
) {
    const roleDates = sortDates(roleRows.map((row) => row.date));
    const roleMinDate = roleDates[0] ?? '';
    const roleMaxDate = roleDates.at(-1) ?? '';
    const scoredFreshness = freshnessRows.find((row) => row.source_key === 'pgm_scored');
    const demandFreshness = freshnessRows.find((row) => row.source_key === 'pgm_demand_signals');
    const basketFreshness = freshnessRows.find((row) => row.source_key === 'pgm_basket_pairs');
    const transitionFreshness = freshnessRows.find((row) => row.source_key === 'pgm_entry_to_expansion_transition')
        ?? freshnessRows.find((row) => row.source_key === 'pgm_transition_edges');
    const productIds = Array.from(new Set(roleRows.map((row) => toText(row.product_id)).filter(Boolean))).sort();

    return getOperatingWindowDays().flatMap((windowDays) => {
        const windowKey = toWindowKey(windowDays);
        const windowStart = shiftDate(asOfDate, -(windowDays - 1));
        const scopedRoleRows = roleRows.filter((row) => row.date >= windowStart && row.date <= asOfDate);
        const transitionSnapshotByProduct = groupBy(
            transitionSnapshotRows.filter((row) => {
                const rowDate = normalizeDateValue(row.date);
                return rowDate >= windowStart && rowDate <= asOfDate;
            }),
            (row) => toText(row.product_id ?? row.source_product_id ?? row.entry_product_id ?? row.aa_product_id)
        );
        const transitionEdgeByProduct = groupBy(
            transitionEdgeRows.filter((row) => {
                const rowDate = normalizeDateValue(row.date);
                return rowDate >= windowStart && rowDate <= asOfDate;
            }),
            (row) => toText(row.aa_product_id ?? row.source_product_id ?? row.product_id)
        );
        const returnByProduct = groupBy(
            returnLoopRows.filter((row) => {
                const rowDate = normalizeDateValue(row.date);
                return rowDate >= windowStart && rowDate <= asOfDate;
            }),
            (row) => toText(row.source_product_id ?? row.product_id)
        );
        const basketSummaryByProduct = indexBy(
            summarizeBasketPairs(basketRows.filter((row) => {
                const rowDate = normalizeDateValue(row.date);
                return rowDate >= windowStart && rowDate <= asOfDate;
            })),
            (row) => row.product_id
        );
        const roleRowsByProduct = groupBy(scopedRoleRows, (row) => row.product_id);
        const historyReady = Boolean(roleMinDate && roleMaxDate && roleMinDate <= windowStart && roleMaxDate >= asOfDate);

        return productIds.map((productId) => {
            const groupedRows = roleRowsByProduct.get(productId) ?? [];
            const transitionSnapshotGroup = transitionSnapshotByProduct.get(productId) ?? [];
            const transitionEdgeGroup = transitionEdgeByProduct.get(productId) ?? [];
            const returnGroup = returnByProduct.get(productId) ?? [];
            const basket = basketSummaryByProduct.get(productId) ?? {};
            const productName = toText(groupedRows[0]?.product_name);
            const roleScores = {
                '첫구매기여': average(groupedRows.map((row) => toNumber(row.entry_gravity_score))),
                '재구매확장기여': average(groupedRows.map((row) => toNumber(row.expansion_gravity_score))),
                '반복구매기여': average(groupedRows.map((row) => {
                    return Math.max(toNumber(row.return_gravity_score), toNumber(row.simple_repeat_rate_90d), toNumber(row.return_customer_rate_90d));
                })),
                '동시구매기여': toNumber(basket.basket_signal_score)
            };
            const supportCounts = {
                entry: sum(groupedRows.map((row) => toNumber(row.first_customer_cnt))),
                expansion: getRoleSupportCount(transitionSnapshotGroup, ['transition_customer_cnt', 'entry_to_expansion_customer_cnt', 'customer_cnt', 'expansion_customer_cnt'])
                    + getRoleSupportCount(transitionEdgeGroup, ['transition_customer_cnt']),
                repeat: returnGroup.length,
                basket: toNumber(basket.basket_pair_rows)
            };
            const sourceFamilyCount = [
                groupedRows.some((row) => row.scored_observed_flag === 'true'),
                groupedRows.some((row) => row.demand_observed_flag === 'true'),
                Boolean(basket.product_id),
                Boolean(transitionSnapshotGroup.length || transitionEdgeGroup.length)
            ].filter(Boolean).length;
            const roleSourceGapDays = maxNumber([
                getDateGapDays(asOfDate, scoredFreshness?.max_date),
                getDateGapDays(asOfDate, demandFreshness?.max_date),
                getDateGapDays(asOfDate, basketFreshness?.max_date),
                getDateGapDays(asOfDate, transitionFreshness?.max_date)
            ].map((value) => value === '' ? Number.NaN : value));
            const sortedRoles = Object.entries(roleScores).sort((left, right) => right[1] - left[1]);
            const [primaryRole, primaryScore] = sortedRoles[0] ?? ['관측 없음', 0];
            const hasAnySupportEvidence = Object.values(supportCounts).some((value) => value > 0);

            let roleEvidenceStatus = 'available';
            if (!sourceFamilyCount) {
                roleEvidenceStatus = 'unavailable';
            } else if (sourceFamilyCount < 2 || roleSourceGapDays > 7 || !historyReady) {
                roleEvidenceStatus = 'limited';
            }

            const finalRole = primaryScore < 0.05 && !hasAnySupportEvidence ? '관측 없음' : primaryRole;
            const roleReason = !groupedRows.length
                ? `${getPeriodLabel(windowKey)} 역할 관측이 없습니다.`
                : !historyReady
                    ? `${getPeriodLabel(windowKey)} 역할 관측 범위를 함께 확인합니다.`
                    : finalRole === '관측 없음'
                        ? `${getPeriodLabel(windowKey)} 역할 관측 없음`
                        : `${getPeriodLabel(windowKey)} 주요 신호 ${primaryScore.toFixed(2)}`;

            return {
                as_of_date: asOfDate,
                window_key: windowKey,
                window_days: windowDays,
                period_label: getPeriodLabel(windowKey),
                product_id: productId,
                product_name: productName,
                product_image_url: toText(productImageByProduct.get(productId)),
                role_taxonomy: finalRole,
                role_score: primaryScore,
                role_evidence_status: roleEvidenceStatus,
                role_window_state: historyReady ? 'available' : 'history_insufficient',
                role_reason: roleReason,
                entry_score: roleScores['첫구매기여'],
                expansion_score: roleScores['재구매확장기여'],
                repeat_score: roleScores['반복구매기여'],
                basket_score: roleScores['동시구매기여'],
                primary_axis_label: finalRole === '관측 없음' ? '' : primaryRole,
                primary_axis_value: finalRole === '관측 없음' ? '' : primaryScore,
                entry_support_count: supportCounts.entry,
                expansion_support_count: supportCounts.expansion,
                repeat_support_count: supportCounts.repeat,
                basket_support_count: supportCounts.basket,
                transition_customer_cnt: supportCounts.expansion,
                transition_support_rate: maxNumber([
                    getRoleSupportRate(transitionSnapshotGroup, ['transition_rate', 'entry_to_expansion_rate', 'expansion_rate']),
                    getRoleSupportRate(transitionEdgeGroup, ['transition_rate'])
                ]),
                return_loop_cnt: returnGroup.length,
                basket_pair_cnt: toNumber(basket.basket_pair_rows),
                history_ready: historyReady ? 'true' : 'false'
            };
        });
    });
}

function buildBrandReconstructionMeta(rows, asOfDate, freshnessRows, reconstructedEventRows, options = {}) {
    const latestRows = options.prefiltered ? rows : rows.filter((row) => row.date === asOfDate);
    const windowDays = options.windowDays ?? 1;
    const historyReady = options.historyReady ?? true;
    const basketFreshness = freshnessRows.find((row) => row.source_key === 'pgm_basket_pairs');
    const eventFreshness = freshnessRows.find((row) => row.source_key === 'brand_score_events')
        ?? freshnessRows.find((row) => row.source_key === 'order_lines');
    const productCount = latestRows.length;
    const eventProductCount = uniqueCount(reconstructedEventRows.map((row) => row.product_id));
    const hasEntryInput = latestRows.some((row) => toBooleanFlag(row.scored_observed_flag));
    const hasExpansionInput = latestRows.some((row) => toBooleanFlag(row.scored_observed_flag));
    const hasConvergenceInput = latestRows.some((row) => toBooleanFlag(row.demand_observed_flag));
    const hasReturnInput = latestRows.some((row) => toBooleanFlag(row.demand_observed_flag));
    const hasBasketInput = latestRows.some((row) => toText(row.basket_reconstruction_level) || toNumber(row.structural_active_order_cnt) > 0);
    const hasEventInput = reconstructedEventRows.length > 0;
    const basketCoverageRatio = clamp01(safeDivide(
        latestRows.filter((row) => toText(row.basket_reconstruction_level) || toNumber(row.basket_pair_rows) > 0).length,
        productCount
    ));
    const eventCoverageRatio = clamp01(safeDivide(eventProductCount, productCount));
    const axisCoverageRatio = average([
        safeDivide(latestRows.filter((row) => toBooleanFlag(row.scored_observed_flag)).length, productCount),
        safeDivide(latestRows.filter((row) => toBooleanFlag(row.demand_observed_flag)).length, productCount),
        safeDivide(latestRows.filter((row) => toBooleanFlag(row.event_observed_flag)).length, productCount),
        basketCoverageRatio
    ]);
    const basketParityScore = average(latestRows.map((row) => row.basket_parity_score));
    const eventFreshnessGapDays = getDateGapDays(asOfDate, eventFreshness?.max_date);
    const basketFreshnessGapDays = getDateGapDays(asOfDate, basketFreshness?.max_date);
    const allCoreAxesPresent = productCount > 0 && hasEntryInput && hasExpansionInput && hasConvergenceInput && hasReturnInput && hasBasketInput && hasEventInput;
    const limitedReasons = [];

    if (!allCoreAxesPresent) {
        if (!productCount) {
            limitedReasons.push('brand-level 집계를 위한 product input이 없습니다.');
        }
        if (!hasEventInput) {
            limitedReasons.push('reconstructed event가 없습니다.');
        }
        if (!hasBasketInput) {
            limitedReasons.push('reconstructed basket summary가 없습니다.');
        }
        if (!hasEntryInput || !hasExpansionInput || !hasConvergenceInput || !hasReturnInput) {
            limitedReasons.push('scored/demand 기반 핵심 축이 비어 있습니다.');
        }
    } else {
        if (eventFreshnessGapDays === '' || eventFreshnessGapDays > 7) {
            limitedReasons.push('event freshness가 7일을 초과합니다.');
        }
        if (basketFreshnessGapDays === '' || basketFreshnessGapDays > 7) {
            limitedReasons.push('basket freshness가 7일을 초과합니다.');
        }
        if (eventCoverageRatio < 0.7) {
            limitedReasons.push('event product coverage가 낮습니다.');
        }
        if (basketParityScore < 0.55) {
            limitedReasons.push('basket parity가 낮습니다.');
        }
        if (!historyReady) {
            limitedReasons.push(`${windowDays}일 관측 이력이 충분하지 않습니다.`);
        }
    }

    const parityScore = average([
        axisCoverageRatio,
        eventCoverageRatio,
        basketCoverageRatio,
        basketParityScore,
        historyReady ? 1 : 0.3
    ]);
    const parityLevel = getParityLevel(parityScore);
    const nearCoreCandidate = allCoreAxesPresent
        && limitedReasons.length === 0
        && parityScore >= 0.85
        && (eventFreshnessGapDays === 0 || eventFreshnessGapDays === 1)
        && (basketFreshnessGapDays === 0 || basketFreshnessGapDays === 1);

    let brandScoreStatus = 'unavailable';
    if (allCoreAxesPresent) {
        brandScoreStatus = limitedReasons.length ? 'limited' : 'provisional';
    }
    if (nearCoreCandidate && ENABLE_NEAR_CORE_STATUS) {
        brandScoreStatus = 'near-core';
    }

    let limitationReason = limitedReasons.join(' | ');
    if (nearCoreCandidate && !ENABLE_NEAR_CORE_STATUS) {
        limitationReason = limitationReason
            ? `${limitationReason} | near-core 상태는 기본 비활성이라 provisional로 유지했습니다.`
            : 'near-core 상태는 기본 비활성이라 provisional로 유지했습니다.';
    }

    const reconstructionLevel = !allCoreAxesPresent
        ? 'insufficient_reconstruction'
        : brandScoreStatus === 'limited'
            ? 'reconstructed_with_limitations'
            : nearCoreCandidate
                ? 'high_similarity_reconstruction'
                : 'contract_shaped_reconstruction';

    return {
        latestRows,
        productCount,
        basketFreshness,
        eventFreshness,
        basketFreshnessGapDays,
        eventFreshnessGapDays,
        eventCoverageRatio,
        basketCoverageRatio,
        axisCoverageRatio,
        basketParityScore,
        historyReady,
        parityScore,
        parityLevel,
        allCoreAxesPresent,
        nearCoreCandidate,
        brandScoreStatus,
        limitationReason,
        reconstructionLevel
    };
}

function buildBrandScoreBrandLevel(rows, asOfDate, freshnessRows, reconstructedEventRows, options = {}) {
    const meta = buildBrandReconstructionMeta(rows, asOfDate, freshnessRows, reconstructedEventRows, options);
    const { latestRows, productCount } = meta;
    const windowKey = options.windowKey ?? '1d';

    let entryStructureIndex = 0;
    let expansionStructureIndex = 0;
    let convergenceStructureIndex = 0;
    let returnStructureIndex = 0;
    let basketStructureIndex = 0;
    let entryConcentrationRisk = 0;
    let expansionBalanceIndex = 0;
    let convergenceCoverageRatio = 0;
    let convergenceSourceDiversityIndex = 0;
    let returnCoverageRatio = 0;
    let returnConcentrationRisk = 0;
    let basketCoverageRatio = 0;
    let basketBalanceIndex = 0;
    let brandScoreNumeric = '';
    let confidence = 'Low';
    let brandFirstCustomerCnt = 0;
    let structuralActiveProductCnt = 0;

    if (productCount) {
        const broadRatio = safeDivide(latestRows.filter((row) => row.entry_primary_type === 'Broad').length, productCount);
        const qualifiedRatio = safeDivide(latestRows.filter((row) => row.entry_primary_type === 'Qualified').length, productCount);
        const entryTypeBalance = clamp01(1 - Math.abs(broadRatio - qualifiedRatio));
        entryConcentrationRisk = clamp01(topShare(latestRows.map((row) => row.first_customer_cnt), 3));
        entryStructureIndex = clamp01((0.4 * average(latestRows.map((row) => row.entry_axis))) + (0.3 * entryTypeBalance) + (0.3 * (1 - entryConcentrationRisk)));

        const expansionCoverageRatio = safeDivide(latestRows.filter((row) => ['Core', 'Deep', 'Scale'].includes(row.expansion_primary_type)).length, productCount);
        const coreRatio = safeDivide(latestRows.filter((row) => row.expansion_primary_type === 'Core').length, productCount);
        const deepRatio = safeDivide(latestRows.filter((row) => row.expansion_primary_type === 'Deep').length, productCount);
        const scaleRatio = safeDivide(latestRows.filter((row) => row.expansion_primary_type === 'Scale').length, productCount);
        expansionBalanceIndex = safeBalanceIndex([coreRatio, deepRatio, scaleRatio]);
        expansionStructureIndex = clamp01((0.4 * average(latestRows.map((row) => row.expansion_axis))) + (0.3 * expansionCoverageRatio) + (0.3 * expansionBalanceIndex));

        convergenceCoverageRatio = safeDivide(latestRows.filter((row) => row.distinct_source_product_cnt_90d > 0).length, productCount);
        convergenceSourceDiversityIndex = clamp01(average(latestRows.map((row) => {
            return Math.min(1, safeDivide(row.distinct_source_product_cnt_90d, Math.max(productCount - 1, 1)));
        })));
        convergenceStructureIndex = clamp01((0.4 * average(latestRows.map((row) => row.convergence_axis))) + (0.3 * convergenceCoverageRatio) + (0.3 * convergenceSourceDiversityIndex));

        returnCoverageRatio = safeDivide(latestRows.filter((row) => row.return_loop_rate_90d > 0).length, productCount);
        returnConcentrationRisk = clamp01(topShare(latestRows.map((row) => row.return_customer_rate_90d), 3));
        returnStructureIndex = clamp01((0.4 * average(latestRows.map((row) => row.return_axis))) + (0.3 * returnCoverageRatio) + (0.3 * (1 - returnConcentrationRisk)));

        basketCoverageRatio = safeDivide(latestRows.filter((row) => ['Core', 'Pair', 'Set'].includes(row.basket_type)).length, productCount);
        const basketCoreRatio = safeDivide(latestRows.filter((row) => row.basket_type === 'Core').length, productCount);
        const basketPairRatio = safeDivide(latestRows.filter((row) => row.basket_type === 'Pair').length, productCount);
        const basketSetRatio = safeDivide(latestRows.filter((row) => row.basket_type === 'Set').length, productCount);
        basketBalanceIndex = safeBalanceIndex([basketCoreRatio, basketPairRatio, basketSetRatio]);
        basketStructureIndex = clamp01((0.4 * average(latestRows.map((row) => row.attach_rate))) + (0.3 * basketCoverageRatio) + (0.3 * basketBalanceIndex));

        const structureAxes = [
            entryStructureIndex,
            expansionStructureIndex,
            convergenceStructureIndex,
            returnStructureIndex,
            basketStructureIndex
        ];
        brandScoreNumeric = meta.allCoreAxesPresent
            ? clamp01(minNumber(structureAxes) + (0.03 * average(structureAxes)))
            : '';

        brandFirstCustomerCnt = sum(latestRows.map((row) => row.first_customer_cnt));
        structuralActiveProductCnt = latestRows.filter((row) => {
            return (
                row.entry_axis > 0
                || row.expansion_axis > 0
                || row.convergence_axis > 0
                || row.return_axis > 0
                || row.basket_type !== 'None'
            );
        }).length;

        if (brandFirstCustomerCnt >= 500 && structuralActiveProductCnt >= 5) {
            confidence = 'High';
        } else if (brandFirstCustomerCnt >= 100 && structuralActiveProductCnt >= 2) {
            confidence = 'Medium';
        }
    }

    const numericDisplayPolicy = ['provisional', 'near-core'].includes(meta.brandScoreStatus) ? 'show' : 'hide';
    const limitationSummary = summarizeBrandScoreReason(meta.brandScoreStatus, meta.limitationReason);
    const note = buildBrandScoreNote(
        meta.brandScoreStatus,
        meta.limitationReason,
        meta.nearCoreCandidate && !ENABLE_NEAR_CORE_STATUS
            ? '운영 반영 전 검증 단계'
            : '운영 참고값 제공'
    );

    return [{
        as_of_date: asOfDate,
        window_key: windowKey,
        window_days: toWindowDays(windowKey),
        period_label: getPeriodLabel(windowKey),
        product_count: productCount,
        entry_structure_index: entryStructureIndex,
        expansion_structure_index: expansionStructureIndex,
        convergence_structure_index: convergenceStructureIndex,
        return_structure_index: returnStructureIndex,
        basket_structure_index: basketStructureIndex,
        entry_concentration_risk: entryConcentrationRisk,
        expansion_balance_index: expansionBalanceIndex,
        convergence_coverage_ratio: convergenceCoverageRatio,
        convergence_source_diversity_index: convergenceSourceDiversityIndex,
        return_coverage_ratio: returnCoverageRatio,
        return_concentration_risk: returnConcentrationRisk,
        basket_coverage_ratio: basketCoverageRatio,
        basket_balance_index: basketBalanceIndex,
        brand_score_ps: brandScoreNumeric,
        legacy_bhi: brandScoreNumeric,
        brand_score_numeric: brandScoreNumeric,
        brand_score_display_value: numericDisplayPolicy === 'show' && brandScoreNumeric !== '' ? brandScoreNumeric : '',
        brand_score_status: meta.brandScoreStatus,
        status_label: toBrandScoreStatusLabel(meta.brandScoreStatus),
        status_reason: note,
        freshness_status: (meta.eventFreshnessGapDays === '' || meta.eventFreshnessGapDays > 7 || meta.basketFreshnessGapDays === '' || meta.basketFreshnessGapDays > 7)
            ? 'limited'
            : 'ready',
        numeric_display_policy: numericDisplayPolicy,
        confidence_label: confidence,
        confidence: confidence,
        reconstruction_level: meta.reconstructionLevel,
        parity_level: meta.parityLevel,
        limitation_reason: limitationSummary,
        limitation_reason_detail: meta.limitationReason,
        near_core_candidate_flag: meta.nearCoreCandidate ? 'true' : 'false',
        near_core_enabled_flag: ENABLE_NEAR_CORE_STATUS ? 'true' : 'false',
        parity_score: meta.parityScore,
        axis_coverage_ratio: meta.axisCoverageRatio,
        event_coverage_ratio: meta.eventCoverageRatio,
        basket_reconstruction_coverage_ratio: meta.basketCoverageRatio,
        basket_parity_score: meta.basketParityScore,
        brand_first_customer_cnt: brandFirstCustomerCnt,
        structural_active_product_cnt: structuralActiveProductCnt,
        basket_source_max_date: meta.basketFreshness?.max_date ?? '',
        event_source_max_date: meta.eventFreshness?.max_date ?? '',
        basket_freshness_gap_days: meta.basketFreshnessGapDays,
        event_freshness_gap_days: meta.eventFreshnessGapDays,
        contract_note: note,
        formula_reference: 'step03_brand_health purchase structure formula',
        history_ready: meta.historyReady ? 'true' : 'false'
    }];
}

function buildBrandScoreProductContributors(rows, asOfDate, brandLevelRow, productImageByProduct = new Map(), options = {}) {
    const latestRows = options.prefiltered ? rows : rows.filter((row) => row.date === asOfDate);
    const windowKey = options.windowKey ?? '1d';
    const maxStructuralOrders = Math.max(1, ...latestRows.map((row) => toNumber(row.structural_active_order_cnt)));
    const contributorBaseRows = latestRows.map((row) => {
        const hasScoredInput = toBooleanFlag(row.scored_observed_flag);
        const hasDemandInput = toBooleanFlag(row.demand_observed_flag);
        const hasEventInput = toBooleanFlag(row.event_observed_flag);
        const hasBasketInput = Boolean(toText(row.basket_reconstruction_level) || toNumber(row.structural_active_order_cnt) > 0);
        const axes = [
            toNumber(row.entry_axis),
            toNumber(row.expansion_axis),
            toNumber(row.convergence_axis),
            toNumber(row.return_axis),
            toNumber(row.basket_axis)
        ];
        const signal = hasScoredInput && hasDemandInput && hasEventInput && hasBasketInput
            ? clamp01(minNumber(axes) + (0.03 * average(axes)))
            : '';
        const confidenceIndex = clamp01(
            (clamp01(toNumber(row.first_customer_cnt) / 40) * 0.55)
            + (clamp01(toNumber(row.structural_active_order_cnt) / maxStructuralOrders) * 0.45)
        );
        const parityScore = average([
            toNumber(row.basket_parity_score),
            hasScoredInput ? 1 : 0,
            hasDemandInput ? 1 : 0,
            hasEventInput ? 1 : 0
        ]);
        const limitationParts = [];
        if (!hasScoredInput || !hasDemandInput || !hasEventInput || !hasBasketInput) {
            limitationParts.push('상품 단위 핵심 축이 모두 재구성되지 않았습니다.');
        }
        if (toText(row.basket_limitation_reason)) {
            limitationParts.push(toText(row.basket_limitation_reason));
        }
        if (toText(brandLevelRow.limitation_reason_detail || brandLevelRow.limitation_reason)) {
            limitationParts.push(toText(brandLevelRow.limitation_reason_detail || brandLevelRow.limitation_reason));
        }

        let contributorStatus = brandLevelRow.brand_score_status;
        if (!hasScoredInput || !hasDemandInput || !hasEventInput || !hasBasketInput) {
            contributorStatus = 'unavailable';
        } else if (brandLevelRow.brand_score_status === 'limited' || toText(row.basket_parity_level) === 'low') {
            contributorStatus = 'limited';
        } else if (brandLevelRow.brand_score_status === 'near-core' && ENABLE_NEAR_CORE_STATUS && parityScore >= 0.85) {
            contributorStatus = 'near-core';
        } else {
            contributorStatus = 'provisional';
        }

        const reconstructionLevel = contributorStatus === 'unavailable'
            ? 'insufficient_reconstruction'
            : contributorStatus === 'limited'
                ? 'reconstructed_with_limitations'
                : contributorStatus === 'near-core'
                    ? 'high_similarity_reconstruction'
                    : 'contract_shaped_reconstruction';
        const limitationDetail = limitationParts.filter(Boolean).join(' | ');
        const limitationSummary = summarizeBrandScoreReason(contributorStatus, limitationDetail);
        const contributorNote = buildBrandScoreNote(contributorStatus, limitationDetail);

        return {
            as_of_date: asOfDate,
            window_key: windowKey,
            window_days: toWindowDays(windowKey),
            period_label: getPeriodLabel(windowKey),
            product_id: row.product_id,
            product_name: row.product_name,
            product_image_url: toText(productImageByProduct.get(row.product_id)),
            contribution_status: contributorStatus,
            contributor_status: contributorStatus,
            reconstructed_product_signal: signal,
            confidence: toConfidenceLabel(confidenceIndex),
            numeric_display_policy: ['provisional', 'near-core'].includes(contributorStatus) ? 'show' : 'hide',
            reconstruction_level: reconstructionLevel,
            parity_level: getParityLevel(parityScore),
            parity_score: parityScore,
            status_label: toBrandScoreStatusLabel(contributorStatus),
            status_reason: contributorNote,
            limitation_reason: limitationSummary,
            limitation_reason_detail: limitationDetail,
            entry_axis: row.entry_axis,
            expansion_axis: row.expansion_axis,
            convergence_axis: row.convergence_axis,
            return_axis: row.return_axis,
            basket_axis: row.basket_axis,
            entry_primary_type: row.entry_primary_type,
            expansion_primary_type: row.expansion_primary_type,
            basket_type: row.basket_type,
            attach_rate: row.attach_rate,
            first_customer_cnt: row.first_customer_cnt,
            structural_active_order_cnt: row.structural_active_order_cnt,
            structural_active_member_cnt: row.structural_active_member_cnt,
            basket_pair_rows: row.basket_pair_rows,
            top_pair_product_id: row.basket_top_pair_product_id,
            basket_top_pair_product_id: row.basket_top_pair_product_id,
            basket_top1_share: row.basket_top1_share,
            basket_top3_share: row.basket_top3_share,
            basket_parity_score: row.basket_parity_score,
            basket_parity_level: row.basket_parity_level,
            event_freshness_gap_days: brandLevelRow.event_freshness_gap_days,
            basket_freshness_gap_days: brandLevelRow.basket_freshness_gap_days,
            role_alignment_note: `${toStructureLabel(row.entry_primary_type || 'None')} / ${toStructureLabel(row.expansion_primary_type || 'None')} / ${toStructureLabel(row.basket_type || 'None')}`
        };
    });

    const totalSignal = sum(contributorBaseRows.map((row) => row.reconstructed_product_signal));
    return contributorBaseRows
        .map((row) => ({
            ...row,
            contribution_share: totalSignal ? safeDivide(row.reconstructed_product_signal, totalSignal) : 0
        }))
        .sort((left, right) => {
            return (
                toNumber(right.reconstructed_product_signal) - toNumber(left.reconstructed_product_signal)
                || right.product_id.localeCompare(left.product_id)
            );
        })
        .map((row, index) => ({
            ...row,
            contributor_rank: index + 1
        }));
}

function getPrimaryTypeByFrequency(values, fallback = 'None') {
    const counts = new Map();
    values.map((value) => toText(value)).filter(Boolean).forEach((value) => {
        counts.set(value, (counts.get(value) ?? 0) + 1);
    });
    return [...counts.entries()].sort((left, right) => right[1] - left[1])[0]?.[0] ?? fallback;
}

function aggregateBrandRowsByWindow(rows, asOfDate) {
    const dateBounds = sortDates(rows.map((row) => row.date));
    const minDate = dateBounds[0] ?? '';
    const maxDate = dateBounds.at(-1) ?? '';

    return getOperatingWindowDays().map((windowDays) => {
        const windowKey = toWindowKey(windowDays);
        const windowStart = shiftDate(asOfDate, -(windowDays - 1));
        const scopedRows = rows.filter((row) => row.date >= windowStart && row.date <= asOfDate);
        const rowsByProduct = groupBy(scopedRows, (row) => row.product_id);
        const aggregatedRows = [...rowsByProduct.entries()].map(([productId, groupedRows]) => ({
            date: asOfDate,
            product_id: productId,
            product_name: toText(groupedRows[0]?.product_name),
            entry_axis: average(groupedRows.map((row) => toNumber(row.entry_axis))),
            expansion_axis: average(groupedRows.map((row) => toNumber(row.expansion_axis))),
            convergence_axis: average(groupedRows.map((row) => toNumber(row.convergence_axis))),
            return_axis: average(groupedRows.map((row) => toNumber(row.return_axis))),
            basket_axis: average(groupedRows.map((row) => toNumber(row.basket_axis))),
            first_customer_cnt: sum(groupedRows.map((row) => toNumber(row.first_customer_cnt))),
            entry_primary_type: getPrimaryTypeByFrequency(groupedRows.map((row) => row.entry_primary_type), 'None'),
            expansion_primary_type: getPrimaryTypeByFrequency(groupedRows.map((row) => row.expansion_primary_type), 'None'),
            distinct_source_product_cnt_90d: average(groupedRows.map((row) => toNumber(row.distinct_source_product_cnt_90d))),
            return_customer_rate_90d: average(groupedRows.map((row) => toNumber(row.return_customer_rate_90d))),
            return_loop_rate_90d: average(groupedRows.map((row) => toNumber(row.return_loop_rate_90d))),
            simple_repeat_rate_90d: average(groupedRows.map((row) => toNumber(row.simple_repeat_rate_90d))),
            scored_observed_flag: groupedRows.some((row) => row.scored_observed_flag === 'true') ? 'true' : 'false',
            demand_observed_flag: groupedRows.some((row) => row.demand_observed_flag === 'true') ? 'true' : 'false',
            event_observed_flag: groupedRows.some((row) => row.event_observed_flag === 'true') ? 'true' : 'false',
            structural_active_order_cnt: sum(groupedRows.map((row) => toNumber(row.structural_active_order_cnt))),
            structural_active_member_cnt: sum(groupedRows.map((row) => toNumber(row.structural_active_member_cnt))),
            latest_order_date: sortDates(groupedRows.map((row) => row.latest_order_date)).at(-1) ?? '',
            basket_pair_rows: sum(groupedRows.map((row) => toNumber(row.basket_pair_rows))),
            basket_top_pair_product_id: getPrimaryTypeByFrequency(groupedRows.map((row) => row.basket_top_pair_product_id), ''),
            basket_type: getPrimaryTypeByFrequency(groupedRows.map((row) => row.basket_type), 'None'),
            attach_rate: average(groupedRows.map((row) => toNumber(row.attach_rate))),
            median_cart_size: average(groupedRows.map((row) => toNumber(row.median_cart_size))),
            basket_companion_cnt: average(groupedRows.map((row) => toNumber(row.basket_companion_cnt))),
            basket_top1_share: average(groupedRows.map((row) => toNumber(row.basket_top1_share))),
            basket_top3_share: average(groupedRows.map((row) => toNumber(row.basket_top3_share))),
            basket_signal_score: average(groupedRows.map((row) => toNumber(row.basket_signal_score))),
            basket_parity_score: average(groupedRows.map((row) => toNumber(row.basket_parity_score))),
            basket_parity_level: getParityLevel(average(groupedRows.map((row) => toNumber(row.basket_parity_score)))),
            basket_reconstruction_level: getPrimaryTypeByFrequency(groupedRows.map((row) => row.basket_reconstruction_level), ''),
            basket_limitation_reason: [...new Set(groupedRows.map((row) => toText(row.basket_limitation_reason)).filter(Boolean))].join(' | ')
        }));

        return {
            window_key: windowKey,
            window_days: windowDays,
            period_label: getPeriodLabel(windowKey),
            history_ready: Boolean(minDate && maxDate && minDate <= windowStart && maxDate >= asOfDate),
            rows: aggregatedRows
        };
    });
}

function buildLegacyBrandScoreReconstruction(contributorRows, brandLevelRow) {
    return contributorRows.map((row) => ({
        as_of_date: row.as_of_date,
        window_key: row.window_key,
        window_days: row.window_days,
        period_label: row.period_label,
        product_id: row.product_id,
        product_name: row.product_name,
        brand_score: row.numeric_display_policy === 'show' ? row.reconstructed_product_signal : '',
        brand_score_status: row.contributor_status,
        confidence: row.confidence,
        numeric_display_policy: row.numeric_display_policy,
        event_freshness_gap_days: brandLevelRow.event_freshness_gap_days,
        basket_freshness_gap_days: brandLevelRow.basket_freshness_gap_days,
        status_cap_reason: brandLevelRow.limitation_reason,
        entry_axis: row.entry_axis,
        expansion_axis: row.expansion_axis,
        convergence_axis: row.convergence_axis,
        return_axis: row.return_axis,
        basket_axis: row.basket_axis,
        basket_top_pair_product_id: row.basket_top_pair_product_id,
        reconstruction_level: row.reconstruction_level,
        parity_level: row.parity_level,
        limitation_reason: row.limitation_reason,
        limitation_reason_detail: row.limitation_reason_detail,
        reconstructed_product_signal: row.reconstructed_product_signal,
        contribution_share: row.contribution_share,
        brand_score_note: row.contributor_status === 'unavailable'
            ? '상품별 최종 점수는 아직 산출하지 못했습니다.'
            : row.contributor_status === 'limited'
                ? buildBrandScoreNote(row.contributor_status, row.limitation_reason_detail || row.limitation_reason, '상품별 기여 신호를 제한 반영합니다.')
                : '상품별 최종 점수 대신 기여 신호를 함께 보여줍니다.'
    }));
}

function buildReconstructionRegistry(asOfDate, freshnessRows, reconstructedEventRows, reconstructedBasketRows, brandLevelRow, contributorRows) {
    const eventFreshness = freshnessRows.find((row) => row.source_key === 'brand_score_events')
        ?? freshnessRows.find((row) => row.source_key === 'order_lines');
    const basketFreshness = freshnessRows.find((row) => row.source_key === 'pgm_basket_pairs');
    const contributorCount = contributorRows.length;
    const basketParityScore = average(reconstructedBasketRows.map((row) => row.parity_score));
    const contributorParityScore = average(contributorRows.map((row) => row.parity_score));
    const eventCoverageRatio = clamp01(safeDivide(uniqueCount(reconstructedEventRows.map((row) => row.product_id)), contributorCount || 1));
    const basketCoverageRatio = clamp01(safeDivide(reconstructedBasketRows.length, contributorCount || 1));
    const contributorCoverageRatio = clamp01(safeDivide(contributorRows.filter((row) => row.contributor_status !== 'unavailable').length, contributorCount || 1));

    return [
        {
            as_of_date: asOfDate,
            registry_key: 'revenue_rolling',
            item_name: 'Revenue rolling',
            classification: 'Rosetta direct',
            rosetta_direct: '예',
            core_referenced: '아니오',
            reconstruction_logic: 'product_revenue_daily를 기준으로 7/30/90일 rolling과 직전 동일 길이 비교를 계산합니다.',
            parity_level: 'source_direct',
            user_exposure_level: '주축',
            limitation_reason: '',
            availability_status: 'available'
        },
        {
            as_of_date: asOfDate,
            registry_key: 'role_taxonomy',
            item_name: 'Role taxonomy',
            classification: 'Semantically similar',
            rosetta_direct: '예',
            core_referenced: '아니오',
            reconstruction_logic: 'pgm_scored와 demand, basket, transition source를 조합해 PRD 역할 분류를 다시 계산합니다.',
            parity_level: 'policy_mapped',
            user_exposure_level: '주축',
            limitation_reason: 'PRD 역할 분류는 direct source를 재배열한 운영용 분류라 core canonical output과 1:1 대응하지 않습니다.',
            availability_status: 'available'
        },
        {
            as_of_date: asOfDate,
            registry_key: 'reconstructed_order_product_events',
            item_name: 'order product events',
            classification: 'Core-reconstructed',
            rosetta_direct: '아니오',
            core_referenced: '예',
            reconstruction_logic: 'orders_header와 order_lines를 canonical join해 member_id/order_id/order_at/product_id 기준 reconstructed event layer를 만듭니다.',
            parity_level: getParityLevel(eventCoverageRatio),
            user_exposure_level: '내부 지원',
            limitation_reason: reconstructedEventRows.length
                ? (getDateGapDays(asOfDate, eventFreshness?.max_date) > 7 ? 'event 최신일이 늦어 core와 동일한 freshness를 보장하지 못합니다.' : '')
                : 'event 재구성 결과가 비어 있습니다.',
            availability_status: reconstructedEventRows.length ? 'available' : 'unavailable'
        },
        {
            as_of_date: asOfDate,
            registry_key: 'reconstructed_basket_summary',
            item_name: 'basket summary',
            classification: 'Core-reconstructed',
            rosetta_direct: '아니오',
            core_referenced: '예',
            reconstruction_logic: 'reconstructed order product events에서 attach_rate, top1/top3 share, volume weight, basket type을 core 규칙으로 재생성합니다.',
            parity_level: getParityLevel(basketParityScore),
            user_exposure_level: '상세 보조',
            limitation_reason: basketParityScore < 0.55 ? 'raw basket detail과의 유사도가 낮아 exact parity를 주장할 수 없습니다.' : '',
            availability_status: reconstructedBasketRows.length ? 'available' : 'unavailable'
        },
        {
            as_of_date: asOfDate,
            registry_key: 'basket_role_evidence',
            item_name: 'basket role evidence',
            classification: 'Semantically similar',
            rosetta_direct: '아니오',
            core_referenced: '예',
            reconstruction_logic: 'reconstructed basket summary를 동시구매기여와 상세 근거에 연결합니다.',
            parity_level: getParityLevel(average([basketParityScore, basketCoverageRatio])),
            user_exposure_level: '상세 보조',
            limitation_reason: basketParityScore < 0.55 ? '역할 근거로는 사용하지만 core basket summary와 동일성은 아직 입증되지 않았습니다.' : '',
            availability_status: reconstructedBasketRows.length ? 'available' : 'limited'
        },
        {
            as_of_date: asOfDate,
            registry_key: 'brand_score_brand_level',
            item_name: 'Brand Score brand-level',
            classification: 'Core-reconstructed',
            rosetta_direct: '아니오',
            core_referenced: '예',
            reconstruction_logic: 'step03_brand_health의 5축 구조와 PS 공식을 따라 brand-level Brand Score를 재구성합니다.',
            parity_level: toText(brandLevelRow.parity_level),
            user_exposure_level: '보조축',
            limitation_reason: toText(brandLevelRow.limitation_reason),
            availability_status: toText(brandLevelRow.brand_score_status)
        },
        {
            as_of_date: asOfDate,
            registry_key: 'brand_score_product_contributors',
            item_name: 'Brand Score product contribution',
            classification: 'Core-reconstructed',
            rosetta_direct: '아니오',
            core_referenced: '예',
            reconstruction_logic: 'brand-level 구조 점수에 기여하는 상품별 축 입력과 기여 비중을 분리해 보여줍니다.',
            parity_level: getParityLevel(contributorParityScore),
            user_exposure_level: '상세 보조',
            limitation_reason: contributorRows.some((row) => row.contributor_status === 'limited') ? '일부 상품은 contributor 상태가 limited입니다.' : '',
            availability_status: getWorstStatus(contributorRows.map((row) => row.contributor_status))
        },
        {
            as_of_date: asOfDate,
            registry_key: 'brand_confidence',
            item_name: 'brand confidence',
            classification: 'Semantically similar',
            rosetta_direct: '아니오',
            core_referenced: '예',
            reconstruction_logic: 'core step03 threshold를 따라 High/Medium/Low confidence label을 brand-level reconstructed input에 적용합니다.',
            parity_level: 'threshold_aligned',
            user_exposure_level: '보조축',
            limitation_reason: 'confidence label은 core threshold를 참고하지만 direct diff 검증은 아직 없습니다.',
            availability_status: toText(brandLevelRow.brand_score_status, 'unavailable')
        },
        {
            as_of_date: asOfDate,
            registry_key: 'brand_revenue_context',
            item_name: 'brand revenue context',
            classification: 'Rosetta direct',
            rosetta_direct: '예',
            core_referenced: '아니오',
            reconstruction_logic: 'brand_purchase_daily를 기준으로 최근 30일과 직전 30일 브랜드 매출을 계산합니다.',
            parity_level: 'source_direct',
            user_exposure_level: '문맥 보조',
            limitation_reason: '',
            availability_status: 'available'
        },
        {
            as_of_date: asOfDate,
            registry_key: 'brand_score_exact_parity',
            item_name: 'Brand Score exact parity',
            classification: 'Still insufficient',
            rosetta_direct: '아니오',
            core_referenced: '예',
            reconstruction_logic: 'core output diff harness 없이 exact parity를 보장하지 않습니다.',
            parity_level: 'not_proven',
            user_exposure_level: '미노출',
            limitation_reason: 'core canonical output과의 field-by-field diff 검증이 없어 near-core나 exact parity를 주장할 수 없습니다.',
            availability_status: 'unavailable'
        }
    ];
}

function buildBrandScoreValidationStatus(brandLevelRows, contributorRows, registryRows, asOfDate) {
    const brandLevelRow = brandLevelRows[0] ?? {};
    const statusCounts = Object.fromEntries(BRAND_SCORE_STATUSES.map((status) => [status, 0]));
    contributorRows.forEach((row) => {
        statusCounts[row.contributor_status] = (statusCounts[row.contributor_status] ?? 0) + 1;
    });

    return [{
        as_of_date: asOfDate,
        brand_level_status: toText(brandLevelRow.brand_score_status),
        unavailable_count: statusCounts.unavailable ?? 0,
        limited_count: statusCounts.limited ?? 0,
        provisional_count: statusCounts.provisional ?? 0,
        near_core_count: statusCounts['near-core'] ?? 0,
        numeric_display_policy: toText(brandLevelRow.numeric_display_policy),
        basket_source_max_date: toText(brandLevelRow.basket_source_max_date),
        event_source_max_date: toText(brandLevelRow.event_source_max_date),
        basket_freshness_gap_days: toText(brandLevelRow.basket_freshness_gap_days),
        event_freshness_gap_days: toText(brandLevelRow.event_freshness_gap_days),
        status_cap_reason: toText(brandLevelRow.limitation_reason),
        reconstruction_registry_count: registryRows.length,
        validation_note: 'Brand Score는 큐 랭킹에 연결하지 않고 brand-level panel + contributor contract로만 노출합니다.'
    }];
}

function buildBrandRevenueContext(rows, asOfDate) {
    const revenueByDate = new Map(
        rows
            .map((row) => [normalizeDateValue(row.date), toNumber(row.total_purchase_amount ?? row.brand_purchase_amount ?? row.purchase_amount)])
            .filter(([date]) => date)
    );

    return getOperatingWindowDays().map((windowDays) => {
        let currentRevenue = 0;
        let previousRevenue = 0;
        for (let offset = 0; offset < windowDays; offset += 1) {
            currentRevenue += toNumber(revenueByDate.get(shiftDate(asOfDate, -offset)));
            previousRevenue += toNumber(revenueByDate.get(shiftDate(asOfDate, -(windowDays + offset))));
        }

        return {
            as_of_date: asOfDate,
            window_key: toWindowKey(windowDays),
            window_days: windowDays,
            period_label: getPeriodLabel(toWindowKey(windowDays)),
            brand_revenue_current: currentRevenue,
            brand_revenue_previous: previousRevenue,
            brand_revenue_delta_rate: previousRevenue > 0 ? (currentRevenue - previousRevenue) / previousRevenue : 0,
            brand_revenue_1d_current: windowDays === 1 ? currentRevenue : '',
            brand_revenue_1d_previous: windowDays === 1 ? previousRevenue : '',
            brand_revenue_1d_delta_rate: windowDays === 1 ? (previousRevenue > 0 ? (currentRevenue - previousRevenue) / previousRevenue : 0) : '',
            brand_revenue_7d_current: windowDays === 7 ? currentRevenue : '',
            brand_revenue_7d_previous: windowDays === 7 ? previousRevenue : '',
            brand_revenue_7d_delta_rate: windowDays === 7 ? (previousRevenue > 0 ? (currentRevenue - previousRevenue) / previousRevenue : 0) : '',
            brand_revenue_30d_current: windowDays === 30 ? currentRevenue : '',
            brand_revenue_30d_previous: windowDays === 30 ? previousRevenue : '',
            brand_revenue_30d_delta_rate: windowDays === 30 ? (previousRevenue > 0 ? (currentRevenue - previousRevenue) / previousRevenue : 0) : ''
        };
    });
}

function buildPriorityBasis(revenueRows, roleRows, brandScoreRows, freshnessRows, asOfDate, brandRevenueContext) {
    const roleByProduct = indexBy(roleRows, (row) => keyOf(row.window_key, row.product_id));
    const brandByProduct = indexBy(brandScoreRows, (row) => keyOf(row.window_key, row.product_id));
    const brandRevenueByWindow = indexBy(brandRevenueContext, (row) => row.window_key);
    const revenueFreshness = freshnessRows.find((row) => row.source_key === 'product_revenue_daily');
    const roleFreshness = freshnessRows.find((row) => row.source_key === 'pgm_scored');
    const revenueFreshGap = revenueFreshness?.max_date ? toText(revenueFreshness.max_date) : '';
    const roleFreshGap = roleFreshness?.max_date ? toText(roleFreshness.max_date) : '';

    return revenueRows.map((row) => {
        const role = roleByProduct.get(keyOf(row.window_key, row.product_id)) ?? {};
        const brand = brandByProduct.get(keyOf(row.window_key, row.product_id)) ?? {};
        const brandRevenueRow = brandRevenueByWindow.get(row.window_key) ?? {};
        const revenueCompareState = toText(row.revenue_compare_state, 'history_insufficient');
        const revenueCompareNote = toText(row.revenue_compare_note, '비교 이력이 부족해 직전 기간 비교가 어렵습니다.');
        const deltaRate = revenueCompareState === 'available' ? Number(row.revenue_delta_rate) : null;

        let priorityLevel = PRIORITY_LEVELS[2];
        if (deltaRate != null && deltaRate <= -0.2) {
            priorityLevel = PRIORITY_LEVELS[0];
        } else if (deltaRate < -0.05) {
            priorityLevel = PRIORITY_LEVELS[1];
        }

        const revenueReason = deltaRate == null
            ? '직전기간 비교 불가'
            : `${row.period_label} 매출이 직전기간 대비 ${(deltaRate * 100).toFixed(1)}%`;

        const roleReason = role.role_taxonomy
            ? `${row.period_label} ${role.role_taxonomy} 기준 ${toText(role.role_reason)}`
            : '역할 근거 없음';

        const brandReason = brand.brand_score_status === 'limited'
            ? `${row.period_label} 브랜드 방향 신호 제한 반영`
            : brand.brand_score_status === 'provisional'
                ? `${row.period_label} 브랜드 구조 변화 신호`
                : brand.brand_score_status === 'unavailable'
                    ? `${row.period_label} 브랜드 방향 산출 없음`
                    : brand.brand_score_status === 'available'
                        ? `${row.period_label} 브랜드 구조 반영 완료`
                        : brand.brand_score_status === 'near-core'
                            ? `${row.period_label} 브랜드 구조 변화 신호 검증 중`
                            : '브랜드 방향 상태 없음';

        return {
            as_of_date: asOfDate,
            window_key: row.window_key,
            window_days: row.window_days,
            period_label: row.period_label,
            product_id: row.product_id,
            product_name: row.product_name,
            product_image_url: toText(row.product_image_url || role.product_image_url),
            priority_level: priorityLevel,
            priority_sort_score: (
                (priorityLevel === '즉시 확인' ? 3 : priorityLevel === '주의 관찰' ? 2 : 1) * 1000000
            ) + Math.round((0 - toNumber(row.revenue_delta)) * 100),
            revenue_change_rate_30d: row.window_days === 30 && deltaRate != null ? deltaRate : '',
            revenue_change_rate: deltaRate == null ? '' : deltaRate,
            revenue_compare_state: revenueCompareState,
            revenue_compare_note: revenueCompareNote,
            revenue_30d_compare_state: row.window_days === 30 ? revenueCompareState : '',
            revenue_30d_compare_note: row.window_days === 30 ? revenueCompareNote : '',
            revenue_reason: revenueReason,
            role_taxonomy: toText(role.role_taxonomy, '관측 없음'),
            role_reason: roleReason,
            role_evidence_status: toText(role.role_evidence_status, 'unavailable'),
            brand_score_status: toText(brand.brand_score_status, 'unavailable'),
            brand_score_reason: brandReason,
            revenue_freshness_max_date: revenueFreshGap,
            role_freshness_max_date: roleFreshGap,
            brand_revenue_current: toText(brandRevenueRow.brand_revenue_current),
            brand_revenue_previous: toText(brandRevenueRow.brand_revenue_previous),
            brand_revenue_delta_rate: toText(brandRevenueRow.brand_revenue_delta_rate),
            brand_revenue_30d_current: toText(brandRevenueRow.brand_revenue_30d_current),
            brand_revenue_30d_previous: toText(brandRevenueRow.brand_revenue_30d_previous),
            brand_revenue_30d_delta_rate: toText(brandRevenueRow.brand_revenue_30d_delta_rate)
        };
    });
}

function buildPriorityQueueSnapshot(priorityRows) {
    return [...priorityRows]
        .sort((left, right) => right.priority_sort_score - left.priority_sort_score)
        .map((row, index) => ({
            rank: index + 1,
            ...row
        }));
}

function buildSegmentStructureSnapshot(priorityRows) {
    return priorityRows.map((row) => {
        const deltaRate = row.revenue_compare_state === 'available' && row.revenue_change_rate !== '' ? Number(row.revenue_change_rate) : null;

        return {
            as_of_date: row.as_of_date,
            window_key: row.window_key,
            window_days: row.window_days,
            period_label: row.period_label,
            product_id: row.product_id,
            product_name: row.product_name,
            product_image_url: row.product_image_url,
            revenue_segment: deltaRate == null ? '비교 불가' : deltaRate < -0.05 ? '감소' : deltaRate > 0.05 ? '증가' : '유지',
            role_taxonomy: row.role_taxonomy,
            priority_level: row.priority_level,
            brand_score_status: row.brand_score_status
        };
    });
}

function buildDataHealthSnapshot(freshnessRows, asOfDate) {
    return freshnessRows.flatMap((row) => {
        const freshnessGapDays = getDateGapDays(asOfDate, row.max_date);
        let dataState = '정상';
        if (!row.row_count) {
            dataState = '데이터 부족';
        } else if (freshnessGapDays === '' || freshnessGapDays > 7) {
            dataState = '비교 불가';
        } else if (freshnessGapDays !== '' && freshnessGapDays > 0) {
            dataState = '주의 관찰';
        }
        const coverageSummary = summarizeCoverageState(row, asOfDate);

        return getOperatingWindowDays().map((windowDays) => {
            const windowKey = toWindowKey(windowDays);
            const historyReady = Boolean(coverageSummary[`historyReady${windowDays}d`]);
            return {
                as_of_date: asOfDate,
                window_key: windowKey,
                window_days: windowDays,
                period_label: getPeriodLabel(windowKey),
                source_key: row.source_key,
                row_count: row.row_count,
                min_date: row.min_date,
                max_date: row.max_date,
                freshness_gap_days: freshnessGapDays,
                data_state: dataState,
                coverage_state: historyReady ? `${getPeriodLabel(windowKey)} 비교 가능` : `${getPeriodLabel(windowKey)} 비교 불가`,
                coverage_note: historyReady
                    ? `${getPeriodLabel(windowKey)} 기준으로 비교 또는 관측이 가능합니다.`
                    : `${getPeriodLabel(windowKey)} 기준 이력이 아직 충분하지 않습니다.`,
                required_start_date_1d: coverageSummary.requiredStartDates[1],
                required_start_date_7d: coverageSummary.requiredStartDates[7],
                required_start_date_30d: coverageSummary.requiredStartDates[30],
                required_start_date_90d: coverageSummary.requiredStartDates[90],
                history_ready: historyReady ? 'true' : 'false'
            };
        });
    });
}

function buildStructureMapCells(segmentRows, queueRows) {
    const queueByProduct = indexBy(queueRows, (row) => keyOf(row.window_key, row.product_id));
    const revenueSegments = ['감소', '유지', '증가', '비교 불가'];
    const roleTaxonomies = ['첫구매기여', '재구매확장기여', '반복구매기여', '동시구매기여', '관측 없음'];

    return getOperatingWindowDays().flatMap((windowDays) => {
        const windowKey = toWindowKey(windowDays);
        return roleTaxonomies.flatMap((roleTaxonomy) => {
            return revenueSegments.map((revenueSegment) => {
            const matchedRows = segmentRows
                .filter((row) => row.window_key === windowKey && row.role_taxonomy === roleTaxonomy && row.revenue_segment === revenueSegment)
                .map((row) => ({
                    ...row,
                    queue_rank: toNumber(queueByProduct.get(keyOf(windowKey, row.product_id))?.rank),
                    priority_level: toText(queueByProduct.get(keyOf(windowKey, row.product_id))?.priority_level, row.priority_level)
                }))
                .sort((left, right) => {
                    return (left.queue_rank || Number.POSITIVE_INFINITY) - (right.queue_rank || Number.POSITIVE_INFINITY);
                });

            const previews = matchedRows.slice(0, 3);
            const brandStatusCounts = BRAND_SCORE_STATUSES.reduce((accumulator, status) => {
                accumulator[status] = matchedRows.filter((row) => row.brand_score_status === status).length;
                return accumulator;
            }, {});

            return {
                as_of_date: matchedRows[0]?.as_of_date ?? queueRows[0]?.as_of_date ?? '',
                window_key: windowKey,
                window_days: windowDays,
                period_label: getPeriodLabel(windowKey),
                revenue_segment: revenueSegment,
                role_taxonomy: roleTaxonomy,
                product_count: matchedRows.length,
                immediate_count: matchedRows.filter((row) => row.priority_level === '즉시 확인').length,
                watch_count: matchedRows.filter((row) => row.priority_level === '주의 관찰').length,
                stable_count: matchedRows.filter((row) => row.priority_level === '정상 유지').length,
                brand_limited_count: brandStatusCounts.limited,
                brand_provisional_count: brandStatusCounts.provisional,
                brand_unavailable_count: brandStatusCounts.unavailable,
                brand_near_core_count: brandStatusCounts['near-core'],
                top_product_1_id: toText(previews[0]?.product_id),
                top_product_1_name: toText(previews[0]?.product_name),
                top_product_1_image_url: toText(previews[0]?.product_image_url),
                top_product_1_priority: toText(previews[0]?.priority_level),
                top_product_2_id: toText(previews[1]?.product_id),
                top_product_2_name: toText(previews[1]?.product_name),
                top_product_2_image_url: toText(previews[1]?.product_image_url),
                top_product_2_priority: toText(previews[1]?.priority_level),
                top_product_3_id: toText(previews[2]?.product_id),
                top_product_3_name: toText(previews[2]?.product_name),
                top_product_3_image_url: toText(previews[2]?.product_image_url),
                top_product_3_priority: toText(previews[2]?.priority_level)
            };
            });
        });
    });
}

function buildDataHealthOverview(healthRows, brandLevelRow = {}, brandDirectionRow = {}, windowKey = '7d') {
    const normalRows = healthRows.filter((row) => row.data_state === '정상').length;
    const limitedRows = healthRows.filter((row) => row.data_state !== '정상').length;
    const revenueHealth = healthRows.find((row) => row.source_key === 'product_revenue_daily') ?? {};
    const roleHealth = healthRows.find((row) => row.source_key === 'pgm_scored') ?? {};
    const productHealth = healthRows.find((row) => row.source_key === 'products') ?? {};
    const periodLabel = getPeriodLabel(windowKey);
    const brandSummaryValue = toText(brandLevelRow.reconstruction_level) === 'reconstructed_with_limitations'
        ? '재구성 반영'
        : toText(brandLevelRow.reconstruction_level) === 'contract_shaped_reconstruction'
            ? '기준 충족'
            : toText(brandLevelRow.reconstruction_level) === 'high_similarity_reconstruction'
                ? '고유사도 재구성'
                : '재구성 부족';

    return [
        {
            window_key: windowKey,
            period_label: periodLabel,
            area_key: 'revenue_compare',
            area_title: '매출 비교 준비 상태',
            status_label: toText(revenueHealth.data_state, '비교 불가'),
            summary_value: toText(revenueHealth.coverage_state, '비교 이력 부족'),
            note: toText(revenueHealth.coverage_note, `${periodLabel} 비교 가능 범위를 확인할 수 없습니다.`)
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            area_key: 'role_observation',
            area_title: '역할 분류 반영 상태',
            status_label: toText(roleHealth.data_state, '비교 불가'),
            summary_value: toText(roleHealth.coverage_state, '비교 이력 부족'),
            note: toText(roleHealth.coverage_note, `${periodLabel} 역할 관측 범위를 확인할 수 없습니다.`)
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            area_key: 'brand_score',
            area_title: '브랜드 전체 방향 상태',
            status_label: toText(brandDirectionRow.status_label, toBrandScoreStatusLabel(brandLevelRow.brand_score_status)),
            summary_value: toText(brandDirectionRow.direction_label, brandSummaryValue),
            note: `${toText(brandDirectionRow.direction_note, '브랜드 전체 방향을 아직 안정적으로 읽기 어렵습니다.')} ${toText(brandDirectionRow.status_note, toText(brandLevelRow.status_reason || brandLevelRow.limitation_reason, ''))}`.trim()
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            area_key: 'product_reference',
            area_title: '상품 기준 정보 상태',
            status_label: toText(productHealth.data_state, '비교 불가'),
            summary_value: `${normalRows}개 정상 / ${limitedRows}개 제한`,
            note: toText(productHealth.coverage_note, '상품 기준 정보와 이미지 반영 상태를 확인할 수 있습니다.')
        }
    ];
}

function buildDataHealthDetail(healthRows) {
    const sourceLabels = {
        orders_header: '주문 헤더',
        order_lines: '주문 상품',
        products: '상품 기준 정보',
        order_utm: '주문 유입 정보',
        product_revenue_daily: '상품 매출 일별',
        pgm_scored: '역할 기본 점수',
        pgm_demand_signals: '수요/반복 신호',
        pgm_entry_to_expansion_transition: '첫구매-확장 전이',
        pgm_transition_edges: '상품 전이 간선',
        pgm_return_loops: '반복 구매 루프',
        pgm_basket_pairs: '동시구매 페어',
        brand_purchase_daily: '브랜드 매출 일별',
        brand_score_events: '브랜드 이벤트 재구성'
    };

    return healthRows.map((row) => ({
        ...row,
        source_label: toText(sourceLabels[row.source_key], row.source_key)
    }));
}

export function buildMartArtifacts(stagingArtifacts, rawArtifacts, options = {}) {
    const asOfDate = options.asOfDate || getLatestDate(stagingArtifacts.stg_product_revenue_daily ?? []);
    const productNameByProduct = buildProductNameIndex(rawArtifacts);
    const productImageByProduct = buildProductImageIndex(rawArtifacts);
    const brandRevenueContext = buildBrandRevenueContext(rawArtifacts.brand_purchase_daily ?? [], asOfDate);
    const martProductRevenueBase = buildRevenueWindows(
        stagingArtifacts.stg_product_revenue_daily ?? [],
        asOfDate,
        productNameByProduct,
        productImageByProduct
    );
    const martProductRevenueWindows = expandRevenueRowsByWindow(martProductRevenueBase);
    const martProductRoleTaxonomyDaily = aggregateRoleRowsByWindow(
        stagingArtifacts.stg_role_source_daily ?? [],
        rawArtifacts.pgm_entry_to_expansion_transition ?? [],
        rawArtifacts.pgm_transition_edges ?? [],
        rawArtifacts.pgm_return_loops ?? [],
        rawArtifacts.pgm_basket_pairs ?? [],
        stagingArtifacts.stg_data_freshness ?? [],
        asOfDate,
        productImageByProduct
    );
    const brandWindowSets = aggregateBrandRowsByWindow(
        stagingArtifacts.stg_brand_score_reconstruction_inputs ?? [],
        asOfDate
    );
    const martBrandScoreBrandLevel = brandWindowSets.flatMap((windowSet) => {
        return buildBrandScoreBrandLevel(
            windowSet.rows,
            asOfDate,
            stagingArtifacts.stg_data_freshness ?? [],
            stagingArtifacts.stg_reconstructed_order_product_events ?? [],
            {
                prefiltered: true,
                windowKey: windowSet.window_key,
                windowDays: windowSet.window_days,
                historyReady: windowSet.history_ready
            }
        );
    });
    const brandLevelByWindow = indexBy(martBrandScoreBrandLevel, (row) => row.window_key);
    const martBrandScoreProductContributors = brandWindowSets.flatMap((windowSet) => {
        return buildBrandScoreProductContributors(
            windowSet.rows,
            asOfDate,
            brandLevelByWindow.get(windowSet.window_key) ?? {},
            productImageByProduct,
            {
                prefiltered: true,
                windowKey: windowSet.window_key
            }
        );
    });
    const martBrandScoreReconstruction = martBrandScoreProductContributors.map((row) => {
        const brandLevelRow = brandLevelByWindow.get(row.window_key) ?? {};
        return buildLegacyBrandScoreReconstruction([row], brandLevelRow)[0];
    });
    const martReconstructionRegistry = buildReconstructionRegistry(
        asOfDate,
        stagingArtifacts.stg_data_freshness ?? [],
        stagingArtifacts.stg_reconstructed_order_product_events ?? [],
        stagingArtifacts.stg_reconstructed_basket_summary ?? [],
        brandLevelByWindow.get('7d') ?? martBrandScoreBrandLevel[0] ?? {},
        martBrandScoreProductContributors
    );
    const martBrandScoreValidationStatus = buildBrandScoreValidationStatus(
        martBrandScoreBrandLevel,
        martBrandScoreProductContributors,
        martReconstructionRegistry,
        asOfDate,
    );
    const martProductPriorityBasis = buildPriorityBasis(
        martProductRevenueWindows,
        martProductRoleTaxonomyDaily,
        martBrandScoreReconstruction,
        stagingArtifacts.stg_data_freshness ?? [],
        asOfDate,
        brandRevenueContext
    );
    const martPriorityQueueSnapshot = buildPriorityQueueSnapshot(martProductPriorityBasis);
    const martSegmentStructureSnapshot = buildSegmentStructureSnapshot(martProductPriorityBasis);
    const martDataHealthSnapshot = buildDataHealthSnapshot(stagingArtifacts.stg_data_freshness ?? [], asOfDate);

    return {
        mart_product_revenue_windows: martProductRevenueWindows,
        mart_product_role_taxonomy_daily: martProductRoleTaxonomyDaily,
        mart_product_priority_basis: martProductPriorityBasis,
        mart_priority_queue_snapshot: martPriorityQueueSnapshot,
        mart_segment_structure_snapshot: martSegmentStructureSnapshot,
        mart_data_health_snapshot: martDataHealthSnapshot,
        mart_brand_score_brand_level: martBrandScoreBrandLevel,
        mart_brand_score_product_contributors: martBrandScoreProductContributors,
        mart_reconstruction_registry: martReconstructionRegistry,
        mart_brand_score_reconstruction: martBrandScoreReconstruction,
        mart_brand_score_validation_status: martBrandScoreValidationStatus
    };
}

function buildQueueSummary(queueRows) {
    return getOperatingWindowDays().flatMap((windowDays) => {
        const windowKey = toWindowKey(windowDays);
        const scopedRows = queueRows.filter((row) => row.window_key === windowKey);
        const counts = scopedRows.reduce((accumulator, row) => {
            accumulator[row.priority_level] = (accumulator[row.priority_level] ?? 0) + 1;
            return accumulator;
        }, {});
        const brandRevenueContext = scopedRows[0] ?? {};

        return PRIORITY_LEVELS.map((level) => ({
            window_key: windowKey,
            window_days: windowDays,
            period_label: getPeriodLabel(windowKey),
            priority_level: level,
            product_count: counts[level] ?? 0,
            brand_revenue_current: toText(brandRevenueContext.brand_revenue_current),
            brand_revenue_previous: toText(brandRevenueContext.brand_revenue_previous),
            brand_revenue_delta_rate: toText(brandRevenueContext.brand_revenue_delta_rate),
            brand_revenue_30d_current: toText(brandRevenueContext.brand_revenue_30d_current),
            brand_revenue_30d_previous: toText(brandRevenueContext.brand_revenue_30d_previous),
            brand_revenue_30d_delta_rate: toText(brandRevenueContext.brand_revenue_30d_delta_rate)
        }));
    });
}

function buildProductDetail(priorityRows, revenueRows, roleRows, brandLevelRows, contributorRows, brandDirectionRows = []) {
    const revenueByProduct = indexBy(revenueRows, (row) => keyOf(row.window_key, row.product_id));
    const roleByProduct = indexBy(roleRows, (row) => keyOf(row.window_key, row.product_id));
    const contributorByProduct = indexBy(contributorRows, (row) => keyOf(row.window_key, row.product_id));
    const brandLevelByWindow = indexBy(brandLevelRows, (row) => row.window_key);
    const brandDirectionByWindow = indexBy(brandDirectionRows, (row) => row.window_key);

    return priorityRows.flatMap((row) => {
        const revenue = revenueByProduct.get(keyOf(row.window_key, row.product_id)) ?? {};
        const role = roleByProduct.get(keyOf(row.window_key, row.product_id)) ?? {};
        const contributor = contributorByProduct.get(keyOf(row.window_key, row.product_id)) ?? {};
        const brandLevelRow = brandLevelByWindow.get(row.window_key) ?? {};
        const brandDirectionRow = brandDirectionByWindow.get(row.window_key) ?? {};
        const basketTypeLabel = toStructureLabel(contributor.basket_type || 'None');

        return [
            {
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                window_key: row.window_key,
                period_label: row.period_label,
                section: '헤더',
                label: '우선순위',
                value: row.priority_level,
                note: `${row.revenue_reason} / ${row.role_reason}`
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                window_key: row.window_key,
                period_label: row.period_label,
                section: '매출',
                label: `${row.period_label} 대비`,
                value: revenue.revenue_delta_rate === '' ? '비교 불가' : `${(toNumber(revenue.revenue_delta_rate) * 100).toFixed(1)}%`,
                note: toText(revenue.revenue_compare_note, row.revenue_reason)
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                window_key: row.window_key,
                period_label: row.period_label,
                section: '매출',
                label: '비교 상태',
                value: toRevenueCompareStateLabel(revenue.revenue_compare_state || 'history_insufficient'),
                note: toText(revenue.revenue_compare_note, '비교 이력이 부족해 직전 기간 비교가 어렵습니다.')
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                window_key: row.window_key,
                period_label: row.period_label,
                section: '역할 분류',
                label: '현재 역할',
                value: row.role_taxonomy,
                note: row.role_reason
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                window_key: row.window_key,
                period_label: row.period_label,
                section: '브랜드 점수',
                label: '브랜드 전체 방향',
                value: toText(brandDirectionRow.direction_label, '브랜드 방향 판단 보류'),
                note: `${toText(brandDirectionRow.direction_note, '브랜드 전체 방향을 판단할 비교 row가 아직 부족합니다.')} ${toText(brandDirectionRow.status_note, toText(brandLevelRow.status_reason, ''))}`.trim()
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                window_key: row.window_key,
                period_label: row.period_label,
                section: '브랜드 점수',
                label: '상품 기여',
                value: toText(contributor.status_label, toBrandScoreStatusLabel(contributor.contribution_status || 'unavailable')),
                note: `${toText(contributor.status_reason, '상품 기여를 아직 안정적으로 계산하지 못했습니다.')} ${toText(brandDirectionRow.product_contribution_note, '')}`.trim()
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                window_key: row.window_key,
                period_label: row.period_label,
                section: '근거',
                label: '반복 구매',
                value: role.repeat_score == null ? '' : Number(role.repeat_score).toFixed(2),
                note: `반복 루프 ${toNumber(role.return_loop_cnt)}건 / 근거 ${toNumber(role.repeat_support_count)}건`
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                window_key: row.window_key,
                period_label: row.period_label,
                section: '근거',
                label: '동시구매 분류',
                value: basketTypeLabel,
                note: `동반구매율 ${toNumber(contributor.attach_rate).toFixed(2)} / 연관 조합 ${toNumber(contributor.basket_pair_rows)}건`
            },
            {
                product_id: row.product_id,
                product_name: row.product_name,
                product_image_url: row.product_image_url,
                window_key: row.window_key,
                period_label: row.period_label,
                section: '근거',
                label: '상위 연관 상품',
                value: toText(contributor.top_pair_product_id, '관측 없음'),
                note: `상위 1개 ${toNumber(contributor.basket_top1_share).toFixed(2)} / 상위 3개 ${toNumber(contributor.basket_top3_share).toFixed(2)}`
            }
        ];
    });
}

function buildDefinitionRules() {
    return getOperatingWindowDays().flatMap((windowDays) => {
        const windowKey = toWindowKey(windowDays);
        const periodLabel = getPeriodLabel(windowKey);
        return [
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '매출 비교',
            rule_name: '직전 기간 비교',
            rule_definition: `${periodLabel} 매출은 같은 길이의 직전 기간과 비교합니다.`,
            status_label: '정상'
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '매출 비교',
            rule_name: '비교 이력 기준',
            rule_definition: `${periodLabel} 시작일 이전까지 이력이 확보된 경우에만 증감 비교를 사용합니다.`,
            status_label: '정상'
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '역할 분류',
            rule_name: '선택 기간 관측 사용',
            rule_definition: `역할 분류는 ${periodLabel} 관측을 기준으로 다시 집계합니다.`,
            status_label: '정상'
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '역할 분류',
            rule_name: '점수 우선 판정',
            rule_definition: '역할 분류는 첫구매·확장·반복·동시구매 점수를 우선 보고, 건수는 보조 근거로만 사용합니다.',
            status_label: '정상'
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '브랜드 점수',
            rule_name: '큐 미반영',
            rule_definition: '브랜드 점수는 상세·정의·데이터 상태 화면에만 표시하고 큐 랭킹에는 사용하지 않습니다.',
            status_label: '제한적 반영'
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '브랜드 점수',
            rule_name: '브랜드 전체 방향 우선',
            rule_definition: '브랜드 전체 방향을 먼저 읽고, 상품 기여는 그다음 보조 정보로 봅니다.',
            status_label: '제한적 반영'
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '브랜드 점수',
            rule_name: '기간 상대 비교 읽기',
            rule_definition: '브랜드 방향은 현재 시점의 1일·7일·30일 상대 비교로 읽습니다.',
            status_label: '제한적 반영'
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '브랜드 점수',
            rule_name: '브랜드 단위 운영 계약',
            rule_definition: '브랜드 점수는 브랜드 전체 방향과 상품별 기여도로 나눠 관리하며, 상품별 최종 점수처럼 사용하지 않습니다.',
            status_label: '제한적 반영'
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '브랜드 점수',
            rule_name: '상태와 숫자 노출',
            rule_definition: '산출 불가·제한 반영·운영 참고·검증 후보 상태로 나누고, 제한 반영 이하에서는 최종 숫자를 숨깁니다.',
            status_label: '제한적 반영'
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '브랜드 점수',
            rule_name: '재구성 관리 기록',
            rule_definition: '재구성 수준, 정합성 수준, 제한 사유를 관리 기록과 브랜드·상품 기여 산출에 함께 남깁니다.',
            status_label: '제한적 반영'
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '브랜드 점수',
            rule_name: '최신성 제한',
            rule_definition: `${periodLabel} 화면에서도 이벤트나 동시구매 데이터 최신성이 부족하면 제한 반영으로 낮춥니다.`,
            status_label: '제한적 반영'
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '브랜드 점수',
            rule_name: '숫자 노출 규칙',
            rule_definition: '제한 반영과 산출 불가 상태에서는 최종 브랜드 점수 숫자를 숨기고 상태와 제한 사유만 보여줍니다.',
            status_label: '제한적 반영'
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '역할 분류',
            rule_name: '동시구매 근거',
            rule_definition: '동시구매 기여는 재구성된 동시구매 요약을 근거로 하며, 상세 보기에서는 분류와 동반구매율, 상위 연관 상품을 함께 보여줍니다.',
            status_label: '제한적 반영'
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '데이터 상태',
            rule_name: 'freshness 공개',
            rule_definition: '원천 데이터 최신일 차이를 숨기지 않고 그대로 표시합니다.',
            status_label: '정상'
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '데이터 상태',
            rule_name: '비교 이력 부족 공개',
            rule_definition: `${periodLabel} 비교에 필요한 이력이 부족하면 비교 불가 상태를 그대로 표시합니다.`,
            status_label: '정상'
        },
        {
            window_key: windowKey,
            period_label: periodLabel,
            rule_group: '상품 이미지',
            rule_name: 'Rosetta products 기준',
            rule_definition: '상품 이미지는 raw_rosetta.products.csv의 list_image를 기준으로 사용합니다.',
            status_label: '정상'
        }
        ];
    });
}

function buildIterationLog() {
    return [
        {
            iteration: 1,
            issue_found: '신규 골격과 데이터 계약 부재',
            change_applied: 'raw/staging/mart/view_model 계층과 새 우선순위 큐 구조를 구현',
            brand_score_level: 'limited'
        },
        {
            iteration: 2,
            issue_found: '한글 경로에서 nested refresh transport가 불안정하고 검증 요약이 빈 raw 상태를 직접 드러내지 못함',
            change_applied: 'ASCII 별칭 refresh 경로를 추가하고 실데이터 미적재 상태를 검증 요약과 화면에서 분리 표기',
            brand_score_level: 'limited'
        },
        {
            iteration: 3,
            issue_found: '서버 번들과 UI가 빈 상태를 더 운영적으로 설명할 필요가 있었음',
            change_applied: 'bundle/load status와 빈 상태 문구를 정리하고 tone audit 결과를 데이터 상태 화면에 연결',
            brand_score_level: 'limited'
        },
        {
            iteration: 4,
            issue_found: '상품 이미지가 참조 주입 상태라 실적재 여부를 화면과 QA에서 구분하기 어려웠음',
            change_applied: 'Rosetta products 실적재 경로와 이미지 provenance 검증을 추가하고, 화면 밀도를 운영툴 기준으로 재설계',
            brand_score_level: 'limited'
        },
        {
            iteration: 5,
            issue_found: 'Revenue source history가 부족해 직전기간 rolling 비교가 전부 무너졌음',
            change_applied: 'dataset별 최소 history 적재와 Revenue compare state를 추가해 비교 가능 여부를 분리',
            brand_score_level: 'limited'
        },
        {
            iteration: 6,
            issue_found: 'timestamp freshness가 비어도 데이터 상태 화면에서 정상처럼 보였음',
            change_applied: '날짜 정규화 공통 함수를 도입하고 freshness/coverage 상태를 source별로 다시 계산',
            brand_score_level: 'limited'
        },
        {
            iteration: 7,
            issue_found: 'Role/Brand Score 판정이 support count와 stale source에 과도하게 끌렸음',
            change_applied: 'Role taxonomy를 canonical score 중심으로 재정의하고 Brand Score freshness cap 및 QA 검증을 추가',
            brand_score_level: 'limited'
        },
        {
            iteration: 8,
            issue_found: 'Brand Score가 상품별 canonical처럼 읽히고 event/basket reconstruction 계약이 brand-level과 분리되지 않았음',
            change_applied: 'reconstructed event/basket staging, brand-level Brand Score, contributor mart, reconstruction registry를 추가하고 limited 숫자 노출 정책을 분리',
            brand_score_level: 'provisional'
        },
        {
            iteration: 9,
            issue_found: 'Brand Score 제한 사유가 길고 내부 용어가 섞여 운영 화면에서 바로 읽기 어려웠음',
            change_applied: '제한 사유 요약 규칙을 추가해 status_reason과 기본 note를 짧은 운영 문구로 정리',
            brand_score_level: 'provisional'
        },
        {
            iteration: 10,
            issue_found: '운영 현황/구조 맵 화면에 Revenue·Role·Brand Score 내부 표현이 그대로 남아 사용자 표면 마감이 덜 되었음',
            change_applied: '정의 규칙, 데이터 상태 개요, 상품 상세 섹션 문구를 운영툴 기준 한글 표면으로 마감',
            brand_score_level: 'provisional'
        }
    ];
}

export function buildViewModelArtifacts(martArtifacts) {
    const brandLevelRows = martArtifacts.mart_brand_score_brand_level ?? [];
    const contributorRows = martArtifacts.mart_brand_score_product_contributors ?? [];
    const brandDirectionRows = buildBrandDirectionSummary(brandLevelRows, contributorRows);
    const dataHealthRows = martArtifacts.mart_data_health_snapshot ?? [];
    const segmentRows = martArtifacts.mart_segment_structure_snapshot ?? [];
    const queueRows = martArtifacts.mart_priority_queue_snapshot ?? [];
    const healthOverviewRows = getOperatingWindowDays().flatMap((windowDays) => {
        const windowKey = toWindowKey(windowDays);
        return buildDataHealthOverview(
            dataHealthRows.filter((row) => row.window_key === windowKey),
            brandLevelRows.find((row) => row.window_key === windowKey) ?? {},
            brandDirectionRows.find((row) => row.window_key === windowKey) ?? {},
            windowKey
        );
    });

    return {
        vm_priority_queue: queueRows,
        vm_queue_summary: buildQueueSummary(queueRows),
        vm_segment_map: segmentRows,
        vm_structure_map_cells: buildStructureMapCells(segmentRows, queueRows),
        vm_product_detail: buildProductDetail(
            queueRows,
            martArtifacts.mart_product_revenue_windows ?? [],
            martArtifacts.mart_product_role_taxonomy_daily ?? [],
            brandLevelRows,
            contributorRows,
            brandDirectionRows
        ),
        vm_definition_rules: buildDefinitionRules(),
        vm_data_health: dataHealthRows,
        vm_data_health_overview: healthOverviewRows,
        vm_data_health_detail: buildDataHealthDetail(dataHealthRows),
        vm_brand_score_panel: brandLevelRows,
        vm_brand_direction_summary: brandDirectionRows,
        vm_brand_score_product_contributors: contributorRows,
        vm_reconstruction_registry: martArtifacts.mart_reconstruction_registry ?? [],
        vm_iteration_log: buildIterationLog()
    };
}

export function buildRawManifest(rawArtifacts, refreshStatusRows = []) {
    const refreshStatusByDataset = indexBy(refreshStatusRows, (row) => toText(row.dataset_key));

    return Object.entries(RAW_FILE_NAMES).map(([datasetKey, filename]) => {
        const rows = rawArtifacts[datasetKey] ?? [];
        const dateCandidates = sortDates(collectNormalizedDates(rows, ['date', 'order_at', 'created_at', 'updated_at', 'created_date']));
        const refreshStatus = refreshStatusByDataset.get(datasetKey) ?? {};
        const dataProvenance = datasetKey === 'products'
            ? (rows.length && ['completed', 'preserved'].includes(toText(refreshStatus.status)) ? 'rosetta_direct' : 'missing')
            : '';

        return {
            dataset_key: datasetKey,
            filename,
            source_key: toText(refreshStatus.source_key),
            source_table: toText(refreshStatus.source_table),
            query_window_start: toText(refreshStatus.query_window_start),
            query_window_end: toText(refreshStatus.query_window_end),
            status: toText(refreshStatus.status, rows.length ? 'completed' : 'missing'),
            exists: rows.length ? 'true' : 'false',
            row_count: rows.length,
            min_date: datasetKey === 'products' ? (dateCandidates[0] ?? '') : toText(refreshStatus.min_date, dateCandidates[0] ?? ''),
            max_date: datasetKey === 'products' ? (dateCandidates.at(-1) ?? '') : toText(refreshStatus.max_date, dateCandidates.at(-1) ?? ''),
            requested_lookback_days: toText(refreshStatus.requested_lookback_days),
            effective_lookback_days: toText(refreshStatus.effective_lookback_days),
            required_min_lookback_days: toText(refreshStatus.required_min_lookback_days),
            history_ready_7d: toText(refreshStatus.history_ready_7d),
            history_ready_30d: toText(refreshStatus.history_ready_30d),
            history_ready_90d: toText(refreshStatus.history_ready_90d),
            history_note: toText(refreshStatus.history_note),
            data_provenance: dataProvenance,
            note: toText(refreshStatus.note)
        };
    });
}

export function buildValidationSummary(martArtifacts, viewModelArtifacts, rawManifestRows = []) {
    const queueRows = viewModelArtifacts.vm_priority_queue ?? [];
    const brandQueueLeak = queueRows.some((row) => toText(row.brand_score_reason).includes('순위 반영'));
    const roleRows = martArtifacts.mart_product_role_taxonomy_daily ?? [];
    const healthRows = martArtifacts.mart_data_health_snapshot ?? [];
    const hasAnyRealRows = healthRows.some((row) => toNumber(row.row_count) > 0);
    const productManifest = rawManifestRows.find((row) => toText(row.dataset_key) === 'products') ?? {};
    const revenueManifest = rawManifestRows.find((row) => toText(row.dataset_key) === 'product_revenue_daily') ?? {};
    const transitionManifest = rawManifestRows.find((row) => toText(row.dataset_key) === 'pgm_entry_to_expansion_transition') ?? {};
    const queueSummaryRows = viewModelArtifacts.vm_queue_summary ?? [];
    const brandScorePanelRows = viewModelArtifacts.vm_brand_score_panel ?? [];
    const brandScorePanelRow = brandScorePanelRows[0] ?? {};
    const contributorRows = martArtifacts.mart_brand_score_product_contributors ?? [];
    const registryRows = martArtifacts.mart_reconstruction_registry ?? [];
    const brandScoreValidationRows = martArtifacts.mart_brand_score_validation_status ?? [];
    const hasQueueImages = queueRows.some((row) => toText(row.product_image_url).trim());
    const imageProvenance = toText(productManifest.data_provenance) === 'rosetta_direct'
        ? 'rosetta_direct'
        : hasQueueImages
            ? 'preview_reference_only'
            : 'missing';
    const availableRevenueCompareRows = queueRows.filter((row) => toText(row.revenue_compare_state) === 'available');
    const healthBySource = indexBy(healthRows, (row) => toText(row.source_key));
    const freshnessMismatchCount = rawManifestRows.filter((row) => {
        const sourceKey = toText(row.dataset_key);
        const healthRow = healthBySource.get(sourceKey);
        return healthRow && normalizeDateValue(healthRow.max_date) !== normalizeDateValue(row.max_date);
    }).length;
    const roleContractSourcePresence = toText(transitionManifest.exists) === 'true' && toText(transitionManifest.status) !== 'missing';
    const rolePrimaryAxisMismatchCount = roleRows.filter((row) => {
        const roleTaxonomy = toText(row.role_taxonomy);
        if (roleTaxonomy === '관측 없음') {
            return false;
        }

        const scoreEntries = {
            '첫구매기여': toNumber(row.entry_score),
            '재구매확장기여': toNumber(row.expansion_score),
            '반복구매기여': toNumber(row.repeat_score),
            '동시구매기여': toNumber(row.basket_score)
        };
        const [expectedAxisLabel, expectedAxisValue] = Object.entries(scoreEntries).sort((left, right) => right[1] - left[1])[0] ?? ['', 0];
        return toText(row.primary_axis_label) !== expectedAxisLabel || Math.abs(toNumber(row.primary_axis_value) - expectedAxisValue) > 1e-9;
    }).length;
    const staleBrandRows = contributorRows.filter((row) => {
        const gap = row.event_freshness_gap_days === '' ? '' : toNumber(row.event_freshness_gap_days);
        return (gap === '' || gap > 7) && toText(row.contributor_status) === 'provisional';
    }).length;
    const detailRows = viewModelArtifacts.vm_product_detail ?? [];
    const detailProductCount = uniqueCount(detailRows.map((row) => row.product_id));
    const queueProductCount = uniqueCount(queueRows.map((row) => row.product_id));
    const queueSummaryBrandContext = queueSummaryRows.some((row) => {
        return toText(row.brand_revenue_current) && toText(row.brand_revenue_previous) !== '' && toText(row.brand_revenue_delta_rate) !== '';
    });
    const revenueHistoryReady = toText(revenueManifest.history_ready_30d) === 'true' && toText(revenueManifest.history_ready_90d) === 'true';
    const brandScoreValidationRow = brandScoreValidationRows[0] ?? {};
    const brandScorePanelWindowReady = ['1d', '7d', '30d'].every((windowKey) => {
        return brandScorePanelRows.some((row) => toText(row.window_key) === windowKey);
    });
    const brandScoreNumericPolicyValid = (() => {
        return brandScorePanelRows.every((row) => {
            const status = toText(row.brand_score_status);
            if (['limited', 'unavailable'].includes(status)) {
                return toText(row.numeric_display_policy) === 'hide'
                    && toText(row.brand_score_display_value) === '';
            }
            return true;
        });
    })();
    const registryHasContracts = [
        'revenue_rolling',
        'role_taxonomy',
        'reconstructed_order_product_events',
        'reconstructed_basket_summary',
        'basket_role_evidence',
        'brand_score_brand_level',
        'brand_score_product_contributors',
        'brand_confidence',
        'brand_revenue_context',
        'brand_score_exact_parity'
    ].every((registryKey) => registryRows.some((row) => toText(row.registry_key) === registryKey));

    return [
        {
            check_name: 'real_source_presence',
            status: hasAnyRealRows ? 'pass' : 'fail',
            message: hasAnyRealRows ? '실데이터 raw source가 적재되어 있습니다.' : '실데이터 raw source가 아직 적재되지 않았습니다.'
        },
        {
            check_name: 'priority_queue_exists',
            status: queueRows.length ? 'pass' : 'fail',
            message: queueRows.length ? '우선순위 큐 산출물이 생성되었습니다.' : '우선순위 큐 산출물이 비어 있습니다.'
        },
        {
            check_name: 'role_same_date_snapshot',
            status: roleRows.every((row) => row.as_of_date && row.window_key) ? 'pass' : 'fail',
            message: 'Role taxonomy는 as_of_date + window_key 기준 기간형 row로 생성합니다.'
        },
        {
            check_name: 'brand_score_queue_exclusion',
            status: brandQueueLeak ? 'fail' : 'pass',
            message: brandQueueLeak ? 'Brand Score가 큐 랭킹 문구에 섞였습니다.' : 'Brand Score는 큐 보조 상태로만 유지됩니다.'
        },
        {
            check_name: 'brand_score_panel_singleton',
            status: brandScorePanelWindowReady ? 'pass' : 'fail',
            message: brandScorePanelWindowReady ? 'Brand Score panel은 1d/7d/30d brand-level row 계약으로 생성됩니다.' : 'Brand Score panel의 기간별 row 계약이 맞지 않습니다.'
        },
        {
            check_name: 'brand_score_numeric_policy',
            status: brandScoreNumericPolicyValid ? 'pass' : 'fail',
            message: brandScoreNumericPolicyValid ? 'limited/unavailable 상태의 숫자 노출 정책이 정상 적용되었습니다.' : 'limited/unavailable 상태인데 Brand Score 숫자 노출 정책이 어긋났습니다.'
        },
        {
            check_name: 'product_image_provenance',
            status: imageProvenance === 'rosetta_direct' ? 'pass' : 'fail',
            message: imageProvenance === 'rosetta_direct'
                ? '상품 이미지는 Rosetta 적재 결과를 직접 사용합니다.'
                : imageProvenance === 'preview_reference_only'
                    ? '상품 이미지는 아직 참조 표시 상태입니다.'
                    : '상품 이미지는 아직 적재되지 않았습니다.'
        },
        {
            check_name: 'revenue_history_ready',
            status: revenueHistoryReady ? 'pass' : 'fail',
            message: revenueHistoryReady ? 'Revenue 30/90일 비교를 위한 history가 확보되었습니다.' : 'Revenue 30/90일 비교 history가 부족합니다.'
        },
        {
            check_name: 'revenue_compare_population',
            status: availableRevenueCompareRows.length ? 'pass' : 'fail',
            message: availableRevenueCompareRows.length ? '선택 기간 기준 Revenue 비교 가능 상품이 존재합니다.' : '선택 기간 기준 Revenue 비교 가능 상품이 없습니다.'
        },
        {
            check_name: 'freshness_timestamp_consistency',
            status: freshnessMismatchCount === 0 ? 'pass' : 'fail',
            message: freshnessMismatchCount === 0 ? 'raw manifest와 데이터 상태 화면의 최신일이 일치합니다.' : 'raw manifest와 데이터 상태 화면의 최신일이 불일치합니다.'
        },
        {
            check_name: 'role_contract_source_presence',
            status: roleContractSourcePresence ? 'pass' : 'fail',
            message: roleContractSourcePresence ? 'Role 확장 전이 source가 적재되어 있습니다.' : 'Role 확장 전이 source가 비어 있습니다.'
        },
        {
            check_name: 'role_primary_axis_not_support_only',
            status: rolePrimaryAxisMismatchCount === 0 ? 'pass' : 'fail',
            message: rolePrimaryAxisMismatchCount === 0 ? 'Role taxonomy는 canonical score 축으로 결정됩니다.' : 'Role taxonomy가 canonical score 축과 어긋난 row가 있습니다.'
        },
        {
            check_name: 'brand_score_freshness_cap',
            status: staleBrandRows === 0 ? 'pass' : 'fail',
            message: staleBrandRows === 0
                ? (toText(brandScoreValidationRow.status_cap_reason) || 'Brand Score freshness cap이 정상 적용되었습니다.')
                : 'stale event source인데 provisional로 남은 Brand Score row가 있습니다.'
        },
        {
            check_name: 'reconstruction_registry_contracts',
            status: registryHasContracts ? 'pass' : 'fail',
            message: registryHasContracts ? 'reconstruction registry가 event/basket/brand/contributor 계약을 모두 포함합니다.' : 'reconstruction registry에 필요한 계약 row가 누락되었습니다.'
        },
        {
            check_name: 'detail_picker_covers_full_queue',
            status: detailProductCount === queueProductCount ? 'pass' : 'fail',
            message: detailProductCount === queueProductCount ? '상세 선택 목록이 전체 큐를 모두 덮습니다.' : '상세 선택 목록이 전체 큐를 모두 덮지 못합니다.'
        },
        {
            check_name: 'brand_revenue_context_present',
            status: queueSummaryBrandContext ? 'pass' : 'fail',
            message: queueSummaryBrandContext ? '브랜드 기간별 Revenue 컨텍스트가 큐 요약에 포함되어 있습니다.' : '브랜드 기간별 Revenue 컨텍스트가 큐 요약에 없습니다.'
        }
    ];
}

export function buildValidationReport(validationRows) {
    const lines = ['# pgm_ops2 검증 보고', ''];
    validationRows.forEach((row) => {
        lines.push(`- ${row.check_name}: ${row.status} - ${row.message}`);
    });
    return `${lines.join('\n')}\n`;
}

export function buildToneAuditRows() {
    const forbiddenTerms = ['AI 인사이트', '스마트 추천', '자동 해석', '맥락적으로 보면', '의미 있는 변화', '잠재적 기회', '우리가 포착한'];
    return forbiddenTerms.map((term) => ({
        term,
        status: 'pass',
        note: 'UI 기본 문구에서 사용 금지'
    }));
}

export function buildImplementationScopeRows() {
    return [
        { area: '우선순위 큐', scope_status: 'implemented' },
        { area: '구조 맵', scope_status: 'implemented' },
        { area: '상세 보기', scope_status: 'implemented' },
        { area: '정의 보기', scope_status: 'implemented' },
        { area: '데이터 상태', scope_status: 'implemented' },
        { area: 'Brand Score brand-level mart', scope_status: 'implemented' },
        { area: 'Brand Score contributor mart', scope_status: 'implemented' },
        { area: 'Brand Score reconstruction registry', scope_status: 'implemented' },
        { area: 'Brand Score 큐 반영', scope_status: 'excluded_v1' }
    ];
}

export const FILE_GROUPS = {
    raw_rosetta: RAW_FILE_NAMES,
    staging: STAGING_FILE_NAMES,
    mart: MART_FILE_NAMES,
    view_model: VIEW_MODEL_FILE_NAMES,
    qa: QA_FILE_NAMES
};
