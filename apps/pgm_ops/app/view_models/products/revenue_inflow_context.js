import { getLatestDate } from '../../transforms/base/date_windows.js';
import { safeDivide } from '../../transforms/base/null_handling.js';

function subtractDays(dateString, amount) {
    const [year, month, day] = String(dateString).split('-').map(Number);
    const date = new Date(Date.UTC(year, month - 1, day));
    date.setUTCDate(date.getUTCDate() - amount);
    return date.toISOString().slice(0, 10);
}

function sumBy(rows, valueGetter) {
    return rows.reduce((sum, row) => sum + Number(valueGetter(row) ?? 0), 0);
}

function pickTopDimension(rows, field) {
    const grouped = rows.reduce((map, row) => {
        const key = row[field] ?? '미지정';
        if (!map.has(key)) {
            map.set(key, []);
        }
        map.get(key).push(row);
        return map;
    }, new Map());

    const ranked = [...grouped.entries()]
        .map(([value, groupRows]) => ({
            value,
            revenue: sumBy(groupRows, (row) => row.purchase_amount)
        }))
        .sort((left, right) => right.revenue - left.revenue);

    return ranked[0] ?? { value: '미지정', revenue: 0 };
}

export function buildRevenueInflowContext(stgOrderWithUtm) {
    const latestDate = getLatestDate(stgOrderWithUtm);
    if (!latestDate) {
        return [];
    }

    const windowStart = subtractDays(latestDate, 29);
    const windowRows = stgOrderWithUtm.filter((row) => row.date >= windowStart && row.date <= latestDate);
    if (!windowRows.length) {
        return [];
    }

    const attributedRows = windowRows.filter((row) => row.utm_source || row.utm_medium || row.utm_campaign);
    const totalRevenue = sumBy(windowRows, (row) => row.purchase_amount);
    const attributedRevenue = sumBy(attributedRows, (row) => row.purchase_amount);
    const topSource = pickTopDimension(attributedRows, 'utm_source');
    const topMedium = pickTopDimension(attributedRows, 'utm_medium');
    const topCampaign = pickTopDimension(attributedRows, 'utm_campaign');

    return [
        {
            context_key: 'utm_attributed_order_share_30d',
            label: '최근 30일 UTM 식별 주문 비중',
            value: safeDivide(attributedRows.length, windowRows.length),
            detail: `식별 주문 ${attributedRows.length}건 / 전체 ${windowRows.length}건`,
            as_of_date: latestDate
        },
        {
            context_key: 'utm_attributed_revenue_share_30d',
            label: '최근 30일 UTM 식별 매출 비중',
            value: safeDivide(attributedRevenue, totalRevenue),
            detail: `식별 매출 ${Math.round(attributedRevenue).toLocaleString('ko-KR')}원`,
            as_of_date: latestDate
        },
        {
            context_key: 'top_utm_source_30d',
            label: '상위 유입 source',
            value: topSource.value,
            detail: `최근 30일 기여 매출 ${Math.round(topSource.revenue).toLocaleString('ko-KR')}원`,
            as_of_date: latestDate
        },
        {
            context_key: 'top_utm_medium_30d',
            label: '상위 유입 medium',
            value: topMedium.value,
            detail: `최근 30일 기여 매출 ${Math.round(topMedium.revenue).toLocaleString('ko-KR')}원`,
            as_of_date: latestDate
        },
        {
            context_key: 'top_utm_campaign_30d',
            label: '상위 campaign',
            value: topCampaign.value,
            detail: `최근 30일 기여 매출 ${Math.round(topCampaign.revenue).toLocaleString('ko-KR')}원`,
            as_of_date: latestDate
        }
    ];
}
