function compareDateStrings(left, right) {
    return left.localeCompare(right);
}

function addDays(dateString, amount) {
    const [year, month, day] = dateString.split('-').map(Number);
    const date = new Date(Date.UTC(year, month - 1, day));
    date.setUTCDate(date.getUTCDate() + amount);
    return date.toISOString().slice(0, 10);
}

export function getLatestDate(rows, field = 'date') {
    return rows.reduce((latest, row) => {
        const value = row[field];
        if (!value) {
            return latest;
        }
        return !latest || value > latest ? value : latest;
    }, null);
}

export function listDateRange(startDate, endDate) {
    if (!startDate || !endDate) {
        return [];
    }

    const range = [];
    let cursor = startDate;

    while (cursor <= endDate) {
        range.push(cursor);
        cursor = addDays(cursor, 1);
    }

    return range;
}

export function enrichRowsWithRollingRevenue(rows, keyFields = []) {
    const grouped = new Map();

    rows.forEach((row) => {
        const key = keyFields.map((field) => row[field] ?? '').join('|');
        if (!grouped.has(key)) {
            grouped.set(key, []);
        }
        grouped.get(key).push(row);
    });

    const enriched = [];

    grouped.forEach((groupRows) => {
        const sortedRows = [...groupRows].sort((left, right) => compareDateStrings(left.date, right.date));
        const minDate = sortedRows[0]?.date;
        const maxDate = sortedRows[sortedRows.length - 1]?.date;
        const range = listDateRange(minDate, maxDate);
        const valueByDate = new Map(sortedRows.map((row) => [row.date, Number(row.revenue ?? 0)]));

        const rollingByDate = new Map();

        range.forEach((date, index) => {
            const previousDate = index > 0 ? range[index - 1] : null;
            const previousValue = previousDate ? valueByDate.get(previousDate) ?? 0 : null;
            const buildWindow = (days) => {
                let total = 0;
                for (let offset = 1; offset <= days; offset += 1) {
                    const targetDate = range[index - offset];
                    if (!targetDate) {
                        continue;
                    }
                    total += valueByDate.get(targetDate) ?? 0;
                }
                return total;
            };

            const currentValue = valueByDate.get(date) ?? 0;
            rollingByDate.set(date, {
                previous_value: previousValue,
                revenue_day_over_day_change_rate: previousValue == null || previousValue === 0
                    ? null
                    : (currentValue - previousValue) / previousValue,
                revenue_7d: buildWindow(7),
                revenue_30d: buildWindow(30),
                revenue_90d: buildWindow(90)
            });
        });

        sortedRows.forEach((row) => {
            enriched.push({
                ...row,
                ...rollingByDate.get(row.date)
            });
        });
    });

    return enriched;
}

export function computeWindowComparison(rows, valueField, latestDate, windowDays) {
    if (!rows.length || !latestDate) {
        return {
            current: 0,
            previous: 0,
            deltaRate: null
        };
    }

    const range = listDateRange(rows[0].date, latestDate);
    const valueByDate = new Map(rows.map((row) => [row.date, Number(row[valueField] ?? 0)]));
    const latestIndex = range.indexOf(latestDate);

    const sumWindow = (startOffset, endOffset) => {
        let total = 0;
        for (let offset = startOffset; offset <= endOffset; offset += 1) {
            const targetDate = range[latestIndex - offset];
            if (!targetDate) {
                continue;
            }
            total += valueByDate.get(targetDate) ?? 0;
        }
        return total;
    };

    const current = sumWindow(1, windowDays);
    const previous = sumWindow(windowDays + 1, windowDays * 2);

    return {
        current,
        previous,
        deltaRate: previous === 0 ? null : (current - previous) / previous
    };
}
