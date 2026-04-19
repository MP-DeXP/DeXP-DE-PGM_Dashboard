function toUtcDate(date) {
    return new Date(`${date}T00:00:00Z`);
}

export function normalizeDateValue(value) {
    const text = String(value ?? '').trim();
    const candidate = text.slice(0, 10);
    return /^\d{4}-\d{2}-\d{2}$/.test(candidate) ? candidate : '';
}

export function isIsoDate(value) {
    return normalizeDateValue(value) !== '';
}

export function shiftDate(date, offsetDays) {
    if (!isIsoDate(date)) {
        return '';
    }

    const next = toUtcDate(date);
    next.setUTCDate(next.getUTCDate() + offsetDays);
    return next.toISOString().slice(0, 10);
}

export function listDateRange(startDate, endDate) {
    if (!isIsoDate(startDate) || !isIsoDate(endDate) || startDate > endDate) {
        return [];
    }

    const dates = [];
    let cursor = startDate;

    while (cursor <= endDate) {
        dates.push(cursor);
        cursor = shiftDate(cursor, 1);
    }

    return dates;
}

export function getLatestDate(rows, key = 'date') {
    return rows
        .map((row) => normalizeDateValue(row[key]))
        .filter((value) => value)
        .sort((left, right) => right.localeCompare(left))[0] ?? '';
}

export function getDateGapDays(asOfDate, observedDate) {
    const normalizedAsOfDate = normalizeDateValue(asOfDate);
    const normalizedObservedDate = normalizeDateValue(observedDate);

    if (!normalizedAsOfDate || !normalizedObservedDate) {
        return '';
    }

    return Math.max(0, Math.round((toUtcDate(normalizedAsOfDate) - toUtcDate(normalizedObservedDate)) / 86400000));
}
