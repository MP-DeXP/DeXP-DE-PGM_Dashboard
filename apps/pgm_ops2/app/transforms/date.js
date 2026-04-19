function toUtcDate(date) {
    return new Date(`${date}T00:00:00Z`);
}

export function isIsoDate(value) {
    return /^\d{4}-\d{2}-\d{2}$/.test(String(value ?? ''));
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
        .map((row) => String(row[key] ?? ''))
        .filter((value) => isIsoDate(value))
        .sort((left, right) => right.localeCompare(left))[0] ?? '';
}
