import { asBooleanString, normalizeNull } from './null_handling.js';

function normalizeDateValue(value) {
    const normalized = normalizeNull(value);

    if (normalized == null) {
        return null;
    }

    if (/^\d{4}-\d{2}-\d{2}$/.test(normalized)) {
        return normalized;
    }

    const maybeDate = new Date(normalized);

    if (Number.isNaN(maybeDate.getTime())) {
        return normalized;
    }

    return maybeDate.toISOString().slice(0, 10);
}

function normalizeNumber(value) {
    const normalized = normalizeNull(value);

    if (normalized == null) {
        return null;
    }

    const numericValue = Number(normalized);
    return Number.isFinite(numericValue) ? numericValue : null;
}

export function normalizeRows(rows, config = {}) {
    return rows.map((row) => {
        const nextRow = {};

        Object.entries(row).forEach(([key, value]) => {
            if (config.idFields?.includes(key)) {
                nextRow[key] = normalizeNull(value);
                return;
            }

            if (config.dateFields?.includes(key)) {
                nextRow[key] = normalizeDateValue(value);
                return;
            }

            if (config.numberFields?.includes(key)) {
                nextRow[key] = normalizeNumber(value);
                return;
            }

            if (config.booleanFields?.includes(key)) {
                nextRow[key] = asBooleanString(value);
                return;
            }

            nextRow[key] = normalizeNull(value);
        });

        return nextRow;
    });
}
