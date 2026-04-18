export function normalizeNull(value) {
    if (value == null) {
        return null;
    }

    const stringValue = String(value).trim();

    if (!stringValue || ['null', 'undefined', 'n/a', '-'].includes(stringValue.toLowerCase())) {
        return null;
    }

    return stringValue;
}

export function safeDivide(numerator, denominator) {
    if (denominator == null || denominator === 0) {
        return null;
    }

    return numerator / denominator;
}

export function asBooleanString(value) {
    if (typeof value === 'boolean') {
        return value ? 'true' : 'false';
    }

    const normalized = normalizeNull(value);

    if (normalized == null) {
        return 'false';
    }

    if (['true', '1', 'y', 'yes'].includes(normalized.toLowerCase())) {
        return 'true';
    }

    return 'false';
}
