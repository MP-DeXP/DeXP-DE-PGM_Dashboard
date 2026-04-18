function escapeCsvCell(value) {
    const stringValue = value == null ? '' : String(value);

    if (/[",\n]/.test(stringValue)) {
        return `"${stringValue.replace(/"/g, '""')}"`;
    }

    return stringValue;
}

export function parseCsv(text) {
    if (!text || !text.trim()) {
        return [];
    }

    const rows = [];
    let currentCell = '';
    let currentRow = [];
    let inQuotes = false;

    for (let index = 0; index < text.length; index += 1) {
        const character = text[index];
        const nextCharacter = text[index + 1];

        if (character === '"') {
            if (inQuotes && nextCharacter === '"') {
                currentCell += '"';
                index += 1;
            } else {
                inQuotes = !inQuotes;
            }
            continue;
        }

        if (character === ',' && !inQuotes) {
            currentRow.push(currentCell);
            currentCell = '';
            continue;
        }

        if ((character === '\n' || character === '\r') && !inQuotes) {
            if (character === '\r' && nextCharacter === '\n') {
                index += 1;
            }

            currentRow.push(currentCell);
            rows.push(currentRow);
            currentCell = '';
            currentRow = [];
            continue;
        }

        currentCell += character;
    }

    currentRow.push(currentCell);
    rows.push(currentRow);

    const [header, ...dataRows] = rows.filter((row) => row.some((cell) => cell !== ''));

    if (!header) {
        return [];
    }

    return dataRows.map((row) => Object.fromEntries(header.map((column, index) => [column, row[index] ?? ''])));
}

export function stringifyCsv(rows, columns = null) {
    if (!rows || !rows.length) {
        return columns ? `${columns.join(',')}\n` : '';
    }

    const header = columns ?? Array.from(
        rows.reduce((set, row) => {
            Object.keys(row).forEach((key) => set.add(key));
            return set;
        }, new Set())
    );

    const lines = [header.join(',')];

    rows.forEach((row) => {
        lines.push(header.map((column) => escapeCsvCell(row[column])).join(','));
    });

    return `${lines.join('\n')}\n`;
}
