import { parseCsv } from './csv_parser.js';

export async function browserReadText(path) {
    const response = await fetch(path);

    if (!response.ok) {
        throw new Error(`Failed to read ${path}: ${response.status}`);
    }

    return response.text();
}

export async function loadCsvArtifact({ path, readText = browserReadText, fallbackRows = [] }) {
    try {
        const text = await readText(path);
        return {
            // Empty text still counts as an artifact read; do not silently replace it with sample rows.
            rows: parseCsv(text),
            source: 'artifact',
            error: null
        };
    } catch (error) {
        return {
            rows: fallbackRows,
            source: 'fallback',
            error
        };
    }
}

export async function loadArtifactCollection(definitions, options = {}) {
    const entries = await Promise.all(
        Object.entries(definitions).map(async ([key, definition]) => {
            const result = await loadCsvArtifact({
                path: definition.path,
                readText: options.readText,
                fallbackRows: definition.fallbackRows ?? []
            });

            return [key, result];
        })
    );

    return Object.fromEntries(entries);
}
