import { parseCsv } from './csv_parser.js';

export async function browserReadText(path) {
    const response = await fetch(path);

    if (!response.ok) {
        throw new Error(`Failed to read ${path}: ${response.status}`);
    }

    return response.text();
}

export async function loadCsvArtifact({ path, alternatePaths = [], readText = browserReadText, fallbackRows = [] }) {
    const candidatePaths = [path, ...alternatePaths.filter(Boolean)];
    let lastError = null;

    for (const candidatePath of candidatePaths) {
        try {
            const text = await readText(candidatePath);
            return {
                // Empty text still counts as an artifact read; do not silently replace it with sample rows.
                rows: parseCsv(text),
                source: 'artifact',
                error: null,
                resolvedPath: candidatePath
            };
        } catch (error) {
            lastError = error;
        }
    }

    return {
        rows: fallbackRows,
        source: 'fallback',
        error: lastError,
        resolvedPath: null
    };
}

export async function loadArtifactCollection(definitions, options = {}) {
    const entries = await Promise.all(
        Object.entries(definitions).map(async ([key, definition]) => {
            const result = await loadCsvArtifact({
                path: definition.path,
                alternatePaths: definition.alternatePaths ?? [],
                readText: options.readText,
                fallbackRows: definition.fallbackRows ?? []
            });

            return [key, result];
        })
    );

    return Object.fromEntries(entries);
}
