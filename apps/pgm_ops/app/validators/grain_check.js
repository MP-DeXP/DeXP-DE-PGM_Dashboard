export function runGrainCheck(artifactName, rows, schema) {
    if (!schema.primaryKey?.length) {
        return {
            artifact_name: artifactName,
            check_name: 'grain_check',
            status: 'warn',
            message: 'No explicit PK candidate defined for this artifact.'
        };
    }

    const duplicates = new Map();

    rows.forEach((row) => {
        const key = schema.primaryKey.map((field) => row[field] ?? '').join('|');
        duplicates.set(key, (duplicates.get(key) ?? 0) + 1);
    });

    const duplicateKeys = [...duplicates.entries()].filter(([, count]) => count > 1).map(([key]) => key);

    return {
        artifact_name: artifactName,
        check_name: 'grain_check',
        status: duplicateKeys.length ? 'fail' : 'pass',
        message: duplicateKeys.length
            ? `Duplicate grain detected: ${duplicateKeys.slice(0, 5).join(', ')}`
            : 'Primary grain is unique.'
    };
}
