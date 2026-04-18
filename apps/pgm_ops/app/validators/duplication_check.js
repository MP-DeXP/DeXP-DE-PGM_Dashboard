export function runDuplicationCheck(artifactName, rows, keyFields) {
    const uniqueKeys = new Set(rows.map((row) => keyFields.map((field) => row[field] ?? '').join('|')));

    return {
        artifact_name: artifactName,
        check_name: 'duplication_check',
        status: uniqueKeys.size === rows.length ? 'pass' : 'warn',
        message: uniqueKeys.size === rows.length
            ? 'No duplicate presentation rows detected.'
            : `Duplicate presentation rows found: ${rows.length - uniqueKeys.size}`
    };
}
