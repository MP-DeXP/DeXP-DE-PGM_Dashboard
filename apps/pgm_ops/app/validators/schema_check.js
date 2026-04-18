export function runSchemaCheck(artifactName, rows, schema) {
    const presentColumns = new Set(rows[0] ? Object.keys(rows[0]) : []);
    const missingColumns = schema.required.filter((column) => !presentColumns.has(column));

    return {
        artifact_name: artifactName,
        check_name: 'schema_check',
        status: missingColumns.length ? 'fail' : 'pass',
        message: missingColumns.length
            ? `Missing columns: ${missingColumns.join(', ')}`
            : 'Required columns present.'
    };
}
