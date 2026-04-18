export function runNullCheck(artifactName, rows, schema) {
    const violations = [];

    schema.required.forEach((column) => {
        if (schema.nullable.includes(column)) {
            return;
        }

        const nullCount = rows.filter((row) => row[column] == null || row[column] === '').length;

        if (nullCount > 0) {
            violations.push(`${column}:${nullCount}`);
        }
    });

    return {
        artifact_name: artifactName,
        check_name: 'null_check',
        status: violations.length ? 'warn' : 'pass',
        message: violations.length
            ? `Nulls in required columns: ${violations.join(', ')}`
            : 'Required columns are populated.'
    };
}
