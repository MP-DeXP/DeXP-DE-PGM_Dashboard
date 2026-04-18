import { APP_SETTINGS } from '../config/settings.js';
import { getLatestDate } from '../transforms/base/date_windows.js';

export function runFreshnessCheck(artifactName, rows, dateField = 'date') {
    const latestDate = getLatestDate(rows, dateField);

    if (!latestDate) {
        return {
            artifact_name: artifactName,
            check_name: 'freshness_check',
            status: 'warn',
            message: 'No date field available for freshness evaluation.'
        };
    }

    const diffDays = Math.floor((Date.now() - new Date(`${latestDate}T00:00:00`).getTime()) / 86400000);
    const status = diffDays > APP_SETTINGS.validationFreshnessGraceDays ? 'warn' : 'pass';

    return {
        artifact_name: artifactName,
        check_name: 'freshness_check',
        status,
        message: `Latest ${dateField}=${latestDate}, age=${diffDays}d`
    };
}
