import { buildRoleWindowSummary } from '../../transforms/role/role_windows.js';

export function buildRoleStructureChart(productDailyMetrics, productRoleStateDaily) {
    return buildRoleWindowSummary(productDailyMetrics, productRoleStateDaily);
}
