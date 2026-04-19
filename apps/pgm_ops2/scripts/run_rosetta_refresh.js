import { ARTIFACT_DIR_URLS } from '../app/config/paths.js';
import {
    DEFAULT_LOOKBACK_DAYS,
    RAW_DATASET_MIN_HISTORY_DAYS,
    RAW_FILE_NAMES,
    RAW_METADATA_FILE_NAME
} from '../app/config/constants.js';
import { exists, readCsvFile, writeCsvFile } from '../app/loaders/files.js';
import {
    getRosettaErrorCode,
    RosettaCodexBridgeClient,
    RosettaMcpClient,
    ROSETTA_ERROR_CODES
} from '../app/rosetta/client.js';
import { getRawDatasetColumns, getRosettaQuerySpecs } from '../app/rosetta/query_specs.js';
import { shiftDate } from '../app/transforms/date.js';
import { parsePipelineCliArgs } from './pipeline_cli.js';

const DEFAULT_ROSETTA_ENDPOINT = 'https://dev-rosetta-api.mercuryx.net/mcp';
const HISTORY_READY_WINDOWS = [7, 30, 90];
const BRAND_SCORE_EVENT_SPEC = {
    datasetKey: 'brand_score_events',
    sourceKey: 'derived_brand_score_events',
    sourceTable: 'silver_meta_order_item(minimum_event_layer)',
    dateFields: ['order_at']
};

const REFRESH_STATUS_COLUMNS = [
    'dataset_key',
    'filename',
    'source_key',
    'source_table',
    'query_window_start',
    'query_window_end',
    'requested_lookback_days',
    'effective_lookback_days',
    'required_min_lookback_days',
    'status',
    'row_count',
    'min_date',
    'max_date',
    'history_ready_7d',
    'history_ready_30d',
    'history_ready_90d',
    'history_note',
    'note',
    'provenance',
    'counts_toward_completion',
    'auth_mode'
];

const REFRESH_FAILURE_STATUS_CODES = {
    auth_missing: ROSETTA_ERROR_CODES.auth_missing,
    mcp_bridge_failed: ROSETTA_ERROR_CODES.mcp_bridge_failed,
    query_failed: ROSETTA_ERROR_CODES.query_failed,
    source_empty: 'source_empty'
};

function getToken() {
    return process.env.PGM_OPS2_ROSETTA_BEARER_TOKEN ?? '';
}

function getEndpoint() {
    return process.env.PGM_OPS2_ROSETTA_ENDPOINT ?? DEFAULT_ROSETTA_ENDPOINT;
}

function getAuthModePreference() {
    const rawValue = String(process.env.PGM_OPS2_ROSETTA_AUTH_MODE ?? 'auto').trim().toLowerCase();
    if (rawValue === 'bridge' || rawValue === 'bearer') {
        return rawValue;
    }
    return 'auto';
}

function buildRuntimeState() {
    const authPreference = getAuthModePreference();
    const bearerToken = getToken();

    return {
        authPreference,
        bridgeClient: authPreference === 'bearer' ? null : new RosettaCodexBridgeClient(),
        bearerClient: authPreference === 'bridge' || !bearerToken
            ? null
            : new RosettaMcpClient({
                endpoint: getEndpoint(),
                bearerToken
            }),
        bridgeFailure: null
    };
}

function collectDateValues(rows, dateFields = []) {
    return rows
        .flatMap((row) => dateFields.map((field) => String(row[field] ?? '')))
        .map((value) => value.slice(0, 10))
        .filter((value) => /^\d{4}-\d{2}-\d{2}$/.test(value))
        .sort();
}

function getBounds(asOfDate, lookbackDays) {
    if (!lookbackDays) {
        return {
            asOfDate,
            lookbackStart: ''
        };
    }

    return {
        asOfDate,
        lookbackStart: shiftDate(asOfDate, -(lookbackDays - 1))
    };
}

function getRequiredMinLookbackDays(datasetKey) {
    return Number(RAW_DATASET_MIN_HISTORY_DAYS[datasetKey] ?? 0);
}

function buildDatasetRefreshPlan(datasetKey, asOfDate, requestedLookbackDays) {
    const requiredMinLookbackDays = getRequiredMinLookbackDays(datasetKey);
    const effectiveLookbackDays = requiredMinLookbackDays > 0
        ? Math.max(requestedLookbackDays, requiredMinLookbackDays)
        : 0;
    const bounds = getBounds(asOfDate, effectiveLookbackDays);

    return {
        datasetKey,
        asOfDate,
        bounds,
        requestedLookbackDays,
        effectiveLookbackDays,
        requiredMinLookbackDays
    };
}

function isHistoryReady(dates, asOfDate, windowDays) {
    if (!dates.length) {
        return false;
    }

    const requiredStart = shiftDate(asOfDate, -(windowDays - 1));
    return dates[0] <= requiredStart && dates.at(-1) >= asOfDate;
}

function getStatusSpec(datasetKey, spec) {
    if (spec) {
        return spec;
    }
    if (datasetKey === BRAND_SCORE_EVENT_SPEC.datasetKey) {
        return BRAND_SCORE_EVENT_SPEC;
    }
    return {
        datasetKey,
        sourceKey: '',
        sourceTable: '',
        dateFields: ['date', 'order_at', 'created_at', 'updated_at']
    };
}

function buildHistoryMeta(dates, plan, metricsSource = 'loaded') {
    const minDate = dates[0] ?? '';
    const maxDate = dates.at(-1) ?? '';
    const isPreservedSnapshot = metricsSource === 'snapshot_preserved';
    const preservedSuffix = isPreservedSnapshot ? ' 보존 snapshot 기준입니다.' : '';

    if (!plan.effectiveLookbackDays) {
        const ready = dates.length > 0 ? 'true' : 'false';
        return {
            minDate,
            maxDate,
            history_ready_7d: ready,
            history_ready_30d: ready,
            history_ready_90d: ready,
            history_note: dates.length
                ? `이력 최소 요구가 없는 전체 스냅샷입니다.${preservedSuffix}`
                : `이력 최소 요구가 없는 전체 스냅샷이지만 관측 행이 없습니다.${preservedSuffix}`
        };
    }

    const lookbackNote = plan.effectiveLookbackDays > plan.requestedLookbackDays
        ? `최소 이력 ${plan.requiredMinLookbackDays}일 요구로 실제 조회 기간을 ${plan.effectiveLookbackDays}일로 확장했습니다.`
        : `요청 기간 ${plan.effectiveLookbackDays}일을 그대로 조회했습니다.`;

    if (!dates.length) {
        return {
            minDate,
            maxDate,
            history_ready_7d: 'false',
            history_ready_30d: 'false',
            history_ready_90d: 'false',
            history_note: `${lookbackNote} 날짜 관측이 없어 준비 여부를 판단할 수 없습니다.${preservedSuffix}`
        };
    }

    const sourceLabel = isPreservedSnapshot ? '보존 snapshot 관측 범위' : '관측 범위';
    return {
        minDate,
        maxDate,
        history_ready_7d: isHistoryReady(dates, plan.asOfDate, HISTORY_READY_WINDOWS[0]) ? 'true' : 'false',
        history_ready_30d: isHistoryReady(dates, plan.asOfDate, HISTORY_READY_WINDOWS[1]) ? 'true' : 'false',
        history_ready_90d: isHistoryReady(dates, plan.asOfDate, HISTORY_READY_WINDOWS[2]) ? 'true' : 'false',
        history_note: `${lookbackNote} ${sourceLabel} ${minDate}~${maxDate} 기준으로 준비 여부를 계산했습니다.${preservedSuffix}`
    };
}

function buildStatusRow(specInput, rows, status, note, plan, extras = {}) {
    const spec = getStatusSpec(specInput.datasetKey, specInput);
    const metricRows = extras.metricRows ?? rows;
    const dates = collectDateValues(metricRows, spec.dateFields);
    const historyMeta = buildHistoryMeta(dates, plan, extras.metricsSource);

    return {
        dataset_key: spec.datasetKey,
        filename: RAW_FILE_NAMES[spec.datasetKey],
        source_key: spec.sourceKey,
        source_table: spec.sourceTable,
        query_window_start: plan.bounds.lookbackStart,
        query_window_end: plan.bounds.asOfDate,
        requested_lookback_days: plan.requestedLookbackDays,
        effective_lookback_days: plan.effectiveLookbackDays,
        required_min_lookback_days: plan.requiredMinLookbackDays,
        status,
        row_count: rows.length,
        min_date: historyMeta.minDate,
        max_date: historyMeta.maxDate,
        history_ready_7d: historyMeta.history_ready_7d,
        history_ready_30d: historyMeta.history_ready_30d,
        history_ready_90d: historyMeta.history_ready_90d,
        history_note: historyMeta.history_note,
        note,
        provenance: extras.provenance ?? '',
        counts_toward_completion: extras.countsTowardCompletion ?? 'false',
        auth_mode: extras.authMode ?? ''
    };
}

async function ensureHeaderOnlyFile(datasetKey) {
    const url = new URL(RAW_FILE_NAMES[datasetKey], ARTIFACT_DIR_URLS.raw_rosetta);
    const columns = getRawDatasetColumns()[datasetKey];
    const alreadyExists = await exists(url);
    const existingRows = alreadyExists ? await readCsvFile(url) : [];

    if (!existingRows.length) {
        await writeCsvFile(url, [], columns);
    }

    return existingRows;
}

async function writeDataset(datasetKey, rows) {
    await writeCsvFile(
        new URL(RAW_FILE_NAMES[datasetKey], ARTIFACT_DIR_URLS.raw_rosetta),
        rows,
        getRawDatasetColumns()[datasetKey]
    );
}

function deriveBrandScoreEvents(orderLinesRows) {
    return orderLinesRows.map((row) => ({
        order_id: row.order_id ?? '',
        order_at: row.order_at ?? '',
        product_id: row.product_id ?? '',
        member_id: row.customer_id ?? '',
        event_type: 'purchase',
        quantity: row.quantity ?? '',
        payment_amount: row.payment_amount ?? ''
    }));
}

async function executeQueryWithFallback(runtimeState, { connectionId, sql }) {
    if (runtimeState.bridgeClient && runtimeState.authPreference !== 'bearer' && !runtimeState.bridgeFailure) {
        try {
            return {
                result: await runtimeState.bridgeClient.executeQuery({ connectionId, sql }),
                provenance: 'rosetta_mcp_bridge',
                authMode: 'codex_mcp_bridge'
            };
        } catch (error) {
            const errorCode = getRosettaErrorCode(error);
            if (errorCode === ROSETTA_ERROR_CODES.query_failed) {
                throw error;
            }

            runtimeState.bridgeFailure = {
                code: errorCode,
                message: String(error.message ?? error)
            };

            if (!runtimeState.bearerClient || runtimeState.authPreference === 'bridge') {
                throw error;
            }

            console.warn(`[pgm_ops2] bridge fallback -> bearer: ${runtimeState.bridgeFailure.message}`);
        }
    }

    if (runtimeState.bearerClient) {
        return {
            result: await runtimeState.bearerClient.executeQuery({ connectionId, sql }),
            provenance: 'rosetta_bearer',
            authMode: 'bearer_token'
        };
    }

    if (runtimeState.bridgeFailure) {
        const error = new Error(runtimeState.bridgeFailure.message);
        error.code = runtimeState.bridgeFailure.code;
        throw error;
    }

    const error = new Error('Rosetta 인증 정보가 없습니다. Codex MCP 로그인 또는 bearer token이 필요합니다.');
    error.code = ROSETTA_ERROR_CODES.auth_missing;
    throw error;
}

async function fetchAllRowsForSpec(runtimeState, spec, bounds) {
    const rows = [];
    const provenanceSet = new Set();
    const authModeSet = new Set();
    let offset = 0;

    while (true) {
        const { result, provenance, authMode } = await executeQueryWithFallback(runtimeState, {
            connectionId: spec.connectionId,
            sql: spec.buildSql({
                asOfDate: bounds.asOfDate,
                lookbackStart: bounds.lookbackStart,
                limit: spec.pageSize,
                offset
            })
        });

        provenanceSet.add(provenance);
        authModeSet.add(authMode);
        rows.push(...result.rows);

        if (result.rows.length < spec.pageSize) {
            break;
        }

        offset += spec.pageSize;
    }

    return {
        rows,
        provenance: [...provenanceSet].join('+'),
        authMode: [...authModeSet].join('+')
    };
}

async function writeRefreshStatus(rows) {
    await writeCsvFile(
        new URL(RAW_METADATA_FILE_NAME, ARTIFACT_DIR_URLS.raw_rosetta),
        rows,
        REFRESH_STATUS_COLUMNS
    );
}

function buildFailureNote(statusCode, message, preservedCount = 0) {
    const normalizedMessage = String(message ?? '').trim() || 'Rosetta refresh failed.';
    return preservedCount > 0
        ? `${statusCode}: ${normalizedMessage} 기존 snapshot은 유지했습니다.`
        : `${statusCode}: ${normalizedMessage}`;
}

async function buildBlockedStateRows(
    asOfDate,
    requestedLookbackDays,
    statusCode,
    message,
    querySpecs = [],
    authMode = '',
    datasetKeys = Object.keys(RAW_FILE_NAMES)
) {
    const specByDataset = Object.fromEntries(querySpecs.map((spec) => [spec.datasetKey, spec]));
    const statusRows = [];

    for (const datasetKey of datasetKeys) {
        const existingRows = await ensureHeaderOnlyFile(datasetKey);
        const plan = buildDatasetRefreshPlan(datasetKey, asOfDate, requestedLookbackDays);
        const spec = getStatusSpec(datasetKey, specByDataset[datasetKey]);

        statusRows.push(buildStatusRow(
            spec,
            [],
            statusCode,
            buildFailureNote(statusCode, message, existingRows.length),
            plan,
            {
                provenance: existingRows.length ? 'snapshot_preserved' : '',
                countsTowardCompletion: 'false',
                authMode,
                metricRows: existingRows,
                metricsSource: existingRows.length ? 'snapshot_preserved' : 'loaded'
            }
        ));
    }

    return statusRows;
}

function classifyFailureStatus(error) {
    const errorCode = getRosettaErrorCode(error);
    if (errorCode === ROSETTA_ERROR_CODES.auth_missing) {
        return REFRESH_FAILURE_STATUS_CODES.auth_missing;
    }
    if (errorCode === ROSETTA_ERROR_CODES.mcp_bridge_failed) {
        return REFRESH_FAILURE_STATUS_CODES.mcp_bridge_failed;
    }
    return REFRESH_FAILURE_STATUS_CODES.query_failed;
}

function buildSuccessNote(spec, authMode) {
    const notes = [];
    if (spec.successNote) {
        notes.push(spec.successNote);
    }
    notes.push(`loaded_via=${authMode}`);
    return notes.join(' ');
}

export async function refreshRawExtractFromRosetta(options = {}) {
    const asOfDate = options.asOfDate || '';
    const requestedLookbackDays = Number.parseInt(options.lookbackDays, 10) || DEFAULT_LOOKBACK_DAYS;

    if (!asOfDate) {
        throw new Error('Rosetta refresh requires --as-of-date YYYY-MM-DD.');
    }

    const querySpecs = getRosettaQuerySpecs();
    const runtimeState = buildRuntimeState();
    const datasetRows = {};
    const refreshStatuses = [];

    if (runtimeState.authPreference === 'bearer' && !runtimeState.bearerClient) {
        const reason = 'PGM_OPS2_ROSETTA_AUTH_MODE=bearer 이지만 PGM_OPS2_ROSETTA_BEARER_TOKEN이 없습니다.';
        await writeRefreshStatus(
            await buildBlockedStateRows(
                asOfDate,
                requestedLookbackDays,
                REFRESH_FAILURE_STATUS_CODES.auth_missing,
                reason,
                querySpecs,
                'bearer_token',
                Object.keys(RAW_FILE_NAMES)
            )
        );
        console.log(reason);
        return;
    }

    for (const spec of querySpecs) {
        const plan = buildDatasetRefreshPlan(spec.datasetKey, asOfDate, requestedLookbackDays);

        try {
            const { rows, provenance, authMode } = await fetchAllRowsForSpec(runtimeState, spec, plan.bounds);

            if (!rows.length) {
                const preservedRows = await ensureHeaderOnlyFile(spec.datasetKey);
                datasetRows[spec.datasetKey] = [];
                refreshStatuses.push(buildStatusRow(
                    spec,
                    [],
                    REFRESH_FAILURE_STATUS_CODES.source_empty,
                    buildFailureNote(
                        REFRESH_FAILURE_STATUS_CODES.source_empty,
                        `${spec.sourceTable} 조회 결과가 비어 있습니다.`,
                        preservedRows.length
                    ),
                    plan,
                    {
                        provenance: preservedRows.length ? `${provenance}+snapshot_preserved` : provenance,
                        countsTowardCompletion: 'false',
                        authMode,
                        metricRows: preservedRows,
                        metricsSource: preservedRows.length ? 'snapshot_preserved' : 'loaded'
                    }
                ));
                console.warn(`[pgm_ops2] source empty ${spec.datasetKey}`);
                continue;
            }

            datasetRows[spec.datasetKey] = rows;
            await writeDataset(spec.datasetKey, rows);
            refreshStatuses.push(buildStatusRow(
                spec,
                rows,
                'completed',
                buildSuccessNote(spec, authMode),
                plan,
                {
                    provenance,
                    countsTowardCompletion: 'true',
                    authMode
                }
            ));
            console.log(`[pgm_ops2] refreshed ${spec.datasetKey}: ${rows.length} rows (${authMode})`);
        } catch (error) {
            datasetRows[spec.datasetKey] = [];
            const statusCode = classifyFailureStatus(error);
            const preservedRows = await ensureHeaderOnlyFile(spec.datasetKey);
            refreshStatuses.push(buildStatusRow(
                spec,
                [],
                statusCode,
                buildFailureNote(statusCode, error.message ?? 'Rosetta query failed', preservedRows.length),
                plan,
                {
                    provenance: preservedRows.length ? 'snapshot_preserved' : '',
                    countsTowardCompletion: 'false',
                    authMode: runtimeState.bridgeFailure?.code === statusCode
                        ? 'codex_mcp_bridge'
                        : (runtimeState.bearerClient ? 'bearer_token' : ''),
                    metricRows: preservedRows,
                    metricsSource: preservedRows.length ? 'snapshot_preserved' : 'loaded'
                }
            ));
            console.warn(`[pgm_ops2] failed ${spec.datasetKey}: ${error.message ?? error}`);

            if (
                (statusCode === REFRESH_FAILURE_STATUS_CODES.auth_missing || statusCode === REFRESH_FAILURE_STATUS_CODES.mcp_bridge_failed)
                && !runtimeState.bearerClient
            ) {
                const remainingSpecs = querySpecs.slice(querySpecs.indexOf(spec) + 1);
                const blockedRows = await buildBlockedStateRows(
                    asOfDate,
                    requestedLookbackDays,
                    statusCode,
                    error.message ?? 'Rosetta auth or bridge is unavailable.',
                    remainingSpecs,
                    'codex_mcp_bridge',
                    remainingSpecs.map((item) => item.datasetKey)
                );
                refreshStatuses.push(...blockedRows);
                break;
            }
        }
    }

    const brandScoreEvents = deriveBrandScoreEvents(datasetRows.order_lines ?? []);
    if (brandScoreEvents.length) {
        await writeDataset('brand_score_events', brandScoreEvents);
    }
    const preservedBrandScoreRows = brandScoreEvents.length ? [] : await ensureHeaderOnlyFile('brand_score_events');
    const brandScorePlan = buildDatasetRefreshPlan('brand_score_events', asOfDate, requestedLookbackDays);

    refreshStatuses.push(buildStatusRow(
        BRAND_SCORE_EVENT_SPEC,
        brandScoreEvents,
        brandScoreEvents.length ? 'completed' : REFRESH_FAILURE_STATUS_CODES.source_empty,
        brandScoreEvents.length
            ? 'order_lines 기반 최소 event layer를 파생했습니다.'
            : buildFailureNote(
                REFRESH_FAILURE_STATUS_CODES.source_empty,
                'order_lines 기반 최소 event layer를 만들 수 없었습니다.',
                preservedBrandScoreRows.length
            ),
        brandScorePlan,
        {
            provenance: brandScoreEvents.length
                ? 'derived_from_order_lines'
                : (preservedBrandScoreRows.length ? 'snapshot_preserved' : 'derived_from_order_lines'),
            countsTowardCompletion: brandScoreEvents.length ? 'true' : 'false',
            authMode: 'derived',
            metricRows: brandScoreEvents.length ? brandScoreEvents : preservedBrandScoreRows,
            metricsSource: brandScoreEvents.length ? 'loaded' : (preservedBrandScoreRows.length ? 'snapshot_preserved' : 'loaded')
        }
    ));

    await writeRefreshStatus(refreshStatuses);
}

export async function main(argv = process.argv.slice(2)) {
    const options = parsePipelineCliArgs(argv);
    await refreshRawExtractFromRosetta(options);
}

if (import.meta.url === new URL(process.argv[1], 'file:').href) {
    await main();
}
