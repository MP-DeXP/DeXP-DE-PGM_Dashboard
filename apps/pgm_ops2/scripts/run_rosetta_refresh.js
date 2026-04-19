import { ARTIFACT_DIR_URLS } from '../app/config/paths.js';
import { DEFAULT_LOOKBACK_DAYS, RAW_FILE_NAMES, RAW_METADATA_FILE_NAME } from '../app/config/constants.js';
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

const REFRESH_STATUS_COLUMNS = [
    'dataset_key',
    'filename',
    'source_key',
    'source_table',
    'query_window_start',
    'query_window_end',
    'status',
    'row_count',
    'min_date',
    'max_date',
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

function buildStatusRow(spec, rows, status, note, bounds, extras = {}) {
    const dates = collectDateValues(rows, spec.dateFields);

    return {
        dataset_key: spec.datasetKey,
        filename: RAW_FILE_NAMES[spec.datasetKey],
        source_key: spec.sourceKey,
        source_table: spec.sourceTable,
        query_window_start: bounds.lookbackStart,
        query_window_end: bounds.asOfDate,
        status,
        row_count: rows.length,
        min_date: dates[0] ?? '',
        max_date: dates.at(-1) ?? '',
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

function getBounds(asOfDate, lookbackDays) {
    return {
        asOfDate,
        lookbackStart: shiftDate(asOfDate, -(lookbackDays - 1))
    };
}

function buildFailureNote(statusCode, message, preservedCount = 0) {
    const normalizedMessage = String(message ?? '').trim() || 'Rosetta refresh failed.';
    return preservedCount > 0
        ? `${statusCode}: ${normalizedMessage} 기존 snapshot은 유지했습니다.`
        : `${statusCode}: ${normalizedMessage}`;
}

async function buildBlockedStateRows(bounds, statusCode, message, querySpecs = [], authMode = '', datasetKeys = Object.keys(RAW_FILE_NAMES)) {
    const specByDataset = Object.fromEntries(querySpecs.map((spec) => [spec.datasetKey, spec]));
    const statusRows = [];

    for (const datasetKey of datasetKeys) {
        const existingRows = await ensureHeaderOnlyFile(datasetKey);
        const existingDates = collectDateValues(existingRows, ['date', 'order_at', 'created_at', 'updated_at']);
        const spec = specByDataset[datasetKey];

        statusRows.push({
            dataset_key: datasetKey,
            filename: RAW_FILE_NAMES[datasetKey],
            source_key: datasetKey === 'brand_score_events'
                ? 'derived_brand_score_events'
                : (spec?.sourceKey ?? ''),
            source_table: datasetKey === 'brand_score_events'
                ? 'silver_meta_order_item(minimum_event_layer)'
                : (spec?.sourceTable ?? ''),
            query_window_start: bounds.lookbackStart,
            query_window_end: bounds.asOfDate,
            status: statusCode,
            row_count: 0,
            min_date: existingDates[0] ?? '',
            max_date: existingDates.at(-1) ?? '',
            note: buildFailureNote(statusCode, message, existingRows.length),
            provenance: existingRows.length ? 'snapshot_preserved' : '',
            counts_toward_completion: 'false',
            auth_mode: authMode
        });
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
    const lookbackDays = Number.parseInt(options.lookbackDays, 10) || DEFAULT_LOOKBACK_DAYS;

    if (!asOfDate) {
        throw new Error('Rosetta refresh requires --as-of-date YYYY-MM-DD.');
    }

    const bounds = getBounds(asOfDate, lookbackDays);
    const querySpecs = getRosettaQuerySpecs(bounds);
    const runtimeState = buildRuntimeState();
    const datasetRows = {};
    const refreshStatuses = [];

    if (runtimeState.authPreference === 'bearer' && !runtimeState.bearerClient) {
        const reason = 'PGM_OPS2_ROSETTA_AUTH_MODE=bearer 이지만 PGM_OPS2_ROSETTA_BEARER_TOKEN이 없습니다.';
        await writeRefreshStatus(
            await buildBlockedStateRows(
                bounds,
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
        try {
            const { rows, provenance, authMode } = await fetchAllRowsForSpec(runtimeState, spec, bounds);

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
                    bounds,
                    {
                        provenance,
                        countsTowardCompletion: 'false',
                        authMode
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
                bounds,
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
                bounds,
                {
                    provenance: preservedRows.length ? 'snapshot_preserved' : '',
                    countsTowardCompletion: 'false',
                    authMode: runtimeState.bridgeFailure?.code === statusCode
                        ? 'codex_mcp_bridge'
                        : (runtimeState.bearerClient ? 'bearer_token' : '')
                }
            ));
            console.warn(`[pgm_ops2] failed ${spec.datasetKey}: ${error.message ?? error}`);

            if (
                (statusCode === REFRESH_FAILURE_STATUS_CODES.auth_missing || statusCode === REFRESH_FAILURE_STATUS_CODES.mcp_bridge_failed)
                && !runtimeState.bearerClient
            ) {
                const remainingSpecs = querySpecs.slice(querySpecs.indexOf(spec) + 1);
                const blockedRows = await buildBlockedStateRows(
                    bounds,
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

    refreshStatuses.push({
        dataset_key: 'brand_score_events',
        filename: RAW_FILE_NAMES.brand_score_events,
        source_key: 'derived_brand_score_events',
        source_table: 'silver_meta_order_item(minimum_event_layer)',
        query_window_start: bounds.lookbackStart,
        query_window_end: bounds.asOfDate,
        status: brandScoreEvents.length ? 'completed' : REFRESH_FAILURE_STATUS_CODES.source_empty,
        row_count: brandScoreEvents.length,
        min_date: collectDateValues(brandScoreEvents, ['order_at'])[0] ?? '',
        max_date: collectDateValues(brandScoreEvents, ['order_at']).at(-1) ?? '',
        note: brandScoreEvents.length
            ? 'order_lines 기반 최소 event layer를 파생했습니다.'
            : buildFailureNote(
                REFRESH_FAILURE_STATUS_CODES.source_empty,
                'order_lines 기반 최소 event layer를 만들 수 없었습니다.',
                preservedBrandScoreRows.length
            ),
        provenance: 'derived_from_order_lines',
        counts_toward_completion: brandScoreEvents.length ? 'true' : 'false',
        auth_mode: 'derived'
    });

    await writeRefreshStatus(refreshStatuses);
}

export async function main(argv = process.argv.slice(2)) {
    const options = parsePipelineCliArgs(argv);
    await refreshRawExtractFromRosetta(options);
}

if (import.meta.url === new URL(process.argv[1], 'file:').href) {
    await main();
}
