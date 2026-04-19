import { fileURLToPath } from 'node:url';

import { ARTIFACT_DIR_URLS } from '../app/config/paths.js';
import { DEFAULT_LOOKBACK_DAYS, RAW_FILE_NAMES, RAW_METADATA_FILE_NAME } from '../app/config/constants.js';
import { exists, readCsvFile, writeCsvFile } from '../app/loaders/files.js';
import { shiftDate } from '../app/transforms/date.js';
import { RosettaMcpClient } from '../app/rosetta/client.js';
import { getRawDatasetColumns, getRosettaQuerySpecs } from '../app/rosetta/query_specs.js';
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
    'note'
];

function getToken() {
    return process.env.PGM_OPS2_ROSETTA_BEARER_TOKEN ?? '';
}

function getEndpoint() {
    return process.env.PGM_OPS2_ROSETTA_ENDPOINT ?? DEFAULT_ROSETTA_ENDPOINT;
}

function collectDateValues(rows, dateFields = []) {
    return rows
        .flatMap((row) => dateFields.map((field) => String(row[field] ?? '')))
        .map((value) => value.slice(0, 10))
        .filter((value) => /^\d{4}-\d{2}-\d{2}$/.test(value))
        .sort();
}

function buildStatusRow(spec, rows, status, note, bounds) {
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
        note
    };
}

async function ensureHeaderOnlyFile(datasetKey, note) {
    const url = new URL(RAW_FILE_NAMES[datasetKey], ARTIFACT_DIR_URLS.raw_rosetta);
    const columns = getRawDatasetColumns()[datasetKey];
    const alreadyExists = await exists(url);
    const existingRows = alreadyExists ? await readCsvFile(url) : [];

    if (!existingRows.length) {
        await writeCsvFile(url, [], columns);
    }

    return note;
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

async function fetchAllRowsForSpec(client, spec, bounds) {
    const rows = [];
    let offset = 0;

    while (true) {
        const result = await client.executeQuery({
            connectionId: spec.connectionId,
            sql: spec.buildSql({
                asOfDate: bounds.asOfDate,
                lookbackStart: bounds.lookbackStart,
                limit: spec.pageSize,
                offset
            })
        });

        rows.push(...result.rows);
        if (result.rows.length < spec.pageSize) {
            break;
        }

        offset += spec.pageSize;
    }

    return rows;
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

async function writeBlockedState(bounds, reason) {
    const statusRows = [];
    const columnsByDataset = getRawDatasetColumns();
    const querySpecsByDataset = Object.fromEntries(
        getRosettaQuerySpecs(bounds).map((spec) => [spec.datasetKey, spec])
    );

    for (const datasetKey of Object.keys(RAW_FILE_NAMES)) {
        const rawUrl = new URL(RAW_FILE_NAMES[datasetKey], ARTIFACT_DIR_URLS.raw_rosetta);
        const existingRows = await readCsvFile(rawUrl);
        const existingDates = collectDateValues(existingRows, ['date', 'order_at', 'created_at', 'updated_at']);
        const spec = querySpecsByDataset[datasetKey];

        if (datasetKey === 'brand_score_events') {
            if (!existingRows.length) {
                await writeCsvFile(rawUrl, [], columnsByDataset[datasetKey]);
            }
        } else {
            await ensureHeaderOnlyFile(datasetKey, reason);
        }

        statusRows.push({
            dataset_key: datasetKey,
            filename: RAW_FILE_NAMES[datasetKey],
            source_key: datasetKey === 'brand_score_events' ? 'derived_brand_score_events' : (spec?.sourceKey ?? ''),
            source_table: datasetKey === 'brand_score_events' ? 'silver_meta_order_item(minimum_event_layer)' : (spec?.sourceTable ?? ''),
            query_window_start: bounds.lookbackStart,
            query_window_end: bounds.asOfDate,
            status: existingRows.length ? 'preserved' : 'blocked',
            row_count: existingRows.length,
            min_date: existingDates[0] ?? '',
            max_date: existingDates.at(-1) ?? '',
            note: existingRows.length ? `${reason} 기존 snapshot은 유지했습니다.` : reason
        });
    }

    await writeRefreshStatus(statusRows);
    return statusRows;
}

export async function refreshRawExtractFromRosetta(options = {}) {
    const asOfDate = options.asOfDate || '';
    const lookbackDays = Number.parseInt(options.lookbackDays, 10) || DEFAULT_LOOKBACK_DAYS;

    if (!asOfDate) {
        throw new Error('Rosetta refresh requires --as-of-date YYYY-MM-DD.');
    }

    const bounds = getBounds(asOfDate, lookbackDays);
    const bearerToken = getToken();

    if (!bearerToken) {
        const reason = 'PGM_OPS2_ROSETTA_BEARER_TOKEN이 없어 Rosetta 직접 조회를 실행하지 못했습니다.';
        await writeBlockedState(bounds, reason);
        console.log(reason);
        return;
    }

    const client = new RosettaMcpClient({
        endpoint: getEndpoint(),
        bearerToken
    });

    const querySpecs = getRosettaQuerySpecs(bounds);
    const datasetRows = {};
    const refreshStatuses = [];

    for (const spec of querySpecs) {
        try {
            const rows = await fetchAllRowsForSpec(client, spec, bounds);
            datasetRows[spec.datasetKey] = rows;
            await writeDataset(spec.datasetKey, rows);
            refreshStatuses.push(buildStatusRow(spec, rows, 'completed', '', bounds));
            console.log(`[pgm_ops2] refreshed ${spec.datasetKey}: ${rows.length} rows`);
        } catch (error) {
            datasetRows[spec.datasetKey] = [];
            await writeCsvFile(
                new URL(RAW_FILE_NAMES[spec.datasetKey], ARTIFACT_DIR_URLS.raw_rosetta),
                [],
                spec.columns
            );
            refreshStatuses.push(buildStatusRow(
                spec,
                [],
                'failed',
                error.message ?? 'Rosetta query failed',
                bounds
            ));
            console.warn(`[pgm_ops2] failed ${spec.datasetKey}: ${error.message ?? error}`);
        }
    }

    const brandScoreEvents = deriveBrandScoreEvents(datasetRows.order_lines ?? []);
    await writeDataset('brand_score_events', brandScoreEvents);
    refreshStatuses.push({
        dataset_key: 'brand_score_events',
        filename: RAW_FILE_NAMES.brand_score_events,
        source_key: 'derived_brand_score_events',
        source_table: 'silver_meta_order_item(minimum_event_layer)',
        query_window_start: bounds.lookbackStart,
        query_window_end: bounds.asOfDate,
        status: 'completed',
        row_count: brandScoreEvents.length,
        min_date: collectDateValues(brandScoreEvents, ['order_at'])[0] ?? '',
        max_date: collectDateValues(brandScoreEvents, ['order_at']).at(-1) ?? '',
        note: 'event_type은 purchase 고정, member_id는 customer_id에서 직접 매핑했습니다.'
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
