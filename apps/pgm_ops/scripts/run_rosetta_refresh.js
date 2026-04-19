import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { DEFAULT_EXTRACT_LOOKBACK_DAYS } from '../app/config/constants.js';
import { parsePipelineCliArgs } from './pipeline_cli.js';

function toBoolean(value) {
    if (value === true || value === false) {
        return value;
    }

    if (value == null || value === '') {
        return false;
    }

    return !['0', 'false', 'no'].includes(String(value).toLowerCase());
}

function buildRefreshPrompt({ workspaceRoot, rawDir, asOfDate, lookbackDays, mxChannelId, mxPlatform }) {
    return [
        `You are in ${workspaceRoot}.`,
        'Use Rosetta MCP tools to refresh the raw CSV artifacts under the raw_extract directory.',
        `Target raw_extract directory: ${rawDir}`,
        `as_of_date: ${asOfDate}`,
        `lookback_days: ${lookbackDays}`,
        `mx_channel_id: ${mxChannelId || '(preserve existing tenant scope if omitted)'}`,
        `mx_platform: ${mxPlatform || '(preserve existing tenant scope if omitted)'}`,
        '',
        'Requirements:',
        '- Only update files in the raw_extract directory unless you must inspect nearby project files to preserve the CSV contract.',
        '- Preserve each file header exactly as it already exists.',
        '- Refresh these files from Rosetta-backed sources: orders.csv, order_items.csv, products.csv, product_daily.csv, members.csv, order_with_utm.csv, pgm_transition_edge.csv, pgm_loop_detail.csv, pgm_scored.csv.',
        '- product_window_metrics.csv and brand_window_metrics.csv may be regenerated from refreshed product_daily data when direct sources are unavailable.',
        '- For pgm_scored.csv, keep observed dates as observed. If earlier role history is unavailable, synthetically backfill earlier dates from the latest available snapshot so the lookback window remains usable.',
        '- Keep outputs sorted consistently by natural keys and date.',
        '- Do not modify downstream staging, mart, view_model, or qa artifacts.',
        '- Print a concise completion summary with row counts and min/max dates for each refreshed file.',
        '',
        'Helpful source notes verified in this workspace:',
        '- core.silver_meta_order has full 120-day coverage for the active tenant.',
        '- core.silver_meta_order_item coverage currently ends around 2026-03-22 for this tenant.',
        '- dma.silver_fact_product and dma.silver_order_with_utm have full 120-day coverage.',
        '- dma.gold_pgm_product_transition_edge and dma.gold_pgm_return_gravity_loop_detail only cover about 2026-04-06 through 2026-04-17.',
        '- dma.gold_pgm_scored only covers about 2026-04-05 through 2026-04-17.',
        '- Existing raw CSV files in raw_extract are the contract to preserve.'
    ].join('\n');
}

function runCommand(command, args, options = {}) {
    return new Promise((resolve, reject) => {
        const child = spawn(command, args, {
            cwd: options.cwd,
            stdio: 'inherit',
            env: {
                ...process.env,
                ...options.env
            }
        });

        child.on('error', reject);
        child.on('exit', (code, signal) => {
            if (code === 0) {
                resolve();
                return;
            }

            reject(new Error(`${command} exited with code ${code ?? 'unknown'}${signal ? ` (signal: ${signal})` : ''}`));
        });
    });
}

export async function refreshRawExtractFromRosetta(options = {}) {
    const scriptDir = path.dirname(fileURLToPath(import.meta.url));
    const workspaceRoot = path.resolve(scriptDir, '../../..');
    const rawDir = path.join(workspaceRoot, 'apps/pgm_ops/artifacts/raw_extract');
    const asOfDate = options.asOfDate || '';
    const lookbackDays = Number.parseInt(options.lookbackDays, 10) || DEFAULT_EXTRACT_LOOKBACK_DAYS;
    const mxChannelId = options.mxChannelId ? String(options.mxChannelId) : '';
    const mxPlatform = options.mxPlatform ? String(options.mxPlatform) : '';

    if (toBoolean(options.sample)) {
        console.log('[run_rosetta_refresh] sample mode requested; skipping Rosetta refresh.');
        return;
    }

    if (!asOfDate) {
        throw new Error('Rosetta refresh requires --as-of-date YYYY-MM-DD.');
    }

    const prompt = buildRefreshPrompt({
        workspaceRoot,
        rawDir,
        asOfDate,
        lookbackDays,
        mxChannelId,
        mxPlatform
    });

    console.log(`[run_rosetta_refresh] refreshing raw_extract for ${asOfDate} (${lookbackDays}d lookback)`);
    await runCommand('codex', [
        'exec',
        '--ephemeral',
        '--dangerously-bypass-approvals-and-sandbox',
        '-C',
        workspaceRoot,
        prompt
    ], {
        cwd: workspaceRoot
    });
}

export async function main(argv = process.argv.slice(2)) {
    const cliOptions = parsePipelineCliArgs(argv);
    await refreshRawExtractFromRosetta(cliOptions);
}

if (import.meta.url === new URL(process.argv[1], 'file:').href) {
    await main();
}
