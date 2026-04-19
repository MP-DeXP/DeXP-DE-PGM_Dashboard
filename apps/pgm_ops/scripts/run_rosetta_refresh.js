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
        `${workspaceRoot}에서 작업합니다.`,
        'Rosetta MCP 도구로 raw_extract 디렉터리 아래 raw CSV artifact를 새로 고칩니다.',
        `대상 raw_extract 디렉터리: ${rawDir}`,
        `as_of_date: ${asOfDate}`,
        `lookback_days: ${lookbackDays}`,
        `mx_channel_id: ${mxChannelId || '(preserve existing tenant scope if omitted)'}`,
        `mx_platform: ${mxPlatform || '(preserve existing tenant scope if omitted)'}`,
        '',
        '우선순위:',
        '- 1순위 truth source: orders.csv, order_items.csv, product_daily.csv. 날짜 범위와 매출 기준선은 이 세 파일을 먼저 맞춥니다.',
        '- 2순위 보조 source: products.csv, members.csv, order_with_utm.csv. 테넌트 범위와 기존 CSV contract를 유지합니다.',
        '- 3순위 제한 source: pgm_scored.csv, pgm_transition_edge.csv, pgm_loop_detail.csv. 실제 관측된 날짜만 적재하고, 없는 날짜를 최신 스냅샷으로 승격/백필하지 않습니다.',
        '- product_window_metrics.csv와 brand_window_metrics.csv는 직접 source가 부족할 때만 refreshed product_daily.csv에서 재계산합니다.',
        '',
        '날짜 검증 순서:',
        '- 1. 기존 raw CSV header와 컬럼 계약을 먼저 읽고 그대로 유지합니다.',
        '- 2. orders/product_daily 기준으로 요청한 as_of_date와 lookback 구간의 실제 min/max date를 확인합니다.',
        '- 3. 각 보조 source의 min/max date를 확인해 truth source보다 짧은 구간은 그대로 부족분으로 남깁니다.',
        '- 4. pgm_scored는 same-date observed 원칙을 유지합니다. latest available snapshot을 정상 evidence처럼 확장하지 않습니다.',
        '- 5. 어떤 source라도 요청 구간을 다 못 채우면 그 사실을 completion summary에 날짜와 함께 명시합니다.',
        '',
        '요구사항:',
        '- CSV contract 보존에 필요한 인접 프로젝트 파일 확인 외에는 raw_extract 디렉터리 안의 파일만 수정합니다.',
        '- 각 파일 header는 현재 파일과 완전히 동일하게 유지합니다.',
        '- Rosetta 기반 source로 orders.csv, order_items.csv, products.csv, product_daily.csv, members.csv, order_with_utm.csv, pgm_transition_edge.csv, pgm_loop_detail.csv, pgm_scored.csv를 갱신합니다.',
        '- 출력은 natural key와 date 기준으로 일관되게 정렬합니다.',
        '- downstream staging, mart, view_model, qa artifact는 수정하지 않습니다.',
        '- 완료 시 각 파일 row count와 min/max date, 그리고 부족한 source/date를 짧게 요약합니다.',
        '',
        '이 워크스페이스에서 확인된 source 메모:',
        '- core.silver_meta_order는 active tenant 기준 120일 커버리지가 있습니다.',
        '- core.silver_meta_order_item은 이 tenant에서 현재 2026-03-22 전후까지만 커버됩니다.',
        '- dma.silver_fact_product와 dma.silver_order_with_utm는 120일 커버리지가 있습니다.',
        '- dma.gold_pgm_product_transition_edge와 dma.gold_pgm_return_gravity_loop_detail은 대략 2026-04-06부터 2026-04-17까지만 커버됩니다.',
        '- dma.gold_pgm_scored는 대략 2026-04-05부터 2026-04-17까지만 커버됩니다.',
        '- raw_extract 안의 기존 CSV 파일이 유지해야 할 계약입니다.'
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
