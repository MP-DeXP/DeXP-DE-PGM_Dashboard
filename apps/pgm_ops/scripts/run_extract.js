import fs from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { ARTIFACT_DIR_URLS } from '../app/config/paths.js';
import { RAW_INPUT_FILES, QA_FILES } from '../app/config/constants.js';
import { parseCsv, stringifyCsv } from '../app/loaders/csv_parser.js';
import { buildRawExtractManifest, prepareRawArtifacts } from '../app/pipeline/build_all.js';
import { parsePipelineCliArgs } from './pipeline_cli.js';
import { refreshRawExtractFromRosetta } from './run_rosetta_refresh.js';

async function readRawArtifacts() {
    const rawDir = fileURLToPath(ARTIFACT_DIR_URLS.raw_extract);
    const artifacts = {};

    await Promise.all(Object.entries(RAW_INPUT_FILES).map(async ([key, filename]) => {
        try {
            const text = await fs.readFile(`${rawDir}/${filename}`, 'utf8');
            artifacts[key] = parseCsv(text);
        } catch (error) {
            artifacts[key] = [];
        }
    }));

    return artifacts;
}

export async function main(argv = process.argv.slice(2)) {
    const cliOptions = parsePipelineCliArgs(argv);
    const qaDir = fileURLToPath(ARTIFACT_DIR_URLS.qa);
    const rawDir = fileURLToPath(ARTIFACT_DIR_URLS.raw_extract);

    if (cliOptions.refreshRosetta) {
        await refreshRawExtractFromRosetta(cliOptions);
    }

    const rawArtifacts = await readRawArtifacts();
    const { rawArtifacts: preparedRawArtifacts, extractContext } = prepareRawArtifacts(rawArtifacts, cliOptions);
    const manifest = buildRawExtractManifest(preparedRawArtifacts, { extractContext });

    await Promise.all(Object.entries(extractContext.syntheticArtifacts ?? {}).map(async ([artifactName]) => {
        const filename = RAW_INPUT_FILES[artifactName];
        if (!filename) {
            return;
        }

        if (!(preparedRawArtifacts[artifactName] ?? []).length) {
            return;
        }

        await fs.writeFile(`${rawDir}/${filename}`, stringifyCsv(preparedRawArtifacts[artifactName] ?? []), 'utf8');
    }));

    await fs.writeFile(`${qaDir}/${QA_FILES.raw_extract_manifest}`, stringifyCsv(manifest), 'utf8');
    console.log(`as_of_date=${extractContext.asOfDate || 'n/a'} lookback_days=${extractContext.lookbackDays} role_history_mode=${extractContext.roleHistoryMode}`);
    if (extractContext.warnings?.length) {
        extractContext.warnings.forEach((warning) => {
            console.warn(`[run_extract] ${warning.code}: ${warning.message}`);
        });
    }
    console.log(`Wrote ${QA_FILES.raw_extract_manifest}`);
}

if (import.meta.url === new URL(process.argv[1], 'file:').href) {
    await main();
}
