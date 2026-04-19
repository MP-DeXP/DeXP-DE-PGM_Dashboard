import fs from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { ARTIFACT_DIR_URLS } from '../app/config/paths.js';
import { MART_FILES, QA_FILES, RAW_INPUT_FILES, STAGING_FILES, VIEW_MODEL_FILES } from '../app/config/constants.js';
import { parseCsv, stringifyCsv } from '../app/loaders/csv_parser.js';
import { buildCoverageReport, buildRawExtractManifest, buildValidationReport, buildValidationSummary, prepareRawArtifacts } from '../app/pipeline/build_all.js';
import { parsePipelineCliArgs } from './pipeline_cli.js';

async function loadLayer(directoryUrl, fileMap) {
    const directory = fileURLToPath(directoryUrl);
    const entries = await Promise.all(
        Object.entries(fileMap).map(async ([key, filename]) => {
            try {
                const text = await fs.readFile(`${directory}/${filename}`, 'utf8');
                return [key, parseCsv(text)];
            } catch (error) {
                return [key, []];
            }
        })
    );

    return Object.fromEntries(entries);
}

export async function main(argv = process.argv.slice(2)) {
    const cliOptions = parsePipelineCliArgs(argv);
    const rawArtifacts = await loadLayer(ARTIFACT_DIR_URLS.raw_extract, RAW_INPUT_FILES);
    const { rawArtifacts: preparedRawArtifacts, extractContext } = prepareRawArtifacts(rawArtifacts, cliOptions);
    const stagingArtifacts = await loadLayer(ARTIFACT_DIR_URLS.staging, STAGING_FILES);
    const martArtifacts = await loadLayer(ARTIFACT_DIR_URLS.mart, MART_FILES);
    const viewModelArtifacts = await loadLayer(ARTIFACT_DIR_URLS.view_model, VIEW_MODEL_FILES);
    const rawManifest = buildRawExtractManifest(preparedRawArtifacts, { extractContext });
    const validationSummary = buildValidationSummary({
        staging: stagingArtifacts,
        mart: martArtifacts,
        view_model: viewModelArtifacts
    });
    const coverageReport = buildCoverageReport(martArtifacts, {
        rawArtifacts: preparedRawArtifacts,
        extractContext,
        manifest: rawManifest
    });
    const validationReport = buildValidationReport(validationSummary, coverageReport);
    const qaDir = fileURLToPath(ARTIFACT_DIR_URLS.qa);
    const rawDir = fileURLToPath(ARTIFACT_DIR_URLS.raw_extract);

    await fs.writeFile(`${qaDir}/${QA_FILES.validation_summary}`, stringifyCsv(validationSummary), 'utf8');
    await fs.writeFile(`${qaDir}/${QA_FILES.coverage_report}`, stringifyCsv(coverageReport), 'utf8');
    await fs.writeFile(`${qaDir}/${QA_FILES.validation_report}`, validationReport, 'utf8');

    const missingRaw = await Promise.all(Object.values(RAW_INPUT_FILES).map(async (filename) => {
        try {
            await fs.access(`${rawDir}/${filename}`);
            return null;
        } catch (error) {
            return filename;
        }
    }));

    if (missingRaw.filter(Boolean).length) {
        console.warn(`Missing raw inputs: ${missingRaw.filter(Boolean).join(', ')}`);
    }

    console.log('Wrote validation outputs.');
}

if (import.meta.url === new URL(process.argv[1], 'file:').href) {
    await main();
}
