import fs from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { ARTIFACT_DIR_URLS } from '../app/config/paths.js';
import { RAW_INPUT_FILES, VIEW_MODEL_FILES, STAGING_FILES, MART_FILES } from '../app/config/constants.js';
import { parseCsv, stringifyCsv } from '../app/loaders/csv_parser.js';
import { buildStagingArtifacts, buildMartArtifacts, buildViewModelArtifacts } from '../app/pipeline/build_all.js';

async function loadRawArtifacts() {
    const rawDir = fileURLToPath(ARTIFACT_DIR_URLS.raw_extract);
    const entries = await Promise.all(
        Object.entries(RAW_INPUT_FILES).map(async ([key, filename]) => {
            try {
                const text = await fs.readFile(`${rawDir}/${filename}`, 'utf8');
                return [key, parseCsv(text)];
            } catch (error) {
                return [key, []];
            }
        })
    );

    return Object.fromEntries(entries);
}

async function writeLayer(directoryUrl, fileMap, artifacts) {
    const directory = fileURLToPath(directoryUrl);
    await Promise.all(Object.entries(fileMap).map(async ([key, filename]) => {
        await fs.writeFile(`${directory}/${filename}`, stringifyCsv(artifacts[key] ?? []), 'utf8');
    }));
}

export async function main() {
    const rawArtifacts = await loadRawArtifacts();
    const stagingArtifacts = buildStagingArtifacts(rawArtifacts);
    const martArtifacts = buildMartArtifacts(stagingArtifacts);
    const viewModelArtifacts = buildViewModelArtifacts(martArtifacts, stagingArtifacts);

    await writeLayer(ARTIFACT_DIR_URLS.staging, STAGING_FILES, stagingArtifacts);
    await writeLayer(ARTIFACT_DIR_URLS.mart, MART_FILES, martArtifacts);
    await writeLayer(ARTIFACT_DIR_URLS.view_model, VIEW_MODEL_FILES, viewModelArtifacts);

    console.log('Wrote staging, mart, and view_model artifacts.');
}

if (import.meta.url === new URL(process.argv[1], 'file:').href) {
    await main();
}
