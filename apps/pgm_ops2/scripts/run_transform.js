import { ARTIFACT_DIR_URLS } from '../app/config/paths.js';
import { MART_FILE_NAMES, RAW_FILE_NAMES, STAGING_FILE_NAMES } from '../app/config/constants.js';
import { readCsvFile, writeCsvFile } from '../app/loaders/files.js';
import { buildMartArtifacts, buildStagingArtifacts } from '../app/pipeline/build_all.js';
import { parsePipelineCliArgs } from './pipeline_cli.js';

async function readRawArtifacts() {
    const artifacts = {};
    await Promise.all(Object.entries(RAW_FILE_NAMES).map(async ([key, filename]) => {
        artifacts[key] = await readCsvFile(new URL(filename, ARTIFACT_DIR_URLS.raw_rosetta));
    }));
    return artifacts;
}

export async function main(argv = process.argv.slice(2)) {
    const options = parsePipelineCliArgs(argv);
    const rawArtifacts = await readRawArtifacts();
    const stagingArtifacts = buildStagingArtifacts(rawArtifacts);
    const martArtifacts = buildMartArtifacts(stagingArtifacts, rawArtifacts, options);

    await Promise.all([
        ...Object.entries(STAGING_FILE_NAMES).map(([key, filename]) => writeCsvFile(new URL(filename, ARTIFACT_DIR_URLS.staging), stagingArtifacts[key] ?? [])),
        ...Object.entries(MART_FILE_NAMES).map(([key, filename]) => writeCsvFile(new URL(filename, ARTIFACT_DIR_URLS.mart), martArtifacts[key] ?? []))
    ]);
}

if (import.meta.url === new URL(process.argv[1], 'file:').href) {
    await main();
}
