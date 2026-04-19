import { fileURLToPath } from 'node:url';

import { ARTIFACT_DIR_URLS } from '../app/config/paths.js';
import { RAW_FILE_NAMES, QA_FILE_NAMES, RAW_METADATA_FILE_NAME } from '../app/config/constants.js';
import { readCsvFile, writeCsvFile } from '../app/loaders/files.js';
import { buildRawManifest } from '../app/pipeline/build_all.js';
import { parsePipelineCliArgs } from './pipeline_cli.js';
import { refreshRawExtractFromRosetta } from './run_rosetta_refresh.js';

async function readRawArtifacts() {
    const artifacts = {};

    await Promise.all(Object.entries(RAW_FILE_NAMES).map(async ([key, filename]) => {
        artifacts[key] = await readCsvFile(new URL(filename, ARTIFACT_DIR_URLS.raw_rosetta));
    }));

    return artifacts;
}

export async function main(argv = process.argv.slice(2)) {
    const options = parsePipelineCliArgs(argv);

    if (options.refreshRosetta) {
        await refreshRawExtractFromRosetta(options);
    }

    const rawArtifacts = await readRawArtifacts();
    const rawRefreshStatus = await readCsvFile(new URL(RAW_METADATA_FILE_NAME, ARTIFACT_DIR_URLS.raw_rosetta));
    await writeCsvFile(
        new URL(QA_FILE_NAMES.raw_manifest, ARTIFACT_DIR_URLS.qa),
        buildRawManifest(rawArtifacts, rawRefreshStatus)
    );
    console.log(`Wrote ${fileURLToPath(new URL(QA_FILE_NAMES.raw_manifest, ARTIFACT_DIR_URLS.qa))}`);
}

if (import.meta.url === new URL(process.argv[1], 'file:').href) {
    await main();
}
