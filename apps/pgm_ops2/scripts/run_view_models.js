import { ARTIFACT_DIR_URLS } from '../app/config/paths.js';
import { MART_FILE_NAMES, VIEW_MODEL_FILE_NAMES } from '../app/config/constants.js';
import { readCsvFile, writeCsvFile } from '../app/loaders/files.js';
import { buildViewModelArtifacts } from '../app/pipeline/build_all.js';

async function readMartArtifacts() {
    const artifacts = {};
    await Promise.all(Object.entries(MART_FILE_NAMES).map(async ([key, filename]) => {
        artifacts[key] = await readCsvFile(new URL(filename, ARTIFACT_DIR_URLS.mart));
    }));
    return artifacts;
}

export async function main() {
    const martArtifacts = await readMartArtifacts();
    const viewModelArtifacts = buildViewModelArtifacts(martArtifacts);

    await Promise.all(
        Object.entries(VIEW_MODEL_FILE_NAMES).map(([key, filename]) => {
            return writeCsvFile(new URL(filename, ARTIFACT_DIR_URLS.view_model), viewModelArtifacts[key] ?? []);
        })
    );
}

if (import.meta.url === new URL(process.argv[1], 'file:').href) {
    await main();
}
