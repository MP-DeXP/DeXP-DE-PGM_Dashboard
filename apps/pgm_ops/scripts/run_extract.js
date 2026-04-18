import fs from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { ARTIFACT_DIR_URLS } from '../app/config/paths.js';
import { RAW_INPUT_FILES, QA_FILES } from '../app/config/constants.js';
import { parseCsv, stringifyCsv } from '../app/loaders/csv_parser.js';
import { buildRawExtractManifest } from '../app/pipeline/build_all.js';

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

export async function main() {
    const qaDir = fileURLToPath(ARTIFACT_DIR_URLS.qa);
    const rawArtifacts = await readRawArtifacts();
    const manifest = buildRawExtractManifest(rawArtifacts);

    await fs.writeFile(`${qaDir}/${QA_FILES.raw_extract_manifest}`, stringifyCsv(manifest), 'utf8');
    console.log(`Wrote ${QA_FILES.raw_extract_manifest}`);
}

if (import.meta.url === new URL(process.argv[1], 'file:').href) {
    await main();
}
