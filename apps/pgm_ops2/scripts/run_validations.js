import { ARTIFACT_DIR_URLS } from '../app/config/paths.js';
import { MART_FILE_NAMES, QA_FILE_NAMES, VIEW_MODEL_FILE_NAMES } from '../app/config/constants.js';
import { readCsvFile, readTextFile, writeCsvFile, writeTextFile } from '../app/loaders/files.js';
import {
    buildImplementationScopeRows,
    buildToneAuditRows,
    buildValidationReport,
    buildValidationSummary
} from '../app/pipeline/build_all.js';

async function readMartArtifacts() {
    const artifacts = {};
    await Promise.all(Object.entries(MART_FILE_NAMES).map(async ([key, filename]) => {
        artifacts[key] = await readCsvFile(new URL(filename, ARTIFACT_DIR_URLS.mart));
    }));
    return artifacts;
}

async function readViewModelArtifacts() {
    const artifacts = {};
    await Promise.all(Object.entries(VIEW_MODEL_FILE_NAMES).map(async ([key, filename]) => {
        artifacts[key] = await readCsvFile(new URL(filename, ARTIFACT_DIR_URLS.view_model));
    }));
    return artifacts;
}

async function readRawManifest() {
    return readCsvFile(new URL(QA_FILE_NAMES.raw_manifest, ARTIFACT_DIR_URLS.qa));
}

async function buildToneAudit() {
    const uiTextFiles = [
        new URL('../index.html', import.meta.url),
        new URL('../pgm_ops2.js', import.meta.url),
        new URL('../pgm_ops2.css', import.meta.url)
    ];
    const baseRows = buildToneAuditRows();

    const texts = await Promise.all(uiTextFiles.map(async (url) => {
        try {
            return await readTextFile(url);
        } catch {
            return '';
        }
    }));
    const combined = texts.join('\n');

    return baseRows.map((row) => ({
        ...row,
        status: combined.includes(row.term) ? 'fail' : 'pass'
    }));
}

export async function main() {
    const [martArtifacts, viewModelArtifacts, rawManifestRows] = await Promise.all([
        readMartArtifacts(),
        readViewModelArtifacts(),
        readRawManifest()
    ]);
    const validationSummary = buildValidationSummary(martArtifacts, viewModelArtifacts, rawManifestRows);
    const toneAudit = await buildToneAudit();

    await Promise.all([
        writeCsvFile(new URL(QA_FILE_NAMES.validation_summary, ARTIFACT_DIR_URLS.qa), validationSummary),
        writeTextFile(new URL(QA_FILE_NAMES.validation_report, ARTIFACT_DIR_URLS.qa), buildValidationReport(validationSummary)),
        writeCsvFile(new URL(QA_FILE_NAMES.tone_audit, ARTIFACT_DIR_URLS.qa), toneAudit),
        writeCsvFile(new URL(QA_FILE_NAMES.implementation_scope, ARTIFACT_DIR_URLS.qa), buildImplementationScopeRows())
    ]);
}

if (import.meta.url === new URL(process.argv[1], 'file:').href) {
    await main();
}
