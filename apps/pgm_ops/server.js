import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { promises as fs } from 'node:fs';

import { parseCsv } from './app/loaders/csv_parser.js';
import { MART_FILES, QA_FILES, VIEW_MODEL_FILES } from './app/config/constants.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const APP_ROOT = __dirname;
const REPO_ROOT = path.resolve(APP_ROOT, '..', '..');
const ARTIFACT_ROOT = path.join(APP_ROOT, 'artifacts');
const HOST = process.env.HOST ?? '127.0.0.1';
const PORT = Number(process.env.PORT ?? 8000);

const MIME_TYPES = {
    '.css': 'text/css; charset=utf-8',
    '.csv': 'text/csv; charset=utf-8',
    '.html': 'text/html; charset=utf-8',
    '.ico': 'image/x-icon',
    '.js': 'text/javascript; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.md': 'text/markdown; charset=utf-8',
    '.svg': 'image/svg+xml',
    '.txt': 'text/plain; charset=utf-8'
};

function send(res, statusCode, headers, body) {
    res.writeHead(statusCode, headers);
    if (body === null || body === undefined) {
        res.end();
        return;
    }

    res.end(body);
}

function contentTypeFor(filePath) {
    return MIME_TYPES[path.extname(filePath).toLowerCase()] ?? 'application/octet-stream';
}

async function readText(filePath) {
    return fs.readFile(filePath, 'utf8');
}

async function pathExists(filePath) {
    try {
        await fs.access(filePath);
        return true;
    } catch {
        return false;
    }
}

function isSafeSegment(value) {
    return Boolean(value) && !value.includes('..') && !value.includes('/') && !value.includes('\\');
}

function listArtifactFiles(layerDir, filenames) {
    return Promise.all(
        filenames.map(async (filename) => ({
            filename,
            exists: await pathExists(path.join(layerDir, filename))
        }))
    );
}

function summarizeStatusCounts(rows) {
    return rows.reduce(
        (accumulator, row) => {
            const status = String(row.status ?? '').toLowerCase();
            if (status === 'pass') {
                accumulator.pass += 1;
            } else if (status === 'warn') {
                accumulator.warn += 1;
            } else if (status === 'fail') {
                accumulator.fail += 1;
            }
            return accumulator;
        },
        { pass: 0, warn: 0, fail: 0 }
    );
}

function findLatestDate(rows) {
    const candidateKeys = ['as_of_date', 'snapshot_date', 'date', 'order_date'];
    let latest = null;

    rows.forEach((row) => {
        candidateKeys.forEach((key) => {
            const value = row[key];
            if (!value) {
                return;
            }

            if (!latest || String(value) > latest) {
                latest = String(value);
            }
        });
    });

    return latest;
}

async function readCsvRows(filePath) {
    const text = await readText(filePath);
    return parseCsv(text);
}

async function getLatestSnapshotDate() {
    const candidates = [
        path.join(ARTIFACT_ROOT, 'view_model', VIEW_MODEL_FILES.overview_daily_cards),
        path.join(ARTIFACT_ROOT, 'mart', MART_FILES.product_daily_metrics),
        path.join(ARTIFACT_ROOT, 'raw_extract', 'product_daily.csv'),
        path.join(ARTIFACT_ROOT, 'raw_extract', 'orders.csv')
    ];

    for (const candidate of candidates) {
        if (!(await pathExists(candidate))) {
            continue;
        }

        const rows = await readCsvRows(candidate);
        const latest = findLatestDate(rows);
        if (latest) {
            return latest;
        }
    }

    return null;
}

async function buildLoadStatus() {
    const validationSummaryPath = path.join(ARTIFACT_ROOT, 'qa', QA_FILES.validation_summary);
    const coverageReportPath = path.join(ARTIFACT_ROOT, 'qa', QA_FILES.coverage_report);
    const rawManifestPath = path.join(ARTIFACT_ROOT, 'qa', QA_FILES.raw_extract_manifest);

    const [validationSummary, coverageReport, rawManifest, latestSnapshotDate, viewModelArtifacts, martArtifacts, qaArtifacts] = await Promise.all([
        pathExists(validationSummaryPath) ? readCsvRows(validationSummaryPath) : Promise.resolve([]),
        pathExists(coverageReportPath) ? readCsvRows(coverageReportPath) : Promise.resolve([]),
        pathExists(rawManifestPath) ? readCsvRows(rawManifestPath) : Promise.resolve([]),
        getLatestSnapshotDate(),
        listArtifactFiles(path.join(ARTIFACT_ROOT, 'view_model'), Object.values(VIEW_MODEL_FILES)),
        listArtifactFiles(path.join(ARTIFACT_ROOT, 'mart'), Object.values(MART_FILES)),
        listArtifactFiles(path.join(ARTIFACT_ROOT, 'qa'), Object.values(QA_FILES))
    ]);

    const validationCounts = summarizeStatusCounts(validationSummary);
    const availableRawArtifacts = rawManifest.filter((row) => String(row.exists).toLowerCase() === 'true').length;

    return {
        app: 'pgm_ops',
        mode: 'artifact',
        sourceLabel: '실데이터',
        chromeNote: '최신 동기화 기준',
        latestSnapshotDate,
        generatedAt: new Date().toISOString(),
        artifactRoot: 'apps/pgm_ops/artifacts',
        qaSummary: validationCounts,
        rawExtractManifest: {
            required: rawManifest.length,
            available: availableRawArtifacts
        },
        artifacts: {
            view_model: viewModelArtifacts,
            mart: martArtifacts,
            qa: qaArtifacts
        },
        coverage: Object.fromEntries(
            coverageReport.map((row) => [row.metric_name, { value: row.metric_value, message: row.message }])
        ),
        note: '화면은 최신 동기화 결과를 그대로 읽어 표시합니다.'
    };
}

async function readArtifactFile(layer, artifactName) {
    const filePath = path.join(ARTIFACT_ROOT, layer, artifactName);
    const exists = await pathExists(filePath);
    if (!exists) {
        return null;
    }

    return {
        filePath,
        body: await readText(filePath)
    };
}

async function serveArtifact(res, layer, artifactName) {
    if (!isSafeSegment(artifactName)) {
        send(res, 400, { 'Content-Type': 'text/plain; charset=utf-8' }, 'Invalid artifact name');
        return;
    }

    const normalizedName = artifactName.endsWith('.csv') || artifactName.endsWith('.md') || artifactName.endsWith('.json')
        ? artifactName
        : `${artifactName}.csv`;

    const artifact = await readArtifactFile(layer, normalizedName);
    if (!artifact) {
        send(res, 404, { 'Content-Type': 'text/plain; charset=utf-8' }, 'Artifact not found');
        return;
    }

    send(res, 200, { 'Content-Type': contentTypeFor(artifact.filePath), 'Cache-Control': 'no-store' }, artifact.body);
}

async function serveQaArtifact(res, artifactName) {
    if (!isSafeSegment(artifactName)) {
        send(res, 400, { 'Content-Type': 'text/plain; charset=utf-8' }, 'Invalid artifact name');
        return;
    }

    const candidates = artifactName.includes('.')
        ? [artifactName]
        : [`${artifactName}.csv`, `${artifactName}.md`, `${artifactName}.json`, artifactName];

    for (const candidate of candidates) {
        const artifact = await readArtifactFile('qa', candidate);
        if (artifact) {
            send(res, 200, { 'Content-Type': contentTypeFor(artifact.filePath), 'Cache-Control': 'no-store' }, artifact.body);
            return;
        }
    }

    send(res, 404, { 'Content-Type': 'text/plain; charset=utf-8' }, 'QA artifact not found');
}

async function serveStaticFile(res, requestPath, search = '') {
    const decodedPath = decodeURIComponent(requestPath);
    const absoluteCandidate = path.resolve(REPO_ROOT, `.${decodedPath}`);
    const repoRootPrefix = `${REPO_ROOT}${path.sep}`;

    if (absoluteCandidate !== REPO_ROOT && !absoluteCandidate.startsWith(repoRootPrefix)) {
        send(res, 403, { 'Content-Type': 'text/plain; charset=utf-8' }, 'Forbidden');
        return;
    }

    let filePath = absoluteCandidate;
    if (await pathExists(filePath)) {
        const stat = await fs.stat(filePath);
        if (stat.isDirectory()) {
            if (!decodedPath.endsWith('/')) {
                send(res, 301, { Location: `${decodedPath}/${search}` }, null);
                return;
            }
            filePath = path.join(filePath, 'index.html');
        }
    } else if (decodedPath.endsWith('/')) {
        filePath = path.join(filePath, 'index.html');
    }

    if (!(await pathExists(filePath))) {
        send(res, 404, { 'Content-Type': 'text/plain; charset=utf-8' }, 'Not found');
        return;
    }

    const body = await readText(filePath);
    send(res, 200, { 'Content-Type': contentTypeFor(filePath), 'Cache-Control': 'no-store' }, body);
}

async function handleRequest(req, res) {
    if (req.method !== 'GET' && req.method !== 'HEAD') {
        send(res, 405, { Allow: 'GET, HEAD', 'Content-Type': 'text/plain; charset=utf-8' }, 'Method not allowed');
        return;
    }

    const requestUrl = new URL(req.url, `http://${req.headers.host ?? `${HOST}:${PORT}`}`);
    const { pathname, search } = requestUrl;

    if (pathname === '/api/pgm-ops/meta/load-status.json') {
        const loadStatus = await buildLoadStatus();
        send(res, 200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }, JSON.stringify(loadStatus, null, 2));
        return;
    }

    if (pathname.startsWith('/api/pgm-ops/view-model/')) {
        await serveArtifact(res, 'view_model', pathname.slice('/api/pgm-ops/view-model/'.length));
        return;
    }

    if (pathname.startsWith('/api/pgm-ops/mart/')) {
        await serveArtifact(res, 'mart', pathname.slice('/api/pgm-ops/mart/'.length));
        return;
    }

    if (pathname.startsWith('/api/pgm-ops/qa/')) {
        await serveQaArtifact(res, pathname.slice('/api/pgm-ops/qa/'.length));
        return;
    }

    await serveStaticFile(res, pathname === '/' ? '/index.html' : pathname, search);
}

const server = http.createServer((req, res) => {
    handleRequest(req, res).catch((error) => {
        console.error(error);
        send(res, 500, { 'Content-Type': 'text/plain; charset=utf-8' }, 'Internal server error');
    });
});

server.listen(PORT, HOST, () => {
    console.log(`pgm_ops CSV server listening on http://${HOST}:${PORT}`);
});
