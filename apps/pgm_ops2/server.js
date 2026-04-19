import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { promises as fs } from 'node:fs';

import { parseCsv } from './app/loaders/csv.js';
import { MART_FILE_NAMES, QA_FILE_NAMES, VIEW_MODEL_FILE_NAMES } from './app/config/constants.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const APP_ROOT = __dirname;
const REPO_ROOT = path.resolve(APP_ROOT, '..', '..');
const ARTIFACT_ROOT = path.join(APP_ROOT, 'artifacts');
const HOST = process.env.HOST ?? '127.0.0.1';
const PORT = Number(process.env.PORT ?? 8012);

const MIME_TYPES = {
    '.css': 'text/css; charset=utf-8',
    '.csv': 'text/csv; charset=utf-8',
    '.html': 'text/html; charset=utf-8',
    '.js': 'text/javascript; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.md': 'text/markdown; charset=utf-8',
    '.txt': 'text/plain; charset=utf-8'
};

function send(res, statusCode, headers, body) {
    res.writeHead(statusCode, headers);
    res.end(body);
}

function contentTypeFor(filePath) {
    return MIME_TYPES[path.extname(filePath).toLowerCase()] ?? 'application/octet-stream';
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

async function readText(filePath) {
    return fs.readFile(filePath, 'utf8');
}

async function readCsvRows(filePath) {
    if (!(await pathExists(filePath))) {
        return [];
    }
    return parseCsv(await readText(filePath));
}

async function readArtifactLayer(layerName, fileMap) {
    const layerDir = path.join(ARTIFACT_ROOT, layerName);
    const entries = await Promise.all(
        Object.entries(fileMap).map(async ([key, filename]) => {
            const filePath = path.join(layerDir, filename);
            if (!(await pathExists(filePath))) {
                return [key, []];
            }

            if (path.extname(filename).toLowerCase() === '.md') {
                return [key, await readText(filePath)];
            }

            return [key, await readCsvRows(filePath)];
        })
    );
    return Object.fromEntries(entries);
}

function summarizeQueue(queueRows) {
    const counts = queueRows.reduce((accumulator, row) => {
        const key = String(row.priority_level ?? '기타');
        accumulator[key] = (accumulator[key] ?? 0) + 1;
        return accumulator;
    }, {});
    return counts;
}

async function buildBundle() {
    const [viewModel, mart, qa] = await Promise.all([
        readArtifactLayer('view_model', VIEW_MODEL_FILE_NAMES),
        readArtifactLayer('mart', MART_FILE_NAMES),
        readArtifactLayer('qa', QA_FILE_NAMES)
    ]);

    const queueRows = viewModel.vm_priority_queue ?? [];
    const healthRows = viewModel.vm_data_health ?? [];
    const hasRealData = healthRows.some((row) => Number(row.row_count ?? 0) > 0);

    return {
        app: 'pgm_ops2',
        generated_at: new Date().toISOString(),
        queue_counts: summarizeQueue(queueRows),
        latest_as_of_date: queueRows[0]?.as_of_date ?? '',
        raw_data_status: hasRealData ? 'real_source_loaded' : 'raw_source_missing',
        view_model: viewModel,
        mart,
        qa
    };
}

async function serveArtifact(res, layer, artifactName) {
    if (!isSafeSegment(artifactName)) {
        send(res, 400, { 'Content-Type': 'text/plain; charset=utf-8' }, 'Invalid artifact name');
        return;
    }

    const normalizedName = artifactName.includes('.') ? artifactName : `${artifactName}.csv`;
    const filePath = path.join(ARTIFACT_ROOT, layer, normalizedName);
    if (!(await pathExists(filePath))) {
        send(res, 404, { 'Content-Type': 'text/plain; charset=utf-8' }, 'Artifact not found');
        return;
    }

    send(res, 200, { 'Content-Type': contentTypeFor(filePath), 'Cache-Control': 'no-store' }, await readText(filePath));
}

async function serveStaticFile(res, requestPath) {
    const decodedPath = decodeURIComponent(requestPath);
    const absoluteCandidate = path.resolve(APP_ROOT, `.${decodedPath}`);
    const appRootPrefix = `${APP_ROOT}${path.sep}`;

    if (absoluteCandidate !== APP_ROOT && !absoluteCandidate.startsWith(appRootPrefix)) {
        send(res, 403, { 'Content-Type': 'text/plain; charset=utf-8' }, 'Forbidden');
        return;
    }

    let filePath = absoluteCandidate;
    if (await pathExists(filePath)) {
        const stat = await fs.stat(filePath);
        if (stat.isDirectory()) {
            filePath = path.join(filePath, 'index.html');
        }
    }

    if (!(await pathExists(filePath))) {
        send(res, 404, { 'Content-Type': 'text/plain; charset=utf-8' }, 'Not found');
        return;
    }

    send(res, 200, { 'Content-Type': contentTypeFor(filePath), 'Cache-Control': 'no-store' }, await fs.readFile(filePath));
}

const server = http.createServer(async (req, res) => {
    try {
        const requestUrl = new URL(req.url ?? '/', `http://${req.headers.host ?? `${HOST}:${PORT}`}`);
        const { pathname } = requestUrl;

        if (pathname === '/api/pgm-ops2/bundle') {
            send(res, 200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }, JSON.stringify(await buildBundle()));
            return;
        }

        if (pathname === '/api/pgm-ops2/load-status') {
            const bundle = await buildBundle();
            send(res, 200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' }, JSON.stringify({
                app: bundle.app,
                generated_at: bundle.generated_at,
                latest_as_of_date: bundle.latest_as_of_date,
                queue_counts: bundle.queue_counts,
                raw_data_status: bundle.raw_data_status
            }));
            return;
        }

        if (pathname.startsWith('/api/pgm-ops2/view-model/')) {
            await serveArtifact(res, 'view_model', pathname.split('/').at(-1));
            return;
        }

        if (pathname.startsWith('/api/pgm-ops2/mart/')) {
            await serveArtifact(res, 'mart', pathname.split('/').at(-1));
            return;
        }

        if (pathname.startsWith('/api/pgm-ops2/qa/')) {
            await serveArtifact(res, 'qa', pathname.split('/').at(-1));
            return;
        }

        if (pathname === '/') {
            await serveStaticFile(res, '/index.html');
            return;
        }

        await serveStaticFile(res, pathname);
    } catch (error) {
        send(res, 500, { 'Content-Type': 'text/plain; charset=utf-8' }, error.stack ?? String(error));
    }
});

server.listen(PORT, HOST, () => {
    console.log(`pgm_ops2 available at http://${HOST}:${PORT}`);
});
