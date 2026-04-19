import fs from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

import { parseCsv, stringifyCsv } from './csv.js';

export async function ensureDir(url) {
    await fs.mkdir(fileURLToPath(url), { recursive: true });
}

export async function readCsvFile(url) {
    try {
        const text = await fs.readFile(fileURLToPath(url), 'utf8');
        return parseCsv(text);
    } catch {
        return [];
    }
}

export async function writeCsvFile(url, rows, columns = null) {
    await ensureDir(new URL('./', url));
    await fs.writeFile(fileURLToPath(url), stringifyCsv(rows, columns), 'utf8');
}

export async function writeTextFile(url, text) {
    await ensureDir(new URL('./', url));
    await fs.writeFile(fileURLToPath(url), text, 'utf8');
}

export async function readTextFile(url) {
    return fs.readFile(fileURLToPath(url), 'utf8');
}

export async function exists(url) {
    try {
        await fs.access(fileURLToPath(url));
        return true;
    } catch {
        return false;
    }
}
