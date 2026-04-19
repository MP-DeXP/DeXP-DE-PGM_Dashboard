import { spawn } from 'node:child_process';
import os from 'node:os';

import { parse as parseYaml } from 'yaml';

const DEFAULT_PROTOCOL_VERSIONS = ['2025-03-26', '2024-11-05'];
const DEFAULT_BRIDGE_TIMEOUT_MS = 120000;

export const ROSETTA_ERROR_CODES = {
    auth_missing: 'auth_missing',
    mcp_bridge_failed: 'mcp_bridge_failed',
    query_failed: 'query_failed'
};

function isPlainObject(value) {
    return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function safeJsonParse(value) {
    try {
        return JSON.parse(value);
    } catch {
        return null;
    }
}

function parseMaybeStructuredText(value) {
    if (!value || typeof value !== 'string') {
        return null;
    }

    const jsonValue = safeJsonParse(value);
    if (jsonValue != null) {
        return jsonValue;
    }

    try {
        return parseYaml(value);
    } catch {
        return null;
    }
}

function looksLikeAuthFailure(message) {
    const lowered = String(message ?? '').toLowerCase();
    return [
        'auth',
        'oauth',
        'login',
        'consent',
        'unauthorized',
        'forbidden',
        'credential',
        'token'
    ].some((token) => lowered.includes(token));
}

function collectTextParts(content = []) {
    return content
        .flatMap((item) => {
            if (!item) {
                return [];
            }

            if (typeof item.text === 'string') {
                return [item.text];
            }

            if (typeof item.content === 'string') {
                return [item.content];
            }

            return [];
        })
        .filter(Boolean);
}

function parseSsePayload(rawText) {
    const payloads = [];
    const events = rawText.split(/\n\n+/).map((chunk) => chunk.trim()).filter(Boolean);

    events.forEach((eventBlock) => {
        const dataLines = eventBlock
            .split('\n')
            .filter((line) => line.startsWith('data:'))
            .map((line) => line.slice(5).trim())
            .filter((line) => line && line !== '[DONE]');

        if (!dataLines.length) {
            return;
        }

        const combined = dataLines.join('\n');
        const parsed = safeJsonParse(combined);
        if (parsed != null) {
            payloads.push(parsed);
        }
    });

    return payloads;
}

function normalizeRpcPayload(bodyText, contentType) {
    if (!bodyText) {
        return null;
    }

    if ((contentType ?? '').includes('text/event-stream')) {
        const ssePayloads = parseSsePayload(bodyText);
        return ssePayloads.at(-1) ?? null;
    }

    return safeJsonParse(bodyText);
}

function parseStructuredCandidate(candidate) {
    if (!candidate) {
        return null;
    }

    if (typeof candidate === 'string') {
        return parseMaybeStructuredText(candidate);
    }

    if (!isPlainObject(candidate)) {
        return null;
    }

    if (typeof candidate.result === 'string') {
        return parseMaybeStructuredText(candidate.result) ?? candidate;
    }

    return candidate;
}

function extractToolResult(payload) {
    if (!payload) {
        return null;
    }

    const candidateRoots = [payload.result, payload].filter(Boolean);

    for (const root of candidateRoots) {
        const structuredCandidates = [root.structuredContent, root.structured_content].filter(Boolean);
        for (const structuredCandidate of structuredCandidates) {
            const parsedStructured = parseStructuredCandidate(structuredCandidate);
            if (parsedStructured != null) {
                return parsedStructured.result ?? parsedStructured;
            }
        }

        if (Array.isArray(root.content)) {
            for (const item of root.content) {
                const itemStructuredCandidates = [item?.structuredContent, item?.structured_content, item?.json].filter(Boolean);
                for (const itemStructuredCandidate of itemStructuredCandidates) {
                    const parsedStructured = parseStructuredCandidate(itemStructuredCandidate);
                    if (parsedStructured != null) {
                        return parsedStructured.result ?? parsedStructured;
                    }
                }
            }

            const parsed = parseMaybeStructuredText(collectTextParts(root.content).join('\n'));
            if (parsed != null) {
                return parsed.result ?? parsed;
            }
        }

        if (typeof root.result === 'string') {
            const parsed = parseMaybeStructuredText(root.result);
            if (parsed != null) {
                return parsed.result ?? parsed;
            }
        }
    }

    return null;
}

function normalizeExecuteQueryResult(result) {
    const parsedResult = typeof result === 'string'
        ? parseMaybeStructuredText(result)
        : (typeof result?.result === 'string' ? parseMaybeStructuredText(result.result) : result);
    const normalizedResult = parsedResult ?? result;

    if (!isPlainObject(normalizedResult)) {
        throw new Error('Rosetta execute_query 응답을 해석하지 못했습니다.');
    }

    if (String(normalizedResult.status ?? '').toLowerCase() !== 'completed') {
        throw new Error(normalizedResult.reason ?? normalizedResult.message ?? 'Rosetta execute_query가 실패했습니다.');
    }

    return {
        columns: Array.isArray(normalizedResult.columns) ? normalizedResult.columns.map(String) : [],
        rows: Array.isArray(normalizedResult.rows) ? normalizedResult.rows.map((row) => {
            if (!isPlainObject(row)) {
                return {};
            }
            return Object.fromEntries(
                Object.entries(row).map(([key, value]) => [key, value == null ? '' : String(value)])
            );
        }) : [],
        rowCount: Number.parseInt(normalizedResult.row_count, 10) || 0,
        filteredSql: typeof normalizedResult.filtered_sql === 'string' ? normalizedResult.filtered_sql : '',
        status: String(normalizedResult.status ?? '')
    };
}

function buildCodexBridgePrompt({ connectionId, sql }) {
    return [
        'Act as a Rosetta MCP bridge.',
        'Call the rosetta MCP tool execute_query exactly once with the JSON arguments below.',
        'Do not call any other tool.',
        'After the tool call finishes, reply with only the word done.',
        '```json',
        JSON.stringify({
            connection_id: connectionId,
            sql
        }),
        '```'
    ].join('\n');
}

function parseCodexJsonEvents(stdoutText) {
    return stdoutText
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean)
        .map((line) => safeJsonParse(line))
        .filter(Boolean);
}

function tailText(value, limit = 1200) {
    const text = String(value ?? '').trim();
    if (!text) {
        return '';
    }
    return text.slice(-limit);
}

function resolveBridgeFailureCode(message) {
    return looksLikeAuthFailure(message) ? ROSETTA_ERROR_CODES.auth_missing : ROSETTA_ERROR_CODES.mcp_bridge_failed;
}

class RosettaBridgeError extends Error {
    constructor(code, message) {
        super(message);
        this.name = 'RosettaBridgeError';
        this.code = code;
    }
}

function normalizeBridgeError(error, fallbackMessage = 'Codex bridge 호출에 실패했습니다.') {
    if (error instanceof RosettaBridgeError) {
        return error;
    }

    const message = String(error?.message ?? fallbackMessage);
    return new RosettaBridgeError(resolveBridgeFailureCode(message), message);
}

function extractCodexToolCallResult(stdoutText) {
    const events = parseCodexJsonEvents(stdoutText);

    for (let index = events.length - 1; index >= 0; index -= 1) {
        const event = events[index];
        const item = event?.item;

        if (event?.type !== 'item.completed' || item?.type !== 'mcp_tool_call') {
            continue;
        }

        if (item.server !== 'rosetta' || item.tool !== 'execute_query') {
            continue;
        }

        if (item.error) {
            const detail = typeof item.error === 'string'
                ? item.error
                : (item.error.message ?? JSON.stringify(item.error));
            throw new RosettaBridgeError(
                looksLikeAuthFailure(detail) ? ROSETTA_ERROR_CODES.auth_missing : ROSETTA_ERROR_CODES.query_failed,
                detail || 'Codex bridge tool call failed.'
            );
        }

        return extractToolResult(item.result ?? item);
    }

    return null;
}

function parseBridgeTimeoutMs() {
    const rawValue = Number.parseInt(process.env.PGM_OPS2_ROSETTA_BRIDGE_TIMEOUT_MS, 10);
    return Number.isFinite(rawValue) && rawValue > 0 ? rawValue : DEFAULT_BRIDGE_TIMEOUT_MS;
}

function resolveBridgeCommand() {
    return process.env.PGM_OPS2_ROSETTA_BRIDGE_COMMAND ?? 'codex';
}

function resolveBridgeWorkdir() {
    const override = process.env.PGM_OPS2_ROSETTA_BRIDGE_CWD;
    return override && override.trim() ? override.trim() : os.tmpdir();
}

function runCodexBridge(prompt, { command, workdir, timeoutMs }) {
    return new Promise((resolve, reject) => {
        const child = spawn(command, [
            'exec',
            '--skip-git-repo-check',
            '--ephemeral',
            '--sandbox',
            'read-only',
            '--color',
            'never',
            '--json',
            '-C',
            workdir,
            '-'
        ], {
            stdio: ['pipe', 'pipe', 'pipe']
        });

        let stdout = '';
        let stderr = '';
        let timedOut = false;

        const timer = setTimeout(() => {
            timedOut = true;
            child.kill('SIGTERM');
            setTimeout(() => {
                child.kill('SIGKILL');
            }, 1000).unref();
        }, timeoutMs);

        child.stdout.on('data', (chunk) => {
            stdout += chunk.toString();
        });

        child.stderr.on('data', (chunk) => {
            stderr += chunk.toString();
        });

        child.on('error', (error) => {
            clearTimeout(timer);
            reject(new RosettaBridgeError(
                ROSETTA_ERROR_CODES.mcp_bridge_failed,
                `Codex bridge 프로세스를 시작하지 못했습니다: ${error.message ?? error}`
            ));
        });

        child.on('close', (code) => {
            clearTimeout(timer);

            if (timedOut) {
                reject(new RosettaBridgeError(
                    ROSETTA_ERROR_CODES.mcp_bridge_failed,
                    `Codex bridge 호출이 ${timeoutMs}ms 안에 끝나지 않았습니다.`
                ));
                return;
            }

            if (code !== 0) {
                const detail = tailText(stderr) || tailText(stdout) || `exit code ${code}`;
                reject(new RosettaBridgeError(resolveBridgeFailureCode(detail), `Codex bridge 종료 실패: ${detail}`));
                return;
            }

            resolve({ stdout, stderr });
        });

        child.stdin.end(prompt);
    });
}

export class RosettaCodexBridgeClient {
    constructor({
        command = resolveBridgeCommand(),
        workdir = resolveBridgeWorkdir(),
        timeoutMs = parseBridgeTimeoutMs()
    } = {}) {
        this.command = command;
        this.workdir = workdir;
        this.timeoutMs = timeoutMs;
    }

    async executeQuery({ connectionId, sql }) {
        try {
            const { stdout } = await runCodexBridge(
                buildCodexBridgePrompt({ connectionId, sql }),
                {
                    command: this.command,
                    workdir: this.workdir,
                    timeoutMs: this.timeoutMs
                }
            );

            const result = extractCodexToolCallResult(stdout);
            if (!result) {
                throw new RosettaBridgeError(
                    ROSETTA_ERROR_CODES.mcp_bridge_failed,
                    `Codex bridge 결과에서 execute_query 응답을 찾지 못했습니다: ${tailText(stdout)}`
                );
            }

            try {
                return normalizeExecuteQueryResult(result);
            } catch (error) {
                throw new RosettaBridgeError(
                    ROSETTA_ERROR_CODES.query_failed,
                    String(error.message ?? error)
                );
            }
        } catch (error) {
            const normalizedError = normalizeBridgeError(error);
            if (
                normalizedError.code === ROSETTA_ERROR_CODES.mcp_bridge_failed
                || normalizedError.code === ROSETTA_ERROR_CODES.auth_missing
            ) {
                throw normalizedError;
            }
            throw new RosettaBridgeError(ROSETTA_ERROR_CODES.query_failed, normalizedError.message);
        }
    }
}

export class RosettaMcpClient {
    constructor({ endpoint, bearerToken }) {
        this.endpoint = endpoint;
        this.bearerToken = bearerToken;
        this.sessionId = null;
        this.requestId = 1;
        this.initialized = false;
    }

    nextId() {
        const current = this.requestId;
        this.requestId += 1;
        return current;
    }

    buildHeaders(extraHeaders = {}) {
        return {
            Accept: 'application/json, text/event-stream',
            Authorization: `Bearer ${this.bearerToken}`,
            'Content-Type': 'application/json',
            ...(this.sessionId ? { 'Mcp-Session-Id': this.sessionId } : {}),
            ...extraHeaders
        };
    }

    async postJsonRpc(payload, { expectBody = true } = {}) {
        const response = await fetch(this.endpoint, {
            method: 'POST',
            headers: this.buildHeaders(),
            body: JSON.stringify(payload)
        });

        const bodyText = await response.text();
        const sessionId = response.headers.get('Mcp-Session-Id');
        if (sessionId) {
            this.sessionId = sessionId;
        }

        if (!response.ok) {
            const detail = bodyText || `${response.status} ${response.statusText}`;
            throw new Error(`Rosetta MCP 호출 실패 (${response.status}): ${detail}`);
        }

        if (!expectBody || !bodyText.trim()) {
            return null;
        }

        const parsed = normalizeRpcPayload(bodyText, response.headers.get('Content-Type'));
        if (!parsed) {
            throw new Error('Rosetta MCP 응답 본문이 비어 있거나 해석 불가입니다.');
        }

        if (parsed.error) {
            throw new Error(parsed.error.message ?? 'Rosetta MCP JSON-RPC 오류');
        }

        return parsed;
    }

    async initialize() {
        if (this.initialized) {
            return;
        }

        let initializedResponse = null;
        let lastError = null;

        for (const protocolVersion of DEFAULT_PROTOCOL_VERSIONS) {
            try {
                initializedResponse = await this.postJsonRpc({
                    jsonrpc: '2.0',
                    id: this.nextId(),
                    method: 'initialize',
                    params: {
                        protocolVersion,
                        capabilities: {},
                        clientInfo: {
                            name: 'pgm_ops2',
                            version: '1.0.0'
                        }
                    }
                });
                break;
            } catch (error) {
                lastError = error;
            }
        }

        if (!initializedResponse) {
            throw lastError ?? new Error('Rosetta MCP initialize에 실패했습니다.');
        }

        await this.postJsonRpc({
            jsonrpc: '2.0',
            method: 'notifications/initialized'
        }, { expectBody: false });

        this.initialized = true;
    }

    async callTool(name, args) {
        await this.initialize();
        const payload = await this.postJsonRpc({
            jsonrpc: '2.0',
            id: this.nextId(),
            method: 'tools/call',
            params: {
                name,
                arguments: args
            }
        });

        return extractToolResult(payload);
    }

    async executeQuery({ connectionId, sql }) {
        const result = await this.callTool('execute_query', {
            connection_id: connectionId,
            sql
        });

        return normalizeExecuteQueryResult(result);
    }
}

export function getRosettaErrorCode(error) {
    return typeof error?.code === 'string' ? error.code : ROSETTA_ERROR_CODES.query_failed;
}
