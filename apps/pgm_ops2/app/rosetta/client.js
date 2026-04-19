import { parse as parseYaml } from 'yaml';

const DEFAULT_PROTOCOL_VERSIONS = ['2025-03-26', '2024-11-05'];

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

function extractToolResult(payload) {
    if (!payload) {
        return null;
    }

    const candidateRoots = [payload.result, payload].filter(Boolean);

    for (const root of candidateRoots) {
        if (isPlainObject(root.structuredContent)) {
            return root.structuredContent;
        }

        if (Array.isArray(root.content)) {
            for (const item of root.content) {
                if (isPlainObject(item?.structuredContent)) {
                    return item.structuredContent;
                }
                if (isPlainObject(item?.json)) {
                    return item.json;
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
    if (!isPlainObject(result)) {
        throw new Error('Rosetta execute_query 응답을 해석하지 못했습니다.');
    }

    if (String(result.status ?? '').toLowerCase() !== 'completed') {
        throw new Error(result.reason ?? result.message ?? 'Rosetta execute_query가 실패했습니다.');
    }

    return {
        columns: Array.isArray(result.columns) ? result.columns.map(String) : [],
        rows: Array.isArray(result.rows) ? result.rows.map((row) => {
            if (!isPlainObject(row)) {
                return {};
            }
            return Object.fromEntries(
                Object.entries(row).map(([key, value]) => [key, value == null ? '' : String(value)])
            );
        }) : [],
        rowCount: Number.parseInt(result.row_count, 10) || 0,
        filteredSql: typeof result.filtered_sql === 'string' ? result.filtered_sql : '',
        status: String(result.status ?? '')
    };
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

