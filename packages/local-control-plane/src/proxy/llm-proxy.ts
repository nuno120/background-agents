/**
 * LLM API proxy — injects stored API keys into LLM provider requests.
 *
 * Route: ALL /llm-proxy/:proxyKey/:provider/*
 *
 * Supports SSE streaming passthrough for streaming LLM responses.
 * Uses node:http/node:https for streaming to avoid Express buffering.
 *
 * Provider auth injection:
 * - Anthropic: x-api-key header + anthropic-version
 * - OpenAI-compatible (zai, openai, deepseek, deepinfra): Authorization: Bearer
 */

import http from "node:http";
import https from "node:https";
import type { Express, Request, Response } from "express";
import type { CredentialStore, LlmCredentials } from "../credentials/credential-store.js";

const MAX_BODY_SIZE = 10 * 1024 * 1024; // 10 MB
const CONNECT_TIMEOUT_MS = 30_000;
const READ_TIMEOUT_MS = 300_000;

/** Map of provider names to their base URLs (fallbacks if not stored). */
const PROVIDER_BASE_URLS: Record<string, string> = {
  anthropic: "https://api.anthropic.com",
  openai: "https://api.openai.com",
  zai: "https://open.bigmodel.cn/api/paas/v4",
  deepseek: "https://api.deepseek.com",
  deepinfra: "https://api.deepinfra.com/v1/openai",
};

function injectAuthHeaders(
  headers: Record<string, string>,
  provider: string,
  apiKey: string
): void {
  if (provider === "anthropic") {
    headers["x-api-key"] = apiKey;
    if (!headers["anthropic-version"]) {
      headers["anthropic-version"] = "2023-06-01";
    }
  } else {
    // OpenAI-compatible providers (zai, openai, deepseek, deepinfra)
    headers["Authorization"] = `Bearer ${apiKey}`;
  }
}

function isStreamingRequest(body: Buffer): boolean {
  try {
    const parsed = JSON.parse(body.toString());
    return parsed.stream === true;
  } catch {
    return false;
  }
}

export function setupLlmProxy(app: Express, credentialStore: CredentialStore): void {
  // Raw body parser for LLM proxy routes
  const rawParser = (req: Request, res: Response, next: () => void) => {
    if (!req.path.startsWith("/llm-proxy/")) return next();

    const contentLength = parseInt(req.headers["content-length"] || "0", 10);
    if (contentLength > MAX_BODY_SIZE) {
      res.status(413).json({ error: "Request body too large" });
      return;
    }

    const chunks: Buffer[] = [];
    let size = 0;

    req.on("data", (chunk: Buffer) => {
      size += chunk.length;
      if (size > MAX_BODY_SIZE) {
        res.status(413).json({ error: "Request body too large" });
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });

    req.on("end", () => {
      (req as any).rawBody = Buffer.concat(chunks);
      next();
    });

    req.on("error", (err) => {
      console.error("[llm-proxy] Request stream error:", err.message);
      res.status(500).json({ error: "Stream error" });
    });
  };

  app.all("/llm-proxy/:proxyKey/:provider/*", rawParser, async (req: Request, res: Response) => {
    const proxyKey = req.params.proxyKey as string;
    const provider = req.params.provider as string;
    const apiPath = (req.params[0] as string) || "";

    // 1. Validate proxy key
    const creds = credentialStore.getByLlmProxyKey(proxyKey);
    if (!creds) {
      console.warn("[llm-proxy] Invalid proxy key");
      res.status(403).json({ error: "Invalid proxy key" });
      return;
    }

    // 2. Validate provider matches stored credentials
    if (provider !== creds.provider) {
      console.warn(`[llm-proxy] Provider mismatch: requested=${provider} stored=${creds.provider}`);
      res.status(403).json({ error: "Provider mismatch" });
      return;
    }

    // 3. Build upstream URL
    const baseUrl = creds.baseUrl || PROVIDER_BASE_URLS[provider] || "";
    if (!baseUrl) {
      res.status(400).json({ error: `Unknown provider: ${provider}` });
      return;
    }
    const upstreamUrl = `${baseUrl.replace(/\/$/, "")}/${apiPath}`;

    // 4. Build headers with injected auth
    const headers: Record<string, string> = {};
    if (req.headers["content-type"]) {
      headers["Content-Type"] = req.headers["content-type"] as string;
    }
    if (req.headers["accept"]) {
      headers["Accept"] = req.headers["accept"] as string;
    }
    // Forward anthropic-specific headers
    if (req.headers["anthropic-version"]) {
      headers["anthropic-version"] = req.headers["anthropic-version"] as string;
    }
    if (req.headers["anthropic-beta"]) {
      headers["anthropic-beta"] = req.headers["anthropic-beta"] as string;
    }

    injectAuthHeaders(headers, provider, creds.apiKey);

    const body: Buffer = (req as any).rawBody || Buffer.alloc(0);
    const streaming = req.method === "POST" && body.length > 0 && isStreamingRequest(body);

    if (streaming) {
      // SSE streaming: use raw http/https to pipe response without buffering
      await handleStreamingRequest(upstreamUrl, req.method, headers, body, res);
    } else {
      // Non-streaming: use fetch, buffer and return
      await handleBufferedRequest(upstreamUrl, req.method, headers, body, res);
    }
  });
}

async function handleStreamingRequest(
  url: string,
  method: string,
  headers: Record<string, string>,
  body: Buffer,
  res: Response
): Promise<void> {
  return new Promise<void>((resolve) => {
    const parsed = new URL(url);
    const transport = parsed.protocol === "https:" ? https : http;

    const options: http.RequestOptions = {
      hostname: parsed.hostname,
      port: parsed.port || (parsed.protocol === "https:" ? 443 : 80),
      path: parsed.pathname + parsed.search,
      method,
      headers: {
        ...headers,
        "Content-Length": Buffer.byteLength(body).toString(),
      },
      timeout: CONNECT_TIMEOUT_MS,
    };

    const upstream = transport.request(options, (upstreamRes) => {
      // Forward status and headers
      res.writeHead(upstreamRes.statusCode || 502, {
        "Content-Type": upstreamRes.headers["content-type"] || "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
        ...(upstreamRes.headers["x-request-id"]
          ? { "x-request-id": upstreamRes.headers["x-request-id"] }
          : {}),
      });

      // Pipe upstream SSE chunks directly to client
      upstreamRes.on("data", (chunk: Buffer) => {
        res.write(chunk);
      });

      upstreamRes.on("end", () => {
        res.end();
        resolve();
      });

      upstreamRes.on("error", (err) => {
        console.error("[llm-proxy] Upstream stream error:", err.message);
        res.end();
        resolve();
      });

      // Read timeout — if no data for READ_TIMEOUT_MS, close
      upstreamRes.setTimeout(READ_TIMEOUT_MS, () => {
        console.warn("[llm-proxy] Read timeout on streaming response");
        upstreamRes.destroy();
        res.end();
        resolve();
      });
    });

    upstream.on("timeout", () => {
      console.warn("[llm-proxy] Connect timeout");
      upstream.destroy();
      if (!res.headersSent) {
        res.status(504).json({ error: "Upstream connect timeout" });
      }
      resolve();
    });

    upstream.on("error", (err) => {
      console.error("[llm-proxy] Upstream request error:", err.message);
      if (!res.headersSent) {
        res.status(502).json({ error: "Upstream error" });
      }
      resolve();
    });

    upstream.write(body);
    upstream.end();
  });
}

async function handleBufferedRequest(
  url: string,
  method: string,
  headers: Record<string, string>,
  body: Buffer,
  res: Response
): Promise<void> {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), READ_TIMEOUT_MS);

    const fetchOptions: RequestInit = {
      method,
      headers,
      signal: controller.signal,
    };

    if (method !== "GET" && method !== "HEAD" && body.length > 0) {
      fetchOptions.body = body;
    }

    const upstream = await fetch(url, fetchOptions);
    clearTimeout(timeout);

    // Forward status
    res.status(upstream.status);

    // Forward relevant headers
    const contentType = upstream.headers.get("content-type");
    if (contentType) res.setHeader("Content-Type", contentType);

    const requestId = upstream.headers.get("x-request-id");
    if (requestId) res.setHeader("x-request-id", requestId);

    // Send body
    const responseBody = await upstream.arrayBuffer();
    res.send(Buffer.from(responseBody));
  } catch (error) {
    const msg = error instanceof Error ? error.message : String(error);
    console.error(`[llm-proxy] Fetch error: ${msg}`);

    if (!res.headersSent) {
      if (msg.includes("abort")) {
        res.status(504).json({ error: "Upstream timeout" });
      } else {
        res.status(502).json({ error: "Upstream error" });
      }
    }
  }
}
