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
  zai: "https://open.bigmodel.cn/api/paas",
  deepseek: "https://api.deepseek.com",
  deepinfra: "https://api.deepinfra.com/v1/openai",
};

/**
 * OpenCode/OpenAI SDK always prefixes paths with /v1/ (e.g. /v1/chat/completions).
 * Some providers use a different API version path. This map rewrites the version
 * prefix so the upstream URL is correct.
 *   - null  = strip /v1/ entirely (provider base URL already includes full path)
 *   - "vN"  = replace /v1/ with /vN/
 */
const API_VERSION_REWRITE: Record<string, string | null> = {
  zai: "v4",       // zai uses /v4/chat/completions, not /v1/chat/completions
  deepinfra: null,  // deepinfra base URL is .../v1/openai, paths are /chat/completions directly
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

    // 1. Validate proxy key and look up credentials for the requested provider
    const creds = credentialStore.getByLlmProxyKey(proxyKey, provider);
    if (!creds) {
      console.warn(`[llm-proxy] Invalid proxy key or unknown provider: ${provider}`);
      res.status(403).json({ error: "Invalid proxy key or provider" });
      return;
    }

    // 2. Build upstream URL
    const baseUrl = creds.baseUrl || PROVIDER_BASE_URLS[provider] || "";
    if (!baseUrl) {
      res.status(400).json({ error: `Unknown provider: ${provider}` });
      return;
    }

    // Rewrite /v1/ prefix for providers that use a different API version path.
    // OpenCode/OpenAI SDK always uses /v1/ but some providers need different paths.
    let finalApiPath = apiPath;
    const versionRewrite = API_VERSION_REWRITE[provider];
    if (versionRewrite !== undefined && finalApiPath.startsWith("v1/")) {
      if (versionRewrite === null) {
        // Strip /v1/ entirely (base URL already has full path)
        finalApiPath = finalApiPath.slice(3);
      } else {
        // Replace /v1/ with /vN/, but only if base URL doesn't already end with /vN
        const baseEndsWithVersion = baseUrl.replace(/\/$/, "").endsWith(`/${versionRewrite}`);
        if (baseEndsWithVersion) {
          // Base already has version (e.g. .../v4), just strip /v1/
          finalApiPath = finalApiPath.slice(3);
        } else {
          // Base doesn't have version, replace /v1/ with /vN/
          finalApiPath = `${versionRewrite}/${finalApiPath.slice(3)}`;
        }
      }
    }

    const upstreamUrl = `${baseUrl.replace(/\/$/, "")}/${finalApiPath}`;

    // Log the request with path rewrite details for debugging
    const rewriteInfo = finalApiPath !== apiPath ? ` (rewritten from ${apiPath})` : "";
    console.log(`[llm-proxy] ${req.method} ${provider}/${finalApiPath}${rewriteInfo} → ${upstreamUrl}`);

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
    console.log(`[llm-proxy] mode=${streaming ? "streaming" : "buffered"} bodySize=${body.length}`);

    if (streaming) {
      // SSE streaming: use raw http/https to pipe response without buffering
      await handleStreamingRequest(upstreamUrl, req.method, headers, body, res, provider);
    } else {
      // Non-streaming: use fetch, buffer and return
      await handleBufferedRequest(upstreamUrl, req.method, headers, body, res, provider);
    }
  });
}

async function handleStreamingRequest(
  url: string,
  method: string,
  headers: Record<string, string>,
  body: Buffer,
  res: Response,
  provider: string
): Promise<void> {
  const startTime = Date.now();
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

    let chunkCount = 0;
    let totalBytes = 0;

    const upstream = transport.request(options, (upstreamRes) => {
      const ttfb = Date.now() - startTime;
      console.log(`[llm-proxy] ${provider} upstream responded status=${upstreamRes.statusCode} ttfb=${ttfb}ms`);

      if (upstreamRes.statusCode && upstreamRes.statusCode >= 400) {
        // Log error response body for debugging
        const errorChunks: Buffer[] = [];
        upstreamRes.on("data", (chunk: Buffer) => errorChunks.push(chunk));
        upstreamRes.on("end", () => {
          const errorBody = Buffer.concat(errorChunks).toString().slice(0, 500);
          console.error(`[llm-proxy] ${provider} upstream error body: ${errorBody}`);
          res.writeHead(upstreamRes.statusCode || 502, {
            "Content-Type": upstreamRes.headers["content-type"] || "application/json",
          });
          res.end(Buffer.concat(errorChunks));
          resolve();
        });
        return;
      }

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
        chunkCount++;
        totalBytes += chunk.length;
        res.write(chunk);
      });

      upstreamRes.on("end", () => {
        const elapsed = Date.now() - startTime;
        console.log(`[llm-proxy] ${provider} stream done chunks=${chunkCount} bytes=${totalBytes} elapsed=${elapsed}ms`);
        res.end();
        resolve();
      });

      upstreamRes.on("error", (err) => {
        const elapsed = Date.now() - startTime;
        console.error(`[llm-proxy] ${provider} upstream stream error after ${elapsed}ms: ${err.message}`);
        res.end();
        resolve();
      });

      // Read timeout — if no data for READ_TIMEOUT_MS, close
      upstreamRes.setTimeout(READ_TIMEOUT_MS, () => {
        const elapsed = Date.now() - startTime;
        console.warn(`[llm-proxy] ${provider} read timeout after ${elapsed}ms chunks=${chunkCount}`);
        upstreamRes.destroy();
        res.end();
        resolve();
      });
    });

    upstream.on("timeout", () => {
      const elapsed = Date.now() - startTime;
      console.warn(`[llm-proxy] ${provider} connect timeout after ${elapsed}ms`);
      upstream.destroy();
      if (!res.headersSent) {
        res.status(504).json({ error: "Upstream connect timeout" });
      }
      resolve();
    });

    upstream.on("error", (err) => {
      const elapsed = Date.now() - startTime;
      console.error(`[llm-proxy] ${provider} request error after ${elapsed}ms: ${err.message}`);
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
  res: Response,
  provider: string
): Promise<void> {
  const startTime = Date.now();
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

    const elapsed = Date.now() - startTime;
    console.log(`[llm-proxy] ${provider} buffered response status=${upstream.status} elapsed=${elapsed}ms`);

    // Forward status
    res.status(upstream.status);

    // Forward relevant headers
    const contentType = upstream.headers.get("content-type");
    if (contentType) res.setHeader("Content-Type", contentType);

    const requestId = upstream.headers.get("x-request-id");
    if (requestId) res.setHeader("x-request-id", requestId);

    // Send body
    const responseBody = await upstream.arrayBuffer();
    if (upstream.status >= 400) {
      const errorPreview = Buffer.from(responseBody).toString().slice(0, 500);
      console.error(`[llm-proxy] ${provider} upstream error (${upstream.status}): ${errorPreview}`);
    }
    res.send(Buffer.from(responseBody));
  } catch (error) {
    const elapsed = Date.now() - startTime;
    const msg = error instanceof Error ? error.message : String(error);
    console.error(`[llm-proxy] ${provider} fetch error after ${elapsed}ms: ${msg}`);

    if (!res.headersSent) {
      if (msg.includes("abort")) {
        res.status(504).json({ error: "Upstream timeout" });
      } else {
        res.status(502).json({ error: "Upstream error" });
      }
    }
  }
}
