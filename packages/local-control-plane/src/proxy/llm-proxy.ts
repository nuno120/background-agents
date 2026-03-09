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
 *
 * Resilience features:
 * - Per-session error tracking (C1) — consecutive 5xx trigger provider_unhealthy
 * - LLM result callbacks (C3) — feed session-level health tracking
 * - Transparent failover (A) — on 5xx or 429, try next provider in model chain
 */

import http from "node:http";
import https from "node:https";
import type { Express, Request, Response } from "express";
import type { CredentialStore, LlmCredentials } from "../credentials/credential-store.js";

const MAX_BODY_SIZE = 10 * 1024 * 1024; // 10 MB
const CONNECT_TIMEOUT_MS = 30_000;
const READ_TIMEOUT_MS = 300_000;

const CONSECUTIVE_ERROR_THRESHOLD = 3;

/** Map of provider names to their base URLs (fallbacks if not stored). */
const PROVIDER_BASE_URLS: Record<string, string> = {
  anthropic: "https://api.anthropic.com",
  openai: "https://api.openai.com",
  zai: "https://open.bigmodel.cn/api/paas",
  deepseek: "https://api.deepseek.com",
  deepinfra: "https://api.deepinfra.com/v1",
};

/**
 * OpenCode/OpenAI SDK always prefixes paths with /v1/ (e.g. /v1/chat/completions).
 * Some providers use a different API version path. This map rewrites the version
 * prefix so the upstream URL is correct.
 *   - null  = strip /v1/ entirely (provider base URL already includes full path)
 *   - "vN"  = replace /v1/ with /vN/
 */
const API_VERSION_REWRITE: Record<string, string | null> = {
  zai: "v4", // zai uses /v4/chat/completions, not /v1/chat/completions
  deepinfra: null, // deepinfra base URL is .../v1, SDK sends openai/chat/completions — no rewrite needed
};

// ── Error tracking (C1) ─────────────────────────────────────────────────

/**
 * Per-session error tracking for provider health detection.
 * Key: proxyKey, Value: Map<provider, consecutiveErrorCount>
 */
const sessionErrors = new Map<string, Map<string, number>>();

/** Clean up error tracking when session is destroyed. */
export function clearSessionErrors(proxyKey: string): void {
  sessionErrors.delete(proxyKey);
}

// ── Callbacks type ──────────────────────────────────────────────────────

export interface LlmProxyCallbacks {
  onProviderUnhealthy?: (sessionId: string, provider: string, errorCount: number) => void;
  onLlmResult?: (sessionId: string, success: boolean) => void;
}

// ── Helpers ─────────────────────────────────────────────────────────────

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

function rewriteApiPath(apiPath: string, provider: string, baseUrl: string): string {
  let finalPath = apiPath;
  const versionRewrite = API_VERSION_REWRITE[provider];
  if (versionRewrite === undefined) return finalPath;

  const baseEndsWithVersion = baseUrl.replace(/\/$/, "").endsWith(`/${versionRewrite}`);

  if (finalPath.startsWith("v1/")) {
    if (versionRewrite === null) {
      // Strip v1/ entirely (e.g. deepinfra base URL already includes /v1)
      finalPath = finalPath.slice(3);
    } else if (baseEndsWithVersion) {
      // Base URL already ends with the version — just strip v1/
      finalPath = finalPath.slice(3);
    } else {
      // Replace v1/ with target version
      finalPath = `${versionRewrite}/${finalPath.slice(3)}`;
    }
  } else if (versionRewrite !== null && !baseEndsWithVersion) {
    // Path has no version prefix (e.g. "chat/completions") and base URL doesn't
    // include the version — prepend target version (e.g. "v4/chat/completions")
    finalPath = `${versionRewrite}/${finalPath}`;
  }

  return finalPath;
}

function extractModelFromBody(body: Buffer): string | null {
  try {
    const parsed = JSON.parse(body.toString());
    return parsed.model || null;
  } catch {
    return null;
  }
}

function rewriteModelInBody(body: Buffer, newModel: string): Buffer {
  try {
    const parsed = JSON.parse(body.toString());
    parsed.model = newModel;
    return Buffer.from(JSON.stringify(parsed));
  } catch {
    return body;
  }
}

function trackLlmResult(
  proxyKey: string,
  provider: string,
  success: boolean,
  credentialStore: CredentialStore,
  callbacks?: LlmProxyCallbacks
): void {
  // C1: Track consecutive errors
  if (!sessionErrors.has(proxyKey)) {
    sessionErrors.set(proxyKey, new Map());
  }
  const counts = sessionErrors.get(proxyKey)!;

  if (success) {
    counts.set(provider, 0);
  } else {
    const errorCount = (counts.get(provider) || 0) + 1;
    counts.set(provider, errorCount);

    if (errorCount >= CONSECUTIVE_ERROR_THRESHOLD) {
      console.warn(
        `[llm-proxy] Provider ${provider} has ${errorCount} consecutive errors for proxy key ${proxyKey.slice(0, 8)}...`
      );
      const sessionId = credentialStore.getSessionIdByLlmProxyKey(proxyKey);
      if (sessionId && callbacks?.onProviderUnhealthy) {
        callbacks.onProviderUnhealthy(sessionId, provider, errorCount);
      }
      counts.set(provider, 0); // Reset to avoid repeated notifications
    }
  }

  // C3: Report every result for session-level health tracking
  if (callbacks?.onLlmResult) {
    const sessionId = credentialStore.getSessionIdByLlmProxyKey(proxyKey);
    if (sessionId) {
      callbacks.onLlmResult(sessionId, success);
    }
  }
}

// ── Streaming with failover support ─────────────────────────────────────

type StreamResult = "success" | "headers_sent" | "failed_before_headers";

async function attemptStreamingRequest(
  url: string,
  method: string,
  headers: Record<string, string>,
  body: Buffer,
  res: Response,
  provider: string
): Promise<StreamResult> {
  const startTime = Date.now();
  return new Promise<StreamResult>((resolve) => {
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
      console.log(
        `[llm-proxy] ${provider} upstream responded status=${upstreamRes.statusCode} ttfb=${ttfb}ms`
      );

      if (
        upstreamRes.statusCode &&
        (upstreamRes.statusCode < 200 || upstreamRes.statusCode >= 300)
      ) {
        // Any non-2xx — retry with next provider in chain
        upstreamRes.resume();
        upstreamRes.on("end", () => resolve("failed_before_headers"));
        return;
      }

      // Success — forward response (can't retry after this)
      res.writeHead(upstreamRes.statusCode || 200, {
        "Content-Type": upstreamRes.headers["content-type"] || "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
        ...(upstreamRes.headers["x-request-id"]
          ? { "x-request-id": upstreamRes.headers["x-request-id"] }
          : {}),
      });

      upstreamRes.on("data", (chunk: Buffer) => {
        chunkCount++;
        totalBytes += chunk.length;
        res.write(chunk);
      });

      upstreamRes.on("end", () => {
        const elapsed = Date.now() - startTime;
        console.log(
          `[llm-proxy] ${provider} stream done chunks=${chunkCount} bytes=${totalBytes} elapsed=${elapsed}ms`
        );
        res.end();
        resolve("success");
      });

      upstreamRes.on("error", (err) => {
        const elapsed = Date.now() - startTime;
        console.error(
          `[llm-proxy] ${provider} upstream stream error after ${elapsed}ms: ${err.message}`
        );
        res.end();
        resolve("headers_sent");
      });

      // Read timeout
      upstreamRes.setTimeout(READ_TIMEOUT_MS, () => {
        const elapsed = Date.now() - startTime;
        console.warn(
          `[llm-proxy] ${provider} read timeout after ${elapsed}ms chunks=${chunkCount}`
        );
        upstreamRes.destroy();
        res.end();
        resolve("headers_sent");
      });
    });

    upstream.on("timeout", () => {
      const elapsed = Date.now() - startTime;
      console.warn(`[llm-proxy] ${provider} connect timeout after ${elapsed}ms`);
      upstream.destroy();
      resolve("failed_before_headers");
    });

    upstream.on("error", (err) => {
      const elapsed = Date.now() - startTime;
      console.error(`[llm-proxy] ${provider} request error after ${elapsed}ms: ${err.message}`);
      resolve("failed_before_headers");
    });

    upstream.write(body);
    upstream.end();
  });
}

// ── Buffered request with failover support ──────────────────────────────

interface BufferedResult {
  status: number;
  contentType: string | null;
  body: Buffer;
}

async function attemptBufferedRequest(
  url: string,
  method: string,
  headers: Record<string, string>,
  body: Buffer,
  provider: string
): Promise<BufferedResult> {
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
    console.log(
      `[llm-proxy] ${provider} buffered response status=${upstream.status} elapsed=${elapsed}ms`
    );

    if (upstream.status >= 400) {
      const responseBody = await upstream.arrayBuffer();
      const errorPreview = Buffer.from(responseBody).toString().slice(0, 500);
      console.error(`[llm-proxy] ${provider} upstream error (${upstream.status}): ${errorPreview}`);
      return {
        status: upstream.status,
        contentType: upstream.headers.get("content-type"),
        body: Buffer.from(responseBody),
      };
    }

    const responseBody = await upstream.arrayBuffer();
    return {
      status: upstream.status,
      contentType: upstream.headers.get("content-type"),
      body: Buffer.from(responseBody),
    };
  } catch (error) {
    const elapsed = Date.now() - startTime;
    const msg = error instanceof Error ? error.message : String(error);
    console.error(`[llm-proxy] ${provider} fetch error after ${elapsed}ms: ${msg}`);
    return {
      status: msg.includes("abort") ? 504 : 502,
      contentType: "application/json",
      body: Buffer.from(
        JSON.stringify({ error: msg.includes("abort") ? "Upstream timeout" : "Upstream error" })
      ),
    };
  }
}

// ── Main request handler with failover ──────────────────────────────────

async function handleRequestWithFailover(
  req: Request,
  res: Response,
  primaryProvider: string,
  primaryCreds: (LlmCredentials & { sessionId: string }) | null,
  proxyKey: string,
  apiPath: string,
  body: Buffer,
  credentialStore: CredentialStore,
  callbacks?: LlmProxyCallbacks
): Promise<void> {
  const modelChains = credentialStore.getModelChains(proxyKey);
  const streaming = req.method === "POST" && body.length > 0 && isStreamingRequest(body);

  // Build list of providers to try
  const providersToTry: Array<{ provider: string; creds: LlmCredentials; model: string | null }> =
    [];

  if (primaryProvider === "sandbox") {
    // Profile-based routing: model name in body IS the profile name.
    // Look up the chain for this profile and try each real provider in order.
    const profileName = extractModelFromBody(body);
    if (!profileName || !modelChains) {
      res.status(400).json({ error: "Missing profile name or model chains for sandbox routing" });
      return;
    }

    const chain = modelChains[profileName];
    if (!chain || chain.length === 0) {
      res.status(400).json({ error: `Unknown sandbox profile: ${profileName}` });
      return;
    }

    for (const entry of chain) {
      const creds = credentialStore.getByLlmProxyKey(proxyKey, entry.provider);
      if (creds) {
        providersToTry.push({
          provider: entry.provider,
          creds,
          model: entry.model,
        });
      }
    }

    if (providersToTry.length === 0) {
      res.status(500).json({ error: "No credentials available for any provider in chain" });
      return;
    }

    console.log(
      `[llm-proxy] sandbox profile=${profileName} chain=${providersToTry.map((p) => p.provider).join(",")}`
    );
  } else {
    // Direct provider routing (legacy path): provider in URL is primary
    if (!primaryCreds) {
      res.status(403).json({ error: "Invalid proxy key or provider" });
      return;
    }
    providersToTry.push({ provider: primaryProvider, creds: primaryCreds, model: null });

    // Find alternative providers from model chains for failover
    if (modelChains) {
      const requestModel = extractModelFromBody(body);
      if (requestModel) {
        const chain =
          modelChains[requestModel] || modelChains[`${primaryProvider}/${requestModel}`];
        if (chain) {
          for (const entry of chain) {
            const altBaseProvider = entry.provider.split("-")[0];
            if (altBaseProvider === primaryProvider.split("-")[0]) continue;
            const altCreds = credentialStore.getByLlmProxyKey(proxyKey, altBaseProvider);
            if (altCreds) {
              providersToTry.push({
                provider: altBaseProvider,
                creds: altCreds,
                model: entry.model,
              });
            }
          }
        }
      }
    }
  }

  console.log(
    `[llm-proxy] mode=${streaming ? "streaming" : "buffered"} bodySize=${body.length} providers=${providersToTry.map((p) => p.provider).join(",")}`
  );

  // Try each provider
  for (let i = 0; i < providersToTry.length; i++) {
    const attempt = providersToTry[i];
    const isLastAttempt = i === providersToTry.length - 1;

    // Strip provider prefix from model name in request body.
    // Providers expect just the model name, not the routing prefix
    // (e.g. "deepinfra/Qwen/Qwen3-32B" → "Qwen/Qwen3-32B").
    // This applies to BOTH primary and failover attempts because OpenCode
    // may or may not strip the prefix depending on model name format.
    let attemptBody = body;
    const baseProvider = attempt.provider.split("-")[0];
    if (attempt.model) {
      let apiModel = attempt.model;
      if (apiModel.startsWith(`${baseProvider}/`)) {
        apiModel = apiModel.slice(baseProvider.length + 1);
      }
      attemptBody = rewriteModelInBody(body, apiModel);
    } else {
      // Primary attempt: check if model in body has the provider prefix
      const bodyModel = extractModelFromBody(body);
      if (bodyModel && bodyModel.startsWith(`${baseProvider}/`)) {
        const stripped = bodyModel.slice(baseProvider.length + 1);
        attemptBody = rewriteModelInBody(body, stripped);
      }
    }

    // Build upstream URL for this provider
    const baseUrl = attempt.creds.baseUrl || PROVIDER_BASE_URLS[attempt.provider] || "";
    if (!baseUrl) {
      if (isLastAttempt) {
        res.status(400).json({ error: `Unknown provider: ${attempt.provider}` });
        return;
      }
      continue;
    }

    const finalApiPath = rewriteApiPath(apiPath, attempt.provider, baseUrl);
    const upstreamUrl = `${baseUrl.replace(/\/$/, "")}/${finalApiPath}`;

    // Build headers with injected auth
    const headers: Record<string, string> = {};
    if (req.headers["content-type"])
      headers["Content-Type"] = req.headers["content-type"] as string;
    if (req.headers["accept"]) headers["Accept"] = req.headers["accept"] as string;
    if (req.headers["anthropic-version"])
      headers["anthropic-version"] = req.headers["anthropic-version"] as string;
    if (req.headers["anthropic-beta"])
      headers["anthropic-beta"] = req.headers["anthropic-beta"] as string;
    injectAuthHeaders(headers, attempt.provider, attempt.creds.apiKey);

    if (i > 0) {
      const rewriteInfo = finalApiPath !== apiPath ? ` (rewritten from ${apiPath})` : "";
      console.log(
        `[llm-proxy] FAILOVER: trying ${attempt.provider} (attempt ${i + 1}/${providersToTry.length}) model=${attempt.model}${rewriteInfo} → ${upstreamUrl}`
      );
    } else {
      const rewriteInfo = finalApiPath !== apiPath ? ` (rewritten from ${apiPath})` : "";
      console.log(
        `[llm-proxy] ${req.method} ${attempt.provider}/${finalApiPath}${rewriteInfo} → ${upstreamUrl}`
      );
    }

    if (streaming) {
      const result = await attemptStreamingRequest(
        upstreamUrl,
        req.method,
        headers,
        attemptBody,
        res,
        attempt.provider
      );

      if (result === "success") {
        trackLlmResult(proxyKey, attempt.provider, true, credentialStore, callbacks);
        return;
      }
      if (result === "headers_sent") {
        // Partially sent — can't retry, but it's a failure
        trackLlmResult(proxyKey, attempt.provider, false, credentialStore, callbacks);
        return;
      }
      // failed_before_headers — try next provider
      trackLlmResult(proxyKey, attempt.provider, false, credentialStore, callbacks);

      if (isLastAttempt) {
        if (!res.headersSent) {
          res.status(502).json({ error: "All providers failed" });
        }
        return;
      }
      continue;
    } else {
      const result = await attemptBufferedRequest(
        upstreamUrl,
        req.method,
        headers,
        attemptBody,
        attempt.provider
      );
      const isRetryable = result.status < 200 || result.status >= 300;

      trackLlmResult(proxyKey, attempt.provider, !isRetryable, credentialStore, callbacks);

      if (!isRetryable || isLastAttempt) {
        // Success, non-retriable 4xx, or last attempt — send response
        res.status(result.status);
        if (result.contentType) res.setHeader("Content-Type", result.contentType);
        res.send(result.body);
        return;
      }

      // 5xx/429 and not last attempt — try next provider
      console.log(
        `[llm-proxy] ${attempt.provider} returned ${result.status}, trying next provider`
      );
      continue;
    }
  }
}

// ── Setup ───────────────────────────────────────────────────────────────

export function setupLlmProxy(
  app: Express,
  credentialStore: CredentialStore,
  callbacks?: LlmProxyCallbacks
): void {
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

    const body: Buffer = (req as any).rawBody || Buffer.alloc(0);

    if (provider === "sandbox") {
      // Virtual "sandbox" provider — just verify the proxy key is valid.
      // Real provider credentials are resolved per-attempt from the chain.
      const sessionId = credentialStore.getSessionIdByLlmProxyKey(proxyKey);
      if (!sessionId) {
        console.warn(`[llm-proxy] Invalid proxy key for sandbox provider`);
        res.status(403).json({ error: "Invalid proxy key" });
        return;
      }

      await handleRequestWithFailover(
        req,
        res,
        provider,
        null,
        proxyKey,
        apiPath,
        body,
        credentialStore,
        callbacks
      );
      return;
    }

    // Direct provider routing — look up credentials for the specific provider
    const creds = credentialStore.getByLlmProxyKey(proxyKey, provider);
    if (!creds) {
      console.warn(`[llm-proxy] Invalid proxy key or unknown provider: ${provider}`);
      res.status(403).json({ error: "Invalid proxy key or provider" });
      return;
    }

    await handleRequestWithFailover(
      req,
      res,
      provider,
      creds,
      proxyKey,
      apiPath,
      body,
      credentialStore,
      callbacks
    );
  });
}
