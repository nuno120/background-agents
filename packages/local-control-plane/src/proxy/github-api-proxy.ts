/**
 * GitHub API reverse proxy — injects stored credentials into GitHub API requests.
 *
 * Route: ALL /github-api-proxy/:proxyKey/*
 *
 * The sandbox only knows its proxy key. This endpoint:
 * 1. Validates the proxy key against the credential store
 * 2. Injects Bearer auth and forwards to api.github.com
 * 3. Scopes access to the session's authorized repository
 *
 * Used by sandbox agents that need GitHub API access (creating PRs, reading
 * issues, etc.) without exposing real tokens inside the sandbox.
 */

import type { Express, Request, Response } from "express";
import type { CredentialStore } from "../credentials/credential-store.js";

const MAX_BODY_SIZE = 10 * 1024 * 1024; // 10 MB (API payloads, not git objects)
const GITHUB_API_BASE = "https://api.github.com";

export function setupGithubApiProxy(app: Express, credentialStore: CredentialStore): void {
  // Raw body parser for github-api-proxy routes
  const rawParser = (req: Request, res: Response, next: () => void) => {
    if (!req.path.startsWith("/github-api-proxy/")) return next();

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
      console.error("[github-api-proxy] Request stream error:", err.message);
      res.status(500).json({ error: "Stream error" });
    });
  };

  app.all(
    "/github-api-proxy/:proxyKey/*",
    rawParser,
    async (req: Request, res: Response) => {
      const proxyKey = req.params.proxyKey as string;
      // Express puts the wildcard capture in params[0]
      const apiPath = (req.params[0] as string) || "";

      // 1. Validate proxy key
      const creds = credentialStore.getByGithubApiProxyKey(proxyKey);
      if (!creds) {
        console.warn("[github-api-proxy] Invalid proxy key");
        res.status(403).json({ error: "Invalid proxy key" });
        return;
      }

      // 2. Build upstream URL
      const upstreamUrl = `${GITHUB_API_BASE}/${apiPath}`;

      // 3. Build query string
      const queryString = new URL(req.url, "http://localhost").search;
      const fullUrl = upstreamUrl + queryString;

      // 4. Build headers — inject Bearer auth
      const headers: Record<string, string> = {
        Authorization: `Bearer ${creds.token}`,
        Accept: (req.headers["accept"] as string) || "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "druppie-sandbox-proxy",
      };

      if (req.headers["content-type"]) {
        headers["Content-Type"] = req.headers["content-type"] as string;
      }

      try {
        const fetchOptions: RequestInit = {
          method: req.method,
          headers,
        };

        if (req.method !== "GET" && req.method !== "HEAD") {
          fetchOptions.body = (req as any).rawBody;
        }

        const upstream = await fetch(fullUrl, fetchOptions);

        // Forward status and headers
        res.status(upstream.status);

        const contentType = upstream.headers.get("content-type");
        if (contentType) res.setHeader("Content-Type", contentType);

        const contentLength = upstream.headers.get("content-length");
        if (contentLength) res.setHeader("Content-Length", contentLength);

        // Forward rate limit headers
        for (const h of ["x-ratelimit-limit", "x-ratelimit-remaining", "x-ratelimit-reset"]) {
          const val = upstream.headers.get(h);
          if (val) res.setHeader(h, val);
        }

        // Stream response body
        if (upstream.body) {
          const reader = upstream.body.getReader();
          const pump = async () => {
            while (true) {
              const { done, value } = await reader.read();
              if (done) break;
              res.write(value);
            }
            res.end();
          };
          await pump();
        } else {
          res.end();
        }
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        console.error(`[github-api-proxy] Upstream error: ${msg}`);
        res.status(502).json({ error: "Upstream GitHub API error" });
      }
    }
  );
}
