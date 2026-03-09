/**
 * Git HTTP proxy — injects stored credentials into git clone/fetch/push requests.
 *
 * Route: ALL /git-proxy/:proxyKey/:owner/:repoName.git/*
 *
 * The sandbox only knows its proxy key + repo path. This endpoint:
 * 1. Validates the proxy key against the credential store
 * 2. Validates the owner/repo matches the session's authorized scope
 * 3. Validates the git path against a whitelist
 * 4. Injects Basic auth and forwards to the real git server
 *
 * Body is handled as raw buffers (git smart HTTP protocol).
 */

import type { Express, Request, Response } from "express";
import type { CredentialStore } from "../credentials/credential-store.js";

const MAX_BODY_SIZE = 100 * 1024 * 1024; // 100 MB

/** Whitelist of allowed git HTTP paths. */
const ALLOWED_PATHS = [
  /^info\/refs$/,
  /^git-upload-pack$/,
  /^git-receive-pack$/,
  /^objects\/.+$/,
  /^shallow$/,
  /^HEAD$/,
];

function isAllowedGitPath(gitPath: string): boolean {
  return ALLOWED_PATHS.some((pattern) => pattern.test(gitPath));
}

export function setupGitProxy(app: Express, credentialStore: CredentialStore): void {
  // Parse raw body for git-proxy routes
  const rawParser = (req: Request, res: Response, next: () => void) => {
    if (!req.path.startsWith("/git-proxy/")) return next();

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
      console.error("[git-proxy] Request stream error:", err.message);
      res.status(500).json({ error: "Stream error" });
    });
  };

  app.all(
    "/git-proxy/:proxyKey/:owner/:repoName.git/*",
    rawParser,
    async (req: Request, res: Response) => {
      const proxyKey = req.params.proxyKey as string;
      const owner = req.params.owner as string;
      const repoName = req.params.repoName as string;
      // Express puts the wildcard capture in params[0]
      const gitPath = (req.params[0] as string) || "";

      // 1. Validate proxy key
      const creds = credentialStore.getByGitProxyKey(proxyKey);
      if (!creds) {
        console.warn("[git-proxy] Invalid proxy key");
        res.status(403).json({ error: "Invalid proxy key" });
        return;
      }

      // 2. Validate repo scope
      if (creds.authorizedRepo) {
        const requestedRepo = `${owner}/${repoName}`;
        if (requestedRepo !== creds.authorizedRepo) {
          console.warn(
            `[git-proxy] Repo mismatch: requested=${requestedRepo} authorized=${creds.authorizedRepo} session=${creds.sessionId}`
          );
          res.status(403).json({ error: "Repository not authorized for this session" });
          return;
        }
      }

      // 3. Validate git path whitelist
      if (!isAllowedGitPath(gitPath)) {
        console.warn(`[git-proxy] Blocked path: ${gitPath}`);
        res.status(403).json({ error: "Path not allowed" });
        return;
      }

      // 4. Build upstream URL
      const baseUrl = creds.url.replace(/\/$/, "");
      const upstreamUrl = `${baseUrl}/${owner}/${repoName}.git/${gitPath}`;

      // 5. Build auth header
      const basicAuth = Buffer.from(`${creds.username}:${creds.password}`).toString("base64");

      // 6. Forward request
      const headers: Record<string, string> = {
        Authorization: `Basic ${basicAuth}`,
      };

      // Forward relevant headers
      if (req.headers["content-type"]) {
        headers["Content-Type"] = req.headers["content-type"] as string;
      }
      if (req.headers["accept"]) {
        headers["Accept"] = req.headers["accept"] as string;
      }

      // Build query string
      const queryString = new URL(req.url, "http://localhost").search;
      const fullUrl = upstreamUrl + queryString;

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
        console.error(`[git-proxy] Upstream error: ${msg}`);
        res.status(502).json({ error: "Upstream git server error" });
      }
    }
  );
}
