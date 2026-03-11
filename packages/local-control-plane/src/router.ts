/**
 * API router for local control plane.
 * Adapted from packages/control-plane/src/router.ts — replaces DO stubs
 * with direct SessionManager calls and D1 with LocalSessionIndexStore.
 */

import crypto from "crypto";
import { verifyInternalToken, VALID_MODELS, isValidModel } from "@open-inspect/shared";
import type { SessionManager } from "./session/session-manager.js";
import type { LocalSessionIndexStore } from "./db/session-index-local.js";
import type { CredentialStore } from "./credentials/credential-store.js";
import type { Express, Request, Response } from "express";
import { config } from "./config.js";

// ── Helpers ──────────────────────────────────────────────────────────────

function generateId(bytes = 16): string {
  return crypto.randomBytes(bytes).toString("hex");
}

function jsonResponse(res: Response, data: unknown, status = 200): void {
  res.status(status).json(data);
}

function errorResponse(res: Response, message: string, status = 400): void {
  res.status(status).json({ error: message });
}

// ── Route types ──────────────────────────────────────────────────────────

interface RouteContext {
  sessionManager: SessionManager;
  sessionIndex: LocalSessionIndexStore;
  internalSecret: string;
  traceId: string;
  requestId: string;
}

// ── Auth ─────────────────────────────────────────────────────────────────

const PUBLIC_ROUTES: RegExp[] = [/^\/health$/, /^\/git-proxy\//, /^\/github-api-proxy\//, /^\/llm-proxy\//];

const SANDBOX_AUTH_ROUTES: RegExp[] = [
  /^\/sessions\/[^/]+\/pr$/,
  /^\/sessions\/[^/]+\/openai-token-refresh$/,
];

function isPublicRoute(path: string): boolean {
  return PUBLIC_ROUTES.some((p) => p.test(path));
}

function isSandboxAuthRoute(path: string): boolean {
  return SANDBOX_AUTH_ROUTES.some((p) => p.test(path));
}

async function verifyAuth(req: Request, ctx: RouteContext, path: string): Promise<string | null> {
  if (isPublicRoute(path)) return null;

  // Try HMAC auth first
  if (!ctx.internalSecret) {
    return "Internal authentication not configured";
  }

  const authHeader = req.headers.authorization ?? null;
  const isValid = await verifyInternalToken(authHeader, ctx.internalSecret);

  if (isValid) return null;

  // HMAC failed — check if sandbox auth route
  if (isSandboxAuthRoute(path)) {
    const sessionIdMatch = path.match(/^\/sessions\/([^/]+)\//);
    if (sessionIdMatch) {
      const sessionId = sessionIdMatch[1];
      const token = authHeader?.startsWith("Bearer ") ? authHeader.slice(7) : null;
      if (!token) return "Unauthorized: Missing sandbox token";

      const instance = ctx.sessionManager.get(sessionId);
      const result = await instance.handleVerifySandboxToken({ token });
      if (result.valid) return null;
      return "Unauthorized: Invalid sandbox token";
    }
  }

  return "Unauthorized";
}

// ── Route handler setup ──────────────────────────────────────────────────

export function setupRoutes(
  app: Express,
  sessionManager: SessionManager,
  sessionIndex: LocalSessionIndexStore,
  internalSecret: string,
  credentialStore?: CredentialStore
): void {
  // CORS middleware
  app.use((_req, res, next) => {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
    res.header("Access-Control-Allow-Headers", "Content-Type, Authorization");
    res.header("Access-Control-Max-Age", "86400");
    next();
  });

  app.options("*", (_req, res) => {
    res.sendStatus(204);
  });

  // Auth middleware (skip OPTIONS, skip public routes)
  app.use(async (req, res, next) => {
    if (req.method === "OPTIONS") return next();

    const ctx = buildContext(sessionManager, sessionIndex, internalSecret, req);
    const authError = await verifyAuth(req, ctx, req.path);
    if (authError) {
      const status = authError.includes("not configured") ? 500 : 401;
      errorResponse(res, authError, status);
      return;
    }
    next();
  });

  // ── Health ───────────────────────────────────────────────────────────
  app.get("/health", (_req, res) => {
    jsonResponse(res, { status: "healthy", service: "open-inspect-control-plane" });
  });

  // ── Sessions ─────────────────────────────────────────────────────────

  app.get("/sessions", (req, res) => {
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 100);
    const offset = parseInt(req.query.offset as string) || 0;
    const status = (req.query.status as string) || undefined;
    const excludeStatus = (req.query.excludeStatus as string) || undefined;

    const result = sessionIndex.list({ status, excludeStatus, limit, offset });
    jsonResponse(res, {
      sessions: result.sessions,
      total: result.total,
      hasMore: result.hasMore,
    });
  });

  app.post("/sessions", async (req, res) => {
    const body = req.body;

    if (!body.repoOwner || !body.repoName) {
      errorResponse(res, "repoOwner and repoName are required");
      return;
    }

    const repoOwner = body.repoOwner.toLowerCase();
    const repoName = body.repoName.toLowerCase();

    const sessionId = generateId();
    const instance = sessionManager.get(sessionId);

    const model = body.model || "zai-coding-plan/glm-4.7";
    const reasoningEffort = body.reasoningEffort ?? null;

    // Store credentials and generate proxy keys if provided
    let proxyKeys: { gitProxyKey: string | null; llmProxyKey: string | null } | null = null;
    let availableLlmProviders: string[] = [];
    if (credentialStore && body.credentials) {
      proxyKeys = credentialStore.store(sessionId, body.credentials, body.modelChains ?? null);
      availableLlmProviders = credentialStore.getAvailableProviders(sessionId);
    }

    const initResult = await instance.handleInit({
      sessionName: sessionId,
      repoOwner,
      repoName,
      repoId: body.repoId ?? null,
      title: body.title,
      model,
      reasoningEffort,
      userId: body.userId || "anonymous",
      githubLogin: body.githubLogin,
      githubName: body.githubName,
      githubEmail: body.githubEmail,
      githubTokenEncrypted: body.githubTokenEncrypted ?? null,
      gitUrl: body.gitUrl ?? null,
      proxyKeys: proxyKeys ?? undefined,
      availableLlmProviders,
      agentModels: body.agentModels ?? null,
      opencodeFiles: body.opencodeFiles ?? body.agentFiles ?? null,
      modelChains: body.modelChains ?? null,
    });

    if (!initResult) {
      errorResponse(res, "Failed to create session", 500);
      return;
    }

    const now = Date.now();
    sessionIndex.create({
      id: sessionId,
      title: body.title || null,
      repoOwner,
      repoName,
      model,
      reasoningEffort,
      status: "created",
      createdAt: now,
      updatedAt: now,
    });

    jsonResponse(res, { sessionId, status: "created" }, 201);
  });

  app.get("/sessions/:id", async (req, res) => {
    const sessionId = req.params.id;
    const instance = sessionManager.get(sessionId);
    const state = await instance.handleGetState();

    if (!state) {
      errorResponse(res, "Session not found", 404);
      return;
    }

    jsonResponse(res, state);
  });

  app.delete("/sessions/:id", async (req, res) => {
    const sessionId = req.params.id;
    credentialStore?.destroy(sessionId);
    // Destroy sandbox container before deleting index entry
    try {
      const instance = sessionManager.get(sessionId);
      await instance.handleDestroyContainer();
    } catch {
      // Non-fatal: container may already be gone or session may not exist
    }
    sessionIndex.delete(sessionId);
    jsonResponse(res, { status: "deleted", sessionId });
  });

  app.post("/sessions/:id/prompt", async (req, res) => {
    const sessionId = req.params.id;
    const body = req.body;

    if (!body.content) {
      errorResponse(res, "content is required");
      return;
    }

    const instance = sessionManager.get(sessionId);
    const result = await instance.handleEnqueuePrompt({
      content: body.content,
      authorId: body.authorId || "anonymous",
      source: body.source || "web",
      model: body.model,
      reasoningEffort: body.reasoningEffort,
      attachments: body.attachments,
      callbackContext:
        body.callbackContext ||
        (body.callbackUrl
          ? { callbackUrl: body.callbackUrl, callbackSecret: body.callbackSecret }
          : undefined),
    });

    // Touch session index timestamp
    sessionIndex.touchUpdatedAt(sessionId);

    jsonResponse(res, result);
  });

  app.post("/sessions/:id/stop", async (req, res) => {
    const sessionId = req.params.id;
    const instance = sessionManager.get(sessionId);
    // stopExecution is handled internally via the WS "stop" command;
    // for the HTTP API, we enqueue a stop through the instance.
    // We need a public stop handler — call the internal method.
    // The original DO forwards to handleStop, but we can use the same logic.
    const wsManager = (instance as any).wsManager;
    const sandboxWs = wsManager?.getSandboxSocket?.();

    // Mark processing messages as failed
    const processingMsg = (instance as any).sql
      ?.exec?.("SELECT id FROM messages WHERE status = 'processing' LIMIT 1")
      ?.toArray?.()?.[0] as any;

    if (processingMsg) {
      (instance as any).sql?.exec?.(
        "UPDATE messages SET status = 'failed', completed_at = ? WHERE id = ?",
        Date.now(),
        processingMsg.id
      );
    }

    if (sandboxWs) {
      wsManager.send(sandboxWs, { type: "stop" });
    }

    jsonResponse(res, { status: "stopped" });
  });

  app.get("/sessions/:id/events", (req, res) => {
    const sessionId = req.params.id;
    const instance = sessionManager.get(sessionId);
    const result = instance.handleListEvents({
      limit: req.query.limit,
      cursor: req.query.cursor,
      type: req.query.type,
      message_id: req.query.message_id,
    });
    jsonResponse(res, result);
  });

  app.get("/sessions/:id/artifacts", (req, res) => {
    const sessionId = req.params.id;
    const instance = sessionManager.get(sessionId);
    jsonResponse(res, instance.handleListArtifacts());
  });

  app.get("/sessions/:id/participants", (req, res) => {
    const sessionId = req.params.id;
    const instance = sessionManager.get(sessionId);
    jsonResponse(res, instance.handleListParticipants());
  });

  app.post("/sessions/:id/participants", async (req, res) => {
    const sessionId = req.params.id;
    const instance = sessionManager.get(sessionId);
    // handleGenerateWsToken doubles as add-participant
    const result = await instance.handleGenerateWsToken(req.body);
    jsonResponse(res, result);
  });

  app.get("/sessions/:id/messages", (req, res) => {
    const sessionId = req.params.id;
    const instance = sessionManager.get(sessionId);
    const result = instance.handleListMessages({
      limit: req.query.limit,
      cursor: req.query.cursor,
      status: req.query.status,
    });
    jsonResponse(res, result);
  });

  app.post("/sessions/:id/ws-token", async (req, res) => {
    const sessionId = req.params.id;
    const body = req.body;

    if (!body.userId) {
      errorResponse(res, "userId is required");
      return;
    }

    const instance = sessionManager.get(sessionId);
    const result = await instance.handleGenerateWsToken({
      userId: body.userId,
      githubUserId: body.githubUserId,
      githubLogin: body.githubLogin,
      githubName: body.githubName,
      githubEmail: body.githubEmail,
    });

    jsonResponse(res, result);
  });

  app.post("/sessions/:id/archive", async (req, res) => {
    const sessionId = req.params.id;
    const instance = sessionManager.get(sessionId);
    const result = await instance.handleArchive(req.body);

    if (!result.error) {
      sessionIndex.updateStatus(sessionId, "archived");
    }

    jsonResponse(res, result);
  });

  app.post("/sessions/:id/unarchive", async (req, res) => {
    const sessionId = req.params.id;
    const instance = sessionManager.get(sessionId);
    const result = await instance.handleUnarchive(req.body);

    if (!result.error) {
      sessionIndex.updateStatus(sessionId, "active");
    }

    jsonResponse(res, result);
  });

  // ── Sandbox event endpoint (called by sandbox bridge) ────────────────

  app.post("/sessions/:id/sandbox-event", async (req, res) => {
    const sessionId = req.params.id;
    const instance = sessionManager.get(sessionId);
    // Forward event to session instance for processing
    try {
      await (instance as any).processSandboxEvent(req.body);
      jsonResponse(res, { status: "ok" });
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      errorResponse(res, msg, 500);
    }
  });

  // ── PR creation (from sandbox) ──────────────────────────────────────

  app.post("/sessions/:id/pr", async (req, res) => {
    const sessionId = req.params.id;
    const body = req.body;
    if (!body.title || !body.body || !body.head || !body.base) {
      errorResponse(res, "title, body, head, and base are required");
      return;
    }

    // Look up git credentials from the credential store
    if (!credentialStore) {
      errorResponse(res, "Credential store not available", 500);
      return;
    }

    const proxyKeys = credentialStore.getProxyKeys(sessionId);
    if (!proxyKeys?.gitProxyKey) {
      errorResponse(res, "No git credentials for this session", 403);
      return;
    }

    const creds = credentialStore.getByGitProxyKey(proxyKeys.gitProxyKey);
    if (!creds) {
      errorResponse(res, "Git credentials not found", 403);
      return;
    }

    // Determine the git host and create PR accordingly
    const repoPath = creds.authorizedRepo || body.repo;
    if (!repoPath) {
      errorResponse(res, "Cannot determine target repository");
      return;
    }

    const [owner, repo] = repoPath.split("/");
    const isGitHub = creds.provider === "github" || creds.url.includes("github.com");

    try {
      let prUrl: string;
      let prNumber: number;

      if (isGitHub) {
        // GitHub API: create pull request
        const ghResp = await fetch(`https://api.github.com/repos/${owner}/${repo}/pulls`, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${creds.password}`,
            Accept: "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            title: body.title,
            body: body.body,
            head: body.head,
            base: body.base,
          }),
        });

        if (!ghResp.ok) {
          const errText = await ghResp.text();
          console.error(`[pr] GitHub PR creation failed: ${ghResp.status} ${errText}`);
          errorResponse(res, `GitHub API error: ${ghResp.status}`, ghResp.status);
          return;
        }

        const prData = (await ghResp.json()) as { number: number; html_url: string };
        prNumber = prData.number;
        prUrl = prData.html_url;
      } else {
        // Gitea API: create pull request
        const giteaBase = creds.url.replace(/\/$/, "");
        const giteaResp = await fetch(`${giteaBase}/api/v1/repos/${owner}/${repo}/pulls`, {
          method: "POST",
          headers: {
            Authorization: `Basic ${Buffer.from(`${creds.username}:${creds.password}`).toString("base64")}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            title: body.title,
            body: body.body,
            head: body.head,
            base: body.base,
          }),
        });

        if (!giteaResp.ok) {
          const errText = await giteaResp.text();
          console.error(`[pr] Gitea PR creation failed: ${giteaResp.status} ${errText}`);
          errorResponse(res, `Gitea API error: ${giteaResp.status}`, giteaResp.status);
          return;
        }

        const prData = (await giteaResp.json()) as { number: number; html_url: string };
        prNumber = prData.number;
        prUrl = prData.html_url;
      }

      console.log(`[pr] PR #${prNumber} created: ${prUrl}`);
      jsonResponse(res, { status: "created", prNumber, prUrl });
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      console.error(`[pr] Error creating PR: ${msg}`);
      errorResponse(res, "Failed to create pull request", 500);
    }
  });

  // ── OpenAI token refresh (from sandbox) ─────────────────────────────

  app.post("/sessions/:id/openai-token-refresh", (_req, res) => {
    jsonResponse(res, { status: "not_applicable" });
  });

  // ── Repos (GitHub App installation) ─────────────────────────────────

  app.get("/repos", async (_req, res) => {
    const { GITHUB_APP_ID, GITHUB_APP_PRIVATE_KEY, GITHUB_APP_INSTALLATION_ID } = config;

    if (!GITHUB_APP_ID || !GITHUB_APP_PRIVATE_KEY || !GITHUB_APP_INSTALLATION_ID) {
      errorResponse(res, "GitHub App not configured", 500);
      return;
    }

    try {
      // Generate JWT — handle literal \n in env var and PKCS#1 key format
      const privateKey = GITHUB_APP_PRIVATE_KEY.replace(/\\n/g, "\n");
      const now = Math.floor(Date.now() / 1000);
      const header = Buffer.from(JSON.stringify({ alg: "RS256", typ: "JWT" })).toString(
        "base64url"
      );
      const payload = Buffer.from(
        JSON.stringify({ iat: now - 60, exp: now + 600, iss: GITHUB_APP_ID })
      ).toString("base64url");
      const signer = crypto.createSign("RSA-SHA256");
      signer.update(`${header}.${payload}`);
      const signature = signer.sign(privateKey, "base64url");
      const jwtToken = `${header}.${payload}.${signature}`;

      // Exchange for installation token
      const tokenRes = await fetch(
        `https://api.github.com/app/installations/${GITHUB_APP_INSTALLATION_ID}/access_tokens`,
        {
          method: "POST",
          headers: {
            Authorization: `Bearer ${jwtToken}`,
            Accept: "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
          },
        }
      );

      if (!tokenRes.ok) {
        const err = await tokenRes.text();
        console.error("[repos] Token exchange failed:", err);
        errorResponse(res, "Failed to authenticate with GitHub", 500);
        return;
      }

      const { token } = (await tokenRes.json()) as { token: string };

      // Fetch repos (paginate)
      const repos: any[] = [];
      let page = 1;
      while (true) {
        const reposRes = await fetch(
          `https://api.github.com/installation/repositories?per_page=100&page=${page}`,
          {
            headers: {
              Authorization: `Bearer ${token}`,
              Accept: "application/vnd.github+json",
              "X-GitHub-Api-Version": "2022-11-28",
            },
          }
        );

        if (!reposRes.ok) break;

        const data = (await reposRes.json()) as { repositories: any[] };
        for (const r of data.repositories) {
          repos.push({
            id: r.id,
            owner: r.owner?.login ?? "",
            name: r.name,
            fullName: r.full_name,
            description: r.description ?? null,
            private: r.private,
            defaultBranch: r.default_branch,
          });
        }

        if (data.repositories.length < 100) break;
        page++;
      }

      jsonResponse(res, { repos, cached: false, cachedAt: new Date().toISOString() });
    } catch (e) {
      console.error("[repos] Error:", e);
      errorResponse(res, "Failed to fetch repositories", 500);
    }
  });

  // ── Model preferences ─────────────────────────────────────────────

  // In-memory store (persists for lifetime of process)
  let enabledModels: string[] | null = null;

  app.get("/model-preferences", (_req, res) => {
    jsonResponse(res, { enabledModels: enabledModels ?? [...VALID_MODELS] });
  });

  app.put("/model-preferences", (req, res) => {
    const body = req.body;
    if (!body?.enabledModels || !Array.isArray(body.enabledModels)) {
      errorResponse(res, "Request body must include enabledModels array");
      return;
    }

    const deduplicated = [...new Set(body.enabledModels as string[])];
    const invalid = deduplicated.filter((m) => !isValidModel(m));
    if (invalid.length > 0) {
      errorResponse(res, `Invalid model(s): ${invalid.join(", ")}`);
      return;
    }
    if (deduplicated.length === 0) {
      errorResponse(res, "At least one model must be enabled");
      return;
    }

    enabledModels = deduplicated;
    jsonResponse(res, { status: "updated", enabledModels });
  });

  // ── 404 fallback ────────────────────────────────────────────────────

  app.use((_req, res) => {
    errorResponse(res, "Not found", 404);
  });
}

function buildContext(
  sessionManager: SessionManager,
  sessionIndex: LocalSessionIndexStore,
  internalSecret: string,
  req: Request
): RouteContext {
  return {
    sessionManager,
    sessionIndex,
    internalSecret,
    traceId: (req.headers["x-trace-id"] as string) || crypto.randomUUID(),
    requestId: crypto.randomUUID().slice(0, 8),
  };
}
