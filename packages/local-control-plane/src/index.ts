/**
 * Local Control Plane — Express + WebSocket server entry point.
 * Replaces the Cloudflare Workers entry point with a standalone Node.js server.
 *
 * Startup: tsx src/index.ts
 */

import http from "http";
import express from "express";
import { WebSocketServer } from "ws";
import path from "path";
import Database from "better-sqlite3";
import { config } from "./config.js";
import { SessionManager } from "./session/session-manager.js";
import { createLocalSandboxClient } from "./sandbox/local-client.js";
import { LocalSessionIndexStore } from "./db/session-index-local.js";
import { CredentialStore } from "./credentials/credential-store.js";
import { setupGitProxy } from "./proxy/git-proxy.js";
import { setupGithubApiProxy } from "./proxy/github-api-proxy.js";
import { setupLlmProxy } from "./proxy/llm-proxy.js";
import { setupRoutes } from "./router.js";
import fs from "fs";

// ── Bootstrap ────────────────────────────────────────────────────────────

// Ensure data directory exists
fs.mkdirSync(config.DATA_DIR, { recursive: true });

// Session index — shared SQLite database for listing sessions
const indexDbPath = path.join(config.DATA_DIR, "session-index.db");
const indexDb = new Database(indexDbPath);
indexDb.pragma("journal_mode = WAL");
indexDb.pragma("busy_timeout = 5000");
const sessionIndex = new LocalSessionIndexStore(indexDb);

// Sandbox client — HTTP client for the local sandbox manager
const sandboxClient = createLocalSandboxClient(config.MODAL_API_SECRET, config.SANDBOX_MANAGER_URL);

// Credential store — in-memory, per-session credential storage for proxy endpoints
const credentialStore = new CredentialStore();

// Session manager — Map<sessionId, SessionInstance>
const sessionManager = new SessionManager(sandboxClient, sessionIndex, credentialStore);

// ── Express app ──────────────────────────────────────────────────────────

const app = express();
// JSON body parser — skip proxy paths (they use their own raw body parsers
// to support streaming passthrough; express.json() would consume the body stream).
app.use((req, res, next) => {
  if (
    req.path.startsWith("/llm-proxy/") ||
    req.path.startsWith("/github-api-proxy/") ||
    req.path.startsWith("/git-proxy/")
  ) return next();
  express.json({ limit: "10mb" })(req, res, next);
});

// Set up proxy routes BEFORE auth middleware (proxy key IS the auth)
setupGitProxy(app, credentialStore);
setupGithubApiProxy(app, credentialStore);
setupLlmProxy(app, credentialStore, {
  onProviderUnhealthy: (sessionId, provider, errorCount) => {
    try {
      const instance = sessionManager.get(sessionId);
      instance
        .processSandboxEvent({
          type: "provider_unhealthy",
          provider,
          consecutiveErrors: errorCount,
          source: "proxy_error_counter",
          timestamp: Date.now(),
        })
        .catch(console.error);
    } catch (e) {
      console.error(`[llm-proxy] Failed to notify session ${sessionId} of unhealthy provider:`, e);
    }
  },
  onLlmResult: (sessionId, success) => {
    try {
      const instance = sessionManager.get(sessionId);
      instance.updateLlmHealth(success);
    } catch {
      // Session may have been destroyed — ignore
    }
  },
});

// Set up API routes (includes auth middleware)
setupRoutes(
  app,
  sessionManager,
  sessionIndex,
  config.INTERNAL_CALLBACK_SECRET || config.MODAL_API_SECRET,
  credentialStore
);

// ── HTTP + WebSocket server ──────────────────────────────────────────────

const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

/**
 * Handle HTTP upgrade requests for WebSocket connections.
 * Path format: /sessions/:id/ws
 * Query params distinguish sandbox vs client connections:
 *   - ?type=sandbox&sandboxId=...  (sandbox bridge connection)
 *   - No type param                (client connection)
 */
server.on("upgrade", (req, socket, head) => {
  const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
  const match = url.pathname.match(/^\/sessions\/([^/]+)\/ws$/);

  if (!match) {
    socket.write("HTTP/1.1 400 Bad Request\r\n\r\n");
    socket.destroy();
    return;
  }

  const sessionId = match[1];
  const isSandbox = url.searchParams.get("type") === "sandbox";
  // Bridge sends sandbox ID as X-Sandbox-ID header; also check query params
  const sandboxId =
    url.searchParams.get("sandboxId") || (req.headers["x-sandbox-id"] as string) || undefined;
  const authHeader = req.headers.authorization;

  console.log(
    `[ws] Upgrade request: session=${sessionId} sandbox=${isSandbox} sandboxId=${sandboxId || "none"}`
  );

  wss.handleUpgrade(req, socket, head, (ws) => {
    const instance = sessionManager.get(sessionId);
    const accepted = instance.handleWebSocketUpgrade(ws, isSandbox, sandboxId, authHeader);

    if (!accepted) {
      // handleWebSocketUpgrade already closed the socket
      return;
    }

    // Wire up message/close/error handlers
    ws.on("message", async (data) => {
      try {
        const message = data.toString();
        await instance.handleMessage(ws, message);
      } catch (e) {
        console.error(`[ws] Message error session=${sessionId}:`, e);
      }
    });

    ws.on("close", (code, reason) => {
      instance.handleClose(ws, code, reason.toString());
      console.log(`[ws] Closed: session=${sessionId} code=${code}`);
    });

    ws.on("error", (err) => {
      console.error(`[ws] Error session=${sessionId}:`, err.message);
    });
  });
});

// ── Start ────────────────────────────────────────────────────────────────

server.listen(config.PORT, () => {
  console.log(`Local Control Plane running on http://localhost:${config.PORT}`);
  console.log(`  WebSocket: ws://localhost:${config.PORT}/sessions/:id/ws`);
  console.log(`  Sandbox Manager: ${config.SANDBOX_MANAGER_URL}`);
  console.log(`  Data directory: ${config.DATA_DIR}`);
});

// Graceful shutdown
process.on("SIGTERM", () => {
  console.log("Shutting down...");
  server.close(() => {
    indexDb.close();
    process.exit(0);
  });
});

process.on("SIGINT", () => {
  console.log("Shutting down...");
  server.close(() => {
    indexDb.close();
    process.exit(0);
  });
});
