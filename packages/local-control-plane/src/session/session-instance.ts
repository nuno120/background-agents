/**
 * SessionInstance — the Node.js equivalent of SessionDO.
 *
 * Key adaptations from the Cloudflare Durable Object:
 * - `extends DurableObject` → plain class
 * - `this.ctx.storage.sql` → SqlStorageAdapter (better-sqlite3)
 * - `this.ctx.storage.setAlarm(ts)` → setTimeout
 * - `this.ctx.waitUntil(p)` → p.catch(console.error) (fire-and-forget)
 * - `new WebSocketPair()` → handled by the ws library at the server level
 * - Hibernation recovery → not needed (Node process is persistent)
 *
 * This file re-implements the SessionDO initialization, WebSocket handling,
 * and HTTP internal API routes. The business logic services (repository, schema,
 * lifecycle manager, etc.) are imported from the original control-plane package
 * wherever possible, or from local adaptations.
 */

import crypto from "crypto";
import type { WebSocket as WsWebSocket } from "ws";
import { generateInternalToken } from "@open-inspect/shared";
import type { SqlStorage } from "./sqlite-adapter.js";
import type { LocalSandboxClient } from "../sandbox/local-client.js";
import type { LocalSessionIndexStore } from "../db/session-index-local.js";
import type { CredentialStore } from "../credentials/credential-store.js";
import { NodeWebSocketManager, type ClientInfo } from "./websocket-manager-node.js";

// We re-use the original session schema and repository since they depend only on SqlStorage.
// They must be copied or symlinked from packages/control-plane/src/session/.
// For now, we inline the critical parts.

const WS_AUTH_TIMEOUT_MS = 30000;

/** Minimal logger. */
function createLog(sessionId: string) {
  return {
    info: (msg: string, ctx?: object) =>
      console.log(`[${sessionId}] ${msg}`, ctx ? JSON.stringify(ctx) : ""),
    warn: (msg: string, ctx?: object) =>
      console.warn(`[${sessionId}] ${msg}`, ctx ? JSON.stringify(ctx) : ""),
    error: (msg: string, ctx?: object) =>
      console.error(`[${sessionId}] ${msg}`, ctx ? JSON.stringify(ctx) : ""),
    debug: (_msg: string, _ctx?: object) => {},
  };
}

function generateId(bytes = 16): string {
  return crypto.randomBytes(bytes).toString("hex");
}

async function hashToken(token: string): Promise<string> {
  const hash = crypto.createHash("sha256").update(token).digest("hex");
  return hash;
}

// ── Schema (inline from schema.ts) ──────────────────────────────────────

const SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS session (
  id TEXT PRIMARY KEY,
  session_name TEXT,
  title TEXT,
  repo_owner TEXT NOT NULL,
  repo_name TEXT NOT NULL,
  repo_id INTEGER,
  repo_default_branch TEXT NOT NULL DEFAULT 'main',
  branch_name TEXT,
  base_sha TEXT,
  current_sha TEXT,
  opencode_session_id TEXT,
  model TEXT DEFAULT 'anthropic/claude-haiku-4-5',
  reasoning_effort TEXT,
  git_url TEXT,
  status TEXT DEFAULT 'created',
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS participants (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL,
  github_user_id TEXT,
  github_login TEXT,
  github_email TEXT,
  github_name TEXT,
  role TEXT NOT NULL DEFAULT 'member',
  github_access_token_encrypted TEXT,
  github_refresh_token_encrypted TEXT,
  github_token_expires_at INTEGER,
  ws_auth_token TEXT,
  ws_token_created_at INTEGER,
  joined_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
  id TEXT PRIMARY KEY,
  author_id TEXT NOT NULL,
  content TEXT NOT NULL,
  source TEXT NOT NULL,
  model TEXT,
  reasoning_effort TEXT,
  attachments TEXT,
  callback_context TEXT,
  status TEXT DEFAULT 'pending',
  error_message TEXT,
  created_at INTEGER NOT NULL,
  started_at INTEGER,
  completed_at INTEGER,
  FOREIGN KEY (author_id) REFERENCES participants(id)
);
CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  data TEXT NOT NULL,
  message_id TEXT,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS artifacts (
  id TEXT PRIMARY KEY,
  type TEXT NOT NULL,
  url TEXT,
  metadata TEXT,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS sandbox (
  id TEXT PRIMARY KEY,
  modal_sandbox_id TEXT,
  modal_object_id TEXT,
  snapshot_id TEXT,
  snapshot_image_id TEXT,
  auth_token TEXT,
  status TEXT DEFAULT 'pending',
  git_sync_status TEXT DEFAULT 'pending',
  last_heartbeat INTEGER,
  last_activity INTEGER,
  last_spawn_error TEXT,
  last_spawn_error_at INTEGER,
  spawn_failure_count INTEGER DEFAULT 0,
  last_spawn_failure INTEGER,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS ws_client_mapping (
  ws_id TEXT PRIMARY KEY,
  participant_id TEXT NOT NULL,
  client_id TEXT,
  created_at INTEGER NOT NULL,
  FOREIGN KEY (participant_id) REFERENCES participants(id)
);
CREATE INDEX IF NOT EXISTS idx_messages_status ON messages(status);
CREATE INDEX IF NOT EXISTS idx_messages_author ON messages(author_id);
CREATE INDEX IF NOT EXISTS idx_events_message ON events(message_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at, id);
CREATE INDEX IF NOT EXISTS idx_participants_user ON participants(user_id);
`;

function initSchema(sql: SqlStorage): void {
  sql.exec(SCHEMA_SQL);
}

// ── SessionInstance ─────────────────────────────────────────────────────

export class SessionInstance {
  private sql: SqlStorage;
  private log: ReturnType<typeof createLog>;
  private wsManager: NodeWebSocketManager;
  private sandboxClient: LocalSandboxClient;
  private sessionIndex: LocalSessionIndexStore;
  private config: any;
  private initialized = false;
  private alarmTimer: ReturnType<typeof setTimeout> | null = null;
  private isSpawningSandbox = false;
  private gitProxyKey: string | null = null;
  private llmProxyKey: string | null = null;
  private githubApiProxyKey: string | null = null;
  private availableLlmProviders: string[] = [];
  private agentModels: Record<string, string> | null = null;
  private opencodeFiles: Record<string, string> | null = null;
  private modelChains: Record<string, Array<{ provider: string; model: string }>> | null = null;
  private credentialStore: CredentialStore | null = null;
  private lastSuccessfulLlmCall: number | null = null;
  private lastLlmCallAttempt: number | null = null;
  private llmCallAttemptCount = 0;
  private static readonly LLM_FAILURE_TIMEOUT_MS = 300_000; // 5 minutes of LLM failures → kill
  private static readonly LLM_MIN_ATTEMPTS_BEFORE_KILL = 3; // Minimum failed attempts before C3 triggers

  constructor(
    private sessionId: string,
    sql: SqlStorage,
    sandboxClient: LocalSandboxClient,
    sessionIndex: LocalSessionIndexStore,
    appConfig: any,
    credentialStore?: CredentialStore
  ) {
    this.sql = sql;
    this.log = createLog(sessionId);
    this.wsManager = new NodeWebSocketManager(WS_AUTH_TIMEOUT_MS);
    this.sandboxClient = sandboxClient;
    this.sessionIndex = sessionIndex;
    this.config = appConfig;
    this.credentialStore = credentialStore ?? null;
    this.ensureInitialized();
  }

  private ensureInitialized(): void {
    if (this.initialized) return;
    initSchema(this.sql);
    this.initialized = true;
  }

  // ── SQL helpers ───────────────────────────────────────────────────────

  private getSession(): any | null {
    const rows = this.sql.exec("SELECT * FROM session LIMIT 1").toArray() as any[];
    return rows[0] ?? null;
  }

  private getSandbox(): any | null {
    const rows = this.sql.exec("SELECT * FROM sandbox LIMIT 1").toArray() as any[];
    return rows[0] ?? null;
  }

  private getMessageCount(): number {
    const row = this.sql.exec("SELECT COUNT(*) as count FROM messages").one() as any;
    return row?.count ?? 0;
  }

  private getIsProcessing(): boolean {
    const rows = this.sql
      .exec("SELECT id FROM messages WHERE status = 'processing' LIMIT 1")
      .toArray();
    return rows.length > 0;
  }

  private updateSandboxStatus(status: string): void {
    this.sql.exec(
      "UPDATE sandbox SET status = ? WHERE id = (SELECT id FROM sandbox LIMIT 1)",
      status
    );
  }

  private updateSandboxLastActivity(timestamp: number): void {
    this.sql.exec(
      "UPDATE sandbox SET last_activity = ? WHERE id = (SELECT id FROM sandbox LIMIT 1)",
      timestamp
    );
  }

  // ── Alarm (setTimeout replacement for DurableObject setAlarm) ─────────

  private scheduleAlarm(timestampMs: number): void {
    if (this.alarmTimer) clearTimeout(this.alarmTimer);
    const delay = Math.max(0, timestampMs - Date.now());
    this.alarmTimer = setTimeout(() => this.handleAlarm(), delay);
  }

  private async handleAlarm(): Promise<void> {
    const sandbox = this.getSandbox();
    if (!sandbox) return;

    if (sandbox.status === "stopped" || sandbox.status === "failed" || sandbox.status === "stale")
      return;

    const now = Date.now();
    const inactivityTimeoutMs = parseInt(this.config.SANDBOX_INACTIVITY_TIMEOUT_MS || "600000", 10);

    // Check 1: Total inactivity (no sandbox activity at all)
    if (sandbox.last_activity) {
      const inactiveTime = now - sandbox.last_activity;
      if (inactiveTime >= inactivityTimeoutMs) {
        this.log.info("Inactivity timeout", { last_activity: sandbox.last_activity });
        this.updateSandboxStatus("stopped");
        this.broadcast({ type: "sandbox_status", status: "stopped" });
        await this.destroySandboxContainer();
        const sandboxSocket = this.wsManager.getSandboxSocket();
        if (sandboxSocket) {
          this.wsManager.send(sandboxSocket, { type: "shutdown" });
        }
        return;
      }
    }

    // Check 2 (C3): LLM health — if LLM calls have been failing for too long
    // Requires minimum number of attempts to avoid false-triggers on initial startup
    if (
      this.lastLlmCallAttempt &&
      this.llmCallAttemptCount >= SessionInstance.LLM_MIN_ATTEMPTS_BEFORE_KILL
    ) {
      const referenceTime = this.lastSuccessfulLlmCall || this.lastLlmCallAttempt;
      const failingDuration = now - referenceTime;

      if (failingDuration >= SessionInstance.LLM_FAILURE_TIMEOUT_MS) {
        this.log.warn("LLM failure timeout", {
          lastSuccessfulLlmCall: this.lastSuccessfulLlmCall,
          lastLlmCallAttempt: this.lastLlmCallAttempt,
          llmCallAttemptCount: this.llmCallAttemptCount,
          failingMs: failingDuration,
        });

        // Fire provider_unhealthy event + webhook
        await this.processSandboxEvent({
          type: "provider_unhealthy",
          reason: "llm_failure_timeout",
          source: "activity_watchdog",
          failingMs: failingDuration,
          timestamp: now,
        });

        // Kill the sandbox
        this.updateSandboxStatus("failed");
        this.broadcast({ type: "sandbox_error", error: "LLM provider unresponsive" });
        await this.destroySandboxContainer();
        return;
      }
    }

    // Reschedule — check every minute at most
    this.scheduleAlarm(now + Math.min(inactivityTimeoutMs, 60_000));
  }

  /** Invalidate all proxy keys for this session (git + LLM). Idempotent. */
  private destroyCredentials(): void {
    if (this.credentialStore) {
      this.credentialStore.destroy(this.sessionId);
      this.log.info("Credentials destroyed");
    }
  }

  /**
   * Destroy (stop + remove) the sandbox container via sandbox-manager.
   * Non-fatal: logs errors but doesn't throw.
   */
  private async destroySandboxContainer(): Promise<void> {
    // Invalidate proxy keys before destroying container
    this.destroyCredentials();

    const sandbox = this.getSandbox();
    if (!sandbox?.modal_sandbox_id) return;

    try {
      const stopUrl = this.sandboxClient.getStopSandboxUrl();
      const secret = this.config.MODAL_API_SECRET;
      if (!secret) {
        this.log.warn("Cannot destroy sandbox: no MODAL_API_SECRET configured");
        return;
      }

      const token = await generateInternalToken(secret);
      const response = await fetch(stopUrl, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ sandbox_id: sandbox.modal_sandbox_id }),
      });

      if (response.ok) {
        this.log.info("Sandbox container destroyed", {
          sandbox_id: sandbox.modal_sandbox_id,
        });
      } else {
        const text = await response.text();
        this.log.warn("Sandbox container destroy failed (non-fatal)", {
          sandbox_id: sandbox.modal_sandbox_id,
          status: response.status,
          error: text,
        });
      }
    } catch (error) {
      this.log.warn("Sandbox container destroy error (non-fatal)", {
        sandbox_id: sandbox.modal_sandbox_id,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }

  // ── WebSocket handling ────────────────────────────────────────────────

  handleWebSocketUpgrade(
    ws: WsWebSocket,
    isSandbox: boolean,
    sandboxId?: string,
    authHeader?: string
  ): boolean {
    this.ensureInitialized();

    if (isSandbox) {
      // Validate sandbox auth
      const sandbox = this.getSandbox();
      const expectedToken = sandbox?.auth_token;
      const expectedSandboxId = sandbox?.modal_sandbox_id;

      if (sandbox?.status === "stopped" || sandbox?.status === "stale") {
        this.log.warn("Rejecting sandbox WS: stopped/stale");
        ws.close(1000, "Sandbox is stopped");
        return false;
      }

      if (expectedSandboxId && sandboxId !== expectedSandboxId) {
        this.log.warn("Rejecting sandbox WS: ID mismatch");
        ws.close(1008, "Forbidden: Wrong sandbox ID");
        return false;
      }

      if (!authHeader || authHeader !== `Bearer ${expectedToken}`) {
        this.log.warn("Rejecting sandbox WS: token mismatch");
        ws.close(1008, "Unauthorized");
        return false;
      }

      // Accept sandbox WebSocket
      this.wsManager.acceptAndSetSandboxSocket(ws, sandboxId);
      this.isSpawningSandbox = false;
      this.updateSandboxStatus("ready");
      this.broadcast({ type: "sandbox_status", status: "ready" });

      const now = Date.now();
      this.updateSandboxLastActivity(now);
      const timeoutMs = parseInt(this.config.SANDBOX_INACTIVITY_TIMEOUT_MS || "600000", 10);
      this.scheduleAlarm(now + timeoutMs);

      this.log.info("Sandbox WebSocket connected", { sandbox_id: sandboxId });

      // Process pending messages
      this.processMessageQueue().catch(console.error);
      return true;
    }

    // Client WebSocket — accept and enforce auth timeout
    const wsId = `ws-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    this.wsManager.acceptClientSocket(ws, wsId);
    this.wsManager.enforceAuthTimeout(ws, wsId).catch(console.error);
    return true;
  }

  async handleMessage(ws: WsWebSocket, message: string): Promise<void> {
    this.ensureInitialized();

    const { kind } = this.wsManager.classify(ws);
    if (kind === "sandbox") {
      await this.handleSandboxMessage(ws, message);
    } else {
      await this.handleClientMessage(ws, message);
    }
  }

  handleClose(ws: WsWebSocket, code: number, _reason: string): void {
    this.ensureInitialized();
    const { kind } = this.wsManager.classify(ws);

    if (kind === "sandbox") {
      const wasActive = this.wsManager.clearSandboxSocketIfMatch(ws);
      if (wasActive && (code === 1000 || code === 1001)) {
        this.updateSandboxStatus("stopped");
        // Fire-and-forget container destruction on clean close
        this.destroySandboxContainer().catch((e) =>
          this.log.error("Destroy after WS close failed", {
            error: e instanceof Error ? e.message : String(e),
          })
        );
      }
    } else {
      const client = this.wsManager.removeClient(ws);
      if (client) {
        this.broadcast({ type: "presence_leave", userId: client.userId });
      }
    }
  }

  // ── Client message handling ───────────────────────────────────────────

  private async handleClientMessage(ws: WsWebSocket, raw: string): Promise<void> {
    try {
      const data = JSON.parse(raw);
      switch (data.type) {
        case "ping":
          this.wsManager.send(ws, { type: "pong", timestamp: Date.now() });
          break;

        case "subscribe":
          await this.handleSubscribe(ws, data);
          break;

        case "prompt":
          await this.handlePrompt(ws, data);
          break;

        case "stop":
          await this.stopExecution();
          break;

        case "fetch_history":
          this.handleFetchHistory(ws, data);
          break;

        case "typing":
          // Proactive warming
          this.warmSandbox().catch(console.error);
          break;

        case "presence":
          // No-op for now
          break;
      }
    } catch (e) {
      this.log.error("Client message error", {
        error: e instanceof Error ? e.message : String(e),
      });
    }
  }

  private async handleSubscribe(
    ws: WsWebSocket,
    data: { token: string; clientId: string }
  ): Promise<void> {
    if (!data.token) {
      ws.close(4001, "Authentication required");
      return;
    }

    const tokenHash = await hashToken(data.token);
    const rows = this.sql
      .exec("SELECT * FROM participants WHERE ws_auth_token = ?", tokenHash)
      .toArray() as any[];
    const participant = rows[0];

    if (!participant) {
      ws.close(4001, "Invalid authentication token");
      return;
    }

    const clientInfo: ClientInfo = {
      participantId: participant.id,
      userId: participant.user_id,
      name: participant.github_name || participant.github_login || participant.user_id,
      avatar: participant.github_login
        ? `https://avatars.githubusercontent.com/${participant.github_login}`
        : undefined,
      status: "active",
      lastSeen: Date.now(),
      clientId: data.clientId,
      ws,
    };

    this.wsManager.setClient(ws, clientInfo);

    // Gather session state and replay
    const sandbox = this.getSandbox();
    const state = this.getSessionState(sandbox);
    const replay = this.getReplayData();

    this.wsManager.send(ws, {
      type: "subscribed",
      sessionId: state.id,
      state,
      participantId: participant.id,
      participant: {
        participantId: participant.id,
        name: clientInfo.name,
        avatar: clientInfo.avatar,
      },
      replay,
      spawnError: sandbox?.last_spawn_error ?? null,
    });

    this.log.info("Client subscribed", {
      participant_id: participant.id,
      user_id: participant.user_id,
    });
  }

  private getSessionState(sandbox?: any): any {
    const session = this.getSession();
    sandbox ??= this.getSandbox();
    return {
      id: session?.id ?? this.sessionId,
      title: session?.title ?? null,
      repoOwner: session?.repo_owner ?? "",
      repoName: session?.repo_name ?? "",
      branchName: session?.branch_name ?? null,
      status: session?.status ?? "created",
      sandboxStatus: sandbox?.status ?? "pending",
      messageCount: this.getMessageCount(),
      createdAt: session?.created_at ?? Date.now(),
      model: session?.model ?? "zai-coding-plan/glm-4.7",
      reasoningEffort: session?.reasoning_effort ?? undefined,
      isProcessing: this.getIsProcessing(),
    };
  }

  private getReplayData(): {
    events: any[];
    hasMore: boolean;
    cursor: { timestamp: number; id: string } | null;
  } {
    const LIMIT = 500;
    const rows = this.sql
      .exec(
        `SELECT * FROM (
           SELECT * FROM events WHERE type != 'heartbeat'
           ORDER BY created_at DESC, id DESC LIMIT ?
         ) sub ORDER BY created_at ASC, id ASC`,
        LIMIT
      )
      .toArray() as any[];

    const events: any[] = [];
    for (const row of rows) {
      try {
        events.push(JSON.parse(row.data));
      } catch {
        /* ignore malformed JSON */
      }
    }

    const cursor = rows.length > 0 ? { timestamp: rows[0].created_at, id: rows[0].id } : null;

    return { events, hasMore: rows.length >= LIMIT, cursor };
  }

  private handleFetchHistory(
    ws: WsWebSocket,
    data: { cursor?: { timestamp: number; id: string }; limit?: number }
  ): void {
    if (!data.cursor) {
      this.wsManager.send(ws, {
        type: "error",
        code: "INVALID_CURSOR",
        message: "Invalid cursor",
      });
      return;
    }

    const limit = Math.max(1, Math.min(data.limit ?? 200, 500));
    const rows = this.sql
      .exec(
        `SELECT * FROM events
         WHERE type != 'heartbeat' AND ((created_at < ?1) OR (created_at = ?1 AND id < ?2))
         ORDER BY created_at DESC, id DESC LIMIT ?3`,
        data.cursor.timestamp,
        data.cursor.id,
        limit + 1
      )
      .toArray() as any[];

    const hasMore = rows.length > limit;
    if (hasMore) rows.pop();
    rows.reverse();

    const items: any[] = [];
    for (const row of rows) {
      try {
        items.push(JSON.parse(row.data));
      } catch {
        /* ignore malformed JSON */
      }
    }

    const oldestEvent = rows.length > 0 ? rows[0] : null;

    this.wsManager.send(ws, {
      type: "history_page",
      items,
      hasMore,
      cursor: oldestEvent ? { timestamp: oldestEvent.created_at, id: oldestEvent.id } : null,
    });
  }

  // ── Prompt handling ───────────────────────────────────────────────────

  private async handlePrompt(
    ws: WsWebSocket,
    data: { content: string; model?: string; reasoningEffort?: string }
  ): Promise<void> {
    const client = this.wsManager.getClient(ws);
    if (!client) {
      this.wsManager.send(ws, {
        type: "error",
        code: "NOT_SUBSCRIBED",
        message: "Must subscribe first",
      });
      return;
    }

    const participant = this.sql
      .exec("SELECT * FROM participants WHERE id = ?", client.participantId)
      .toArray()[0] as any;

    if (!participant) return;

    const now = Date.now();
    const messageId = generateId();
    const session = this.getSession();

    // Create message
    this.sql.exec(
      `INSERT INTO messages (id, author_id, content, source, model, reasoning_effort, status, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      messageId,
      client.participantId,
      data.content,
      "web",
      data.model || null,
      data.reasoningEffort || null,
      "pending",
      now
    );

    // Write user_message event for timeline replay
    const userMessageEvent = {
      type: "user_message",
      content: data.content,
      messageId,
      timestamp: now,
      author: {
        participantId: client.participantId,
        name: client.name,
        avatar: client.avatar,
      },
    };
    this.sql.exec(
      `INSERT INTO events (id, type, data, message_id, created_at) VALUES (?, ?, ?, ?, ?)`,
      `user_message:${messageId}`,
      "user_message",
      JSON.stringify(userMessageEvent),
      messageId,
      now
    );
    this.broadcast({ type: "sandbox_event", event: userMessageEvent });

    // Update session to active
    if (session && session.status === "created") {
      this.sql.exec(
        "UPDATE session SET status = 'active', updated_at = ? WHERE id = ?",
        now,
        session.id
      );
      this.sessionIndex.updateStatus(session.session_name || session.id, "active");
    }

    const position = (
      this.sql
        .exec("SELECT COUNT(*) as count FROM messages WHERE status IN ('pending', 'processing')")
        .one() as any
    ).count;
    this.wsManager.send(ws, { type: "prompt_queued", messageId, position });

    // Touch session index
    this.sessionIndex.touchUpdatedAt(session?.session_name || session?.id || this.sessionId);

    // Process queue
    await this.processMessageQueue();
  }

  private async processMessageQueue(): Promise<void> {
    // Check if already processing
    if (this.getIsProcessing()) return;

    // Get next pending message
    const nextMsg = this.sql
      .exec("SELECT * FROM messages WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1")
      .toArray()[0] as any;
    if (!nextMsg) return;

    // Ensure sandbox is connected
    const sandboxWs = this.wsManager.getSandboxSocket();
    if (!sandboxWs) {
      // Try to spawn sandbox
      await this.spawnSandbox();
      return;
    }

    const now = Date.now();
    this.sql.exec(
      "UPDATE messages SET status = 'processing', started_at = ? WHERE id = ?",
      now,
      nextMsg.id
    );

    // Get author info
    const participant = this.sql
      .exec("SELECT * FROM participants WHERE id = ?", nextMsg.author_id)
      .toArray()[0] as any;

    // Send prompt to sandbox
    const session = this.getSession();
    const command = {
      type: "prompt",
      messageId: nextMsg.id,
      content: nextMsg.content,
      model: nextMsg.model || session?.model,
      reasoningEffort: nextMsg.reasoning_effort || session?.reasoning_effort,
      author: {
        userId: participant?.user_id || "",
        githubName: participant?.github_name || null,
        githubEmail: participant?.github_email || null,
      },
    };

    if (nextMsg.attachments) {
      try {
        (command as any).attachments = JSON.parse(nextMsg.attachments);
      } catch {
        /* ignore malformed JSON */
      }
    }

    this.wsManager.send(sandboxWs, command);
    this.broadcast({ type: "processing_status", isProcessing: true });
    this.updateSandboxLastActivity(now);
  }

  // ── Sandbox event handling ────────────────────────────────────────────

  private async handleSandboxMessage(ws: WsWebSocket, raw: string): Promise<void> {
    try {
      const event = JSON.parse(raw);
      await this.processSandboxEvent(event);
    } catch (e) {
      this.log.error("Sandbox message error", {
        error: e instanceof Error ? e.message : String(e),
      });
    }
  }

  /** Called by proxy on each LLM call result for C3 health tracking. */
  updateLlmHealth(success: boolean): void {
    const now = Date.now();
    this.lastLlmCallAttempt = now;
    this.llmCallAttemptCount++;
    if (success) {
      this.lastSuccessfulLlmCall = now;
    }
  }

  async processSandboxEvent(event: any): Promise<void> {
    const now = Date.now();

    switch (event.type) {
      case "heartbeat":
        this.sql.exec(
          "UPDATE sandbox SET last_heartbeat = ? WHERE id = (SELECT id FROM sandbox LIMIT 1)",
          now
        );
        this.updateSandboxStatus(event.status || "ready");
        break;

      case "token":
        // Accumulate tokens — upsert single event per message
        if (event.messageId) {
          this.sql.exec(
            `INSERT INTO events (id, type, data, message_id, created_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET data = excluded.data, created_at = excluded.created_at`,
            `token:${event.messageId}`,
            "token",
            JSON.stringify(event),
            event.messageId,
            now
          );
        }
        this.broadcast({ type: "sandbox_event", event });
        this.updateSandboxLastActivity(now);
        break;

      case "tool_call":
      case "tool_result":
      case "step_start":
      case "step_finish":
      case "error":
        this.sql.exec(
          `INSERT INTO events (id, type, data, message_id, created_at)
           VALUES (?, ?, ?, ?, ?)`,
          generateId(),
          event.type,
          JSON.stringify(event),
          event.messageId || null,
          now
        );
        this.broadcast({ type: "sandbox_event", event });
        this.updateSandboxLastActivity(now);
        break;

      case "execution_complete":
        // Complete the processing message
        if (event.messageId) {
          const status = event.success ? "completed" : "failed";
          this.sql.exec(
            "UPDATE messages SET status = ?, completed_at = ? WHERE id = ?",
            status,
            now,
            event.messageId
          );

          // Upsert execution_complete event
          this.sql.exec(
            `INSERT INTO events (id, type, data, message_id, created_at)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET data = excluded.data, created_at = excluded.created_at`,
            `execution_complete:${event.messageId}`,
            "execution_complete",
            JSON.stringify(event),
            event.messageId,
            now
          );
        }

        // Send webhook callback if message has a callbackUrl
        if (event.messageId) {
          this.sendWebhookCallback(event.messageId, event.success ?? true).catch((err) => {
            this.log.error("Webhook callback failed", {
              messageId: event.messageId,
              error: String(err),
            });
          });
        }

        // Invalidate proxy keys — sandbox is done, no more git/LLM access needed
        this.destroyCredentials();

        this.broadcast({ type: "sandbox_event", event });
        this.broadcast({ type: "processing_status", isProcessing: false });
        this.updateSandboxLastActivity(now);

        // Destroy the sandbox container — execution is done, no reason to keep it alive.
        // This also prevents the alarm watchdog from firing a stale provider_unhealthy
        // event minutes later (the C3 check would otherwise timeout on frozen LLM health fields).
        this.updateSandboxStatus("stopped");
        this.destroySandboxContainer().catch((e) =>
          this.log.error("Container destroy after execution_complete failed", {
            error: e instanceof Error ? e.message : String(e),
          })
        );

        // Process next message in queue (no-op if queue is empty)
        await this.processMessageQueue();
        break;

      case "conversation_history":
        this.sql.exec(
          `INSERT INTO events (id, type, data, message_id, created_at)
           VALUES (?, ?, ?, ?, ?)`,
          generateId(),
          event.type,
          JSON.stringify(event),
          event.messageId || null,
          now
        );
        this.broadcast({ type: "sandbox_event", event });
        break;

      case "git_sync":
        if (event.status) {
          this.sql.exec(
            "UPDATE sandbox SET git_sync_status = ? WHERE id = (SELECT id FROM sandbox LIMIT 1)",
            event.status
          );
        }
        if (event.sha) {
          const session = this.getSession();
          if (session) {
            if (!session.base_sha) {
              this.sql.exec(
                "UPDATE session SET base_sha = ?, current_sha = ? WHERE id = ?",
                event.sha,
                event.sha,
                session.id
              );
            } else {
              this.sql.exec(
                "UPDATE session SET current_sha = ? WHERE id = ?",
                event.sha,
                session.id
              );
            }
          }
        }
        this.sql.exec(
          `INSERT INTO events (id, type, data, message_id, created_at) VALUES (?, ?, ?, ?, ?)`,
          generateId(),
          "git_sync",
          JSON.stringify(event),
          null,
          now
        );
        this.broadcast({ type: "sandbox_event", event });
        break;

      case "artifact":
        this.sql.exec(
          `INSERT INTO artifacts (id, type, url, metadata, created_at) VALUES (?, ?, ?, ?, ?)`,
          generateId(),
          event.artifactType,
          event.url,
          event.metadata ? JSON.stringify(event.metadata) : null,
          now
        );
        this.broadcast({
          type: "artifact_created",
          artifact: {
            id: generateId(),
            type: event.artifactType,
            url: event.url,
          },
        });
        break;

      case "push_complete":
      case "push_error":
        this.broadcast({ type: "sandbox_event", event });
        break;

      case "provider_unhealthy":
        this.sql.exec(
          `INSERT INTO events (id, type, data, message_id, created_at) VALUES (?, ?, ?, ?, ?)`,
          generateId(),
          "provider_unhealthy",
          JSON.stringify(event),
          null,
          now
        );
        this.broadcast({ type: "sandbox_event", event });

        // Fire failure webhook so Druppie can retry with a different model
        {
          const processingMsg = this.sql
            .exec("SELECT id FROM messages WHERE status = 'processing' LIMIT 1")
            .toArray()[0] as any;
          if (processingMsg) {
            this.sendWebhookCallback(processingMsg.id, false).catch((err) => {
              this.log.error("Provider unhealthy webhook failed", { error: String(err) });
            });
          }
        }
        break;

      default:
        // Forward unknown events to clients
        this.broadcast({ type: "sandbox_event", event });
    }
  }

  // ── Sandbox lifecycle ─────────────────────────────────────────────────

  private async spawnSandbox(): Promise<void> {
    if (this.isSpawningSandbox) return;

    const sandbox = this.getSandbox();
    if (sandbox?.status === "spawning" || sandbox?.status === "connecting") return;

    // Check for snapshot restore
    if (
      sandbox?.snapshot_image_id &&
      (sandbox.status === "stopped" || sandbox.status === "stale" || sandbox.status === "failed")
    ) {
      await this.restoreFromSnapshot(sandbox.snapshot_image_id);
      return;
    }

    this.isSpawningSandbox = true;

    try {
      const session = this.getSession();
      if (!session) {
        this.log.error("Cannot spawn: no session");
        return;
      }

      const now = Date.now();
      const sandboxAuthToken = generateId();
      const expectedSandboxId = `sandbox-${session.repo_owner}-${session.repo_name}-${now}`;

      this.sql.exec(
        `UPDATE sandbox SET status = 'spawning', created_at = ?, auth_token = ?, modal_sandbox_id = ?
         WHERE id = (SELECT id FROM sandbox LIMIT 1)`,
        now,
        sandboxAuthToken,
        expectedSandboxId
      );
      this.broadcast({ type: "sandbox_status", status: "spawning" });

      const sessionId = session.session_name || session.id;
      const controlPlaneUrl = this.config.WORKER_URL || "http://localhost:8787";

      // Build proxy URLs if credential store provided proxy keys
      let gitUrl = session.git_url || undefined;
      const userEnvVars: Record<string, string> = {};

      if (this.gitProxyKey) {
        gitUrl = `${controlPlaneUrl}/git-proxy/${this.gitProxyKey}/${session.repo_owner}/${session.repo_name}.git`;
      }
      if (this.llmProxyKey) {
        userEnvVars["LLM_PROXY_URL"] = `${controlPlaneUrl}/llm-proxy/${this.llmProxyKey}`;
      }
      if (this.githubApiProxyKey) {
        userEnvVars["GITHUB_API_PROXY_URL"] = `${controlPlaneUrl}/github-api-proxy/${this.githubApiProxyKey}`;
      }
      if (this.availableLlmProviders.length > 0) {
        userEnvVars["AVAILABLE_LLM_PROVIDERS"] = JSON.stringify(this.availableLlmProviders);
      }
      if (this.agentModels) {
        userEnvVars["SANDBOX_AGENT_MODELS"] = JSON.stringify(this.agentModels);
      }
      if (this.opencodeFiles) {
        userEnvVars["SANDBOX_OPENCODE_FILES"] = JSON.stringify(this.opencodeFiles);
      }
      if (this.modelChains) {
        userEnvVars["SANDBOX_MODEL_CHAINS"] = JSON.stringify(this.modelChains);
      }

      // Extract provider from model string (e.g. "zai-coding-plan/glm-4.7" -> "zai-coding-plan")
      const fullModel = session.model || "zai-coding-plan/glm-4.7";
      const modelProvider = fullModel.includes("/") ? fullModel.split("/")[0] : "anthropic";

      const result = await this.sandboxClient.createSandbox({
        sessionId,
        sandboxId: expectedSandboxId,
        repoOwner: session.repo_owner,
        repoName: session.repo_name,
        controlPlaneUrl,
        sandboxAuthToken,
        provider: modelProvider,
        model: fullModel,
        gitUrl,
        userEnvVars: Object.keys(userEnvVars).length > 0 ? userEnvVars : undefined,
      });

      if (result.modalObjectId) {
        this.sql.exec(
          "UPDATE sandbox SET modal_object_id = ? WHERE id = (SELECT id FROM sandbox LIMIT 1)",
          result.modalObjectId
        );
      }

      this.updateSandboxStatus("connecting");
      this.broadcast({ type: "sandbox_status", status: "connecting" });

      // Reset circuit breaker on success
      this.sql.exec(
        "UPDATE sandbox SET spawn_failure_count = 0 WHERE id = (SELECT id FROM sandbox LIMIT 1)"
      );
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      this.log.error("Sandbox spawn failed", { error: msg });
      this.sql.exec(
        "UPDATE sandbox SET last_spawn_error = ?, last_spawn_error_at = ? WHERE id = (SELECT id FROM sandbox LIMIT 1)",
        msg,
        Date.now()
      );
      this.updateSandboxStatus("failed");
      this.broadcast({ type: "sandbox_error", error: msg });
    } finally {
      this.isSpawningSandbox = false;
    }
  }

  private async restoreFromSnapshot(snapshotImageId: string): Promise<void> {
    this.isSpawningSandbox = true;

    try {
      const session = this.getSession();
      if (!session) return;

      const now = Date.now();
      const sandboxAuthToken = generateId();
      const expectedSandboxId = `sandbox-${session.repo_owner}-${session.repo_name}-${now}`;

      this.sql.exec(
        `UPDATE sandbox SET status = 'spawning', created_at = ?, auth_token = ?, modal_sandbox_id = ?
         WHERE id = (SELECT id FROM sandbox LIMIT 1)`,
        now,
        sandboxAuthToken,
        expectedSandboxId
      );
      this.broadcast({ type: "sandbox_status", status: "spawning" });

      const sessionId = session.session_name || session.id;
      const controlPlaneUrl = this.config.WORKER_URL || "http://localhost:8787";

      // Build user env vars for proxy URLs
      const userEnvVars: Record<string, string> = {};
      if (this.llmProxyKey) {
        userEnvVars["LLM_PROXY_URL"] = `${controlPlaneUrl}/llm-proxy/${this.llmProxyKey}`;
      }
      if (this.githubApiProxyKey) {
        userEnvVars["GITHUB_API_PROXY_URL"] = `${controlPlaneUrl}/github-api-proxy/${this.githubApiProxyKey}`;
      }
      if (this.availableLlmProviders.length > 0) {
        userEnvVars["AVAILABLE_LLM_PROVIDERS"] = JSON.stringify(this.availableLlmProviders);
      }
      if (this.agentModels) {
        userEnvVars["SANDBOX_AGENT_MODELS"] = JSON.stringify(this.agentModels);
      }
      if (this.opencodeFiles) {
        userEnvVars["SANDBOX_OPENCODE_FILES"] = JSON.stringify(this.opencodeFiles);
      }
      if (this.modelChains) {
        userEnvVars["SANDBOX_MODEL_CHAINS"] = JSON.stringify(this.modelChains);
      }

      // Extract provider from model string (e.g. "zai-coding-plan/glm-4.7" -> "zai-coding-plan")
      const restoreModel = session.model || "zai-coding-plan/glm-4.7";
      const restoreProvider = restoreModel.includes("/") ? restoreModel.split("/")[0] : "anthropic";

      // Call restore endpoint
      const response = await fetch(`${this.config.SANDBOX_MANAGER_URL}/api/restore-sandbox`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          snapshot_image_id: snapshotImageId,
          session_config: {
            session_id: sessionId,
            repo_owner: session.repo_owner,
            repo_name: session.repo_name,
            provider: restoreProvider,
            model: restoreModel,
          },
          sandbox_id: expectedSandboxId,
          control_plane_url: controlPlaneUrl,
          sandbox_auth_token: sandboxAuthToken,
          user_env_vars: Object.keys(userEnvVars).length > 0 ? userEnvVars : undefined,
        }),
      });

      const result = (await response.json()) as any;
      if (result.success) {
        if (result.data?.modal_object_id) {
          this.sql.exec(
            "UPDATE sandbox SET modal_object_id = ? WHERE id = (SELECT id FROM sandbox LIMIT 1)",
            result.data.modal_object_id
          );
        }
        this.updateSandboxStatus("connecting");
        this.broadcast({ type: "sandbox_status", status: "connecting" });
        this.broadcast({
          type: "sandbox_restored",
          message: "Session restored from snapshot",
        });
      } else {
        this.updateSandboxStatus("failed");
        this.broadcast({
          type: "sandbox_error",
          error: result.error || "Restore failed",
        });
      }
    } catch (error) {
      const msg = error instanceof Error ? error.message : String(error);
      this.log.error("Restore failed", { error: msg });
      this.updateSandboxStatus("failed");
      this.broadcast({ type: "sandbox_error", error: msg });
    } finally {
      this.isSpawningSandbox = false;
    }
  }

  private async warmSandbox(): Promise<void> {
    const sandbox = this.getSandbox();
    if (this.wsManager.getSandboxSocket()) return;
    if (this.isSpawningSandbox) return;
    if (sandbox?.status === "spawning" || sandbox?.status === "connecting") return;

    this.broadcast({ type: "sandbox_warming" });
    await this.spawnSandbox();
  }

  private async stopExecution(): Promise<void> {
    const processingMsg = this.sql
      .exec("SELECT id FROM messages WHERE status = 'processing' LIMIT 1")
      .toArray()[0] as any;

    if (processingMsg) {
      this.sql.exec(
        "UPDATE messages SET status = 'failed', completed_at = ? WHERE id = ?",
        Date.now(),
        processingMsg.id
      );

      // Broadcast synthetic execution_complete
      const event = {
        type: "execution_complete",
        messageId: processingMsg.id,
        success: false,
        error: "Stopped by user",
        sandboxId: this.getSandbox()?.modal_sandbox_id || "",
        timestamp: Date.now(),
      };
      this.broadcast({ type: "sandbox_event", event });
      this.broadcast({ type: "processing_status", isProcessing: false });

      // Send webhook callback for stopped execution
      this.sendWebhookCallback(processingMsg.id, false).catch((err) => {
        this.log.error("Webhook callback failed on stop", { error: String(err) });
      });
    }

    // Forward stop to sandbox
    const sandboxWs = this.wsManager.getSandboxSocket();
    if (sandboxWs) {
      this.wsManager.send(sandboxWs, { type: "stop" });
    }
  }

  // ── Broadcasting ──────────────────────────────────────────────────────

  private broadcast(message: object): void {
    this.wsManager.forEachClientSocket("authenticated_only", (ws) => {
      this.wsManager.send(ws, message);
    });
  }

  // ── HTTP Internal API (called by router) ──────────────────────────────

  async handleInit(body: any): Promise<any> {
    this.ensureInitialized();
    const now = Date.now();

    const sessionId = this.sessionId;
    const sessionName = body.sessionName;

    // Upsert session
    this.sql.exec(
      `INSERT OR REPLACE INTO session (id, session_name, title, repo_owner, repo_name, repo_id, model, reasoning_effort, git_url, status, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      sessionId,
      sessionName,
      body.title ?? null,
      body.repoOwner,
      body.repoName,
      body.repoId ?? null,
      body.model || "zai-coding-plan/glm-4.7",
      body.reasoningEffort ?? null,
      body.gitUrl ?? null,
      "created",
      now,
      now
    );

    // Create sandbox record
    const sandboxId = generateId();
    this.sql.exec(
      `INSERT INTO sandbox (id, status, git_sync_status, created_at)
       VALUES (?, ?, ?, ?)`,
      sandboxId,
      "pending",
      "pending",
      0
    );

    // Create owner participant
    const participantId = generateId();
    this.sql.exec(
      `INSERT INTO participants (id, user_id, github_login, github_name, github_email, github_access_token_encrypted, role, joined_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      participantId,
      body.userId,
      body.githubLogin ?? null,
      body.githubName ?? null,
      body.githubEmail ?? null,
      body.githubTokenEncrypted ?? null,
      "owner",
      now
    );

    // Store proxy keys if provided (from credential store)
    if (body.proxyKeys) {
      this.gitProxyKey = body.proxyKeys.gitProxyKey ?? null;
      this.llmProxyKey = body.proxyKeys.llmProxyKey ?? null;
      this.githubApiProxyKey = body.proxyKeys.githubApiProxyKey ?? null;
    }

    // Store available LLM providers for multi-provider sandbox config
    if (body.availableLlmProviders) {
      this.availableLlmProviders = body.availableLlmProviders;
    }

    // Store per-agent model overrides and agent definition files
    this.agentModels = body.agentModels ?? null;
    this.opencodeFiles = body.opencodeFiles ?? null;
    this.modelChains = body.modelChains ?? null;

    // Trigger warm sandbox
    this.warmSandbox().catch(console.error);

    return { sessionId, status: "created" };
  }

  async handleGetState(): Promise<any> {
    const session = this.getSession();
    if (!session) return null;
    const sandbox = this.getSandbox();

    return {
      id: session.id,
      title: session.title,
      repoOwner: session.repo_owner,
      repoName: session.repo_name,
      repoDefaultBranch: session.repo_default_branch,
      branchName: session.branch_name,
      baseSha: session.base_sha,
      currentSha: session.current_sha,
      opencodeSessionId: session.opencode_session_id,
      status: session.status,
      model: session.model,
      reasoningEffort: session.reasoning_effort ?? undefined,
      createdAt: session.created_at,
      updatedAt: session.updated_at,
      sandbox: sandbox
        ? {
            id: sandbox.id,
            modalSandboxId: sandbox.modal_sandbox_id,
            status: sandbox.status,
            gitSyncStatus: sandbox.git_sync_status,
            lastHeartbeat: sandbox.last_heartbeat,
          }
        : null,
    };
  }

  async handleGenerateWsToken(body: any): Promise<any> {
    if (!body.userId) return { error: "userId is required" };

    const now = Date.now();

    // Find or create participant
    let participant = (
      this.sql.exec("SELECT * FROM participants WHERE user_id = ?", body.userId).toArray() as any[]
    )[0];

    if (!participant) {
      const id = generateId();
      this.sql.exec(
        `INSERT INTO participants (id, user_id, github_login, github_name, github_email, role, joined_at)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        id,
        body.userId,
        body.githubLogin ?? null,
        body.githubName ?? null,
        body.githubEmail ?? null,
        "member",
        now
      );
      participant = (
        this.sql
          .exec("SELECT * FROM participants WHERE user_id = ?", body.userId)
          .toArray() as any[]
      )[0];
    } else {
      // Update participant info
      this.sql.exec(
        `UPDATE participants SET
           github_login = COALESCE(?, github_login),
           github_name = COALESCE(?, github_name),
           github_email = COALESCE(?, github_email)
         WHERE id = ?`,
        body.githubLogin ?? null,
        body.githubName ?? null,
        body.githubEmail ?? null,
        participant.id
      );
    }

    // Generate WS token
    const plainToken = generateId(32);
    const tokenHash = await hashToken(plainToken);

    this.sql.exec(
      "UPDATE participants SET ws_auth_token = ?, ws_token_created_at = ? WHERE id = ?",
      tokenHash,
      now,
      participant.id
    );

    return { token: plainToken, participantId: participant.id };
  }

  async handleEnqueuePrompt(body: any): Promise<any> {
    const now = Date.now();
    const messageId = generateId();

    // Find or create participant
    let participant = (
      this.sql
        .exec("SELECT * FROM participants WHERE user_id = ?", body.authorId)
        .toArray() as any[]
    )[0];

    if (!participant) {
      const id = generateId();
      this.sql.exec(
        `INSERT INTO participants (id, user_id, role, joined_at)
         VALUES (?, ?, ?, ?)`,
        id,
        body.authorId,
        "member",
        now
      );
      participant = { id, user_id: body.authorId };
    }

    this.sql.exec(
      `INSERT INTO messages (id, author_id, content, source, model, reasoning_effort, callback_context, status, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      messageId,
      participant.id,
      body.content,
      body.source || "web",
      body.model ?? null,
      body.reasoningEffort ?? null,
      body.callbackContext ? JSON.stringify(body.callbackContext) : null,
      "pending",
      now
    );

    // Update session to active
    const session = this.getSession();
    if (session && session.status === "created") {
      this.sql.exec(
        "UPDATE session SET status = 'active', updated_at = ? WHERE id = ?",
        now,
        session.id
      );
    }

    this.processMessageQueue().catch(console.error);

    return { messageId, status: "queued", position: 1 };
  }

  handleListEvents(query: any): any {
    const limit = Math.min(parseInt(query.limit ?? "50"), 2000);
    const cursor = query.cursor;
    const type = query.type;
    const messageId = query.message_id;

    let sql = "SELECT * FROM events WHERE 1=1";
    const params: any[] = [];

    if (type) {
      sql += " AND type = ?";
      params.push(type);
    }
    if (messageId) {
      sql += " AND message_id = ?";
      params.push(messageId);
    }
    if (cursor) {
      sql += " AND created_at < ?";
      params.push(parseInt(cursor));
    }

    sql += " ORDER BY created_at DESC LIMIT ?";
    params.push(limit + 1);

    const events = this.sql.exec(sql, ...params).toArray() as any[];
    const hasMore = events.length > limit;
    if (hasMore) events.pop();

    return {
      events: events.map((e: any) => ({
        id: e.id,
        type: e.type,
        data: JSON.parse(e.data),
        messageId: e.message_id,
        createdAt: e.created_at,
      })),
      cursor: events.length > 0 ? events[events.length - 1].created_at.toString() : undefined,
      hasMore,
    };
  }

  handleListMessages(query: any): any {
    const limit = Math.min(parseInt(query.limit ?? "50"), 100);
    const cursor = query.cursor;
    const status = query.status;

    let sql = "SELECT * FROM messages WHERE 1=1";
    const params: any[] = [];

    if (status) {
      sql += " AND status = ?";
      params.push(status);
    }
    if (cursor) {
      sql += " AND created_at < ?";
      params.push(parseInt(cursor));
    }

    sql += " ORDER BY created_at DESC LIMIT ?";
    params.push(limit + 1);

    const messages = this.sql.exec(sql, ...params).toArray() as any[];
    const hasMore = messages.length > limit;
    if (hasMore) messages.pop();

    return {
      messages: messages.map((m: any) => ({
        id: m.id,
        authorId: m.author_id,
        content: m.content,
        source: m.source,
        status: m.status,
        createdAt: m.created_at,
        startedAt: m.started_at,
        completedAt: m.completed_at,
      })),
      cursor: messages.length > 0 ? messages[messages.length - 1].created_at.toString() : undefined,
      hasMore,
    };
  }

  handleListArtifacts(): any {
    const artifacts = this.sql
      .exec("SELECT * FROM artifacts ORDER BY created_at DESC")
      .toArray() as any[];

    return {
      artifacts: artifacts.map((a: any) => ({
        id: a.id,
        type: a.type,
        url: a.url,
        metadata: a.metadata ? JSON.parse(a.metadata) : null,
        createdAt: a.created_at,
      })),
    };
  }

  handleListParticipants(): any {
    const participants = this.sql
      .exec("SELECT * FROM participants ORDER BY joined_at")
      .toArray() as any[];

    return {
      participants: participants.map((p: any) => ({
        id: p.id,
        userId: p.user_id,
        githubLogin: p.github_login,
        githubName: p.github_name,
        role: p.role,
        joinedAt: p.joined_at,
      })),
    };
  }

  async handleArchive(_body: any): Promise<any> {
    const session = this.getSession();
    if (!session) return { error: "Session not found" };

    this.sql.exec(
      "UPDATE session SET status = 'archived', updated_at = ? WHERE id = ?",
      Date.now(),
      session.id
    );
    this.broadcast({ type: "session_status", status: "archived" });
    this.sessionIndex.updateStatus(session.session_name || session.id, "archived");

    return { status: "archived" };
  }

  async handleUnarchive(_body: any): Promise<any> {
    const session = this.getSession();
    if (!session) return { error: "Session not found" };

    this.sql.exec(
      "UPDATE session SET status = 'active', updated_at = ? WHERE id = ?",
      Date.now(),
      session.id
    );
    this.broadcast({ type: "session_status", status: "active" });
    this.sessionIndex.updateStatus(session.session_name || session.id, "active");

    return { status: "active" };
  }

  async handleVerifySandboxToken(body: any): Promise<any> {
    const sandbox = this.getSandbox();
    if (!sandbox) return { valid: false, error: "No sandbox" };
    if (sandbox.status === "stopped" || sandbox.status === "stale") {
      return { valid: false, error: "Sandbox stopped" };
    }
    if (body.token !== sandbox.auth_token) {
      return { valid: false, error: "Invalid token" };
    }
    return { valid: true };
  }

  /**
   * Public API: destroy the sandbox container (called from router DELETE route).
   */
  async handleDestroyContainer(): Promise<{ success: boolean }> {
    await this.destroySandboxContainer();
    return { success: true };
  }

  // ── Webhook Callback ──────────────────────────────────────────────────

  /**
   * Send a webhook callback to a URL stored in the message's callback_context.
   * Used by Druppie to resume paused agents after sandbox completion.
   */
  private async sendWebhookCallback(messageId: string, success: boolean): Promise<void> {
    // Look up the message's callback context
    const row = this.sql
      .exec("SELECT callback_context FROM messages WHERE id = ?", messageId)
      .toArray()[0] as any;

    if (!row?.callback_context) return;

    let context: any;
    try {
      context = JSON.parse(row.callback_context);
    } catch {
      return;
    }

    const callbackUrl = context?.callbackUrl;
    if (!callbackUrl) return;

    const callbackSecret = context?.callbackSecret || "";
    const session = this.getSession();
    const sessionId = session?.id || this.sessionId;

    const payloadData = {
      sessionId,
      messageId,
      success,
      timestamp: Date.now(),
    };

    // Sign the payload with HMAC-SHA256 if secret is provided
    let signature = "";
    if (callbackSecret) {
      signature = crypto
        .createHmac("sha256", callbackSecret)
        .update(JSON.stringify(payloadData))
        .digest("hex");
    }

    // Retry up to 2 times
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        const response = await fetch(callbackUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "X-Signature": signature,
          },
          body: JSON.stringify(payloadData),
        });

        if (response.ok) {
          this.log.info("Webhook callback succeeded", {
            messageId,
            callbackUrl,
            success,
          });
          return;
        }

        const responseText = await response.text();
        this.log.error("Webhook callback failed", {
          messageId,
          callbackUrl,
          status: response.status,
          response: responseText,
        });
      } catch (err) {
        this.log.error("Webhook callback attempt failed", {
          messageId,
          callbackUrl,
          attempt: attempt + 1,
          error: err instanceof Error ? err.message : String(err),
        });
      }

      // Wait 1s before retry
      if (attempt < 1) {
        await new Promise((r) => setTimeout(r, 1000));
      }
    }

    this.log.error("Webhook callback failed after retries", {
      messageId,
      callbackUrl,
    });
  }
}
