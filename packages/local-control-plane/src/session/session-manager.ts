/**
 * SessionManager — in-process Map<sessionId, SessionInstance>.
 * Replaces Cloudflare Durable Object namespace routing.
 *
 * Each session gets its own SQLite database file and in-memory state.
 */

import path from "path";
import Database from "better-sqlite3";
import { config } from "../config.js";
import { SessionInstance } from "./session-instance.js";
import { SqlStorageAdapter } from "./sqlite-adapter.js";
import type { LocalSandboxClient } from "../sandbox/local-client.js";
import type { LocalSessionIndexStore } from "../db/session-index-local.js";
import type { CredentialStore } from "../credentials/credential-store.js";

import fs from "fs";

export class SessionManager {
  private sessions = new Map<string, SessionInstance>();
  private sandboxClient: LocalSandboxClient;
  private sessionIndex: LocalSessionIndexStore;
  private credentialStore: CredentialStore;

  constructor(
    sandboxClient: LocalSandboxClient,
    sessionIndex: LocalSessionIndexStore,
    credentialStore: CredentialStore
  ) {
    this.sandboxClient = sandboxClient;
    this.sessionIndex = sessionIndex;
    this.credentialStore = credentialStore;

    // Ensure data directory exists
    const sessionDir = path.join(config.DATA_DIR, "sessions");
    fs.mkdirSync(sessionDir, { recursive: true });
  }

  /**
   * Get or create a SessionInstance for the given session ID.
   */
  get(sessionId: string): SessionInstance {
    let instance = this.sessions.get(sessionId);
    if (!instance) {
      instance = this.createInstance(sessionId);
      this.sessions.set(sessionId, instance);
    }
    return instance;
  }

  /**
   * Check if a session instance exists (without creating it).
   */
  has(sessionId: string): boolean {
    return this.sessions.has(sessionId);
  }

  private createInstance(sessionId: string): SessionInstance {
    const dbPath = path.join(config.DATA_DIR, "sessions", `${sessionId}.db`);
    const db = new Database(dbPath);
    db.pragma("journal_mode = WAL");
    db.pragma("busy_timeout = 5000");

    const sql = new SqlStorageAdapter(db);

    return new SessionInstance(
      sessionId,
      sql,
      this.sandboxClient,
      this.sessionIndex,
      config,
      this.credentialStore
    );
  }
}
