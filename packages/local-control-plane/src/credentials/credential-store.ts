/**
 * In-memory credential store for sandbox sessions.
 *
 * Druppie sends credentials per-session at POST /sessions time.
 * The store generates random proxy keys, maps them to credentials,
 * and provides lookup for the git and LLM proxy endpoints.
 *
 * Credentials are wiped on session delete or process restart.
 */

import crypto from "crypto";

// ── Types ────────────────────────────────────────────────────────────────

export interface GitCredentials {
  provider: string; // "gitea" or "github"
  url: string; // e.g. "http://gitea:3000"
  username: string;
  password: string;
  authorizedRepo?: string; // "owner/repo" — enforced by git proxy
}

export interface LlmCredentials {
  provider: string; // "zai", "anthropic", "openai", "deepseek", etc.
  apiKey: string;
  baseUrl: string; // e.g. "https://open.bigmodel.cn/api/paas/v4"
}

export interface GithubApiCredentials {
  token: string; // GitHub App installation token
  authorizedRepo?: string; // "owner/repo" — for logging/scoping
}

export interface SessionCredentials {
  git?: GitCredentials;
  llm?: LlmCredentials | LlmCredentials[];
  githubApi?: GithubApiCredentials;
}

export interface ProxyKeys {
  gitProxyKey: string | null;
  llmProxyKey: string | null;
  githubApiProxyKey: string | null;
}

/** Model chain entry for proxy failover. */
export interface ModelChainEntry {
  provider: string;
  model: string;
}

interface StoredSession {
  sessionId: string;
  gitCredentials: GitCredentials | null;
  /** provider name -> LlmCredentials (multi-provider support) */
  llmCredentials: Map<string, LlmCredentials>;
  githubApiCredentials: GithubApiCredentials | null;
  gitProxyKey: string | null;
  llmProxyKey: string | null;
  githubApiProxyKey: string | null;
  /** Model chains for proxy failover — keyed by model string */
  modelChains: Record<string, ModelChainEntry[]> | null;
}

// ── Credential Store ─────────────────────────────────────────────────────

export class CredentialStore {
  /** sessionId -> stored credentials + proxy keys */
  private sessions = new Map<string, StoredSession>();
  /** gitProxyKey -> sessionId (reverse index) */
  private gitKeyIndex = new Map<string, string>();
  /** llmProxyKey -> sessionId (reverse index) */
  private llmKeyIndex = new Map<string, string>();
  /** githubApiProxyKey -> sessionId (reverse index) */
  private githubApiKeyIndex = new Map<string, string>();

  /**
   * Store credentials for a session and generate proxy keys.
   * Accepts llm as a single LlmCredentials or an array (multi-provider).
   * Returns the generated proxy keys.
   */
  store(
    sessionId: string,
    credentials: SessionCredentials,
    modelChains?: Record<string, ModelChainEntry[]> | null
  ): ProxyKeys {
    // Clean up any existing entry for this session
    this.destroy(sessionId);

    const gitProxyKey = credentials.git ? crypto.randomBytes(32).toString("hex") : null;

    // Normalize llm into a Map<provider, LlmCredentials>
    const llmMap = new Map<string, LlmCredentials>();
    if (credentials.llm) {
      const llmArray = Array.isArray(credentials.llm) ? credentials.llm : [credentials.llm];
      for (const cred of llmArray) {
        if (cred.provider && cred.apiKey) {
          llmMap.set(cred.provider, cred);
        }
      }
    }

    const llmProxyKey = llmMap.size > 0 ? crypto.randomBytes(32).toString("hex") : null;
    const githubApiProxyKey = credentials.githubApi?.token
      ? crypto.randomBytes(32).toString("hex")
      : null;

    const stored: StoredSession = {
      sessionId,
      gitCredentials: credentials.git ?? null,
      llmCredentials: llmMap,
      githubApiCredentials: credentials.githubApi ?? null,
      gitProxyKey,
      llmProxyKey,
      githubApiProxyKey,
      modelChains: modelChains ?? null,
    };

    this.sessions.set(sessionId, stored);

    if (gitProxyKey) {
      this.gitKeyIndex.set(gitProxyKey, sessionId);
    }
    if (llmProxyKey) {
      this.llmKeyIndex.set(llmProxyKey, sessionId);
    }
    if (githubApiProxyKey) {
      this.githubApiKeyIndex.set(githubApiProxyKey, sessionId);
    }

    return { gitProxyKey, llmProxyKey, githubApiProxyKey };
  }

  /** Look up git credentials by proxy key. Returns null if key is invalid. */
  getByGitProxyKey(key: string): (GitCredentials & { sessionId: string }) | null {
    const sessionId = this.gitKeyIndex.get(key);
    if (!sessionId) return null;

    const stored = this.sessions.get(sessionId);
    if (!stored?.gitCredentials) return null;

    return { ...stored.gitCredentials, sessionId };
  }

  /** Look up GitHub API credentials by proxy key. Returns null if key is invalid. */
  getByGithubApiProxyKey(key: string): (GithubApiCredentials & { sessionId: string }) | null {
    const sessionId = this.githubApiKeyIndex.get(key);
    if (!sessionId) return null;

    const stored = this.sessions.get(sessionId);
    if (!stored?.githubApiCredentials) return null;

    return { ...stored.githubApiCredentials, sessionId };
  }

  /**
   * Look up LLM credentials by proxy key and provider.
   * Returns null if key is invalid or provider not found.
   */
  getByLlmProxyKey(key: string, provider: string): (LlmCredentials & { sessionId: string }) | null {
    const sessionId = this.llmKeyIndex.get(key);
    if (!sessionId) return null;

    const stored = this.sessions.get(sessionId);
    if (!stored) return null;

    const creds = stored.llmCredentials.get(provider);
    if (!creds) return null;

    return { ...creds, sessionId };
  }

  /** Get the list of available provider names for a session. */
  getAvailableProviders(sessionId: string): string[] {
    const stored = this.sessions.get(sessionId);
    if (!stored) return [];
    return Array.from(stored.llmCredentials.keys());
  }

  /** Get proxy keys for a session. Returns null if session not found. */
  getProxyKeys(sessionId: string): ProxyKeys | null {
    const stored = this.sessions.get(sessionId);
    if (!stored) return null;

    return {
      gitProxyKey: stored.gitProxyKey,
      llmProxyKey: stored.llmProxyKey,
      githubApiProxyKey: stored.githubApiProxyKey,
    };
  }

  /** Wipe all credentials for a session. Idempotent. */
  destroy(sessionId: string): void {
    const stored = this.sessions.get(sessionId);
    if (!stored) return;

    if (stored.gitProxyKey) {
      this.gitKeyIndex.delete(stored.gitProxyKey);
    }
    if (stored.llmProxyKey) {
      this.llmKeyIndex.delete(stored.llmProxyKey);
    }
    if (stored.githubApiProxyKey) {
      this.githubApiKeyIndex.delete(stored.githubApiProxyKey);
    }

    this.sessions.delete(sessionId);
  }

  /** Get model chains for proxy failover by LLM proxy key. */
  getModelChains(proxyKey: string): Record<string, ModelChainEntry[]> | null {
    const sessionId = this.llmKeyIndex.get(proxyKey);
    if (!sessionId) return null;
    const stored = this.sessions.get(sessionId);
    return stored?.modelChains ?? null;
  }

  /** Resolve sessionId from an LLM proxy key. */
  getSessionIdByLlmProxyKey(proxyKey: string): string | null {
    return this.llmKeyIndex.get(proxyKey) ?? null;
  }

  /** Get all available providers with credentials for a session (by proxy key). */
  getAvailableProvidersByProxyKey(proxyKey: string): string[] {
    const sessionId = this.llmKeyIndex.get(proxyKey);
    if (!sessionId) return [];
    const stored = this.sessions.get(sessionId);
    if (!stored) return [];
    return Array.from(stored.llmCredentials.keys());
  }

  /** Number of active sessions with credentials. */
  get size(): number {
    return this.sessions.size;
  }
}
