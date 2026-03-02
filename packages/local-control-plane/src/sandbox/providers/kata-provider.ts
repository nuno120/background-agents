/**
 * Kata sandbox provider — implements SandboxProvider using the local sandbox manager.
 * Replaces ModalSandboxProvider with calls to the local FastAPI sandbox manager.
 */

import { generateInternalToken } from "@open-inspect/shared";
import type { LocalSandboxClient } from "../local-client.js";

// Re-declare the interfaces locally to avoid importing from the CF-typed package.
// These match packages/control-plane/src/sandbox/provider.ts exactly.

export const DEFAULT_SANDBOX_TIMEOUT_SECONDS = 7200;

export interface SandboxProviderCapabilities {
  supportsSnapshots: boolean;
  supportsRestore: boolean;
  supportsWarm: boolean;
}

export interface CreateSandboxConfig {
  sessionId: string;
  sandboxId: string;
  repoOwner: string;
  repoName: string;
  controlPlaneUrl: string;
  sandboxAuthToken: string;
  provider: string;
  model: string;
  userEnvVars?: Record<string, string>;
  opencodeSessionId?: string;
  gitUserName?: string;
  gitUserEmail?: string;
  traceId?: string;
  requestId?: string;
}

export interface CreateSandboxResult {
  sandboxId: string;
  providerObjectId?: string;
  status: string;
  createdAt: number;
}

export interface RestoreConfig {
  snapshotImageId: string;
  sessionId: string;
  sandboxId: string;
  sandboxAuthToken: string;
  controlPlaneUrl: string;
  repoOwner: string;
  repoName: string;
  provider: string;
  model: string;
  userEnvVars?: Record<string, string>;
  timeoutSeconds?: number;
  traceId?: string;
  requestId?: string;
}

export interface RestoreResult {
  success: boolean;
  sandboxId?: string;
  providerObjectId?: string;
  error?: string;
}

export interface SnapshotConfig {
  providerObjectId: string;
  sessionId: string;
  reason: string;
  traceId?: string;
  requestId?: string;
}

export interface SnapshotResult {
  success: boolean;
  imageId?: string;
  error?: string;
}

export type SandboxErrorType = "transient" | "permanent";

export class SandboxProviderError extends Error {
  constructor(
    message: string,
    public readonly errorType: SandboxErrorType,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = "SandboxProviderError";
  }
}

export interface DestroyConfig {
  sandboxId: string;
  traceId?: string;
  requestId?: string;
}

export interface DestroyResult {
  success: boolean;
  error?: string;
}

export interface SandboxProvider {
  readonly name: string;
  readonly capabilities: SandboxProviderCapabilities;
  createSandbox(config: CreateSandboxConfig): Promise<CreateSandboxResult>;
  restoreFromSnapshot?(config: RestoreConfig): Promise<RestoreResult>;
  takeSnapshot?(config: SnapshotConfig): Promise<SnapshotResult>;
  destroySandbox?(config: DestroyConfig): Promise<DestroyResult>;
}

/**
 * KataSandboxProvider — calls the local sandbox manager HTTP API.
 */
export class KataSandboxProvider implements SandboxProvider {
  readonly name = "kata";

  readonly capabilities: SandboxProviderCapabilities = {
    supportsSnapshots: true,
    supportsRestore: true,
    supportsWarm: true,
  };

  constructor(
    private readonly client: LocalSandboxClient,
    private readonly secret: string
  ) {}

  async createSandbox(config: CreateSandboxConfig): Promise<CreateSandboxResult> {
    try {
      const result = await this.client.createSandbox({
        sessionId: config.sessionId,
        sandboxId: config.sandboxId,
        repoOwner: config.repoOwner,
        repoName: config.repoName,
        controlPlaneUrl: config.controlPlaneUrl,
        sandboxAuthToken: config.sandboxAuthToken,
        opencodeSessionId: config.opencodeSessionId,
        gitUserName: config.gitUserName,
        gitUserEmail: config.gitUserEmail,
        provider: config.provider,
        model: config.model,
        userEnvVars: config.userEnvVars,
      });

      return {
        sandboxId: result.sandboxId,
        providerObjectId: result.modalObjectId,
        status: result.status,
        createdAt: result.createdAt,
      };
    } catch (error) {
      throw this.classifyError("Failed to create sandbox", error);
    }
  }

  async restoreFromSnapshot(config: RestoreConfig): Promise<RestoreResult> {
    try {
      const restoreUrl = this.client.getRestoreSandboxUrl();
      const authToken = await generateInternalToken(this.secret);

      const headers: Record<string, string> = {
        "Content-Type": "application/json",
        Authorization: `Bearer ${authToken}`,
      };

      const response = await fetch(restoreUrl, {
        method: "POST",
        headers,
        body: JSON.stringify({
          snapshot_image_id: config.snapshotImageId,
          session_config: {
            session_id: config.sessionId,
            repo_owner: config.repoOwner,
            repo_name: config.repoName,
            provider: config.provider,
            model: config.model,
          },
          sandbox_id: config.sandboxId,
          control_plane_url: config.controlPlaneUrl,
          sandbox_auth_token: config.sandboxAuthToken,
          user_env_vars: config.userEnvVars || null,
          timeout_seconds: config.timeoutSeconds ?? DEFAULT_SANDBOX_TIMEOUT_SECONDS,
        }),
      });

      if (!response.ok) {
        throw new SandboxProviderError(
          `Restore failed with HTTP ${response.status}`,
          response.status >= 500 ? "transient" : "permanent"
        );
      }

      const result = (await response.json()) as {
        success: boolean;
        data?: { sandbox_id: string; modal_object_id?: string };
        error?: string;
      };

      if (result.success) {
        return {
          success: true,
          sandboxId: result.data?.sandbox_id,
          providerObjectId: result.data?.modal_object_id,
        };
      }

      return { success: false, error: result.error || "Unknown restore error" };
    } catch (error) {
      if (error instanceof SandboxProviderError) throw error;
      throw this.classifyError("Failed to restore sandbox", error);
    }
  }

  async destroySandbox(config: DestroyConfig): Promise<DestroyResult> {
    try {
      const stopUrl = this.client.getStopSandboxUrl();
      const authToken = await generateInternalToken(this.secret);

      const headers: Record<string, string> = {
        "Content-Type": "application/json",
        Authorization: `Bearer ${authToken}`,
      };

      const response = await fetch(stopUrl, {
        method: "POST",
        headers,
        body: JSON.stringify({ sandbox_id: config.sandboxId }),
      });

      if (!response.ok) {
        const text = await response.text();
        return { success: false, error: `Stop failed with HTTP ${response.status}: ${text}` };
      }

      const result = (await response.json()) as {
        success: boolean;
        error?: string;
      };

      return { success: result.success, error: result.error };
    } catch (error) {
      // Non-fatal: container may already be gone
      return {
        success: false,
        error: `Failed to destroy sandbox: ${error instanceof Error ? error.message : String(error)}`,
      };
    }
  }

  async takeSnapshot(config: SnapshotConfig): Promise<SnapshotResult> {
    try {
      const snapshotUrl = this.client.getSnapshotSandboxUrl();
      const authToken = await generateInternalToken(this.secret);

      const headers: Record<string, string> = {
        "Content-Type": "application/json",
        Authorization: `Bearer ${authToken}`,
      };

      const response = await fetch(snapshotUrl, {
        method: "POST",
        headers,
        body: JSON.stringify({
          sandbox_id: config.providerObjectId,
          session_id: config.sessionId,
          reason: config.reason,
        }),
      });

      if (!response.ok) {
        throw new SandboxProviderError(
          `Snapshot failed with HTTP ${response.status}`,
          response.status >= 500 ? "transient" : "permanent"
        );
      }

      const result = (await response.json()) as {
        success: boolean;
        data?: { image_id: string };
        error?: string;
      };

      if (result.success && result.data?.image_id) {
        return { success: true, imageId: result.data.image_id };
      }

      return { success: false, error: result.error || "Unknown snapshot error" };
    } catch (error) {
      if (error instanceof SandboxProviderError) throw error;
      throw this.classifyError("Failed to take snapshot", error);
    }
  }

  private classifyError(message: string, error: unknown): SandboxProviderError {
    if (error instanceof Error) {
      const msg = error.message.toLowerCase();
      if (
        msg.includes("fetch failed") ||
        msg.includes("econnrefused") ||
        msg.includes("econnreset") ||
        msg.includes("timeout")
      ) {
        return new SandboxProviderError(`${message}: ${error.message}`, "transient", error);
      }
    }
    return new SandboxProviderError(
      `${message}: ${error instanceof Error ? error.message : String(error)}`,
      "permanent",
      error instanceof Error ? error : undefined
    );
  }
}

export function createKataProvider(
  client: LocalSandboxClient,
  secret: string
): KataSandboxProvider {
  return new KataSandboxProvider(client, secret);
}
