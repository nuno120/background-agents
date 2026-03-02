/**
 * HTTP client for the local sandbox manager.
 * Replaces ModalClient — same interface, different URLs.
 */

import { generateInternalToken } from "@open-inspect/shared";

export interface CorrelationHeaders {
  trace_id?: string;
  request_id?: string;
  session_id?: string;
  sandbox_id?: string;
}

export interface CreateSandboxRequest {
  sessionId: string;
  sandboxId?: string;
  repoOwner: string;
  repoName: string;
  controlPlaneUrl: string;
  sandboxAuthToken: string;
  snapshotId?: string;
  opencodeSessionId?: string;
  gitUserName?: string;
  gitUserEmail?: string;
  provider?: string;
  model?: string;
  userEnvVars?: Record<string, string>;
  gitUrl?: string;
}

export interface CreateSandboxResponse {
  sandboxId: string;
  modalObjectId?: string;
  status: string;
  createdAt: number;
}

export interface WarmSandboxRequest {
  repoOwner: string;
  repoName: string;
  controlPlaneUrl?: string;
}

export interface WarmSandboxResponse {
  sandboxId: string;
  status: string;
}

interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
}

export class LocalSandboxClient {
  private baseUrl: string;
  private secret: string;

  constructor(secret: string, baseUrl: string) {
    this.secret = secret;
    this.baseUrl = baseUrl.replace(/\/$/, "");
  }

  getSnapshotSandboxUrl(): string {
    return `${this.baseUrl}/api/snapshot-sandbox`;
  }

  getRestoreSandboxUrl(): string {
    return `${this.baseUrl}/api/restore-sandbox`;
  }

  getStopSandboxUrl(): string {
    return `${this.baseUrl}/api/stop-sandbox`;
  }

  private async getPostHeaders(correlation?: CorrelationHeaders): Promise<Record<string, string>> {
    const token = await generateInternalToken(this.secret);
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    };
    if (correlation?.trace_id) headers["x-trace-id"] = correlation.trace_id;
    if (correlation?.request_id) headers["x-request-id"] = correlation.request_id;
    if (correlation?.session_id) headers["x-session-id"] = correlation.session_id;
    if (correlation?.sandbox_id) headers["x-sandbox-id"] = correlation.sandbox_id;
    return headers;
  }

  async createSandbox(
    request: CreateSandboxRequest,
    correlation?: CorrelationHeaders
  ): Promise<CreateSandboxResponse> {
    const headers = await this.getPostHeaders(correlation);
    const response = await fetch(`${this.baseUrl}/api/create-sandbox`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        session_id: request.sessionId,
        sandbox_id: request.sandboxId || null,
        repo_owner: request.repoOwner,
        repo_name: request.repoName,
        control_plane_url: request.controlPlaneUrl,
        sandbox_auth_token: request.sandboxAuthToken,
        snapshot_id: request.snapshotId || null,
        opencode_session_id: request.opencodeSessionId || null,
        git_user_name: request.gitUserName || null,
        git_user_email: request.gitUserEmail || null,
        provider: request.provider || "zai-coding-plan",
        model: request.model || "glm-4.7",
        user_env_vars: request.userEnvVars || null,
        git_url: request.gitUrl || null,
      }),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Sandbox API error: ${response.status} ${text}`);
    }

    const result = (await response.json()) as ApiResponse<{
      sandbox_id: string;
      modal_object_id?: string;
      status: string;
      created_at: number;
    }>;

    if (!result.success || !result.data) {
      throw new Error(`Sandbox API error: ${result.error || "Unknown error"}`);
    }

    return {
      sandboxId: result.data.sandbox_id,
      modalObjectId: result.data.modal_object_id,
      status: result.data.status,
      createdAt: result.data.created_at,
    };
  }

  async warmSandbox(
    request: WarmSandboxRequest,
    correlation?: CorrelationHeaders
  ): Promise<WarmSandboxResponse> {
    const headers = await this.getPostHeaders(correlation);
    const response = await fetch(`${this.baseUrl}/api/warm-sandbox`, {
      method: "POST",
      headers,
      body: JSON.stringify({
        repo_owner: request.repoOwner,
        repo_name: request.repoName,
        control_plane_url: request.controlPlaneUrl || "",
      }),
    });

    if (!response.ok) {
      const text = await response.text();
      throw new Error(`Sandbox API error: ${response.status} ${text}`);
    }

    const result = (await response.json()) as ApiResponse<{
      sandbox_id: string;
      status: string;
    }>;

    if (!result.success || !result.data) {
      throw new Error(`Sandbox API error: ${result.error || "Unknown error"}`);
    }

    return {
      sandboxId: result.data.sandbox_id,
      status: result.data.status,
    };
  }

  async health(): Promise<{ status: string; service: string }> {
    const response = await fetch(`${this.baseUrl}/api/health`);
    if (!response.ok) {
      throw new Error(`Sandbox API error: ${response.status}`);
    }
    const result = (await response.json()) as ApiResponse<{
      status: string;
      service: string;
    }>;
    if (!result.success || !result.data) {
      throw new Error(`Sandbox API error: ${result.error || "Unknown error"}`);
    }
    return result.data;
  }
}

export function createLocalSandboxClient(secret: string, baseUrl: string): LocalSandboxClient {
  return new LocalSandboxClient(secret, baseUrl);
}
