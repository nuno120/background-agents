"""
Local Sandbox Manager — FastAPI HTTP API.

Manages sandbox containers using either Docker (cross-platform) or Kata
(Linux, VM-level isolation).  The runtime is selected via SANDBOX_RUNTIME
env var ("docker" or "kata").

Maintains the same JSON request/response shapes so the control plane's
LocalSandboxClient / KataProvider can call these endpoints unchanged.
"""

import json
import logging
import time

from fastapi import FastAPI, Header, HTTPException, Request

from . import config
from .auth import AuthConfigurationError, verify_internal_token
from .github_token import generate_installation_token
from .snapshot_store import SnapshotStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
log = logging.getLogger("sandbox-api")

app = FastAPI(title="Open-Inspect Local Sandbox Manager")
snapshot_store = SnapshotStore()

# ── Runtime selection ─────────────────────────────────────────────────────

if config.SANDBOX_RUNTIME == "kata":
    from .container_manager import ContainerError, KataContainerManager
    manager = KataContainerManager()
    log.info("Using Kata container runtime (containerd + %s)", config.KATA_RUNTIME)
else:
    from .docker_manager import ContainerError, DockerContainerManager  # type: ignore[assignment]
    manager = DockerContainerManager()  # type: ignore[assignment]
    log.info("Using Docker container runtime")


# ---------------------------------------------------------------------------
# Auth helper
# ---------------------------------------------------------------------------

def require_auth(authorization: str | None) -> None:
    try:
        if not verify_internal_token(authorization):
            raise HTTPException(status_code=401, detail="Unauthorized")
    except AuthConfigurationError as e:
        raise HTTPException(status_code=503, detail=str(e))


# ---------------------------------------------------------------------------
# POST /api/create-sandbox
# ---------------------------------------------------------------------------

@app.post("/api/create-sandbox")
async def create_sandbox(
    request: Request,
    authorization: str | None = Header(None),
):
    require_auth(authorization)
    body = await request.json()

    start_time = time.time()

    sandbox_id = body.get("sandbox_id") or f"sandbox-{body.get('repo_owner')}-{body.get('repo_name')}-{int(time.time() * 1000)}"

    # Generate GitHub App token
    github_app_token = None
    try:
        github_app_token = generate_installation_token()
    except Exception as e:
        log.warning("GitHub token generation failed: %s", e)

    # Build env vars (user vars first, system vars override — same as manager.py)
    env_vars: dict[str, str] = {}

    user_env_vars = body.get("user_env_vars")
    if user_env_vars:
        env_vars.update(user_env_vars)

    env_vars.update({
        "PYTHONUNBUFFERED": "1",
        "SANDBOX_ID": sandbox_id,
        "CONTROL_PLANE_URL": body.get("control_plane_url", ""),
        "SANDBOX_AUTH_TOKEN": body.get("sandbox_auth_token", ""),
        "REPO_OWNER": body.get("repo_owner", ""),
        "REPO_NAME": body.get("repo_name", ""),
    })

    if github_app_token:
        env_vars["GITHUB_APP_TOKEN"] = github_app_token
        env_vars["GITHUB_TOKEN"] = github_app_token

    # Pass through GIT_URL for non-GitHub git hosts (e.g. Gitea)
    git_url = body.get("git_url")
    if git_url:
        env_vars["GIT_URL"] = git_url

    # Note: LLM API keys are no longer injected directly. Sandboxes use
    # LLM_PROXY_URL (passed via user_env_vars) to proxy through the control plane.

    # Build session config JSON
    session_config = {
        "session_id": body.get("session_id"),
        "repo_owner": body.get("repo_owner"),
        "repo_name": body.get("repo_name"),
        "provider": body.get("provider", "anthropic"),
        "model": body.get("model", "claude-sonnet-4-6"),
    }
    git_user_name = body.get("git_user_name")
    git_user_email = body.get("git_user_email")
    if git_user_name and git_user_email:
        session_config["git_user"] = {"name": git_user_name, "email": git_user_email}

    env_vars["SESSION_CONFIG"] = json.dumps(session_config)

    try:
        container_id = await manager.create_sandbox(
            sandbox_id=sandbox_id,
            env_vars=env_vars,
        )

        duration_ms = int((time.time() - start_time) * 1000)
        log.info("Sandbox created in %dms: %s", duration_ms, sandbox_id)

        return {
            "success": True,
            "data": {
                "sandbox_id": sandbox_id,
                "modal_object_id": container_id,
                "status": "warming",
                "created_at": time.time(),
            },
        }
    except ContainerError as e:
        log.error("Sandbox creation failed: %s", e)
        return {"success": False, "error": str(e)}
    except Exception as e:
        log.error("Sandbox creation failed: %s", e)
        return {"success": False, "error": str(e)}


# ---------------------------------------------------------------------------
# POST /api/stop-sandbox
# ---------------------------------------------------------------------------

@app.post("/api/stop-sandbox")
async def stop_sandbox(
    request: Request,
    authorization: str | None = Header(None),
):
    require_auth(authorization)
    body = await request.json()

    sandbox_id = body.get("sandbox_id")
    if not sandbox_id:
        raise HTTPException(status_code=400, detail="sandbox_id is required")

    try:
        await manager.stop_sandbox(sandbox_id)
        log.info("Sandbox stopped: %s", sandbox_id)
        return {"success": True, "data": {"sandbox_id": sandbox_id}}
    except ContainerError as e:
        log.error("Sandbox stop failed: %s", e)
        return {"success": False, "error": str(e)}
    except Exception as e:
        log.error("Sandbox stop failed: %s", e)
        return {"success": False, "error": str(e)}


# ---------------------------------------------------------------------------
# POST /api/snapshot-sandbox
# ---------------------------------------------------------------------------

@app.post("/api/snapshot-sandbox")
async def snapshot_sandbox(
    request: Request,
    authorization: str | None = Header(None),
):
    require_auth(authorization)
    body = await request.json()

    sandbox_id = body.get("sandbox_id")
    if not sandbox_id:
        raise HTTPException(status_code=400, detail="sandbox_id is required")

    session_id = body.get("session_id", "")
    reason = body.get("reason", "manual")

    try:
        # Look up repo info from any existing snapshot or use empty strings
        existing = snapshot_store.get_latest("", "")
        repo_owner = body.get("repo_owner", "")
        repo_name = body.get("repo_name", "")

        image_id = await manager.take_snapshot(
            container_id=sandbox_id,
            sandbox_id=sandbox_id,
            session_id=session_id,
            repo_owner=repo_owner,
            repo_name=repo_name,
            reason=reason,
        )

        return {
            "success": True,
            "data": {
                "image_id": image_id,
                "sandbox_id": sandbox_id,
                "session_id": session_id,
                "reason": reason,
            },
        }
    except ContainerError as e:
        log.error("Snapshot failed: %s", e)
        return {"success": False, "error": str(e)}
    except Exception as e:
        log.error("Snapshot failed: %s", e)
        return {"success": False, "error": str(e)}


# ---------------------------------------------------------------------------
# POST /api/restore-sandbox
# ---------------------------------------------------------------------------

@app.post("/api/restore-sandbox")
async def restore_sandbox(
    request: Request,
    authorization: str | None = Header(None),
):
    require_auth(authorization)
    body = await request.json()

    snapshot_image_id = body.get("snapshot_image_id")
    if not snapshot_image_id:
        raise HTTPException(status_code=400, detail="snapshot_image_id is required")

    session_config = body.get("session_config", {})
    sandbox_id = body.get("sandbox_id") or f"sandbox-{session_config.get('repo_owner', '')}-{session_config.get('repo_name', '')}-{int(time.time() * 1000)}"
    sandbox_auth_token = body.get("sandbox_auth_token", "")
    control_plane_url = body.get("control_plane_url", "")

    # Generate GitHub App token
    github_app_token = None
    try:
        github_app_token = generate_installation_token()
    except Exception as e:
        log.warning("GitHub token generation failed: %s", e)

    # Build env vars
    env_vars: dict[str, str] = {}

    user_env_vars = body.get("user_env_vars")
    if user_env_vars:
        env_vars.update(user_env_vars)

    env_vars.update({
        "PYTHONUNBUFFERED": "1",
        "SANDBOX_ID": sandbox_id,
        "CONTROL_PLANE_URL": control_plane_url,
        "SANDBOX_AUTH_TOKEN": sandbox_auth_token,
        "REPO_OWNER": session_config.get("repo_owner", ""),
        "REPO_NAME": session_config.get("repo_name", ""),
        "SESSION_CONFIG": json.dumps(session_config),
    })

    if github_app_token:
        env_vars["GITHUB_APP_TOKEN"] = github_app_token
        env_vars["GITHUB_TOKEN"] = github_app_token

    try:
        container_id = await manager.restore_from_snapshot(
            image_id=snapshot_image_id,
            sandbox_id=sandbox_id,
            env_vars=env_vars,
        )

        return {
            "success": True,
            "data": {
                "sandbox_id": sandbox_id,
                "modal_object_id": container_id,
                "status": "warming",
            },
        }
    except ContainerError as e:
        log.error("Restore failed: %s", e)
        return {"success": False, "error": str(e)}
    except Exception as e:
        log.error("Restore failed: %s", e)
        return {"success": False, "error": str(e)}


# ---------------------------------------------------------------------------
# POST /api/warm-sandbox
# ---------------------------------------------------------------------------

@app.post("/api/warm-sandbox")
async def warm_sandbox(
    request: Request,
    authorization: str | None = Header(None),
):
    require_auth(authorization)
    body = await request.json()

    repo_owner = body.get("repo_owner", "")
    repo_name = body.get("repo_name", "")
    sandbox_id = f"sandbox-{repo_owner}-{repo_name}-{int(time.time() * 1000)}"

    env_vars: dict[str, str] = {
        "PYTHONUNBUFFERED": "1",
        "SANDBOX_ID": sandbox_id,
        "CONTROL_PLANE_URL": body.get("control_plane_url", ""),
        "REPO_OWNER": repo_owner,
        "REPO_NAME": repo_name,
    }

    try:
        await manager.create_sandbox(sandbox_id=sandbox_id, env_vars=env_vars)
        return {
            "success": True,
            "data": {
                "sandbox_id": sandbox_id,
                "status": "warming",
            },
        }
    except Exception as e:
        log.error("Warm sandbox failed: %s", e)
        return {"success": False, "error": str(e)}


# ---------------------------------------------------------------------------
# GET /api/health
# ---------------------------------------------------------------------------

@app.get("/api/health")
async def health():
    return {
        "success": True,
        "data": {
            "status": "healthy",
            "service": "open-inspect-local-sandbox",
            "runtime": config.SANDBOX_RUNTIME,
        },
    }
