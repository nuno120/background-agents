"""Configuration for the local sandbox manager."""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from package root
_env_path = Path(__file__).parent.parent / ".env"
if _env_path.exists():
    load_dotenv(_env_path)


def get(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


# Shared HMAC secret for authenticating requests from the control plane.
MODAL_API_SECRET = get("MODAL_API_SECRET")

# GitHub App credentials (for generating installation tokens).
GITHUB_APP_ID = get("GITHUB_APP_ID")
GITHUB_APP_PRIVATE_KEY = get("GITHUB_APP_PRIVATE_KEY").replace("\\n", "\n")
GITHUB_APP_INSTALLATION_ID = get("GITHUB_APP_INSTALLATION_ID")

# Note: LLM API keys are no longer configured here. Sandboxes receive
# LLM_PROXY_URL via user_env_vars and proxy through the control plane.

# ── Runtime selection ─────────────────────────────────────────────────────
# "docker" — uses Docker CLI; works on Linux, Windows, macOS.  (default)
# "kata"   — uses containerd + Kata runtime; Linux only, needs nested virt.
SANDBOX_RUNTIME = get("SANDBOX_RUNTIME", "docker")

# Container image (used by both runtimes).
SANDBOX_IMAGE = get("SANDBOX_IMAGE", "open-inspect-sandbox:latest")

# ── Kata-specific settings ────────────────────────────────────────────────
KATA_RUNTIME = get("KATA_RUNTIME", "io.containerd.kata.v2")
CONTAINERD_NAMESPACE = get("CONTAINERD_NAMESPACE", "default")

# ── Docker-specific settings ──────────────────────────────────────────────
DOCKER_MEMORY_LIMIT = get("DOCKER_MEMORY_LIMIT", "4g")
DOCKER_CPU_LIMIT = get("DOCKER_CPU_LIMIT", "2")
DOCKER_PIDS_LIMIT = int(get("DOCKER_PIDS_LIMIT", "8192"))
DOCKER_NETWORK = get("DOCKER_NETWORK")

# Snapshot storage directory.
_default_snapshot_dir = str(Path(__file__).parent.parent / "data" / "snapshots")
SNAPSHOT_DIR = get("SNAPSHOT_DIR", _default_snapshot_dir)

# Default sandbox timeout in seconds.
DEFAULT_SANDBOX_TIMEOUT_SECONDS = int(get("DEFAULT_SANDBOX_TIMEOUT_SECONDS", "7200"))
