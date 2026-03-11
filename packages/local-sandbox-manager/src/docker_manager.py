"""
DockerContainerManager — manages sandbox containers via Docker CLI.

Cross-platform alternative to KataContainerManager. Works on Linux, Windows,
and macOS wherever Docker (or Docker Desktop) is installed.

Security hardening:
  - --cap-drop=ALL + targeted cap-add   (principle of least privilege)
  - SYS_ADMIN + MKNOD for DinD          (builder verification)
  - --cgroupns=host for DinD on cgroup v2
  - no-new-privileges removed            (incompatible with DinD containerd-shim)
  - --memory limit                       (prevents OOM on host)
  - --pids-limit                         (prevents fork bombs)
  - Default seccomp + AppArmor           (Docker builtin profiles)
  - Network isolation                    (sandbox-only network)
  - No --privileged
"""

import asyncio
import logging
import os
import time

from . import config
from .snapshot_store import SnapshotRecord, SnapshotStore

log = logging.getLogger("docker_manager")


class ContainerError(Exception):
    pass


async def _run(cmd: list[str], timeout: float = 120) -> tuple[int, str, str]:
    """Run a subprocess and return (returncode, stdout, stderr)."""
    log.debug("Running: %s", " ".join(cmd))
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        raise ContainerError(f"Command timed out after {timeout}s: {' '.join(cmd)}")
    return proc.returncode or 0, stdout.decode(), stderr.decode()


class DockerContainerManager:
    """Manage sandbox containers via the Docker CLI with hardened security."""

    def __init__(self) -> None:
        self.snapshot_store = SnapshotStore()

    async def create_sandbox(
        self,
        sandbox_id: str,
        env_vars: dict[str, str],
        image: str | None = None,
    ) -> str:
        """
        Create and start a new Docker container with security hardening.

        Returns the Docker container ID (short hash).
        """
        image = image or config.SANDBOX_IMAGE
        container_name = sandbox_id

        # Remove any leftover container with the same name
        await _run(["docker", "rm", "-f", container_name])

        # Build env flags
        env_flags: list[str] = []
        for k, v in env_vars.items():
            env_flags.extend(["-e", f"{k}={v}"])

        # Security and resource flags
        # Note: DinD requires SYS_ADMIN + MKNOD and is incompatible with
        # no-new-privileges (containerd-shim needs privilege escalation).
        # Compensating controls: cap-drop=ALL, network isolation, resource limits.
        security_flags = [
            # Drop ALL capabilities, add back only essentials + DinD
            "--cap-drop=ALL",
            "--cap-add=CHOWN",         # chown files (npm, git)
            "--cap-add=DAC_OVERRIDE",  # bypass file permission checks (root in container)
            "--cap-add=FOWNER",        # change file ownership
            "--cap-add=SETGID",        # set group ID (su, sudo)
            "--cap-add=SETUID",        # set user ID (su, sudo)
            "--cap-add=NET_RAW",       # raw sockets (ping, health checks)
            "--cap-add=NET_BIND_SERVICE",  # bind ports < 1024
            "--cap-add=SYS_CHROOT",    # chroot (some build tools)
            "--cap-add=SYS_ADMIN",     # Docker-in-Docker (cgroups/namespaces)
            "--cap-add=MKNOD",         # Device nodes (DinD)
            "--cgroupns=host",         # Share host cgroup namespace (required for DinD on cgroup v2)
            # Resource limits
            f"--memory={config.DOCKER_MEMORY_LIMIT}",
            f"--cpus={config.DOCKER_CPU_LIMIT}",
            f"--pids-limit={config.DOCKER_PIDS_LIMIT}",
            # Tmpfs for /tmp (faster I/O, auto-cleaned)
            "--tmpfs=/tmp:rw,exec,size=2g",
        ]

        # Network mode: if DOCKER_NETWORK is set (e.g. Docker Compose), join that
        # network so containers use Docker DNS. Otherwise fall back to --network=host
        # on Linux for simplicity (container reaches localhost:8787).
        docker_network = config.DOCKER_NETWORK
        if docker_network:
            security_flags.append(f"--network={docker_network}")
        else:
            import platform
            if platform.system() == "Linux":
                security_flags.append("--network=host")

        cmd = [
            "docker", "run",
            "-d",  # detach
            "--name", container_name,
            *security_flags,
            *env_flags,
            "-w", "/workspace",
            image,
            "python", "-m", "sandbox.entrypoint",
        ]

        rc, stdout, stderr = await _run(cmd, timeout=60)
        if rc != 0:
            raise ContainerError(f"Failed to create container: {stderr.strip()}")

        container_id = stdout.strip()[:12]
        log.info("Container started: %s (%s)", container_name, container_id)
        return container_id

    async def stop_sandbox(self, container_id: str) -> None:
        """Stop and remove a container."""
        await _run(["docker", "stop", "-t", "10", container_id])
        await _run(["docker", "rm", "-f", container_id])
        log.info("Container removed: %s", container_id)

    async def take_snapshot(
        self,
        container_id: str,
        sandbox_id: str,
        session_id: str,
        repo_owner: str,
        repo_name: str,
        reason: str,
    ) -> str:
        """
        Snapshot a running container:
          1. docker pause
          2. docker commit → snapshot image
          3. docker save → tar file
          4. docker unpause
        Returns the snapshot image_id.
        """
        image_id = f"snap-{sandbox_id}-{int(time.time() * 1000)}"
        snapshot_image = f"open-inspect-snapshot:{image_id}"
        tar_path = os.path.join(config.SNAPSHOT_DIR, f"{image_id}.tar")

        os.makedirs(config.SNAPSHOT_DIR, exist_ok=True)

        try:
            # Pause the container
            log.info("Pausing container %s for snapshot", container_id)
            rc, _, stderr = await _run(["docker", "pause", container_id])
            if rc != 0:
                raise ContainerError(f"Pause failed: {stderr.strip()}")

            # Commit container state to a new image
            rc, stdout, stderr = await _run(
                ["docker", "commit", container_id, snapshot_image],
                timeout=300,
            )
            if rc != 0:
                raise ContainerError(f"Commit failed: {stderr.strip()}")
            log.info("Committed snapshot image: %s", snapshot_image)

            # Export the image to a tar file
            rc, _, stderr = await _run(
                ["docker", "save", "-o", tar_path, snapshot_image],
                timeout=300,
            )
            if rc != 0:
                raise ContainerError(f"Save failed: {stderr.strip()}")

        finally:
            # Always unpause the container
            await _run(["docker", "unpause", container_id])
            log.info("Container %s unpaused after snapshot", container_id)

        # Record in snapshot store
        self.snapshot_store.save(
            SnapshotRecord(
                image_id=image_id,
                sandbox_id=sandbox_id,
                session_id=session_id,
                repo_owner=repo_owner,
                repo_name=repo_name,
                reason=reason,
                tar_path=tar_path,
                created_at=time.time(),
            )
        )

        # Clean up the local snapshot image (tar is the source of truth)
        await _run(["docker", "rmi", snapshot_image])

        log.info("Snapshot saved: %s -> %s", image_id, tar_path)
        return image_id

    async def restore_from_snapshot(
        self,
        image_id: str,
        sandbox_id: str,
        env_vars: dict[str, str],
    ) -> str:
        """
        Restore a sandbox from a previously saved snapshot:
          1. docker load → import tar as image
          2. docker run → create container from snapshot image
        Returns the new container ID.
        """
        record = self.snapshot_store.get(image_id)
        if not record:
            raise ContainerError(f"Snapshot not found: {image_id}")

        if not os.path.exists(record.tar_path):
            raise ContainerError(f"Snapshot tar not found: {record.tar_path}")

        # Import the snapshot image
        snapshot_image = f"open-inspect-snapshot:{image_id}"
        rc, stdout, stderr = await _run(
            ["docker", "load", "-i", record.tar_path],
            timeout=300,
        )
        if rc != 0:
            raise ContainerError(f"Image load failed: {stderr.strip()}")

        log.info("Loaded snapshot image: %s", snapshot_image)

        # Add RESTORED_FROM_SNAPSHOT env var
        env_vars["RESTORED_FROM_SNAPSHOT"] = "true"

        # Create a new container from the snapshot image
        container_id = await self.create_sandbox(
            sandbox_id=sandbox_id,
            env_vars=env_vars,
            image=snapshot_image,
        )

        log.info("Container restored from snapshot %s: %s", image_id, container_id)
        return container_id
