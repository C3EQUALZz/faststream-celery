import os
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from celery import Celery
from docker.errors import DockerException
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from faststream_celery import CeleryBroker

RABBITMQ_PORT = 5672
REDIS_PORT = 6379

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONTAINER_STARTUP_TIMEOUT = 60
WORKER_STARTUP_TIMEOUT = 60.0


def _start(container: DockerContainer, ready_log: str) -> Iterator[DockerContainer]:
    """Start a container, skipping the session when Docker is unavailable."""
    try:
        container.start()
    except (DockerException, OSError) as exc:
        pytest.skip(f"Docker is not available, skipping integration tests: {exc}")

    try:
        wait_for_logs(container, ready_log, timeout=CONTAINER_STARTUP_TIMEOUT)
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def rabbit_url() -> Iterator[str]:
    """A live RabbitMQ, shared by the whole session."""
    container = DockerContainer("rabbitmq:3.13-alpine").with_exposed_ports(RABBITMQ_PORT)

    for started in _start(container, "Server startup complete"):
        host = started.get_container_host_ip()
        port = started.get_exposed_port(RABBITMQ_PORT)
        yield f"amqp://guest:guest@{host}:{port}//"


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    """A live Redis, shared by the whole session."""
    container = DockerContainer("redis:7-alpine").with_exposed_ports(REDIS_PORT)

    for started in _start(container, "Ready to accept connections"):
        host = started.get_container_host_ip()
        port = started.get_exposed_port(REDIS_PORT)
        yield f"redis://{host}:{port}/0"


@pytest.fixture()
def broker(broker_url: str) -> CeleryBroker:
    """An unstarted broker on the transport under test.

    Handlers are registered before start, so the fixture hands back a broker
    the test still has to run — see `tests.helpers.running`.
    """
    return CeleryBroker(broker_url)


@pytest.fixture()
def result_backend(broker_url: str) -> str:
    """Where a Celery client and worker exchange results.

    AMQP has no result store of its own, so RPC replies carry the result;
    Redis doubles as the `celery-task-meta-*` backend.
    """
    return broker_url if broker_url.startswith("redis://") else "rpc://"


@pytest.fixture()
def celery_app(broker_url: str, result_backend: str) -> Celery:
    """A real Celery client on the transport under test."""
    return Celery(
        "faststream_celery_tests",
        broker=broker_url,
        backend=result_backend,
    )


@pytest.fixture()
def celery_worker(
    broker_url: str,
    result_backend: str,
    queue: str,
    tmp_path: Path,
) -> Iterator[Path]:
    """Run a real `celery worker` (solo pool) consuming the test queue.

    Returns the path of the file the worker's tasks append their payloads to.
    """
    log_file = tmp_path / "task-log.txt"
    env = {
        **os.environ,
        "CELERY_TEST_BROKER_URL": broker_url,
        "CELERY_TEST_RESULT_BACKEND": result_backend,
        "CELERY_TEST_LOG": str(log_file),
        "PYTHONPATH": os.pathsep.join(
            part for part in (str(PROJECT_ROOT), os.environ.get("PYTHONPATH", "")) if part
        ),
    }
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "celery",
            "-A",
            "tests.integration.celery_tasks:app",
            "worker",
            f"--queues={queue}",
            "--pool=solo",
            "--loglevel=WARNING",
            "--without-heartbeat",
            "--without-mingle",
            "--without-gossip",
        ],
        cwd=PROJECT_ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        _wait_for_worker(broker_url, process)
        yield log_file
    finally:
        process.terminate()
        process.wait(timeout=30)


def _wait_for_worker(broker_url: str, process: "subprocess.Popen[bytes]") -> None:
    """Block until the worker answers a control ping, or give up."""
    app = Celery("faststream_celery_probe", broker=broker_url)

    deadline = time.monotonic() + WORKER_STARTUP_TIMEOUT
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail(f"celery worker exited early with code {process.returncode}")

        if app.control.ping(timeout=1.0):
            return

    pytest.fail(f"celery worker did not start within {WORKER_STARTUP_TIMEOUT}s")
