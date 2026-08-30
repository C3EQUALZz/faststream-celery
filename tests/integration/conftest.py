import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from celery import Celery
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

RABBITMQ_PORT = 5672
PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def rabbit_url() -> Iterator[str]:
    """Spin up RabbitMQ in Docker; skip the whole session without Docker."""
    try:
        container = DockerContainer(
            "rabbitmq:3.13-alpine",
        ).with_exposed_ports(RABBITMQ_PORT)
        container.start()
    except Exception as exc:
        pytest.skip(f"Docker is not available, skipping integration tests: {exc}")

    try:
        wait_for_logs(container, "Server startup complete", timeout=60)
        host = container.get_container_host_ip()
        port = container.get_exposed_port(RABBITMQ_PORT)
        yield f"amqp://guest:guest@{host}:{port}//"
    finally:
        container.stop()


@pytest.fixture()
def celery_app(rabbit_url: str) -> Celery:
    return Celery("faststream_celery_tests", broker=rabbit_url)


@pytest.fixture()
def celery_worker(rabbit_url: str, queue: str, tmp_path: Path) -> Iterator[Path]:
    """Run a real `celery worker` (solo pool) consuming the test queue.

    Returns the path of the file the worker's tasks append their payloads to.
    """
    log_file = tmp_path / "task-log.txt"
    env = {
        **os.environ,
        "CELERY_TEST_BROKER_URL": rabbit_url,
        "CELERY_TEST_LOG": str(log_file),
        "PYTHONPATH": os.pathsep.join(
            part
            for part in (str(PROJECT_ROOT), os.environ.get("PYTHONPATH", ""))
            if part
        ),
    }
    process = subprocess.Popen(  # noqa: S603
        [
            sys.executable,
            "-m",
            "celery",
            "-A",
            "tests.integration.celery_tasks:app",
            "worker",
            "--pool=solo",
            f"--queues={queue}",
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
        yield log_file
    finally:
        process.terminate()
        process.wait(timeout=30)
