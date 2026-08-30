"""The optional integrations must stay optional.

Each of `fastapi`, `opentelemetry` and `prometheus_client` is an extra, so
importing the package itself must work without any of them installed. The
check runs in a subprocess with those imports blocked, because they are
already imported into this one.
"""

import subprocess
import sys
import textwrap

import pytest

# Installs an import hook that refuses the modules an extra would provide,
# then runs whatever the test appends.
BLOCKER = """
import sys

class Blocker:
    def __init__(self, names):
        self.names = set(names)

    def find_spec(self, name, path=None, target=None):
        if name.split('.')[0] in self.names:
            raise ImportError('No module named ' + repr(name))
        return None

_blocked = BLOCKED_NAMES

for _module in list(sys.modules):
    if _module.split('.')[0] in _blocked:
        del sys.modules[_module]

sys.meta_path.insert(0, Blocker(_blocked))
"""


def run_without(*blocked: str, body: str) -> "subprocess.CompletedProcess[str]":
    """Run a snippet in a subprocess where `blocked` cannot be imported."""
    script = BLOCKER.replace("BLOCKED_NAMES", repr(list(blocked)))
    script += textwrap.dedent(body)

    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )


class TestCoreImport:
    @pytest.mark.parametrize(
        "blocked",
        (
            ("fastapi",),
            ("opentelemetry",),
            ("prometheus_client",),
            ("fastapi", "opentelemetry", "prometheus_client"),
        ),
    )
    def test_the_package_imports_without_the_extras(
        self,
        blocked: tuple[str, ...],
    ) -> None:
        result = run_without(
            *blocked,
            body="""
            from faststream_celery import (
                CeleryBroker,
                CeleryRouter,
                CeleryTask,
                TestCeleryBroker,
            )

            broker = CeleryBroker()
            broker.subscriber("celery", task="proj.tasks.add")
            print("ok")
            """,
        )

        assert result.returncode == 0, result.stderr
        assert "ok" in result.stdout

    def test_annotations_import_without_the_extras(self) -> None:
        result = run_without(
            "fastapi",
            "opentelemetry",
            "prometheus_client",
            body="""
            from faststream_celery import annotations

            assert annotations.CeleryMessage is not None
            print("ok")
            """,
        )

        assert result.returncode == 0, result.stderr
        assert "ok" in result.stdout


class TestExtraImports:
    def test_the_fastapi_module_explains_the_missing_extra(self) -> None:
        result = run_without(
            "fastapi",
            body="""
            try:
                import faststream_celery.fastapi
            except ImportError as exc:
                print(exc)
            """,
        )

        assert result.returncode == 0, result.stderr
        assert "faststream-celery[fastapi]" in result.stdout

    @pytest.mark.parametrize(
        ("module", "blocked"),
        (
            ("faststream_celery.opentelemetry", "opentelemetry"),
            ("faststream_celery.prometheus", "prometheus_client"),
        ),
    )
    def test_an_observability_module_needs_its_extra(
        self,
        module: str,
        blocked: str,
    ) -> None:
        result = run_without(
            blocked,
            body=f"""
            try:
                import {module}
            except ImportError:
                print("refused")
            """,
        )

        assert result.returncode == 0, result.stderr
        assert "refused" in result.stdout
