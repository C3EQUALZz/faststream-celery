"""The private ``faststream._internal`` API must stay in one adapter module.

``docs/design.md`` §12 makes this the project's main defence against breakage
between faststream releases: one file to repair, checked by the nightly CI run
against ``faststream@main``.
"""

import ast
from pathlib import Path

import pytest

PACKAGE_ROOT = Path(__file__).resolve().parents[2] / "src" / "faststream_celery"
ADAPTER = PACKAGE_ROOT / "_internal.py"

# One adapter per optional surface: the FastAPI one cannot live in the
# package-level module without making FastAPI a hard dependency.
ADAPTER_NAME = "_internal.py"

PRIVATE_MODULE = "faststream._internal"


def _source_files() -> list[Path]:
    return sorted(PACKAGE_ROOT.rglob("*.py"))


def _private_imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            if node.module == PRIVATE_MODULE or node.module.startswith(
                f"{PRIVATE_MODULE}.",
            ):
                found.append(node.module)

        elif isinstance(node, ast.Import):
            found.extend(
                alias.name
                for alias in node.names
                if alias.name == PRIVATE_MODULE
                or alias.name.startswith(f"{PRIVATE_MODULE}.")
            )

    return found


def test_the_package_has_source_files_to_check() -> None:
    assert _source_files()
    assert ADAPTER.exists()


def test_every_adapter_module_imports_the_private_api() -> None:
    adapters = [p for p in _source_files() if p.name == ADAPTER_NAME]

    assert adapters
    assert all(_private_imports(p) for p in adapters)


@pytest.mark.parametrize(
    "path",
    [p for p in _source_files() if p.name != ADAPTER_NAME],
    ids=lambda p: str(p.relative_to(PACKAGE_ROOT)),
)
def test_no_other_module_imports_the_private_api(path: Path) -> None:
    imports = _private_imports(path)

    assert not imports, (
        f"{path.relative_to(PACKAGE_ROOT)} imports {imports} directly. "
        f"Re-export it from faststream_celery/_internal.py instead "
        f"(see docs/design.md §12)."
    )
