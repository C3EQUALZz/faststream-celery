"""Version matrix for faststream-celery.

The package inherits from the private ``faststream._internal`` API, so a
faststream patch release can break it without warning (``docs/design.md``
§12). This matrix runs the unit suite against every supported Python, both
the minimum and latest supported faststream, and both ends of the kombu range.

    nox                     # the whole matrix
    nox -s tests            # unit tests across the matrix
    nox -s tests -k 'faststream_latest and kombu_latest'  # latest releases only
    nox -s integration      # integration tests (needs Docker)
    nox -s tests-3.12       # one Python only
"""

import nox

nox.options.default_venv_backend = "uv"
nox.options.sessions = ["tests"]

PYTHON_VERSIONS = ("3.10", "3.11", "3.12", "3.13", "3.14")

# Match the minimum and latest allowed by pyproject.toml.
FASTSTREAM_VERSIONS = ("==0.7.5", ">=0.7.5")

# Both ends of `kombu>=5.3,<6`.
KOMBU_VERSIONS = ("==5.3.0", ">=5.3,<6")

# What `tests/unit` imports beyond the package itself: the optional
# integrations ship with it, so they are exercised on every matrix point.
UNIT_TEST_DEPS = (
    "pytest",
    "pytest-asyncio",
    "pytest-cov",
    "celery-types",
    "fastapi",
    "httpx",
    "opentelemetry-sdk",
    "prometheus-client",
    "redis",
)

INTEGRATION_TEST_DEPS = (
    *UNIT_TEST_DEPS,
    "celery",
    "testcontainers",
)


@nox.session(python=PYTHON_VERSIONS)
@nox.parametrize(
    "faststream", FASTSTREAM_VERSIONS, ids=("faststream_min", "faststream_latest")
)
@nox.parametrize("kombu", KOMBU_VERSIONS, ids=("kombu_min", "kombu_latest"))
def tests(session: nox.Session, faststream: str, kombu: str) -> None:
    """Run the unit suite against one point of the matrix."""
    session.install(
        "--upgrade",
        "-e",
        ".",
        f"faststream{faststream}",
        f"kombu{kombu}",
        *UNIT_TEST_DEPS,
    )

    session.run("pytest", "tests/unit", *session.posargs)


@nox.session(python=PYTHON_VERSIONS[-1])
def integration(session: nox.Session) -> None:
    """Run the integration suite against live Celery (requires Docker)."""
    session.install("-e", ".")
    session.install(*INTEGRATION_TEST_DEPS)

    session.run(
        "pytest",
        "tests/integration",
        "-m",
        "connected",
        *session.posargs,
    )


@nox.session(python=PYTHON_VERSIONS[-1])
def lint(session: nox.Session) -> None:
    """Run ruff and mypy."""
    session.install("-e", ".")
    session.install("ruff", "mypy", *INTEGRATION_TEST_DEPS)

    session.run("ruff", "format", "--check", ".")
    session.run("ruff", "check", "--no-fix", ".")
    session.run("mypy")
