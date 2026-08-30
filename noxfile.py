"""Version matrix for faststream-celery.

The package inherits from the private ``faststream._internal`` API, so a
faststream patch release can break it without warning (``docs/design.md``
§12). This matrix runs the unit suite against every supported Python, both
ends of the pinned faststream range, and both ends of the kombu range.

    nox                     # the whole matrix
    nox -s tests            # unit tests across the matrix
    nox -s integration      # integration tests (needs Docker)
    nox -s tests-3.12       # one Python only
"""

import nox

nox.options.default_venv_backend = "uv"
nox.options.sessions = ["tests"]

PYTHON_VERSIONS = ("3.10", "3.11", "3.12", "3.13", "3.14")

# Both ends of `faststream>=0.7.5,<0.8`; extend as new minors are released.
FASTSTREAM_VERSIONS = ("==0.7.5", ">=0.7.5,<0.8")

# Both ends of `kombu>=5.3,<6`.
KOMBU_VERSIONS = ("==5.3.0", ">=5.3,<6")

FASTSTREAM_MAIN = "faststream @ git+https://github.com/ag2ai/faststream.git@main"


@nox.session(python=PYTHON_VERSIONS)
@nox.parametrize("faststream", FASTSTREAM_VERSIONS)
@nox.parametrize("kombu", KOMBU_VERSIONS)
def tests(session: nox.Session, faststream: str, kombu: str) -> None:
    """Run the unit suite against one point of the matrix."""
    session.install("-e", ".")
    session.install(f"faststream{faststream}", f"kombu{kombu}")
    session.install("pytest", "pytest-asyncio", "pytest-cov")

    session.run("pytest", "tests/unit", *session.posargs)


@nox.session(python=PYTHON_VERSIONS[-1])
def nightly(session: nox.Session) -> None:
    """Run the unit suite against faststream's git main.

    Mirrors the scheduled CI job, so a breaking change in the private API is
    reproducible locally.
    """
    session.install("-e", ".")
    session.install(FASTSTREAM_MAIN)
    session.install("pytest", "pytest-asyncio", "pytest-cov")

    session.run("pytest", "tests/unit", *session.posargs)


@nox.session(python=PYTHON_VERSIONS[-1])
def integration(session: nox.Session) -> None:
    """Run the integration suite against live Celery (requires Docker)."""
    session.install("-e", ".")
    session.install(
        "pytest",
        "pytest-asyncio",
        "pytest-cov",
        "celery",
        "testcontainers",
    )

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
    session.install("ruff", "mypy", "pytest", "celery", "testcontainers")

    session.run("ruff", "format", "--check", ".")
    session.run("ruff", "check", "--no-fix", ".")
    session.run("mypy")
