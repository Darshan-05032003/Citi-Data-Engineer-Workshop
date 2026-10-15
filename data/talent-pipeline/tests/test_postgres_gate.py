"""The Postgres parity test's availability gate: skip locally, fail when REQUIRE_POSTGRES=1 (CI).

No database is needed: shutil.which and subprocess.run are faked.
"""
import subprocess

import pytest

from tests import test_postgres as tp

ENV = {"POSTGRES_NAME": "db", "POSTGRES_USER": "u", "POSTGRES_PASS": "x",
       "POSTGRES_HOST": "pg.example", "POSTGRES_PORT": "6000"}
ALL_VARS = [*ENV, "REQUIRE_POSTGRES"]


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    """Start every test with no POSTGRES_* or REQUIRE_POSTGRES in the environment."""
    for name in ALL_VARS:
        monkeypatch.delenv(name, raising=False)


def _set(monkeypatch, **overrides) -> None:
    for name, value in {**ENV, **overrides}.items():
        monkeypatch.setenv(name, value)


def _fake_pg(monkeypatch, returncode: int, installed: bool = True) -> None:
    monkeypatch.setattr(tp.shutil, "which", lambda _name: "/usr/bin/pg_isready" if installed else None)
    monkeypatch.setattr(tp.subprocess, "run",
                        lambda *a, **k: subprocess.CompletedProcess(a, returncode))


def test_missing_env_skips_locally_and_fails_in_ci(monkeypatch) -> None:
    """Without POSTGRES_* → skip; with REQUIRE_POSTGRES=1 → fail, naming the variables."""
    with pytest.raises(pytest.skip.Exception, match="POSTGRES_NAME, POSTGRES_USER, POSTGRES_PASS"):
        tp._pg_env()
    monkeypatch.setenv("REQUIRE_POSTGRES", "1")
    with pytest.raises(pytest.fail.Exception, match="REQUIRE_POSTGRES is set but POSTGRES_NAME"):
        tp._pg_env()


def test_unreachable_server_skips_locally_and_fails_in_ci(monkeypatch) -> None:
    """pg_isready failing → skip, or fail with host:port in the message."""
    _set(monkeypatch)
    _fake_pg(monkeypatch, returncode=2)
    with pytest.raises(pytest.skip.Exception, match="not reachable at pg.example:6000"):
        tp._pg_env()
    monkeypatch.setenv("REQUIRE_POSTGRES", "1")
    with pytest.raises(pytest.fail.Exception, match="REQUIRE_POSTGRES is set but PostgreSQL is not "
                                                    "reachable at pg.example:6000"):
        tp._pg_env()


def test_missing_pg_isready_skips_locally_and_fails_in_ci(monkeypatch) -> None:
    """No pg_isready binary → skip, or fail telling to install postgresql-client."""
    _set(monkeypatch)
    _fake_pg(monkeypatch, returncode=0, installed=False)
    with pytest.raises(pytest.skip.Exception, match="pg_isready"):
        tp._pg_env()
    monkeypatch.setenv("REQUIRE_POSTGRES", "true")
    with pytest.raises(pytest.fail.Exception, match="postgresql-client"):
        tp._pg_env()


@pytest.mark.parametrize("flag", [None, "1"])
def test_reachable_server_returns_settings(monkeypatch, flag) -> None:
    """When Postgres is up the gate passes, whether or not REQUIRE_POSTGRES is set."""
    _set(monkeypatch)
    if flag:
        monkeypatch.setenv("REQUIRE_POSTGRES", flag)
    _fake_pg(monkeypatch, returncode=0)
    env = tp._pg_env()
    assert env["POSTGRES_HOST"] == "pg.example" and env["POSTGRES_PORT"] == "6000"
    assert env["POSTGRES_SCHEMA"] == tp.PARITY_SCHEMA and env["POSTGRES_PASS"] == "x"


def test_false_like_flag_does_not_require(monkeypatch) -> None:
    """REQUIRE_POSTGRES=0 keeps the clean local skip."""
    monkeypatch.setenv("REQUIRE_POSTGRES", "0")
    with pytest.raises(pytest.skip.Exception):
        tp._pg_env()
