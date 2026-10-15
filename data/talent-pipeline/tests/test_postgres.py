"""SOURCE_MODE=postgres: config, JDBC settings, retry, Bronze ingest via a fake JDBC source,
and (marked integration + postgres) a files-vs-postgres parity run on a real local PostgreSQL.

Unit tests replace job._jdbc_table, the single JDBC access point, with typed tables built
from tests/fixtures/mini_source/, so they need no database. Credentials come only from env vars.
"""
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

import job

JOB_DIR = Path(job.__file__).resolve().parent
MINI_SOURCE = JOB_DIR / "tests" / "fixtures" / "mini_source"
PG_ENV = {"SOURCE_MODE": "postgres", "POSTGRES_NAME": "db", "POSTGRES_USER": "u",
          "POSTGRES_PASS": "not-a-real-secret"}


# --- config, JDBC settings, packages -------------------------------------------------------
def test_load_config_postgres_settings() -> None:
    """Defaults for host/port/schema; values from env; files is the default mode."""
    assert job.load_config(env={}).source_mode == "files"
    cfg = job.load_config(env=PG_ENV)
    assert (cfg.source_mode, cfg.pg_host, cfg.pg_port, cfg.pg_schema) == (
        "postgres", "localhost", 5432, "hr_source")
    cfg = job.load_config(env={**PG_ENV, "SOURCE_MODE": " Postgres ", "POSTGRES_HOST": "h",
                               "POSTGRES_PORT": "6543", "POSTGRES_SCHEMA": "s"})
    assert (cfg.source_mode, cfg.pg_host, cfg.pg_port, cfg.pg_schema) == ("postgres", "h", 6543, "s")
    with pytest.raises(ValueError, match="SOURCE_MODE must be one of"):
        job.load_config(env={"SOURCE_MODE": "oracle"})


def test_jdbc_url_and_properties() -> None:
    """No credentials in the URL; sslmode=require outside local runs; 10 s connect timeout."""
    local = job.load_config(env=PG_ENV)
    cloud = job.load_config(env={**PG_ENV, "IS_LOCAL": "false", "POSTGRES_HOST": "aurora"})
    assert job.jdbc_url(local) == "jdbc:postgresql://localhost:5432/db"
    assert job.jdbc_url(cloud) == "jdbc:postgresql://aurora:5432/db?sslmode=require"
    assert "not-a-real-secret" not in job.jdbc_url(cloud)
    assert job.jdbc_properties(local) == {
        "user": "u", "password": "not-a-real-secret", "driver": "org.postgresql.Driver",
        "connectTimeout": "10",
    }


def test_spark_packages_per_mode() -> None:
    """postgresql driver only in postgres mode; hadoop-aws only for s3a; both comma-joined."""
    assert "spark.jars.packages" not in job.spark_conf(job.load_config(env={}))
    pg = job.spark_conf(job.load_config(env=PG_ENV))
    assert pg["spark.jars.packages"] == job.POSTGRES_JDBC_PACKAGE
    both = job.spark_conf(job.load_config(env={**PG_ENV, "LAKE_ROOT": "s3a://b/lake"}))
    assert both["spark.jars.packages"] == f"{job.HADOOP_AWS_PACKAGE},{job.POSTGRES_JDBC_PACKAGE}"


def test_password_not_in_repr() -> None:
    """The password never appears in the Config repr (used in logs and errors)."""
    cfg = job.load_config(env=PG_ENV)
    assert "not-a-real-secret" not in repr(cfg) and "pg_password" not in repr(cfg)


# --- retry ------------------------------------------------------------------------------
class _Flaky:
    """Fake connection that fails `failures` times, then returns 'ok'."""

    def __init__(self, failures: int) -> None:
        self.failures, self.calls = failures, 0

    def __call__(self) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise ConnectionError(f"refused #{self.calls}")
        return "ok"


def test_with_retry_recovers_with_backoff() -> None:
    """Two failures, then success: 3 calls, sleeps 2 s and 4 s."""
    sleeps: list[float] = []
    flaky = _Flaky(failures=2)
    assert job.with_retry(flaky, job.get_logger("r"), "pg", sleep=sleeps.append) == "ok"
    assert flaky.calls == 3 and sleeps == [2.0, 4.0]


def test_with_retry_gives_up_after_five_attempts() -> None:
    """Always failing: 1 try + 4 retries (2/4/8/16 s), then the last error is raised."""
    sleeps: list[float] = []
    flaky = _Flaky(failures=99)
    with pytest.raises(ConnectionError, match="refused #5"):
        job.with_retry(flaky, job.get_logger("r"), "pg", sleep=sleeps.append)
    assert flaky.calls == 5 and sleeps == [2.0, 4.0, 8.0, 16.0]


# --- Bronze ingest in postgres mode with a fake JDBC source ------------------------------
def _typed_table(spark, table: str, drop: tuple = (), empty: bool = False):
    """A typed table like JDBC returns (ints, dates, decimals), built from the mini CSVs."""
    df = spark.read.option("header", True).option("inferSchema", True).csv(
        str(MINI_SOURCE / f"{table}.csv"))
    df = df.drop(*drop)
    return df.limit(0) if empty else df


def _fake_jdbc(spark, overrides: dict | None = None, fail_probe: bool = False):
    """Replacement for job._jdbc_table: probe query, or schema.table → typed mini table."""
    overrides = overrides or {}

    def fake(_spark, cfg, dbtable: str):
        if dbtable.startswith("(SELECT 1"):
            if fail_probe:
                raise ConnectionError("connection refused")
            return spark.range(1)
        schema, table = dbtable.split(".")
        assert schema == cfg.pg_schema
        if overrides.get(table) == "missing":
            raise RuntimeError(f'relation "{dbtable}" does not exist')
        return _typed_table(spark, table, **overrides.get(table, {}))
    return fake


def _setup(tmp_path: Path, run_id: str, env: dict | None = None):
    cfg = job.load_config(env={**PG_ENV, "SOURCE_DIR": str(MINI_SOURCE),
                               "LAKE_ROOT": str(tmp_path / "lake"), **(env or {})})
    manifest = job.RunManifest(run_id=run_id, is_local=True, source_dir=cfg.source_dir,
                               lake_root=cfg.lake_root, source_mode=cfg.source_mode)
    return cfg, manifest, job.get_logger(run_id)


def test_ingest_bronze_from_postgres(spark, tmp_path, monkeypatch, capsys) -> None:
    """Typed JDBC columns land in Bronze as strings with postgres lineage and row counts."""
    monkeypatch.setattr(job, "HR_TABLES", ("teams", "employees"))
    monkeypatch.setattr(job, "_jdbc_table", _fake_jdbc(spark))
    cfg, manifest, log = _setup(tmp_path, "pg-bronze")
    job.ingest_into_bronze(spark, cfg, log, manifest)

    teams = spark.read.parquet(f"{cfg.lake_root}/bronze/hr/teams")
    assert dict(teams.dtypes)["team_id"] == "string"
    assert {r["_source_system"] for r in teams.collect()} == {"postgres"}
    assert {r["_source_file"] for r in teams.collect()} == {
        "postgres://localhost:5432/db.hr_source.teams"}
    emp = spark.read.parquet(f"{cfg.lake_root}/bronze/hr/employees")
    assert all(t == "string" for c, t in emp.dtypes if c in job.HR_COLUMNS["employees"])
    assert sorted(r["hire_date"] for r in emp.collect())[0] == "2018-11-01"  # date → ISO string

    assert manifest.source["hr.teams"] == {"rows": 3, "extra_columns": []}
    assert manifest.source["hr.employees"] == {"rows": 6, "extra_columns": []}
    assert [(s.step, s.rows_read, s.rows_written) for s in manifest.steps] == [
        ("bronze.hr.teams", 3, 3), ("bronze.hr.employees", 6, 6),
        ("bronze.learning_activity", 10, 10)]
    out = capsys.readouterr().out
    payload = json.dumps(manifest.to_dict())
    assert "not-a-real-secret" not in out and "not-a-real-secret" not in payload
    assert manifest.to_dict()["source_mode"] == "postgres"


def test_postgres_missing_settings_fail(spark, tmp_path) -> None:
    """Missing POSTGRES_* settings stop the run and name the variables."""
    cfg, manifest, log = _setup(tmp_path, "pg-env", env={"POSTGRES_PASS": "", "POSTGRES_USER": ""})
    with pytest.raises(ValueError, match=r"needs env vars \['POSTGRES_USER', 'POSTGRES_PASS'\]"):
        job.ingest_into_bronze(spark, cfg, log, manifest)
    assert (manifest.steps[-1].step, manifest.steps[-1].status) == ("bronze.hr.connection", "FAILED")


def test_postgres_unreachable_fails_after_retries(spark, tmp_path, monkeypatch) -> None:
    """No connection after 5 attempts → FAILED connection step; the backoff is 2/4/8/16 s."""
    sleeps: list[float] = []
    monkeypatch.setattr(job.time, "sleep", sleeps.append)
    monkeypatch.setattr(job, "_jdbc_table", _fake_jdbc(spark, fail_probe=True))
    cfg, manifest, log = _setup(tmp_path, "pg-down")
    with pytest.raises(ValueError, match=r"cannot connect to postgres localhost:5432/db after 5 "
                                         r"attempts \(ConnectionError\)"):
        job.ingest_into_bronze(spark, cfg, log, manifest)
    assert sleeps == [2.0, 4.0, 8.0, 16.0]
    assert manifest.steps[-1].step == "bronze.hr.connection"


def test_postgres_missing_table_fails(spark, tmp_path, monkeypatch) -> None:
    """A table that cannot be read is named in the error; nothing is written."""
    monkeypatch.setattr(job, "HR_TABLES", ("teams", "employees"))
    monkeypatch.setattr(job, "_jdbc_table", _fake_jdbc(spark, {"employees": "missing"}))
    cfg, manifest, log = _setup(tmp_path, "pg-missing")
    with pytest.raises(ValueError, match="source table not found or unreadable: hr_source.employees"):
        job.ingest_into_bronze(spark, cfg, log, manifest)
    assert not (tmp_path / "lake" / "bronze").exists()


def test_postgres_schema_drift_fails(spark, tmp_path, monkeypatch) -> None:
    """Same drift guard as files mode, labelled with schema.table."""
    monkeypatch.setattr(job, "HR_TABLES", ("employees",))
    monkeypatch.setattr(job, "_jdbc_table", _fake_jdbc(spark, {"employees": {"drop": ("email",)}}))
    cfg, manifest, log = _setup(tmp_path, "pg-drift")
    with pytest.raises(ValueError, match=r"schema drift in hr_source.employees: missing columns \['email'\]"):
        job.ingest_into_bronze(spark, cfg, log, manifest)


def test_postgres_empty_table_fails(spark, tmp_path, monkeypatch) -> None:
    """A table with 0 rows stops the run before it is written."""
    monkeypatch.setattr(job, "HR_TABLES", ("teams",))
    monkeypatch.setattr(job, "_jdbc_table", _fake_jdbc(spark, {"teams": {"empty": True}}))
    cfg, manifest, log = _setup(tmp_path, "pg-empty")
    with pytest.raises(ValueError, match="hr_source.teams has 0 data rows"):
        job.ingest_into_bronze(spark, cfg, log, manifest)
    assert not (tmp_path / "lake" / "bronze" / "hr" / "teams").exists()


# --- parity on a real PostgreSQL (local) ---------------------------------------------------
PARITY_SCHEMA = "hr_source_pytest"


def _unavailable(reason: str) -> None:
    """Skip the parity test locally; fail it when REQUIRE_POSTGRES is set (CI), so it cannot silently not run."""
    if os.environ.get("REQUIRE_POSTGRES", "").strip().lower() in {"1", "true", "yes"}:
        pytest.fail(f"REQUIRE_POSTGRES is set but {reason}", pytrace=False)
    pytest.skip(reason)


def _pg_env() -> dict:
    """POSTGRES_* from the environment. Skips (or fails, with REQUIRE_POSTGRES=1) when they are
    missing or PostgreSQL is not reachable."""
    needed = ("POSTGRES_NAME", "POSTGRES_USER", "POSTGRES_PASS")
    if not all(os.environ.get(k) for k in needed):
        _unavailable(f"{', '.join(needed)} are not all set (also POSTGRES_HOST/PORT) for the Postgres parity test")
    host, port = os.environ.get("POSTGRES_HOST", "localhost"), os.environ.get("POSTGRES_PORT", "5432")
    if not shutil.which("pg_isready"):
        _unavailable("pg_isready (postgresql-client) is not installed")
    if subprocess.run(["pg_isready", "-h", host, "-p", port, "-t", "5"],
                      capture_output=True).returncode != 0:
        _unavailable(f"PostgreSQL is not reachable at {host}:{port}")
    return {k: os.environ[k] for k in needed} | {"POSTGRES_HOST": host, "POSTGRES_PORT": port,
                                                  "POSTGRES_SCHEMA": PARITY_SCHEMA}


@pytest.mark.integration
@pytest.mark.postgres
def test_files_and_postgres_modes_give_identical_results(spark, tmp_path) -> None:
    """Load the real HR CSVs into Postgres with the loader script, run job.py in both modes:
    every step count and every Gold content hash must match."""
    from tests.test_pipeline_integration import EXPECTED, _latest_manifest, _run_job, _snapshot

    env = _pg_env()
    load = subprocess.run(["bash", "scripts/load_source_postgres.sh"], cwd=JOB_DIR,
                          env={**os.environ, **env}, capture_output=True, text=True, timeout=300)
    assert load.returncode == 0, load.stderr[-2000:]
    try:
        results = {}
        for mode, extra in (("files", {}), ("postgres", {**env, "SOURCE_MODE": "postgres"})):
            lake = tmp_path / mode
            code, _, tail = _run_job(lake, extra)
            assert code == 0, f"{mode} run failed:\n{tail}"
            results[mode] = (_latest_manifest(spark, lake), _snapshot(spark, lake))
    finally:
        subprocess.run(["psql", f"host={env['POSTGRES_HOST']} port={env['POSTGRES_PORT']} "
                                f"dbname={env['POSTGRES_NAME']} user={env['POSTGRES_USER']}",
                        "-qc", f"DROP SCHEMA IF EXISTS {PARITY_SCHEMA} CASCADE"],
                       env={**os.environ, "PGPASSWORD": env["POSTGRES_PASS"]}, capture_output=True)

    (m_files, s_files), (m_pg, s_pg) = results["files"], results["postgres"]
    assert m_pg["source_mode"] == "postgres" and m_pg["status"] == "SUCCESS"
    keys = ("status", "rows_read", "rows_written", "rows_quarantined", "rows_deduped",
            "rows_warned", "note")
    steps_files = {s["step"]: {k: s[k] for k in keys} for s in m_files["steps"]}
    steps_pg = {s["step"]: {k: s[k] for k in keys} for s in m_pg["steps"]}
    assert len(steps_pg) == 26 and steps_pg == steps_files
    for table, n in EXPECTED["hr"].items():
        assert steps_pg[f"bronze.hr.{table}"]["rows_written"] == n, table
        assert m_pg["source"][f"hr.{table}"]["rows"] == n, table
    assert s_pg["counts"] == s_files["counts"]
    assert s_pg["hashes"] == s_files["hashes"]  # all 10 Gold datasets, content-identical
