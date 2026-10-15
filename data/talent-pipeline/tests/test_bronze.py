"""Config and source reading, Bronze landing (strings + lineage) and the source guards (schema drift, empty sources, S3A)."""
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

import job
from tests.helpers import FIXTURES, JOB_DIR, MINI_SOURCE


def test_import_creates_no_spark_session() -> None:
    """Importing job in a fresh interpreter must not start Spark or a JVM."""
    code = (
        "import job; from pyspark import SparkContext; from pyspark.sql import SparkSession; "
        "assert SparkSession.getActiveSession() is None; "
        "assert SparkContext._jvm is None; print('ok')"
    )
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=JOB_DIR, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


def test_load_config_defaults() -> None:
    """With no env vars set, config falls back to local defaults."""
    cfg = job.load_config(env={})
    assert cfg.is_local is True
    assert cfg.lake_root == str(JOB_DIR / "lake")
    assert Path(cfg.source_dir).name == "data-engineering-performance-talent-management"


def test_bronze_learning_drops_blank_lines(spark) -> None:
    """3-line JSONL with a blank middle line → 2 rows, _line_no 1 and 3, path partitions kept."""
    lines = job.read_learning_lines(spark, str(FIXTURES / "learning_activity"))
    rows = (
        job.bronze_learning_rows(lines, "test-run", datetime.now(timezone.utc))
        .orderBy("_line_no")
        .collect()
    )
    assert [r["_line_no"] for r in rows] == [1, 3]
    assert {(r["year"], r["month"], r["day"]) for r in rows} == {("2024", "01", "01")}
    assert all(len(r["_record_hash"]) == 64 for r in rows)
    assert rows[0]["value"].startswith('{"event_id": "EV1"')


def test_bronze_hr_csv_strips_crlf(spark, tmp_path: Path) -> None:
    """CRLF CSV → all columns string, no trailing \\r in the last column."""
    (tmp_path / "t.csv").write_bytes(b'id,comment,score\r\n1,"a, b",5\r\n2,c,7\r\n')
    df = job.read_hr_csv(spark, str(tmp_path), "t")
    assert {t for _, t in df.dtypes} == {"string"}
    rows = df.orderBy("id").collect()
    assert [r["score"] for r in rows] == ["5", "7"]
    assert rows[0]["comment"] == "a, b"


def test_spark_conf_s3a_endpoint() -> None:
    """hadoop-aws only for s3a:// paths; regional endpoint only when AWS_REGION is also set."""
    local = job.spark_conf(job.load_config(env={}))
    assert "spark.jars.packages" not in local and "spark.hadoop.fs.s3a.endpoint" not in local

    cloud_env = {"SOURCE_DIR": "s3a://b/source", "LAKE_ROOT": "s3a://b/lake", "AWS_REGION": "ap-south-1"}
    cloud = job.spark_conf(job.load_config(env=cloud_env))
    assert cloud["spark.jars.packages"] == job.HADOOP_AWS_PACKAGE
    assert cloud["spark.hadoop.fs.s3a.endpoint"] == "s3.ap-south-1.amazonaws.com"

    no_region = job.spark_conf(job.load_config(env={**cloud_env, "AWS_REGION": ""}))
    assert "spark.jars.packages" in no_region and "spark.hadoop.fs.s3a.endpoint" not in no_region


def test_load_config_parsing() -> None:
    """IS_LOCAL truthy strings, trailing slash on LAKE_ROOT, empty AWS_REGION."""
    assert job.load_config(env={"IS_LOCAL": "false"}).is_local is False
    assert job.load_config(env={"IS_LOCAL": " YES "}).is_local is True
    assert job.load_config(env={"IS_LOCAL": "1"}).is_local is True
    cfg = job.load_config(
        env={"SOURCE_DIR": "s3a://b/source", "LAKE_ROOT": "s3a://b/lake/", "AWS_REGION": ""}
    )
    assert (cfg.source_dir, cfg.lake_root, cfg.aws_region) == (
        "s3a://b/source", "s3a://b/lake", None
    )


def test_spark_conf_s3a_source_only() -> None:
    """An s3a:// SOURCE_DIR alone is enough to add hadoop-aws."""
    cfg = job.Config(is_local=False, source_dir="s3a://b/source", lake_root="/tmp/lake")
    assert job.spark_conf(cfg)["spark.jars.packages"] == job.HADOOP_AWS_PACKAGE


def test_files_written_and_monthly_layout(spark, run) -> None:
    """One parquet file per step output; Bronze learning is partitioned by year/month only."""
    cfg, manifest, _ = run
    assert {s.step: s.files_written for s in manifest.steps} == {s.step: 1 for s in manifest.steps}

    bronze = Path(cfg.lake_root) / "bronze" / "learning_activity"
    assert sorted(str(p.relative_to(bronze)) for p in bronze.glob("year=*/month=*")) == [
        "year=2024/month=03"
    ]
    assert not list(bronze.rglob("day=*"))
    days = spark.read.parquet(str(bronze / "year=*" / "month=*")).select("day").distinct()
    assert sorted(r["day"] for r in days.collect()) == ["01", "02"]  # day=03 file is empty

    quarantine = Path(cfg.lake_root) / "quarantine" / "learning_events" / "run_id=mini-run"
    assert len(list(quarantine.glob("*.parquet"))) == 1


@pytest.fixture
def source(tmp_path: Path) -> Path:
    """Writable copy of the mini dataset."""
    return Path(shutil.copytree(MINI_SOURCE, tmp_path / "source"))


def _setup(source: Path, tmp_path: Path, run_id: str):
    cfg = job.Config(is_local=True, source_dir=str(source), lake_root=str(tmp_path / "lake"))
    manifest = job.RunManifest(run_id=run_id, is_local=True, source_dir=cfg.source_dir,
                               lake_root=cfg.lake_root)
    return cfg, manifest, job.get_logger(run_id)


def _drop_column(csv: Path, column: str) -> None:
    rows = [line.split(",") for line in csv.read_text().splitlines()]
    idx = rows[0].index(column)
    csv.write_text("\n".join(",".join(r[:idx] + r[idx + 1:]) for r in rows) + "\n")


def _assert_failed(manifest: job.RunManifest, step: str, text: str) -> None:
    last = manifest.steps[-1]
    assert (last.step, last.status) == (step, "FAILED") and text in last.note


def test_schema_drift_missing_column_fails(spark, source, tmp_path, monkeypatch) -> None:
    """A CSV header missing a schema.sql column stops the run and names the column."""
    monkeypatch.setattr(job, "HR_TABLES", ("employees",))
    _drop_column(source / "employees.csv", "email")
    cfg, manifest, log = _setup(source, tmp_path, "drift")
    with pytest.raises(ValueError, match=r"schema drift in employees.csv: missing columns \['email'\]"):
        job.ingest_into_bronze(spark, cfg, log, manifest)
    _assert_failed(manifest, "bronze.hr.employees", "missing columns ['email']")
    assert not (tmp_path / "lake" / "bronze").exists()  # nothing written before the check


def test_schema_drift_extra_column_warns_and_keeps(spark, source, tmp_path, monkeypatch,
                                                   capsys) -> None:
    """An extra CSV column only warns; Bronze keeps it and the manifest lists it."""
    monkeypatch.setattr(job, "HR_TABLES", ("teams",))
    csv = source / "teams.csv"
    lines = csv.read_text().splitlines()
    csv.write_text("\n".join([lines[0] + ",region"] + [ln + ",EMEA" for ln in lines[1:]]) + "\n")
    cfg, manifest, log = _setup(source, tmp_path, "extra")
    job.ingest_into_bronze(spark, cfg, log, manifest)

    out = capsys.readouterr().out
    assert "WARNING" in out and "schema drift in teams.csv: extra columns ['region'] kept" in out
    assert manifest.source["hr.teams"]["extra_columns"] == ["region"]
    assert [s.status for s in manifest.steps] == ["SUCCESS", "SUCCESS"]
    bronze = spark.read.parquet(f"{cfg.lake_root}/bronze/hr/teams")
    assert "region" in bronze.columns and bronze.count() == 3


def test_header_only_csv_fails(spark, source, tmp_path, monkeypatch) -> None:
    """A CSV with a header but 0 data rows stops the run before anything is written."""
    monkeypatch.setattr(job, "HR_TABLES", ("teams",))
    csv = source / "teams.csv"
    csv.write_text(csv.read_text().splitlines()[0] + "\n")
    cfg, manifest, log = _setup(source, tmp_path, "header-only")
    with pytest.raises(ValueError, match="teams.csv has 0 data rows"):
        job.ingest_into_bronze(spark, cfg, log, manifest)
    _assert_failed(manifest, "bronze.hr.teams", "0 data rows")
    assert not (tmp_path / "lake" / "bronze" / "hr" / "teams").exists()


def test_zero_byte_csv_fails(spark, source, tmp_path, monkeypatch) -> None:
    """A 0-byte CSV is reported as empty, not as a missing header."""
    monkeypatch.setattr(job, "HR_TABLES", ("teams",))
    (source / "teams.csv").write_bytes(b"")
    cfg, manifest, log = _setup(source, tmp_path, "zero-bytes")
    with pytest.raises(ValueError, match=r"teams.csv is empty \(0 bytes\)"):
        job.ingest_into_bronze(spark, cfg, log, manifest)
    _assert_failed(manifest, "bronze.hr.teams", "empty (0 bytes)")


def test_no_learning_files_fails(spark, source, tmp_path, monkeypatch) -> None:
    """No learning JSONL files at all stops the run."""
    monkeypatch.setattr(job, "HR_TABLES", ())
    shutil.rmtree(source / "learning_activity")
    cfg, manifest, log = _setup(source, tmp_path, "no-learning")
    with pytest.raises(ValueError, match="no learning files under"):
        job.ingest_into_bronze(spark, cfg, log, manifest)
    _assert_failed(manifest, "bronze.learning_activity", "no learning files")


def test_only_empty_learning_files_fails(spark, source, tmp_path, monkeypatch) -> None:
    """Learning files that exist but hold 0 lines stop the run (Bronze rows_read must be > 0)."""
    monkeypatch.setattr(job, "HR_TABLES", ())
    for f in (source / "learning_activity").rglob("*.jsonl"):
        f.write_bytes(b"")
    cfg, manifest, log = _setup(source, tmp_path, "empty-learning")
    with pytest.raises(ValueError, match="contain 0 lines"):
        job.ingest_into_bronze(spark, cfg, log, manifest)
    _assert_failed(manifest, "bronze.learning_activity", "contain 0 lines")
    assert manifest.source["learning_activity"] == {"files": 3, "bytes": 0}


def test_main_schema_drift_writes_failed_manifest(spark, source, tmp_path, monkeypatch) -> None:
    """End to end: drift → exit 1 and a FAILED manifest whose step note names the column."""
    _drop_column(source / "employees.csv", "hire_date")
    lake = tmp_path / "lake"
    monkeypatch.setenv("IS_LOCAL", "true")
    monkeypatch.setenv("SOURCE_DIR", str(source))
    monkeypatch.setenv("LAKE_ROOT", str(lake))
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.setattr(spark, "stop", lambda: None)  # keep the shared session alive

    assert job.main() == 1
    [part] = lake.glob("_runs/run_id=*/part-*.txt")
    manifest = json.loads(part.read_text())
    assert manifest["status"] == "FAILED" and manifest["spark_version"] == "3.5.0"
    assert manifest["steps"][-1]["note"] == (
        "schema drift in employees.csv: missing columns ['hire_date']"
    )


def test_spark_conf_s3a_retries_only_for_s3a() -> None:
    """S3A retry/backoff settings are added for s3a:// paths and never for local runs."""
    local = job.spark_conf(job.Config(is_local=True, source_dir="/src", lake_root="/lake"))
    assert not any("fs.s3a" in k for k in local)
    cloud = job.spark_conf(job.Config(is_local=False, source_dir="s3a://b/src",
                                      lake_root="s3a://b/lake"))
    assert {k: cloud[k] for k in job.S3A_RETRY_CONF} == {
        "spark.hadoop.fs.s3a.attempts.maximum": "10",
        "spark.hadoop.fs.s3a.retry.limit": "7",
        "spark.hadoop.fs.s3a.retry.interval": "500ms",
    }


# --- lineage -----------------------------------------------------------------------------
def test_bronze_hr_lineage_columns_are_present_and_non_null(spark, run) -> None:
    """Every Bronze HR row carries the run id, ingest time, source system and source file."""
    cfg, manifest, _ = run
    for table in job.HR_TABLES:
        df = spark.read.parquet(f"{cfg.lake_root}/bronze/hr/{table}")
        assert df.count() > 0, table
        for col in ("_run_id", "_ingested_at", "_source_system", "_source_file"):
            assert col in df.columns, (table, col)
            assert df.where(f"{col} IS NULL").count() == 0, (table, col)
        row = df.first()
        assert (row["_run_id"], row["_source_system"]) == (manifest.run_id, "file"), table
        assert row["_source_file"].endswith(f"{table}.csv"), table


def test_bronze_learning_lineage_columns_are_present_and_non_null(spark, run) -> None:
    """Every Bronze learning line keeps run id, ingest time, source file, line number and hash."""
    cfg, manifest, _ = run
    df = spark.read.parquet(f"{cfg.lake_root}/bronze/learning_activity/year=*/month=*")
    assert df.count() == 10
    for col in ("_run_id", "_ingested_at", "_source_file", "_line_no", "_record_hash"):
        assert col in df.columns, col
        assert df.where(f"{col} IS NULL").count() == 0, col
    assert {r["_run_id"] for r in df.select("_run_id").distinct().collect()} == {manifest.run_id}
    assert df.where("length(_record_hash) != 64").count() == 0  # sha256 hex
    assert df.where("_source_file NOT LIKE '%.jsonl'").count() == 0
