"""Source guards and S3A settings: schema drift, empty sources, retries.

Each test edits a temp copy of tests/fixtures/mini_source/. HR_TABLES is narrowed to the
table under test so the suite stays fast; the guard code is the same for every table.
"""
import json
import shutil
from pathlib import Path

import pytest

import job

MINI_SOURCE = Path(__file__).resolve().parent / "fixtures" / "mini_source"


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
