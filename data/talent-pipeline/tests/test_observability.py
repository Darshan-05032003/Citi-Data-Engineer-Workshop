"""Run manifest, per-step metrics, reconciliation and the failure path of main()."""
import json
from pathlib import Path

import job
from tests.helpers import EXPECTED_STEPS, MINI_SOURCE, _steps
from tests.test_postgres import PG_ENV, _fake_jdbc, _setup


def test_write_manifest_round_trip(spark, lake_root: str) -> None:
    """A manifest written through Spark reads back identical."""
    cfg = job.Config(is_local=True, source_dir="/tmp/src", lake_root=lake_root)
    manifest = job.RunManifest(
        run_id="test-run", is_local=True, source_dir=cfg.source_dir, lake_root=lake_root
    )
    manifest.add_step(job.StepMetrics(step="bronze", rows_read=3, rows_written=2, rows_deduped=1))
    manifest.finish("SUCCESS")

    path = job.write_manifest(spark, cfg, manifest)

    assert path.endswith("/_runs/run_id=test-run")
    rows = [r.value for r in spark.read.text(path).collect()]
    assert len(rows) == 1
    assert json.loads(rows[0]) == manifest.to_dict()


def test_bronze_and_silver_step_counts(run) -> None:
    """Every Bronze/Silver step matches the hand-computed counts."""
    _, manifest, _ = run
    steps = _steps(manifest)
    for name, expected in EXPECTED_STEPS.items():
        s = steps[name]
        actual = (s.rows_read, s.rows_written, s.rows_quarantined, s.rows_deduped, s.rows_warned)
        assert actual == expected, name
    assert steps["bronze.learning_activity"].note == (
        "files=3 zero_line_files=1 blank_lines_dropped=0"
    )


def test_manifest_reconciles_and_round_trips(run) -> None:
    """26 steps, each reconciled; the written manifest equals the in-memory one."""
    _, manifest, manifest_dir = run
    assert manifest.status == "SUCCESS" and len(manifest.steps) == 26
    for s in manifest.steps:
        if s.step.startswith(("bronze.hr", "silver")):
            assert s.rows_read == s.rows_written + s.rows_quarantined + s.rows_deduped, s.step
    parts = list(Path(manifest_dir).glob("part-*.txt"))
    assert len(parts) == 1
    assert json.loads(parts[0].read_text()) == json.loads(
        json.dumps(manifest.to_dict(), sort_keys=True)
    )


def test_manifest_observability_fields(run) -> None:
    """code_version, spark_version, source sizes and per-step timing are recorded."""
    _, manifest, _ = run
    data = manifest.to_dict()
    assert data["code_version"] == job.code_version() and len(data["code_version"]) == 12
    assert data["spark_version"] == "3.5.0"
    for table in job.HR_TABLES:
        assert data["source"][f"hr.{table}"] == {
            "bytes": (MINI_SOURCE / f"{table}.csv").stat().st_size, "extra_columns": [],
        }
    jsonl = list((MINI_SOURCE / "learning_activity").rglob("*.jsonl"))
    assert data["source"]["learning_activity"] == {
        "files": 3, "bytes": sum(p.stat().st_size for p in jsonl),
    }
    for s in data["steps"]:
        assert s["duration_s"] > 0 and s["rows_per_s"] > 0, s["step"]
        rows = s["rows_written"] if s["step"].startswith("gold.") else s["rows_read"]
        assert s["rows_per_s"] == round(rows / s["duration_s"], 1), s["step"]


def test_main_failure_records_failed_manifest(spark, tmp_path: Path, monkeypatch) -> None:
    """main() on a missing SOURCE_DIR exits 1 and still writes a FAILED manifest."""
    lake = tmp_path / "lake"
    monkeypatch.setenv("IS_LOCAL", "true")
    monkeypatch.setenv("SOURCE_DIR", str(tmp_path / "missing"))
    monkeypatch.setenv("LAKE_ROOT", str(lake))
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.setattr(spark, "stop", lambda: None)  # keep the shared session alive

    assert job.main() == 1
    parts = list(lake.glob("_runs/run_id=*/part-*.txt"))
    assert len(parts) == 1
    manifest = json.loads(parts[0].read_text())
    assert manifest["status"] == "FAILED"
    [failed] = manifest["steps"]  # the source check records the failing step and reason
    assert failed["step"] == "bronze.hr.employees" and failed["status"] == "FAILED"
    assert failed["note"].startswith("source file not found:")


# --- lineage and logs ---------------------------------------------------------------------
def test_manifest_lineage_fields_are_present_and_non_null(run) -> None:
    """The manifest identifies the run, its code, its engine and its source, in memory and on disk."""
    cfg, manifest, manifest_dir = run
    written = json.loads(next(Path(manifest_dir).glob("part-*.txt")).read_text())
    for data in (manifest.to_dict(), written):
        for key in ("run_id", "started_at", "ended_at", "duration_s", "status", "code_version",
                    "spark_version", "source_dir", "lake_root", "source_mode"):
            assert data[key] is not None, key
        assert len(data["code_version"]) == 12 and data["code_version"] == job.code_version()
        assert data["run_id"] == "mini-run" and data["source_dir"] == cfg.source_dir


def test_every_step_logs_its_counts(run, run_log) -> None:
    """Each manifest step has a log line with the same counts, plus files_written and duration."""
    _, manifest, _ = run
    messages = [m for _, m in run_log]
    for s in manifest.steps:
        if s.step == "bronze.learning_activity":
            expect = f"bronze: learning_activity files=3 lines={s.rows_read} written={s.rows_written}"
        elif s.step.startswith("bronze.hr."):
            expect = f"bronze: hr.{s.step[10:]} read={s.rows_read} written={s.rows_written}"
        elif s.step == "silver.learning_events":
            expect = (f"silver: learning_events read={s.rows_read} = deduped {s.rows_deduped} + "
                      f"quarantined {s.rows_quarantined} + written {s.rows_written}")
        elif s.step.startswith("silver.hr."):
            expect = (f"silver: hr.{s.step[10:]} read={s.rows_read} written={s.rows_written} "
                      f"quarantined={s.rows_quarantined} warned={s.rows_warned}")
        else:
            expect = f"gold: {s.step[5:]} rows={s.rows_written}"
        hits = [m for m in messages if expect in m]
        assert len(hits) == 1, (s.step, expect)
        assert f"files_written={s.files_written}" in hits[0] and "duration_s=" in hits[0], s.step


def test_run_log_has_only_the_expected_warning(run_log) -> None:
    """The mini run logs no errors; its only warning is the empty learning file."""
    assert [lvl for lvl, _ in run_log if lvl == "ERROR"] == []
    warnings = [m for lvl, m in run_log if lvl == "WARNING"]
    assert len(warnings) == 1 and "file has zero lines" in warnings[0] and "day=03" in warnings[0]


def test_postgres_password_is_never_logged_on_success(spark, tmp_path, monkeypatch, caplog, capsys) -> None:
    """A successful postgres-mode ingest logs its progress but never the password value."""
    monkeypatch.setattr(job, "HR_TABLES", ("teams", "employees"))
    monkeypatch.setattr(job, "_jdbc_table", _fake_jdbc(spark))
    cfg, manifest, log = _setup(tmp_path, "pw-ok")
    log.addHandler(caplog.handler)  # the job logger does not propagate to the root logger
    job.ingest_into_bronze(spark, cfg, log, manifest)

    secret = PG_ENV["POSTGRES_PASS"]
    assert cfg.pg_password == secret and "postgres source" in caplog.text  # it was set, and logging happened
    out = capsys.readouterr()
    assert secret not in caplog.text and secret not in out.out + out.err
    assert secret not in json.dumps(manifest.to_dict())


def test_postgres_password_is_never_logged_when_main_fails(spark, tmp_path, monkeypatch, caplog, capsys) -> None:
    """main() with an unreachable database retries, fails and exits 1; no log line, traceback or
    manifest contains the password."""
    secret = "pw-for-the-log-test-9f3a"
    lake = tmp_path / "lake"
    for key, value in {"IS_LOCAL": "true", "SOURCE_MODE": "postgres", "SOURCE_DIR": str(MINI_SOURCE),
                       "LAKE_ROOT": str(lake), "POSTGRES_NAME": "db", "POSTGRES_USER": "u",
                       "POSTGRES_PASS": secret}.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(job.time, "sleep", lambda _seconds: None)  # skip the 2/4/8/16 s backoff
    monkeypatch.setattr(job, "_jdbc_table", _fake_jdbc(spark, fail_probe=True))
    monkeypatch.setattr(spark, "stop", lambda: None)  # keep the shared session alive
    original_get_logger = job.get_logger

    def get_logger_with_caplog(run_id):
        log = original_get_logger(run_id)
        log.addHandler(caplog.handler)
        return log

    monkeypatch.setattr(job, "get_logger", get_logger_with_caplog)

    assert job.main() == 1
    assert "cannot connect to postgres" in caplog.text and "attempt 1/5" in caplog.text
    out = capsys.readouterr()
    manifest_text = next(lake.glob("_runs/run_id=*/part-*.txt")).read_text()
    assert json.loads(manifest_text)["status"] == "FAILED"
    for text in (caplog.text, out.out, out.err, manifest_text):
        assert secret not in text
