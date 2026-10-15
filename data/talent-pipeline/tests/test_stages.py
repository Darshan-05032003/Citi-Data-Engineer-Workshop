"""In-process stage tests: Bronze → Silver → Gold on the hand-made mini dataset.

tests/fixtures/mini_source/ follows the real file formats; every expected count below is
computed by hand from those files (see docs/test_evidence.md, "Unit coverage").
"""
import json
from pathlib import Path

import pytest
from pyspark.sql import functions as F

import job

MINI_SOURCE = Path(__file__).resolve().parent / "fixtures" / "mini_source"

# step -> (read, written, quarantined, deduped, warned)
EXPECTED_STEPS = {
    "bronze.hr.employees": (6, 6, 0, 0, 0),
    "bronze.hr.teams": (3, 3, 0, 0, 0),
    "bronze.hr.competencies": (3, 3, 0, 0, 0),
    "bronze.hr.projects": (2, 2, 0, 0, 0),
    "bronze.hr.performance_reviews": (12, 12, 0, 0, 0),
    "bronze.hr.competency_assessments": (8, 8, 0, 0, 0),
    "bronze.hr.talent_review": (6, 6, 0, 0, 0),
    "bronze.learning_activity": (10, 10, 0, 0, 0),
    "silver.hr.teams": (3, 3, 0, 0, 0),
    "silver.hr.competencies": (3, 3, 0, 0, 0),
    "silver.hr.employees": (6, 6, 0, 0, 0),
    "silver.hr.projects": (2, 2, 0, 0, 0),
    "silver.hr.performance_reviews": (12, 11, 1, 0, 1),
    "silver.hr.competency_assessments": (8, 7, 1, 0, 1),
    "silver.hr.talent_review": (6, 6, 0, 0, 0),
    "silver.learning_events": (10, 6, 3, 1, 2),
}
EXPECTED_GOLD_ROWS = {
    "employee_performance_history": 11,
    "employee_performance_summary": 6,
    "employee_competency_gaps": 7,
    "team_performance_outcomes": 3,
    "competency_distribution": 6,
    "learning_progress": 5,
    "learning_summary": 6,
    "promotion_readiness": 6,
    "attrition_risk_context": 6,
    "attrition_by_trend": 5,
}


@pytest.fixture(scope="module")
def run(spark, tmp_path_factory):
    """Run the three stages once in-process on the mini dataset and write the manifest."""
    lake = str(tmp_path_factory.mktemp("stages") / "lake")
    cfg = job.Config(is_local=True, source_dir=str(MINI_SOURCE), lake_root=lake)
    manifest = job.RunManifest(
        run_id="mini-run", is_local=True, source_dir=cfg.source_dir, lake_root=lake
    )
    manifest.spark_version = spark.version  # as main() does after starting Spark
    log = job.get_logger(manifest.run_id)
    job.ingest_into_bronze(spark, cfg, log, manifest)
    job.transform_to_silver(spark, cfg, log, manifest)
    job.aggregate_to_gold(spark, cfg, log, manifest)
    manifest.finish("SUCCESS")
    manifest_dir = job.write_manifest(spark, cfg, manifest)
    return cfg, manifest, manifest_dir


def _steps(manifest: job.RunManifest) -> dict[str, job.StepMetrics]:
    return {s.step: s for s in manifest.steps}


def _gold(spark, cfg: job.Config, name: str):
    return spark.read.parquet(f"{cfg.lake_root}/gold/{name}")


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


def test_silver_learning_rules(run) -> None:
    """Quarantine and warnings are attributed to the right rules."""
    _, manifest, _ = run
    note = json.loads(_steps(manifest)["silver.learning_events"].note)
    assert note["quarantine_by_rule"] == {
        "learning_events.employee_id.not_null": 1,
        "learning_events.event_ts.invalid": 1,
        "learning_events.raw.malformed_json": 1,
    }
    assert note["warnings_by_rule"] == {
        "learning_events.event_text.notes_used": 1,
        "learning_events.event_ts.before_hire": 1,
    }


def test_quarantine_folders(spark, run) -> None:
    """Quarantine is written only for entities with error rows, with all reasons kept."""
    cfg, _, _ = run
    root = Path(cfg.lake_root) / "quarantine"
    assert sorted(p.name for p in root.iterdir()) == [
        "competency_assessments", "learning_events", "performance_reviews",
    ]
    reviews = spark.read.parquet(str(root / "performance_reviews" / "run_id=mini-run"))
    assert [r["rule_ids"] for r in reviews.collect()] == [
        ["performance_reviews.rating.domain"]
    ]
    assessments = spark.read.parquet(str(root / "competency_assessments" / "run_id=mini-run"))
    assert [r["rule_ids"] for r in assessments.collect()] == [
        ["competency_assessments.employee_id.fk"]
    ]


@pytest.mark.parametrize("name", sorted(EXPECTED_GOLD_ROWS))
def test_gold_rows_and_grain(spark, run, name: str) -> None:
    """Each Gold dataset has the expected rows and one row per grain key."""
    cfg, manifest, _ = run
    gold = _gold(spark, cfg, name)
    keys = job.GOLD_GRAINS[name]
    assert gold.count() == EXPECTED_GOLD_ROWS[name]
    assert gold.select(*keys).distinct().count() == EXPECTED_GOLD_ROWS[name]
    assert _steps(manifest)[f"gold.{name}"].rows_written == EXPECTED_GOLD_ROWS[name]


def test_gold_business_values(spark, run) -> None:
    """Key Gold answers on the mini dataset, computed by hand."""
    cfg, _, _ = run
    trend = {
        r["derived_rating_trend"]: r["count"]
        for r in _gold(spark, cfg, "employee_performance_summary")
        .groupBy("derived_rating_trend").count().collect()
    }
    assert trend == {"improving": 3, "declining": 1, "stable": 1, None: 1}

    gaps = {
        r["derived_gap"]: r["count"]
        for r in _gold(spark, cfg, "employee_competency_gaps").groupBy("derived_gap").count().collect()
    }
    assert gaps == {-1: 1, 0: 3, 1: 1, 2: 2}

    g3 = _gold(spark, cfg, "promotion_readiness")
    assert g3.where("derived_high_potential_ready_now").count() == 2
    assert g3.where("critical_gap_count > 0").count() == 2

    g4 = _gold(spark, cfg, "learning_progress")
    assert g4.where("dq_completion_without_enrollment").count() == 1

    teams = {r["team_id"]: r for r in _gold(spark, cfg, "team_performance_outcomes").collect()}
    assert [t for t, r in teams.items() if r["has_projects"]] == [1]
    assert teams[1]["project_count"] == 2 and teams[1]["avg_rating_latest"] == 3.5
    assert teams[3]["avg_rating_latest"] == 5.0  # emp6's H2 rating 7 was quarantined

    dist = _gold(spark, cfg, "competency_distribution")
    assert dist.agg(F.sum("assessed_count")).first()[0] == 7

    by_trend = {
        (r["attrition_risk"], r["derived_rating_trend"]): r["headcount"]
        for r in _gold(spark, cfg, "attrition_by_trend").collect()
    }
    assert by_trend[("Low", "improving")] == 2 and sum(by_trend.values()) == 6


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


def test_read_latest_bronze_hr_picks_newest(spark, tmp_path: Path) -> None:
    """With two ingest_date partitions the newest one is read."""
    cfg = job.Config(is_local=True, source_dir="unused", lake_root=str(tmp_path))
    base = tmp_path / "bronze" / "hr" / "teams"
    spark.createDataFrame([("1",)], "team_id string").write.parquet(
        str(base / "ingest_date=2026-01-01")
    )
    spark.createDataFrame([("1",), ("2",)], "team_id string").write.parquet(
        str(base / "ingest_date=2026-02-01")
    )
    df = job.read_latest_bronze_hr(spark, cfg, "teams")
    assert df.count() == 2
    assert {r["_bronze_ingest_date"] for r in df.collect()} == {"2026-02-01"}


def test_read_latest_bronze_hr_missing(spark, tmp_path: Path) -> None:
    """No Bronze partitions is an error, not an empty Silver table."""
    cfg = job.Config(is_local=True, source_dir="unused", lake_root=str(tmp_path))
    with pytest.raises(FileNotFoundError, match="no bronze partitions for teams"):
        job.read_latest_bronze_hr(spark, cfg, "teams")


def test_write_gold_rejects_duplicate_keys(spark, tmp_path: Path) -> None:
    """A Gold write with a duplicate grain key fails the run."""
    cfg = job.Config(is_local=True, source_dir="unused", lake_root=str(tmp_path))
    manifest = job.RunManifest(run_id="dup", is_local=True, source_dir="unused",
                               lake_root=str(tmp_path))
    df = spark.createDataFrame([(1,), (1,)], "employee_id int")
    with pytest.raises(ValueError, match="1 duplicate"):
        job.write_gold(spark, cfg, job.get_logger("dup"), manifest,
                       "employee_performance_summary", df)


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
