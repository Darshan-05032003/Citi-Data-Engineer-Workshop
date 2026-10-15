"""Silver: typing, DQ errors to quarantine, dedupe, warnings, reading the latest Bronze partition."""
import json
from pathlib import Path

import pytest

import job
from tests.helpers import FIXTURES, _steps


def test_silver_learning_pipeline_on_fixture(spark) -> None:
    """malformed → quarantine, duplicate → deduped, null employee / bad date → quarantine,
    notes-only → kept with a warning; read = malformed + deduped + quarantined + written."""
    text = (FIXTURES / "learning_events.jsonl").read_text().splitlines()
    bronze = spark.createDataFrame(
        [(line, i, "fixture.jsonl", "h", None) for i, line in enumerate(text, start=1)],
        "value string, _line_no int, _source_file string, _record_hash string, "
        "_ingested_at timestamp",
    )
    employees = spark.createDataFrame(
        [(1, job.datetime(2020, 1, 1).date())], "employee_id int, hire_date date"
    )

    parsed, malformed = job.apply_rules(job.parse_learning(bronze), "learning_events_raw", "t")
    kept, deduped = job.dedupe_events(parsed)
    valid, quarantine = job.apply_rules(
        job.conform_learning(kept, employees), "learning_events", "t"
    )

    assert [r["_line_no"] for r in malformed.collect()] == [7]
    assert malformed.collect()[0]["rule_ids"] == ["learning_events.raw.malformed_json"]
    assert deduped == 1
    q = {r["event_id"]: r["rule_ids"] for r in quarantine.collect()}
    assert q == {
        "E3": ["learning_events.employee_id.not_null"],
        "E4": ["learning_events.event_ts.invalid"],
    }
    v = {r["event_id"]: r for r in valid.collect()}
    assert sorted(v) == ["E1", "E2", "E5"]
    assert v["E5"]["event_text"] == "from notes" and v["E5"]["text_source_field"] == "notes"
    assert v["E5"]["dq_warnings"] == ["learning_events.event_text.notes_used"]
    assert v["E1"]["dq_warnings"] == [] and v["E2"]["score"] == 80.0
    assert len(text) == malformed.count() + deduped + quarantine.count() + valid.count()


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


# --- lineage -----------------------------------------------------------------------------
def test_silver_lineage_columns_are_present_and_non_null(spark, run) -> None:
    """Silver stamps this run's id and keeps the Bronze source lineage on every row."""
    cfg, manifest, _ = run
    hr_cols = ("_run_id", "_ingested_at", "_source_system", "_source_file")
    learning_cols = ("_run_id", "_ingested_at", "_source_file", "_line_no", "_record_hash")
    tables = {t: (f"{cfg.lake_root}/silver/{t}", hr_cols) for t in job.HR_SILVER_ORDER}
    tables["learning_events"] = (
        f"{cfg.lake_root}/silver/learning_events/event_year=*/event_month=*", learning_cols)
    for name, (path, cols) in tables.items():
        df = spark.read.parquet(path)
        assert df.count() > 0, name
        for col in cols:
            assert col in df.columns, (name, col)
            assert df.where(f"{col} IS NULL").count() == 0, (name, col)
        assert {r["_run_id"] for r in df.select("_run_id").distinct().collect()} == {manifest.run_id}, name
