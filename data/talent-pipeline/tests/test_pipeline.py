"""Integration tests: run the real pipeline twice into a temp lake (PLAN.md §6).

job.py runs in a subprocess, exactly as in production. Running main() in-process would
reuse the test SparkSession (ignoring the job's config) and stop it at the end.
"""
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import pytest
from pyspark.sql import functions as F

import job

pytestmark = pytest.mark.integration

JOB_DIR = Path(job.__file__).resolve().parent
TECH_COLS = ("_gold_run_id", "_built_at", "_run_id")

# Mirrors the expected numbers in docs/PLAN.md §2.
EXPECTED = {
    "hr": {
        "employees": 2000, "teams": 100, "competencies": 40, "projects": 33,
        "performance_reviews": 4000, "competency_assessments": 4000, "talent_review": 2000,
    },
    "learning_files": 783,
    "learning_lines": 29868,
    "malformed": 13,
    "deduped": 152,
    "null_employee": 76,
    "invalid_date": 53,
    "learning_valid": 29574,
    "warn_reviews": 666,
    "warn_assessments": 234,
    "warn_notes": 38,
    "warn_before_hire": 5161,
    "trend": {"improving": 898, "stable": 872, "declining": 230},
    "gaps": {-1: 1480, 0: 1527, 1: 668, 2: 325},
    "critical": 325,
    "hipo_ready_now": 357,
    "risk": {"Low": 1077, "Medium": 612, "High": 311},
    "teams_with_projects": 27,
    "project_count": 33,
    "completion_without_enrollment": 3255,
}


def _run_job(lake: Path) -> tuple[int, float, str]:
    env = {**os.environ, "LAKE_ROOT": str(lake), "IS_LOCAL": "true"}
    env.pop("SPARK_HOME", None)
    t0 = time.monotonic()
    proc = subprocess.run(
        [sys.executable, "job.py"], cwd=JOB_DIR, env=env, capture_output=True, text=True,
        timeout=900,
    )
    return proc.returncode, round(time.monotonic() - t0, 1), proc.stdout[-3000:]


def _latest_manifest(spark, lake: Path) -> dict:
    runs = sorted(p for p in (lake / "_runs").glob("run_id=*"))
    return json.loads(spark.read.text(str(runs[-1])).first()["value"])


def _silver_frames(spark, lake: Path) -> dict:
    frames = {t: spark.read.parquet(str(lake / "silver" / t)) for t in job.HR_TABLES}
    frames["learning_events"] = spark.read.parquet(
        str(lake / "silver" / "learning_events" / "event_year=*" / "event_month=*")
    )
    return frames


def _gold_frames(spark, lake: Path) -> dict:
    return {n: spark.read.parquet(str(lake / "gold" / n)) for n in job.GOLD_GRAINS}


def _content_hash(df) -> tuple:
    """Order-independent fingerprint of a dataset, ignoring run/time columns."""
    data = df.drop(*[c for c in TECH_COLS if c in df.columns])
    row_hash = F.xxhash64(*[F.col(c) for c in sorted(data.columns)]).cast("decimal(38,0)")
    r = data.agg(F.count("*").alias("n"), F.sum(row_hash).alias("h")).first()
    return r["n"], str(r["h"])


def _duplicate_keys(df, keys) -> int:
    return df.count() - df.select(*keys).distinct().count()


def _snapshot(spark, lake: Path) -> dict:
    silver, gold = _silver_frames(spark, lake), _gold_frames(spark, lake)
    silver_keys = {**{t: (job.HR_PK[t],) for t in job.HR_TABLES}, "learning_events": ("event_id",)}
    return {
        "manifest": _latest_manifest(spark, lake),
        "counts": {f"silver.{n}": df.count() for n, df in silver.items()}
        | {f"gold.{n}": df.count() for n, df in gold.items()},
        "dupes": {f"silver.{n}": _duplicate_keys(df, silver_keys[n]) for n, df in silver.items()}
        | {f"gold.{n}": _duplicate_keys(df, job.GOLD_GRAINS[n]) for n, df in gold.items()},
        "hashes": {n: _content_hash(df) for n, df in gold.items()},
    }


@pytest.fixture(scope="module")
def runs(spark, tmp_path_factory):
    """Run the pipeline twice on the same temp LAKE_ROOT; snapshot after each run."""
    lake = tmp_path_factory.mktemp("lake")
    result = {}
    for i in (1, 2):
        code, secs, tail = _run_job(lake)
        assert code == 0, f"run {i} failed:\n{tail}"
        result[i] = {"exit": code, "seconds": secs, **_snapshot(spark, lake)}
    result["lake"] = lake
    return result


def _steps(manifest: dict) -> dict:
    return {s["step"]: s for s in manifest["steps"]}


def test_runs_succeed(runs) -> None:
    for i in (1, 2):
        assert runs[i]["exit"] == 0
        assert runs[i]["manifest"]["status"] == "SUCCESS"
    assert runs[1]["manifest"]["run_id"] != runs[2]["manifest"]["run_id"]


def test_expected_numbers(runs, spark) -> None:
    steps = _steps(runs[1]["manifest"])
    exp = EXPECTED

    # Bronze
    for t, n in exp["hr"].items():
        assert steps[f"bronze.hr.{t}"]["rows_written"] == n, t
    bl = steps["bronze.learning_activity"]
    assert bl["rows_read"] == exp["learning_lines"]
    assert bl["rows_written"] == exp["learning_lines"]
    assert int(re.search(r"files=(\d+)", bl["note"]).group(1)) == exp["learning_files"]

    # Silver HR
    for t, n in exp["hr"].items():
        s = steps[f"silver.hr.{t}"]
        assert (s["rows_read"], s["rows_written"], s["rows_quarantined"]) == (n, n, 0), t
    assert steps["silver.hr.performance_reviews"]["rows_warned"] == exp["warn_reviews"]
    assert steps["silver.hr.competency_assessments"]["rows_warned"] == exp["warn_assessments"]

    # Silver learning
    sl = steps["silver.learning_events"]
    note = json.loads(sl["note"])
    q, w = note["quarantine_by_rule"], note["warnings_by_rule"]
    assert sl["rows_read"] == exp["learning_lines"]
    assert sl["rows_deduped"] == exp["deduped"]
    assert sl["rows_written"] == exp["learning_valid"]
    assert q["learning_events.raw.malformed_json"] == exp["malformed"]
    assert q["learning_events.employee_id.not_null"] == exp["null_employee"]
    assert q["learning_events.event_ts.invalid"] == exp["invalid_date"]
    assert w["learning_events.event_text.notes_used"] == exp["warn_notes"]
    assert w["learning_events.event_ts.before_hire"] == exp["warn_before_hire"]

    # Gold
    gold = _gold_frames(spark, runs["lake"])

    def dist(name, col):
        return {r[0]: r[1] for r in gold[name].groupBy(col).count().collect()}

    def total(name, col):
        return gold[name].agg(F.sum(col)).first()[0]

    assert runs[1]["counts"]["gold.employee_performance_history"] == 4000
    assert runs[1]["counts"]["gold.employee_performance_summary"] == 2000
    assert dist("employee_performance_summary", "derived_rating_trend") == exp["trend"]
    assert runs[1]["counts"]["gold.employee_competency_gaps"] == 4000
    assert dist("employee_competency_gaps", "derived_gap") == exp["gaps"]
    assert dist("employee_competency_gaps", "derived_gap_severity")["critical"] == exp["critical"]
    assert runs[1]["counts"]["gold.promotion_readiness"] == 2000
    assert dist("promotion_readiness", "derived_high_potential_ready_now")[True] == exp["hipo_ready_now"]
    assert total("learning_progress", "total_events") == exp["learning_valid"]
    assert dist("learning_progress", "dq_completion_without_enrollment")[True] == exp["completion_without_enrollment"]
    assert runs[1]["counts"]["gold.learning_summary"] == 2000
    assert runs[1]["counts"]["gold.attrition_risk_context"] == 2000
    assert dist("attrition_risk_context", "attrition_risk") == exp["risk"]
    assert total("attrition_by_trend", "headcount") == 2000
    assert runs[1]["counts"]["gold.team_performance_outcomes"] == 100
    assert total("team_performance_outcomes", "project_count") == exp["project_count"]
    assert dist("team_performance_outcomes", "has_projects")[True] == exp["teams_with_projects"]
    assert total("team_performance_outcomes", "headcount") == exp["hr"]["employees"]
    assert total("competency_distribution", "assessed_count") == exp["hr"]["competency_assessments"]


def test_reconciliation(runs) -> None:
    """bronze.* / silver.*: read = written + quarantined + deduped. gold.*: rows written."""
    for i in (1, 2):
        for name, s in _steps(runs[i]["manifest"]).items():
            if name.startswith(("bronze.", "silver.")):
                assert s["rows_read"] == (
                    s["rows_written"] + s["rows_quarantined"] + s["rows_deduped"]
                ), f"run {i} {name}: {s}"
            else:
                assert name.startswith("gold.") and s["rows_written"] > 0, f"run {i} {name}"


def test_rerun_is_idempotent(runs) -> None:
    """Second run on the same lake: same counts, no duplicate keys, identical Gold content."""
    assert runs[2]["counts"] == runs[1]["counts"]
    assert all(v == 0 for v in runs[2]["dupes"].values()), runs[2]["dupes"]
    assert runs[2]["hashes"] == runs[1]["hashes"]
