"""Gold: business logic per dataset, grain uniqueness and row counts on the mini dataset."""
from pathlib import Path

import pytest
from pyspark.sql import functions as F

import job
from tests.helpers import EXPECTED_GOLD_ROWS, _gold, _steps


def test_gold_rating_change_and_trend(spark) -> None:
    """(3→5) improving, (4→4) stable, (5→2) declining; first period has no previous rating."""
    reviews = spark.createDataFrame(
        [(e, p, r, []) for e, ratings in {1: (3, 5), 2: (4, 4), 3: (5, 2)}.items()
         for p, r in zip(("2024-H2", "2025-H1"), ratings)],
        "employee_id int, review_period string, rating int, dq_warnings array<string>",
    ).withColumns({"derived_period_start": job.F.lit(None).cast("date"),
                   "derived_period_end": job.F.lit(None).cast("date")})
    history = job.gold_performance_history(reviews)
    h = {(r["employee_id"], r["review_period"]): r for r in history.collect()}
    assert h[(1, "2024-H2")]["previous_rating"] is None
    assert h[(1, "2024-H2")]["derived_rating_change"] is None
    assert [h[(e, "2025-H1")]["derived_rating_change"] for e in (1, 2, 3)] == [2, 0, -3]
    s = {r["employee_id"]: r for r in job.gold_performance_summary(history).collect()}
    assert [s[e]["derived_rating_trend"] for e in (1, 2, 3)] == ["improving", "stable", "declining"]
    assert s[1]["latest_rating"] == 5 and s[1]["avg_rating"] == 4.0


def test_gold_gap_severity_threshold(spark) -> None:
    """Gap = required − proficiency; critical at ≥ threshold (default 2), moderate below."""
    assessments = spark.createDataFrame(
        [(i, 1, 3, 3 + g, None, []) for i, g in enumerate((-1, 0, 1, 2, 3), start=1)],
        "employee_id int, competency_id int, proficiency_level int, required_level int, "
        "assessed_date date, dq_warnings array<string>",
    )
    competencies = spark.createDataFrame([(1, "Python", "Technical")],
                                         "competency_id int, name string, category string")

    def severities(**kw):
        rows = job.gold_competency_gaps(assessments, competencies, **kw).collect()
        return [r["derived_gap_severity"] for r in sorted(rows, key=lambda r: r["derived_gap"])]

    assert severities() == ["none", "none", "moderate", "critical", "critical"]
    assert severities(critical_gap=3) == ["none", "none", "moderate", "moderate", "critical"]


def test_gold_team_two_projects_no_fanout(spark) -> None:
    """A team with 2 projects keeps headcount 3; a team without projects gets 0 / False."""
    teams = spark.createDataFrame([(1, "A", "Eng"), (2, "B", "Ops")],
                                  "team_id int, name string, department string")
    employees = spark.createDataFrame([(1, 1), (2, 1), (3, 1), (4, 2)],
                                      "employee_id int, team_id int")
    reviews = spark.createDataFrame(
        [(e, "2025-H1", r) for e, r in ((1, 4), (2, 5), (3, 2), (4, 3))],
        "employee_id int, review_period string, rating int",
    )
    projects = spark.createDataFrame(
        [(1, 1, "on-time", 80.0), (2, 1, "delayed", 60.0)],
        "project_id int, team_id int, outcome_status string, outcome_score double",
    )
    g6 = {r["team_id"]: r for r in job.gold_team_outcomes(teams, employees, reviews, projects).collect()}
    assert (g6[1]["headcount"], g6[1]["project_count"], g6[1]["on_time_rate"]) == (3, 2, 0.5)
    assert g6[1]["avg_outcome_score"] == 70.0 and g6[1]["pct_rated_4_plus"] == round(2 / 3, 4)
    assert (g6[2]["headcount"], g6[2]["project_count"], g6[2]["has_projects"]) == (1, 0, False)
    assert g6[2]["on_time_rate"] is None


def test_gold_completion_requires_course_completed(spark) -> None:
    """module_completed alone is not completion; course_completed is, even without enrollment."""
    ts = job.datetime(2024, 3, 1)
    events = spark.createDataFrame(
        [
            (1, "A", "enrollment", ts, None), (1, "A", "module_completed", ts, None),
            (1, "B", "course_completed", ts, None),
            (1, "C", "quiz_attempted", ts, 60.0), (1, "C", "quiz_passed", ts, 85.0),
        ],
        "employee_id int, course_name string, event_type string, event_ts timestamp, "
        "score double",
    )
    g4 = {r["course_name"]: r for r in job.gold_learning_progress(events).collect()}
    assert g4["A"]["is_course_completed"] is False and g4["A"]["module_completed_count"] == 1
    assert g4["B"]["is_course_completed"] is True and g4["B"]["completed_at"] == ts
    assert g4["B"]["dq_completion_without_enrollment"] is True
    assert g4["A"]["dq_completion_without_enrollment"] is False
    assert g4["C"]["best_quiz_score"] == 85.0

    employees = spark.createDataFrame([(1,), (2,)], "employee_id int")
    g4b = {r["employee_id"]: r for r in job.gold_learning_summary(
        employees, job.gold_learning_progress(events)).collect()}
    assert (g4b[1]["courses_enrolled"], g4b[1]["courses_completed"], g4b[1]["total_events"]) == (1, 1, 5)
    assert (g4b[2]["courses_enrolled"], g4b[2]["courses_completed"],
            g4b[2]["courses_certified"], g4b[2]["total_events"]) == (0, 0, 0, 0)


def test_gold_promotion_readiness_no_fanout(spark) -> None:
    """2 critical-gap rows + 3 completed courses for one employee → still exactly one G3 row."""
    talent = spark.createDataFrame(
        [(1, "2025-H1", "High", "Ready Now"), (2, "2025-H1", "High", "1-2 Years")],
        "employee_id int, review_cycle string, potential_rating string, "
        "promotion_readiness string",
    )
    summary = spark.createDataFrame([(1, 4, "improving"), (2, 3, "stable")],
                                    "employee_id int, latest_rating int, derived_rating_trend string")
    gaps = spark.createDataFrame([(1, 10, "critical"), (1, 11, "critical"), (2, 10, "none")],
                                 "employee_id int, competency_id int, derived_gap_severity string")
    learning = spark.createDataFrame([(1, 3), (2, 0)], "employee_id int, courses_completed long")
    rows = job.gold_promotion_readiness(talent, summary, gaps, learning).collect()
    assert len(rows) == 2
    g3 = {r["employee_id"]: r for r in rows}
    assert (g3[1]["critical_gap_count"], g3[1]["courses_completed"]) == (2, 3)
    assert g3[1]["derived_high_potential_ready_now"] is True
    assert g3[2]["derived_high_potential_ready_now"] is False and g3[2]["critical_gap_count"] == 0


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


def test_write_gold_rejects_duplicate_keys(spark, tmp_path: Path) -> None:
    """A Gold write with a duplicate grain key fails the run."""
    cfg = job.Config(is_local=True, source_dir="unused", lake_root=str(tmp_path))
    manifest = job.RunManifest(run_id="dup", is_local=True, source_dir="unused",
                               lake_root=str(tmp_path))
    df = spark.createDataFrame([(1,), (1,)], "employee_id int")
    with pytest.raises(ValueError, match="1 duplicate"):
        job.write_gold(spark, cfg, job.get_logger("dup"), manifest,
                       "employee_performance_summary", df)


# --- lineage -----------------------------------------------------------------------------
def test_gold_lineage_columns_are_present_and_non_null(spark, run) -> None:
    """Every Gold dataset is stamped with the run that built it and the build time."""
    cfg, manifest, _ = run
    for name in EXPECTED_GOLD_ROWS:
        df = _gold(spark, cfg, name)
        for col in ("_gold_run_id", "_built_at"):
            assert col in df.columns, (name, col)
            assert df.where(f"{col} IS NULL").count() == 0, (name, col)
        assert {r["_gold_run_id"] for r in df.select("_gold_run_id").distinct().collect()} == {
            manifest.run_id}, name
