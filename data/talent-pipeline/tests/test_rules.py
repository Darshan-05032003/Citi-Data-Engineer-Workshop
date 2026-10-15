"""Unit tests for job.py: import safety, config defaults, manifest IO."""
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import job

JOB_DIR = Path(job.__file__).resolve().parent
FIXTURES = JOB_DIR / "tests" / "fixtures"


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


def _strings(spark, columns: str, rows: list[tuple]):
    """Bronze-like DataFrame: every column a string."""
    return spark.createDataFrame(rows, ", ".join(f"{c} string" for c in columns.split(",")))


def _split(spark, table: str, bronze, parents=None):
    checked = job.prepare_hr(table, bronze, parents or {})
    valid, quarantine = job.apply_rules(checked, table, "test-run")
    return (
        {r[job.HR_PK[table]]: r for r in valid.collect()},
        {r[f"_chk_raw_{job.HR_PK[table]}"]: r for r in quarantine.collect()},
    )


def test_dq_employees_not_null_unique_cast_fk(spark) -> None:
    """Each generic rule type passes and fails; a multi-error row is quarantined once."""
    teams = spark.createDataFrame([(1,)], "team_id int")
    cols = "employee_id,first_name,last_name,email,team_id,job_level,hire_date"
    emp = _strings(spark, cols, [
        ("1", "A", "B", "a@x", "1", "IC1", "2024-01-01"),   # ok
        (None, "C", "D", "c@x", "1", "IC1", "2024-01-01"),  # PK missing
        ("3", "E", "F", "e@x", "1", "IC1", "2024-01-01"),   # duplicate PK
        ("3", "G", "H", "g@x", "1", "IC1", "2024-01-01"),   # duplicate PK
        ("4", "I", "J", "i@x", "1", "IC1", "not-a-date"),   # cast fails
        ("5", "K", "L", "k@x", "999", "IC1", "2024-01-01"), # FK fails
        ("6", "M", "N", "m@x", "999", "IC1", "bad"),        # cast + FK
    ])
    checked = job.prepare_hr("employees", emp, {"teams": teams})
    valid, quarantine = job.apply_rules(checked, "employees", "test-run")

    assert [r["employee_id"] for r in valid.collect()] == [1]
    assert valid.collect()[0]["dq_warnings"] == []
    q = sorted((r["_chk_raw_employee_id"] or "", sorted(r["rule_ids"])) for r in quarantine.collect())
    assert q == [
        ("", ["employees.employee_id.not_null"]),
        ("3", ["employees.employee_id.unique"]),
        ("3", ["employees.employee_id.unique"]),
        ("4", ["employees.hire_date.cast"]),
        ("5", ["employees.team_id.fk"]),
        ("6", ["employees.hire_date.cast", "employees.team_id.fk"]),
    ]
    multi = [r for r in quarantine.collect() if r["_chk_raw_employee_id"] == "6"]
    assert len(multi) == 1 and len(multi[0]["dq_reasons"]) == 2
    assert {r["_run_id"] for r in quarantine.collect()} == {"test-run"}


def test_dq_domain_enum_range_regex(spark) -> None:
    """Enum, range and regex domain rules each pass and fail."""
    comp = _strings(spark, "competency_id,name,category",
                    [("1", "Python", "Technical"), ("2", "Golf", "Other")])
    valid, quarantine = _split(spark, "competencies", comp)
    assert set(valid) == {1}
    assert quarantine["2"]["rule_ids"] == ["competencies.category.domain"]

    employees = spark.createDataFrame(
        [(1, job.datetime(2020, 1, 1).date())], "employee_id int, hire_date date"
    )
    cols = "review_id,employee_id,review_period,rating,manager_comment,submitted_date"
    rev = _strings(spark, cols, [
        ("1", "1", "2024-H2", "5", None, "2025-01-10"),  # ok (null comment allowed)
        ("2", "1", "2024-H2", "7", "x", "2025-01-10"),   # rating out of range
        ("3", "1", "2024-H3", "3", "x", "2025-01-10"),   # bad period
    ])
    valid, quarantine = _split(spark, "performance_reviews", rev, {"employees": employees})
    assert set(valid) == {1}
    assert str(valid[1]["derived_period_start"]) == "2024-07-01"
    assert str(valid[1]["derived_period_end"]) == "2024-12-31"
    assert quarantine["2"]["rule_ids"] == ["performance_reviews.rating.domain"]
    assert quarantine["3"]["rule_ids"] == ["performance_reviews.review_period.domain"]


def test_dq_warning_flags_but_keeps_row(spark) -> None:
    """Assessment before hire → kept in valid with a warning; after hire → no warning."""
    employees = spark.createDataFrame(
        [(1, job.datetime(2024, 6, 1).date())], "employee_id int, hire_date date"
    )
    competencies = spark.createDataFrame([(1,)], "competency_id int")
    cols = "assessment_id,employee_id,competency_id,proficiency_level,required_level,assessed_date"
    ass = _strings(spark, cols, [
        ("1", "1", "1", "3", "4", "2024-01-15"),  # before hire → warning
        ("2", "1", "1", "3", "4", "2024-07-15"),  # after hire → clean
    ])
    valid, quarantine = _split(
        spark, "competency_assessments", ass,
        {"employees": employees, "competencies": competencies},
    )
    assert quarantine == {}
    assert valid[1]["dq_warnings"] == ["competency_assessments.assessed_date.before_hire"]
    assert valid[2]["dq_warnings"] == []
    assert "_chk_hire_date" not in valid[1].asDict()


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
