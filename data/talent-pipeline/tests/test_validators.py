"""DQ rules (validators): not-null, unique, cast, FK, domain, regex, warnings. No pipeline run needed."""

import job


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
