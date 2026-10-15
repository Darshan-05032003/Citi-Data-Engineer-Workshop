# Test evidence

Run started (UTC): 2026-10-15T06:08:40Z · branch: feature/talent-pipeline · Python 3.11.0rc1 · PySpark 3.5.0 · Java 17 (venv activate)

## Command
```
cd data/talent-pipeline && source .venv/bin/activate && pytest -q --durations=5
```

Covers 14 unit tests (`tests/test_rules.py`) and 4 integration tests (`tests/test_pipeline.py`, marker `integration`). The integration fixture runs `python job.py` twice into one temporary LAKE_ROOT on the real dataset and checks every CLAUDE.md expected number, per-step reconciliation (bronze/silver), and re-run idempotency (equal counts, no duplicate keys, identical Gold content hashes).

## Output
```
..................                                                       [100%]
============================= slowest 5 durations ==============================
203.59s setup    tests/test_pipeline.py::test_runs_succeed
5.24s call     tests/test_rules.py::test_silver_learning_pipeline_on_fixture
2.97s call     tests/test_rules.py::test_dq_employees_not_null_unique_cast_fk
2.87s call     tests/test_rules.py::test_dq_domain_enum_range_regex
2.37s call     tests/test_pipeline.py::test_expected_numbers
18 passed in 230.45s (0:03:50)
wall=230.85s
```

## Numbers verified by the integration test

Values copied from `tests/test_pipeline.py` `EXPECTED` and the latest local manifest (`lake/_runs/run_id=20261015T055916Z-0b2e43`, status SUCCESS). All are asserted by `test_expected_numbers` / `test_rerun_is_idempotent`, which passed in the run above.

| Metric | Value | Source |
|---|---|---|
| Source: HR rows employees/teams/competencies/projects/performance_reviews/competency_assessments/talent_review | 2,000/100/40/33/4,000/4,000/2,000 | EXPECTED['hr']; manifest bronze.hr.* rows_written |
| Learning files / lines | 783 / 29,868 | EXPECTED; manifest bronze.learning_activity |
| Learning malformed / duplicates / null employee / invalid date / valid | 13 / 152 / 76 / 53 / 29,574 | EXPECTED; manifest silver.learning_events |
| Warnings: reviews / assessments before hire | 666 / 234 | EXPECTED; manifest silver.hr.* rows_warned |
| Warnings: notes instead of text / events before hire | 38 / 5,161 | EXPECTED; manifest silver.learning_events note |
| Silver HR quarantined | 0 | test_expected_numbers; manifest silver.hr.* |
| G1b trend improving / stable / declining | 898/872/230 | EXPECTED['trend'] |
| G2 gaps −1 / 0 / 1 / 2 (critical) | 1480/1527/668/325 (325) | EXPECTED['gaps'], ['critical'] |
| G3 high potential + Ready Now | 357 | EXPECTED['hipo_ready_now'] |
| G4 completion without enrollment (pairs) | 3,255 | EXPECTED |
| G5 attrition risk Low / Medium / High | 1,077/612/311 | EXPECTED['risk'] |
| G6 teams with projects / project_count | 27 / 33 | EXPECTED |
| Gold rows: employee_performance_history, employee_performance_summary, employee_competency_gaps, team_performance_outcomes, competency_distribution, learning_progress, learning_summary, promotion_readiness, attrition_risk_context, attrition_by_trend | 4,000, 2,000, 4,000, 100, 400, 25,009, 2,000, 2,000, 2,000, 9 | manifest gold.* rows_written (run 20261015T055916Z-0b2e43) |
