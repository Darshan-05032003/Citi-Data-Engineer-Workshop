# Test evidence

Run started (UTC): 2026-10-15T08:13:22Z · branch: feature/talent-pipeline · Python 3.11.0rc1 · PySpark 3.5.0 · Java 17 (venv activate)

## Command
```
cd data/talent-pipeline && source .venv/bin/activate && pytest -q --durations=5
```

Covers 15 unit tests (`tests/test_rules.py`) and 4 integration tests (`tests/test_pipeline.py`, marker `integration`). The integration fixture runs `python job.py` twice into one temporary LAKE_ROOT on the real dataset and checks every expected number (PLAN.md §2), per-step reconciliation (bronze/silver), and re-run idempotency (equal counts, no duplicate keys, identical Gold content hashes).

## Output
```
...................                                                      [100%]
============================= slowest 5 durations ==============================
191.53s setup    tests/test_pipeline.py::test_runs_succeed
4.50s call     tests/test_rules.py::test_silver_learning_pipeline_on_fixture
2.72s call     tests/test_rules.py::test_dq_domain_enum_range_regex
2.51s call     tests/test_rules.py::test_dq_warning_flags_but_keeps_row
2.50s call     tests/test_rules.py::test_dq_employees_not_null_unique_cast_fk
19 passed in 216.11s (0:03:36)
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

## Cloud run (EKS)

- run_id `20261015T084021Z-13ec40` · status **SUCCESS** · duration **1476.222 s** (local run `20261015T081756Z-03a037`: 91.569 s) · started 2026-10-15T08:40:21.969337+00:00
- Runtime: EKS Job `coding-workshop-talent-pipeline`, image `jupyter/all-spark-notebook` (Spark 3.5.0), `IS_LOCAL=false`,
  `SOURCE_DIR=s3a://coding-workshop-datalake-f2a96bad/source` (790 objects: 7 CSVs + 783 JSONL), `LAKE_ROOT=s3a://coding-workshop-datalake-f2a96bad/lake`.
- **Comparison: 26 of 26 manifest steps match** the local manifest (row counts below, plus status and the per-rule
  quarantine/warning notes, which are identical).
- Evidence files: [`cloud_run_log.txt`](cloud_run_log.txt) (pipeline log lines of this run) and [`cloud_manifest.json`](cloud_manifest.json) (its manifest).
- An earlier cloud run, `20261015T071439Z-9de56f` (1440.3 s), also matched the local manifest on 26 of 26 steps. The notebook was
  executed against that run's Gold copy (`lake_cloud/`): 0 errors, 7 charts, all printed findings identical to the local run.

| step | local (read / written / quarantined / deduped / warned) | cloud (same) | match |
|---|---|---|---|
| bronze.hr.employees | 2,000 / 2,000 / 0 / 0 / 0 | 2,000 / 2,000 / 0 / 0 / 0 | ✓ |
| bronze.hr.teams | 100 / 100 / 0 / 0 / 0 | 100 / 100 / 0 / 0 / 0 | ✓ |
| bronze.hr.competencies | 40 / 40 / 0 / 0 / 0 | 40 / 40 / 0 / 0 / 0 | ✓ |
| bronze.hr.projects | 33 / 33 / 0 / 0 / 0 | 33 / 33 / 0 / 0 / 0 | ✓ |
| bronze.hr.performance_reviews | 4,000 / 4,000 / 0 / 0 / 0 | 4,000 / 4,000 / 0 / 0 / 0 | ✓ |
| bronze.hr.competency_assessments | 4,000 / 4,000 / 0 / 0 / 0 | 4,000 / 4,000 / 0 / 0 / 0 | ✓ |
| bronze.hr.talent_review | 2,000 / 2,000 / 0 / 0 / 0 | 2,000 / 2,000 / 0 / 0 / 0 | ✓ |
| bronze.learning_activity | 29,868 / 29,868 / 0 / 0 / 0 | 29,868 / 29,868 / 0 / 0 / 0 | ✓ |
| silver.hr.teams | 100 / 100 / 0 / 0 / 0 | 100 / 100 / 0 / 0 / 0 | ✓ |
| silver.hr.competencies | 40 / 40 / 0 / 0 / 0 | 40 / 40 / 0 / 0 / 0 | ✓ |
| silver.hr.employees | 2,000 / 2,000 / 0 / 0 / 0 | 2,000 / 2,000 / 0 / 0 / 0 | ✓ |
| silver.hr.projects | 33 / 33 / 0 / 0 / 0 | 33 / 33 / 0 / 0 / 0 | ✓ |
| silver.hr.performance_reviews | 4,000 / 4,000 / 0 / 0 / 666 | 4,000 / 4,000 / 0 / 0 / 666 | ✓ |
| silver.hr.competency_assessments | 4,000 / 4,000 / 0 / 0 / 234 | 4,000 / 4,000 / 0 / 0 / 234 | ✓ |
| silver.hr.talent_review | 2,000 / 2,000 / 0 / 0 / 0 | 2,000 / 2,000 / 0 / 0 / 0 | ✓ |
| silver.learning_events | 29,868 / 29,574 / 142 / 152 / 5,185 | 29,868 / 29,574 / 142 / 152 / 5,185 | ✓ |
| gold.employee_performance_history | 0 / 4,000 / 0 / 0 / 0 | 0 / 4,000 / 0 / 0 / 0 | ✓ |
| gold.employee_performance_summary | 0 / 2,000 / 0 / 0 / 0 | 0 / 2,000 / 0 / 0 / 0 | ✓ |
| gold.employee_competency_gaps | 0 / 4,000 / 0 / 0 / 0 | 0 / 4,000 / 0 / 0 / 0 | ✓ |
| gold.team_performance_outcomes | 0 / 100 / 0 / 0 / 0 | 0 / 100 / 0 / 0 / 0 | ✓ |
| gold.competency_distribution | 0 / 400 / 0 / 0 / 0 | 0 / 400 / 0 / 0 / 0 | ✓ |
| gold.learning_progress | 0 / 25,009 / 0 / 0 / 0 | 0 / 25,009 / 0 / 0 / 0 | ✓ |
| gold.learning_summary | 0 / 2,000 / 0 / 0 / 0 | 0 / 2,000 / 0 / 0 / 0 | ✓ |
| gold.promotion_readiness | 0 / 2,000 / 0 / 0 / 0 | 0 / 2,000 / 0 / 0 / 0 | ✓ |
| gold.attrition_risk_context | 0 / 2,000 / 0 / 0 / 0 | 0 / 2,000 / 0 / 0 / 0 | ✓ |
| gold.attrition_by_trend | 0 / 9 / 0 / 0 / 0 | 0 / 9 / 0 / 0 / 0 | ✓ |

Commands (read-only, prefixed with `source ENVIRONMENT.config && unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3 &&`):
```
kubectl logs job/coding-workshop-talent-pipeline            # full pod log -> filtered into cloud_run_log.txt
aws s3 ls s3://coding-workshop-datalake-f2a96bad/lake/_runs/
aws s3 cp s3://coding-workshop-datalake-f2a96bad/lake/_runs/run_id=20261015T084021Z-13ec40/part-00000-<id>-c000.txt -
aws s3 ls s3://coding-workshop-datalake-f2a96bad/source/ --recursive | wc -l
```
