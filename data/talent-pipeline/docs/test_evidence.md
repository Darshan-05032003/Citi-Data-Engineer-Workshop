# Test evidence

Run date (UTC): 2026-10-15 · branch: feature/talent-pipeline · Python 3.11.0rc1 · PySpark 3.5.0 · Java 17 (venv activate)

## Command
```
cd data/talent-pipeline && source .venv/bin/activate && pytest -q
```

Covers 46 unit tests (`tests/test_rules.py` 15, `tests/test_stages.py` 23, `tests/test_guards.py` 8; see [Unit coverage](#unit-coverage)) and 4 integration tests (`tests/test_pipeline.py`, marker `integration`). The integration fixture runs `python job.py` twice into one temporary LAKE_ROOT on the real dataset and checks every expected number ([DESIGN.md](DESIGN.md#reference-numbers)), per-step reconciliation (bronze/silver), and re-run idempotency (equal counts, no duplicate keys, identical Gold content hashes).

## Output
```
..................................................                       [100%]
50 passed in 209.78s (0:03:29)
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

## Unit coverage

The integration tests run `job.py` in a subprocess, so coverage does not count them. `tests/test_stages.py` therefore runs
`ingest_into_bronze` → `transform_to_silver` → `aggregate_to_gold` → `write_manifest` **in-process** on a hand-made mini
dataset (`tests/fixtures/mini_source/`, real file formats). It also covers the remaining branches: config parsing, `spark_conf`,
`read_latest_bronze_hr` (newest of 2 ingest dates / no partitions), duplicate Gold keys, the `main()` failure path, and the
manifest's observability fields (`code_version`, `spark_version`, source bytes, per-step `duration_s`/`rows_per_s`).

`tests/test_guards.py` (8 tests) checks the source guards on edited copies of the mini dataset:
- a missing column fails and names the column, both directly and end to end through `main()` with a FAILED manifest;
- an extra column only warns and is kept;
- a header-only CSV fails, and so does a 0-byte CSV;
- no learning files fails, and so do learning files with 0 lines;
- the S3A retry settings appear only for `s3a://` paths.

```
cd data/talent-pipeline && source .venv/bin/activate && pytest -q -m "not integration" --cov=job --cov-report=term-missing
Name     Stmts   Miss  Cover   Missing
--------------------------------------
job.py     461      9    98%   685, 813, 1324-1326, 1336-1338, 1345
--------------------------------------
TOTAL      461      9    98%
46 passed, 4 deselected in 69.93s (0:01:09)
```

Before the mini-dataset tests, unit coverage was 58%. Still uncovered:
- the two reconciliation `raise` lines (685, 813);
- `main()`'s success tail (1324-1326, covered by the integration tests);
- `main()`'s "could not write manifest" branch (1336-1338);
- `sys.exit(main())` under `__main__` (1345).

The real-dataset run after these changes, `20261015T194137Z-c917c2` (SUCCESS, 95.99 s, code_version `d6d5cdadaaa6`),
matched the previous local run `20261015T081756Z-03a037` on all 26 steps: status, all counts and notes.

**Mini dataset (hand-computed expectations, asserted by `test_stages.py`):**
- HR: 3 teams, 3 competencies, 6 employees, 2 projects on team 1, 12 reviews, 8 assessments, 6 talent rows.
- HR error rows:
  - review 12 has rating 7, so it is quarantined by `performance_reviews.rating.domain`;
  - assessment 8 has employee_id 99, so it is quarantined by `competency_assessments.employee_id.fk`.
- Employee 4 is hired on 2024-08-01, so their H1 review and their assessment are flagged as before-hire warnings.
- Learning: 3 files.
  - The day=01 file has 4 lines, including 1 malformed line.
  - The day=02 file has 6 lines: a duplicate E3, a null employee_id, a bad date, a `notes`-only event, an event before hire, and a completion without enrollment.
  - The day=03 file is empty (0 bytes).

| Step | read / written / quarantined / deduped / warned |
|---|---|
| bronze.hr teams, competencies, employees, projects | 3, 3, 6, 2 (read = written) |
| bronze.hr performance_reviews, competency_assessments, talent_review | 12, 8, 6 (read = written) |
| bronze.learning_activity | 10 / 10 / 0 / 0 / 0, with files=3 and zero_line_files=1 |
| silver.hr.performance_reviews | 12 / 11 / 1 / 0 / 1 |
| silver.hr.competency_assessments | 8 / 7 / 1 / 0 / 1 |
| silver.learning_events | 10 / 6 / 3 (malformed 1, not_null 1, event_ts.invalid 1) / 1 / 2 (notes_used 1, before_hire 1) |
| Gold rows | history 11, summary 6, gaps 7, teams 3, competency_distribution 6, learning_progress 5, learning_summary 6, promotion_readiness 6, attrition_context 6, attrition_by_trend 5 |
| Gold values | trend improving/declining/stable/null 3/1/1/1; gaps −1/0/1/2 = 1/3/1/2; High + Ready Now 2; completion without enrollment 1; 1 team with projects (2 projects) |

## CI

`.github/workflows/talent-pipeline.yml` (GitHub Actions). It runs on push and pull request when `data/talent-pipeline/**`
or the workflow file changes, and on manual dispatch. Permissions are `contents: read`, and it uses no secrets.

| Job | Runs | Fails when |
|---|---|---|
| `unit` | Python 3.11 + Temurin Java 17; `pip install -r requirements-dev.txt`; `pytest -q -m "not integration" --cov=job --cov-report=term --cov-fail-under=90` | any unit test fails or `job.py` coverage < 90% |
| `integration` (needs `unit`, 20 min timeout) | same setup; `bash scripts/fetch_dataset.sh` (download + sha256 check); `pytest -q -m integration` | the dataset can't be fetched or verified (clear `::error` annotation: "Dataset download failed … not a pipeline test failure"), or any integration test fails |
| `security` | `pip install bandit==1.7.10`; `bandit -r job.py -ll` | bandit finds a medium or high severity issue |

**The same commands, run locally** (2026-10-15, before the first push of the workflow):

```
$ pytest -q -m "not integration" --cov=job --cov-report=term --cov-fail-under=90
job.py     461      9    98%
Required test coverage of 90% reached. Total coverage: 98.05%
46 passed, 4 deselected in 73.40s (0:01:13)

$ bash scripts/fetch_dataset.sh
Dataset already present, skipping: …/sampleData/data-engineering-sample-performance-scenario/data-engineering-performance-talent-management

$ pytest -q -m integration
4 passed, 46 deselected in 144.90s (0:02:24)

$ bandit -r job.py -ll          # bandit 1.7.10
Test results:
	No issues identified.
Total lines of code: 1130 · Total lines skipped (#nosec): 0 · Issues by severity: Low 0, Medium 0, High 0
```

The failure path of the dataset step was also simulated: the download command was replaced by one that fails. The step
printed the `::error` annotation and exited 1.

## Performance

File layout and caching change (see [DESIGN.md](DESIGN.md#performance)). Each run below used the real dataset, started from
a fresh `lake/` (except the second run), and ran `python job.py` locally. Durations are the manifest step `duration_s`
summed per layer; file counts come from `find lake/<layer> -name '*.parquet' | wc -l`.

| Layer | Before (`8e832b`): s | Before: files | After (`404778`): s | After: files | Second run (`1be11b`): s | Second run: files |
|---|---|---|---|---|---|---|
| Bronze HR | 8.26 | 7 | 8.33 | 7 | 8.78 | 7 |
| Bronze learning | 25.53 | 782 | 12.06 | 37 | 12.66 | 37 |
| Silver HR | 12.18 | 7 | 13.38 | 7 | 13.12 | 7 |
| Silver learning | 24.01 | 111 | 12.28 | 37 | 11.13 | 37 |
| Quarantine | — | 15 | — | 1 | — | 2 (one folder per run_id) |
| Gold | 11.64 | 10 | 12.41 | 10 | 11.81 | 10 |
| **Run total** | **96.04** | **932** | **75.32** | **99** | **70.03** | **100** |

- **Same results:** all 26 steps of the after-run (`20261015T200502Z-404778`) match the earlier run
  `20261015T194137Z-c917c2` on status, every count and every note. The before-run (`20261015T200013Z-8e832b`) used
  the previous code (commit 0520438).
- **Idempotent:** the second run on the same lake (`20261015T200647Z-1be11b`) matches `404778` on all 26 steps.
  The Gold content hashes of all 10 datasets are identical, computed the same way as `tests/test_pipeline.py::_content_hash`.
- **Tests:** `tests/test_stages.py::test_files_written_and_monthly_layout` checks `files_written == 1` for every step on
  the mini dataset, the `year=2024/month=03` Bronze layout with no `day=` folder, and a single quarantine file.

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

## Notebook: local vs S3

The same notebook was executed twice on 2026-10-15 with `jupyter nbconvert --to notebook --execute` (commands in the
README, "Notebook data source"). The first run used the default local lake; the second ran from a scratch copy with
`LAKE_ROOT=s3://coding-workshop-datalake-f2a96bad/lake`, after `source ENVIRONMENT.config`.

| Mode | Lake | Run line (cell 3) | Errors | Charts |
|---|---|---|---|---|
| Local | `../lake` | `run_id=20261015T200647Z-1be11b  status=SUCCESS  duration_s=70.025` | 0 | 7 |
| S3 | `s3://coding-workshop-datalake-f2a96bad/lake` | `run_id=20261015T095920Z-649058  status=SUCCESS  duration_s=1452.405` | 0 | 7 |

- **All 7 finding sentences** (cells 6, 10, 14, 18, 22, 26, 30) are **identical** between the two modes, checked by
  comparing the printed text. They are also identical to the outputs committed before this change.
- The S3-executed copy was not committed, and nothing was written to S3. No credential-like strings appear in either copy
  or in `talent_insights.html`.
