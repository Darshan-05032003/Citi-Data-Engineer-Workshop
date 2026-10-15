# Test evidence

Run date (UTC): 2026-10-15 · branch: feature/talent-pipeline · Python 3.11.0rc1 · PySpark 3.5.0 · Java 17 (venv activate)

## Command
```
cd data/talent-pipeline && source .venv/bin/activate && pytest -q
```

Covers 73 unit tests, 5 integration tests and 2 cloud tests, organised by layer (file → tests: `test_validators.py` 3, `test_bronze.py` 18, `test_silver.py` 6, `test_gold.py` 18, `test_observability.py` 10, `test_postgres.py` 12, `test_postgres_gate.py` 6; see [Unit coverage](#unit-coverage) and the table in the README). Integration: 4 in `tests/test_pipeline_integration.py` (marker `integration`) and the files-vs-postgres parity test in `tests/test_postgres.py` (markers `integration` + `postgres`; this run had `REQUIRE_POSTGRES=1` and `POSTGRES_NAME/USER/PASS` set for the local server, so it ran instead of skipping). Cloud: `tests/test_cloud_smoke.py` (marker `cloud`, skipped here). The integration fixture runs `python job.py` twice into one temporary LAKE_ROOT on the real dataset and checks every expected number ([DESIGN.md](DESIGN.md#reference-numbers)), per-step reconciliation (bronze/silver), and re-run idempotency (equal counts, no duplicate keys, identical Gold content hashes).

## Output
```
..................ss....................................................
........
SKIPPED [1] tests/test_cloud_smoke.py:56 and :69: cloud smoke test: set CLOUD_SMOKE=1 and LAKE_ROOT=s3a://bucket/prefix to run it
78 passed, 2 skipped in 370.12s (0:06:10)
```

## Numbers verified by the integration test

Values copied from `tests/test_pipeline_integration.py` `EXPECTED` and the latest local manifest (`lake/_runs/run_id=20261015T055916Z-0b2e43`, status SUCCESS). All are asserted by `test_expected_numbers` / `test_rerun_is_idempotent`, which passed in the run above.

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

The integration tests run `job.py` in a subprocess, so coverage does not count them. the in-process tests (shared fixture `conftest.py::run`) therefore run
`ingest_into_bronze` → `transform_to_silver` → `aggregate_to_gold` → `write_manifest` **in-process** on a hand-made mini
dataset (`tests/fixtures/mini_source/`, real file formats). It also covers the remaining branches: config parsing, `spark_conf`,
`read_latest_bronze_hr` (newest of 2 ingest dates / no partitions), duplicate Gold keys, the `main()` failure path, and the
manifest's observability fields (`code_version`, `spark_version`, source bytes, per-step `duration_s`/`rows_per_s`).

The source guards in `tests/test_bronze.py` (8 tests) are checked on edited copies of the mini dataset:
- a missing column fails and names the column, both directly and end to end through `main()` with a FAILED manifest;
- an extra column only warns and is kept;
- a header-only CSV fails, and so does a 0-byte CSV;
- no learning files fails, and so do learning files with 0 lines;
- the S3A retry settings appear only for `s3a://` paths.

```
cd data/talent-pipeline && source .venv/bin/activate && pytest -q -m "not integration and not cloud" --cov=job --cov-report=term --cov-fail-under=90
Name     Stmts   Miss  Cover   Missing
--------------------------------------
job.py     536     10    98%
--------------------------------------
TOTAL      536     10    98%
Required test coverage of 90% reached. Total coverage: 98.13%
73 passed, 7 deselected in 94.73s (0:01:34)
```

Before the mini-dataset tests, unit coverage was 58%. Still uncovered:
- the real `spark.read.jdbc` call (471, run by the Postgres parity test in a subprocess);
- the two reconciliation `raise` lines (839, 967);
- `main()`'s success tail (1481-1483, covered by the integration tests);
- `main()`'s "could not write manifest" branch (1493-1495);
- `sys.exit(main())` under `__main__` (1502).

The real-dataset run after these changes, `20261015T194137Z-c917c2` (SUCCESS, 95.99 s, code_version `d6d5cdadaaa6`),
matched the previous local run `20261015T081756Z-03a037` on all 26 steps: status, all counts and notes.

**Mini dataset (hand-computed expectations, asserted by the layer tests through `conftest.py::run`):**
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

## Cloud Postgres access check

On 2026-10-15 the EKS job's route to Aurora was checked and found closed (P0: `pg_isready` from a pod and from the VDI got no
response; Aurora's security group accepts only its own members). To test whether the missing rule could be added, one dry run
was made. It makes no change: `aws ec2 authorize-security-group-ingress --group-id sg-0a86fb52894403a4c --protocol tcp --port 5432
--source-group <EKS cluster SG> --dry-run`. Result (encoded authorization message omitted):

`You are not authorized to perform this operation. User: arn:aws:sts::332991882156:assumed-role/coding-workshop-assume-ap-south-1-f2a96bad/lambda_session is not authorized to perform: ec2:AuthorizeSecurityGroupIngress on resource: arn:aws:ec2:ap-south-1:332991882156:security-group/sg-0a86fb52894403a4c with an explicit deny in an identity-based policy.`

The workshop role explicitly denies the action, so the cloud job stays in files mode. No infra change was applied, and the draft
Terraform change (commit `009ef77`) was reverted (`21a1b20`). Postgres mode itself is covered by the local and CI parity tests below.

## PostgreSQL source mode

Local PostgreSQL 18.6, database `postgres`, schema `hr_source`, run on 2026-10-15.

**Loader** (`scripts/load_source_postgres.sh`, run twice; both runs gave the same counts):
```
Recreating schema hr_source on localhost:5432/postgres
… (the dataset's own load.sh: CREATE TABLE ×7, CREATE INDEX ×3, COPY per table) …
Row counts in hr_source:
teams 100 · competencies 40 · employees 2000 · projects 33 · performance_reviews 4000 · competency_assessments 4000 · talent_review 2000
```

**Postgres-mode run** (`SOURCE_MODE=postgres python job.py`, scratch lake), last lines:
```
INFO … gold: attrition_by_trend rows=9 grain=attrition_risk×derived_rating_trend unique files_written=1 duration_s=1.16 rows_per_s=8
INFO … manifest written to …/lake_pg/_runs/run_id=20261015T210848Z-3dea60
INFO … end status=SUCCESS duration_s=57.765
```
The password does not appear in the log (checked with grep: 0 matches).

**Files vs postgres**, same code, two fresh lakes:
- **26/26 manifest steps match:** status, every count, every note and `files_written`.
- **10/10 Gold content hashes match**, computed with `tests/test_pipeline_integration.py::_content_hash`.
- The Postgres manifest records `source_mode=postgres` and HR rows teams 100, competencies 40, employees 2000,
  projects 33, performance_reviews 4000, competency_assessments 4000, talent_review 2000.
- `tests/test_postgres.py::test_files_and_postgres_modes_give_identical_results` asserts the same automatically.
  It uses its own schema, `hr_source_pytest`, and drops it afterwards.
- **Unit tests** (12, no database; a fake JDBC source supplies typed mini tables) cover:
  - config, JDBC URL/sslmode, `connectTimeout=10`, and the Spark packages per mode
  - retry: 2/4/8/16 s, 5 attempts, last error raised
  - the password kept out of the repr, logs and manifest
  - Bronze written as strings, with postgres lineage
  - failures: missing settings, unreachable database, missing table, schema drift, empty table
- **Bandit 1.7.10** on `job.py`: no issues.

## Test organisation, lineage, logs and cloud smoke

Tests are grouped by layer (README, "Tests"); the 69 earlier tests were moved without edits (collected ids compared before and
after: none missing). New in this change, 11 tests (73 unit + 2 cloud; 69 → 80):

| Test | Proves |
|---|---|
| `test_bronze_hr_lineage_columns_are_present_and_non_null` | all 7 Bronze HR tables carry `_run_id`, `_ingested_at`, `_source_system`, `_source_file` (non-null; `file`, ends with `<table>.csv`) |
| `test_bronze_learning_lineage_columns_are_present_and_non_null` | Bronze learning keeps `_run_id`, `_ingested_at`, `_source_file`, `_line_no`, `_record_hash` (sha256, 64 chars) |
| `test_silver_lineage_columns_are_present_and_non_null` | Silver HR and learning rows keep the lineage and are stamped with this run's `_run_id` |
| `test_gold_lineage_columns_are_present_and_non_null` | all 10 Gold datasets carry `_gold_run_id` and `_built_at` |
| `test_manifest_lineage_fields_are_present_and_non_null` | the manifest, in memory and as written, has run id, times, `code_version` (= `job.code_version()`), `spark_version`, source dir, mode |
| `test_every_step_logs_its_counts` | each of the 26 steps has exactly one log line with the same counts, `files_written` and `duration_s` |
| `test_run_log_has_only_the_expected_warning` | no ERROR; the only WARNING is the empty learning file |
| `test_postgres_password_is_never_logged_on_success` / `..._when_main_fails` | with `POSTGRES_PASS` set, the value is absent from log output (caplog), stdout/stderr and the manifest, including the retry and failure paths |
| `test_cloud_smoke.py` (2 tests) | newest cloud manifest is SUCCESS with 26 steps and no FAILED step; Gold row counts and headline numbers equal the expected values |

**Cloud smoke, run once on 2026-10-15** (read-only; `source ENVIRONMENT.config`, then `CLOUD_SMOKE=1 LAKE_ROOT=s3a://coding-workshop-datalake-f2a96bad/lake pytest -q -m cloud`):
- without `CLOUD_SMOKE`: 2 skipped;
- with `CLOUD_SMOKE=1` but a non-S3 `LAKE_ROOT`: fails with "CLOUD_SMOKE=1 needs LAKE_ROOT=s3a://bucket/prefix";
- against the deployed lake: **2 passed in 4.5 s** (newest run `20261015T095920Z-649058`).

CI runs the unit tests with `-m "not integration and not cloud"`; the integration job's `pytest -q -m integration` does not select the `cloud` marker.

## CI

`.github/workflows/talent-pipeline.yml` (GitHub Actions). It runs on push and pull request when `data/talent-pipeline/**`
or the workflow file changes, and on manual dispatch. Permissions are `contents: read`, and it uses no secrets.

| Job | Runs | Fails when |
|---|---|---|
| `unit` | Python 3.11 + Temurin Java 17; `pip install -r requirements-dev.txt`; `pytest -q -m "not integration" --cov=job --cov-report=term --cov-fail-under=90` | any unit test fails or `job.py` coverage < 90% |
| `integration` (needs `unit`, 20 min timeout) | same setup, plus a `postgres:17` service container (health-checked with `pg_isready`, throwaway CI-only credentials in the job `env`, not GitHub secrets) and `REQUIRE_POSTGRES=1`; installs `postgresql-client` only if `psql`/`pg_isready` are missing; `bash scripts/fetch_dataset.sh` (download + sha256 check); `bash scripts/load_source_postgres.sh`; `pytest -q -m integration` (includes the files-vs-postgres parity test) | the dataset can't be fetched or verified (clear `::error` annotation: "Dataset download failed … not a pipeline test failure"), Postgres is unreachable (the parity test fails instead of skipping), or any integration test fails |
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
  The Gold content hashes of all 10 datasets are identical, computed the same way as `tests/test_pipeline_integration.py::_content_hash`.
- **Tests:** `tests/test_bronze.py::test_files_written_and_monthly_layout` checks `files_written == 1` for every step on
  the mini dataset, the `year=2024/month=03` Bronze layout with no `day=` folder, and a single quarantine file.

## Cloud run (EKS)

Final cloud run of the current code, after the file-layout change (F4) and everything since:

- run_id `20261015T225127Z-da8bc6` · status **SUCCESS** · duration **368.0 s** · started 2026-10-15T22:51:27.079243+00:00
  · `code_version` **75d99c812e4f**: Helm adds a leading newline to job.py in the ConfigMap; sha256("\n"+job.py) = 75d99c812e4f
  (the committed job.py alone hashes to de236fc1c408), so the deployed code is this repo's job.py.
- Runtime: EKS Job `coding-workshop-talent-pipeline`, image `jupyter/all-spark-notebook` (Spark 3.5.0), `IS_LOCAL=false`,
  `SOURCE_MODE=files`, `SOURCE_DIR=s3a://coding-workshop-datalake-f2a96bad/source` (790 objects), `LAKE_ROOT=s3a://coding-workshop-datalake-f2a96bad/lake`.
- Before the run, only `lake/bronze/learning_activity/` was cleared in S3 (782 objects in the old `year/month/day` layout,
  checked with `--dryrun` first); `source/`, `_runs/` and the other layers were kept.
- **Comparison: 26 of 26 manifest steps match** a fresh local files-mode run (`20261015T225511Z-d46925`, 73.5 s) on status, every
  count, every note (quarantine/warning counts by rule) and `files_written` (98 parquet files in total).
- **Cloud smoke test** on this run: `2 passed, 78 deselected in 4.14s`
  (`CLOUD_SMOKE=1 LAKE_ROOT=s3a://coding-workshop-datalake-f2a96bad/lake pytest -q -m cloud`; it reads the newest manifest, `20261015T225127Z-da8bc6`).
- **Full local suite** the same day: `78 passed, 2 skipped in 404.99s (0:06:44)` (the 2 skipped are the cloud tests, run separately above).
- Evidence files: [`cloud_run_log.txt`](cloud_run_log.txt) (pipeline log lines of this run) and [`cloud_manifest.json`](cloud_manifest.json) (its manifest).
- Earlier cloud runs with the old code and layout (`20261015T071439Z-9de56f` 1,440.3 s, `20261015T084021Z-13ec40` 1,476.2 s,
  `20261015T095920Z-649058` 1,452.4 s) also matched the local counts on 26 of 26 steps.

| Layer | local s | cloud s |
|---|---|---|
| bronze.hr | 9.3 | 38.7 |
| bronze.learning_activity | 11.8 | 91.0 |
| silver.hr | 11.6 | 46.7 |
| silver.learning_events | 10.9 | 64.6 |
| gold | 11.5 | 59.2 |

| step | local (read / written / quarantined / deduped / warned) | cloud (same) | files | local s | cloud s | match |
|---|---|---|---|---|---|---|
| bronze.hr.employees | 2,000 / 2,000 / 0 / 0 / 0 | 2,000 / 2,000 / 0 / 0 / 0 | 1 | 3.9 | 10.8 | ✓ |
| bronze.hr.teams | 100 / 100 / 0 / 0 / 0 | 100 / 100 / 0 / 0 / 0 | 1 | 1.0 | 5.3 | ✓ |
| bronze.hr.competencies | 40 / 40 / 0 / 0 / 0 | 40 / 40 / 0 / 0 / 0 | 1 | 0.9 | 4.6 | ✓ |
| bronze.hr.projects | 33 / 33 / 0 / 0 / 0 | 33 / 33 / 0 / 0 / 0 | 1 | 1.3 | 4.6 | ✓ |
| bronze.hr.performance_reviews | 4,000 / 4,000 / 0 / 0 / 0 | 4,000 / 4,000 / 0 / 0 / 0 | 1 | 0.9 | 4.9 | ✓ |
| bronze.hr.competency_assessments | 4,000 / 4,000 / 0 / 0 / 0 | 4,000 / 4,000 / 0 / 0 / 0 | 1 | 0.7 | 4.3 | ✓ |
| bronze.hr.talent_review | 2,000 / 2,000 / 0 / 0 / 0 | 2,000 / 2,000 / 0 / 0 / 0 | 1 | 0.6 | 4.2 | ✓ |
| bronze.learning_activity | 29,868 / 29,868 / 0 / 0 / 0 | 29,868 / 29,868 / 0 / 0 / 0 | 37 | 11.8 | 91.0 | ✓ |
| silver.hr.teams | 100 / 100 / 0 / 0 / 0 | 100 / 100 / 0 / 0 / 0 | 1 | 1.9 | 6.2 | ✓ |
| silver.hr.competencies | 40 / 40 / 0 / 0 / 0 | 40 / 40 / 0 / 0 / 0 | 1 | 0.9 | 5.2 | ✓ |
| silver.hr.employees | 2,000 / 2,000 / 0 / 0 / 0 | 2,000 / 2,000 / 0 / 0 / 0 | 1 | 1.9 | 7.9 | ✓ |
| silver.hr.projects | 33 / 33 / 0 / 0 / 0 | 33 / 33 / 0 / 0 / 0 | 1 | 1.5 | 5.6 | ✓ |
| silver.hr.performance_reviews | 4,000 / 4,000 / 0 / 0 / 666 | 4,000 / 4,000 / 0 / 0 / 666 | 1 | 2.0 | 8.4 | ✓ |
| silver.hr.competency_assessments | 4,000 / 4,000 / 0 / 0 / 234 | 4,000 / 4,000 / 0 / 0 / 234 | 1 | 1.9 | 8.1 | ✓ |
| silver.hr.talent_review | 2,000 / 2,000 / 0 / 0 / 0 | 2,000 / 2,000 / 0 / 0 / 0 | 1 | 1.5 | 5.3 | ✓ |
| silver.learning_events | 29,868 / 29,574 / 142 / 152 / 5,185 | 29,868 / 29,574 / 142 / 152 / 5,185 | 37 | 10.9 | 64.6 | ✓ |
| gold.employee_performance_history | 0 / 4,000 / 0 / 0 / 0 | 0 / 4,000 / 0 / 0 / 0 | 1 | 0.9 | 3.7 | ✓ |
| gold.employee_performance_summary | 0 / 2,000 / 0 / 0 / 0 | 0 / 2,000 / 0 / 0 / 0 | 1 | 1.0 | 4.6 | ✓ |
| gold.employee_competency_gaps | 0 / 4,000 / 0 / 0 / 0 | 0 / 4,000 / 0 / 0 / 0 | 1 | 0.6 | 3.8 | ✓ |
| gold.team_performance_outcomes | 0 / 100 / 0 / 0 / 0 | 0 / 100 / 0 / 0 / 0 | 1 | 1.1 | 4.6 | ✓ |
| gold.competency_distribution | 0 / 400 / 0 / 0 / 0 | 0 / 400 / 0 / 0 / 0 | 1 | 1.1 | 4.5 | ✓ |
| gold.learning_progress | 0 / 25,009 / 0 / 0 / 0 | 0 / 25,009 / 0 / 0 / 0 | 1 | 1.2 | 7.3 | ✓ |
| gold.learning_summary | 0 / 2,000 / 0 / 0 / 0 | 0 / 2,000 / 0 / 0 / 0 | 1 | 1.1 | 7.1 | ✓ |
| gold.promotion_readiness | 0 / 2,000 / 0 / 0 / 0 | 0 / 2,000 / 0 / 0 / 0 | 1 | 1.4 | 9.0 | ✓ |
| gold.attrition_risk_context | 0 / 2,000 / 0 / 0 / 0 | 0 / 2,000 / 0 / 0 / 0 | 1 | 1.9 | 9.9 | ✓ |
| gold.attrition_by_trend | 0 / 9 / 0 / 0 / 0 | 0 / 9 / 0 / 0 / 0 | 1 | 1.2 | 4.6 | ✓ |

Commands (read-only, prefixed with `source ENVIRONMENT.config && unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3 &&`):
```
kubectl logs job/coding-workshop-talent-pipeline            # full pod log -> filtered into cloud_run_log.txt
aws s3 ls s3://coding-workshop-datalake-f2a96bad/lake/_runs/
aws s3 cp s3://coding-workshop-datalake-f2a96bad/lake/_runs/run_id=20261015T225127Z-da8bc6/part-00000-<id>-c000.txt -
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


**Final notebook run (committed outputs), 2026-10-15:** `LAKE_ROOT=s3://coding-workshop-datalake-f2a96bad/lake jupyter nbconvert
--to notebook --execute --inplace talent_insights.ipynb`, then exported to `docs/talent_insights.html`.

| Check | Result |
|---|---|
| Run line (cell 3) | `run_id=20261015T225127Z-da8bc6  status=SUCCESS  duration_s=368.045` (the final cloud run) |
| Errors / charts | 0 / 7; the HTML parses and holds 7 images and the new run_id |
| trend improving / stable / declining | 898 / 872 / 230 ✓ |
| critical gaps (gap ≥ 2) | 325 ✓ |
| High potential + Ready Now | 357 ✓ |
| attrition risk Low / Medium / High | 1,077 / 612 / 311 ✓ |
| teams with projects / projects | 27 / 33 ✓ |
| 7 finding sentences | identical to the previous (local-run) outputs |
