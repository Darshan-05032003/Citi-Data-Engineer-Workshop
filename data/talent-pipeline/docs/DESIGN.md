# Design: ACME Talent Pipeline

One PySpark job (`job.py`) builds a Bronze → Silver → Gold lake from ACME's HR extracts and learning-activity
logs. It writes a run manifest for every run. The same file runs locally and as a Kubernetes Job on EKS against S3.

## Architecture

```
SOURCE_DIR (7 HR CSVs + learning_activity/*.jsonl)
  → Bronze   raw, all strings, + lineage (_run_id, _ingested_at, _source_file …)
  → Silver   typed, validated: errors → quarantine/, warnings → dq_warnings
  → Gold     10 analysis datasets, one grain each (duplicate keys fail the write)
  → _runs/   manifest JSON: status, code_version, source sizes, per-step counts + timings
```

| Layer | Contract |
|---|---|
| Bronze HR | CSV rows as strings, one partition per `ingest_date`; nothing dropped |
| Bronze learning | one row per raw JSONL line (`value`, `_line_no`, `_source_file`, `_record_hash`), partitioned by `year/month` with `day` as a column, one file per month (see Performance) |
| Silver | typed columns, every DQ rule applied, row-level `dq_warnings`; learning events partitioned by `event_year/event_month` |
| Quarantine | failing rows with all `rule_ids` and `dq_reasons`, per entity and `run_id` |
| Gold | G1 employee×period, G1b employee, G2 employee×competency, G3 employee, G4 employee×course, G4b employee, G5 employee, G5b attrition_risk×trend, G6 team, G7 competency×department |

Code order in `job.py`: config → logging → Spark session → DQ rules → apply_rules/quarantine → Bronze → Silver →
Gold (one function per dataset) → manifest → `main()`. Nothing runs at import time.

## Decisions

- **Two HR source modes, one Bronze contract.** `SOURCE_MODE=files` (default) reads the CSVs from `SOURCE_DIR`;
  `SOURCE_MODE=postgres` reads the same 7 tables over JDBC. Learning events always come from the JSONL files.
  See "Source modes" below.
- **A single `job.py`.** The Helm chart ships only `job.py` + `requirements.txt` to the pod. Tests, notebook and
  docs stay outside the job.
- **`pyspark==3.5.0` only.** This matches the cloud image (Spark 3.5.0, Python 3.11, Java 17, Hadoop 3.3.4).
  `hadoop-aws:3.3.4` is added only when a path starts with `s3a://`. No Iceberg, and no pandas in the job.
- **Config from environment variables only:** `IS_LOCAL`, `SOURCE_DIR`, `LAKE_ROOT` (plus `AWS_REGION` in cloud).
- **All IO through Spark**, including the manifest, so a local path and `s3a://` behave the same way.
- **Declarative DQ rules.** Each `Rule(rule_id, entity, column, severity, condition, description)` is generated
  from the schema contract (not-null, cast, unique PK, FK) or from the source CHECK constraints (domains, ranges).
- **Errors vs warnings.** A row failing any *error* rule is quarantined once, with every reason, and kept out of
  Silver. A *warning* keeps the row and adds the rule id to `dq_warnings`; examples are a review period ending
  before hire and a learning event before hire. NULL counts as a pass (SQL CHECK semantics), so a missing value
  is reported once, by its `not_null` rule.
- **Dedupe before validate.** Learning events are first checked for malformed JSON, then deduplicated on
  `event_id`, and only then validated. A repeated bad event is therefore counted as one duplicate plus one
  quarantined row, not twice. Each step must reconcile: `read = written + quarantined + deduped`; otherwise the
  run fails.
- **Idempotent re-runs.** Bronze overwrites only its own partition (dynamic partition overwrite); Silver and Gold
  are full overwrites. Running twice gives identical Gold content (checked by an integration test).
- **Cloud credentials: Kubernetes Secret, not IRSA.** The EKS node role has no S3 permissions, and the node
  requires IMDSv2 with hop limit 1, so pods cannot use instance credentials. Short-lived STS credentials are
  injected from the deploying shell into a Secret (`envFrom`) and never written to Terraform state. Production
  would use IRSA (an IAM role per service account).

## Source modes

| | `files` (default) | `postgres` |
|---|---|---|
| HR tables | `SOURCE_DIR/<table>.csv` | `POSTGRES_SCHEMA.<table>` over JDBC (`org.postgresql:postgresql:42.7.4`, added only in this mode) |
| Learning events | `SOURCE_DIR/learning_activity/` | same (always files) |
| Pre-write checks | file exists, not 0 bytes, header has every `schema.sql` column | settings present, connection probe with retry, table readable, every `schema.sql` column |
| Lineage | `_source_system=file`, `_source_file=<file URI>` | `_source_system=postgres`, `_source_file=postgres://host:port/db.schema.table` |
| `manifest.source.hr.<table>` | `bytes` | `rows` |

**Why Postgres columns are cast to string in Bronze.**
- Bronze is the raw landing layer, and the CSV path delivers every column as a string.
- Casting JDBC columns (int, date, numeric) to string gives Bronze the **same contract in both modes**.
- As a result, Silver's casts and DQ rules, quarantine and every Gold dataset run unchanged.
- This is proven on the real dataset: 26/26 manifest steps and 10/10 Gold content hashes are identical between the modes.
- **Trade-off:** in postgres mode the database has already enforced the types, so Silver's `*.cast` rules can no longer fire.
  They stay in place for the files path.

**Connection handling.**
- The JDBC URL holds no credentials; user and password are passed only as JDBC properties.
- `IS_LOCAL=false` adds `sslmode=require`, which Aurora needs.
- A probe query (`SELECT 1`) runs before anything else, with 1 try + 4 retries and 2/4/8/16 s backoff (about 30 s).
  Aurora Serverless v2 at 0 ACU needs that long to resume.
- `connectTimeout=10` stops a dead host from hanging a try.
- If the probe still fails, the run records `FAILED bronze.hr.connection` with host, port and database, never the password.

**Loading the source.**
- `scripts/load_source_postgres.sh` recreates the schema (`DROP … CASCADE; CREATE`) and runs the dataset's **unedited**
  `load.sh` with `PGOPTIONS='-c search_path=<schema>'`. The same environment variables work locally and against Aurora.
- **Cloud:** the cloud job runs files mode. EKS cannot reach Aurora on 5432, and the workshop role explicitly denies
  `ec2:AuthorizeSecurityGroupIngress` (dry-run, 2026-10-15), so the rule cannot be added here. Production would add that rule
  (or use security groups for pods) and take the password from Secrets Manager via IRSA. See the README, "Known limitations".

## Observability and error handling

**What is recorded:**
- Every step records counts (read, written, quarantined, deduped, warned), `duration_s` and `rows_per_s`. Each step has one
  log line with all of them, at INFO, WARNING or ERROR level.
- `rows_per_s` = `rows_read / duration_s` for Bronze and Silver steps, and `rows_written / duration_s` for Gold steps,
  because Gold steps record no `rows_read`.
- The manifest adds the run's start, end, duration and status; `code_version`, the first 12 hex characters of the sha256 of
  `job.py`, which works in the pod without git; `spark_version`; and `source`, the byte size of each HR CSV plus the learning
  file count and total bytes, taken from Hadoop FileStatus for both local paths and S3.

**What fails the run:** source checks run **before Bronze writes anything**. A failure records a `FAILED` step with the
reason, the manifest status becomes `FAILED`, and the exit code is 1. Silver and Gold keep the last good run.

| Workshop error case | Behaviour in this job |
|---|---|
| Database connection timeout | Postgres mode: a connection probe runs before anything is written, with 1 try + 4 retries and exponential backoff (2/4/8/16 s), plus a 10 s JDBC `connectTimeout`. After that it fails with `FAILED bronze.hr.connection` (see Source modes). For S3, timeouts are handled by explicit S3A retries with exponential backoff, for `s3a://` paths only: `fs.s3a.attempts.maximum=10` (AWS SDK retries), `fs.s3a.retry.limit=7`, `fs.s3a.retry.interval=500ms`. These are close to Hadoop 3.3.4's defaults, set explicitly so the policy is visible. If retries run out, the run fails and the manifest is `FAILED`. |
| Schema drift / mismatch | Each CSV header is checked against the full column list from `schema.sql` (`HR_COLUMNS`). A **missing** column fails the run with a message naming the columns, e.g. `schema drift in employees.csv: missing columns ['email']`. An **extra** column logs a WARNING, is kept in Bronze, and is listed in `manifest.source`. The guide suggests quarantining the file instead. Failing is chosen because a table without a required column cannot be loaded at all, and loading the other tables would leave Silver inconsistent (foreign keys, before-hire checks). |
| Malformed / truncated row | Row-level: malformed JSON, bad types, domain and foreign-key violations go to `quarantine/<entity>/run_id=…` with every reason. Valid rows are written. Counts per rule are logged and stored in the manifest. |
| S3 Access Denied | Not retried. The run fails immediately with the AWS error in the log. The fix is permissions: the Kubernetes Secret credentials here, IRSA in production. If the manifest write itself is denied, the job logs `could not write manifest` and exits 1. |
| Zero rows written | Halts: a missing or 0-byte CSV, a CSV with 0 data rows, no learning files, or 0 learning lines all fail the run before anything is written. Every step must also reconcile (`read = written + quarantined + deduped`), and every Gold write checks its grain. |

Bronze checklist:
- Source byte sizes are recorded in the manifest.
- Bronze learning is partitioned by `year=/month=`, with `day` kept as a column. This deliberately differs from the guide's
  `day=DD` example; see Performance.
- Bronze HR is partitioned by `ingest_date`, and a re-run overwrites only that day's partition.

## Performance

**The small-file problem.** The source has 29,868 learning events in 783 daily files, about 38 events per day. Partitioning
Bronze by day therefore produced 782 parquet files of about 9 KB each, and Silver wrote about 3 files per month. On S3,
every file costs list, open, write and commit-rename (copy plus delete) requests. The cloud run spent about 13 min in
Bronze learning and about 6.5 min in Silver learning, of 25 min in total.

**What changed** (layout and caching only; row counts and Gold values are identical):
- **Bronze learning:** partitioned by `year/month`, with `repartition(year, month)` so each month is exactly one file
  (about 37 files instead of 782). `day` stays a column.
- **Raw learning lines are cached.** The 783 landing files are read once; before, the per-file statistics and the write
  each read them.
- **Silver learning:** caches its Bronze read, which was previously scanned for the count, the dedupe, the write and
  the quarantine. It writes with `repartition(event_year, event_month)`, so each partition is one file.
- **Small HR Silver tables, quarantine and every Gold dataset** are written with `coalesce(1)`, so each is one file.
- Each manifest step records `files_written`.

**Measured locally** (real dataset, fresh `lake/` each time, `local[2]`; sum of step `duration_s` per layer):

| Layer | Before: seconds | Before: parquet files | After: seconds | After: parquet files |
|---|---|---|---|---|
| Bronze HR (7 tables) | 8.26 | 7 | 8.33 | 7 |
| Bronze learning | 25.53 | 782 | 12.06 | 37 |
| Silver HR (7 tables) | 12.18 | 7 | 13.38 | 7 |
| Silver learning | 24.01 | 111 | 12.28 | 37 |
| Quarantine | — | 15 | — | 1 |
| Gold (10 datasets) | 11.64 | 10 | 12.41 | 10 |
| **Run total** | **96.04** | **932** | **75.32** | **99** |

- Parquet files fell by 89% (932 → 99). The two learning steps run about 2× faster, and the run as a whole about 22% faster.
- Local disk hides most of the per-file cost. On S3 each avoided file saves several HTTP requests, so the cloud gain
  should be larger. It has not been measured yet; the next cloud run's manifest will show it, since each step records
  `duration_s` and `files_written`.
- A second run on the same lake gave identical counts and identical Gold content hashes. The only extra file was the new
  run's quarantine folder, which is kept per `run_id` on purpose.

**Why not the S3A magic committer.**
- Spark's cloud committer (`PathOutputCommitProtocol`) does not support `partitionOverwriteMode=dynamic`, which Bronze
  relies on to replace only the partitions it writes.
- It also needs the extra `spark-hadoop-cloud` jar.
- With about 99 files per run, the classic committer's rename cost is now small.

**Next options, if more speed is needed:**
- **Compact the source:** monthly instead of daily landing files. Globbing 783 day folders also costs about 800 S3 LIST calls.
- **Give the pod 2 CPUs:** `local[2]` currently shares 1 CPU.
- **Bake pyspark into the image:** skips the ~300 MB pip install at start-up (about 30 s).

## Notebook data source

The notebook (`notebooks/talent_insights.ipynb`) reads only the Gold datasets and the newest run manifest.

**One loader for both modes.**
- `LAKE_ROOT` is read from the environment: a local folder (default `../lake`) or `s3://bucket/prefix`, with `s3a://`
  also accepted. This is the same variable `job.py` uses.
- A `pyarrow.fs` filesystem is chosen once: `LocalFileSystem`, or `S3FileSystem` with the region from `AWS_REGION`
  (default `ap-south-1`).
- `gold(name)` uses `pd.read_parquet(..., filesystem=FS)` and keeps the Decimal→float conversion.
- `latest_manifest()` picks the highest `run_id=` folder under `_runs/` and parses its JSON text file.
- Spark's `_SUCCESS` and `.crc` files are ignored by pyarrow.

**Credentials.** The notebook never contains credentials and never writes. In S3 mode it uses the standard AWS
environment or profile chain, which is `source ENVIRONMENT.config` in this workshop. The exact commands are in the README
("Notebook data source").

**Why direct S3 reads.** The guide asks the notebook to connect to S3 and load Gold. Reading in place shows the cloud
run's actual output, without first downloading a copy (`lake_cloud/`). Both modes produce identical findings.

## Recorded vs derived

**Recorded** values keep their source names and are never recalculated: `rating`, `review_period`,
`proficiency_level`, `required_level`, `potential_rating`, `promotion_readiness`, `attrition_risk`,
`outcome_score`, `outcome_status`, `event_type`, `score`.

**Row-level derived values** carry the `derived_` prefix:

| Column | Dataset | Definition |
|---|---|---|
| `derived_period_start` / `derived_period_end` | Silver reviews, G1 | `YYYY-H1` → Jan 1 – Jun 30; `YYYY-H2` → Jul 1 – Dec 31 |
| `derived_rating_change` | G1, G1b | `rating − previous_rating` (previous period of the same employee) |
| `derived_rating_trend` | G1b, G3, G5, G5b | change > 0 improving, < 0 declining, = 0 stable, null if only one review |
| `derived_gap` | G2 | `required_level − proficiency_level` |
| `derived_gap_severity` | G2 | gap ≥ 2 critical, gap > 0 moderate, otherwise none |
| `derived_high_potential_ready_now` | G3 | `potential_rating = 'High'` and `promotion_readiness = 'Ready Now'` |

**Aggregates** are named after their formula:
- `avg_rating`, `avg_rating_latest`, `avg_outcome_score`, `avg_proficiency`, `avg_required`: averages, rounded to 2 decimals.
- `pct_rated_4_plus`, `pct_meeting_requirement`, `on_time_rate`, `pct_of_risk_band`: shares between 0 and 1.
- `*_count`, `headcount`, `courses_*`, `total_events`, `critical_gap_count`: counts. Employees with no activity get 0, not null.
- `is_course_completed`: true only if a `course_completed` event exists. Module or quiz events never imply completion.

**DQ flags** (`dq_*`) mark data problems in Gold without removing rows:
- `dq_reviewed_before_hire` (G1)
- `dq_assessed_before_hire` (G2)
- `dq_completion_without_enrollment` (G4)

Gold describes associations, not causes. G3 is an analytic shortlist, not an HR decision, and G5 is context for the
recorded `attrition_risk`, not a prediction.

## Reference numbers

These are the expected results on the workshop dataset. `tests/test_pipeline_integration.py` asserts them, and the cloud run
matches them (see `test_evidence.md`).

| Area | Expected |
|---|---|
| HR rows | employees 2,000 · teams 100 · competencies 40 · projects 33 · performance_reviews 4,000 · competency_assessments 4,000 · talent_review 2,000 |
| Learning | 783 files / 29,868 lines → 13 malformed, 152 duplicates, 76 null employee_id, 53 invalid dates, 29,574 valid |
| Warnings | 666 reviews period-ended-before-hire · 234 assessments before hire · 38 `notes` instead of `text` (39 raw lines) · 5,161 events before hire |
| Gold | gaps −1/0/1/2 = 1,480/1,527/668/325 (critical 325) · High + Ready Now 357 · attrition L/M/H 1,077/612/311 · trend improving/stable/declining 898/872/230 · 27 teams with projects (33 projects) · 3,255 completion-without-enrollment pairs (3,262 raw) |

Counts after Silver differ slightly from raw profiling (38 vs 39, 3,255 vs 3,262) because duplicates and invalid
rows are removed first.
