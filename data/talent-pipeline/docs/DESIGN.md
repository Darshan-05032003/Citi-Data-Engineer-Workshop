# Design: ACME Talent Pipeline

One PySpark job (`job.py`) builds a Bronze → Silver → Gold lake from ACME's HR extracts and learning-activity
logs. It writes a run manifest for every run. The same file runs locally and as a Kubernetes Job on EKS against S3.

## Architecture

```
SOURCE_DIR (7 HR CSVs + learning_activity/*.jsonl)
  → Bronze   raw, all strings, + lineage (_run_id, _ingested_at, _source_file …)
  → Silver   typed, validated: errors → quarantine/, warnings → dq_warnings
  → Gold     10 analysis datasets, one grain each (duplicate keys fail the write)
  → _runs/   manifest JSON: status + per-step read/written/quarantined/deduped/warned
```

| Layer | Contract |
|---|---|
| Bronze HR | CSV rows as strings, one partition per `ingest_date`; nothing dropped |
| Bronze learning | one row per raw JSONL line (`value`, `_line_no`, `_source_file`, `_record_hash`), partitioned like the source (`year/month/day`) |
| Silver | typed columns, every DQ rule applied, row-level `dq_warnings`; learning events partitioned by `event_year/event_month` |
| Quarantine | failing rows with all `rule_ids` and `dq_reasons`, per entity and `run_id` |
| Gold | G1 employee×period, G1b employee, G2 employee×competency, G3 employee, G4 employee×course, G4b employee, G5 employee, G5b attrition_risk×trend, G6 team, G7 competency×department |

Code order in `job.py`: config → logging → Spark session → DQ rules → apply_rules/quarantine → Bronze → Silver →
Gold (one function per dataset) → manifest → `main()`. Nothing runs at import time.

## Decisions

- **Files instead of Postgres.** Bronze reads the CSVs and JSONL files directly from `SOURCE_DIR`, so the local
  and cloud runs share one code path and need no database.
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

These are the expected results on the workshop dataset. `tests/test_pipeline.py` asserts them, and the cloud run
matches them (see `test_evidence.md`).

| Area | Expected |
|---|---|
| HR rows | employees 2,000 · teams 100 · competencies 40 · projects 33 · performance_reviews 4,000 · competency_assessments 4,000 · talent_review 2,000 |
| Learning | 783 files / 29,868 lines → 13 malformed, 152 duplicates, 76 null employee_id, 53 invalid dates, 29,574 valid |
| Warnings | 666 reviews period-ended-before-hire · 234 assessments before hire · 38 `notes` instead of `text` (39 raw lines) · 5,161 events before hire |
| Gold | gaps −1/0/1/2 = 1,480/1,527/668/325 (critical 325) · High + Ready Now 357 · attrition L/M/H 1,077/612/311 · trend improving/stable/declining 898/872/230 · 27 teams with projects (33 projects) · 3,255 completion-without-enrollment pairs (3,262 raw) |

Counts after Silver differ slightly from raw profiling (38 vs 39, 3,255 vs 3,262) because duplicates and invalid
rows are removed first.
