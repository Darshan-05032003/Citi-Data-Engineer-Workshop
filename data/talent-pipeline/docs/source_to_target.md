# Source-to-target mapping

Column lineage per layer, as built by `job.py`. Rule IDs are `<entity>.<column>.<check>`; the tables
show only the `<check>` part (`(W)` = warning, row kept and flagged in `dq_warnings`; otherwise error →
quarantine). Rule ID lists were generated from `job.RULES`; column lists from the written Parquet schemas.

## 1. Source → Bronze (no rules; nothing parsed or cast)

| Source | Target | Transform |
|---|---|---|
| `<table>.csv`, every column | `bronze/hr/<table>/` same name | read as **string** (header, no inference, CRLF stripped) |
| — | `_run_id`, `_ingested_at` | run id / run start timestamp |
| — | `_source_system`, `_source_file` | `'file'`, `input_file_name()` |
| — | `ingest_date` (partition) | UTC date of the run |
| JSONL line | `bronze/learning_activity/` `value` | raw line, blank lines dropped |
| line position | `_line_no` | 1-based line number in its file |
| file path | `_source_file`; `year`/`month` (partition folders), `day` (column) | path; regex on `year=/month=/day=` (strings) |
| — | `_record_hash`, `_run_id`, `_ingested_at` | `sha2(value, 256)`; run id / timestamp |

## 2. Bronze → Silver HR (read newest `ingest_date` folder; parents validated first)

Common to every table: Bronze lineage kept (`_ingested_at`, `_source_system`, `_source_file`),
`ingest_date` → `_bronze_ingest_date`, `_run_id` = this run, `dq_warnings` = failed warning rule IDs.
Column names are unchanged; all checks run on the raw string (`not_null`) and the typed value.

| Table | Column | Type | Rules |
|---|---|---|---|
| teams | team_id | int | not_null, cast, unique |
| | name, department | string | not_null |
| competencies | competency_id | int | not_null, cast, unique |
| | name | string | not_null |
| | category | string | not_null, domain (Technical/Leadership/Domain) |
| employees | employee_id | int | not_null, cast, unique |
| | first_name, last_name, email, job_level | string | not_null |
| | team_id | int | not_null, cast, fk → teams |
| | hire_date | date | not_null, cast |
| projects | project_id | int | not_null, cast, unique |
| | name | string | not_null |
| | team_id | int | not_null, cast, fk → teams |
| | outcome_status | string | not_null, domain (on-time/delayed/at-risk/cancelled) |
| | outcome_score | decimal(5,2) | not_null, cast |
| performance_reviews | review_id | int | not_null, cast, unique |
| | employee_id | int | not_null, cast, fk → employees |
| | review_period | string | not_null, domain (`^\d{4}-H[12]$`) |
| | rating | int | not_null, cast, domain (1–5) |
| | submitted_date | date | not_null, cast |
| | manager_comment | string | — (nullable) |
| | **derived_period_start / derived_period_end** | date | derived: H1 = Jan 1–Jun 30, H2 = Jul 1–Dec 31; `derived_period_end.before_hire(W)` |
| competency_assessments | assessment_id | int | not_null, cast, unique |
| | employee_id | int | not_null, cast, fk → employees |
| | competency_id | int | not_null, cast, fk → competencies |
| | proficiency_level, required_level | int | not_null, cast, domain (1–5) |
| | assessed_date | date | not_null, cast, before_hire(W) |
| talent_review | employee_id | int | not_null, cast, unique, fk → employees |
| | review_cycle | string | not_null |
| | potential_rating | string | not_null, domain (Low/Medium/High) |
| | promotion_readiness | string | not_null, domain (Ready Now/1-2 Years/Not Ready) |
| | attrition_risk | string | not_null, domain (Low/Medium/High) |

Quarantine rows (`quarantine/<table>/run_id=<id>/`) keep the raw strings (`_chk_raw_<col>`), check flags,
`rule_ids`, `dq_reasons`, `_run_id`, `_quarantined_at`.

## 3. Bronze → Silver learning_events

Order: parse → malformed check → **dedupe on `event_id`** (keep first by `_source_file`, `_line_no`) → rules.

| Source | Target | Type | Transform | Rules (entity `learning_events`) |
|---|---|---|---|---|
| `value` | `value` | string | kept (raw line) | `raw.malformed_json` (entity `learning_events_raw`, runs before dedupe) |
| JSON `event_id` | `event_id` | string | parsed | dedupe key (duplicates counted, not stored) |
| JSON `employee_id` | `employee_id` | int | parsed, cast | not_null, cast, fk → employees |
| JSON `event_date` | `event_date` | string | parsed (kept as recorded) | — |
| `event_date` | `event_ts` | timestamp | `to_timestamp(…, "yyyy-MM-dd'T'HH:mm:ss")` | `event_ts.invalid`; `event_ts.before_hire(W)` |
| `event_ts` | `event_year`, `event_month` | int | year/month (partition folders) | — |
| JSON `event_type` | `event_type` | string | parsed | domain (7 types) |
| JSON `course_name` | `course_name` | string | parsed | — |
| JSON `text`, `notes` | `text`, `notes` | string | parsed (kept) | — |
| `text`, `notes` | `event_text` | string | `coalesce(text, notes)` | — |
| `text`, `notes` | `text_source_field` | string | `'text'` / `'notes'` | `event_text.notes_used(W)` |
| JSON `score` | `score` | double | parsed as string, cast | cast, domain (0–100, null allowed) |
| Bronze | `_line_no`, `_source_file`, `_record_hash`, `_ingested_at` | | kept | — |
| — | `dq_warnings`, `_run_id` | | warning rule IDs; this run | — |

## 4. Silver → Gold (Silver only; children aggregated to the grain before joins)

`R` = recorded (source value), `D` = derived (`derived_` prefix), `A` = aggregate. Every dataset also has
`_gold_run_id`, `_built_at`; grain keys are checked unique on write.

| Dataset (grain) | Target columns | Built from | Kind |
|---|---|---|---|
| G1 `employee_performance_history` (employee × period) | employee_id, review_period, rating, derived_period_start/end | silver performance_reviews | R / D |
| | previous_rating; derived_rating_change | `lag(rating)` per employee; rating − previous | D |
| | dq_reviewed_before_hire | `dq_warnings` contains `…derived_period_end.before_hire` | D |
| G1b `employee_performance_summary` (employee) | avg_rating, review_count | G1 per employee | A |
| | latest_period, latest_rating, derived_rating_change | G1 at max(review_period) | R |
| | derived_rating_trend | change > 0 improving, < 0 declining, 0 stable | D |
| G2 `employee_competency_gaps` (employee × competency) | proficiency_level, required_level, assessed_date | silver competency_assessments | R |
| | competency_name, competency_category | silver competencies | R |
| | derived_gap; derived_gap_severity | required − proficiency; ≥ 2 critical (parameter), > 0 moderate, else none | D |
| | dq_assessed_before_hire | from `dq_warnings` | D |
| G3 `promotion_readiness` (employee) | review_cycle, potential_rating, promotion_readiness | silver talent_review | R |
| | derived_high_potential_ready_now | High AND Ready Now | D |
| | latest_rating, derived_rating_trend; critical_gap_count; courses_completed | G1b; G2 rollup; G4b | A |
| G4 `learning_progress` (employee × course) | `<event_type>_count` ×7, total_events, first/last_event_ts | silver learning_events | A |
| | is_course_completed; completed_at | `course_completed_count > 0` only; min ts of that event | D |
| | best_quiz_score; has_certification | max score of quiz events; `certification_earned_count > 0` | A / D |
| | dq_completion_without_enrollment | completed AND enrollment_count = 0 | D |
| G4b `learning_summary` (employee) | courses_enrolled/completed/certified, total_events | G4 per employee, left join from employees, 0 not null | A |
| G5 `attrition_risk_context` (employee) | review_cycle, attrition_risk | silver talent_review | R |
| | rating_2024_H2, rating_2025_H1 | G1 pivoted by period (names from data) | R |
| | derived_rating_trend; critical_gap_count; G4b totals | G1b; G2 rollup; G4b | A |
| G5b `attrition_by_trend` (risk × trend) | headcount; pct_of_risk_band | G5 count; headcount ÷ band total | A |
| G6 `team_performance_outcomes` (team) | team_name, department | silver teams | R |
| | headcount | employees per team | A |
| | avg_rating_latest, pct_rated_4_plus, rating_period | latest-period reviews per team | A |
| | project_count, avg_outcome_score, on_time_rate; has_projects | projects per team (before join); project_count > 0 | A / D |
| G7 `competency_distribution` (competency × department) | assessed_count, avg_proficiency, avg_required, pct_meeting_requirement, level_1…5_count | assessments → employee → team department | A |
| | competency_name, competency_category | silver competencies | R |
