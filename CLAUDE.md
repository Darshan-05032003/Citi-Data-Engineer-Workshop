# Project: ACME Talent Pipeline (Citi DE workshop), deadline mode
Job dir: data/talent-pipeline/ · Plan: data/talent-pipeline/docs/PLAN.md (source of truth) · Log: data/talent-pipeline/docs/PROGRESS.md

## Workflow for EVERY task
1. Restate the task simply + which requirement it satisfies.
2. Inspect the relevant files first; run `git status`.
3. Propose scope, exact files, behaviour, acceptance criteria. STOP and wait for "approved".
4. Implement only the approved scope: small, readable code; type hints; docstrings; PEP 8.
5. Run the relevant commands/tests and show REAL output. Never invent results.
6. Explain the code, results and limitations; list every command you ran.
7. End with the PROGRESS REPORT block and append it to docs/PROGRESS.md.

## Current phase: DEPLOYMENT — builder is read-only on AWS; Darshan runs deploy/apply
- Dataset (source, read-only, never commit): sampleData/data-engineering-sample-performance-scenario/ (ignored via .git/info/exclude).

## Layout rules (single-file job)
- ALL pipeline logic lives in data/talent-pipeline/job.py: the cloud receives only job.py + requirements.txt.
- Section order: config → logging → spark session → DQ rules → apply_rules/quarantine → bronze → silver → gold (one function per dataset) → run manifest → main().
- Nothing runs at import time: no SparkSession and no os.environ reads at module level. Everything starts in main() under `if __name__ == "__main__":`.
- All data IO goes through Spark (the manifest too), so the same code works for a local LAKE_ROOT and s3a://.
- Config comes only from env vars: IS_LOCAL, SOURCE_DIR, LAKE_ROOT. Same job.py locally and in cloud.
- No Postgres/JDBC for now — sources are files in SOURCE_DIR.
- Versions match the cloud image (Spark 3.5.0, Python 3.11, Java 17, Hadoop 3.3.4): requirements.txt = pyspark==3.5.0 only. Spark packages: only org.apache.hadoop:hadoop-aws:3.3.4, added only for s3a:// paths. No Iceberg. Never put pandas/numpy in requirements.txt.
- Tests live in data/talent-pipeline/tests/ and run locally only.

## Hard rules
- NEVER run git push; never add or change remotes. Commit locally only when I say "commit".
- NEVER run terraform apply/destroy, bin/deploy-backend.sh, bin/cleanup-environment.sh or bin/setup-participant.sh. I run those myself after reviewing a plan. You may run terraform init/plan/validate/fmt/output/console and read-only aws/kubectl/helm commands.
- Shell env vars don't persist between your commands: prefix AWS commands with `source ENVIRONMENT.config &&`. Never print ENVIRONMENT.config, AWS keys/tokens or POSTGRES_PASS; print variable NAMES or yes/no only.
- Never modify the original dataset files, schema.sql or load.sh. Don't touch frontend/ or backend/. Infra changes only as listed in PLAN.md §8.2.
- A failing test means diagnose; never weaken or skip it.
- Don't name folders lib/, build/, logs/, tmp/, assets/ (root .gitignore ignores them).
- Label recorded vs derived fields; no causal claims.

## Expected numbers
HR: employees 2000, teams 100, competencies 40, projects 33, performance_reviews 4000, competency_assessments 4000, talent_review 2000.
Learning: 783 files / 29,868 lines → 13 malformed, 152 duplicates, 76 null employee_id, 53 invalid dates, 29,574 valid.
Warnings: 666 reviews period-ended-before-hire; 234 assessments before hire; notes-instead-of-text 38 after dedupe (39 lines); events before hire 5,161.
Gold: gaps −1/0/1/2 = 1480/1527/668/325 (critical 325); HiPo+Ready Now 357; attrition L/M/H 1077/612/311; teams with projects 27; project_count sum 33; completion-without-enrollment 3,255 employee×course pairs after Silver (3,262 raw).

## Output style
- Be concise. Plan ≤ 10 bullets; explanation ≤ 5 bullets.
- Show only relevant output lines: pytest -q, tail -n 30 for logs, counts instead of data dumps, ≤ 3 sample rows, diffs instead of full files.
- Never re-read or re-print large files (PLAN.md, data, job.py) unless the task needs it.
- PROGRESS REPORT stays complete, but one line per item.

## PROGRESS REPORT format
=== PROGRESS REPORT — Task <id>: <name> ===
Status: DONE | PARTIAL | BLOCKED
Files created/modified: <paths>
Commands run: <each command> → <one-line real result>
Tests: <pytest summary line, or "none">
Acceptance criteria: <each criterion> → ✓ / ✗ (+ actual value)
Key numbers: <counts/values produced>
Decisions/assumptions made: <...>
Problems or questions for Darshan: <...>
Git: branch=<name>; `git status --short` → <output>
=== END REPORT ===
