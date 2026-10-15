# ACME Talent Pipeline: Deadline Plan (v2, single job.py + required cloud deploy)

Citi Data Engineering Workshop · Builder: Claude Code in VS Code · Coach: claude.ai chat
**Deadline: tomorrow (15 Oct) afternoon IST, then a 3-minute live demo to a mentor panel. Cloud deployment is required.**

What v2 changes:
- All pipeline logic lives in one `job.py`. This resolves D3.
- Cloud deployment is now in scope, with the infra changes it needs (§8).
- The sequence is rebuilt around the deadline, with a cloud "walking skeleton" deployed early (§7).
- `CLAUDE.md` and every remaining prompt are updated (§10).

**What was verified for v2 (14 Oct, 23:00 IST):**
- Re-read the repo at upstream `4c38b17`. The only change since `56bd0bb` is the example's `pyspark` bump from 3.5.4 to 3.5.8; the Iceberg-for-Spark-4.1 conflict is still there.
- Inspected the Docker Hub tags of `jupyter/all-spark-notebook` and the docker-stacks Dockerfile at that date.
- Checked Spark 3.5.0's and Hadoop 3.3.4's build files.
- Searched both of your chats for PROGRESS reports. None were found, and nothing has been implemented yet, so the prompts start from zero (Step 0, then Task 0).

---

## 0. Working agreement
- **No pushing to GitHub.** Nobody runs `git push` or adds/changes a remote until Darshan names a destination. Local commits only when he says "commit".
- **Small steps.** One prompt = one reviewable change.
- **The loop:** the coach (this chat) writes a prompt → Darshan pastes it into Claude Code → the builder plans and waits for `approved`, then builds, tests, explains and ends with a PROGRESS REPORT → Darshan pastes the report back to the coach.
- **One prompt at a time (agreed 14 Oct, 23:18):** the coach gives the next prompt only after Darshan pastes the previous prompt's result. As of 23:18 on 14 Oct nothing was implemented, so work starts at Task S. The prompts in §10.3 are the blueprint; each one may be adjusted to the actual results before it's given.
- **Token-efficient prompts (agreed 15 Oct, 09:42):** every builder prompt uses the fixed format `Task <id> — <name>. Ref: PLAN.md §x` / DO / DON'T / ACCEPT / SHOW, references PLAN.md and CLAUDE.md instead of repeating them, and has its model + effort on one line above it. The coach's replies are the next prompt plus at most 2 lines on what to check. The §10.3 prompts are rewritten into this format as they're issued.
- **Local first (agreed 15 Oct, 09:48):** no infra changes and nothing deployment-related (C1/C2/C4–C6, AWS, Terraform) until Darshan says so. Build and test the full pipeline locally first. All pipeline code lives only in `job.py`; supporting files (requirements, pytest.ini, tests, scripts, notebook, docs) are allowed. Local order: S → R → 1a → 1b → 2 → 3 → 4 → 5 → 6 → 7 → 8 → N1 → D1, then deployment.
- No Postgres (15 Oct): Bronze reads the 7 CSVs + learning_activity JSONL directly from SOURCE_DIR (local folder now, S3 later). Config = IS_LOCAL, SOURCE_DIR, LAKE_ROOT. Task 2 dropped; order 1b → 3 → 4 → 5 → 6 → 7 → 8 → N1 → D1.
- **The two Claudes share no memory.** `CLAUDE.md` holds the builder's rules, `docs/PROGRESS.md` holds the history, and this doc holds the plan.
- **Cloud safety:** the builder may run `terraform init/plan/validate/fmt/output/console` and read-only `aws`/`kubectl` commands. **Darshan alone runs** `deploy-backend.sh`, `setup-participant.sh`, `cleanup-environment.sh` and any `terraform apply/destroy`, and only after reviewing a plan.

---

## 1. Verified facts that drive the design

| Fact | Evidence | Consequence |
|---|---|---|
| The cloud job receives **only `job.py` + `requirements.txt`** | `infra/helm/templates/python-job.yaml` (ConfigMap with 2 keys) | One `job.py` holds all logic. Tests and scripts stay local. |
| The Helm Job has **no `env:` section** | Same template; no env block | ⚠️ **An infra change is required.** Without it, the cloud job never receives `LAKE_ROOT` or the `POSTGRES_*` values (§8.2). |
| Job name in the cluster is **`coding-workshop-talent-pipeline`** | Template name = `<chart name>-<pythonJob.name>`; chart = `coding-workshop` | Use `kubectl logs -f job/coding-workshop-talent-pipeline`. The guide's `job/{{job-name}}` is wrong. |
| `ttlSecondsAfterFinished: 100` | `infra/helm/values.yaml` | The finished pod and its logs are **deleted after 100 s**. Raise it to 1800 for the demo (§8.2). |
| Cloud image `jupyter/all-spark-notebook:latest` = **Spark 3.5.0, Python 3.11.6, OpenJDK 17, Hadoop 3, pandas 2.0.3** | Docker Hub: `latest` has the same digest as tags `spark-3.5.0`, `python-3.11`, `hadoop-3`, last updated 2023-10-20. docker-stacks Dockerfile from that date. | Pin everything to Spark 3.5.0. The Docker Hub image is frozen and is **no longer updated** (new images are on quay.io), but it's stable. |
| In that image, PySpark is **not pip-installed**. It becomes importable only through a notebook start-up hook. The Helm Job overrides that with `sh -c` (it bypasses `tini`/`start.sh`). | Dockerfile: Spark at `/usr/local/spark`; PYTHONPATH set only by the `before-notebook.d/spark-config.sh` hook | `requirements.txt` **must** include `pyspark==3.5.0`. It matches the image's JVM exactly because `SPARK_HOME=/usr/local/spark` is set. Don't pin pandas or numpy; that would upgrade the image's pandas 2.0.3. |
| Spark 3.5.0 is built with **Hadoop 3.3.4**; Hadoop 3.3.4 uses **aws-java-sdk-bundle 1.12.262** | `apache/spark@v3.5.0/pom.xml`, `apache/hadoop@rel/release-3.3.4/hadoop-project/pom.xml` | Use `spark.jars.packages = org.apache.hadoop:hadoop-aws:3.3.4` (it pulls the SDK bundle) plus `org.postgresql:postgresql:42.7.4`. |
| `pip install pyspark==3.5.0` downloads a ~300 MB package on **every** cloud run | pyspark sdist size | The cloud job takes a few minutes before Spark logs start. That's expected; say so in the demo. |
| No data-lake bucket exists. IAM allows `s3:*` on `${app_name}*${app_id}*` | `infra/s3.tf`, `infra/policy.tftpl` | Add `coding-workshop-datalake-<app_id>`. It follows the same naming as the existing `coding-workshop-website-<app_id>`. |
| Aurora PG 17.7 Serverless v2, **min capacity 0** (auto-pauses) | `infra/rds.tf` | The first connection after a pause can be slow, so JDBC needs a retry with backoff. DB name is `codingworkshop`, user `superadmin`. |
| The EKS step **fails if `aws_ds_ip` is empty** | `infra/eks.tf` `null_resource.helm_chart` | `setup-participant.sh` must yield `TF_VAR_aws_ds_ip`. C1 checks this. |
| `deploy-backend.sh` runs `setup-participant.sh` and then **`terraform apply -auto-approve`** | `bin/deploy-backend.sh` | Plan first, always. Darshan runs it. |
| `cleanup-environment.sh` **zips the whole project and uploads it to the workshop's S3** (`backup-<participant>.zip`), then runs `terraform destroy -auto-approve` | `bin/cleanup-environment.sh` | It may be how organizers collect submissions. **Ask them before running it.** |
| Root `.gitignore` ignores `lib/ build/ logs tmp assets *.config` | `.gitignore` | Don't use those folder names. |

**Unverified risks, checked by early prompts:**
- **R1:** Can the EKS **pod** reach S3 using the node's IAM role? (instance metadata hop limit; role permissions)
- **R2:** Can the pod reach **Aurora** on 5432? Security groups: nodes use the EKS cluster SG, while Aurora uses `coding-workshop-<app_id>`.
- **R3:** Can the **VDI** reach Aurora to load the CSVs?
- **R4:** Is 2 GiB enough for a 1 GiB Spark driver? It should be (§8.4).
- **R5:** Will the JupyterHub placeholder pod leave CPU for the job on the single t3.medium node?
- **R6:** How long do the temporary AWS credentials last?

C1 and C4a test all of these early, so there's time to react or ask the organizers.

---

## 2. Dataset facts (unchanged from v1, verified by profiling)

**HR tables:** employees 2,000 · teams 100 · competencies 40 · projects 33 · performance_reviews 4,000 · competency_assessments 4,000 · talent_review 2,000. 0 orphan keys. All values satisfy the `schema.sql` CHECK constraints.

**Learning JSONL:** 783 files (1 empty: `year=2025/month=09/day=03`), 29,868 non-blank lines, which reconcile exactly:

| Category | Count |
|---|---|
| Malformed JSON | 13 |
| Exact duplicates (same event_id, same file) | 152 |
| Null employee_id | 76 |
| Invalid event_date (`yesterday`, empty, `N/A`, `2026-13-45`) | 53 |
| **Valid** | **29,574** |

**Warnings (flag, don't drop):**
- 666 reviews (396 employees) are for a period that ended before the hire date.
- 234 assessments are dated before hire.
- 5,161 events (after dedupe) are dated before hire.
- 38 events after dedupe (39 lines) use `notes` instead of `text`.
- 3,255 employee×course pairs after Silver (3,262 raw) have a completion but no enrollment event.

**Limits that shape the analysis:**
- There are only 2 review periods, so a trend is a single change.
- There's 1 talent-review cycle.
- Projects link only to teams (27 of 100 teams) and have no dates.
- There's no "key competency" flag.
- No mapping exists from courses to competencies.

**Source mapping:**
- The 7 CSVs load into Postgres schema `hr_source`: local Postgres locally, Aurora in the cloud.
- `learning_activity/` is copied to `{LAKE_ROOT}/landing/learning_activity/`: a local folder locally, the S3 lake bucket in the cloud.

---

## 3. Layout and architecture (single job.py)

```
CLAUDE.md                              builder rules (repo root)
.claude/settings.local.json            deny list (push, apply/destroy, deploy/cleanup/setup scripts)
infra/s3.tf, output.tf, eks.tf,        small, approved infra changes (§8.2)
infra/helm/values.yaml, templates/python-job.yaml
data/talent-pipeline/
  job.py                ← EVERYTHING the pipeline does (shipped to cloud)
  requirements.txt      ← pyspark==3.5.0 only (shipped to cloud)
  requirements-dev.txt  ← pyspark==3.5.0, pytest, pytest-cov, pandas<3, pyarrow, matplotlib, jupyter
  pytest.ini            ← pythonpath = .  (tests can `import job`)
  .gitignore            ← lake/ lake_cloud/ .venv/ source_data/ spark-warehouse/ metastore_db/
  README.md
  scripts/load_source_postgres.sh   scripts/stage_landing.sh
  tests/conftest.py  tests/fixtures/  tests/test_rules.py  tests/test_pipeline.py
  notebooks/talent_insights.ipynb
  docs/PLAN.md  docs/PROGRESS.md  docs/test_evidence.md
```

**Sections in `job.py`** (in order, each with a header comment):

1. **Config**: `load_config()` reads `IS_LOCAL`, `POSTGRES_*` and `LAKE_ROOT` from env. Called only in `main()`.
2. **Logging**: `get_logger(run_id)` writes `INFO/WARN/ERROR` lines with the run_id. It never logs the password.
3. **Spark session**: `create_spark(cfg)` uses `local[2]`, `spark.driver.memory=1g`, 4 shuffle partitions, dynamic partition overwrite, and the Postgres + hadoop-aws packages.
4. **DQ rules**: a declarative list `RULES = [Rule(rule_id, entity, column, severity, condition, description), ...]`.
5. **Quality engine**: `apply_rules(df, entity)` returns valid rows (with a `dq_warnings` column) and quarantined rows (with `rule_id`, `dq_reason`, `_run_id`). `write_quarantine()` saves the rejects.
6. **Bronze**: `bronze_hr()` and `bronze_learning()`.
7. **Silver**: `silver_hr()` and `silver_learning()`.
8. **Gold**: `gold_performance_history` (G1), `gold_performance_summary` (G1b), `gold_competency_gaps` (G2), `gold_promotion_readiness` (G3), `gold_learning_progress` (G4), `gold_learning_summary` (G4b), `gold_attrition_context` (G5/G5b), `gold_team_outcomes` (G6), `gold_competency_distribution` (G7).
9. **Run manifest**: `RunManifest` records per-step rows read/written/quarantined/deduped, start/end/duration and status. It's written to `{LAKE_ROOT}/_runs/run_id=<id>/` **through Spark**, so local paths and `s3a://` both work.
10. **main()**: only under `if __name__ == "__main__":`. Nothing runs at import.

**Lake layout (identical locally and in S3):**

```
{LAKE_ROOT}/landing/learning_activity/year=/month=/day=/events-000.jsonl
{LAKE_ROOT}/bronze/hr/<table>/ingest_date=YYYY-MM-DD/          (+ _run_id, _ingested_at, _source_system, _source_object)
{LAKE_ROOT}/bronze/learning_activity/year=/month=/day=/         (raw line, _line_no, _source_file, _record_hash)
{LAKE_ROOT}/silver/<entity>/        {LAKE_ROOT}/quarantine/<entity>/run_id=<id>/
{LAKE_ROOT}/gold/<dataset>/         {LAKE_ROOT}/_runs/run_id=<id>/
```

**Idempotency:**
- Bronze overwrites only its own `ingest_date` partition.
- Silver and Gold are deterministic full rebuilds with overwrite.

---

## 4. Gold datasets (definitions unchanged from v1)

| # | Dataset | Grain | Must-pass checks |
|---|---|---|---|
| G1 | employee_performance_history | employee × period (4,000) | Unique (employee, period); `rating_change` = 2025-H1 − 2024-H2 |
| G1b | employee_performance_summary | employee (2,000) | `derived_rating_trend` improving/stable/declining sums to 2,000 |
| G2 | employee_competency_gaps | employee × competency (4,000) | Gap counts −1/0/1/2 = 1480/1527/668/325. Critical (gap ≥ 2, configurable) = 325 |
| G3 | promotion_readiness | employee (2,000) | `derived_high_potential_ready_now` = 357. No fan-out. |
| G4 | learning_progress | employee × course | Sum of event counts = 29,574. Completion = `course_completed` event only. |
| G4b | learning_summary | employee (2,000) | Zeros, not nulls |
| G5 | attrition_risk_context | employee (2,000) | Recorded risk L/M/H = 1077/612/311 |
| G5b | attrition_by_trend | risk × trend | Headcount sums to 2,000 |
| G6 | team_performance_outcomes | team (100) | Projects pre-aggregated per team. project_count sum = 33; has_projects = 27; headcount sum = 2,000 |
| G7 | competency_distribution | competency × department | Assessed count sum = 4,000 |

Business questions: Q1 → G1/G1b · Q2 → G2 · Q3 → G3 · Q4 → G4/G4b · Q5 → G5/G5b · Q6 → G6 · Q7 → G7

Language rules for the demo:
- "Recorded" means the value comes from source; `derived_` means we calculated it.
- Q5 and Q6 show association, not causation.
- G3 is an analytic shortlist, not an HR decision.

---

## 5. Data quality and lineage (unchanged logic, now inside job.py)

- **Errors go to quarantine:** missing or duplicate PK, failed cast, out-of-domain value, orphan FK, malformed JSON, null employee_id, invalid event date.
- **Warnings are flagged in `dq_warnings`:** period ended before hire, assessment/event before hire, completion without enrollment, `notes` used instead of `text`.
- **Learning events are deduped on `event_id` before validation.** The count is logged.
- **Reconciliation per step:** read = written + quarantined + deduped. It's asserted in the manifest and in `test_pipeline.py`.

---

## 6. Tests (local only)

| File | Contents |
|---|---|
| `tests/conftest.py` | Session-scoped `local[1]` Spark fixture; a temporary `LAKE_ROOT` |
| `tests/fixtures/` | Tiny CSV/JSONL samples (malformed line, duplicate, null employee, bad date, `notes` field, blank line) |
| `tests/test_rules.py` | Unit: each rule type passes and fails; `apply_rules` splits correctly; dedupe; the `notes`→`text` coalesce; critical-gap threshold; no fan-out in G6 |
| `tests/test_pipeline.py` | Integration (needs local Postgres with `hr_source` loaded and local landing): every §2/§4 count, reconciliation, and run twice → identical Gold counts and no duplicate keys |

**Cloud evidence** isn't a pytest test. It's the cloud manifest showing the same counts as the local manifest (C5/C6).

---

## 7. Deadline sequence

**Recommendation (a change to your requested order):** deploy a *walking skeleton* to the cloud right after Bronze works locally, instead of waiting until the end.

The cloud has 6 unknowns (R1–R6) that only show up there, and EKS takes about 20–30 minutes to come up. Finding out at noon tomorrow that the pod can't reach Aurora would sink the demo. While EKS provisions, you keep building Silver and Gold locally.

| # | Prompt | When | Cut if late? |
|---|---|---|---|
| Step 0 | You: clone, folders, copy plan, unzip dataset | Tonight | No |
| 0 | Read-only check + create CLAUDE.md, PROGRESS.md, .gitignore, deny list | Tonight | No |
| 1a | Project files + venv | Tonight | No |
| 1b | job.py skeleton (config, logging, Spark, manifest, main) | Tonight | No |
| 2 | Source scripts + local load | Tonight | No |
| 3 | Bronze | Tonight | No |
| C1 | Cloud preflight + `terraform plan` (read-only) | Tonight | **Never** |
| C2 | Infra changes (bucket, Helm env/Secret, ttl) + plan | Morning, first thing | **Never** |
| — | **You run** `deploy-backend.sh aws` (~20–30 min) | Morning | **Never** |
| 4 | DQ engine + unit tests (while EKS provisions) | Morning | Trim to 1 test per rule type |
| C4a | Verify cloud + connectivity tests (R1–R5) | Morning | **Never** |
| C4b | Load Aurora `hr_source` + upload landing to S3 | Morning | **Never** |
| C5 | First cloud run (Bronze) + evidence | Morning | **Never** |
| 5 | Silver (HR + learning) | Late morning | No |
| 6 | Gold part A: G1, G1b, G2, G5, G5b, G7 | Midday | No |
| 7 | Gold part B: G4, G4b, G3, G6 | Midday | No |
| 8 | Integration test + run-twice idempotency | Midday | Keep counts; idempotency can be shown by a manual second run |
| C6 | Final cloud run + local-vs-cloud manifest comparison | Early afternoon | **Never** |
| N1 | Notebook Q1–Q7 | Early afternoon | Shrink to 1 chart per question with a short note |
| D1 | README + test_evidence + limitations | Before the demo | README only |
| C7 | Cleanup (after evaluation, organizers consulted) | After the demo | n/a |

**Already cut:** LocalStack S3, Iceberg, performance tests, coverage targets, publishing Gold to Postgres, JupyterHub notebook in the cloud.

**3-minute demo outline:**
1. **Problem and architecture (30 s):** diagram from Postgres + S3 through Bronze/Silver/Gold to the notebook.
2. **Cloud run (60 s):** `kubectl logs` of `coding-workshop-talent-pipeline`, then the manifest in S3 with counts.
3. **Data quality (30 s):** "29,868 lines = 13 malformed + 152 duplicates + 76 + 53 quarantined + 29,574 valid; local and cloud match."
4. **Notebook (50 s):** 3 strongest charts (Q2 gaps, Q3 promotion, Q5 attrition vs trend) with the recorded/derived caveat.
5. **Close (10 s):** tests, idempotency, what's next.

---

## 8. Cloud deployment design

### 8.1 What runs where
| Component | Local (VDI) | Cloud (AWS) |
|---|---|---|
| HR source | Local Postgres, schema `hr_source` | Aurora PG (`aws_postgres_enabled` default true), schema `hr_source` |
| Learning source | `lake/landing/...` folder | `s3://coding-workshop-datalake-<app_id>/landing/...` |
| Lake | `LAKE_ROOT=<job dir>/lake` | `LAKE_ROOT=s3a://coding-workshop-datalake-<app_id>` |
| Runner | `python job.py` (venv, Python 3.11, pyspark 3.5.0) | Helm Job on EKS: image all-spark-notebook (Spark 3.5.0, Python 3.11), `pip install -r requirements.txt && python /app/job.py` |
| Env vars | Exported in your shell | Injected by Helm (§8.2) |

### 8.2 Infra changes (prompt C2; you review the diff and plan)
1. **`infra/s3.tf`:** add `aws_s3_bucket.datalake`, named `format("%s-datalake-%s", var.aws_project, local.app_id)`, with `force_destroy = true` so cleanup works and `tags = local.app_tags`. Add output `datalake_bucket_name` to **`infra/output.tf`**.
2. **`infra/helm/values.yaml`:** add `pythonJob.env: {}` and `pythonJob.secretEnv: {}`.
3. **`infra/helm/templates/python-job.yaml`:** render `env:` from `pythonJob.env`, and render a Secret from `pythonJob.secretEnv` loaded with `envFrom`.
4. **`infra/eks.tf` `null_resource.helm_python_job`:**
   - add `--set-string pythonJob.env.LAKE_ROOT=s3a://<datalake bucket>`
   - add `IS_LOCAL`, `POSTGRES_HOST/PORT/NAME/USER` from `local.env_vars`
   - pass `POSTGRES_PASS` through the local-exec **`environment {}` block** into `pythonJob.secretEnv`, so the password never appears in the printed command
   - set `pythonJob.ttlSecondsAfterFinished=1800` and `pythonJob.backoffLimit=1`
5. **Enable EKS** with the env var `TF_VAR_aws_eks_enabled=true` (no file change), as the guide describes.

### 8.3 Spark settings shared by both environments
- `spark.jars.packages=org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.3.4`
- `spark.driver.memory=1g`
- `spark.sql.shuffle.partitions=4`
- `spark.sql.sources.partitionOverwriteMode=dynamic`

S3A uses the default AWS credential chain. In the pod that's the node's instance role; no keys go in code. JDBC URL: `jdbc:postgresql://HOST:PORT/NAME`, with `?sslmode=require` when `IS_LOCAL=false`. The connection retries 3 times with backoff.

### 8.4 Resource check
- **Pod limit:** 2 GiB / 1 CPU. That covers a 1 GiB driver heap, about 0.4 GiB JVM overhead and about 0.2 GiB of Python, for roughly 1.6 GiB in total, on 14 MB of data.
- **If OOMKilled:** raise the limit to 3 GiB with `--set` (the node has 4 GiB).
- **If the pod stays Pending:** check `kubectl describe pod` for "Insufficient cpu" (R5). Lowering the job's CPU request to 0.25 is the fix.

### 8.5 Loading the cloud sources (prompt C4b)
- **Aurora:**
  - Get the endpoint and user from `terraform console` (`local.env_vars`). Export the password straight into the shell without printing it.
  - Run `scripts/load_source_postgres.sh` with `IS_LOCAL=false`.
  - If the VDI can't reach Aurora (R3), the fallback is a temporary `postgres:17` pod in the cluster: copy in the CSVs + scripts with `kubectl cp`, run the load, then delete the pod.
- **S3:** run `scripts/stage_landing.sh` with `LAKE_ROOT=s3a://…`. It uses `aws s3 sync` and should give 783 objects.

### 8.6 Running and re-running the cloud job (you run these)
```
kubectl delete job coding-workshop-talent-pipeline --ignore-not-found
./bin/deploy-backend.sh aws            # re-applies; the job is re-installed (its trigger is timestamp())
kubectl logs -f job/coding-workshop-talent-pipeline
```
The first apply also starts the job before any sources are loaded, so **that first run is expected to fail**. That's fine: `backoffLimit=1` stops it retrying.

### 8.7 Cost (approximate; the account belongs to the workshop)
| Resource | Rough cost |
|---|---|
| EKS control plane | about $0.10/h |
| 1× t3.medium SPOT | a few cents/h |
| Aurora Serverless v2 | about $0.12 per ACU-hour while active; scales to 0 when idle |
| S3 (lake) | negligible |

That's roughly $0.20–0.40/h while everything is up. Confirm limits with the organizers.

### 8.8 Cleanup (after evaluation)
- **Option A, full:** `./bin/cleanup-environment.sh`. It uploads a project backup zip to the workshop bucket, then destroys everything. Ask the organizers first; the zip may be the submission. Delete `data/talent-pipeline/lake/` beforehand so the zip stays small.
- **Option B, EKS only:** set `TF_VAR_aws_eks_enabled=false` and run `deploy-backend.sh aws` after reviewing a plan. Aurora and S3 stay.

---

## 9. AWS access: what you need and how to check

**You need these:**
1. `EVENT_ID`, `PARTICIPANT_ID`, `PARTICIPANT_CODE` and `AWS_REGION` in `~/.bashrc`, from the organizers' email.
2. Tools on the VDI: `aws`, `terraform` ≥ 1.11, `kubectl`, `helm`, `psql`, `jq`, `dig`.

**Check them yourself in your own terminal:**
```
source ~/.bashrc && ./bin/setup-participant.sh       # fetches TEMPORARY credentials → ENVIRONMENT.config
source ENVIRONMENT.config
unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3           # make sure you're not pointed at LocalStack
aws sts get-caller-identity                          # Account must NOT be 000000000000
[ -n "$TF_VAR_aws_ds_ip" ] && echo "ds_ip OK" || echo "ds_ip MISSING (EKS step will fail)"
aws s3 ls "s3://coding-workshop-tfstate-$PARTICIPANT_ID" >/dev/null && echo "state bucket OK"
```

**If something fails:**
- `ExpiredToken` later: re-run `setup-participant.sh`. The credentials are temporary (R6); `deploy-backend.sh` refreshes them automatically.
- `get-caller-identity`, the state bucket or the DS IP fails: ask the organizers now, not tomorrow.

---

## 10. Builder files and prompts

### 10.1 `CLAUDE.md` (v2; replaces v1 in full)
```markdown
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

## Current phase: LOCAL ONLY
- No infra changes and nothing deployment-related (AWS, Terraform, kubectl, bin/ scripts) until Darshan explicitly says so.
- Dataset (source, read-only, never commit): sampleData/data-engineering-sample-performance-scenario/ (ignored via .git/info/exclude).

## Layout rules (single-file job)
- ALL pipeline logic lives in data/talent-pipeline/job.py: the cloud receives only job.py + requirements.txt.
- Section order: config → logging → spark session → DQ rules → apply_rules/quarantine → bronze → silver → gold (one function per dataset) → run manifest → main().
- Nothing runs at import time: no SparkSession and no os.environ reads at module level. Everything starts in main() under `if __name__ == "__main__":`.
- All data IO goes through Spark (the manifest too), so the same code works for a local LAKE_ROOT and s3a://.
- Config comes only from env vars: IS_LOCAL, POSTGRES_HOST/PORT/NAME/USER/PASS, LAKE_ROOT. Same job.py locally and in cloud.
- Versions match the cloud image (Spark 3.5.0, Python 3.11, Java 17, Hadoop 3.3.4): requirements.txt = pyspark==3.5.0 only. Spark packages: org.postgresql:postgresql:42.7.4, org.apache.hadoop:hadoop-aws:3.3.4. No Iceberg. Never put pandas/numpy in requirements.txt.
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
Warnings: 666 reviews period-ended-before-hire; 234 assessments before hire.
Gold: gaps −1/0/1/2 = 1480/1527/668/325 (critical 325); HiPo+Ready Now 357; attrition L/M/H 1077/612/311; teams with projects 27; project_count sum 33.

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
```

### 10.2 Model, effort and mode per prompt
Start a new conversation tab per task. **Ultracode stays OFF.** After approving a plan, keep **Manual** (review each edit).

| Prompt | Model | Effort | Mode |
|---|---|---|---|
| 0 setup | Sonnet 5.5 | low | Manual |
| 1a project files | Sonnet 5.5 | medium | Plan |
| 1b job.py skeleton | opusplan | medium | Plan |
| 2 sources | opusplan | medium | Plan |
| 3 Bronze | opusplan | high | Plan |
| C1 preflight | Opus 5.5 | medium | Manual |
| C2 infra changes | Opus 5.5 | high | Plan |
| 4 DQ engine | Opus 5.5 | high | Plan |
| C4a cloud verify | Opus 5.5 | medium | Manual |
| C4b cloud load | Sonnet 5.5 | medium | Manual |
| C5 / C6 cloud evidence | Sonnet 5.5 | medium | Manual |
| 5 Silver | Opus 5.5 | high | Plan |
| 6 / 7 Gold | opusplan | high | Plan |
| 8 integration tests | opusplan | medium | Plan |
| N1 notebook · D1 docs | Sonnet 5.5 | medium | Plan |
| Debugging a failure | Opus 5.5 | high (xhigh if stuck) | Plan |

### 10.3 Prompts

**Step 0: you, by hand (about 5 min, no AI)**
```
# 1. Get the repo (skip the clone if you already have it; then just cd into it)
cd ~ && git clone https://github.com/citi/coding-workshop-participant && cd coding-workshop-participant
git status                                   # should be clean

# 2. Make the job folders
mkdir -p data/talent-pipeline/docs data/talent-pipeline/source_data

# 3. Put this plan into the repo (use the path where you downloaded it)
cp ~/Downloads/ACME_Talent_Pipeline_Plan.md data/talent-pipeline/docs/PLAN.md

# 4. Unzip the dataset into source_data (use your zip's real path)
unzip ~/Downloads/data-engineering-sample-performance-scenario.zip -d data/talent-pipeline/source_data/
#    → data/talent-pipeline/source_data/data-engineering-performance-talent-management/

# 5. Open the repo folder in VS Code → Claude Code panel → new conversation
```

**Task 0: first-time setup (read-only check, then create the builder files)**
```
Task 0 — first-time setup. Work in two parts and STOP between them.
Rules for this task: never git push or commit, never change remotes, never print secrets or the contents of ENVIRONMENT.config.

PART A — read-only, run now:
1. git status; git branch --show-current; git log --oneline -3; git remote -v
2. python3 --version; ls /usr/bin/python3.*; java -version; psql --version; pg_isready; aws --version; terraform version; kubectl version --client; helm version --short; jq --version
3. Check that data/talent-pipeline/docs/PLAN.md exists. Check that data/talent-pipeline/source_data/data-engineering-performance-talent-management/ contains schema.sql, load.sh, the 7 CSVs, and learning_activity/ with 783 .jsonl files (count them).
4. Which POSTGRES_* env vars are set (names only, never values)? Does ENVIRONMENT.config exist at the repo root (yes/no only)?
Explain the results simply, propose Part B, then STOP until I say "approved".

PART B — only after I approve:
5. Create local branch feature/talent-pipeline (never push).
6. Create CLAUDE.md at the repo root containing exactly the markdown block in PLAN.md §10.1 (without the ``` fences).
7. Create data/talent-pipeline/docs/PROGRESS.md with the title "# Progress log".
8. Create data/talent-pipeline/.gitignore with: lake/ lake_cloud/ .venv/ source_data/ spark-warehouse/ metastore_db/ *.tfplan __pycache__/ .pytest_cache/
9. Create .claude/settings.local.json with permissions.deny = Bash(git push:*), Bash(git remote add:*), Bash(git remote set-url:*), Bash(terraform apply:*), Bash(terraform destroy:*), Bash(./bin/deploy-backend.sh:*), Bash(bin/deploy-backend.sh:*), Bash(./bin/cleanup-environment.sh:*), Bash(bin/cleanup-environment.sh:*), Bash(./bin/setup-participant.sh:*), Bash(bin/setup-participant.sh:*). Add the line .claude/settings.local.json to .git/info/exclude (a local-only ignore; do not edit the root .gitignore).
10. Show git status --short — source_data/ must NOT appear.
End with the PROGRESS REPORT in the PLAN.md §10.1 format (Task 0) and append it to PROGRESS.md.
```

**Task 1a: project files and venv**
```
Task 1a — project files (PLAN.md §3). Plan first; wait for "approved".
In data/talent-pipeline/ create: requirements.txt (exactly: pyspark==3.5.0), requirements-dev.txt (pyspark==3.5.0, pytest, pytest-cov, pandas<3, pyarrow, matplotlib, jupyter, nbconvert), pytest.ini (pythonpath = . ; testpaths = tests), tests/conftest.py (session-scoped SparkSession local[1] fixture + a tmp LAKE_ROOT fixture).
Create .venv with Python 3.11 to match the cloud image. If python3.11 is not installed, STOP and show me options (e.g. `pip install uv` then `uv python install 3.11`) before doing anything else. If Java is not 11 or 17, tell me.
Acceptance: `.venv/bin/python -c "import pyspark; print(pyspark.__version__)"` prints 3.5.0; `pytest --collect-only` exits 0 or 5 ("no tests collected" is fine at this stage). Show output.
```

**Task 1b: job.py skeleton**
```
Task 1b — job.py skeleton (PLAN.md §3 section list, §8.3 Spark settings). Plan first.
Create data/talent-pipeline/job.py with all 10 section headers. Implement now: load_config() (IS_LOCAL default "true", POSTGRES_* , LAKE_ROOT default <folder of job.py>/lake), get_logger(run_id), create_spark(cfg) with the §8.3 settings (no Iceberg), RunManifest + write_manifest(spark, cfg, manifest) writing JSON through Spark to {LAKE_ROOT}/_runs/run_id=<id>/, and main(). Bronze/Silver/Gold/DQ sections are stubs that log "not implemented yet".
Nothing at import time. Never log POSTGRES_PASS.
Add tests/test_rules.py with: test that `import job` does not create a SparkSession; test load_config defaults; test write_manifest round-trip using the spark fixture.
Acceptance: `.venv/bin/python job.py` (from data/talent-pipeline) exits 0 and writes lake/_runs/run_id=*/; pytest passes. Show output + the manifest JSON. Explain each section to me in 2–3 sentences.
```

**Task 2: source scripts and local load**
```
Task 2 — source systems, local (PLAN.md §2 mapping, §8.5). Plan first.
Create data/talent-pipeline/scripts/load_source_postgres.sh: args/env SOURCE_DIR (folder with schema.sql, load.sh, 7 CSVs) + POSTGRES_* + IS_LOCAL. It runs DROP SCHEMA IF EXISTS hr_source CASCADE; CREATE SCHEMA hr_source; then the ORIGINAL load.sh with PGOPTIONS='-c search_path=hr_source' (never edit load.sh/schema.sql), then prints row counts per table. Never echo the password.
Create scripts/stage_landing.sh: copies SOURCE_DIR/learning_activity to $LAKE_ROOT/landing/learning_activity — cp -r for a local path; `aws s3 sync` (convert s3a:// to s3://) when LAKE_ROOT starts with s3a://. Prints the file/object count.
Run both locally. Acceptance: counts 2000/100/40/33/4000/4000/2000; running the load twice succeeds; 783 landing files; sha256 of the 7 CSVs unchanged before/after. Show output.
```

**Task 3: Bronze**
```
Task 3 — Bronze in job.py (PLAN.md §3 lake layout, §5, §8.3). Plan first.
bronze_hr(): read the 7 hr_source tables via Spark JDBC (sslmode=require when IS_LOCAL=false; retry connection 3× with exponential backoff) → Parquet {LAKE_ROOT}/bronze/hr/<table>/ingest_date=<today>/ + _run_id, _ingested_at, _source_system='postgres.hr_source', _source_object=<table>; overwrite only that partition.
bronze_learning(): spark.read.text(landing path, wholetext=True) → split lines + posexplode for _line_no → drop blank lines → add _source_file (input_file_name()), _record_hash (sha2 of raw line), _run_id, _ingested_at → bronze/learning_activity/ partitioned by year/month/day (from the path). Do NOT parse JSON. WARN for files with zero lines.
Record rows read/written per source in the manifest. Unit test on a 3-line fixture with one blank line.
Acceptance: Bronze counts 2000/100/40/33/4000/4000/2000 and 29,868 learning lines; second run gives identical counts; tests pass. Show output.
```

**Task C1: cloud preflight (read-only)**
```
Task C1 — CLOUD PREFLIGHT, READ-ONLY (PLAN.md §1 risks, §9). Nothing that changes AWS. Never print secrets.
I have already run ./bin/setup-participant.sh in my terminal (ENVIRONMENT.config exists).
Prefix every AWS/terraform command with: source ENVIRONMENT.config && unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3 &&
1. aws sts get-caller-identity → show Account and Arn (Account must not be 000000000000).
2. yes/no: TF_VAR_aws_ds_ip set? TF_VAR_aws_app_code set (value may be shown)? AWS_REGION?
3. aws s3 ls s3://coding-workshop-tfstate-$PARTICIPANT_ID → exists?
4. Tools present: terraform (>=1.11), kubectl, helm, aws, psql, jq.
5. cd infra; terraform init -reconfigure -backend-config="bucket=coding-workshop-tfstate-$PARTICIPANT_ID" -backend-config="region=$AWS_REGION"; terraform state list; TF_VAR_aws_eks_enabled=true terraform plan -out=c1.tfplan. Summarise add/change/destroy grouped by EKS, Aurora, S3, Helm, Lambda/CloudFront, other. Flag every DESTROY loudly.
Explain each result simply. End with PROGRESS REPORT (Task C1).
```

**Task C2: infra changes (diff and plan only)**
```
Task C2 — infra changes exactly as PLAN.md §8.2 items 1–4. Plan first; show me the full diff; do NOT apply.
After editing: terraform fmt; terraform validate; render the chart locally with `helm template x infra/helm --set pythonJob.enabled=true --set pythonJob.name=talent-pipeline --set-file pythonJob.scriptContent=data/talent-pipeline/job.py --set-file pythonJob.requirementsContent=data/talent-pipeline/requirements.txt --set-string pythonJob.env.LAKE_ROOT=s3a://dummy --set-string pythonJob.secretEnv.POSTGRES_PASS=dummy` and show the Job + Secret parts; then (with the C1 prefix) TF_VAR_aws_eks_enabled=true terraform plan -out=c2.tfplan and summarise only the differences vs C1.
Acceptance: rendered Job has env + envFrom Secret, ttlSecondsAfterFinished 1800, backoffLimit 1; validate passes; plan adds the datalake bucket; nothing is destroyed; the password never appears in any command text. End with PROGRESS REPORT.
```

**You run (after reviewing C1 and C2 and approving the cost):**
```
echo 'export TF_VAR_aws_eks_enabled=true' >> ~/.bashrc && source ~/.bashrc
./bin/deploy-backend.sh aws          # ~20–30 min. The first job run fails (no sources yet). Expected.
```
Paste the last ~30 lines of output here.

**Task 4: DQ engine (do this while EKS provisions)**
```
Task 4 — DQ section of job.py (PLAN.md §5). Plan first.
Implement the Rule dataclass, the declarative RULES list for all entities (from schema.sql CHECK constraints + PLAN.md §2/§5), apply_rules(df, entity, run_id) → (valid_df with dq_warnings array, quarantine_df with rule_id, dq_reason, _run_id, _quarantined_at), and write_quarantine(). Errors quarantine; warnings flag.
Unit tests in tests/test_rules.py: for each rule type one passing and one failing row; a row failing two rules is quarantined once with both reasons.
Acceptance: pytest passes; show output and explain how a new rule is added (one line).
```

**Task C4a: verify cloud and test connectivity (read-only plus temporary test pods)**
```
Task C4a — verify the deployed cloud (PLAN.md §1 R1–R5). Read-only except temporary test pods you delete afterwards. Use the C1 prefix.
1. terraform output datalake_bucket_name; aws s3 ls s3://<bucket>; aws eks update-kubeconfig --name <cluster name from terraform state> --region $AWS_REGION; kubectl get nodes; kubectl get pods -A.
2. R1: kubectl run s3test --rm -i --restart=Never --image=amazon/aws-cli -- s3 ls s3://<bucket>  (uses the node role).
3. R2: get POSTGRES_HOST from `terraform console` (local.env_vars["POSTGRES_HOST"]); kubectl run pgtest --rm -i --restart=Never --image=postgres:17 -- pg_isready -h <host> -p 5432.
4. R3: pg_isready -h <host> -p 5432 from the VDI.
5. R5: kubectl describe node | grep -A8 "Allocated resources".
Explain each result simply; for any failure, diagnose and propose a fix but do not apply it. End with PROGRESS REPORT.
```

**Task C4b: load the cloud sources**
```
Task C4b — load cloud sources (PLAN.md §8.5). Plan first. Use the C1 prefix.
Export POSTGRES_HOST/PORT/NAME/USER from terraform console (local.env_vars) and export POSTGRES_PASS the same way WITHOUT printing it (e.g. export POSTGRES_PASS=$(echo 'local.env_vars["POSTGRES_PASS"]' | terraform console | tr -d '"')); IS_LOCAL=false.
If C4a showed the VDI reaches Aurora: run scripts/load_source_postgres.sh → show counts. If not: propose the in-cluster postgres:17 pod fallback and wait for my approval.
Then LAKE_ROOT=s3a://<bucket> scripts/stage_landing.sh → aws s3 ls --recursive s3://<bucket>/landing/ | wc -l must be 783.
Acceptance: Aurora counts 2000/100/40/33/4000/4000/2000; 783 objects. End with PROGRESS REPORT.
```

**You run:** `kubectl delete job coding-workshop-talent-pipeline --ignore-not-found && ./bin/deploy-backend.sh aws && kubectl logs -f job/coding-workshop-talent-pipeline`. Save the log output.

**Task C5: cloud run evidence**
```
Task C5 — cloud run evidence (read-only, C1 prefix). kubectl get jobs,pods; kubectl logs job/coding-workshop-talent-pipeline --tail=80; list s3://<bucket>/_runs/ and print the newest manifest (aws s3 cp ... -); compare its Bronze counts with the latest local manifest in a table (local | cloud | match?). If the job failed, diagnose from the logs (OOMKilled? Pending? JDBC timeout? S3 403?) and propose a fix without applying it. End with PROGRESS REPORT.
```

**Task 5: Silver**
```
Task 5 — Silver in job.py (PLAN.md §5, §2). Plan first. Do HR first and show me results, then learning.
silver_hr(): cast types, add review period_start/period_end, apply_rules, FK checks, warnings (period ended before hire; assessment before hire), write silver/<table>/ + quarantine.
silver_learning(): parse raw lines with an explicit schema (malformed → quarantine malformed_json, keep raw line); dedupe on event_id BEFORE validation (log count); coalesce text/notes → event_text + text_source_field; employee_id → int; parse event_ts; apply_rules (null employee_id, invalid date → quarantine; event before hire → warning); write silver/learning_events partitioned by event_year/event_month.
Record reconciliation (read = written + quarantined + deduped) in the manifest.
Acceptance: HR counts unchanged, 0 HR quarantines, warnings 666 / 234; learning 13 malformed, 152 deduped, 76 + 53 quarantined, 29,574 valid. Show output.
```

**Task 6: Gold part A**
```
Task 6 — Gold G1, G1b, G2, G5, G5b, G7 in job.py (PLAN.md §4). Plan first.
One function per dataset; aggregate child tables to the target grain BEFORE joining; derived columns prefixed derived_; critical-gap threshold is a parameter (default 2). Unit tests on tiny fixtures for rating_change/trend and gap severity.
Acceptance (full run): G1 4,000 unique (employee, period); G1b 2,000 and trend counts sum to 2,000; G2 gap counts 1480/1527/668/325, critical 325; G5 2,000 with risk 1077/612/311; G5b sums to 2,000; G7 assessed sum 4,000. Show 3 sample rows each.
```

**Task 7: Gold part B**
```
Task 7 — Gold G4, G4b, G3, G6 in job.py (PLAN.md §4). Plan first.
G4 completion = course_completed event only; G4b left-joined from employees with zeros; G3 joins per-employee rollups only (no fan-out); G6 aggregates projects per team BEFORE joining. Unit test: one team with 2 projects does not double its headcount.
Acceptance: G4 event-count sum 29,574 and unique (employee, course); G4b 2,000; G3 2,000 with derived_high_potential_ready_now = 357; G6 100 rows, project_count sum 33, has_projects 27, headcount sum 2,000. Show 3 sample rows each.
```

**Task 8: integration test and idempotency**
```
Task 8 — tests/test_pipeline.py (PLAN.md §6). Plan first.
Integration test (marked so it only runs when local Postgres is up): run main() into a temp LAKE_ROOT; assert every count in CLAUDE.md "Expected numbers" plus reconciliation per step; run main() a second time and assert identical Gold row counts and no duplicate keys at each Gold grain.
Acceptance: full pytest passes; save the command + output to docs/test_evidence.md.
```

**You run:** the same re-run command as before C5.

**Task C6: final cloud evidence**
```
Task C6 — final cloud evidence (read-only). Same as C5 but compare ALL counts (Bronze, Silver, quarantine, every Gold dataset) local vs cloud, and download s3://<bucket>/gold/ to data/talent-pipeline/lake_cloud/gold/ (gitignored) for the notebook. Write the comparison table into docs/test_evidence.md. End with PROGRESS REPORT.
```

**Task N1: notebook**
```
Task N1 — notebooks/talent_insights.ipynb. Plan first.
Read ONLY Gold Parquet (pandas + pyarrow) from a GOLD_DIR variable at the top (default ../lake/gold; can point at ../lake_cloud/gold). One section per question Q1–Q7: markdown question → one matplotlib chart → 2–3 sentence finding from the ACTUAL numbers → caveat (recorded vs derived; association not causation; PLAN.md §2 limits; Q6 n=27 teams). First cell: a reconciliation summary table from the latest manifest. Execute top-to-bottom with nbconvert --execute and show it ran without errors. Do not invent findings.
```

**Task D1: minimal docs**
```
Task D1 — data/talent-pipeline/README.md: what it does (5 lines), architecture (mermaid), how to run locally (env vars, scripts, job, tests), how it runs in cloud (PLAN.md §8 summary + the exact commands), design decisions (single job.py and why; pyspark 3.5.0 to match the image; quarantine vs warning), known limitations (PLAN.md §2), cleanup. Keep it to ~1 page. Re-run pytest and append the output to docs/test_evidence.md.
```

**Task C7: cleanup (after evaluation, organizers consulted)** is in §8.8. You run it; the builder doesn't.
