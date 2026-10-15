"""ACME Talent Pipeline: single-file Spark job (Bronze → Silver → Gold).

All pipeline logic lives in this file because the cloud Helm Job receives only
job.py + requirements.txt. Nothing runs at import time: config is read and Spark
is started only inside main().
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

JOB_DIR = Path(__file__).resolve().parent
DATASET_DIR_NAME = "data-engineering-performance-talent-management"
DEFAULT_SOURCE_DIR = (
    JOB_DIR.parent.parent
    / "sampleData"
    / "data-engineering-sample-performance-scenario"
    / DATASET_DIR_NAME
)
DEFAULT_LAKE_ROOT = JOB_DIR / "lake"
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"


# =============================================================================
# 1. Config
# =============================================================================
@dataclass(frozen=True)
class Config:
    """Runtime settings, read from environment variables only."""

    is_local: bool
    source_dir: str
    lake_root: str
    aws_region: Optional[str] = None


def _parse_bool(value: str) -> bool:
    """Interpret common truthy strings ("true", "1", "yes")."""
    return value.strip().lower() in {"true", "1", "yes"}


def load_config(env: Optional[Mapping[str, str]] = None) -> Config:
    """Build a Config from env vars (IS_LOCAL, SOURCE_DIR, LAKE_ROOT, AWS_REGION)."""
    env = os.environ if env is None else env
    return Config(
        is_local=_parse_bool(env.get("IS_LOCAL", "true")),
        source_dir=env.get("SOURCE_DIR", str(DEFAULT_SOURCE_DIR)),
        lake_root=env.get("LAKE_ROOT", str(DEFAULT_LAKE_ROOT)).rstrip("/"),
        aws_region=env.get("AWS_REGION") or None,
    )


# =============================================================================
# 2. Logging
# =============================================================================
def get_logger(run_id: str) -> logging.Logger:
    """Return a stdout logger whose lines carry the run_id."""
    logger = logging.getLogger("talent_pipeline")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(f"%(asctime)s %(levelname)s run_id={run_id} %(message)s")
    )
    logger.addHandler(handler)
    return logger


# =============================================================================
# 3. Spark session
# =============================================================================
def spark_conf(cfg: Config) -> dict[str, str]:
    """Spark settings; hadoop-aws (+ regional S3 endpoint if AWS_REGION) only for s3a:// paths."""
    conf = {
        "spark.driver.memory": "1g",
        "spark.sql.shuffle.partitions": "4",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.ui.enabled": "false",
        "spark.ui.showConsoleProgress": "false",
    }
    if any(p.startswith("s3a://") for p in (cfg.source_dir, cfg.lake_root)):
        conf["spark.jars.packages"] = HADOOP_AWS_PACKAGE
        if cfg.aws_region:
            conf["spark.hadoop.fs.s3a.endpoint"] = f"s3.{cfg.aws_region}.amazonaws.com"
    return conf


def create_spark_session(cfg: Config) -> SparkSession:
    """Create the local[2] SparkSession with spark_conf(cfg)."""
    builder = SparkSession.builder.master("local[2]").appName("talent-pipeline")
    for key, value in spark_conf(cfg).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# =============================================================================
# 4. DQ rules
# =============================================================================
@dataclass(frozen=True)
class Rule:
    """One data-quality check.

    condition is a Spark SQL expression that is TRUE when the row passes. As with SQL
    CHECK constraints, a NULL result counts as a pass (missing values are reported once,
    by the not_null rule). severity "error" quarantines the row; "warning" only flags it.
    """

    rule_id: str
    entity: str
    column: str
    severity: str  # "error" | "warning"
    condition: str
    description: str


# HR source contract, from schema.sql. Columns not in HR_TYPES stay string.
HR_TYPES: dict[str, dict[str, str]] = {
    "teams": {"team_id": "int"},
    "competencies": {"competency_id": "int"},
    "employees": {"employee_id": "int", "team_id": "int", "hire_date": "date"},
    "projects": {"project_id": "int", "team_id": "int", "outcome_score": "decimal(5,2)"},
    "performance_reviews": {
        "review_id": "int", "employee_id": "int", "rating": "int", "submitted_date": "date",
    },
    "competency_assessments": {
        "assessment_id": "int", "employee_id": "int", "competency_id": "int",
        "proficiency_level": "int", "required_level": "int", "assessed_date": "date",
    },
    "talent_review": {"employee_id": "int"},
}
HR_PK = {
    "teams": "team_id",
    "competencies": "competency_id",
    "employees": "employee_id",
    "projects": "project_id",
    "performance_reviews": "review_id",
    "competency_assessments": "assessment_id",
    "talent_review": "employee_id",
}
HR_REQUIRED = {  # NOT NULL columns (performance_reviews.manager_comment is nullable)
    "teams": ("team_id", "name", "department"),
    "competencies": ("competency_id", "name", "category"),
    "employees": (
        "employee_id", "first_name", "last_name", "email", "team_id", "job_level", "hire_date",
    ),
    "projects": ("project_id", "name", "team_id", "outcome_status", "outcome_score"),
    "performance_reviews": (
        "review_id", "employee_id", "review_period", "rating", "submitted_date",
    ),
    "competency_assessments": (
        "assessment_id", "employee_id", "competency_id", "proficiency_level",
        "required_level", "assessed_date",
    ),
    "talent_review": (
        "employee_id", "review_cycle", "potential_rating", "promotion_readiness",
        "attrition_risk",
    ),
}
HR_FKS = {  # (child table, column) -> parent table (same column name in parent)
    ("employees", "team_id"): "teams",
    ("projects", "team_id"): "teams",
    ("performance_reviews", "employee_id"): "employees",
    ("competency_assessments", "employee_id"): "employees",
    ("competency_assessments", "competency_id"): "competencies",
    ("talent_review", "employee_id"): "employees",
}


LEARNING_EVENT_TYPES = (
    "enrollment",
    "module_started",
    "module_completed",
    "quiz_attempted",
    "quiz_passed",
    "course_completed",
    "certification_earned",
)


def _domain(entity: str, column: str, check: str, description: str) -> Rule:
    return Rule(f"{entity}.{column}.domain", entity, column, "error", check, description)


RULES: list[Rule] = [
    *(
        Rule(f"{t}.{c}.not_null", t, c, "error", f"_chk_raw_{c} IS NOT NULL", f"{c} is missing")
        for t, cols in HR_REQUIRED.items()
        for c in cols
    ),
    *(
        Rule(
            f"{t}.{c}.cast", t, c, "error", f"_chk_raw_{c} IS NULL OR {c} IS NOT NULL",
            f"{c} is not a valid {typ}",
        )
        for t, types in HR_TYPES.items()
        for c, typ in types.items()
    ),
    *(
        Rule(f"{t}.{pk}.unique", t, pk, "error", "_chk_pk_unique", f"duplicate {pk}")
        for t, pk in HR_PK.items()
    ),
    *(
        Rule(f"{t}.{c}.fk", t, c, "error", f"_chk_fk_{c}", f"{c} not found in {parent}")
        for (t, c), parent in HR_FKS.items()
    ),
    _domain("competencies", "category", "category IN ('Technical', 'Leadership', 'Domain')",
            "category not in Technical/Leadership/Domain"),
    _domain("projects", "outcome_status",
            "outcome_status IN ('on-time', 'delayed', 'at-risk', 'cancelled')",
            "outcome_status not in on-time/delayed/at-risk/cancelled"),
    _domain("performance_reviews", "rating", "rating BETWEEN 1 AND 5", "rating not 1-5"),
    _domain("performance_reviews", "review_period", r"review_period RLIKE '^\\d{4}-H[12]$'",
            "review_period not YYYY-H1/H2"),
    _domain("competency_assessments", "proficiency_level", "proficiency_level BETWEEN 1 AND 5",
            "proficiency_level not 1-5"),
    _domain("competency_assessments", "required_level", "required_level BETWEEN 1 AND 5",
            "required_level not 1-5"),
    _domain("talent_review", "potential_rating", "potential_rating IN ('Low', 'Medium', 'High')",
            "potential_rating not Low/Medium/High"),
    _domain("talent_review", "promotion_readiness",
            "promotion_readiness IN ('Ready Now', '1-2 Years', 'Not Ready')",
            "promotion_readiness not Ready Now/1-2 Years/Not Ready"),
    _domain("talent_review", "attrition_risk", "attrition_risk IN ('Low', 'Medium', 'High')",
            "attrition_risk not Low/Medium/High"),
    Rule("performance_reviews.derived_period_end.before_hire", "performance_reviews",
         "derived_period_end", "warning", "derived_period_end >= _chk_hire_date",
         "review period ended before the employee's hire date"),
    Rule("competency_assessments.assessed_date.before_hire", "competency_assessments",
         "assessed_date", "warning", "assessed_date >= _chk_hire_date",
         "assessment dated before the employee's hire date"),
    # Learning events, stage 1 (before dedupe): the line must be valid JSON.
    Rule("learning_events.raw.malformed_json", "learning_events_raw", "value", "error",
         "_corrupt_record IS NULL", "line is not valid JSON"),
    # Learning events, stage 2 (after dedupe).
    Rule("learning_events.employee_id.not_null", "learning_events", "employee_id", "error",
         "_chk_raw_employee_id IS NOT NULL", "employee_id is missing"),
    Rule("learning_events.employee_id.cast", "learning_events", "employee_id", "error",
         "_chk_raw_employee_id IS NULL OR employee_id IS NOT NULL", "employee_id is not an int"),
    Rule("learning_events.event_ts.invalid", "learning_events", "event_date", "error",
         "event_ts IS NOT NULL", "event_date is not yyyy-MM-ddTHH:mm:ss"),
    Rule("learning_events.employee_id.fk", "learning_events", "employee_id", "error",
         "_chk_fk_employee_id", "employee_id not found in employees"),
    _domain("learning_events", "event_type",
            f"event_type IN ({', '.join(repr(t) for t in LEARNING_EVENT_TYPES)})",
            "event_type not one of the 7 known types"),
    Rule("learning_events.score.cast", "learning_events", "score", "error",
         "_chk_raw_score IS NULL OR score IS NOT NULL", "score is not a number"),
    _domain("learning_events", "score", "score BETWEEN 0 AND 100", "score not 0-100"),
    Rule("learning_events.event_ts.before_hire", "learning_events", "event_ts", "warning",
         "to_date(event_ts) >= _chk_hire_date", "event dated before the employee's hire date"),
    Rule("learning_events.event_text.notes_used", "learning_events", "event_text", "warning",
         "text_source_field = 'text'", "event text came from 'notes' instead of 'text'"),
]


# =============================================================================
# 5. Quality engine (apply_rules / quarantine)
# =============================================================================
def cast_columns(df: DataFrame, types: Mapping[str, str]) -> DataFrame:
    """Keep each business column's raw string as _chk_raw_<col>, then cast typed columns."""
    business = [c for c in df.columns if not c.startswith("_")]
    df = df.withColumns({f"_chk_raw_{c}": F.col(c) for c in business})
    return df.withColumns({c: F.col(c).cast(t) for c, t in types.items()})


def add_pk_flag(df: DataFrame, pk: str) -> DataFrame:
    """_chk_pk_unique: TRUE unless another row has the same non-null PK."""
    dupes = F.count(F.lit(1)).over(Window.partitionBy(pk))
    return df.withColumn("_chk_pk_unique", F.col(pk).isNull() | (dupes == 1))


def add_fk_flag(df: DataFrame, column: str, parent_keys: DataFrame) -> DataFrame:
    """_chk_fk_<column>: TRUE if the value is null or exists in parent_keys[column]."""
    keys = parent_keys.select(F.col(column).alias("_fk_key")).distinct().withColumn(
        "_fk_hit", F.lit(True)
    )
    joined = df.join(keys, df[column] == keys["_fk_key"], "left")
    return joined.withColumn(
        f"_chk_fk_{column}", F.col(column).isNull() | F.col("_fk_hit").isNotNull()
    ).drop("_fk_key", "_fk_hit")


def apply_rules(
    df: DataFrame, entity: str, run_id: str, rules: Optional[list[Rule]] = None
) -> tuple[DataFrame, DataFrame]:
    """Split df into (valid rows + dq_warnings, quarantined rows + rule_ids/dq_reasons).

    Precomputed _chk_* columns must already exist. A row failing several error rules is
    quarantined once, with every failed rule listed.
    """
    rules = [r for r in (RULES if rules is None else rules) if r.entity == entity]

    def failed(rule: Rule, value: str) -> Any:
        return F.when(F.expr(f"NOT ({rule.condition})"), F.lit(value))

    errors = [r for r in rules if r.severity == "error"]
    warnings = [r for r in rules if r.severity == "warning"]
    none = F.array().cast("array<string>")
    checked = df.withColumns(
        {
            "rule_ids": F.array_compact(F.array(*[failed(r, r.rule_id) for r in errors]))
            if errors else none,
            "dq_reasons": F.array_compact(
                F.array(*[failed(r, f"{r.rule_id}: {r.description}") for r in errors])
            ) if errors else none,
            "dq_warnings": F.array_compact(F.array(*[failed(r, r.rule_id) for r in warnings]))
            if warnings else none,
        }
    )
    is_error = F.size("rule_ids") > 0
    helpers = [c for c in checked.columns if c.startswith("_chk_")]
    valid = checked.where(~is_error).drop("rule_ids", "dq_reasons", *helpers)
    quarantine = checked.where(is_error).withColumns(
        {"_run_id": F.lit(run_id), "_quarantined_at": F.current_timestamp()}
    )
    return valid, quarantine


# =============================================================================
# 6. Bronze
# =============================================================================
HR_TABLES = (
    "employees",
    "teams",
    "competencies",
    "projects",
    "performance_reviews",
    "competency_assessments",
    "talent_review",
)
LEARNING_DIR = "learning_activity"
LEARNING_GLOB = "year=*/month=*/day=*/*.jsonl"
PARTITION_PATH_RE = r"year=([^/]+)/month=([^/]+)/day=([^/]+)/[^/]+$"


def read_hr_csv(spark: SparkSession, source_dir: str, table: str) -> DataFrame:
    """Read one HR CSV exactly as delivered: header row, every column a string."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("quote", '"')
        .option("escape", '"')
        .csv(f"{source_dir}/{table}.csv")
    )


def add_lineage(df: DataFrame, run_id: str, ingested_at: datetime) -> DataFrame:
    """Append the run id and ingest timestamp to every row."""
    return df.withColumn("_run_id", F.lit(run_id)).withColumn(
        "_ingested_at", F.lit(ingested_at).cast("timestamp")
    )


def read_learning_lines(spark: SparkSession, landing_path: str) -> DataFrame:
    """Every physical line of every JSONL file (blank lines kept), with 1-based _line_no.

    Spark skips 0-byte files here; list_files() is used to find those.
    """
    files = spark.read.text(f"{landing_path}/{LEARNING_GLOB}", wholetext=True).withColumn(
        "_source_file", F.input_file_name()
    )
    body = F.regexp_replace("value", r"\r?\n\z", "")  # drop the final newline only
    return files.select(
        "_source_file", F.posexplode(F.split(body, r"\r?\n")).alias("_pos", "value")
    ).select("value", (F.col("_pos") + 1).alias("_line_no"), "_source_file")


def bronze_learning_rows(lines: DataFrame, run_id: str, ingested_at: datetime) -> DataFrame:
    """Non-blank lines + hash + lineage + year/month/day taken from the file path."""
    rows = lines.where(F.trim("value") != "").withColumn(
        "_record_hash", F.sha2("value", 256)
    )
    rows = add_lineage(rows, run_id, ingested_at)
    for idx, part in enumerate(("year", "month", "day"), start=1):
        rows = rows.withColumn(part, F.regexp_extract("_source_file", PARTITION_PATH_RE, idx))
    return rows


def _glob_status(spark: SparkSession, glob_path: str) -> list[Any]:
    """Hadoop FileStatus objects matching a glob (local or s3a://)."""
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(glob_path)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return list(fs.globStatus(path) or [])


def list_files(spark: SparkSession, glob_path: str) -> list[str]:
    """List files matching a glob, including 0-byte files that Spark readers skip."""
    return [s.getPath().toString() for s in _glob_status(spark, glob_path) if s.isFile()]


def list_dirs(spark: SparkSession, glob_path: str) -> list[str]:
    """List directories matching a glob."""
    return [s.getPath().toString() for s in _glob_status(spark, glob_path) if s.isDirectory()]


def _partition_key(path: str) -> str:
    """'year=…/month=…/day=…/file' part of a path, so file: and s3a: URIs compare equal."""
    return "/".join(path.rstrip("/").split("/")[-4:])


def ingest_into_bronze(
    spark: SparkSession, cfg: Config, log: logging.Logger, manifest: "RunManifest"
) -> None:
    """Land the 7 HR CSVs and raw learning JSONL lines in {LAKE_ROOT}/bronze, unparsed."""
    run_id = manifest.run_id
    ingested_at = datetime.fromisoformat(manifest.started_at)
    ingest_date = ingested_at.date().isoformat()  # UTC date of the run

    for table in HR_TABLES:
        src = read_hr_csv(spark, cfg.source_dir, table)
        rows_read = src.count()
        out = add_lineage(src, run_id, ingested_at).withColumns(
            {
                "_source_system": F.lit("file"),
                "_source_file": F.input_file_name(),
                "ingest_date": F.lit(ingest_date),
            }
        )
        path = f"{cfg.lake_root}/bronze/hr/{table}"
        out.write.mode("overwrite").partitionBy("ingest_date").parquet(path)
        rows_written = spark.read.parquet(f"{path}/ingest_date={ingest_date}").count()
        log.info("bronze: hr.%s read=%d written=%d", table, rows_read, rows_written)
        manifest.add_step(
            StepMetrics(step=f"bronze.hr.{table}", rows_read=rows_read, rows_written=rows_written)
        )

    landing = f"{cfg.source_dir}/{LEARNING_DIR}"
    lines = read_learning_lines(spark, landing)
    per_file = {
        r["_source_file"]: (r["lines"], r["non_blank"])
        for r in lines.groupBy("_source_file")
        .agg(
            F.count("*").alias("lines"),
            F.sum((F.trim("value") != "").cast("int")).alias("non_blank"),
        )
        .collect()
    }
    non_blank_by_key = {_partition_key(p): nb for p, (_, nb) in per_file.items()}
    listed = list_files(spark, f"{landing}/{LEARNING_GLOB}")
    zero_line_files = [p for p in listed if non_blank_by_key.get(_partition_key(p), 0) == 0]
    for p in zero_line_files:
        log.warning("bronze: file has zero lines: %s", p)

    rows_read = sum(n for n, _ in per_file.values())
    path = f"{cfg.lake_root}/bronze/learning_activity"
    bronze_learning_rows(lines, run_id, ingested_at).write.mode("overwrite").partitionBy(
        "year", "month", "day"
    ).parquet(path)
    rows_written = spark.read.parquet(path).where(F.col("_run_id") == run_id).count()
    blank = rows_read - rows_written
    log.info(
        "bronze: learning_activity files=%d lines=%d written=%d blank_dropped=%d",
        len(listed), rows_read, rows_written, blank,
    )
    manifest.add_step(
        StepMetrics(
            step="bronze.learning_activity",
            rows_read=rows_read,
            rows_written=rows_written,
            note=f"files={len(listed)} zero_line_files={len(zero_line_files)} "
            f"blank_lines_dropped={blank}",
        )
    )


# =============================================================================
# 7. Silver
# =============================================================================
HR_SILVER_ORDER = (  # parents before children, as in schema.sql
    "teams",
    "competencies",
    "employees",
    "projects",
    "performance_reviews",
    "competency_assessments",
    "talent_review",
)


def read_latest_bronze_hr(spark: SparkSession, cfg: Config, table: str) -> DataFrame:
    """Read the newest ingest_date partition folder directly (no partition inference)."""
    dirs = list_dirs(spark, f"{cfg.lake_root}/bronze/hr/{table}/ingest_date=*")
    if not dirs:
        raise FileNotFoundError(f"no bronze partitions for {table}")
    by_date = {d.rstrip("/").rsplit("ingest_date=", 1)[1]: d for d in dirs}
    ingest_date = max(by_date)
    latest = by_date[ingest_date]
    return spark.read.parquet(latest).withColumn("_bronze_ingest_date", F.lit(ingest_date))


def add_review_period_dates(df: DataFrame) -> DataFrame:
    """derived_period_start/end from review_period 'YYYY-H1' (Jan–Jun) or 'YYYY-H2' (Jul–Dec)."""
    year = F.regexp_extract("review_period", r"^(\d{4})-H[12]$", 1)
    half = F.regexp_extract("review_period", r"^\d{4}-H([12])$", 1)
    y = F.when(year != "", year.cast("int"))
    h1 = half == "1"
    return df.withColumns(
        {
            "derived_period_start": F.make_date(y, F.when(h1, 1).otherwise(7), F.lit(1)),
            "derived_period_end": F.make_date(
                y, F.when(h1, 6).otherwise(12), F.when(h1, 30).otherwise(31)
            ),
        }
    )


def prepare_hr(table: str, bronze: DataFrame, parents: Mapping[str, DataFrame]) -> DataFrame:
    """Cast types and add the _chk_* flag columns the HR rules need.

    parents maps table name -> its already-validated Silver DataFrame.
    """
    df = bronze.drop("_run_id")  # replaced by this run's id on write
    df = cast_columns(df, HR_TYPES[table])
    if table == "performance_reviews":
        df = add_review_period_dates(df)
    df = add_pk_flag(df, HR_PK[table])
    for (child, column), parent in HR_FKS.items():
        if child == table:
            df = add_fk_flag(df, column, parents[parent])
    if table in ("performance_reviews", "competency_assessments"):
        hires = parents["employees"].select(
            F.col("employee_id").alias("_hire_emp"), F.col("hire_date").alias("_chk_hire_date")
        )
        df = df.join(hires, df["employee_id"] == hires["_hire_emp"], "left").drop("_hire_emp")
    return df


def transform_to_silver(
    spark: SparkSession, cfg: Config, log: logging.Logger, manifest: "RunManifest"
) -> None:
    """Type, validate and conform Bronze HR tables into Silver; quarantine error rows."""
    run_id = manifest.run_id
    valid_by_table: dict[str, DataFrame] = {}
    for table in HR_SILVER_ORDER:
        bronze = read_latest_bronze_hr(spark, cfg, table)
        rows_read = bronze.count()
        checked = prepare_hr(table, bronze, valid_by_table).cache()
        valid, quarantine = apply_rules(checked, table, run_id)
        valid = valid.withColumn("_run_id", F.lit(run_id))

        path = f"{cfg.lake_root}/silver/{table}"
        valid.write.mode("overwrite").parquet(path)
        silver = spark.read.parquet(path)
        rows_written = silver.count()
        rows_warned = silver.where(F.size("dq_warnings") > 0).count()
        rows_quarantined = quarantine.count()
        if rows_quarantined:
            quarantine.write.mode("overwrite").parquet(
                f"{cfg.lake_root}/quarantine/{table}/run_id={run_id}"
            )
        checked.unpersist()

        if rows_read != rows_written + rows_quarantined:
            raise ValueError(
                f"silver.hr.{table} reconciliation failed: read={rows_read} "
                f"written={rows_written} quarantined={rows_quarantined}"
            )
        valid_by_table[table] = silver
        log.info(
            "silver: hr.%s read=%d written=%d quarantined=%d warned=%d",
            table, rows_read, rows_written, rows_quarantined, rows_warned,
        )
        manifest.add_step(
            StepMetrics(
                step=f"silver.hr.{table}",
                rows_read=rows_read,
                rows_written=rows_written,
                rows_quarantined=rows_quarantined,
                rows_warned=rows_warned,
            )
        )

    silver_learning(spark, cfg, log, manifest, valid_by_table["employees"])


LEARNING_JSON_SCHEMA = (
    "event_id string, employee_id string, event_date string, event_type string, "
    "course_name string, text string, notes string, score string, _corrupt_record string"
)
EVENT_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss"


def read_bronze_learning(spark: SparkSession, cfg: Config) -> DataFrame:
    """Read Bronze learning lines through a glob, so year/month/day are not inferred."""
    return spark.read.parquet(f"{cfg.lake_root}/bronze/learning_activity/year=*/month=*/day=*")


def parse_learning(bronze: DataFrame) -> DataFrame:
    """Parse each raw line with an explicit schema; malformed lines get _corrupt_record."""
    parsed = F.from_json(
        "value",
        LEARNING_JSON_SCHEMA,
        {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
    )
    return bronze.select(
        "value", "_line_no", "_source_file", "_record_hash", "_ingested_at",
        parsed.alias("_json"),
    ).select("*", "_json.*").drop("_json")


def dedupe_events(df: DataFrame) -> tuple[DataFrame, int]:
    """Keep the first row per event_id by (_source_file, _line_no); null ids are kept."""
    order = Window.partitionBy("event_id").orderBy("_source_file", "_line_no")
    ranked = df.withColumn(
        "_rn", F.when(F.col("event_id").isNull(), 1).otherwise(F.row_number().over(order))
    )
    deduped = ranked.where("_rn > 1").count()
    return ranked.where("_rn = 1").drop("_rn"), deduped


def conform_learning(df: DataFrame, employees: DataFrame) -> DataFrame:
    """Types, event_text/event_ts/partitions, and the _chk_* columns the learning rules need."""
    df = df.drop("_corrupt_record").withColumns(
        {
            "event_text": F.coalesce("text", "notes"),
            "text_source_field": F.when(F.col("text").isNotNull(), "text").when(
                F.col("notes").isNotNull(), "notes"
            ),
        }
    )
    df = cast_columns(df, {"employee_id": "int", "score": "double"})
    df = df.withColumn("event_ts", F.to_timestamp("event_date", EVENT_TS_FORMAT)).withColumns(
        {"event_year": F.year("event_ts"), "event_month": F.month("event_ts")}
    )
    df = add_fk_flag(df, "employee_id", employees)
    hires = employees.select(
        F.col("employee_id").alias("_hire_emp"), F.col("hire_date").alias("_chk_hire_date")
    )
    return df.join(hires, df["employee_id"] == hires["_hire_emp"], "left").drop("_hire_emp")


def _counts_by(df: DataFrame, array_col: str) -> dict[str, int]:
    """Row counts per element of an array column (e.g. rule_ids, dq_warnings)."""
    rows = df.select(F.explode(array_col).alias("k")).groupBy("k").count().collect()
    return {r["k"]: r["count"] for r in sorted(rows, key=lambda r: r["k"])}


def silver_learning(
    spark: SparkSession,
    cfg: Config,
    log: logging.Logger,
    manifest: "RunManifest",
    employees: DataFrame,
) -> None:
    """Bronze lines → parse → malformed check → dedupe → rules → silver/learning_events."""
    run_id = manifest.run_id
    bronze = read_bronze_learning(spark, cfg)
    rows_read = bronze.count()

    parsed, malformed_q = apply_rules(parse_learning(bronze), "learning_events_raw", run_id)
    kept, rows_deduped = dedupe_events(parsed)
    checked = conform_learning(kept, employees).cache()
    valid, rules_q = apply_rules(checked, "learning_events", run_id)

    path = f"{cfg.lake_root}/silver/learning_events"
    valid.withColumn("_run_id", F.lit(run_id)).write.mode("overwrite").partitionBy(
        "event_year", "event_month"
    ).parquet(path)
    silver = spark.read.parquet(path)
    rows_written = silver.count()
    warnings = _counts_by(silver, "dq_warnings")
    rows_warned = silver.where(F.size("dq_warnings") > 0).count()

    quarantine = malformed_q.unionByName(rules_q, allowMissingColumns=True)
    errors = _counts_by(quarantine, "rule_ids")
    rows_quarantined = quarantine.count()
    if rows_quarantined:
        quarantine.write.mode("overwrite").parquet(
            f"{cfg.lake_root}/quarantine/learning_events/run_id={run_id}"
        )
    checked.unpersist()

    if rows_read != rows_written + rows_quarantined + rows_deduped:
        raise ValueError(
            f"silver.learning_events reconciliation failed: read={rows_read} "
            f"written={rows_written} quarantined={rows_quarantined} deduped={rows_deduped}"
        )
    log.info(
        "silver: learning_events read=%d = deduped %d + quarantined %d + written %d",
        rows_read, rows_deduped, rows_quarantined, rows_written,
    )
    log.info("silver: learning_events quarantine by rule %s", errors)
    log.info("silver: learning_events warnings by rule %s", warnings)
    manifest.add_step(
        StepMetrics(
            step="silver.learning_events",
            rows_read=rows_read,
            rows_written=rows_written,
            rows_quarantined=rows_quarantined,
            rows_deduped=rows_deduped,
            rows_warned=rows_warned,
            note=json.dumps({"quarantine_by_rule": errors, "warnings_by_rule": warnings}),
        )
    )


# =============================================================================
# 8. Gold
# =============================================================================
DEFAULT_CRITICAL_GAP = 2
REVIEW_BEFORE_HIRE = "performance_reviews.derived_period_end.before_hire"
ASSESSMENT_BEFORE_HIRE = "competency_assessments.assessed_date.before_hire"


def latest_period(reviews: DataFrame) -> str:
    """Most recent review_period in the data ('YYYY-Hn' sorts correctly as a string)."""
    return reviews.agg(F.max("review_period")).first()[0]


def gold_performance_history(reviews: DataFrame) -> DataFrame:
    """G1 (employee × period): recorded rating, previous rating and derived change."""
    by_emp = Window.partitionBy("employee_id").orderBy("review_period")
    return reviews.select(
        "employee_id",
        "review_period",
        "derived_period_start",
        "derived_period_end",
        "rating",
        F.lag("rating").over(by_emp).alias("previous_rating"),
        F.array_contains("dq_warnings", REVIEW_BEFORE_HIRE).alias("dq_reviewed_before_hire"),
    ).withColumn("derived_rating_change", F.col("rating") - F.col("previous_rating"))


def gold_performance_summary(history: DataFrame) -> DataFrame:
    """G1b (employee): latest rating, average rating and derived trend from G1."""
    latest = latest_period(history)
    per_emp = history.groupBy("employee_id").agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("review_count"),
    )
    last = history.where(F.col("review_period") == latest).select(
        "employee_id",
        F.col("review_period").alias("latest_period"),
        F.col("rating").alias("latest_rating"),
        "derived_rating_change",
    )
    change = F.col("derived_rating_change")
    return per_emp.join(last, "employee_id", "left").withColumn(
        "derived_rating_trend",
        F.when(change > 0, "improving").when(change < 0, "declining").when(change == 0, "stable"),
    )


def gold_competency_gaps(
    assessments: DataFrame, competencies: DataFrame, critical_gap: int = DEFAULT_CRITICAL_GAP
) -> DataFrame:
    """G2 (employee × competency): derived gap = required − proficiency, with severity."""
    comp = competencies.select(
        "competency_id",
        F.col("name").alias("competency_name"),
        F.col("category").alias("competency_category"),
    )
    gap = F.col("required_level") - F.col("proficiency_level")
    return (
        assessments.select(
            "employee_id",
            "competency_id",
            "proficiency_level",
            "required_level",
            "assessed_date",
            F.array_contains("dq_warnings", ASSESSMENT_BEFORE_HIRE).alias(
                "dq_assessed_before_hire"
            ),
        )
        .join(comp, "competency_id", "left")
        .withColumn("derived_gap", gap)
        .withColumn(
            "derived_gap_severity",
            F.when(gap >= critical_gap, "critical").when(gap > 0, "moderate").otherwise("none"),
        )
    )


def gold_team_outcomes(
    teams: DataFrame, employees: DataFrame, reviews: DataFrame, projects: DataFrame
) -> DataFrame:
    """G6 (team): headcount, latest-period ratings and project outcomes, each pre-aggregated."""
    period = latest_period(reviews)
    headcount = employees.groupBy("team_id").agg(F.count("*").alias("headcount"))
    ratings = (
        reviews.where(F.col("review_period") == period)
        .select("employee_id", "rating")
        .join(employees.select("employee_id", "team_id"), "employee_id")
        .groupBy("team_id")
        .agg(
            F.round(F.avg("rating"), 2).alias("avg_rating_latest"),
            F.round(F.avg((F.col("rating") >= 4).cast("double")), 4).alias("pct_rated_4_plus"),
        )
        .withColumn("rating_period", F.lit(period))
    )
    proj = projects.groupBy("team_id").agg(
        F.count("*").alias("project_count"),
        F.round(F.avg("outcome_score"), 2).alias("avg_outcome_score"),
        F.round(F.avg((F.col("outcome_status") == "on-time").cast("double")), 4).alias(
            "on_time_rate"
        ),
    )
    return (
        teams.select("team_id", F.col("name").alias("team_name"), "department")
        .join(headcount, "team_id", "left")
        .join(ratings, "team_id", "left")
        .join(proj, "team_id", "left")
        .withColumns(
            {
                "headcount": F.coalesce("headcount", F.lit(0)),
                "project_count": F.coalesce("project_count", F.lit(0)),
            }
        )
        .withColumn("has_projects", F.col("project_count") > 0)
    )


def gold_competency_distribution(
    assessments: DataFrame, employees: DataFrame, teams: DataFrame, competencies: DataFrame
) -> DataFrame:
    """G7 (competency × department): assessed counts, averages and level distribution."""
    dept = employees.select("employee_id", "team_id").join(
        teams.select("team_id", "department"), "team_id"
    )
    level = F.col("proficiency_level")
    return (
        assessments.join(dept.select("employee_id", "department"), "employee_id")
        .groupBy("competency_id", "department")
        .agg(
            F.count("*").alias("assessed_count"),
            F.round(F.avg("proficiency_level"), 2).alias("avg_proficiency"),
            F.round(F.avg("required_level"), 2).alias("avg_required"),
            F.round(
                F.avg((F.col("proficiency_level") >= F.col("required_level")).cast("double")), 4
            ).alias("pct_meeting_requirement"),
            *[F.sum((level == n).cast("int")).alias(f"level_{n}_count") for n in range(1, 6)],
        )
        .join(
            competencies.select(
                "competency_id",
                F.col("name").alias("competency_name"),
                F.col("category").alias("competency_category"),
            ),
            "competency_id",
            "left",
        )
    )


QUIZ_EVENT_TYPES = ("quiz_attempted", "quiz_passed")


def gold_learning_progress(events: DataFrame) -> DataFrame:
    """G4 (employee × course): event counts and recorded completion/certification.

    A course counts as completed only if a course_completed event exists; module and
    quiz events never imply completion.
    """
    etype = F.col("event_type")
    return (
        events.groupBy("employee_id", "course_name")
        .agg(
            *[F.sum((etype == t).cast("int")).alias(f"{t}_count") for t in LEARNING_EVENT_TYPES],
            F.count("*").alias("total_events"),
            F.min("event_ts").alias("first_event_ts"),
            F.max("event_ts").alias("last_event_ts"),
            F.min(F.when(etype == "course_completed", F.col("event_ts"))).alias("completed_at"),
            F.max(F.when(etype.isin(*QUIZ_EVENT_TYPES), F.col("score"))).alias("best_quiz_score"),
        )
        .withColumns(
            {
                "is_course_completed": F.col("course_completed_count") > 0,
                "has_certification": F.col("certification_earned_count") > 0,
            }
        )
        .withColumn(
            "dq_completion_without_enrollment",
            F.col("is_course_completed") & (F.col("enrollment_count") == 0),
        )
    )


def gold_learning_summary(employees: DataFrame, progress: DataFrame) -> DataFrame:
    """G4b (employee): course and event totals for every employee; zeros, never nulls."""
    per_emp = progress.groupBy("employee_id").agg(
        F.sum((F.col("enrollment_count") > 0).cast("int")).alias("courses_enrolled"),
        F.sum(F.col("is_course_completed").cast("int")).alias("courses_completed"),
        F.sum(F.col("has_certification").cast("int")).alias("courses_certified"),
        F.sum("total_events").alias("total_events"),
    )
    totals = ("courses_enrolled", "courses_completed", "courses_certified", "total_events")
    return (
        employees.select("employee_id")
        .join(per_emp, "employee_id", "left")
        .withColumns({c: F.coalesce(F.col(c), F.lit(0)).cast("long") for c in totals})
    )


def _critical_gap_counts(gaps: DataFrame) -> DataFrame:
    """Per-employee count of critical competency gaps (rollup of G2)."""
    return gaps.groupBy("employee_id").agg(
        F.sum((F.col("derived_gap_severity") == "critical").cast("int")).alias(
            "critical_gap_count"
        )
    )


def gold_promotion_readiness(
    talent_review: DataFrame, summary: DataFrame, gaps: DataFrame, learning_summary: DataFrame
) -> DataFrame:
    """G3 (employee): an analytic shortlist view, not an HR decision.

    potential_rating / promotion_readiness are recorded values; the context columns are
    joined from per-employee rollups only (G1b, G2, G4b), so rows never fan out.
    """
    return (
        talent_review.select(
            "employee_id", "review_cycle", "potential_rating", "promotion_readiness"
        )
        .withColumn(
            "derived_high_potential_ready_now",
            (F.col("potential_rating") == "High") & (F.col("promotion_readiness") == "Ready Now"),
        )
        .join(summary.select("employee_id", "latest_rating", "derived_rating_trend"),
              "employee_id", "left")
        .join(_critical_gap_counts(gaps), "employee_id", "left")
        .join(learning_summary.select("employee_id", "courses_completed"), "employee_id", "left")
        .withColumns(
            {
                "critical_gap_count": F.coalesce("critical_gap_count", F.lit(0)),
                "courses_completed": F.coalesce("courses_completed", F.lit(0)),
            }
        )
    )


def gold_attrition_context(
    talent_review: DataFrame,
    history: DataFrame,
    summary: DataFrame,
    gaps: DataFrame,
    learning_summary: DataFrame,
) -> DataFrame:
    """G5 (employee): recorded attrition_risk next to performance, gap and learning context.

    Descriptive context only (association, not causation); not a prediction.
    """
    ratings = (
        history.withColumn(
            "_period", F.concat(F.lit("rating_"), F.regexp_replace("review_period", "-", "_"))
        )
        .groupBy("employee_id")
        .pivot("_period")
        .agg(F.first("rating"))
    )
    return (
        talent_review.select("employee_id", "review_cycle", "attrition_risk")
        .join(ratings, "employee_id", "left")
        .join(summary.select("employee_id", "derived_rating_trend"), "employee_id", "left")
        .join(_critical_gap_counts(gaps), "employee_id", "left")
        .join(learning_summary, "employee_id", "left")
        .withColumn("critical_gap_count", F.coalesce("critical_gap_count", F.lit(0)))
    )


def gold_attrition_by_trend(attrition: DataFrame) -> DataFrame:
    """G5b (attrition_risk × derived_rating_trend): headcount and share of each risk band.

    Association only, not causation.
    """
    band = Window.partitionBy("attrition_risk")
    return (
        attrition.groupBy("attrition_risk", "derived_rating_trend")
        .agg(F.count("*").alias("headcount"))
        .withColumn(
            "pct_of_risk_band", F.round(F.col("headcount") / F.sum("headcount").over(band), 4)
        )
    )


GOLD_GRAINS = {
    "employee_performance_history": ("employee_id", "review_period"),
    "employee_performance_summary": ("employee_id",),
    "employee_competency_gaps": ("employee_id", "competency_id"),
    "team_performance_outcomes": ("team_id",),
    "competency_distribution": ("competency_id", "department"),
    "learning_progress": ("employee_id", "course_name"),
    "learning_summary": ("employee_id",),
    "promotion_readiness": ("employee_id",),
    "attrition_risk_context": ("employee_id",),
    "attrition_by_trend": ("attrition_risk", "derived_rating_trend"),
}


def write_gold(
    spark: SparkSession,
    cfg: Config,
    log: logging.Logger,
    manifest: "RunManifest",
    name: str,
    df: DataFrame,
) -> None:
    """Stamp, write (overwrite), read back, check grain uniqueness, record the manifest step."""
    path = f"{cfg.lake_root}/gold/{name}"
    df.withColumns(
        {"_gold_run_id": F.lit(manifest.run_id), "_built_at": F.current_timestamp()}
    ).write.mode("overwrite").parquet(path)
    gold = spark.read.parquet(path)
    rows_written = gold.count()
    keys = GOLD_GRAINS[name]
    distinct_keys = gold.select(*keys).distinct().count()
    if distinct_keys != rows_written:
        raise ValueError(f"gold.{name}: {rows_written - distinct_keys} duplicate {keys} keys")
    log.info("gold: %s rows=%d grain=%s unique", name, rows_written, "×".join(keys))
    manifest.add_step(StepMetrics(step=f"gold.{name}", rows_written=rows_written))


def aggregate_to_gold(
    spark: SparkSession, cfg: Config, log: logging.Logger, manifest: "RunManifest"
) -> None:
    """Build all Gold datasets (G1–G7) from Silver only."""
    silver = {
        t: spark.read.parquet(f"{cfg.lake_root}/silver/{t}")
        for t in ("teams", "competencies", "employees", "projects", "performance_reviews",
                  "competency_assessments", "talent_review")
    }
    events = spark.read.parquet(
        f"{cfg.lake_root}/silver/learning_events/event_year=*/event_month=*"
    )
    history = gold_performance_history(silver["performance_reviews"])
    summary = gold_performance_summary(history)
    gaps = gold_competency_gaps(silver["competency_assessments"], silver["competencies"])
    progress = gold_learning_progress(events)
    learning = gold_learning_summary(silver["employees"], progress)
    attrition = gold_attrition_context(silver["talent_review"], history, summary, gaps, learning)
    datasets = {
        "employee_performance_history": history,
        "employee_performance_summary": summary,
        "employee_competency_gaps": gaps,
        "team_performance_outcomes": gold_team_outcomes(
            silver["teams"], silver["employees"], silver["performance_reviews"],
            silver["projects"],
        ),
        "competency_distribution": gold_competency_distribution(
            silver["competency_assessments"], silver["employees"], silver["teams"],
            silver["competencies"],
        ),
        "learning_progress": progress,
        "learning_summary": learning,
        "promotion_readiness": gold_promotion_readiness(
            silver["talent_review"], summary, gaps, learning
        ),
        "attrition_risk_context": attrition,
        "attrition_by_trend": gold_attrition_by_trend(attrition),
    }
    for name, df in datasets.items():
        write_gold(spark, cfg, log, manifest, name, df)


# =============================================================================
# 9. Run manifest
# =============================================================================
@dataclass
class StepMetrics:
    """Row counts and status for one pipeline step."""

    step: str
    status: str = "SUCCESS"
    rows_read: int = 0
    rows_written: int = 0
    rows_quarantined: int = 0
    rows_deduped: int = 0
    rows_warned: int = 0
    note: str = ""


@dataclass
class RunManifest:
    """Record of one pipeline run: timing, status, config and per-step metrics."""

    run_id: str
    is_local: bool
    source_dir: str
    lake_root: str
    started_at: str = field(default_factory=lambda: _utc_now().isoformat())
    ended_at: Optional[str] = None
    duration_s: Optional[float] = None
    status: str = "RUNNING"
    steps: list[StepMetrics] = field(default_factory=list)
    _t0: float = field(default_factory=time.monotonic, repr=False)

    def add_step(self, metrics: StepMetrics) -> None:
        """Append one step's metrics."""
        self.steps.append(metrics)

    def finish(self, status: str) -> None:
        """Stamp end time, duration and final status."""
        self.ended_at = _utc_now().isoformat()
        self.duration_s = round(time.monotonic() - self._t0, 3)
        self.status = status

    def to_dict(self) -> dict[str, Any]:
        """Serialisable view of the manifest (internal timer excluded)."""
        data = asdict(self)
        data.pop("_t0")
        return data


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def new_run_id() -> str:
    """Sortable, unique run id, e.g. 20261015T101500Z-a1b2c3."""
    return f"{_utc_now():%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"


def manifest_path(cfg: Config, run_id: str) -> str:
    """Folder that holds one run's manifest."""
    return f"{cfg.lake_root}/_runs/run_id={run_id}"


def write_manifest(spark: SparkSession, cfg: Config, manifest: RunManifest) -> str:
    """Write the manifest as one JSON line through Spark (works for local paths and s3a://)."""
    path = manifest_path(cfg, manifest.run_id)
    payload = json.dumps(manifest.to_dict(), sort_keys=True)
    (
        spark.createDataFrame([(payload,)], "value string")
        .coalesce(1)
        .write.mode("overwrite")
        .text(path)
    )
    return path


# =============================================================================
# 10. main()
# =============================================================================
def main() -> int:
    """Run the pipeline end to end; return the process exit code."""
    run_id = new_run_id()
    log = get_logger(run_id)
    cfg = load_config()
    log.info(
        "start is_local=%s source_dir=%s lake_root=%s", cfg.is_local, cfg.source_dir, cfg.lake_root
    )
    manifest = RunManifest(
        run_id=run_id, is_local=cfg.is_local, source_dir=cfg.source_dir, lake_root=cfg.lake_root
    )
    spark: Optional[SparkSession] = None
    exit_code = 0
    try:
        spark = create_spark_session(cfg)
        log.info("spark %s started", spark.version)
        ingest_into_bronze(spark, cfg, log, manifest)
        transform_to_silver(spark, cfg, log, manifest)
        aggregate_to_gold(spark, cfg, log, manifest)
        manifest.finish("SUCCESS")
    except Exception:  # noqa: BLE001 - log, record FAILED, exit non-zero
        log.exception("run failed")
        manifest.finish("FAILED")
        exit_code = 1
    finally:
        if spark is not None:
            try:
                path = write_manifest(spark, cfg, manifest)
                log.info("manifest written to %s", path)
            except Exception:  # noqa: BLE001
                log.exception("could not write manifest")
                exit_code = 1
            spark.stop()
    log.info("end status=%s duration_s=%s", manifest.status, manifest.duration_s)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
