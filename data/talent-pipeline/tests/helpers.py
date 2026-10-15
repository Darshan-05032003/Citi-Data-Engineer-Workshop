"""Shared constants and small helpers for the tests (fixtures live in conftest.py)."""
from pathlib import Path

import job

JOB_DIR = Path(job.__file__).resolve().parent
FIXTURES = JOB_DIR / "tests" / "fixtures"
MINI_SOURCE = FIXTURES / "mini_source"

# step -> (read, written, quarantined, deduped, warned)
EXPECTED_STEPS = {
    "bronze.hr.employees": (6, 6, 0, 0, 0),
    "bronze.hr.teams": (3, 3, 0, 0, 0),
    "bronze.hr.competencies": (3, 3, 0, 0, 0),
    "bronze.hr.projects": (2, 2, 0, 0, 0),
    "bronze.hr.performance_reviews": (12, 12, 0, 0, 0),
    "bronze.hr.competency_assessments": (8, 8, 0, 0, 0),
    "bronze.hr.talent_review": (6, 6, 0, 0, 0),
    "bronze.learning_activity": (10, 10, 0, 0, 0),
    "silver.hr.teams": (3, 3, 0, 0, 0),
    "silver.hr.competencies": (3, 3, 0, 0, 0),
    "silver.hr.employees": (6, 6, 0, 0, 0),
    "silver.hr.projects": (2, 2, 0, 0, 0),
    "silver.hr.performance_reviews": (12, 11, 1, 0, 1),
    "silver.hr.competency_assessments": (8, 7, 1, 0, 1),
    "silver.hr.talent_review": (6, 6, 0, 0, 0),
    "silver.learning_events": (10, 6, 3, 1, 2),
}
EXPECTED_GOLD_ROWS = {
    "employee_performance_history": 11,
    "employee_performance_summary": 6,
    "employee_competency_gaps": 7,
    "team_performance_outcomes": 3,
    "competency_distribution": 6,
    "learning_progress": 5,
    "learning_summary": 6,
    "promotion_readiness": 6,
    "attrition_risk_context": 6,
    "attrition_by_trend": 5,
}


def _steps(manifest: job.RunManifest) -> dict[str, job.StepMetrics]:
    return {s.step: s for s in manifest.steps}


def _gold(spark, cfg: job.Config, name: str):
    return spark.read.parquet(f"{cfg.lake_root}/gold/{name}")
