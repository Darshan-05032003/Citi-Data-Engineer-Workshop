"""Cloud smoke test (marker `cloud`): the deployed S3 lake after a cloud run.

Skipped unless CLOUD_SMOKE=1. It only reads: the newest run manifest and the Gold datasets under
LAKE_ROOT (s3a://bucket/prefix or s3://bucket/prefix) through pyarrow's S3 filesystem, with credentials
from the standard AWS environment/profile chain and the region from AWS_REGION (default ap-south-1).

    source ENVIRONMENT.config && unset AWS_ENDPOINT_URL AWS_ENDPOINT_URL_S3
    CLOUD_SMOKE=1 LAKE_ROOT=s3a://coding-workshop-datalake-f2a96bad/lake pytest -q -m cloud
"""
import json
import os

import pandas as pd
import pytest
from pyarrow import fs

from tests.test_pipeline_integration import EXPECTED

pytestmark = pytest.mark.cloud

GOLD_ROWS = {
    "employee_performance_history": 4000, "employee_performance_summary": 2000,
    "employee_competency_gaps": 4000, "promotion_readiness": 2000, "learning_summary": 2000,
    "attrition_risk_context": 2000, "team_performance_outcomes": 100, "competency_distribution": 400,
}


@pytest.fixture(scope="module")
def lake():
    """(filesystem, 'bucket/prefix') for the deployed lake; skip unless CLOUD_SMOKE=1."""
    if os.environ.get("CLOUD_SMOKE") != "1":
        pytest.skip("cloud smoke test: set CLOUD_SMOKE=1 and LAKE_ROOT=s3a://bucket/prefix to run it")
    root = os.environ.get("LAKE_ROOT", "")
    if not root.startswith(("s3://", "s3a://")):
        pytest.fail(f"CLOUD_SMOKE=1 needs LAKE_ROOT=s3a://bucket/prefix, got {root!r}", pytrace=False)
    filesystem = fs.S3FileSystem(region=os.environ.get("AWS_REGION", "ap-south-1"))
    return filesystem, root.split("://", 1)[1].rstrip("/")


def _manifest(lake) -> dict:
    filesystem, base = lake
    runs = [i.path for i in filesystem.get_file_info(fs.FileSelector(f"{base}/_runs"))
            if i.base_name.startswith("run_id=")]
    assert runs, f"no run manifests under {base}/_runs"
    part = next(i.path for i in filesystem.get_file_info(fs.FileSelector(max(runs)))
                if i.base_name.startswith("part-") and i.base_name.endswith(".txt"))
    with filesystem.open_input_stream(part) as f:
        return json.loads(f.read().decode("utf-8"))


def _gold(lake, name: str) -> pd.DataFrame:
    filesystem, base = lake
    return pd.read_parquet(f"{base}/gold/{name}", filesystem=filesystem)


def test_latest_cloud_run_succeeded_with_26_steps(lake) -> None:
    """The newest manifest is a finished cloud run: SUCCESS, not local, 26 steps, no FAILED step."""
    manifest = _manifest(lake)
    assert manifest["status"] == "SUCCESS", manifest["status"]
    assert manifest["is_local"] is False
    assert len(manifest["steps"]) == 26
    assert all(s["status"] == "SUCCESS" for s in manifest["steps"])
    steps = {s["step"]: s for s in manifest["steps"]}
    for table, rows in EXPECTED["hr"].items():
        assert steps[f"bronze.hr.{table}"]["rows_written"] == rows, table
    assert steps["silver.learning_events"]["rows_written"] == EXPECTED["learning_valid"]


def test_cloud_gold_key_numbers(lake) -> None:
    """Row counts and the headline Gold numbers of the cloud lake equal the expected values."""
    for name, rows in GOLD_ROWS.items():
        assert len(_gold(lake, name)) == rows, name
    summary = _gold(lake, "employee_performance_summary")
    assert summary["derived_rating_trend"].value_counts().to_dict() == EXPECTED["trend"]
    gaps = _gold(lake, "employee_competency_gaps")
    assert {int(k): int(v) for k, v in gaps["derived_gap"].value_counts().items()} == EXPECTED["gaps"]
    assert int((gaps["derived_gap_severity"] == "critical").sum()) == EXPECTED["critical"]
    promotion = _gold(lake, "promotion_readiness")
    assert int(promotion["derived_high_potential_ready_now"].sum()) == EXPECTED["hipo_ready_now"]
    risk = _gold(lake, "attrition_risk_context")["attrition_risk"].value_counts().to_dict()
    assert risk == EXPECTED["risk"]
    teams = _gold(lake, "team_performance_outcomes")
    assert int(teams["has_projects"].sum()) == EXPECTED["teams_with_projects"]
    assert int(teams["project_count"].sum()) == EXPECTED["project_count"]
    progress = _gold(lake, "learning_progress")
    assert int(progress["dq_completion_without_enrollment"].sum()) == EXPECTED["completion_without_enrollment"]
