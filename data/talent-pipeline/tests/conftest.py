"""Shared pytest fixtures: a local Spark session and a temporary lake root."""
import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


def pytest_configure(config: pytest.Config) -> None:
    """Make Spark workers (and the job subprocesses) use this interpreter unless set already."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped local[1] SparkSession for unit and integration tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("talent-pipeline-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def lake_root(tmp_path: Path) -> str:
    """Temporary LAKE_ROOT directory, removed by pytest after each test."""
    return str(tmp_path / "lake")
