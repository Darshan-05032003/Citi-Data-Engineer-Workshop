"""Shared pytest fixtures: a local Spark session, a temporary lake root, the mini dataset and one
in-process pipeline run on it that most layer tests read."""
import logging
import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

import job
from tests.helpers import MINI_SOURCE


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


@pytest.fixture(scope="session")
def mini_source() -> Path:
    """The hand-made mini dataset in the real file formats (7 CSVs + learning_activity/)."""
    return MINI_SOURCE


class _Collector(logging.Handler):
    """Collects the formatted message of every record, for tests on what a run logged."""

    def __init__(self, sink: list) -> None:
        super().__init__(level=logging.INFO)
        self.sink = sink

    def emit(self, record: logging.LogRecord) -> None:
        self.sink.append((record.levelname, record.getMessage()))


@pytest.fixture(scope="session")
def run_log() -> list:
    """(level, message) of every log line of the shared `run` fixture, filled while it runs."""
    return []


@pytest.fixture(scope="session")
def run(spark, tmp_path_factory, run_log):
    """Run the three stages once in-process on the mini dataset and write the manifest."""
    lake = str(tmp_path_factory.mktemp("stages") / "lake")
    cfg = job.Config(is_local=True, source_dir=str(MINI_SOURCE), lake_root=lake)
    manifest = job.RunManifest(
        run_id="mini-run", is_local=True, source_dir=cfg.source_dir, lake_root=lake
    )
    manifest.spark_version = spark.version  # as main() does after starting Spark
    log = job.get_logger(manifest.run_id)
    log.addHandler(_Collector(run_log))
    job.ingest_into_bronze(spark, cfg, log, manifest)
    job.transform_to_silver(spark, cfg, log, manifest)
    job.aggregate_to_gold(spark, cfg, log, manifest)
    manifest.finish("SUCCESS")
    manifest_dir = job.write_manifest(spark, cfg, manifest)
    return cfg, manifest, manifest_dir
