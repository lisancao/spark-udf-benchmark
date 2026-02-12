"""Pytest fixtures for UDF benchmark tests."""

import os
import sys

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session", autouse=True)
def _set_pyspark_python():
    """Ensure Spark workers use the same Python as the driver."""
    orig = os.environ.get("PYSPARK_PYTHON")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    yield
    if orig is None:
        os.environ.pop("PYSPARK_PYTHON", None)
    else:
        os.environ["PYSPARK_PYTHON"] = orig


@pytest.fixture(scope="session")
def spark():
    """Create a Spark Connect session for testing (local, minimal resources)."""
    session = (
        SparkSession.builder.appName("udf-benchmark-tests")
        .remote("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()
