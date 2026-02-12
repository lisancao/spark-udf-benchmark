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
    """Create a SparkSession for testing (local[2], minimal resources)."""
    session = (
        SparkSession.builder.appName("udf-benchmark-tests")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
