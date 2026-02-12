"""Smoke tests for the UDF performance benchmark.

Runs with TINY scale (1 000 rows) to verify UDF types produce correct
results and that timing is recorded.

Arrow UDFs are skipped in classic mode (codegen limitation in Spark 4.1).
"""

import pytest
from pyspark.sql import functions as F

from spark_udf_benchmark import UdfBenchmarkResult, UdfPipelineBenchmark

TINY_ROWS = 1_000


@pytest.fixture(scope="module")
def benchmark(spark):
    return UdfPipelineBenchmark(spark, warmup_runs=0, benchmark_runs=1)


@pytest.fixture(scope="module")
def test_df(benchmark):
    df = benchmark.generate_test_data(TINY_ROWS)
    df.cache()
    df.count()
    return df


class TestDataGeneration:

    def test_row_count(self, test_df):
        assert test_df.count() == TINY_ROWS

    def test_columns_present(self, test_df):
        assert set(test_df.columns) == {"id", "value", "name"}

    def test_value_range(self, test_df):
        stats = test_df.agg(
            F.min("value").alias("min_v"), F.max("value").alias("max_v"),
        ).collect()[0]
        assert stats.min_v >= 0.0
        assert stats.max_v < 10.0

    def test_name_format(self, test_df):
        sample = test_df.select("name").limit(5).collect()
        for row in sample:
            assert row.name.startswith("item_")


class TestBuiltinUDFs:

    def test_arithmetic(self, benchmark, test_df):
        result = benchmark._builtin_arithmetic(test_df)
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(1.0)

    def test_string(self, benchmark, test_df):
        result = benchmark._builtin_string(test_df)
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == "ITEM_00000_SUFFIX"

    def test_cdf(self, benchmark, test_df):
        result = benchmark._builtin_cdf(test_df)
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(0.5, abs=0.01)


class TestSqlUDFs:

    def test_arithmetic(self, benchmark, test_df):
        benchmark._register_sql_udfs()
        result = benchmark._sql_udf_arithmetic(test_df)
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(1.0)

    def test_string(self, benchmark, test_df):
        benchmark._register_sql_udfs()
        result = benchmark._sql_udf_string(test_df)
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == "ITEM_00000_SUFFIX"


class TestArrowUDFs:

    @pytest.fixture(scope="class")
    def arrow_udfs(self):
        return UdfPipelineBenchmark._make_arrow_udfs()

    def test_arithmetic(self, test_df, arrow_udfs):
        result = test_df.withColumn("result", arrow_udfs["arithmetic"](F.col("value")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(1.0)

    def test_string(self, test_df, arrow_udfs):
        result = test_df.withColumn("result", arrow_udfs["string"](F.col("name")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == "ITEM_00000_SUFFIX"

    def test_cdf(self, test_df, arrow_udfs):
        result = test_df.withColumn("result", arrow_udfs["cdf"](F.col("value")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(0.5, abs=0.01)


class TestPandasUDFs:

    @pytest.fixture(scope="class")
    def pandas_udfs(self):
        return UdfPipelineBenchmark._make_pandas_udfs()

    def test_arithmetic(self, test_df, pandas_udfs):
        result = test_df.withColumn("result", pandas_udfs["arithmetic"](F.col("value")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(1.0)

    def test_string(self, test_df, pandas_udfs):
        result = test_df.withColumn("result", pandas_udfs["string"](F.col("name")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == "ITEM_00000_SUFFIX"

    def test_cdf(self, test_df, pandas_udfs):
        result = test_df.withColumn("result", pandas_udfs["cdf"](F.col("value")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(0.5, abs=0.01)


class TestPythonUDFs:

    @pytest.fixture(scope="class")
    def python_udfs(self):
        return UdfPipelineBenchmark._make_python_udfs()

    def test_arithmetic(self, spark, test_df, python_udfs):
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        result = test_df.withColumn("result", python_udfs["arithmetic"](F.col("value")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(1.0)

    def test_string(self, spark, test_df, python_udfs):
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        result = test_df.withColumn("result", python_udfs["string"](F.col("name")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == "ITEM_00000_SUFFIX"

    def test_cdf(self, spark, test_df, python_udfs):
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        result = test_df.withColumn("result", python_udfs["cdf"](F.col("value")))
        row = result.filter(F.col("id") == 0).select("result").collect()[0]
        assert row.result == pytest.approx(0.5, abs=0.01)


class TestRunComparison:

    def test_run_comparison_returns_results(self, benchmark):
        results = benchmark.run_comparison(row_count=TINY_ROWS)
        assert len(results) >= 14
        for _key, result in results.items():
            assert isinstance(result, UdfBenchmarkResult)
            assert result.row_count == TINY_ROWS
            assert result.duration_seconds > 0
            assert result.rows_per_second > 0
            assert result.run_id == benchmark.run_id

    def test_baseline_overhead_is_one(self, benchmark):
        results = benchmark.run_comparison(row_count=TINY_ROWS)
        for _key, result in results.items():
            if result.udf_type == "builtin":
                assert result.relative_overhead == pytest.approx(1.0)
