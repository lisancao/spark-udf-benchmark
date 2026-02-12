"""UDF Performance Benchmark: Compare overhead of different UDF types.

Benchmarks 6 UDF types across 3 workload tiers to measure relative overhead:
1. Built-in functions (baseline, ~1x)
2. SQL UDFs (CREATE FUNCTION)
3. Arrow UDFs (Spark 4.1, SPARK-53014)
4. Pandas UDFs (@pandas_udf)
5. Arrow-optimized Python UDFs (@udf with arrow enabled)
6. Pickle Python UDFs (@udf with arrow disabled)

Note: Scala/Java UDFs are excluded (require compiled JAR).

Workloads:
- arithmetic: x * 2 + 1 (trivial)
- string: upper(x) + '_SUFFIX' (moderate)
- cdf: 0.5 * (1 + erf(x / sqrt(2))) cumulative normal CDF (complex)
"""

import math
import statistics
import time
import uuid
from typing import Any, Callable, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType

from .result import UdfBenchmarkResult


class UdfPipelineBenchmark:
    """Benchmark UDF types across workload complexity tiers."""

    WORKLOADS = ["arithmetic", "string", "cdf"]
    UDF_TYPES = [
        "builtin", "sql_udf", "arrow_udf",
        "pandas_udf", "arrow_opt_python", "pickle_python",
    ]

    def __init__(
        self,
        spark: SparkSession,
        warmup_runs: int = 1,
        benchmark_runs: int = 3,
    ):
        self.spark = spark
        self.warmup_runs = warmup_runs
        self.benchmark_runs = benchmark_runs
        self.run_id = str(uuid.uuid4())[:8]

    # ------------------------------------------------------------------
    # Data generation
    # ------------------------------------------------------------------

    def generate_test_data(self, row_count: int) -> DataFrame:
        """Generate a DataFrame with id, value (double), and name (string)."""
        return (
            self.spark.range(0, row_count)
            .withColumn("value", (F.col("id") % 1000).cast("double") / 100.0)
            .withColumn(
                "name",
                F.concat(F.lit("item_"), F.lpad((F.col("id") % 10000).cast("string"), 5, "0")),
            )
        )

    # ------------------------------------------------------------------
    # Built-in expressions (baseline)
    # ------------------------------------------------------------------

    @staticmethod
    def _builtin_arithmetic(df: DataFrame) -> DataFrame:
        return df.withColumn("result", F.col("value") * 2 + 1)

    @staticmethod
    def _builtin_string(df: DataFrame) -> DataFrame:
        return df.withColumn("result", F.concat(F.upper(F.col("name")), F.lit("_SUFFIX")))

    @staticmethod
    def _builtin_cdf(df: DataFrame) -> DataFrame:
        sqrt2 = math.sqrt(2)
        # Abramowitz & Stegun approximation for erf (no built-in erf in Spark)
        x = F.col("value")
        t = F.lit(1.0) / (F.lit(1.0) + F.lit(0.3275911) * F.abs(x / sqrt2))
        approx = (
            F.lit(1.0)
            - (
                F.lit(0.254829592) * t
                - F.lit(0.284496736) * t * t
                + F.lit(1.421413741) * t * t * t
                - F.lit(1.453152027) * t * t * t * t
                + F.lit(1.061405429) * t * t * t * t * t
            )
            * F.exp(-x * x / 2)
        )
        erf_approx = F.when(x / sqrt2 >= 0, approx).otherwise(F.lit(1.0) - approx)
        return df.withColumn("result", F.lit(0.5) * (F.lit(1.0) + erf_approx))

    # ------------------------------------------------------------------
    # SQL UDFs
    # ------------------------------------------------------------------

    def _register_sql_udfs(self) -> None:
        self.spark.sql("DROP TEMPORARY FUNCTION IF EXISTS sql_arith")
        self.spark.sql(
            "CREATE TEMPORARY FUNCTION sql_arith(x DOUBLE) RETURNS DOUBLE "
            "RETURN x * 2 + 1"
        )
        self.spark.sql("DROP TEMPORARY FUNCTION IF EXISTS sql_string")
        self.spark.sql(
            "CREATE TEMPORARY FUNCTION sql_string(x STRING) RETURNS STRING "
            "RETURN concat(upper(x), '_SUFFIX')"
        )

    def _sql_udf_arithmetic(self, df: DataFrame) -> DataFrame:
        df.createOrReplaceTempView("_udf_bench_data")
        return self.spark.sql(
            "SELECT *, sql_arith(value) AS result FROM _udf_bench_data"
        )

    def _sql_udf_string(self, df: DataFrame) -> DataFrame:
        df.createOrReplaceTempView("_udf_bench_data")
        return self.spark.sql(
            "SELECT *, sql_string(name) AS result FROM _udf_bench_data"
        )

    # ------------------------------------------------------------------
    # Arrow-optimized UDFs (@udf with useArrow=True, SPARK-43082)
    # ------------------------------------------------------------------

    @staticmethod
    def _make_arrow_udfs() -> Dict[str, Callable]:
        """Create Arrow-optimized UDFs via useArrow=True.

        These go through ArrowEvalPython (vectorized Arrow batches)
        unlike the config toggle which is still row-at-a-time.
        """
        from pyspark.sql.functions import udf

        @udf(DoubleType(), useArrow=True)
        def arrow_arith(x):
            return float(x) * 2 + 1 if x is not None else None

        @udf(StringType(), useArrow=True)
        def arrow_string(x):
            return x.upper() + "_SUFFIX" if x is not None else None

        @udf(DoubleType(), useArrow=True)
        def arrow_cdf(x):
            if x is None:
                return None
            import math as _math
            return 0.5 * (1.0 + _math.erf(float(x) / _math.sqrt(2.0)))

        return {"arithmetic": arrow_arith, "string": arrow_string, "cdf": arrow_cdf}

    # ------------------------------------------------------------------
    # Pandas UDFs
    # ------------------------------------------------------------------

    @staticmethod
    def _make_pandas_udfs() -> Dict[str, Callable]:
        """Create pandas UDFs. Returns dict of workload -> udf."""
        from pyspark.sql.functions import pandas_udf

        @pandas_udf(DoubleType())
        def pandas_arith(x):
            return x * 2 + 1

        @pandas_udf(StringType())
        def pandas_string(x):
            return x.str.upper() + "_SUFFIX"

        @pandas_udf(DoubleType())
        def pandas_cdf(x):
            import numpy as np
            import pandas as pd
            from scipy.special import erf as _erf
            result = 0.5 * (1.0 + _erf(x.to_numpy() / np.sqrt(2.0)))
            return pd.Series(result)

        return {"arithmetic": pandas_arith, "string": pandas_string, "cdf": pandas_cdf}

    # ------------------------------------------------------------------
    # Regular Python UDFs (arrow-optimised or pickle)
    # ------------------------------------------------------------------

    @staticmethod
    def _make_python_udfs() -> Dict[str, Callable]:
        """Create plain @udf functions. Arrow vs pickle is toggled by config."""
        from pyspark.sql.functions import udf

        @udf(DoubleType())
        def py_arith(x):
            return float(x) * 2 + 1 if x is not None else None

        @udf(StringType())
        def py_string(x):
            return x.upper() + "_SUFFIX" if x is not None else None

        @udf(DoubleType())
        def py_cdf(x):
            if x is None:
                return None
            import math as _math
            return 0.5 * (1.0 + _math.erf(float(x) / _math.sqrt(2.0)))

        return {"arithmetic": py_arith, "string": py_string, "cdf": py_cdf}

    # ------------------------------------------------------------------
    # Timing helper
    # ------------------------------------------------------------------

    def _time_udf(self, df: DataFrame, apply_fn: Callable, runs: int) -> List[float]:
        """Apply *apply_fn(df)* and force materialisation. Return list of durations.

        Uses ``agg(count("result"))`` instead of ``.count()`` to prevent the
        Catalyst optimizer from pruning the UDF column via ColumnPruning.
        """
        durations: List[float] = []
        for _ in range(runs):
            result_df = apply_fn(df)
            start = time.perf_counter()
            result_df.agg(F.count("result")).collect()
            durations.append(time.perf_counter() - start)
        return durations

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_comparison(
        self,
        row_count: int = 100_000,
    ) -> Dict[str, UdfBenchmarkResult]:
        """Run all UDF benchmarks and return results keyed by '{udf_type}_{workload}'."""

        print(f"\n{'=' * 70}")
        print("UDF PERFORMANCE BENCHMARK")
        print(f"{'=' * 70}")
        print(f"Run ID: {self.run_id}")
        print(f"Rows: {row_count:,}")
        print(f"Warmup runs: {self.warmup_runs}, Benchmark runs: {self.benchmark_runs}")

        # Generate + cache test data
        print("\nGenerating test data...")
        df = self.generate_test_data(row_count)
        df.cache()
        df.count()
        print(f"Generated {row_count:,} rows  (id, value, name)")

        # Register SQL UDFs
        self._register_sql_udfs()

        # Build UDF lookup
        arrow_udfs = self._make_arrow_udfs()
        pandas_udfs = self._make_pandas_udfs()
        python_udfs = self._make_python_udfs()

        def _apply(udf_fn: Callable, col_name: str) -> Callable:
            def _inner(d: DataFrame) -> DataFrame:
                return d.withColumn("result", udf_fn(F.col(col_name)))
            return _inner

        udf_matrix: Dict[str, Dict[str, Any]] = {
            "builtin": {
                "arithmetic": lambda d: self._builtin_arithmetic(d),
                "string": lambda d: self._builtin_string(d),
                "cdf": lambda d: self._builtin_cdf(d),
            },
            "sql_udf": {
                "arithmetic": lambda d: self._sql_udf_arithmetic(d),
                "string": lambda d: self._sql_udf_string(d),
                # CDF not expressible in SQL UDF
            },
            "arrow_udf": {
                "arithmetic": _apply(arrow_udfs["arithmetic"], "value"),
                "string": _apply(arrow_udfs["string"], "name"),
                "cdf": _apply(arrow_udfs["cdf"], "value"),
            },
            "pandas_udf": {
                "arithmetic": _apply(pandas_udfs["arithmetic"], "value"),
                "string": _apply(pandas_udfs["string"], "name"),
                "cdf": _apply(pandas_udfs["cdf"], "value"),
            },
            "arrow_opt_python": {
                "arithmetic": _apply(python_udfs["arithmetic"], "value"),
                "string": _apply(python_udfs["string"], "name"),
                "cdf": _apply(python_udfs["cdf"], "value"),
            },
            "pickle_python": {
                "arithmetic": _apply(python_udfs["arithmetic"], "value"),
                "string": _apply(python_udfs["string"], "name"),
                "cdf": _apply(python_udfs["cdf"], "value"),
            },
        }

        results: Dict[str, UdfBenchmarkResult] = {}
        baseline_medians: Dict[str, float] = {}

        for workload in self.WORKLOADS:
            workload_labels = {
                "arithmetic": "Arithmetic (x*2+1)",
                "string": "String (upper+suffix)",
                "cdf": "Normal CDF",
            }
            print(f"\n{'─' * 70}")
            print(
                f"WORKLOAD: {workload} ({workload_labels[workload]})  |  "
                f"{row_count:,} rows  |  {self.benchmark_runs} runs (median)"
            )
            print(f"{'─' * 70}")
            print(f"{'UDF Type':<22} {'Duration (s)':>13} {'Rows/s':>12} {'Overhead':>10}")
            print(f"{'─' * 70}")

            for udf_type in self.UDF_TYPES:
                apply_fn = udf_matrix.get(udf_type, {}).get(workload)
                if apply_fn is None:
                    print(f"{udf_type:<22} {'(not supported)':>13}")
                    continue

                # Toggle arrow config for python UDFs
                if udf_type == "arrow_opt_python":
                    self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
                elif udf_type == "pickle_python":
                    self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

                try:
                    self._time_udf(df, apply_fn, self.warmup_runs)
                    durations = self._time_udf(df, apply_fn, self.benchmark_runs)
                except Exception as exc:
                    print(f"{udf_type:<22} {'(error)':>13}  {type(exc).__name__}")
                    continue
                finally:
                    if udf_type in ("arrow_opt_python", "pickle_python"):
                        self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

                median_dur = statistics.median(durations)
                rps = row_count / median_dur if median_dur > 0 else 0

                if udf_type == "builtin":
                    baseline_medians[workload] = median_dur
                    overhead = 1.0
                else:
                    baseline = baseline_medians.get(workload, median_dur)
                    overhead = median_dur / baseline if baseline > 0 else 0

                key = f"{udf_type}_{workload}"
                results[key] = UdfBenchmarkResult(
                    udf_type=udf_type,
                    workload=workload,
                    row_count=row_count,
                    duration_seconds=median_dur,
                    rows_per_second=rps,
                    relative_overhead=overhead,
                    run_id=self.run_id,
                )

                overhead_str = "baseline" if udf_type == "builtin" else f"{overhead:.1f}x"
                print(f"{udf_type:<22} {median_dur:>13.4f} {rps:>12,.0f} {overhead_str:>10}")

        df.unpersist()

        print(f"\n{'=' * 70}")
        print(f"Benchmark complete. {len(results)} measurements recorded.")
        print(f"{'=' * 70}")

        return results

    def cleanup(self) -> None:
        """Drop temporary SQL UDFs."""
        self.spark.sql("DROP TEMPORARY FUNCTION IF EXISTS sql_arith")
        self.spark.sql("DROP TEMPORARY FUNCTION IF EXISTS sql_string")
