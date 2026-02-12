# Spark UDF Performance Benchmark

Reproducible benchmark comparing the overhead of 6 PySpark UDF types across 3 workload complexity tiers, running on Spark Connect.

## Performance Hierarchy

<p align="center">
  <img src="graphics/03_performance_hierarchy.svg" alt="PySpark UDF Performance Hierarchy" width="800"/>
</p>

## Results Summary (50M rows, Spark 4.1.0)

<p align="center">
  <img src="graphics/01_overhead_at_50m.svg" alt="UDF Overhead at 50M Rows" width="800"/>
</p>

| UDF Type | Arithmetic | String | CDF | Trend |
|----------|-----------|--------|-----|-------|
| Built-in functions | 1.0x | 1.0x | 1.0x | Baseline |
| SQL UDFs | 0.8x | 1.0x | n/a | Same as built-in |
| Pandas UDFs | 6.5x | 4.8x | 5.2x | Flat ~5-7x |
| Arrow UDFs | **16.6x** | 5.8x | **15.9x** | Between Pandas and row-at-a-time |
| Arrow-opt Python | **40.6x** | 11.2x | **35.2x** | Superlinear growth |
| Pickle Python | **39.7x** | 11.2x | **35.3x** | Superlinear growth |

> Row-at-a-time `@udf` hits **~40x** overhead at 50M rows. Arrow UDFs (`useArrow=True`) cut this roughly in half by using vectorized transport, but Pandas UDFs remain fastest for Python at ~5-7x.

## Overhead Scaling (100K to 50M Rows)

<p align="center">
  <img src="graphics/02_overhead_scaling.svg" alt="Overhead Scaling from 100K to 50M Rows" width="800"/>
</p>

> Full analysis across 100K, 1M, 10M, and 50M rows in [docs/report.md](docs/report.md).

## Quick Start

### Docker (fully reproducible, no local Spark needed)

```bash
# Run benchmark at 100K rows
docker compose run --rm benchmark --rows 100000

# Run at 50M rows
docker compose run --rm benchmark --rows 50000000

# Run tests
docker compose run --rm test
```

### Local (requires Spark 4.1 + Java 17)

```bash
# Install dependencies
pip install -e ".[dev]"

# Run tests (1K rows, ~10s)
make test

# Run benchmark at different scales
make bench-100k
make bench-1m
make bench-10m
make bench-50m
```

### Direct invocation

```bash
PYSPARK_PYTHON=python3 PYTHONPATH=src python -m spark_udf_benchmark \
    --rows 1000000 --warmup 1 --runs 3 --output-dir results/
```

## UDF Types Tested

| # | Type | Implementation | Overhead at 50M |
|---|------|---------------|----------------|
| 1 | Built-in functions | `pyspark.sql.functions` | 1.0x (baseline) |
| 2 | SQL UDFs | `CREATE TEMPORARY FUNCTION` | ~1x |
| 3 | Arrow UDFs | `@udf(useArrow=True)` (SPARK-43082) | ~6-17x |
| 4 | Pandas UDFs | `@pandas_udf` | ~5-7x |
| 5 | Arrow-opt Python | `@udf` + `arrow.pyspark.enabled=true` | ~35-41x |
| 6 | Pickle Python | `@udf` + `arrow.pyspark.enabled=false` | ~35-40x |

> Scala/Java UDFs excluded (require compiled JAR). Uses Spark Connect (`remote("local[*]")`) for all benchmarks.

## Workloads

| Workload | Expression | Complexity |
|----------|-----------|------------|
| arithmetic | `x * 2 + 1` | Trivial |
| string | `upper(x) + '_SUFFIX'` | Moderate |
| cdf | `0.5 * (1 + erf(x / sqrt(2)))` | Complex (Normal CDF) |

## Methodology

- All benchmarks run via Spark Connect (`SparkSession.builder.remote("local[*]")`)
- Each UDF applied via `df.withColumn("result", udf_expr)` then `.agg(F.count("result")).collect()` forces materialization (prevents Catalyst column pruning)
- 1 warmup run (discarded), 3 timed runs, report median
- Overhead = `median_duration / builtin_median_duration` per workload
- Synthetic data: `spark.range()` with 3 columns (`id: long`, `value: double`, `name: string`)

## Key Findings

1. **SQL UDFs are free** -- they compile to Catalyst expressions, same path as built-in
2. **Pandas UDFs plateau at ~5-7x** -- vectorized Arrow batch transfer amortizes serde
3. **Arrow UDFs (`useArrow=True`) hit ~6-17x** -- vectorized transport via `ArrowEvalPython`, but still per-row Python execution
4. **Row-at-a-time `@udf` hits ~40x at 50M rows** -- per-row JVM-Python serde dominates
5. **Arrow transport doesn't help `@udf`** -- changes format, not execution model (still row-at-a-time)
6. **`@arrow_udf` (SPARK-53014) has a codegen bug** -- `FoldableUnevaluable.doGenCode` fails in both classic and Connect modes in Spark 4.1.0; `@udf(useArrow=True)` is the working alternative

## Project Structure

```
spark-udf-benchmark/
├── src/spark_udf_benchmark/
│   ├── __init__.py          # Package exports
│   ├── __main__.py          # CLI entry point
│   ├── benchmark.py         # UdfPipelineBenchmark class
│   └── result.py            # UdfBenchmarkResult dataclass
├── tests/
│   ├── conftest.py          # Spark Connect session fixture
│   └── test_benchmark.py    # 20 smoke tests (1K rows)
├── graphics/                # SVG visualizations
│   ├── 01_overhead_at_50m.svg
│   ├── 02_overhead_scaling.svg
│   └── 03_performance_hierarchy.svg
├── results/                 # Checked-in JSON from our runs
│   ├── 100k.json
│   ├── 1m.json
│   ├── 10m.json
│   └── 50m.json
├── docs/
│   └── report.md            # Full analysis with all scales
├── Dockerfile               # Spark 4.1.0 + Java 17 + Python 3.12
├── docker-compose.yml       # One-command execution
├── Makefile                 # make test / make bench-50m
└── pyproject.toml           # Dependencies and project config
```

## Environment

| Component | Version |
|-----------|---------|
| Apache Spark | 4.1.0 |
| Python | 3.12 |
| Java | 17 |
| PyArrow | 17-18 |
| pandas | 2.x |
| scipy | 1.12+ |
| Mode | Spark Connect |
