# Spark UDF Performance Benchmark

Reproducible benchmark comparing the overhead of 6 PySpark UDF types across 3 workload complexity tiers. Built to independently verify the performance claims in the [Arrow-Python UDF hierarchy graphic](../graphics/03_performance_hierarchy.svg).

## Results Summary (50M rows, Spark 4.1.0)

| UDF Type | Arithmetic | String | CDF | Trend |
|----------|-----------|--------|-----|-------|
| Built-in functions | 1.0x | 1.0x | 1.0x | Baseline |
| SQL UDFs | 0.9x | 1.0x | n/a | Same as built-in |
| Pandas UDFs | 7.4x | 4.6x | 5.3x | Flat ~5-7x |
| Arrow-opt Python | **45.0x** | 10.7x | **37.2x** | Superlinear growth |
| Pickle Python | **44.8x** | 10.8x | **34.4x** | Superlinear growth |

> Pickle Python UDFs hit **44.8x** overhead at 50M rows, confirming the graphic's ~50x claim.
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
| 3 | Arrow UDFs | `@arrow_udf` (SPARK-53014) | Error in classic mode |
| 4 | Pandas UDFs | `@pandas_udf` | ~5-7x |
| 5 | Arrow-opt Python | `@udf` + `arrow.pyspark.enabled=true` | ~37-45x |
| 6 | Pickle Python | `@udf` + `arrow.pyspark.enabled=false` | ~35-45x |

> Scala/Java UDFs excluded (require compiled JAR). Arrow UDFs require Spark Connect.

## Workloads

| Workload | Expression | Complexity |
|----------|-----------|------------|
| arithmetic | `x * 2 + 1` | Trivial |
| string | `upper(x) + '_SUFFIX'` | Moderate |
| cdf | `0.5 * (1 + erf(x / sqrt(2)))` | Complex (Normal CDF) |

## Methodology

- Each UDF applied via `df.withColumn("result", udf_expr)` then `.agg(F.count("result")).collect()` forces materialization (prevents Catalyst column pruning)
- 1 warmup run (discarded), 3 timed runs, report median
- Overhead = `median_duration / builtin_median_duration` per workload
- Synthetic data: `spark.range()` with 3 columns (`id: long`, `value: double`, `name: string`)

## Key Findings

1. **SQL UDFs are free** -- they compile to Catalyst expressions, same path as built-in
2. **Pandas UDFs plateau at ~5-7x** -- vectorized Arrow batch transfer amortizes serde
3. **Row-at-a-time `@udf` hits ~45x at 50M rows** -- per-row JVM-Python serde dominates
4. **Arrow transport doesn't help `@udf`** -- changes format, not execution model (still row-at-a-time)
5. **The overhead is about per-row serde, not network** -- JVM-Python is always local socket

## Project Structure

```
spark-udf-benchmark/
├── src/spark_udf_benchmark/
│   ├── __init__.py          # Package exports
│   ├── __main__.py          # CLI entry point
│   ├── benchmark.py         # UdfPipelineBenchmark class
│   └── result.py            # UdfBenchmarkResult dataclass
├── tests/
│   ├── conftest.py          # Spark session fixture
│   └── test_benchmark.py    # 20 smoke tests (1K rows)
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
