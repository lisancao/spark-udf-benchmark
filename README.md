# Spark UDF Performance Benchmark

**Need custom Python logic in Spark? Here's what it costs and how to minimize it.**

Reproducible benchmark comparing the overhead of 6 PySpark UDF types across 3 workload complexity tiers, running on Spark Connect. The goal: help you choose the right UDF type for your use case.

## The Practical Question

Everyone starts with `@udf`. It works. But at scale, it hits **~40x overhead** vs built-in functions. The good news: you have options. The bad news: not all "Arrow" options are equal.

## Transport vs Execution: Why Arrow Config Doesn't Help @udf

UDF performance has two independent axes: **transport format** (how data moves between JVM and Python) and **execution model** (how Python processes each row). Most people conflate them.

<p align="center">
  <img src="graphics/05_transport_vs_execution.svg" alt="Transport vs Execution Model — 2x2 Matrix" width="800"/>
</p>

|  | Pickle Transport | Arrow Transport |
|---|---|---|
| **Row-at-a-time execution** | `@udf` (default) **~40x** | `@udf` + arrow config **~40x** |
| **Vectorized batch execution** | *(not possible)* | `@pandas_udf` **~5-7x** |

**Key insight:** Switching transport format from pickle to Arrow while keeping row-at-a-time execution gives you **39.7x vs 40.6x** — essentially zero difference. The bottleneck is per-row Python function calls, not serialization format. Arrow batches only help when paired with vectorized execution.

Arrow UDFs (`@udf(useArrow=True)`) sit between these extremes at **~6-17x** — they use Arrow batch transport with `ArrowEvalPython` but still call Python per-row.

## Python UDF Comparison (50M rows)

If you need Python, which UDF type should you use?

<p align="center">
  <img src="graphics/04_python_udf_comparison.svg" alt="Python UDF Comparison — Absolute Wall-Clock Times" width="800"/>
</p>

| UDF Type | Arithmetic | String | CDF |
|----------|-----------|--------|-----|
| Pickle `@udf` (default) | 4.71s | 5.63s | 5.52s |
| Arrow-opt `@udf` | 4.81s (1.0x) | 5.65s (1.0x) | 5.51s (1.0x) |
| Arrow UDF (`useArrow`) | 1.97s (**2.4x faster**) | 2.90s (**1.9x faster**) | 2.49s (**2.2x faster**) |
| Pandas UDF | 0.77s (**6.1x faster**) | 2.43s (**2.3x faster**) | 0.81s (**6.8x faster**) |

Switching from `@udf` to `@pandas_udf` saves **3.9 seconds per 50M rows** on arithmetic. Arrow UDF saves 2.7 seconds. Arrow-opt config saves nothing.

## Decision Guide

```
Need custom logic in Spark?
│
├─ Can you express it in SQL?
│  └─ YES → SQL UDF (CREATE TEMPORARY FUNCTION)     ~1.0x  ← free
│
├─ Can you use built-in pyspark.sql.functions?
│  └─ YES → Built-in functions                      ~1.0x  ← free
│
├─ Need Python — can you use Pandas/NumPy ops?
│  └─ YES → @pandas_udf                             ~5-7x  ← best Python option
│
├─ Need Python — can't use Pandas?
│  └─ TRY → @udf(useArrow=True)                     ~6-17x ← 2x better than default
│
└─ Last resort
   └─ @udf (default pickle)                         ~35-40x
```

## Performance Hierarchy

<p align="center">
  <img src="graphics/03_performance_hierarchy.svg" alt="PySpark UDF Performance Hierarchy" width="800"/>
</p>

## Overhead vs Built-in Functions (50M rows)

For reference, here's the full picture including JVM-native baselines. Note that the high multipliers (40x) reflect comparison against Tungsten-optimized built-ins — the absolute times (4.7s for 50M rows) may be perfectly acceptable for your workload.

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

| # | Type | Implementation | Overhead at 50M | Wall-clock (arithmetic) |
|---|------|---------------|----------------|------------------------|
| 1 | Built-in functions | `pyspark.sql.functions` | 1.0x (baseline) | 0.12s |
| 2 | SQL UDFs | `CREATE TEMPORARY FUNCTION` | ~1x | 0.10s |
| 3 | Pandas UDFs | `@pandas_udf` | ~5-7x | 0.77s |
| 4 | Arrow UDFs | `@udf(useArrow=True)` (SPARK-43082) | ~6-17x | 1.97s |
| 5 | Arrow-opt Python | `@udf` + `arrow.pyspark.enabled=true` | ~35-41x | 4.81s |
| 6 | Pickle Python | `@udf` + `arrow.pyspark.enabled=false` | ~35-40x | 4.71s |

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

1. **If you need Python, use `@pandas_udf`** — 5-7x overhead at any scale, 6.1x faster than default `@udf` on arithmetic. NumPy/Pandas vectorization is the key lever.
2. **`@udf(useArrow=True)` is a solid middle ground** — 2-2.4x faster than default `@udf` when you can't use Pandas APIs. Uses `ArrowEvalPython` for vectorized transport.
3. **Arrow transport config doesn't help `@udf`** — 39.7x vs 40.6x. Changes serialization format, not execution model. The bottleneck is per-row Python calls.
4. **SQL UDFs are free** — they compile to Catalyst expressions, same as built-in functions.
5. **`@arrow_udf` (SPARK-53014) has a codegen bug** — `FoldableUnevaluable.doGenCode` fails in Spark 4.1.0; `@udf(useArrow=True)` is the working alternative.

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
│   ├── 03_performance_hierarchy.svg
│   ├── 04_python_udf_comparison.svg
│   └── 05_transport_vs_execution.svg
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
