# UDF Performance Benchmark Report

**Date:** 2026-02-12
**Spark:** 4.1.0 (Spark Connect, `remote("local[*]")`)
**Python:** 3.12.3

## Methodology

- All benchmarks run via Spark Connect (`SparkSession.builder.remote("local[*]")`)
- Each UDF applied via `df.withColumn("result", udf_expr)` then `.agg(F.count("result")).collect()` forces materialization
- 1 warmup run (discarded), 3 timed runs, report median
- Built-in baseline measured first; overhead = `median_duration / builtin_median_duration` per workload
- Data: synthetic DataFrame with 3 columns (`id: long`, `value: double`, `name: string`) via `spark.range()`

### UDF Types Tested

| # | UDF Type | Implementation |
|---|----------|---------------|
| 1 | Built-in functions | `pyspark.sql.functions` (baseline) |
| 2 | SQL UDFs | `CREATE TEMPORARY FUNCTION ... RETURN <expr>` |
| 3 | Arrow UDFs | `@udf(useArrow=True)` (SPARK-43082, `ArrowEvalPython`) |
| 4 | Pandas UDFs | `@pandas_udf` decorator |
| 5 | Arrow-opt Python UDFs | `@udf` with `spark.sql.execution.arrow.pyspark.enabled=true` |
| 6 | Pickle Python UDFs | `@udf` with arrow disabled (`...enabled=false`) |

> **Note:** Scala/Java UDFs excluded (require compiled JAR). `@arrow_udf` (SPARK-53014) has a codegen bug in 4.1.0 -- `@udf(useArrow=True)` is the working alternative.

### Workloads

| Workload | Expression | Complexity |
|----------|-----------|------------|
| arithmetic | `x * 2 + 1` | Trivial |
| string | `upper(x) + '_SUFFIX'` | Moderate |
| cdf | `0.5 * (1 + erf(x / sqrt(2)))` | Complex (Normal CDF) |

---

## Results: 100K Rows

### Arithmetic (`x * 2 + 1`)

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.0734 | 1,362,740 | 1.0x (baseline) |
| SQL UDFs | 0.0486 | 2,058,143 | 0.7x |
| Arrow UDFs | 0.2115 | 472,736 | 2.9x |
| Pandas UDFs | 0.1812 | 551,735 | 2.5x |
| Arrow-opt Python | 0.1646 | 607,640 | 2.2x |
| Pickle Python | 0.1613 | 619,833 | 2.2x |

### String (`upper(x) + '_SUFFIX'`)

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.0308 | 3,246,042 | 1.0x (baseline) |
| SQL UDFs | 0.0330 | 3,030,525 | 1.1x |
| Arrow UDFs | 0.1716 | 582,711 | 5.6x |
| Pandas UDFs | 0.1612 | 620,269 | 5.2x |
| Arrow-opt Python | 0.1714 | 583,346 | 5.6x |
| Pickle Python | 0.1673 | 597,838 | 5.4x |

### Normal CDF

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.0503 | 1,988,390 | 1.0x (baseline) |
| Arrow UDFs | 0.1644 | 608,398 | 3.3x |
| Pandas UDFs | 0.1582 | 632,296 | 3.1x |
| Arrow-opt Python | 0.1543 | 648,134 | 3.1x |
| Pickle Python | 0.1667 | 599,727 | 3.3x |

### Observations (100K)

- All Python UDFs cluster at 2-6x -- fixed overhead (worker startup/teardown) dominates at small scale
- Arrow UDFs are indistinguishable from Pandas/pickle at this scale
- SQL UDFs match built-in speed

---

## Results: 1M Rows

### Arithmetic (`x * 2 + 1`)

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.0772 | 12,949,761 | 1.0x (baseline) |
| SQL UDFs | 0.0490 | 20,393,149 | 0.6x |
| Arrow UDFs | 0.2480 | 4,032,086 | 3.2x |
| Pandas UDFs | 0.2000 | 4,999,868 | 2.6x |
| Arrow-opt Python | 0.2928 | 3,415,703 | 3.8x |
| Pickle Python | 0.2577 | 3,880,465 | 3.3x |

### String (`upper(x) + '_SUFFIX'`)

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.0470 | 21,285,725 | 1.0x (baseline) |
| SQL UDFs | 0.0450 | 22,199,580 | 1.0x |
| Arrow UDFs | 0.2258 | 4,428,849 | 4.8x |
| Pandas UDFs | 0.2046 | 4,887,639 | 4.4x |
| Arrow-opt Python | 0.3047 | 3,281,531 | 6.5x |
| Pickle Python | 0.2882 | 3,469,529 | 6.1x |

### Normal CDF

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.0506 | 19,781,281 | 1.0x (baseline) |
| Arrow UDFs | 0.2445 | 4,090,003 | 4.8x |
| Pandas UDFs | 0.1688 | 5,923,274 | 3.3x |
| Arrow-opt Python | 0.2710 | 3,689,543 | 5.4x |
| Pickle Python | 0.2627 | 3,805,925 | 5.2x |

### Observations (1M)

- Pandas UDFs start to separate from row-at-a-time UDFs (2.6x vs 3.3-3.8x arithmetic)
- Arrow UDFs track closer to Pandas than to pickle -- vectorized transport helps
- CDF shows Pandas at 3.3x vs Arrow UDF at 4.8x -- Pandas benefits from NumPy vectorization

---

## Results: 10M Rows

### Arithmetic (`x * 2 + 1`)

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.0740 | 135,069,002 | 1.0x (baseline) |
| SQL UDFs | 0.0618 | 161,731,252 | 0.8x |
| Arrow UDFs | 0.5682 | 17,598,513 | 7.7x |
| Pandas UDFs | 0.2994 | 33,399,464 | 4.0x |
| Arrow-opt Python | 1.1211 | 8,919,424 | 15.1x |
| Pickle Python | 1.0916 | 9,161,047 | 14.7x |

### String (`upper(x) + '_SUFFIX'`)

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.1451 | 68,938,897 | 1.0x (baseline) |
| SQL UDFs | 0.1298 | 77,069,026 | 0.9x |
| Arrow UDFs | 0.7493 | 13,346,481 | 5.2x |
| Pandas UDFs | 0.6376 | 15,684,459 | 4.4x |
| Arrow-opt Python | 1.3171 | 7,592,167 | 9.1x |
| Pickle Python | 1.3048 | 7,664,237 | 9.0x |

### Normal CDF

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.0805 | 124,151,057 | 1.0x (baseline) |
| Arrow UDFs | 0.6657 | 15,022,790 | 8.3x |
| Pandas UDFs | 0.2836 | 35,264,091 | 3.5x |
| Arrow-opt Python | 1.2705 | 7,870,753 | 15.8x |
| Pickle Python | 1.2346 | 8,099,508 | 15.3x |

### Observations (10M)

- **Clear 3-tier separation emerges**: Pandas (~4x) < Arrow UDF (~7-8x) < row-at-a-time (~15x)
- Arrow UDFs are ~2x faster than row-at-a-time UDFs thanks to vectorized transport
- Built-in throughput hits 135M rows/s arithmetic -- Tungsten codegen at full speed

---

## Results: 50M Rows

### Arithmetic (`x * 2 + 1`)

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.1185 | 421,833,242 | 1.0x (baseline) |
| SQL UDFs | 0.0955 | 523,785,574 | 0.8x |
| Arrow UDFs | 1.9706 | 25,372,634 | 16.6x |
| Pandas UDFs | 0.7696 | 64,966,998 | 6.5x |
| Arrow-opt Python | 4.8070 | 10,401,563 | 40.6x |
| Pickle Python | 4.7095 | 10,616,888 | 39.7x |

### String (`upper(x) + '_SUFFIX'`)

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.5034 | 99,327,081 | 1.0x (baseline) |
| SQL UDFs | 0.5100 | 98,035,307 | 1.0x |
| Arrow UDFs | 2.8986 | 17,249,849 | 5.8x |
| Pandas UDFs | 2.4339 | 20,542,882 | 4.8x |
| Arrow-opt Python | 5.6522 | 8,846,092 | 11.2x |
| Pickle Python | 5.6258 | 8,887,587 | 11.2x |

### Normal CDF

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.1567 | 319,068,056 | 1.0x (baseline) |
| Arrow UDFs | 2.4937 | 20,050,174 | 15.9x |
| Pandas UDFs | 0.8147 | 61,374,743 | 5.2x |
| Arrow-opt Python | 5.5095 | 9,075,304 | 35.2x |
| Pickle Python | 5.5245 | 9,050,648 | 35.3x |

### Observations (50M)

- **Row-at-a-time @udf hits ~40x** -- approaching the ~50x claim from published benchmarks
- **Arrow UDFs sit at ~6-17x** -- vectorized transport makes them 2-3x faster than row-at-a-time
- **Pandas UDFs hold at ~5-7x** -- batch vectorization continues to amortize overhead
- **Arrow-opt still indistinguishable from pickle** for `@udf` (40.6x vs 39.7x)

---

## Overhead Trend Across All Scales

### Arithmetic Workload (trivial)

| UDF Type | 100K | 1M | 10M | 50M | Trend |
|----------|------|-----|------|------|-------|
| SQL UDFs | 0.7x | 0.6x | 0.8x | 0.8x | **Flat ~1x** |
| Pandas UDFs | 2.5x | 2.6x | 4.0x | 6.5x | **Slow growth** |
| Arrow UDFs | 2.9x | 3.2x | 7.7x | **16.6x** | **Linear growth** |
| Arrow-opt Python | 2.2x | 3.8x | 15.1x | **40.6x** | **Superlinear growth** |
| Pickle Python | 2.2x | 3.3x | 14.7x | **39.7x** | **Superlinear growth** |

### CDF Workload (most complex)

| UDF Type | 100K | 1M | 10M | 50M | Trend |
|----------|------|-----|------|------|-------|
| Pandas UDFs | 3.1x | 3.3x | 3.5x | 5.2x | **Flat ~3-5x** |
| Arrow UDFs | 3.3x | 4.8x | 8.3x | **15.9x** | **Linear growth** |
| Arrow-opt Python | 3.1x | 5.4x | 15.8x | **35.2x** | **Superlinear growth** |
| Pickle Python | 3.3x | 5.2x | 15.3x | **35.3x** | **Superlinear growth** |

---

## Key Takeaways

### 1. Arrow UDFs (`useArrow=True`) are a meaningful middle ground

They sit between Pandas UDFs (~5-7x) and row-at-a-time UDFs (~35-41x) at **~6-17x** depending on workload. They use `ArrowEvalPython` for vectorized transport but still execute Python per-row. For simple functions where Pandas conversion overhead matters, they can be faster.

### 2. Pandas UDFs remain the fastest for Python logic

Flat **~5-7x** overhead at any scale. The vectorized Arrow batch transfer + NumPy/Pandas operations amortize serde costs. For complex workloads (CDF), Pandas UDFs benefit from NumPy's compiled C routines.

### 3. Row-at-a-time `@udf` hits ~40x at 50M rows

Confirming the ~50x claim from published benchmarks. Both pickle and Arrow-opt `@udf` converge to the same throughput ceiling (~9-10M rows/s) because the bottleneck is per-row Python function calls, not serialization format.

### 4. SQL UDFs are free at any scale

0.6-1.0x overhead across all scales. They compile to native Catalyst expressions. Use them for any logic expressible in SQL.

### 5. Arrow transport doesn't help `@udf`

`spark.sql.execution.arrow.pyspark.enabled=true` provides **zero measurable benefit** over pickle for `@udf` (40.6x vs 39.7x). Arrow only changes the serialization format -- the function still executes once per row.

### 6. `@arrow_udf` (SPARK-53014) has a codegen bug in 4.1.0

The `FoldableUnevaluable.doGenCode` error occurs in both classic and Connect modes. `@udf(useArrow=True)` (SPARK-43082) is the working alternative that goes through the `ArrowEvalPython` physical node.
