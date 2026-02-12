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

> **Note:** Scala/Java UDFs excluded (require compiled JAR). `@arrow_udf` (SPARK-53014) works in Spark 4.1.0 — see [preliminary results](#arrow_udf-preliminary-results) below.

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

## Disambiguating "Arrow UDFs" in Spark 4.1

Spark 4.1 introduces three different mechanisms that all involve "Arrow" and "UDF" in their names. They are not interchangeable:

| Mechanism | Decorator / Config | Python receives | Physical operator | Overhead |
|-----------|-------------------|-----------------|-------------------|----------|
| Arrow-opt config | `@udf` + `arrow.pyspark.enabled=true` | Scalars (per row) | `BatchEvalPython` | ~40x |
| Arrow UDF (useArrow) | `@udf(useArrow=True)` (SPARK-43082) | Scalars (per row) | `ArrowEvalPython` | ~6-17x |
| Arrow-native UDF | `@arrow_udf` (SPARK-53014) | `pyarrow.Array` (batch) | `ArrowEvalPython` | ~5x (verified) |

The critical distinction is the **physical operator**: `BatchEvalPython` processes rows one at a time regardless of wire format (hence ~40x for both pickle and Arrow-opt config). `ArrowEvalPython` uses batched Arrow transport, but what your function *receives* still varies — `@udf(useArrow=True)` unpacks batches back to scalars for per-row calls, while `@arrow_udf` and `@pandas_udf` pass entire arrays/Series for vectorized processing.

We verified that `@arrow_udf` works in Spark 4.1.0 (both classic and Connect modes). It performs on par with `@pandas_udf` (~5x) and is faster for string operations because `pyarrow.compute` avoids Pandas overhead. See the [preliminary results](#arrow_udf-preliminary-results) below.

---

## Transport vs Execution: Why Arrow Config Doesn't Help @udf

UDF performance depends on two independent axes: **transport format** (how data moves between JVM and Python) and **execution model** (how Python processes each row).

|  | Pickle Transport | Arrow Transport |
|---|---|---|
| **Row-at-a-time** | `@udf` (default) **~40x** | `@udf` + arrow config **~40x** |
| **Batch transport, scalar exec** | — | `@udf(useArrow=True)` **~6-17x** |
| **Vectorized batch** | — | `@pandas_udf` **~5-7x**, `@arrow_udf` **~5x** |

The data confirms this model:

- **Transport change alone (pickle → arrow, same execution):** 39.7x → 40.6x = **no improvement**. The `arrow.pyspark.enabled` config changes serialization format but keeps `BatchEvalPython`. The function still executes once per row. Per-row Python function call overhead dominates.
- **Physical operator change (BatchEvalPython → ArrowEvalPython, scalar exec):** 39.7x → 16.6x = **2.4x improvement**. `@udf(useArrow=True)` switches to `ArrowEvalPython` which batches Arrow transport between JVM and Python, reducing serde overhead. But the function still receives scalars and executes per-row.
- **Execution model change (scalar → vectorized, same operator):** 16.6x → 5-7x = **2-3x further improvement**. `@pandas_udf` operates on entire Pandas Series using NumPy, amortizing all per-row overhead. `@arrow_udf` does the same with `pyarrow.compute`.

**Bottom line:** The performance hierarchy is: physical operator > execution model > wire format. Changing the wire format alone (`arrow.pyspark.enabled`) does nothing. Changing the operator (`useArrow=True`) gives 2.4x. Changing both operator and execution model (`@pandas_udf`) gives 6.1x.

---

## Key Takeaways

### 1. If you need Python, use `@pandas_udf`

Flat **~5-7x** overhead at any scale. At 50M rows: **0.77s** arithmetic vs **4.71s** for default `@udf` -- a 6.1x wall-clock saving. For complex workloads (CDF), the gap is even larger (6.8x) because NumPy's compiled C routines handle vectorized math.

**When to use:** Your Python logic can operate on Pandas Series or DataFrames (most numeric, string, and statistical operations).

### 2. `@udf(useArrow=True)` is a solid middle ground

Sits at **~6-17x** depending on workload. At 50M rows: **1.97s** arithmetic vs **4.71s** for default `@udf` -- a 2.4x saving. Uses `ArrowEvalPython` for vectorized batch transport but still executes Python per-row.

**When to use:** You need per-row Python logic but can't express it with Pandas/NumPy APIs. Provides meaningful improvement over default `@udf` with minimal code changes (just add `useArrow=True`).

### 3. Arrow transport config has zero effect on `@udf`

`spark.sql.execution.arrow.pyspark.enabled=true` gives **40.6x vs 39.7x** (pickle) -- no measurable benefit. This config only changes the serialization format. The bottleneck is per-row Python function calls (~9-10M rows/s ceiling), not how bytes are serialized. See the Transport vs Execution section above.

### 4. SQL UDFs are free -- use them first

**0.6-1.0x** overhead across all scales. They compile to native Catalyst expressions, same execution plan as built-in functions. Any logic expressible in SQL should use SQL UDFs before reaching for Python.

### 5. Default `@udf` hits ~40x at 50M rows -- avoid at scale

Both pickle and Arrow-opt `@udf` converge to **~40x overhead** at scale (4.71s and 4.81s for 50M rows arithmetic). At 50M rows, that's **4.7 seconds** vs **0.12 seconds** for built-in functions. If your workload processes tens of millions of rows, the default `@udf` is the worst Python option available.

### 6. `@arrow_udf` (SPARK-53014) works and matches `@pandas_udf`

The `@arrow_udf` decorator (SPARK-53014) is a genuinely new execution model where your function receives `pyarrow.Array` objects and can use `pyarrow.compute` for vectorized operations — no Pandas conversion overhead.

We verified this works in Spark 4.1.0 in both classic and Spark Connect modes. Preliminary results at 10M rows:

| UDF Type | Arithmetic | String | CDF |
|----------|-----------|--------|-----|
| `@arrow_udf` | 0.37s (5.4x) | 0.61s (4.2x) | 0.34s (4.2x) |
| `@pandas_udf` | 0.35s (5.0x) | 1.00s (6.9x) | 0.35s (4.3x) |

**Key finding:** `@arrow_udf` is 1.6x faster than `@pandas_udf` on string workloads (0.61s vs 1.00s) because `pyarrow.compute.utf8_upper` operates on Arrow arrays directly, avoiding Pandas string overhead. For arithmetic and CDF workloads, both are equivalent (~5x).

**When to use `@arrow_udf` over `@pandas_udf`:** When your logic can be expressed using `pyarrow.compute` functions — especially for string operations where Pandas adds overhead. If your code already uses Pandas/NumPy, `@pandas_udf` is equally good.
