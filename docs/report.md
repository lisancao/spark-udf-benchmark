# UDF Performance Benchmark Report

**Date:** 2026-02-12
**Spark:** 4.1.0 (Spark Connect, `remote("local[*]")`)
**Python:** 3.12.3
**Driver memory:** 32GB

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
| 3 | `@arrow_udf` | `@arrow_udf` decorator (SPARK-53014, `ArrowEvalPython`) |
| 4 | `@udf(useArrow=True)` | `@udf(useArrow=True)` (SPARK-43082, `ArrowEvalPython`) |
| 5 | `@pandas_udf` | `@pandas_udf` decorator |
| 6 | `@udf` + arrow config | `@udf` with `spark.sql.execution.arrow.pyspark.enabled=true` |
| 7 | `@udf` (default) | `@udf` with arrow disabled (`...enabled=false`) |

> **Note:** Scala/Java UDFs excluded (require compiled JAR). All results at 50M rows with 32GB driver memory.

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
| `@udf(useArrow=True)` | 0.2115 | 472,736 | 2.9x |
| `@pandas_udf` | 0.1812 | 551,735 | 2.5x |
| `@udf` + arrow config | 0.1646 | 607,640 | 2.2x |
| `@udf` (default) | 0.1613 | 619,833 | 2.2x |

### String (`upper(x) + '_SUFFIX'`)

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.0308 | 3,246,042 | 1.0x (baseline) |
| SQL UDFs | 0.0330 | 3,030,525 | 1.1x |
| `@udf(useArrow=True)` | 0.1716 | 582,711 | 5.6x |
| `@pandas_udf` | 0.1612 | 620,269 | 5.2x |
| `@udf` + arrow config | 0.1714 | 583,346 | 5.6x |
| `@udf` (default) | 0.1673 | 597,838 | 5.4x |

### Normal CDF

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.0503 | 1,988,390 | 1.0x (baseline) |
| `@udf(useArrow=True)` | 0.1644 | 608,398 | 3.3x |
| `@pandas_udf` | 0.1582 | 632,296 | 3.1x |
| `@udf` + arrow config | 0.1543 | 648,134 | 3.1x |
| `@udf` (default) | 0.1667 | 599,727 | 3.3x |

### Observations (100K)

- All Python UDFs cluster at 2-6x -- fixed overhead (worker startup/teardown) dominates at small scale
- `@udf(useArrow=True)` is indistinguishable from `@pandas_udf`/`@udf` (default) at this scale
- SQL UDFs match built-in speed

---

## Results: 1M Rows

### Arithmetic (`x * 2 + 1`)

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.0772 | 12,949,761 | 1.0x (baseline) |
| SQL UDFs | 0.0490 | 20,393,149 | 0.6x |
| `@udf(useArrow=True)` | 0.2480 | 4,032,086 | 3.2x |
| `@pandas_udf` | 0.2000 | 4,999,868 | 2.6x |
| `@udf` + arrow config | 0.2928 | 3,415,703 | 3.8x |
| `@udf` (default) | 0.2577 | 3,880,465 | 3.3x |

### String (`upper(x) + '_SUFFIX'`)

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.0470 | 21,285,725 | 1.0x (baseline) |
| SQL UDFs | 0.0450 | 22,199,580 | 1.0x |
| `@udf(useArrow=True)` | 0.2258 | 4,428,849 | 4.8x |
| `@pandas_udf` | 0.2046 | 4,887,639 | 4.4x |
| `@udf` + arrow config | 0.3047 | 3,281,531 | 6.5x |
| `@udf` (default) | 0.2882 | 3,469,529 | 6.1x |

### Normal CDF

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.0506 | 19,781,281 | 1.0x (baseline) |
| `@udf(useArrow=True)` | 0.2445 | 4,090,003 | 4.8x |
| `@pandas_udf` | 0.1688 | 5,923,274 | 3.3x |
| `@udf` + arrow config | 0.2710 | 3,689,543 | 5.4x |
| `@udf` (default) | 0.2627 | 3,805,925 | 5.2x |

### Observations (1M)

- `@pandas_udf` starts to separate from row-at-a-time UDFs (2.6x vs 3.3-3.8x arithmetic)
- `@udf(useArrow=True)` tracks closer to `@pandas_udf` than to `@udf` (default) -- batched transport helps
- CDF shows `@pandas_udf` at 3.3x vs `@udf(useArrow=True)` at 4.8x -- `@pandas_udf` benefits from NumPy vectorization

---

## Results: 10M Rows

### Arithmetic (`x * 2 + 1`)

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.0740 | 135,069,002 | 1.0x (baseline) |
| SQL UDFs | 0.0618 | 161,731,252 | 0.8x |
| `@udf(useArrow=True)` | 0.5682 | 17,598,513 | 7.7x |
| `@pandas_udf` | 0.2994 | 33,399,464 | 4.0x |
| `@udf` + arrow config | 1.1211 | 8,919,424 | 15.1x |
| `@udf` (default) | 1.0916 | 9,161,047 | 14.7x |

### String (`upper(x) + '_SUFFIX'`)

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.1451 | 68,938,897 | 1.0x (baseline) |
| SQL UDFs | 0.1298 | 77,069,026 | 0.9x |
| `@udf(useArrow=True)` | 0.7493 | 13,346,481 | 5.2x |
| `@pandas_udf` | 0.6376 | 15,684,459 | 4.4x |
| `@udf` + arrow config | 1.3171 | 7,592,167 | 9.1x |
| `@udf` (default) | 1.3048 | 7,664,237 | 9.0x |

### Normal CDF

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.0805 | 124,151,057 | 1.0x (baseline) |
| `@udf(useArrow=True)` | 0.6657 | 15,022,790 | 8.3x |
| `@pandas_udf` | 0.2836 | 35,264,091 | 3.5x |
| `@udf` + arrow config | 1.2705 | 7,870,753 | 15.8x |
| `@udf` (default) | 1.2346 | 8,099,508 | 15.3x |

### Observations (10M)

- **Clear 3-tier separation emerges**: `@pandas_udf` (~4x) < `@udf(useArrow=True)` (~7-8x) < row-at-a-time (~15x)
- `@udf(useArrow=True)` is ~2x faster than row-at-a-time UDFs thanks to batched transport
- Built-in throughput hits 135M rows/s arithmetic -- Tungsten codegen at full speed

---

## Results: 50M Rows

### Arithmetic (`x * 2 + 1`)

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.11 | 454,545,455 | 1.0x (baseline) |
| SQL UDFs | 0.09 | 555,555,556 | 0.9x |
| `@arrow_udf` | 0.73 | 68,493,151 | 6.8x |
| `@pandas_udf` | 0.79 | 63,291,139 | 7.4x |
| `@udf(useArrow=True)` | 2.23 | 22,421,525 | 20.8x |
| `@udf` + arrow config | 5.24 | 9,541,985 | 48.9x |
| `@udf` (default) | 5.20 | 9,615,385 | 48.5x |

### String (`upper(x) + '_SUFFIX'`)

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.49 | 102,040,816 | 1.0x (baseline) |
| SQL UDFs | 0.48 | 104,166,667 | 1.0x |
| `@arrow_udf` | 1.55 | 32,258,065 | 3.2x |
| `@pandas_udf` | 2.51 | 19,920,319 | 5.2x |
| `@udf(useArrow=True)` | 4.43 | 11,286,682 | 9.1x |
| `@udf` + arrow config | 6.23 | 8,025,682 | 12.8x |
| `@udf` (default) | 6.22 | 8,038,585 | 12.8x |

### Normal CDF

| UDF Type | Duration (s) | Rows/s | Overhead |
|----------|-------------|--------|----------|
| Built-in functions | 0.18 | 277,777,778 | 1.0x (baseline) |
| `@arrow_udf` | 0.76 | 65,789,474 | 4.3x |
| `@pandas_udf` | 0.82 | 60,975,610 | 4.7x |
| `@udf(useArrow=True)` | 2.94 | 17,006,803 | 16.7x |
| `@udf` + arrow config | 6.20 | 8,064,516 | 35.2x |
| `@udf` (default) | 6.18 | 8,090,615 | 35.1x |

### Observations (50M)

- **Row-at-a-time @udf hits ~49x** -- confirmed with 32GB driver memory and zero cache spill
- **`@arrow_udf` is the fastest Python UDF** -- faster than `@pandas_udf` across all workloads (0.73s vs 0.79s arithmetic, 1.55s vs 2.51s string, 0.76s vs 0.82s CDF)
- **`@arrow_udf` is especially strong on strings: 1.6x faster than `@pandas_udf`** (1.55s vs 2.51s)
- **`@udf(useArrow=True)` sits at ~9-21x** -- batched transport makes it 2-2.3x faster than row-at-a-time
- **`@pandas_udf` holds at ~5-7x** -- batch vectorization continues to amortize overhead
- **`@udf` + arrow config still has zero effect** (48.9x vs 48.5x)

---

## `@arrow_udf` Results: 50M Rows

The `@arrow_udf` decorator (SPARK-53014) was verified working in Spark 4.1.0 in both classic and Spark Connect modes. Confirmed results at 50M rows with 32GB driver memory (median of 3 runs):

### Arithmetic (`x * 2 + 1`)

| UDF Type | Duration (s) | Overhead | vs `@pandas_udf` |
|----------|-------------|----------|-------------------|
| Built-in functions | 0.11 | 1.0x (baseline) | — |
| `@arrow_udf` | 0.73 | 6.8x | **1.1x faster** |
| `@pandas_udf` | 0.79 | 7.4x | baseline |
| `@udf(useArrow=True)` | 2.23 | 20.8x | 2.8x slower |

### String (`upper(x) + '_SUFFIX'`)

| UDF Type | Duration (s) | Overhead | vs `@pandas_udf` |
|----------|-------------|----------|-------------------|
| Built-in functions | 0.49 | 1.0x (baseline) | — |
| `@arrow_udf` | 1.55 | 3.2x | **1.6x faster** |
| `@pandas_udf` | 2.51 | 5.2x | baseline |
| `@udf(useArrow=True)` | 4.43 | 9.1x | 1.8x slower |

### Normal CDF

| UDF Type | Duration (s) | Overhead | vs `@pandas_udf` |
|----------|-------------|----------|-------------------|
| Built-in functions | 0.18 | 1.0x (baseline) | — |
| `@arrow_udf` | 0.76 | 4.3x | **1.1x faster** |
| `@pandas_udf` | 0.82 | 4.7x | baseline |
| `@udf(useArrow=True)` | 2.94 | 16.7x | 3.6x slower |

### Observations (`@arrow_udf` 50M)

- **`@arrow_udf` is FASTER than `@pandas_udf` across ALL workloads** — 0.73s vs 0.79s arithmetic, 1.55s vs 2.51s string, 0.76s vs 0.82s CDF
- **`@arrow_udf` is especially strong on strings: 1.6x faster than `@pandas_udf`** — `pyarrow.compute.utf8_upper` operates on Arrow arrays directly, avoiding the Arrow-to-Pandas-to-Arrow roundtrip that `@pandas_udf` requires for string processing
- **Both vectorized options are ~3x faster than `@udf(useArrow=True)`** — confirming the execution model (batch vectorization vs per-row scalars) is the dominant performance lever, not just the transport format
- **`@arrow_udf` uses `ArrowEvalPython`** with `SQL_SCALAR_ARROW_UDF` (eval type 250), the same physical operator as `@pandas_udf` and `@udf(useArrow=True)`, but receives `pyarrow.Array` objects instead of scalars or Pandas Series

---

## Overhead Trend Across All Scales

### Arithmetic Workload (trivial)

| UDF Type | 100K | 1M | 10M | 50M | Trend |
|----------|------|-----|------|------|-------|
| SQL UDFs | 0.7x | 0.6x | 0.8x | 0.9x | **Flat ~1x** |
| `@arrow_udf` | — | — | — | 6.8x | **~3-7x** |
| `@pandas_udf` | 2.5x | 2.6x | 4.0x | 7.4x | **Slow growth** |
| `@udf(useArrow=True)` | 2.9x | 3.2x | 7.7x | **20.8x** | **Linear growth** |
| `@udf` + arrow config | 2.2x | 3.8x | 15.1x | **48.9x** | **Superlinear growth** |
| `@udf` (default) | 2.2x | 3.3x | 14.7x | **48.5x** | **Superlinear growth** |

### CDF Workload (most complex)

| UDF Type | 100K | 1M | 10M | 50M | Trend |
|----------|------|-----|------|------|-------|
| `@arrow_udf` | — | — | — | 4.3x | **~3-5x** |
| `@pandas_udf` | 3.1x | 3.3x | 3.5x | 4.7x | **Flat ~3-5x** |
| `@udf(useArrow=True)` | 3.3x | 4.8x | 8.3x | **16.7x** | **Linear growth** |
| `@udf` + arrow config | 3.1x | 5.4x | 15.8x | **35.2x** | **Superlinear growth** |
| `@udf` (default) | 3.3x | 5.2x | 15.3x | **35.1x** | **Superlinear growth** |

---

## Disambiguating "Arrow UDFs" in Spark 4.1

Spark 4.1 introduces three different mechanisms that all involve "Arrow" and "UDF" in their names. They are not interchangeable:

| Mechanism | Decorator / Config | Python receives | Physical operator | Overhead |
|-----------|-------------------|-----------------|-------------------|----------|
| `@udf` + arrow config | `@udf` + `arrow.pyspark.enabled=true` | Scalars (per row) | `BatchEvalPython` | ~49x |
| `@udf(useArrow=True)` | `@udf(useArrow=True)` (SPARK-43082) | Scalars (per row) | `ArrowEvalPython` | ~9-21x |
| Arrow-native UDF | `@arrow_udf` (SPARK-53014) | `pyarrow.Array` (batch) | `ArrowEvalPython` | ~3-7x (confirmed at 50M) |

The critical distinction is the **physical operator**: `BatchEvalPython` processes rows one at a time regardless of wire format (hence ~49x for both `@udf` (default) and `@udf` + arrow config). `ArrowEvalPython` uses batched Arrow transport, but what your function *receives* still varies — `@udf(useArrow=True)` unpacks batches back to scalars for per-row calls, while `@arrow_udf` and `@pandas_udf` pass entire arrays/Series for vectorized processing.

We confirmed that `@arrow_udf` works in Spark 4.1.0 (both classic and Connect modes) at 50M rows. It is the fastest Python UDF option, beating `@pandas_udf` across all workloads and especially strong on strings (1.6x faster). See the [results](#arrow_udf-results-50m-rows) above.

---

## Transport vs Execution: Why Arrow Config Doesn't Help @udf

UDF performance depends on two independent axes: **transport format** (how data moves between JVM and Python) and **execution model** (how Python processes each row).

|  | Pickle Transport | Arrow Transport |
|---|---|---|
| **Row-at-a-time** | `@udf` (default) **~49x** | `@udf` + arrow config **~49x** |
| **Batch transport, scalar exec** | — | `@udf(useArrow=True)` **~9-21x** |
| **Vectorized batch** | — | `@pandas_udf` **~5-7x**, `@arrow_udf` **~3-7x** |

The data confirms this model:

- **Transport change alone (pickle → arrow, same execution):** 48.5x → 48.9x = **no improvement**. The `arrow.pyspark.enabled` config changes serialization format but keeps `BatchEvalPython`. The function still executes once per row. Per-row Python function call overhead dominates.
- **Physical operator change (BatchEvalPython → ArrowEvalPython, scalar exec):** 48.5x → 20.8x = **2.3x improvement**. `@udf(useArrow=True)` switches to `ArrowEvalPython` which batches Arrow transport between JVM and Python, reducing serde overhead. But the function still receives scalars and executes per-row.
- **Execution model change (scalar → vectorized, same operator):** 20.8x → 3-7x = **3-7x further improvement**. `@pandas_udf` operates on entire Pandas Series using NumPy, amortizing all per-row overhead. `@arrow_udf` does the same with `pyarrow.compute` and is the fastest option.

**Bottom line:** The performance hierarchy is: physical operator > execution model > wire format. Changing the wire format alone (`arrow.pyspark.enabled`) does nothing. Changing the operator (`useArrow=True`) gives 2.3x. Changing both operator and execution model (`@pandas_udf` / `@arrow_udf`) gives 7-16x.

---

## Key Takeaways

### 1. If you need Python, use `@arrow_udf` first

`@arrow_udf` is the fastest Python UDF option across all workloads. At 50M rows: **0.73s** arithmetic vs **5.20s** for default `@udf` -- a 7.1x wall-clock saving. It is faster than `@pandas_udf` across all workloads (0.73s vs 0.79s arithmetic, 1.55s vs 2.51s string, 0.76s vs 0.82s CDF) and especially strong on strings (1.6x faster, because `pyarrow.compute` avoids Pandas string overhead).

**When to use `@arrow_udf`:** Your Python logic can be expressed with `pyarrow.compute` functions -- the fastest Python option, especially for strings.
**When to use `@pandas_udf`:** Your Python logic uses Pandas/NumPy APIs (most numeric and statistical operations). Still excellent at ~5-7x overhead.

### 2. `@udf(useArrow=True)` is a solid middle ground

Sits at **~9-21x** depending on workload. At 50M rows: **2.23s** arithmetic vs **5.20s** for default `@udf` -- a 2.3x saving. Uses `ArrowEvalPython` for batched transport but still executes Python per-row.

**When to use:** You need per-row Python logic but can't express it with Pandas/NumPy/PyArrow APIs. Provides meaningful improvement over default `@udf` with minimal code changes (just add `useArrow=True`).

### 3. Arrow transport config has zero effect on `@udf`

`spark.sql.execution.arrow.pyspark.enabled=true` gives **48.9x vs 48.5x** (pickle) -- no measurable benefit. This config only changes the serialization format. The bottleneck is per-row Python function calls (~9-10M rows/s ceiling), not how bytes are serialized. See the Transport vs Execution section above.

### 4. SQL UDFs are free -- use them first

**0.6-1.0x** overhead across all scales. They compile to native Catalyst expressions, same execution plan as built-in functions. Any logic expressible in SQL should use SQL UDFs before reaching for Python.

### 5. Default `@udf` hits ~49x at 50M rows -- avoid at scale

Both `@udf` (default) and `@udf` + arrow config converge to **~49x overhead** at scale (5.20s and 5.24s for 50M rows arithmetic). At 50M rows, that's **5.2 seconds** vs **0.11 seconds** for built-in functions. If your workload processes tens of millions of rows, the default `@udf` is the worst Python option available.

### 6. `@arrow_udf` (SPARK-53014) is the fastest Python UDF option

The `@arrow_udf` decorator (SPARK-53014) is a genuinely new execution model where your function receives `pyarrow.Array` objects and can use `pyarrow.compute` for vectorized operations — no Pandas conversion overhead.

Confirmed at 50M rows with 32GB driver memory:

| UDF Type | Arithmetic | String | CDF |
|----------|-----------|--------|-----|
| `@arrow_udf` | 0.73s (6.8x) | 1.55s (3.2x) | 0.76s (4.3x) |
| `@pandas_udf` | 0.79s (7.4x) | 2.51s (5.2x) | 0.82s (4.7x) |

**Key finding:** `@arrow_udf` is faster than `@pandas_udf` across ALL workloads. It is especially strong on strings (1.6x faster: 1.55s vs 2.51s) because `pyarrow.compute.utf8_upper` operates on Arrow arrays directly, avoiding Pandas string overhead.

**When to use `@arrow_udf` over `@pandas_udf`:** When your logic can be expressed using `pyarrow.compute` functions — it is the fastest Python UDF option, especially for string operations. If your code already uses Pandas/NumPy, `@pandas_udf` is still excellent (~5-7x).
