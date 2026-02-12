.PHONY: test bench-100k bench-1m bench-10m bench-50m bench-all docker-build docker-test docker-bench report clean

PYTHON ?= python3
SRC    := src/spark_udf_benchmark
ROWS_100K := 100000
ROWS_1M   := 1000000
ROWS_10M  := 10000000
ROWS_50M  := 50000000

# ── Local (requires Spark + Java installed) ──────────────────────

test:
	PYSPARK_PYTHON=$(PYTHON) PYTHONPATH=src $(PYTHON) -m pytest tests/ -v

bench-100k:
	PYSPARK_PYTHON=$(PYTHON) PYTHONPATH=src $(PYTHON) -m spark_udf_benchmark --rows $(ROWS_100K)

bench-1m:
	PYSPARK_PYTHON=$(PYTHON) PYTHONPATH=src $(PYTHON) -m spark_udf_benchmark --rows $(ROWS_1M)

bench-10m:
	PYSPARK_PYTHON=$(PYTHON) PYTHONPATH=src $(PYTHON) -m spark_udf_benchmark --rows $(ROWS_10M)

bench-50m:
	PYSPARK_PYTHON=$(PYTHON) PYTHONPATH=src $(PYTHON) -m spark_udf_benchmark --rows $(ROWS_50M)

bench-all: bench-100k bench-1m bench-10m bench-50m

# ── Docker (fully reproducible, no local Spark needed) ───────────

docker-build:
	docker compose build

docker-test:
	docker compose run --rm test

docker-bench:
	docker compose run --rm benchmark --rows $(ROWS_50M)

# ── Utilities ────────────────────────────────────────────────────

report:
	@echo "Results in results/ directory:"
	@ls -lh results/*.json 2>/dev/null || echo "  (no results yet — run a benchmark first)"

clean:
	rm -rf __pycache__ .pytest_cache src/*.egg-info derby.log metastore_db spark-warehouse
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
