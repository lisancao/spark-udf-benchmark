"""CLI entry point: python -m spark_udf_benchmark [--rows N]"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession

from .benchmark import UdfPipelineBenchmark


def main() -> int:
    parser = argparse.ArgumentParser(description="UDF Performance Benchmark")
    parser.add_argument("--rows", "-r", type=int, default=100_000, help="Row count (default: 100000)")
    parser.add_argument("--warmup", type=int, default=1, help="Warmup runs (default: 1)")
    parser.add_argument("--runs", type=int, default=3, help="Benchmark runs (default: 3)")
    parser.add_argument("--output-dir", default="results", help="Output directory for JSON")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output on failure")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("UdfBenchmark")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )

    benchmark = UdfPipelineBenchmark(spark, warmup_runs=args.warmup, benchmark_runs=args.runs)

    try:
        results = benchmark.run_comparison(row_count=args.rows)

        os.makedirs(args.output_dir, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(args.output_dir, f"udf_{benchmark.run_id}_{timestamp}.json")

        data = {
            "metadata": {
                "run_id": benchmark.run_id,
                "benchmark_type": "udf_pipeline",
                "row_count": args.rows,
                "warmup_runs": args.warmup,
                "benchmark_runs": args.runs,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
            "results": {k: v.to_dict() for k, v in results.items()},
        }

        with open(filepath, "w") as fh:
            json.dump(data, fh, indent=2)

        print(f"\nResults saved to: {filepath}")
        return 0

    except Exception as e:
        print(f"\nBenchmark failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

    finally:
        benchmark.cleanup()
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
