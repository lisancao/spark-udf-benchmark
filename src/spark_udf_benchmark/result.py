"""Benchmark result dataclass."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict


@dataclass
class UdfBenchmarkResult:
    """Result of a single UDF benchmark measurement."""

    udf_type: str           # e.g. "builtin", "sql_udf", "arrow_udf", etc.
    workload: str           # "arithmetic", "string", "cdf"
    row_count: int
    duration_seconds: float
    rows_per_second: float
    relative_overhead: float  # vs built-in baseline (1.0 = same speed)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    run_id: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "udf_type": self.udf_type,
            "workload": self.workload,
            "row_count": self.row_count,
            "duration_seconds": round(self.duration_seconds, 6),
            "rows_per_second": round(self.rows_per_second, 2),
            "relative_overhead": round(self.relative_overhead, 2),
            "timestamp": self.timestamp.isoformat(),
            "run_id": self.run_id,
        }
