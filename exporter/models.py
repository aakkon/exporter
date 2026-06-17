"""Domain models for the export pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, Tuple


@dataclass(frozen=True)
class TimeWindow:
    """Immutable UTC time range used for data queries."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise ValueError("start must be earlier than end")

    def to_influx_range(self) -> Tuple[str, str]:
        return (
            self.start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            self.end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )


@dataclass(frozen=True)
class SampleData:
    """Canonical internal sample model with standardized field names."""

    tb_name: str
    sample_sn: Optional[str]
    sample_source_position: Optional[str]
    client: Optional[str]
    sample_type: Optional[str]
    leg: Optional[str]
    sample_dev_phase: Optional[str]
    sample_version: Optional[str]
    short_test_name: Optional[str]
    bench_status: Optional[str]
    cycle: Optional[int]
    cycle_objective: Optional[int]
    t_interval_min: Optional[datetime]
    t_interval_max: Optional[datetime]
    file_raw_name: Optional[str] = None

    def has_exportable_metrics(self) -> bool:
        return any((
            self.cycle is not None,
            self.cycle_objective is not None,
            self.bench_status is not None,
        ))

    def to_export_payload(self) -> Dict[str, Any]:
        return {
            "sample_sn": self.sample_sn,
            "sample_source_position": self.sample_source_position,
            "client": self.client,
            "sample_type": self.sample_type,
            "leg": self.leg,
            "sample_dev_phase": self.sample_dev_phase,
            "sample_version": self.sample_version,
            "short_test_name": self.short_test_name,
            "bench_status": self.bench_status,
            "cycle": self.cycle,
            "cycles_objective": self.cycle_objective,
            "t_interval_min": self.t_interval_min.isoformat() if self.t_interval_min else None,
            "t_interval_max": self.t_interval_max.isoformat() if self.t_interval_max else None,
            "timezone": "UTC",
            "test_name": None,
            "metadata_file_name": None,
            "file_raw_name": self.file_raw_name,
            "project": None,
            "client_ref": None,
            "wo": None,
            "coce": None,
            "comment": None,
        }


@dataclass(frozen=True)
class ExportRecord:
    """Final JSON export structure."""

    app: str
    schema_version: str
    timestamp: datetime
    generation_timestamp: datetime
    sample: SampleData

    def to_dict(self) -> Dict[str, Any]:
        base = {
            "app": self.app,
            "source": self.sample.tb_name,
            "timestamp": self.timestamp.isoformat(),
            "schema_version": self.schema_version,
            "generation_timestamp": self.generation_timestamp.isoformat(),
            "timezone": "UTC",
        }
        base.update(self.sample.to_export_payload())
        return base
