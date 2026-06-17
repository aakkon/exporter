"""DataFrame-to-domain transformation logic."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from .models import SampleData


class TestbenchDataProcessor:
    """Maps external dataframe shape into the internal domain model."""

    MINIMUM_REQUIRED_COLUMNS = ["tb_name", "_time"]
    IDENTITY_COLUMNS = [
        "tb_name",
        "dut_sn",
        "dut_position",
        "dut_leg",
        "dut_name",
        "client",
        "hw_level",
        "hw_type",
        "test_name",
        "bench_status",
        "Cycle",
    ]
    ORIGIN_FILE_CANDIDATES = ["filename", "file_name", "originfile", "origin_file"]

    def __init__(self) -> None:
        self.logger = logging.getLogger(f"{__name__}.TestbenchDataProcessor")

    def process(self, df: pd.DataFrame) -> Dict[str, List[SampleData]]:
        self._validate_minimum_schema(df)
        aggregated = self._aggregate(df)
        return self._to_domain_samples(aggregated)

    def _validate_minimum_schema(self, df: pd.DataFrame) -> None:
        missing = [column for column in self.MINIMUM_REQUIRED_COLUMNS if column not in df.columns]
        if missing:
            raise ValueError(f"Missing minimum required columns: {missing}")

    def _aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        origin_column = next(
            (column for column in self.ORIGIN_FILE_CANDIDATES if column in df.columns),
            None,
        )
        if "file_raw_name" not in df.columns:
            df["file_raw_name"] = df[origin_column] if origin_column else None

        for column in self.IDENTITY_COLUMNS:
            if column not in df.columns:
                df[column] = None

        if "FileData_Number_Cycles_Objective" not in df.columns:
            df["FileData_Number_Cycles_Objective"] = None

        group_columns = self.IDENTITY_COLUMNS + ["file_raw_name"]

        return (
            df.groupby(group_columns, observed=True, dropna=False)
            .agg(
                t_min=("_time", "min"),
                t_max=("_time", "max"),
                cycle_objective=("FileData_Number_Cycles_Objective", "last"),
            )
            .reset_index()
        )

    def _to_domain_samples(self, aggregated: pd.DataFrame) -> Dict[str, List[SampleData]]:
        samples_by_testbench: Dict[str, List[SampleData]] = {}

        for row in aggregated.itertuples(index=False):
            sample = SampleData(
                tb_name=self._clean_text(row.tb_name) or "UNKNOWN_TESTBENCH",
                sample_sn=self._normalize_serial(row.dut_sn),
                sample_source_position=self._clean_text(row.dut_position),
                client=self._clean_text(row.client),
                sample_type=self._clean_text(row.dut_name),
                leg=self._clean_text(row.dut_leg),
                sample_dev_phase=self._clean_text(row.hw_level),
                sample_version=self._clean_text(row.hw_type),
                short_test_name=self._clean_text(row.test_name),
                bench_status=self._clean_text(row.bench_status),
                cycle=self._to_optional_int(row.Cycle),
                cycle_objective=self._to_optional_int(row.cycle_objective),
                t_interval_min=row.t_min if pd.notna(row.t_min) else None,
                t_interval_max=row.t_max if pd.notna(row.t_max) else None,
                file_raw_name=self._clean_text(row.file_raw_name),
            )
            samples_by_testbench.setdefault(sample.tb_name, []).append(sample)

        self.logger.info(
            "Processed %s testbenches and %s samples",
            len(samples_by_testbench),
            sum(len(samples) for samples in samples_by_testbench.values()),
        )
        return samples_by_testbench

    @staticmethod
    def _clean_text(value: Any) -> Optional[str]:
        if pd.isna(value) or value is None:
            return None
        text = str(value).strip()
        return text or None

    @staticmethod
    def _normalize_serial(value: Any) -> Optional[str]:
        text = TestbenchDataProcessor._clean_text(value)
        return text.upper() if text else None

    @staticmethod
    def _to_optional_int(value: Any) -> Optional[int]:
        if pd.isna(value) or value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
