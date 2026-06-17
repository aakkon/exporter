"""Export orchestration: query -> process -> payloads."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd

from .config import ApplicationConfig
from .models import ExportRecord, TimeWindow
from .processor import TestbenchDataProcessor
from .repository import InfluxDBRepository


class ExportService:
    """Orchestrates InfluxDB queries, data processing, and payload generation."""

    def __init__(
        self,
        repository: InfluxDBRepository,
        processor: TestbenchDataProcessor,
        app_config: ApplicationConfig,
    ) -> None:
        self.repository = repository
        self.processor = processor
        self.app_config = app_config
        self.logger = logging.getLogger(f"{__name__}.ExportService")

    def _merge_status_with_metrics(
        self,
        status_df: pd.DataFrame,
        metrics_df: Optional[pd.DataFrame],
    ) -> pd.DataFrame:
        """
        Use bench_status rows as the base dataset and attach metrics only when
        they exist in the same window.

        This prevents losing bench states such as STOPPED, DISCONNECTED or MANUAL
        when Cycle/Objectives are not emitted.
        """
        if metrics_df is None or metrics_df.empty:
            return status_df.copy()

        merged = status_df.merge(
            metrics_df,
            on=["tb_name"],
            how="left",
            suffixes=("", "_metrics"),
        )

        if "_time_metrics" in merged.columns:
            merged["_time"] = merged["_time_metrics"].combine_first(merged["_time"])
            merged = merged.drop(columns=["_time_metrics"])

        if "bench_status_metrics" in merged.columns:
            merged = merged.drop(columns=["bench_status_metrics"])

        return merged

    def _enrich_cycle_objectives(
        self,
        df: pd.DataFrame,
        objectives_df: Optional[pd.DataFrame],
    ) -> pd.DataFrame:
        """
        Fill missing FileData_Number_Cycles_Objective values using the last known
        objective per tb_name + test_name.

        Existing objective values from the current window always win.
        """
        if objectives_df is None or objectives_df.empty:
            return df

        df = df.copy()

        required_columns = {"tb_name", "test_name"}
        if not required_columns.issubset(df.columns):
            self.logger.warning(
                "Cannot enrich cycle objectives. Missing columns in main DataFrame: %s",
                sorted(required_columns - set(df.columns)),
            )
            return df

        if "FileData_Number_Cycles_Objective" not in df.columns:
            df["FileData_Number_Cycles_Objective"] = None

        lookup = (
            objectives_df
            .dropna(subset=["tb_name", "test_name", "FileData_Number_Cycles_Objective"])
            .drop_duplicates(subset=["tb_name", "test_name"], keep="last")
            .set_index(["tb_name", "test_name"])["FileData_Number_Cycles_Objective"]
        )

        before_missing = df["FileData_Number_Cycles_Objective"].isna().sum()

        objective_matches = pd.MultiIndex.from_frame(df[["tb_name", "test_name"]]).map(lookup)
        df["FileData_Number_Cycles_Objective"] = (
            df["FileData_Number_Cycles_Objective"]
            .combine_first(pd.Series(objective_matches, index=df.index))
        )

        after_missing = df["FileData_Number_Cycles_Objective"].isna().sum()
        filled_count = before_missing - after_missing

        if filled_count:
            self.logger.info(
                "Enriched %s rows with last known cycle objectives",
                filled_count,
            )

        return df

    def execute(self, window: TimeWindow) -> List[dict]:
        status_df = self.repository.query_bench_status(window)
        if status_df is None or status_df.empty:
            self.logger.warning("No bench status found for export window")
            return []

        metrics_df = self.repository.query(window)
        if metrics_df is None or metrics_df.empty:
            self.logger.info(
                "No Cycle/Objectives found for export window. Exporting bench status only."
            )
        else:
            objectives_df = self.repository.query_last_known_objectives(window)
            metrics_df = self._enrich_cycle_objectives(metrics_df, objectives_df)

            metrics_df = metrics_df.drop_duplicates(subset=["tb_name"], keep="last")
            if "_time" in metrics_df.columns:
                metrics_df = metrics_df.sort_values("_time")

        df = self._merge_status_with_metrics(status_df, metrics_df)

        samples_by_testbench = self.processor.process(df)
        generation_timestamp = datetime.now(timezone.utc)
        payloads: List[dict] = []

        for _, samples in samples_by_testbench.items():
            for sample in samples:
                if not sample.has_exportable_metrics():
                    continue

                record = ExportRecord(
                    app=self.app_config.app_name,
                    schema_version=self.app_config.schema_version,
                    timestamp=window.start,
                    generation_timestamp=generation_timestamp,
                    sample=sample,
                )
                payloads.append(record.to_dict())

        self.logger.info("Export completed with %s payloads", len(payloads))
        return payloads
