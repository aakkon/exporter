"""InfluxDB query adapter."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional

import pandas as pd
from influxdb_client import InfluxDBClient

from .config import InfluxDBConfig
from .models import TimeWindow


class InfluxDBRepository:
    """Adapter that queries InfluxDB and returns a cleaned DataFrame."""

    def __init__(self, config: InfluxDBConfig):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.InfluxDBRepository")

    def query(self, window: TimeWindow) -> Optional[pd.DataFrame]:
        self.logger.info(
            "Querying InfluxDB measurement=%s window=%s -> %s",
            self.config.measurement,
            window.start.isoformat(),
            window.end.isoformat(),
        )
        return self._execute_query(self._build_flux_query(window))

    def query_bench_status(self, window: TimeWindow) -> Optional[pd.DataFrame]:
        """
        Query bench_status as the primary source of truth for bench state.

        bench_status is a tag, so this query intentionally does not filter by
        Cycle/Objectives. It reads the measurement points available in the window
        and returns one latest status per testbench.

        Business rule: bench_status is global at testbench level.
        A bench cannot be STOPPED and MANUAL at the same time, so keeping
        the latest status per tb_name is intentional.
        """
        self.logger.info(
            "Querying bench status measurement=%s window=%s -> %s",
            self.config.measurement,
            window.start.isoformat(),
            window.end.isoformat(),
        )

        df = self._execute_query(self._build_bench_status_query(window))
        if df is None:
            return None

        required_columns = ["_time", "tb_name", "bench_status"]
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            self.logger.warning("Bench status query returned missing columns: %s", missing)
            return None

        return (
            df[required_columns]
            .dropna(subset=["tb_name", "bench_status"])
            .sort_values("_time")
            .drop_duplicates(subset=["tb_name"], keep="last")
            .copy()
        )

    def query_last_known_objectives(self, window: TimeWindow) -> Optional[pd.DataFrame]:
        """
        Query the last known cycle objective before the end of the current window.

        The objective is treated as a global value for the testbench + short test name,
        not as a per-sample value. This avoids losing the objective when it is not
        emitted during every hourly window.
        """
        self.logger.info(
            "Querying last known cycle objectives measurement=%s lookback_days=%s end=%s",
            self.config.measurement,
            self.config.objective_lookback_days,
            window.end.isoformat(),
        )

        df = self._execute_query(self._build_last_known_objectives_query(window))
        if df is None:
            return None

        if "_value" in df.columns and "FileData_Number_Cycles_Objective" not in df.columns:
            df = df.rename(columns={"_value": "FileData_Number_Cycles_Objective"})

        required_columns = ["tb_name", "test_name", "FileData_Number_Cycles_Objective"]
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            self.logger.warning("Last known objectives query returned missing columns: %s", missing)
            return None

        return (
            df[required_columns]
            .dropna(subset=required_columns)
            .drop_duplicates(subset=["tb_name", "test_name"], keep="last")
            .copy()
        )

    def _execute_query(self, flux_query: str) -> Optional[pd.DataFrame]:
        try:
            with InfluxDBClient(
                url=self.config.url,
                token=self.config.token,
                org=self.config.org,
            ) as client:
                result = client.query_api().query_data_frame(flux_query)
        except Exception:
            self.logger.exception("InfluxDB query failed")
            raise

        if isinstance(result, list):
            if not result:
                return None
            result = pd.concat(result, ignore_index=True)

        if result.empty:
            return None

        return self._prepare_dataframe(result)

    def _build_bench_status_query(self, window: TimeWindow) -> str:
        start_str, end_str = window.to_influx_range()

        return f"""
        from(bucket: "{self.config.bucket}")
        |> range(start: {start_str}, stop: {end_str})
        |> filter(fn: (r) => r["_measurement"] == "{self.config.measurement}")
        |> filter(fn: (r) => exists r["bench_status"])
        |> group(columns: ["tb_name"])
        |> last()
        |> keep(columns: ["_time", "tb_name", "bench_status"])
        """

    def _build_last_known_objectives_query(self, window: TimeWindow) -> str:
        objective_start = window.end - timedelta(days=self.config.objective_lookback_days)
        objective_window = TimeWindow(start=objective_start, end=window.end)
        start_str, end_str = objective_window.to_influx_range()

        return f"""
        from(bucket: "{self.config.bucket}")
        |> range(start: {start_str}, stop: {end_str})
        |> filter(fn: (r) => r["_measurement"] == "{self.config.measurement}")
        |> filter(fn: (r) => r["_field"] == "FileData_Number_Cycles_Objective")
        |> group(columns: ["tb_name", "test_name"])
        |> last()
        |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        """

    def _build_flux_query(self, window: TimeWindow) -> str:
        start_str, end_str = window.to_influx_range()
        field_filters = " or ".join(
            f'r["_field"] == "{field_name}"' for field_name in self.config.fields
        )

        return f'''
        from(bucket: "{self.config.bucket}")
        |> range(start: {start_str}, stop: {end_str})
        |> filter(fn: (r) => r["_measurement"] == "{self.config.measurement}")
        |> filter(fn: (r) => {field_filters})
        |> aggregateWindow(every: {self.config.aggregate_window}, fn: last, createEmpty: false)
        |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''

    @staticmethod
    def _prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        if "_time" in df.columns:
            df["_time"] = pd.to_datetime(df["_time"])

        return df.loc[:, ~df.columns.str.startswith(("result", "table"))].copy()
