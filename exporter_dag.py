"""
Hourly Data Exporter — Airflow DAG

Queries InfluxDB for metrics, builds export payloads,
and publishes them to S3 + SQS with gap detection and manifest tracking.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Tuple

# ---- Airflow path setup (must run before exporter imports) ----
AIRFLOW_PATHS = [
    os.path.dirname(os.path.abspath(__file__)),
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "/usr/local/airflow/dags",
    "/opt/airflow",
]
for _path in AIRFLOW_PATHS:
    if os.path.exists(_path) and _path not in sys.path:
        sys.path.insert(0, _path)

from airflow import DAG
from airflow.operators.python import PythonOperator

from exporter.config import ApplicationConfig, DAG_ID, InfluxDBConfig
from exporter.manifest import process_with_gap_detection
from exporter.processor import TestbenchDataProcessor
from exporter.repository import InfluxDBRepository
from exporter.service import ExportService


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main() -> Tuple[int, int]:
    """Return (exit_code, sent_count)."""
    app_config = ApplicationConfig.from_environment()
    setup_logging(app_config.log_level)

    logger = logging.getLogger(__name__)
    logger.info("Starting %s %s", app_config.app_name, app_config.schema_version)

    try:
        influx_config = InfluxDBConfig.from_secrets_manager()
        service = ExportService(
            repository=InfluxDBRepository(influx_config),
            processor=TestbenchDataProcessor(),
            app_config=app_config,
        )
        sent_count, _ = process_with_gap_detection(service)
        logger.info("Export successful: %s payloads published", sent_count)
        return 0, sent_count
    except Exception:
        logger.exception("Export failed")
        return 1, 0


def generate_and_publish_task(**context) -> int:
    logger = logging.getLogger(__name__)
    logger.info("Starting export and publish task")
    exit_code, sent_count = main()
    if exit_code != 0:
        raise RuntimeError(f"Exporter finished with exit code {exit_code}")
    logger.info("Task finished with %s payloads published", sent_count)
    return sent_count


# ---- DAG definition ----

default_args = {
    "owner": "exporter",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    description="Hourly data export to S3 + SQS",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["exporter", "influxdb", "s3", "sqs"],
    doc_md=__doc__,
) as dag:
    task_generate_and_publish = PythonOperator(
        task_id="generate_and_publish",
        python_callable=generate_and_publish_task,
    )


if __name__ == "__main__":
    exit_code, _ = main()
    sys.exit(exit_code)
