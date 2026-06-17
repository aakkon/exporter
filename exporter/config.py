"""Configuration: secrets retrieval and application settings."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Optional, Tuple

from botocore.exceptions import ClientError

from lib.utils.aws_secrets_util import get_boto3_session

DAG_ID = "exporter"


@dataclass(frozen=True)
class InfluxDBConfig:
    url: str
    token: str
    org: str
    bucket: str
    measurement: str
    fields: Tuple[str, ...] = (
        "FileData_Number_Cycles_Objective",
        "Cycle",
    )
    aggregate_window: str = "30s"
    objective_lookback_days: int = 30

    @classmethod
    def from_secrets_manager(
        cls,
        secret_name: Optional[str] = None,
        region: Optional[str] = None,
    ) -> InfluxDBConfig:
        secret_name = secret_name or os.environ["INFLUXDB_SECRET_NAME"]
        region = region or os.getenv("AWS_REGION", "eu-central-1")
        logger = logging.getLogger(__name__)

        try:
            client = get_boto3_session().client("secretsmanager", region_name=region)
            response = client.get_secret_value(SecretId=secret_name)
            secret_data = json.loads(response["SecretString"])
        except ClientError:
            logger.exception("Error retrieving secret '%s'", secret_name)
            raise
        except json.JSONDecodeError:
            logger.exception("Secret '%s' is not valid JSON", secret_name)
            raise

        required_keys = ["url", "token", "org", "bucket", "measurement"]
        missing_keys = [key for key in required_keys if not secret_data.get(key)]
        if missing_keys:
            raise ValueError(f"Missing required keys in secret: {missing_keys}")

        return cls(
            url=secret_data["url"],
            token=secret_data["token"],
            org=secret_data["org"],
            bucket=secret_data["bucket"],
            measurement=secret_data["measurement"],
        )


@dataclass(frozen=True)
class ApplicationConfig:
    app_name: str = "Real_Time_Summary"
    schema_version: str = "v3"
    log_level: str = "INFO"

    @classmethod
    def from_environment(cls) -> ApplicationConfig:
        log_level = os.getenv("LOG_LEVEL", "INFO")
        return cls(log_level=log_level)
