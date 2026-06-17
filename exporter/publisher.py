"""SQS publishing with S3 claim-check pattern.

Payloads are uploaded to S3 first. SQS receives a lightweight
notification with the S3 reference, not the data itself.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import List

from botocore.exceptions import ClientError

from lib.utils.aws_secrets_util import get_boto3_session

from .config import DAG_ID


def _get_sqs_queue_url() -> str:
    return os.environ["SQS_QUEUE_URL"]


def _get_export_s3_bucket() -> str:
    return os.environ["EXPORT_S3_BUCKET"]


def _get_export_s3_prefix() -> str:
    return os.getenv("EXPORT_S3_PREFIX", "exports/")


MAX_DEDUP_ID_LENGTH = 128
SQS_THROTTLING_ERROR_CODES = {
    "Throttling",
    "ThrottlingException",
    "RequestThrottled",
    "TooManyRequestsException",
    "ServiceUnavailable",
    "RequestLimitExceeded",
    "ProvisionedThroughputExceededException",
}


def _get_sqs_client():
    return get_boto3_session().client("sqs")


def _get_s3_client():
    return get_boto3_session().client("s3")


def create_deduplication_id(
    timestamp: datetime,
    tb_name: str,
    dut_sn: str,
    position: str = "",
) -> str:
    timestamp_part = timestamp.strftime("%Y%m%d_%H%M%S_%f")[:21]
    tb_part = (tb_name or "UNKNOWN")[:20]
    pos_part = (position or "")[:10]
    sn_part = (dut_sn or "UNKNOWN")[:40]
    parts = [p for p in [timestamp_part, tb_part, pos_part, sn_part] if p]
    dedup_id = "_".join(parts)
    return dedup_id[:MAX_DEDUP_ID_LENGTH].replace(" ", "_")


def _build_s3_key(payload: dict, deduplication_id: str) -> str:
    timestamp = datetime.fromisoformat(payload["timestamp"])
    date_part = timestamp.strftime("%Y-%m-%d")
    prefix = _get_export_s3_prefix()
    return f"{prefix}{date_part}/{deduplication_id}.json"


def _upload_payload_to_s3(s3_client, payload: dict, s3_key: str) -> None:
    logger = logging.getLogger(__name__)
    bucket = _get_export_s3_bucket()

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(payload, ensure_ascii=False),
            ContentType="application/json",
        )
        logger.info(
            "Payload uploaded to s3://%s/%s source=%s sample_sn=%s",
            bucket,
            s3_key,
            payload.get("source"),
            payload.get("sample_sn"),
        )
    except Exception:
        logger.exception(
            "Failed to upload payload to s3://%s/%s source=%s sample_sn=%s",
            bucket,
            s3_key,
            payload.get("source"),
            payload.get("sample_sn"),
        )
        raise


def _send_sqs_notification(
    sqs_client,
    s3_key: str,
    payload: dict,
    deduplication_id: str,
) -> None:
    logger = logging.getLogger(__name__)

    notification = {
        "event_type": "data_export",
        "bucket": _get_export_s3_bucket(),
        "key": s3_key,
        "source": payload.get("source"),
        "sample_sn": payload.get("sample_sn"),
        "timestamp": payload.get("timestamp"),
        "schema_version": payload.get("schema_version"),
    }

    try:
        sqs_client.send_message(
            QueueUrl=_get_sqs_queue_url(),
            MessageBody=json.dumps(notification, ensure_ascii=False),
            MessageGroupId=DAG_ID,
            MessageDeduplicationId=deduplication_id,
        )
        logger.info(
            "SQS notification sent: source=%s sample_sn=%s key=%s",
            payload.get("source"),
            payload.get("sample_sn"),
            s3_key,
        )
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code", "Unknown")
        error_message = error.response.get("Error", {}).get("Message", "")

        if error_code in SQS_THROTTLING_ERROR_CODES:
            logger.error(
                "SQS throttling detected. error_code=%s error_message=%s "
                "source=%s sample_sn=%s key=%s",
                error_code,
                error_message,
                payload.get("source"),
                payload.get("sample_sn"),
                s3_key,
            )
            raise

        logger.exception(
            "Failed to send SQS notification. error_code=%s error_message=%s "
            "source=%s sample_sn=%s key=%s",
            error_code,
            error_message,
            payload.get("source"),
            payload.get("sample_sn"),
            s3_key,
        )
        raise


def publish_payloads(payloads: List[dict]) -> int:
    sqs_client = _get_sqs_client()
    s3_client = _get_s3_client()
    sent_count = 0

    for payload in payloads:
        dedup_id = create_deduplication_id(
            timestamp=datetime.fromisoformat(payload["timestamp"]),
            tb_name=payload.get("source"),
            dut_sn=payload.get("sample_sn"),
            position=payload.get("sample_source_position", ""),
        )
        s3_key = _build_s3_key(payload, dedup_id)
        _upload_payload_to_s3(s3_client, payload, s3_key)
        _send_sqs_notification(sqs_client, s3_key, payload, dedup_id)
        sent_count += 1

    return sent_count
