"""S3 manifest tracking, gap detection, and window orchestration."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple

from botocore.exceptions import ClientError

from lib.utils.aws_secrets_util import get_boto3_session

from .models import TimeWindow
from .publisher import publish_payloads
from .service import ExportService


def _get_manifest_bucket() -> str:
    return os.environ["MANIFEST_S3_BUCKET"]


def _get_manifest_key() -> str:
    return os.getenv("MANIFEST_S3_KEY", "Manifest_SQS_Processed.json")


def get_s3_client():
    return get_boto3_session().client("s3")


def load_last_processed_window() -> Optional[TimeWindow]:
    logger = logging.getLogger(__name__)
    s3_client = get_s3_client()

    try:
        response = s3_client.get_object(Bucket=_get_manifest_bucket(), Key=_get_manifest_key())
        manifest_data = json.loads(response["Body"].read().decode("utf-8"))

        start = datetime.fromisoformat(manifest_data["last_processed_start"])
        end = datetime.fromisoformat(manifest_data["last_processed_end"])

        logger.info(
            "Loaded last processed window: %s -> %s",
            start.isoformat(),
            end.isoformat(),
        )
        return TimeWindow(start=start, end=end)

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logger.info("No manifest found in S3. This is the first execution")
            return None
        else:
            logger.exception("Error loading manifest from S3")
            raise
    except Exception:
        logger.exception("Unexpected error loading manifest")
        raise


def save_processed_window(window: TimeWindow, payloads_sent: int) -> None:
    logger = logging.getLogger(__name__)
    s3_client = get_s3_client()

    manifest_data = {
        "last_processed_start": window.start.isoformat(),
        "last_processed_end": window.end.isoformat(),
        "generation_timestamp": datetime.now(timezone.utc).isoformat(),
        "payloads_sent": payloads_sent,
    }

    try:
        s3_client.put_object(
            Bucket=_get_manifest_bucket(),
            Key=_get_manifest_key(),
            Body=json.dumps(manifest_data, indent=2),
            ContentType="application/json",
        )
        logger.info(
            "Manifest updated in S3: %s -> %s (%s payloads sent)",
            window.start.isoformat(),
            window.end.isoformat(),
            payloads_sent,
        )
    except Exception:
        logger.exception("Failed to save manifest to S3")
        raise


def get_previous_hour_window() -> TimeWindow:
    """
    Return the previous complete hourly window in UTC.

    Window convention: start <= data_time < end

    Example: if now is 10:30 UTC, returns 09:00 -> 10:00.
    The current open hour is never processed yet.
    """
    now = datetime.now(timezone.utc)
    current_hour = now.replace(minute=0, second=0, microsecond=0)

    return TimeWindow(
        start=current_hour - timedelta(hours=1),
        end=current_hour,
    )


def get_missing_windows(
    last_window: Optional[TimeWindow],
    current_window: TimeWindow,
) -> List[TimeWindow]:
    """
    Calculate pending complete hourly windows using half-open intervals.

    Rules:
    - If there is no manifest yet, process only the current previous-hour window.
    - If the current window is already confirmed in the manifest, process nothing.
    - If there are gaps, process from last_window.end up to current_window.end.
    - Windows are contiguous: the end of one window is exactly the start of the next.
    """
    if last_window is None:
        return [current_window]

    if current_window.end <= last_window.end:
        return []

    missing_windows: List[TimeWindow] = []
    next_window_start = last_window.end

    while next_window_start < current_window.start:
        next_window_end = next_window_start + timedelta(hours=1)
        missing_windows.append(
            TimeWindow(start=next_window_start, end=next_window_end)
        )
        next_window_start = next_window_end

    missing_windows.append(current_window)
    return missing_windows


def process_with_gap_detection(service: ExportService) -> Tuple[int, List[TimeWindow]]:
    """
    Process pending hourly windows and update the manifest only after
    both S3 upload and SQS notification confirm for each window.

    Safe order per window:
    1) Read from InfluxDB.
    2) Build export payloads.
    3) Upload each payload to S3 and send SQS notification.
    4) Advance the manifest only when everything succeeds.

    If any step fails, the Airflow task fails and retries.
    Duplicates may happen in edge cases, but duplicates are safer than lost data.
    """
    logger = logging.getLogger(__name__)

    current_window = get_previous_hour_window()
    last_window = load_last_processed_window()
    windows_to_process = get_missing_windows(last_window, current_window)

    if not windows_to_process:
        logger.info(
            "No pending windows to process. Current previous-hour window is already confirmed: %s -> %s",
            current_window.start.isoformat(),
            current_window.end.isoformat(),
        )
        return 0, []

    if len(windows_to_process) > 1:
        first_window = windows_to_process[0]
        last_window_to_process = windows_to_process[-1]
        total_missing = len(windows_to_process) - 1

        logger.warning(
            "GAP DETECTED! Missing windows detected.\n"
            "Last processed: %s\n"
            "Current window: %s\n"
            "Missing windows: %s\n"
            "Time range to cover: %s -> %s\n"
            "Processing all missing windows automatically...",
            last_window.end.isoformat() if last_window else "NONE (first run)",
            current_window.start.isoformat(),
            total_missing,
            first_window.start.isoformat(),
            last_window_to_process.end.isoformat(),
        )

    total_sent = 0
    processed_windows: List[TimeWindow] = []

    for window in windows_to_process:
        logger.info(
            "Processing window: %s -> %s",
            window.start.isoformat(),
            window.end.isoformat(),
        )

        payloads = service.execute(window)
        sent_count = publish_payloads(payloads)

        save_processed_window(window, sent_count)

        total_sent += sent_count
        processed_windows.append(window)
        logger.info(
            "Window confirmed and manifest advanced: %s -> %s (%s payloads sent)",
            window.start.isoformat(),
            window.end.isoformat(),
            sent_count,
        )

    logger.info("All windows processed successfully. Total payloads published: %s", total_sent)
    return total_sent, processed_windows
