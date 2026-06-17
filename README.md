# Exporter

Airflow DAG that exports hourly time-series data from InfluxDB to S3 and notifies consumers via SQS FIFO, with automatic gap detection and exactly-once-intent delivery.

## Architecture

```
┌────────────┐     ┌──────────────────┐     ┌──────────────┐     ┌───────────┐
│  InfluxDB  │────>│  ExportService   │────>│  S3 (data)   │     │ SQS FIFO  │
│  (metrics) │     │  (query+process) │     │  PutObject   │     │ (notify)  │
└────────────┘     └──────────────────┘     └──────┬───────┘     └─────┬─────┘
                            │                      │                   │
                   ┌────────┴────────┐             └───────┬───────────┘
                   │   Processor     │                     │
                   │ (DataFrame →    │              ┌──────┴───────┐
                   │  domain models) │              │   Publisher  │
                   └─────────────────┘              │  1. S3 put   │
                                                    │  2. SQS notify│
                   ┌─────────────────┐              └──────┬───────┘
                   │  S3 Manifest    │                     │
                   │ (gap detection  │<────────────────────┘
                   │  + idempotency) │  advance only after confirm
                   └─────────────────┘
```

**Claim-check pattern**: payloads are stored in S3; SQS carries only a lightweight notification with the S3 reference (`bucket` + `key`). Consumers fetch the full payload from S3 when ready to process.

**Data flow per hourly window:**

1. Read the S3 manifest to determine the last confirmed window.
2. Detect and queue any missing hourly gaps.
3. For each pending window:
   - Query bench status and cycle metrics from InfluxDB.
   - Enrich with last-known cycle objectives (30-day lookback).
   - Transform into domain models and build export payloads.
   - Upload each payload to S3 (`exports/{date}/{dedup_id}.json`).
   - Send SQS FIFO notification with S3 reference and deduplication ID.
   - Advance the S3 manifest **only after** both S3 and SQS confirm.
4. If any step fails, Airflow retries the task — no silent data loss.

## Project Structure

```
exporter_dag.py               # Airflow DAG entry point (~90 lines)
exporter/
├── models.py                # Domain: TimeWindow, SampleData, ExportRecord
├── config.py                # InfluxDBConfig, ApplicationConfig (Secrets Manager)
├── repository.py            # InfluxDB query adapter (Flux queries)
├── processor.py             # DataFrame → domain model transformation
├── service.py               # Export orchestration (query → process → payloads)
├── publisher.py             # S3 upload + SQS notification (claim-check)
└── manifest.py              # S3 manifest tracking + gap detection
```

## Prerequisites

- Python 3.9+
- Apache Airflow 2.7+ (runtime environment)
- AWS credentials with access to:
  - **Secrets Manager** — InfluxDB connection details
  - **SQS** — `SendMessage` on the target FIFO queue
  - **S3** — `PutObject` on the export data bucket, `GetObject`/`PutObject` on the manifest bucket

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment variables

Copy the example and fill in your values:

```bash
cp .env.example .env
```

| Variable | Required | Description |
|---|---|---|
| `SQS_QUEUE_URL` | Yes | Full URL of the SQS FIFO queue |
| `EXPORT_S3_BUCKET` | Yes | S3 bucket for export payloads (claim-check data store) |
| `MANIFEST_S3_BUCKET` | Yes | S3 bucket for the processing manifest |
| `INFLUXDB_SECRET_NAME` | Yes | Secrets Manager path with InfluxDB credentials |
| `AWS_REGION` | No | AWS region (default: `eu-central-1`) |
| `EXPORT_S3_PREFIX` | No | Key prefix for export objects (default: `exports/`) |
| `MANIFEST_S3_KEY` | No | Manifest object key (default: `Manifest_SQS_Processed.json`) |
| `LOG_LEVEL` | No | Logging level (default: `INFO`) |

### 3. Secrets Manager format

The secret referenced by `INFLUXDB_SECRET_NAME` must contain:

```json
{
  "url": "https://your-influxdb-host:8086",
  "token": "your-influxdb-token",
  "org": "your-org",
  "bucket": "your-bucket",
  "measurement": "your-measurement"
}
```

### 4. SQS notification format

Each SQS message body is a JSON notification pointing to the S3 payload:

```json
{
  "event_type": "data_export",
  "bucket": "your-export-bucket",
  "key": "exports/2026-06-17/20260617_140000_00000_TB01_SN12345.json",
  "source": "TB01",
  "sample_sn": "SN12345",
  "timestamp": "2026-06-17T13:00:00+00:00",
  "schema_version": "v3"
}
```

Consumers read this notification, then fetch the full payload with `s3.GetObject(Bucket, Key)`.

### 5. Deploy to Airflow

Copy `exporter_dag.py` and the `exporter/` package to your Airflow DAGs directory:

```bash
cp exporter_dag.py /path/to/airflow/dags/
cp -r exporter/ /path/to/airflow/dags/exporter/
```

The DAG runs hourly (`0 * * * *`) and processes the previous complete hour.

## Local execution

```bash
export SQS_QUEUE_URL="https://sqs.eu-central-1.amazonaws.com/123456789012/my-queue.fifo"
export EXPORT_S3_BUCKET="my-export-bucket"
export MANIFEST_S3_BUCKET="my-manifest-bucket"
export INFLUXDB_SECRET_NAME="my/secret/path"

python exporter_dag.py
```

## Design Decisions

- **Claim-check pattern**: SQS carries only a reference to S3, not the data. Eliminates the 256 KiB SQS message limit, persists payloads for audit/replay, and decouples producers from consumers.
- **Manifest-first idempotency**: the manifest only advances after both S3 upload and SQS notification succeed. Duplicates are possible on retries (S3 overwrites are idempotent, SQS dedup window handles the rest), but data loss is not.
- **Bench status as source of truth**: bench_status is queried independently from cycle metrics. A bench in STOPPED or DISCONNECTED state is exported even without Cycle data.
- **30-day objective lookback**: cycle objectives are not emitted every hour, so the exporter looks back up to 30 days to find the last known value per testbench + test name.
- **Lazy configuration**: required environment variables are read at call time, not at import time. This prevents Airflow DAG parsing failures when env vars are only available at task execution.
