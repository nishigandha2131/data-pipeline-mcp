from __future__ import annotations

import argparse
import random
import string
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from google.cloud import bigquery

# Allow running as a script from scripts/ while importing project modules.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import bigquery_support as bq


def _rand_id(prefix: str, n: int = 10) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return f"{prefix}_" + "".join(random.choice(alphabet) for _ in range(n))


@dataclass(frozen=True)
class DemoConfig:
    days: int
    pipelines: list[str]
    seed: int


def _generate_runs(cfg: DemoConfig) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    triggers = ["schedule", "manual", "backfill", "webhook"]
    owners = ["data-platform", "analytics", "ml-platform", "infra"]
    statuses = ["success", "failed", "running", "cancelled"]

    runs: list[dict[str, Any]] = []
    base_run_ids: list[str] = []

    for day_offset in range(cfg.days):
        day = now - timedelta(days=day_offset)
        weekday = day.weekday()
        runs_today = 2 if weekday >= 5 else 4

        for _ in range(runs_today):
            pipeline = random.choice(cfg.pipelines)
            run_id = _rand_id("run")
            base_run_ids.append(run_id)

            start = day.replace(hour=random.randint(0, 23), minute=random.randint(0, 59), second=0, microsecond=0)
            status = random.choices(statuses, weights=[0.72, 0.18, 0.06, 0.04], k=1)[0]
            duration_min = int(max(1, random.gauss(18, 8)))

            ended_at = None if status == "running" else (day + timedelta(minutes=duration_min)).replace(tzinfo=timezone.utc)
            if random.random() < 0.02 and ended_at is not None:
                ended_at = (day + timedelta(hours=7, minutes=12)).replace(tzinfo=timezone.utc)

            rows = int(max(0, random.gauss(250_000, 120_000)))
            if random.random() < 0.01:
                rows = -abs(rows)

            git_sha = None if random.random() < 0.08 else _rand_id("sha", n=12)
            trigger = random.choice(triggers)
            owner = random.choice(owners)

            error_message = None
            if status in {"failed", "cancelled"}:
                error_message = random.choice(
                    [
                        "Upstream source timeout",
                        "Schema mismatch: unexpected column",
                        "Out of memory during join",
                        "Permissions error reading s3://…",
                        None,
                    ]
                )
            if status == "success" and random.random() < 0.02:
                error_message = "Transient error recovered after retry"

            runs.append(
                {
                    "run_id": run_id,
                    "pipeline_name": pipeline,
                    "started_at": start.replace(tzinfo=timezone.utc),
                    "ended_at": ended_at,
                    "status": status,
                    "trigger": trigger,
                    "git_sha": git_sha,
                    "owner": owner,
                    "rows_processed": rows,
                    "error_message": error_message,
                }
            )

    if base_run_ids:
        dup_run_id = random.choice(base_run_ids)
        dup_row = next(r for r in runs if r["run_id"] == dup_run_id)
        runs.append(dict(dup_row))

    return runs


def _generate_metrics(cfg: DemoConfig, runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    metrics: list[dict[str, Any]] = []

    for run in runs:
        run_id = run["run_id"]
        started_at: datetime = run["started_at"]
        ended_at: datetime | None = run["ended_at"]
        status: str = run["status"]
        rows_processed = run["rows_processed"]

        if ended_at is None:
            end_ts = started_at + timedelta(minutes=random.randint(5, 60))
        else:
            end_ts = ended_at

        duration_s = max(1.0, (end_ts - started_at).total_seconds())
        latency_ms = duration_s * 1000.0 / random.uniform(1.5, 4.2)
        error_rate = 0.0 if status == "success" else float(min(1.0, abs(random.gauss(0.12, 0.09))))

        if random.random() < 0.02:
            latency_ms *= random.uniform(6.0, 12.0)
        if random.random() < 0.02:
            error_rate = min(1.0, error_rate + random.uniform(0.25, 0.6))

        rowcount_value = float(rows_processed) if rows_processed is not None else None
        metric_ts = (started_at + timedelta(minutes=random.randint(0, 3))).replace(tzinfo=timezone.utc)

        metrics.extend(
            [
                {
                    "run_id": run_id,
                    "metric_ts": metric_ts,
                    "metric_name": "rows_processed",
                    "metric_value": rowcount_value,
                    "unit": "rows",
                    "source": "pipeline",
                },
                {
                    "run_id": run_id,
                    "metric_ts": metric_ts,
                    "metric_name": "duration_seconds",
                    "metric_value": float(duration_s),
                    "unit": "s",
                    "source": "pipeline",
                },
                {
                    "run_id": run_id,
                    "metric_ts": metric_ts,
                    "metric_name": "latency",
                    "metric_value": float(latency_ms),
                    "unit": "ms",
                    "source": "pipeline",
                },
                {
                    "run_id": run_id,
                    "metric_ts": metric_ts,
                    "metric_name": "error_rate",
                    "metric_value": float(error_rate),
                    "unit": "ratio",
                    "source": "pipeline",
                },
            ]
        )

        if random.random() < 0.03:
            metrics.append(
                {
                    "run_id": run_id,
                    "metric_ts": metric_ts,
                    "metric_name": "cpu_pct",
                    "metric_value": None,
                    "unit": "%",
                    "source": "host",
                }
            )

        if random.random() < 0.02:
            metrics.append(
                {
                    "run_id": run_id,
                    "metric_ts": metric_ts,
                    "metric_name": "latency",
                    "metric_value": float(latency_ms / 1000.0),
                    "unit": "s",
                    "source": "pipeline",
                }
            )

        if random.random() < 0.03:
            metrics.append(
                {
                    "run_id": run_id,
                    "metric_ts": metric_ts,
                    "metric_name": "rows_processed",
                    "metric_value": rowcount_value,
                    "unit": "rows",
                    "source": "pipeline",
                }
            )

    for _ in range(3):
        fake_run = _rand_id("run")
        ts = (datetime.now(timezone.utc) - timedelta(hours=random.randint(1, 48))).replace(microsecond=0)
        metrics.append(
            {
                "run_id": fake_run,
                "metric_ts": ts,
                "metric_name": "rows_processed",
                "metric_value": float(random.randint(10_000, 90_000)),
                "unit": "rows",
                "source": "pipeline",
            }
        )

    return metrics


def _generate_dq_logs(cfg: DemoConfig, runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    dq: list[dict[str, Any]] = []
    checks = [
        ("not_null", "error"),
        ("unique_key", "error"),
        ("row_count_freshness", "warn"),
        ("schema_drift", "error"),
        ("valid_values", "warn"),
    ]
    tables = ["raw_events", "stg_users", "fct_orders", "dim_products"]

    for run in runs:
        run_id = run["run_id"]
        started_at: datetime = run["started_at"]
        status: str = run["status"]

        n_checks = 1 if status in {"running", "cancelled"} else random.randint(2, 5)
        checked_at = (started_at + timedelta(minutes=random.randint(1, 30))).replace(tzinfo=timezone.utc)

        for _ in range(n_checks):
            table_name = random.choice(tables)
            check_name, severity = random.choice(checks)

            passed = random.random() < (0.86 if status == "success" else 0.72)
            failed_rows: int | None = 0 if passed else random.randint(1, 2500)

            sample_bad_values = None
            notes = None
            if not passed:
                if check_name == "not_null":
                    sample_bad_values = random.choice(['{"user_id": null}', '{"email": null}', '{"order_id": null}'])
                elif check_name == "unique_key":
                    sample_bad_values = random.choice(['{"order_id": 12345}', '{"user_id": 9981}'])
                elif check_name == "schema_drift":
                    notes = random.choice(
                        [
                            "Unexpected column added: marketing_opt_in",
                            "Type change detected: amount double -> varchar",
                        ]
                    )

            dq_run_id = None if random.random() < 0.02 else run_id
            if not passed and random.random() < 0.03:
                failed_rows = None

            row = {
                "log_id": _rand_id("dq"),
                "run_id": dq_run_id,
                "table_name": table_name,
                "check_name": check_name,
                "severity": severity,
                "checked_at": checked_at,
                "passed": passed,
                "failed_rows": failed_rows,
                "sample_bad_values": sample_bad_values,
                "notes": notes,
            }
            dq.append(row)

            if random.random() < 0.02:
                dq.append(dict(row))

    dq.append(
        {
            "log_id": _rand_id("dq"),
            "run_id": None,
            "table_name": "fct_orders",
            "check_name": "valid_values",
            "severity": "error",
            "checked_at": datetime.now(timezone.utc).replace(microsecond=0),
            "passed": False,
            "failed_rows": 9_999_999,
            "sample_bad_values": '{"amount": "NaN", "currency": "???"}',
            "notes": "Bulk corruption suspected (demo outlier).",
        }
    )

    return dq


def _ensure_table(client: bigquery.Client, table_id: str, schema: list[bigquery.SchemaField]) -> None:
    try:
        client.get_table(table_id)
        return
    except Exception:
        pass
    t = bigquery.Table(table_id, schema=schema)
    client.create_table(t)


def _insert_json_in_batches(
    client: bigquery.Client,
    table_id: str,
    rows: list[dict[str, Any]],
    batch_size: int = 500,
) -> None:
    def _to_jsonable(v: Any) -> Any:
        if isinstance(v, datetime):
            # BigQuery accepts RFC3339 timestamps in JSON rows.
            s = v.astimezone(timezone.utc).replace(microsecond=0).isoformat()
            return s.replace("+00:00", "Z")
        return v

    for i in range(0, len(rows), batch_size):
        raw_batch = rows[i : i + batch_size]
        batch = [{k: _to_jsonable(v) for k, v in r.items()} for r in raw_batch]
        errors = client.insert_rows_json(table_id, batch)
        if errors:
            raise RuntimeError(f"Insert failed for {table_id}: {errors[:3]}")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Seed BigQuery with demo tables for data-pipeline-mcp.")
    p.add_argument("--config", default=None, help="Path to BigQuery YAML config (defaults to BQ_CONFIG_PATH).")
    p.add_argument("--days", type=int, default=30, help="Days of history to generate.")
    p.add_argument("--seed", type=int, default=7, help="Random seed for reproducible demo data.")
    p.add_argument(
        "--mode",
        choices=["append", "recreate"],
        default="append",
        help="append: create tables if missing and append rows; recreate: drop tables and recreate them before inserting.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    settings = bq.load_bigquery_settings(args.config)
    if not settings.default_dataset:
        raise ValueError("BigQuery YAML must set default_dataset to seed demo tables.")
    if settings.default_dataset.strip().upper() in {"REPLACE_ME", "REPLACEME"}:
        raise ValueError('Set default_dataset in config/bigquery.local.yaml (it is currently "REPLACE_ME").')

    random.seed(int(args.seed))

    cfg = DemoConfig(
        days=int(args.days),
        pipelines=["orders_ingest", "user_sync", "marketing_attribution", "fraud_scoring"],
        seed=int(args.seed),
    )

    client = bq.bigquery_client(settings)
    dataset = settings.default_dataset
    project = settings.project_id

    # Validate dataset exists early with a helpful message.
    try:
        client.get_dataset(f"{project}.{dataset}")
    except Exception as e:
        try:
            available = [d.dataset_id for d in client.list_datasets(project)]
        except Exception:
            available = []
        hint = ""
        if available:
            hint = f" Available datasets in {project}: {', '.join(sorted(available))}"
        raise RuntimeError(f"Dataset not found: {project}.{dataset}.{hint}") from e

    runs_table = f"{project}.{dataset}.pipeline_runs"
    metrics_table = f"{project}.{dataset}.pipeline_metrics"
    dq_table = f"{project}.{dataset}.data_quality_log"

    if args.mode == "recreate":
        client.delete_table(runs_table, not_found_ok=True)
        client.delete_table(metrics_table, not_found_ok=True)
        client.delete_table(dq_table, not_found_ok=True)

    _ensure_table(
        client,
        runs_table,
        [
            bigquery.SchemaField("run_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("pipeline_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("started_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("ended_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("trigger", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("git_sha", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("owner", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("rows_processed", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
        ],
    )
    _ensure_table(
        client,
        metrics_table,
        [
            bigquery.SchemaField("run_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("metric_ts", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("metric_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("metric_value", "FLOAT64", mode="NULLABLE"),
            bigquery.SchemaField("unit", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
        ],
    )
    _ensure_table(
        client,
        dq_table,
        [
            bigquery.SchemaField("log_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("run_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("check_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("severity", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("checked_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("passed", "BOOL", mode="REQUIRED"),
            bigquery.SchemaField("failed_rows", "INT64", mode="NULLABLE"),
            bigquery.SchemaField("sample_bad_values", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("notes", "STRING", mode="NULLABLE"),
        ],
    )

    runs = _generate_runs(cfg)
    metrics = _generate_metrics(cfg, runs)
    dq_logs = _generate_dq_logs(cfg, runs)

    _insert_json_in_batches(client, runs_table, runs)
    _insert_json_in_batches(client, metrics_table, metrics)
    _insert_json_in_batches(client, dq_table, dq_logs)

    print(
        f"Seeded BigQuery demo tables ({args.mode} mode):\n"
        f"- {runs_table}: +{len(runs)} rows\n"
        f"- {metrics_table}: +{len(metrics)} rows\n"
        f"- {dq_table}: +{len(dq_logs)} rows\n"
    )


if __name__ == "__main__":
    main()

