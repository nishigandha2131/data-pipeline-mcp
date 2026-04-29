import argparse
import os
import random
import string
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import duckdb


def _rand_id(prefix: str, n: int = 10) -> str:
    alphabet = string.ascii_lowercase + string.digits
    return f"{prefix}_" + "".join(random.choice(alphabet) for _ in range(n))


def _ts(dt: datetime) -> datetime:
    # DuckDB stores tz-naive timestamps by default; keep things consistent.
    return dt.replace(tzinfo=None)


@dataclass(frozen=True)
class DemoConfig:
    db_path: str
    days: int
    pipelines: list[str]
    seed: int


def _create_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        create table if not exists pipeline_runs (
          run_id text,
          pipeline_name text not null,
          started_at timestamp not null,
          ended_at timestamp,
          status text not null,
          trigger text not null,
          git_sha text,
          owner text not null,
          rows_processed bigint,
          error_message text
        );
        """
    )

    con.execute(
        """
        create table if not exists pipeline_metrics (
          run_id text,
          metric_ts timestamp not null,
          metric_name text not null,
          metric_value double,
          unit text,
          source text
        );
        """
    )

    con.execute(
        """
        create table if not exists data_quality_log (
          log_id text,
          run_id text,
          table_name text not null,
          check_name text not null,
          severity text not null,
          checked_at timestamp not null,
          passed boolean not null,
          failed_rows bigint,
          sample_bad_values text,
          notes text
        );
        """
    )


def _truncate_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("delete from pipeline_runs;")
    con.execute("delete from pipeline_metrics;")
    con.execute("delete from data_quality_log;")


def _generate_runs(cfg: DemoConfig) -> list[tuple]:
    now = datetime.now(timezone.utc)
    triggers = ["schedule", "manual", "backfill", "webhook"]
    owners = ["data-platform", "analytics", "ml-platform", "infra"]
    statuses = ["success", "failed", "running", "cancelled"]

    runs: list[tuple] = []
    base_run_ids: list[str] = []

    for day_offset in range(cfg.days):
        day = now - timedelta(days=day_offset)
        # More runs on weekdays than weekends.
        weekday = day.weekday()
        runs_today = 2 if weekday >= 5 else 4

        for _ in range(runs_today):
            pipeline = random.choice(cfg.pipelines)
            run_id = _rand_id("run")
            base_run_ids.append(run_id)

            start = day.replace(hour=random.randint(0, 23), minute=random.randint(0, 59), second=0, microsecond=0)
            start = _ts(start)

            status = random.choices(statuses, weights=[0.72, 0.18, 0.06, 0.04], k=1)[0]
            duration_min = int(max(1, random.gauss(18, 8)))

            ended_at = None if status == "running" else _ts((day + timedelta(minutes=duration_min)).replace(tzinfo=timezone.utc))

            # Intentional long-duration outlier.
            if random.random() < 0.02 and ended_at is not None:
                ended_at = _ts((day + timedelta(hours=7, minutes=12)).replace(tzinfo=timezone.utc))

            rows = int(max(0, random.gauss(250_000, 120_000)))
            # Intentional negative anomaly.
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

            # Intentional contradiction: success with error_message.
            if status == "success" and random.random() < 0.02:
                error_message = "Transient error recovered after retry"

            runs.append(
                (
                    run_id,
                    pipeline,
                    start,
                    ended_at,
                    status,
                    trigger,
                    git_sha,
                    owner,
                    rows,
                    error_message,
                )
            )

    # Intentional duplicate run_id row.
    if base_run_ids:
        dup_run_id = random.choice(base_run_ids)
        dup_row = next(r for r in runs if r[0] == dup_run_id)
        runs.append(dup_row)

    return runs


def _generate_metrics(cfg: DemoConfig, runs: list[tuple]) -> list[tuple]:
    metrics: list[tuple] = []

    for run in runs:
        run_id, pipeline_name, started_at, ended_at, status, *_rest, rows_processed, _err = run
        base_ts = started_at

        if ended_at is None:
            end_ts = started_at + timedelta(minutes=random.randint(5, 60))
        else:
            end_ts = ended_at

        duration_s = max(1.0, (end_ts - started_at).total_seconds())
        # Latency baseline depends on pipeline.
        latency_ms = duration_s * 1000.0 / random.uniform(1.5, 4.2)

        error_rate = 0.0 if status == "success" else float(min(1.0, abs(random.gauss(0.12, 0.09))))

        # Intentional spikes.
        if random.random() < 0.02:
            latency_ms *= random.uniform(6.0, 12.0)
        if random.random() < 0.02:
            error_rate = min(1.0, error_rate + random.uniform(0.25, 0.6))

        rowcount_value = float(rows_processed) if rows_processed is not None else None

        # Primary metric timestamp.
        metric_ts = _ts((base_ts + timedelta(minutes=random.randint(0, 3))).replace(tzinfo=timezone.utc))

        # A few core metrics per run.
        metrics.extend(
            [
                (run_id, metric_ts, "rows_processed", rowcount_value, "rows", "pipeline"),
                (run_id, metric_ts, "duration_seconds", float(duration_s), "s", "pipeline"),
                (run_id, metric_ts, "latency", float(latency_ms), "ms", "pipeline"),
                (run_id, metric_ts, "error_rate", float(error_rate), "ratio", "pipeline"),
            ]
        )

        # Intentional null metric_value.
        if random.random() < 0.03:
            metrics.append((run_id, metric_ts, "cpu_pct", None, "%", "host"))

        # Intentional inconsistent unit (seconds instead of ms).
        if random.random() < 0.02:
            metrics.append((run_id, metric_ts, "latency", float(latency_ms / 1000.0), "s", "pipeline"))

        # Intentional duplicate metric row.
        if random.random() < 0.03:
            metrics.append((run_id, metric_ts, "rows_processed", rowcount_value, "rows", "pipeline"))

    # Intentional orphan metrics (run_id not in pipeline_runs).
    for _ in range(3):
        fake_run = _rand_id("run")
        ts = _ts((datetime.now(timezone.utc) - timedelta(hours=random.randint(1, 48))).replace(tzinfo=timezone.utc))
        metrics.append((fake_run, ts, "rows_processed", float(random.randint(10_000, 90_000)), "rows", "pipeline"))

    return metrics


def _generate_dq_logs(cfg: DemoConfig, runs: list[tuple]) -> list[tuple]:
    dq: list[tuple] = []
    checks = [
        ("not_null", "error"),
        ("unique_key", "error"),
        ("row_count_freshness", "warn"),
        ("schema_drift", "error"),
        ("valid_values", "warn"),
    ]
    tables = ["raw_events", "stg_users", "fct_orders", "dim_products"]

    for run in runs:
        run_id, _pipeline, started_at, _ended_at, status, *_rest = run
        # Fewer checks for running/cancelled.
        n_checks = 1 if status in {"running", "cancelled"} else random.randint(2, 5)
        checked_at = _ts((started_at + timedelta(minutes=random.randint(1, 30))).replace(tzinfo=timezone.utc))

        for _ in range(n_checks):
            table_name = random.choice(tables)
            check_name, severity = random.choice(checks)

            passed = random.random() < (0.86 if status == "success" else 0.72)
            failed_rows = 0 if passed else random.randint(1, 2500)

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

            # Intentional null run_id occasionally.
            dq_run_id = None if random.random() < 0.02 else run_id

            # Intentional anomaly: failed_rows null even when failed.
            if not passed and random.random() < 0.03:
                failed_rows = None

            dq.append(
                (
                    _rand_id("dq"),
                    dq_run_id,
                    table_name,
                    check_name,
                    severity,
                    checked_at,
                    passed,
                    failed_rows,
                    sample_bad_values,
                    notes,
                )
            )

            # Intentional duplicate log (same check at same time).
            if random.random() < 0.02:
                dq.append(dq[-1])

    # Extreme outlier.
    dq.append(
        (
            _rand_id("dq"),
            None,
            "fct_orders",
            "valid_values",
            "error",
            _ts(datetime.now(timezone.utc)),
            False,
            9_999_999,
            '{"amount": "NaN", "currency": "???"}',
            "Bulk corruption suspected (demo outlier).",
        )
    )

    return dq


def generate_demo_db(cfg: DemoConfig) -> str:
    os.makedirs(os.path.dirname(cfg.db_path) or ".", exist_ok=True)

    con = duckdb.connect(cfg.db_path)
    try:
        _create_schema(con)
        _truncate_tables(con)

        runs = _generate_runs(cfg)
        con.executemany(
            """
            insert into pipeline_runs values
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            runs,
        )

        metrics = _generate_metrics(cfg, runs)
        con.executemany(
            """
            insert into pipeline_metrics values
              (?, ?, ?, ?, ?, ?);
            """,
            metrics,
        )

        dq_logs = _generate_dq_logs(cfg, runs)
        con.executemany(
            """
            insert into data_quality_log values
              (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            dq_logs,
        )

        con.execute("create index if not exists idx_pipeline_runs_run_id on pipeline_runs(run_id);")
        con.execute("create index if not exists idx_pipeline_metrics_run_id on pipeline_metrics(run_id);")
        con.execute("create index if not exists idx_dq_run_id on data_quality_log(run_id);")
    finally:
        con.close()

    return cfg.db_path


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate a demo DuckDB database for MCP demos.")
    p.add_argument("--db-path", default=os.getenv("DB_PATH", "pipeline_demo.duckdb"))
    p.add_argument("--days", type=int, default=30)
    p.add_argument("--seed", type=int, default=7)
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    random.seed(args.seed)

    cfg = DemoConfig(
        db_path=args.db_path,
        days=args.days,
        pipelines=["orders_ingest", "user_sync", "marketing_attribution", "fraud_scoring"],
        seed=args.seed,
    )

    path = generate_demo_db(cfg)
    print(f"Demo DB written to: {path}")


if __name__ == "__main__":
    main()

