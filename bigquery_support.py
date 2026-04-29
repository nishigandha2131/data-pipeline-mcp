from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import yaml
from google.cloud import bigquery
from google.oauth2 import service_account


@dataclass
class BigQuerySettings:
    project_id: str
    location: str | None
    default_dataset: str | None
    auth_mode: str
    credentials_path: str | None
    maximum_bytes_billed: int | None
    tables: dict[str, str | None] = field(default_factory=dict)
    config_path: str | None = None


def default_bigquery_config_path(explicit: str | None) -> str:
    return explicit or os.getenv("BQ_CONFIG_PATH", "config/bigquery.local.yaml")


def load_bigquery_settings(config_path: str | None = None) -> BigQuerySettings:
    path = default_bigquery_config_path(config_path)
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"BigQuery config not found: {path}. "
            "Copy config/bigquery.example.yaml to config/bigquery.local.yaml and edit it (see README)."
        )
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise ValueError("BigQuery YAML root must be a mapping.")

    project_id = (raw.get("project_id") or "").strip()
    if not project_id:
        raise ValueError("BigQuery YAML must set project_id.")

    auth = raw.get("auth") or {}
    if not isinstance(auth, dict):
        raise ValueError("BigQuery YAML auth must be a mapping.")
    auth_mode = (auth.get("mode") or "adc").strip().lower()
    if auth_mode not in {"adc", "service_account"}:
        raise ValueError('auth.mode must be "adc" or "service_account".')
    cred_path = auth.get("credentials_path")
    credentials_path = str(cred_path).strip() if cred_path else None
    if auth_mode == "service_account":
        if not credentials_path or not os.path.isfile(credentials_path):
            raise ValueError("auth.mode is service_account but credentials_path is missing or not a file.")

    tables_raw = raw.get("tables") or {}
    if tables_raw is not None and not isinstance(tables_raw, dict):
        raise ValueError("tables must be a mapping of logical names to fully-qualified tables or null.")

    tables: dict[str, str | None] = {}
    for k, v in tables_raw.items():
        if v is None or v == "":
            tables[str(k)] = None
        else:
            tables[str(k)] = str(v).strip()

    if "maximum_bytes_billed" not in raw:
        maximum_bytes_billed = 5 * 1024**3
    elif raw.get("maximum_bytes_billed") is None:
        maximum_bytes_billed = None
    else:
        maximum_bytes_billed = int(raw["maximum_bytes_billed"])

    loc = raw.get("location")
    location = str(loc).strip() if loc else None

    dd = raw.get("default_dataset")
    default_dataset = str(dd).strip() if dd else None

    s = BigQuerySettings(
        project_id=project_id,
        location=location,
        default_dataset=default_dataset,
        auth_mode=auth_mode,
        credentials_path=credentials_path,
        maximum_bytes_billed=maximum_bytes_billed,
        tables=tables,
        config_path=path,
    )
    return s


def bigquery_client(settings: BigQuerySettings) -> bigquery.Client:
    if settings.auth_mode == "service_account":
        creds = service_account.Credentials.from_service_account_file(settings.credentials_path or "")
        return bigquery.Client(
            project=settings.project_id,
            credentials=creds,
            location=settings.location,
        )
    return bigquery.Client(project=settings.project_id, location=settings.location)


def _job_config(settings: BigQuerySettings, params: list[bigquery.ScalarQueryParameter] | None = None) -> bigquery.QueryJobConfig:
    return bigquery.QueryJobConfig(
        query_parameters=params or [],
        use_query_cache=True,
        maximum_bytes_billed=settings.maximum_bytes_billed,
    )


def _run_query(
    client: bigquery.Client,
    settings: BigQuerySettings,
    sql: str,
    params: list[bigquery.ScalarQueryParameter] | None = None,
) -> list[tuple[Any, ...]]:
    job = client.query(sql, job_config=_job_config(settings, params), location=settings.location)
    rows = job.result()
    return [tuple(row.values()) for row in rows]


def _fetch_job_dict(job: bigquery.table.RowIterator) -> dict[str, Any]:
    cols = [s.name for s in job.schema]
    data = [tuple(row.values()) for row in job]
    return {"columns": cols, "rows": data, "row_count": len(data)}


def resolve_logical_table(settings: BigQuerySettings, logical: str) -> str:
    override = settings.tables.get(logical)
    if override:
        return override.strip()
    if not settings.default_dataset:
        raise ValueError(
            f'Set default_dataset in BigQuery YAML or set tables.{logical} to a "project.dataset.table" string.'
        )
    return f"{settings.project_id}.{settings.default_dataset}.{logical}"


def bq_table_sql(ref: str) -> str:
    """Return a single BigQuery Standard SQL table path token: `project.dataset.table`."""
    s = ref.strip()
    if not s:
        raise ValueError("Table reference is empty.")
    parts = s.split(".")
    if len(parts) != 3:
        raise ValueError('Expected fully-qualified "project.dataset.table".')
    return "`" + s.replace("`", "``") + "`"


def qualify_table_arg(settings: BigQuerySettings, table: str) -> str:
    """User/table arg -> project.dataset.table string (no backticks)."""
    t = (table or "").strip()
    if not t:
        raise ValueError("table is empty.")
    parts = t.split(".")
    if len(parts) == 1:
        if not settings.default_dataset:
            raise ValueError("Unqualified table name requires default_dataset in BigQuery YAML.")
        return f"{settings.project_id}.{settings.default_dataset}.{parts[0]}"
    if len(parts) == 2:
        return f"{settings.project_id}.{parts[0]}.{parts[1]}"
    if len(parts) == 3:
        return f"{parts[0]}.{parts[1]}.{parts[2]}"
    raise ValueError('Table must be "table", "dataset.table", or "project.dataset.table".')


def bq_run_readonly_query(settings: BigQuerySettings, sql: str, limit: int) -> dict[str, Any]:
    stmt = _ensure_readonly_single_statement_bq(sql)
    stmt_limited = f"SELECT * FROM ({stmt}) AS _q LIMIT {int(limit)}"
    client = bigquery_client(settings)
    job = client.query(stmt_limited, job_config=_job_config(settings), location=settings.location)
    return _fetch_job_dict(job.result())


_FORBIDDEN_SQL_RE_BQ = re.compile(
    r"\b(insert|update|delete|merge|create|drop|alter|truncate|copy|export|load|grant|revoke)\b",
    re.IGNORECASE,
)
_ALLOWED_PREFIX_RE_BQ = re.compile(r"^\s*(select|with|show|describe|explain)\b", re.IGNORECASE)


def _ensure_readonly_single_statement_bq(sql: str) -> str:
    s = (sql or "").strip()
    if not s:
        raise ValueError("SQL is empty.")
    parts = [p.strip() for p in s.split(";") if p.strip()]
    if len(parts) != 1:
        raise ValueError("Only a single SQL statement is allowed.")
    s = parts[0]
    if not _ALLOWED_PREFIX_RE_BQ.search(s):
        raise ValueError("Only read-only statements are allowed (SELECT/WITH/SHOW/DESCRIBE/EXPLAIN).")
    if _FORBIDDEN_SQL_RE_BQ.search(s):
        raise ValueError("Forbidden SQL keyword detected (write or sensitive statement).")
    return s


def bq_list_tables(settings: BigQuerySettings) -> list[str]:
    if not settings.default_dataset:
        raise ValueError("list_tables for BigQuery requires default_dataset in YAML (dataset to enumerate).")
    client = bigquery_client(settings)
    sql = f"""
    SELECT table_name
    FROM `{settings.project_id}.{settings.default_dataset}`.INFORMATION_SCHEMA.TABLES
    WHERE table_type IN ('BASE TABLE', 'EXTERNAL')
    ORDER BY table_name
    """
    rows = _run_query(client, settings, sql)
    out: list[str] = []
    for (name,) in rows:
        out.append(f"{settings.project_id}.{settings.default_dataset}.{name}")
    return out


def bq_describe_table(settings: BigQuerySettings, table: str) -> dict[str, Any]:
    full = qualify_table_arg(settings, table)
    parts = full.split(".")
    proj, ds, tbl = parts[0], parts[1], parts[2]
    client = bigquery_client(settings)
    sql = f"""
    SELECT column_name, data_type, is_nullable
    FROM `{proj}.{ds}`.INFORMATION_SCHEMA.COLUMNS
    WHERE table_catalog = @p AND table_schema = @d AND table_name = @t
    ORDER BY ordinal_position
    """
    params = [
        bigquery.ScalarQueryParameter("p", "STRING", proj),
        bigquery.ScalarQueryParameter("d", "STRING", ds),
        bigquery.ScalarQueryParameter("t", "STRING", tbl),
    ]
    job = client.query(sql, job_config=_job_config(settings, params), location=settings.location)
    return _fetch_job_dict(job.result())


def bq_check_nulls(settings: BigQuerySettings, table: str) -> dict[str, Any]:
    full = qualify_table_arg(settings, table)
    parts = full.split(".")
    proj, ds, tbl = parts[0], parts[1], parts[2]
    tsql = bq_table_sql(full)
    client = bigquery_client(settings)
    cols_rows = _run_query(
        client,
        settings,
        f"""
        SELECT column_name
        FROM `{proj}.{ds}`.INFORMATION_SCHEMA.COLUMNS
        WHERE table_catalog = @p AND table_schema = @d AND table_name = @t
        ORDER BY ordinal_position
        """,
        [
            bigquery.ScalarQueryParameter("p", "STRING", proj),
            bigquery.ScalarQueryParameter("d", "STRING", ds),
            bigquery.ScalarQueryParameter("t", "STRING", tbl),
        ],
    )
    if not cols_rows:
        raise ValueError(f"Table not found: {table}")
    col_names = [c[0] for c in cols_rows]
    exprs = [f"SUM(IF(`{c.replace('`', '``')}` IS NULL, 1, 0)) AS `{c.replace('`', '``')}`" for c in col_names]
    sql = f"SELECT {', '.join(exprs)} FROM {tsql}"
    res = _run_query(client, settings, sql)[0]
    return {col_names[i]: int(res[i]) for i in range(len(col_names))}


def bq_check_duplicates(settings: BigQuerySettings, table: str, key_columns: list[str], top_n: int) -> dict[str, Any]:
    if not key_columns:
        raise ValueError("key_columns must be a non-empty list.")
    full = qualify_table_arg(settings, table)
    tsql = bq_table_sql(full)
    keys_sql = ", ".join(f"`{c.replace('`', '``')}`" for c in key_columns)
    sql = f"""
    SELECT {keys_sql}, COUNT(*) AS dup_count
    FROM {tsql}
    GROUP BY {keys_sql}
    HAVING COUNT(*) > 1
    ORDER BY dup_count DESC
    LIMIT {int(top_n)}
    """
    client = bigquery_client(settings)
    rows = _run_query(client, settings, sql)
    return {"key_columns": key_columns, "duplicate_groups": rows, "groups_returned": len(rows)}


_BQ_NUMERIC_TYPES = frozenset(
    {
        "INTEGER",
        "INT64",
        "SMALLINT",
        "BIGINT",
        "TINYINT",
        "BYTEINT",
        "FLOAT",
        "FLOAT64",
        "DOUBLE",
        "REAL",
        "NUMERIC",
        "BIGNUMERIC",
    }
)


def bq_profile_column(settings: BigQuerySettings, table: str, column: str, top_n: int) -> dict[str, Any]:
    full = qualify_table_arg(settings, table)
    parts = full.split(".")
    proj, ds, tbl = parts[0], parts[1], parts[2]
    tsql = bq_table_sql(full)
    c = column.strip()
    if not c:
        raise ValueError("column is empty.")
    cq = "`" + c.replace("`", "``") + "`"
    client = bigquery_client(settings)

    basics = _run_query(
        client,
        settings,
        f"""
        SELECT
          COUNT(*) AS total_rows,
          SUM(IF({cq} IS NULL, 1, 0)) AS null_rows,
          COUNT(DISTINCT {cq}) AS distinct_values
        FROM {tsql}
        """,
    )[0]

    top_vals = _run_query(
        client,
        settings,
        f"""
        SELECT {cq} AS value, COUNT(*) AS cnt
        FROM {tsql}
        GROUP BY value
        ORDER BY cnt DESC NULLS LAST
        LIMIT {int(top_n)}
        """,
    )

    dtype_row = _run_query(
        client,
        settings,
        f"""
        SELECT data_type
        FROM `{proj}.{ds}`.INFORMATION_SCHEMA.COLUMNS
        WHERE table_catalog = @p AND table_schema = @d AND table_name = @t AND column_name = @col
        """,
        [
            bigquery.ScalarQueryParameter("p", "STRING", proj),
            bigquery.ScalarQueryParameter("d", "STRING", ds),
            bigquery.ScalarQueryParameter("t", "STRING", tbl),
            bigquery.ScalarQueryParameter("col", "STRING", c),
        ],
    )
    data_type = dtype_row[0][0] if dtype_row else None

    numeric_stats: dict[str, Any] | None = None
    if data_type and data_type.upper() in _BQ_NUMERIC_TYPES:
        row = _run_query(
            client,
            settings,
            f"""
            SELECT
              MIN({cq}) AS min,
              MAX({cq}) AS max,
              AVG({cq}) AS avg,
              STDDEV_SAMP({cq}) AS stddev
            FROM {tsql}
            """,
        )[0]
        numeric_stats = {"min": row[0], "max": row[1], "avg": row[2], "stddev": row[3]}

    return {
        "table": table,
        "column": column,
        "data_type": data_type,
        "total_rows": int(basics[0]),
        "null_rows": int(basics[1]),
        "distinct_values": int(basics[2]),
        "top_values": top_vals,
        "numeric_stats": numeric_stats,
    }


def bq_pipeline_health_summary(settings: BigQuerySettings, days: int) -> dict[str, Any]:
    runs = resolve_logical_table(settings, "pipeline_runs")
    dq = resolve_logical_table(settings, "data_quality_log")
    runs_sql = bq_table_sql(runs)
    dq_sql = bq_table_sql(dq)
    since = datetime.now(timezone.utc) - timedelta(days=int(days))
    since = since.replace(microsecond=0)

    client = bigquery_client(settings)
    params = [bigquery.ScalarQueryParameter("since", "TIMESTAMP", since)]

    by_status = _run_query(
        client,
        settings,
        f"""
        SELECT status, COUNT(*) AS runs
        FROM {runs_sql}
        WHERE started_at >= @since
        GROUP BY status
        ORDER BY runs DESC
        """,
        params,
    )

    latest_per_pipeline = _run_query(
        client,
        settings,
        f"""
        SELECT pipeline_name, run_id, started_at, ended_at, status, rows_processed, error_message
        FROM (
          SELECT *,
            ROW_NUMBER() OVER (PARTITION BY pipeline_name ORDER BY started_at DESC) AS rn
          FROM {runs_sql}
        )
        WHERE rn = 1
        ORDER BY pipeline_name
        """,
    )

    dq_recent_failures = _run_query(
        client,
        settings,
        f"""
        SELECT severity, table_name, check_name, checked_at, passed, failed_rows, notes
        FROM {dq_sql}
        WHERE checked_at >= @since
          AND passed = FALSE
        ORDER BY checked_at DESC
        LIMIT 50
        """,
        params,
    )

    return {
        "window_days": int(days),
        "runs_by_status": by_status,
        "latest_run_per_pipeline": latest_per_pipeline,
        "recent_dq_failures": dq_recent_failures,
    }


def bq_row_count_trend(settings: BigQuerySettings, table: str, date_column: str, days: int) -> dict[str, Any]:
    full = qualify_table_arg(settings, table)
    tsql = bq_table_sql(full)
    dc = "`" + date_column.strip().replace("`", "``") + "`"
    client = bigquery_client(settings)
    sql = f"""
    SELECT
      DATE({dc}) AS day,
      COUNT(*) AS rows
    FROM {tsql}
    WHERE {dc} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {int(days)} DAY)
    GROUP BY day
    ORDER BY day
    """
    rows = _run_query(client, settings, sql)
    return {"table": table, "date_column": date_column, "days": int(days), "trend": rows}


def bq_detect_anomalies(settings: BigQuerySettings, table: str, metric_column: str, top_k: int) -> dict[str, Any]:
    full = qualify_table_arg(settings, table)
    tsql = bq_table_sql(full)
    mc = "`" + metric_column.strip().replace("`", "``") + "`"
    client = bigquery_client(settings)
    sql = f"""
    WITH base AS (
      SELECT {mc} AS x
      FROM {tsql}
      WHERE {mc} IS NOT NULL
    ),
    med AS (
      SELECT APPROX_QUANTILES(x, 100)[OFFSET(50)] AS med
      FROM base
    ),
    stats AS (
      SELECT
        (SELECT med FROM med) AS med,
        APPROX_QUANTILES(ABS(base.x - (SELECT med FROM med)), 100)[OFFSET(50)] AS mad
      FROM base
    )
    SELECT value, median, mad, robust_z
    FROM (
      SELECT
        b.x AS value,
        s.med AS median,
        s.mad AS mad,
        CASE WHEN s.mad = 0 THEN NULL ELSE 0.6745 * (b.x - s.med) / s.mad END AS robust_z
      FROM base b
      CROSS JOIN stats s
    )
    ORDER BY ABS(robust_z) DESC NULLS LAST
    LIMIT {int(top_k)}
    """
    rows = _run_query(client, settings, sql)
    return {"table": table, "metric_column": metric_column, "top_k": int(top_k), "outliers": rows}
