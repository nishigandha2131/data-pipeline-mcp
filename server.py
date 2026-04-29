import os
import re
from datetime import datetime, timedelta
from typing import Any

import duckdb
from fastmcp import FastMCP

import bigquery_support as bq


mcp = FastMCP("data-pipeline-mcp")


def _quote_ident(ident: str) -> str:
    s = (ident or "").strip()
    if not s:
        raise ValueError("Identifier is empty.")
    return '"' + s.replace('"', '""') + '"'


def _default_db_path(db_path: str | None) -> str:
    return db_path or os.getenv("DB_PATH", "pipeline_demo.duckdb")


def _normalize_backend(backend: str) -> str:
    b = (backend or "duckdb").strip().lower()
    if b not in {"duckdb", "bigquery"}:
        raise ValueError('backend must be "duckdb" or "bigquery".')
    return b


def _split_table_name(table: str) -> tuple[str | None, str]:
    t = (table or "").strip()
    if not t:
        raise ValueError("table is empty.")
    if "." in t:
        schema, name = t.split(".", 1)
        schema = schema.strip() or None
        name = name.strip()
        if not name:
            raise ValueError("table name is empty.")
        return schema, name
    return None, t


def _escape_qualified_identifier(table: str) -> str:
    schema, name = _split_table_name(table)
    if schema:
        return f"{_quote_ident(schema)}.{_quote_ident(name)}"
    return _quote_ident(name)


def _quote_sql_string(value: str) -> str:
    # DuckDB uses single-quoted string literals; escape embedded quotes.
    return "'" + value.replace("'", "''") + "'"


_FORBIDDEN_SQL_RE = re.compile(
    r"\b(insert|update|delete|merge|create|drop|alter|truncate|copy|attach|detach|export|import|pragma|set|call)\b",
    re.IGNORECASE,
)

_ALLOWED_PREFIX_RE = re.compile(r"^\s*(select|with|show|describe|explain)\b", re.IGNORECASE)


def _ensure_readonly_single_statement(sql: str) -> str:
    s = (sql or "").strip()
    if not s:
        raise ValueError("SQL is empty.")

    # Very small "single statement" guard: allow at most one trailing ';'.
    parts = [p.strip() for p in s.split(";") if p.strip()]
    if len(parts) != 1:
        raise ValueError("Only a single SQL statement is allowed.")
    s = parts[0]

    if not _ALLOWED_PREFIX_RE.search(s):
        raise ValueError("Only read-only statements are allowed (SELECT/WITH/SHOW/DESCRIBE/EXPLAIN).")
    if _FORBIDDEN_SQL_RE.search(s):
        raise ValueError("Forbidden SQL keyword detected (write or config statement).")
    return s


def _connect(db_path: str, read_only: bool) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path, read_only=read_only)


def _fetch_as_dict(con: duckdb.DuckDBPyConnection, sql: str, params: tuple[Any, ...] | None = None) -> dict[str, Any]:
    rel = con.execute(sql, params or ())
    cols = [d[0] for d in rel.description]
    rows = rel.fetchall()
    return {"columns": cols, "rows": rows, "row_count": len(rows)}


@mcp.tool()
def run_readonly_query(
    sql: str,
    backend: str = "duckdb",
    db_path: str | None = None,
    bigquery_config_path: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    """
    Run a single read-only SQL statement and return results.

    Use backend="duckdb" (default) with db_path, or backend="bigquery" with YAML config
    (bigquery_config_path argument or BQ_CONFIG_PATH env).
    """
    be = _normalize_backend(backend)
    if be == "bigquery":
        settings = bq.load_bigquery_settings(bigquery_config_path)
        return bq.bq_run_readonly_query(settings, sql, limit)

    db = _default_db_path(db_path)
    stmt = _ensure_readonly_single_statement(sql)

    # Keep responses bounded.
    stmt_limited = f"select * from ({stmt}) as _q limit {int(limit)}"

    con = _connect(db, read_only=True)
    try:
        return _fetch_as_dict(con, stmt_limited)
    finally:
        con.close()


@mcp.tool()
def list_tables(
    backend: str = "duckdb",
    db_path: str | None = None,
    bigquery_config_path: str | None = None,
) -> list[str]:
    be = _normalize_backend(backend)
    if be == "bigquery":
        settings = bq.load_bigquery_settings(bigquery_config_path)
        return bq.bq_list_tables(settings)

    db = _default_db_path(db_path)
    con = _connect(db, read_only=True)
    try:
        rows = con.execute(
            """
            select table_schema || '.' || table_name
            from information_schema.tables
            where table_type = 'BASE TABLE'
              and table_schema not in ('information_schema', 'pg_catalog')
            order by 1;
            """
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()


@mcp.tool()
def describe_table(
    table: str,
    backend: str = "duckdb",
    db_path: str | None = None,
    bigquery_config_path: str | None = None,
) -> dict[str, Any]:
    be = _normalize_backend(backend)
    if be == "bigquery":
        settings = bq.load_bigquery_settings(bigquery_config_path)
        return bq.bq_describe_table(settings, table)

    db = _default_db_path(db_path)
    con = _connect(db, read_only=True)
    try:
        # PRAGMA can't be prepared with parameters; keep it safe via string-literal quoting.
        schema, name = _split_table_name(table)
        target = f"{schema}.{name}" if schema else name
        return _fetch_as_dict(con, f"pragma table_info({_quote_sql_string(target)})")
    finally:
        con.close()


@mcp.tool()
def check_nulls(
    table: str,
    backend: str = "duckdb",
    db_path: str | None = None,
    bigquery_config_path: str | None = None,
) -> dict[str, Any]:
    be = _normalize_backend(backend)
    if be == "bigquery":
        settings = bq.load_bigquery_settings(bigquery_config_path)
        return bq.bq_check_nulls(settings, table)

    db = _default_db_path(db_path)
    con = _connect(db, read_only=True)
    try:
        schema, name = _split_table_name(table)
        schema = schema or "main"
        cols = con.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = ?
              and table_name = ?
            order by ordinal_position;
            """,
            (schema, name),
        ).fetchall()

        if not cols:
            raise ValueError(f"Table not found: {table}")

        exprs = [f"sum(case when {_quote_ident(c[0])} is null then 1 else 0 end) as {_quote_ident(c[0])}" for c in cols]
        sql = f"select {', '.join(exprs)} from {_escape_qualified_identifier(table)}"
        res = con.execute(sql).fetchone()
        return {cols[i][0]: int(res[i]) for i in range(len(cols))}
    finally:
        con.close()


@mcp.tool()
def check_duplicates(
    table: str,
    key_columns: list[str],
    backend: str = "duckdb",
    db_path: str | None = None,
    bigquery_config_path: str | None = None,
    top_n: int = 25,
) -> dict[str, Any]:
    be = _normalize_backend(backend)
    if be == "bigquery":
        settings = bq.load_bigquery_settings(bigquery_config_path)
        return bq.bq_check_duplicates(settings, table, key_columns, top_n)

    db = _default_db_path(db_path)
    if not key_columns:
        raise ValueError("key_columns must be a non-empty list.")

    con = _connect(db, read_only=True)
    try:
        keys_sql = ", ".join(_quote_ident(c) for c in key_columns)
        sql = f"""
        select {keys_sql}, count(*) as dup_count
        from {_escape_qualified_identifier(table)}
        group by {keys_sql}
        having count(*) > 1
        order by dup_count desc
        limit {int(top_n)}
        """
        rows = con.execute(sql).fetchall()
        return {"key_columns": key_columns, "duplicate_groups": rows, "groups_returned": len(rows)}
    finally:
        con.close()


@mcp.tool()
def profile_column(
    table: str,
    column: str,
    backend: str = "duckdb",
    db_path: str | None = None,
    bigquery_config_path: str | None = None,
    top_n: int = 10,
) -> dict[str, Any]:
    be = _normalize_backend(backend)
    if be == "bigquery":
        settings = bq.load_bigquery_settings(bigquery_config_path)
        return bq.bq_profile_column(settings, table, column, top_n)

    db = _default_db_path(db_path)
    con = _connect(db, read_only=True)
    try:
        schema, name = _split_table_name(table)
        schema = schema or "main"
        t = _escape_qualified_identifier(table)
        c = _quote_ident(column)

        basics = con.execute(
            f"""
            select
              count(*) as total_rows,
              sum(case when {c} is null then 1 else 0 end) as null_rows,
              count(distinct {c}) as distinct_values
            from {t}
            """
        ).fetchone()

        top_vals = con.execute(
            f"""
            select {c} as value, count(*) as cnt
            from {t}
            group by 1
            order by cnt desc nulls last
            limit {int(top_n)}
            """
        ).fetchall()

        numeric = con.execute(
            """
            select data_type
            from information_schema.columns
            where table_schema = ?
              and table_name = ?
              and column_name = ?
            """,
            (schema, name, column),
        ).fetchone()
        data_type = numeric[0] if numeric else None

        numeric_stats: dict[str, Any] | None = None
        if data_type and data_type.lower() in {
            "tinyint",
            "smallint",
            "integer",
            "bigint",
            "hugeint",
            "utinyint",
            "usmallint",
            "uinteger",
            "ubigint",
            "float",
            "real",
            "double",
            "decimal",
        }:
            row = con.execute(
                f"""
                select
                  min({c}) as min,
                  max({c}) as max,
                  avg({c}) as avg,
                  stddev_samp({c}) as stddev
                from {t}
                """
            ).fetchone()
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
    finally:
        con.close()


@mcp.tool()
def pipeline_health_summary(
    backend: str = "duckdb",
    db_path: str | None = None,
    bigquery_config_path: str | None = None,
    days: int = 14,
) -> dict[str, Any]:
    be = _normalize_backend(backend)
    if be == "bigquery":
        settings = bq.load_bigquery_settings(bigquery_config_path)
        return bq.bq_pipeline_health_summary(settings, days)

    db = _default_db_path(db_path)
    con = _connect(db, read_only=True)
    try:
        since = datetime.utcnow() - timedelta(days=int(days))
        since = since.replace(microsecond=0)

        by_status = con.execute(
            """
            select status, count(*) as runs
            from pipeline_runs
            where started_at >= ?
            group by 1
            order by runs desc;
            """,
            (since,),
        ).fetchall()

        latest_per_pipeline = con.execute(
            """
            select pipeline_name, run_id, started_at, ended_at, status, rows_processed, error_message
            from (
              select *,
                     row_number() over (partition by pipeline_name order by started_at desc) as rn
              from pipeline_runs
            )
            where rn = 1
            order by pipeline_name;
            """
        ).fetchall()

        dq_recent_failures = con.execute(
            """
            select severity, table_name, check_name, checked_at, passed, failed_rows, notes
            from data_quality_log
            where checked_at >= ?
              and passed = false
            order by checked_at desc
            limit 50;
            """,
            (since,),
        ).fetchall()

        return {
            "window_days": int(days),
            "runs_by_status": by_status,
            "latest_run_per_pipeline": latest_per_pipeline,
            "recent_dq_failures": dq_recent_failures,
        }
    finally:
        con.close()


@mcp.tool()
def row_count_trend(
    table: str,
    date_column: str,
    days: int = 30,
    backend: str = "duckdb",
    db_path: str | None = None,
    bigquery_config_path: str | None = None,
) -> dict[str, Any]:
    be = _normalize_backend(backend)
    if be == "bigquery":
        settings = bq.load_bigquery_settings(bigquery_config_path)
        return bq.bq_row_count_trend(settings, table, date_column, days)

    db = _default_db_path(db_path)
    con = _connect(db, read_only=True)
    try:
        t = _escape_qualified_identifier(table)
        dc = _quote_ident(date_column)
        sql = f"""
        select
          date_trunc('day', {dc})::date as day,
          count(*) as rows
        from {t}
        where {dc} >= (current_timestamp - interval '{int(days)} days')
        group by 1
        order by 1;
        """
        rows = con.execute(sql).fetchall()
        return {"table": table, "date_column": date_column, "days": int(days), "trend": rows}
    finally:
        con.close()


@mcp.tool()
def detect_anomalies(
    table: str,
    metric_column: str,
    backend: str = "duckdb",
    db_path: str | None = None,
    bigquery_config_path: str | None = None,
    top_k: int = 10,
) -> dict[str, Any]:
    """
    Detect outliers in a metric using robust z-score (median/MAD).
    """
    be = _normalize_backend(backend)
    if be == "bigquery":
        settings = bq.load_bigquery_settings(bigquery_config_path)
        return bq.bq_detect_anomalies(settings, table, metric_column, top_k)

    db = _default_db_path(db_path)
    con = _connect(db, read_only=True)
    try:
        t = _escape_qualified_identifier(table)
        mc = _quote_ident(metric_column)

        # Robust z-score: z = 0.6745 * (x - median) / MAD
        sql = f"""
        with base as (
          select {mc} as x
          from {t}
          where {mc} is not null
        ),
        med as (
          select median(x) as med
          from base
        ),
        stats as (
          select
            any_value(med.med) as med,
            median(abs(base.x - med.med)) as mad
          from base
          cross join med
        )
        select
          b.x as value,
          s.med as median,
          s.mad as mad,
          case when s.mad = 0 then null else 0.6745 * (b.x - s.med) / s.mad end as robust_z
        from base b
        cross join stats s
        order by abs(robust_z) desc nulls last
        limit {int(top_k)};
        """
        rows = con.execute(sql).fetchall()
        return {"table": table, "metric_column": metric_column, "top_k": int(top_k), "outliers": rows}
    finally:
        con.close()


if __name__ == "__main__":
    mcp.run(transport="stdio")

