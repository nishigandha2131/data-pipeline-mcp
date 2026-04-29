# data-pipeline-mcp

An MCP (Model Context Protocol) **stdio** server that lets any MCP client (e.g. Cursor, Claude Desktop) explore pipeline data in **DuckDB** (local demo file) or **BigQuery** (your project), with the same tools routed by a `backend` parameter.

## What’s included

- **`sample_data.py`**: generates a demo DuckDB database with intentionally messy data (nulls, duplicates, spikes/outliers)
- **`server.py`**: FastMCP server exposing analysis tools over **stdio** transport (DuckDB + BigQuery)
- **`bigquery_support.py`**: loads YAML config and runs BigQuery jobs
- **`config/bigquery.example.yaml`**: template for BigQuery (copy to `config/bigquery.local.yaml`, gitignored)
- **`Makefile`**: `make install` / `make demo` shortcuts
- **`.cursor/mcp.json`**: project-level MCP server config for Cursor

## Prerequisites

- **Python** (project uses `uv` + `pyproject.toml`)
- **uv** installed (`uv` manages the venv and dependencies)

## Quickstart

### 1) Install deps

```bash
uv sync
```

Or:

```bash
make install
```

### 2) Generate the demo database

By default this writes `pipeline_demo.duckdb` in the repo root.

```bash
uv run python sample_data.py
```

Or:

```bash
make demo
```

You can also control the path with an env var or flag:

```bash
DB_PATH=my_demo.duckdb uv run python sample_data.py
uv run python sample_data.py --db-path my_demo.duckdb --days 30 --seed 7
```

### 3) Run the MCP server (stdio)

The server reads `DB_PATH` (defaults to `pipeline_demo.duckdb`).

```bash
uv run server.py
```

## Using it in Cursor

This repo includes `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "data-pipeline-mcp": {
      "command": "uv",
      "args": ["run", "server.py"]
    }
  }
}
```

Open the folder in Cursor and reload/restart if needed so it discovers the MCP server.

## BigQuery setup (optional)

**Important:** `config/bigquery.local.yaml` is **your local config** (it may include project details). It is **gitignored** and should **not** be committed.

1. Copy the example config and edit it:

```bash
cp config/bigquery.example.yaml config/bigquery.local.yaml
```

2. Set **`project_id`**, **`default_dataset`**, and **`auth`** (`adc` or `service_account` + `credentials_path`).

3. Point the server at the file (default path is `config/bigquery.local.yaml`):

```bash
export BQ_CONFIG_PATH=config/bigquery.local.yaml
```

**What is ADC?** **Application Default Credentials** — a standard way for Google client libraries to find credentials on your machine. After `gcloud auth application-default login`, libraries use `~/.config/gcloud/application_default_credentials.json` (no downloaded service-account key file). If `gcloud` warned about a missing **quota project**, run:

```bash
gcloud auth application-default set-quota-project YOUR_PROJECT_ID
```

4. Call tools with **`backend="bigquery"`** (see below). DuckDB tools keep the default **`backend="duckdb"`** and do not need YAML.

For Cursor, set `BQ_CONFIG_PATH` in `.cursor/mcp.json` under `env` (see that file in this repo) so the MCP process sees your YAML.

## Create BigQuery demo tables (optional)

If your BigQuery dataset is empty, you can seed it with the same demo tables used by the DuckDB version:

```bash
cp config/bigquery.example.yaml config/bigquery.local.yaml
# edit project_id/default_dataset/auth (adc)
export BQ_CONFIG_PATH=config/bigquery.local.yaml

uv run python scripts/seed_bigquery_demo.py --config config/bigquery.local.yaml --days 30 --seed 7
```

Or:

```bash
make seed-bq
```

### What `seed-bq` does

`seed-bq` is a **Makefile shortcut** for:

```bash
uv run python scripts/seed_bigquery_demo.py --config config/bigquery.local.yaml --mode append
```

It uses your BigQuery settings from `config/bigquery.local.yaml` (project + dataset + auth) and then:

- Creates these tables **if they don’t exist**:
  - `pipeline_runs`
  - `pipeline_metrics`
  - `data_quality_log`
- Inserts a batch of “messy” demo rows (nulls, duplicates, outliers) for testing the tools.

### Where the data goes

The tables are created inside **your BigQuery dataset**:

- `{project_id}.{default_dataset}.pipeline_runs`
- `{project_id}.{default_dataset}.pipeline_metrics`
- `{project_id}.{default_dataset}.data_quality_log`

These values come from `config/bigquery.local.yaml`.

### Why it’s useful / when you need it

You only need `seed-bq` if you want a **fully working BigQuery demo** of the pipeline tools and your dataset is empty.

Without these demo tables:
- `list_tables(backend="bigquery")` may return an empty list, and
- `pipeline_health_summary(backend="bigquery")` will fail because it expects `pipeline_runs` and `data_quality_log` to exist (or be mapped via YAML `tables:`).

### If you already have existing data

The seeding script is **append-only**. If the tables already exist, it will **append another batch of demo rows**.

That’s great for a quick demo, but it may be **undesirable** if:
- You already store real production data in those tables, or
- You want deterministic row counts (append will grow the tables each time you run it).

**Recommended patterns:**

- **Use a dedicated demo dataset** (safest): create a dataset like `mcp_demo` and point `default_dataset` there before running `seed-bq`.
- **Skip seeding and query your real tables**: set `tables.pipeline_runs`, `tables.pipeline_metrics`, and `tables.data_quality_log` in `config/bigquery.local.yaml` to your real fully-qualified table names (`project.dataset.table`), and don’t run `seed-bq`.

If you want a deterministic “reset” behavior (drop & recreate), run:

```bash
uv run python scripts/seed_bigquery_demo.py --config config/bigquery.local.yaml --mode recreate
```

Or:

```bash
make seed-bq-recreate
```

### Append vs recreate (reset)

- **Append mode** (`make seed-bq`, default): creates tables if missing, then **adds more rows** each time you run it.
- **Recreate mode** (`make seed-bq-recreate`): **drops the 3 demo tables** and recreates them, then inserts a fresh batch.

### Safety notes

- **Do not run `seed-bq` in a dataset that contains production tables with the same names** unless you explicitly want to mix demo rows into those tables.
- Prefer using a separate dataset like `mcp_demo` for demos.

## Available MCP tools

Every tool accepts **`backend`**: `"duckdb"` (default) or `"bigquery"`. For BigQuery, pass **`bigquery_config_path`** or set **`BQ_CONFIG_PATH`**.

- **`run_readonly_query(sql, backend, db_path, bigquery_config_path, limit)`**: read-only, single-statement query runner (results limited)
- **`list_tables(backend, db_path, bigquery_config_path)`**
- **`describe_table(table, backend, db_path, bigquery_config_path)`**
- **`check_nulls(table, backend, db_path, bigquery_config_path)`**
- **`check_duplicates(table, key_columns, backend, db_path, bigquery_config_path, top_n)`**
- **`profile_column(table, column, backend, db_path, bigquery_config_path, top_n)`**
- **`pipeline_health_summary(backend, db_path, bigquery_config_path, days)`**
- **`row_count_trend(table, date_column, days, backend, db_path, bigquery_config_path)`**
- **`detect_anomalies(table, metric_column, backend, db_path, bigquery_config_path, top_k)`**

BigQuery **`pipeline_health_summary`** resolves tables from YAML: either `tables.pipeline_runs` / `tables.data_quality_log` as full `project.dataset.table`, or `null` to use `{project_id}.{default_dataset}.{name}`.

## Example workflow (good first demo)

Try these in your MCP client (Cursor/Claude):

- `list_tables()`
- `pipeline_health_summary()`
- `check_duplicates("pipeline_runs", ["run_id"])`
- `detect_anomalies("pipeline_metrics", "metric_value")`

Then drill in with:

- `run_readonly_query("select status, count(*) runs from pipeline_runs group by 1 order by runs desc")`
- `run_readonly_query("select * from pipeline_runs where ended_at is null")`
- `run_readonly_query("select metric_name, unit, count(*) from pipeline_metrics group by 1,2 order by 3 desc")`

