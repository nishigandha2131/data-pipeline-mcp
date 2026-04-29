.PHONY: install demo seed-bq seed-bq-recreate

install:
	uv sync

demo:
	uv run python sample_data.py

seed-bq:
	uv run python scripts/seed_bigquery_demo.py --config config/bigquery.local.yaml

seed-bq-recreate:
	uv run python scripts/seed_bigquery_demo.py --config config/bigquery.local.yaml --mode recreate
