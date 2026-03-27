.PHONY: setup run backfill test smoke-test db-reset

setup:
	python -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

run:
	python -m pipeline.run_daily

backfill:
	python -m scripts.backfill

smoke-test:
	python -m scripts.test_api

test:
	python -m pytest tests/ -v

db-setup:
	psql "$$DATABASE_URL" -f db/schema.sql
	psql "$$DATABASE_URL" -f db/seed_sources.sql
	psql "$$DATABASE_URL" -f db/seed_topics.sql

db-reset:
	psql "$$DATABASE_URL" -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
	$(MAKE) db-setup
