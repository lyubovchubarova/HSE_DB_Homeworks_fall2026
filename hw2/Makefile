PYTHON ?= python3.11
PG_BIN ?= /opt/homebrew/opt/postgresql@14/bin
PG_DATA := $(CURDIR)/.runtime/postgres
PG_LOG := $(CURDIR)/.runtime/postgres.log
AIRFLOW_HOME := $(CURDIR)/.runtime/airflow
INPUT_FILE ?= data/source/kudago_spb_summer_2026.json
DBT := .venv/bin/dbt
DBT_FLAGS := --project-dir . --profiles-dir .
AIRFLOW_ENV := AIRFLOW_HOME="$(AIRFLOW_HOME)" AIRFLOW__CORE__DAGS_FOLDER="$(CURDIR)/dags" AIRFLOW__CORE__LOAD_EXAMPLES=False

.PHONY: setup start stop download load transform quality publish pipeline test diagnostics airflow airflow-test

setup:
	$(PYTHON) -m venv .venv
	.venv/bin/pip install -r requirements.txt
	$(PYTHON) -m venv .airflow_venv
	.airflow_venv/bin/pip install -r requirements-airflow.txt \
		--constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt
	$(MAKE) start
	$(AIRFLOW_ENV) .airflow_venv/bin/airflow db migrate

start:
	mkdir -p .runtime
	@if "$(PG_BIN)/pg_isready" -h 127.0.0.1 -p 55432 >/dev/null 2>&1; then \
		echo "Using PostgreSQL already running on 127.0.0.1:55432"; \
	else \
		if [ ! -f "$(PG_DATA)/PG_VERSION" ]; then \
			"$(PG_BIN)/initdb" -D "$(PG_DATA)" --auth=trust --username=postgres; \
		fi; \
		"$(PG_BIN)/pg_ctl" -D "$(PG_DATA)" -l "$(PG_LOG)" -o "-p 55432" start; \
	fi
	@if [ "$$("$(PG_BIN)/psql" -h 127.0.0.1 -p 55432 -U postgres -d postgres -Atc "select 1 from pg_database where datname='kudago_dwh'")" != "1" ]; then \
		"$(PG_BIN)/createdb" -h 127.0.0.1 -p 55432 -U postgres kudago_dwh; \
	fi
	@echo "PostgreSQL is ready on 127.0.0.1:55432"

stop:
	@if [ -f "$(PG_DATA)/PG_VERSION" ] && "$(PG_BIN)/pg_ctl" -D "$(PG_DATA)" status >/dev/null 2>&1; then \
		"$(PG_BIN)/pg_ctl" -D "$(PG_DATA)" stop; \
	fi

download:
	.venv/bin/python scripts/download.py

load:
	.venv/bin/python scripts/load_raw.py "$(INPUT_FILE)" --replace-source

transform:
	$(DBT) seed --full-refresh $(DBT_FLAGS)
	$(DBT) run --full-refresh $(DBT_FLAGS)

quality:
	$(DBT) test $(DBT_FLAGS)

publish:
	.venv/bin/python scripts/publish.py

pipeline:
	$(MAKE) load INPUT_FILE="$(INPUT_FILE)"
	$(MAKE) transform
	$(MAKE) quality
	$(MAKE) publish

test:
	$(MAKE) load
	$(DBT) build --full-refresh $(DBT_FLAGS)

diagnostics:
	.venv/bin/python scripts/run_diagnostics.py

airflow:
	$(AIRFLOW_ENV) .airflow_venv/bin/airflow standalone

airflow-test:
	$(AIRFLOW_ENV) .airflow_venv/bin/airflow dags test kudago_spb_elt 2026-06-01
