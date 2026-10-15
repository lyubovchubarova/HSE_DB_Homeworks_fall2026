PYTHON ?= python3.11
PG_BIN ?= /opt/homebrew/opt/postgresql@14/bin
PG_DATA := $(CURDIR)/.runtime/postgres
PG_LOG := $(CURDIR)/.runtime/postgres.log
AIRFLOW_HOME := $(CURDIR)/.runtime/airflow
SNAPSHOT_DATE ?= $(shell date +%F)
INPUT_FILE ?= data/source/trudvsem_vacancies_$(SNAPSHOT_DATE).jsonl.gz
DBT := .venv/bin/dbt
DBT_FLAGS := --project-dir . --profiles-dir .
AIRFLOW_ENV := AIRFLOW_HOME="$(AIRFLOW_HOME)" AIRFLOW__CORE__DAGS_FOLDER="$(CURDIR)/dags" AIRFLOW__CORE__LOAD_EXAMPLES=False

.PHONY: setup start stop download load dbt-build publish pipeline replay unit test diagnostics airflow airflow-test

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
			LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8 "$(PG_BIN)/initdb" \
				-D "$(PG_DATA)" --auth=trust --username=postgres; \
		fi; \
		"$(PG_BIN)/pg_ctl" -D "$(PG_DATA)" -l "$(PG_LOG)" -o "-p 55432" start; \
	fi
	@if [ "$$("$(PG_BIN)/psql" -h 127.0.0.1 -p 55432 -U postgres -d postgres -Atc "select 1 from pg_database where datname='vacancy_dwh'")" != "1" ]; then \
		"$(PG_BIN)/createdb" -h 127.0.0.1 -p 55432 -U postgres vacancy_dwh; \
	fi
	@echo "PostgreSQL is ready on 127.0.0.1:55432"

stop:
	@if [ -f "$(PG_DATA)/PG_VERSION" ] && "$(PG_BIN)/pg_ctl" -D "$(PG_DATA)" status >/dev/null 2>&1; then \
		"$(PG_BIN)/pg_ctl" -D "$(PG_DATA)" stop; \
	fi

download:
	.venv/bin/python scripts/download.py --snapshot-date "$(SNAPSHOT_DATE)" --workers 8 --output "$(INPUT_FILE)"

load:
	.venv/bin/python scripts/load_raw.py "$(INPUT_FILE)"

dbt-build:
	$(DBT) build --full-refresh $(DBT_FLAGS)

publish:
	.venv/bin/python scripts/publish.py

pipeline:
	$(MAKE) download SNAPSHOT_DATE="$(SNAPSHOT_DATE)" INPUT_FILE="$(INPUT_FILE)"
	$(MAKE) load INPUT_FILE="$(INPUT_FILE)"
	$(MAKE) dbt-build
	$(MAKE) publish

replay:
	$(MAKE) load SNAPSHOT_DATE="$(SNAPSHOT_DATE)" INPUT_FILE="$(INPUT_FILE)"
	$(MAKE) dbt-build
	$(MAKE) publish

unit:
	.venv/bin/python -m unittest discover -s unit_tests -v

test:
	$(MAKE) unit
	$(MAKE) load INPUT_FILE="$(INPUT_FILE)"
	$(MAKE) dbt-build

diagnostics:
	.venv/bin/python scripts/run_diagnostics.py

airflow:
	$(AIRFLOW_ENV) .airflow_venv/bin/airflow standalone

airflow-test:
	$(AIRFLOW_ENV) .airflow_venv/bin/airflow dags test trudvsem_vacancy_elt "$(SNAPSHOT_DATE)"
