PYTHON ?= python3
PG_BIN ?= $(shell brew --prefix postgresql@14 2>/dev/null)/bin
DOCKER_CONFIG_DIR := $(CURDIR)/.runtime/docker-config
DOCKER_SOCKET := unix://$(HOME)/.colima/default/docker.sock
DOCKER_ENV := DOCKER_CONFIG="$(DOCKER_CONFIG_DIR)" DOCKER_HOST="$(DOCKER_SOCKET)"
COMPOSE := $(DOCKER_ENV) docker-compose
SNAPSHOT_DATE ?= $(shell date +%F)
RAW_FILE ?= data/source/trudvsem_vacancies_$(SNAPSHOT_DATE).jsonl.gz
REAL_CSV ?= data/trudvsem_vacancies.csv
CONTROL_CSV ?= data/control_vacancies.csv
REAL_METADATA ?= data/trudvsem_metadata.json

.PHONY: docker-setup setup download build-csv up wait prepare benchmark replication backup-restore mvcc equality render test all replay stop destroy

docker-setup:
	brew list docker docker-compose colima postgresql@14 >/dev/null 2>&1 || \
		brew install docker docker-compose colima postgresql@14
	colima status >/dev/null 2>&1 || colima start --cpu 4 --memory 8 --disk 20

setup: docker-setup
	$(PYTHON) -m venv .venv
	.venv/bin/pip install -r requirements.txt
	mkdir -p "$(DOCKER_CONFIG_DIR)"
	printf '{}\n' > "$(DOCKER_CONFIG_DIR)/config.json"

download:
	.venv/bin/python ../hw2/scripts/download.py \
		--snapshot-date "$(SNAPSHOT_DATE)" \
		--workers 8 \
		--output "$(RAW_FILE)"
	$(MAKE) build-csv RAW_FILE="$(RAW_FILE)" REAL_CSV="$(REAL_CSV)" CONTROL_CSV="$(CONTROL_CSV)" REAL_METADATA="$(REAL_METADATA)"

build-csv:
	.venv/bin/python scripts/build_vacancy_csv.py \
		--input "$(RAW_FILE)" \
		--output "$(REAL_CSV)" \
		--control-output "$(CONTROL_CSV)" \
		--metadata "$(REAL_METADATA)"

up:
	mkdir -p "$(DOCKER_CONFIG_DIR)"
	printf '{}\n' > "$(DOCKER_CONFIG_DIR)/config.json"
	$(COMPOSE) up -d --build
	$(MAKE) wait

wait:
	PYTHONPATH=scripts .venv/bin/python -c \
		"from cluster import wait_for_roles; print([(n['name'], n['role']) for n in wait_for_roles()])"

prepare:
	HW3_REAL_CSV="$(CURDIR)/$(REAL_CSV)" \
	HW3_CONTROL_CSV="$(CURDIR)/$(CONTROL_CSV)" \
	HW3_REAL_METADATA="$(CURDIR)/$(REAL_METADATA)" \
		.venv/bin/python scripts/prepare.py

benchmark:
	.venv/bin/python scripts/benchmark.py

replication:
	.venv/bin/python scripts/replication_demo.py

backup-restore:
	PG_BIN="$(PG_BIN)" .venv/bin/python scripts/backup_restore.py

mvcc:
	.venv/bin/python scripts/mvcc_demo.py

equality:
	@port="$$(PYTHONPATH=scripts .venv/bin/python -c \
		"from cluster import wait_for_roles; print(wait_for_roles()[0]['port'])")"; \
	PGPASSWORD=postgres "$(PG_BIN)/psql" -h 127.0.0.1 -p "$$port" \
		-U postgres -d hw3 -f sql/05_equality.sql | tee evidence/equality.txt

render:
	.venv/bin/python scripts/render_evidence.py

test:
	.venv/bin/python -m unittest discover -s tests -v

all: download up prepare benchmark replication backup-restore mvcc equality test render

replay: build-csv up prepare benchmark replication backup-restore mvcc equality test render

stop:
	$(COMPOSE) stop

destroy:
	$(COMPOSE) down -v
