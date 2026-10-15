PYTHON := .venv/bin/python
COLIMA_SOCKET := $(HOME)/.colima/default/docker.sock
DOCKER_CONFIG := $(CURDIR)/.runtime/docker-config
export DOCKER_CONFIG

ifneq ($(wildcard $(COLIMA_SOCKET)),)
DOCKER_HOST ?= unix://$(COLIMA_SOCKET)
export DOCKER_HOST
endif

.PHONY: setup download-new up prepare verify benchmark render test all stop destroy

setup:
	python3 -m venv .venv
	$(PYTHON) -m pip install -r requirements.txt
	mkdir -p $(DOCKER_CONFIG)
	printf '{}\n' > $(DOCKER_CONFIG)/config.json

download-new: setup
	$(PYTHON) scripts/download_new_batch.py --snapshot-date $$(date +%F)

up:
	mkdir -p $(DOCKER_CONFIG)
	printf '{}\n' > $(DOCKER_CONFIG)/config.json
	docker-compose up -d

prepare: setup
	$(PYTHON) scripts/prepare.py

verify: setup
	$(PYTHON) scripts/verify.py

benchmark: setup
	$(PYTHON) scripts/benchmark.py

render: setup
	$(PYTHON) scripts/render_evidence.py

test: setup
	$(PYTHON) -m unittest discover -s tests -v

all: test up prepare verify benchmark render

stop:
	docker-compose stop

destroy:
	docker-compose down -v --remove-orphans

