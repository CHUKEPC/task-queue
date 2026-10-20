.PHONY: install lint format type-check test docker-build docker-up docker-down clean all run worker help

VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

install: ## Create venv and install dependencies
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -e ".[dev]"

lint: ## Run linter
	$(PYTHON) -m ruff check src/ tests/
	$(PYTHON) -m ruff format --check src/ tests/

format: ## Auto-format code
	$(PYTHON) -m ruff format src/ tests/
	$(PYTHON) -m ruff check --fix src/ tests/

type-check: ## Run type checker
	$(PYTHON) -m mypy src/

test: ## Run tests with coverage
	$(PYTHON) -m pytest

run: ## Run FastAPI development server
	$(PYTHON) -m uvicorn task_queue.main:app --reload --host 0.0.0.0 --port 8000

worker: ## Run Celery worker
	$(PYTHON) -m celery -A task_queue.celery_app worker --loglevel=info -Q critical,high,medium,low,celery

docker-build: ## Build Docker image
	docker build -t task-queue .

docker-up: ## Start all services
	docker compose up -d

docker-down: ## Stop all services
	docker compose down

clean: ## Remove caches and venv
	rm -rf $(VENV) .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage coverage.xml
	find . -type d -name __pycache__ -exec rm -rf {} +

all: lint type-check test ## Run all checks

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
