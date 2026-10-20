# Task Queue System

[![CI](https://github.com/CHUKEPC/task-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/CHUKEPC/task-queue/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688.svg?style=flat&logo=FastAPI&logoColor=white)](https://fastapi.tiangolo.com)
[![Celery](https://img.shields.io/badge/Celery-5.3+-37814A.svg?style=flat&logo=Celery&logoColor=white)](https://docs.celeryq.dev/)
[![Redis](https://img.shields.io/badge/Redis-7.0+-DC382D.svg?style=flat&logo=Redis&logoColor=white)](https://redis.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16+-4169E1.svg?style=flat&logo=PostgreSQL&logoColor=white)](https://www.postgresql.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A scalable, distributed task queue system with priority scheduling, exponential backoff retries, real-time WebSocket status updates, Celery Canvas workflows (chains & groups), analytical metrics dashboards, and worker cluster health monitoring. Built with **Celery**, **Redis**, **FastAPI**, **PostgreSQL**, and **SQLAlchemy 2.0** following clean layered architecture.

---

## Features

- **Priority Queue Scheduling**: 4 distinct priority tiers (`critical`, `high`, `medium`, `low`) mapped directly to Redis priority queues and numerical weights (0–9).
- **Extensible Task Types**:
  - `email`: Transactional email dispatch simulation with validation and delivery receipts.
  - `report`: Business intelligence and financial summary calculation with multi-format export metadata (`json`, `csv`, `pdf`).
  - `data_processing`: High-throughput record transformation, cleaning, deduplication, statistical aggregation, and SHA-256 integrity checksums.
- **Robust Retry Mechanism**: Automatic exponential backoff (`retry_backoff=True`), jitter, max retries limit, and manual re-enqueue capabilities for failed jobs.
- **Full Lifecycle Tracking**: Fine-grained task status lifecycle: `pending` → `running` → `success` / `failed` / `retrying` / `cancelled`.
- **Real-Time WebSockets**: Live status broadcasts to global or task-specific channels (`/ws/tasks` and `/ws/tasks/{task_id}`) powered by Redis Pub/Sub integration.
- **Celery Canvas Workflows**: Support for sequential task execution pipelines (`chains`) and parallel execution batches (`groups`).
- **Delayed & Scheduled Execution**: Delay task processing via `countdown_seconds` or schedule execution at a specific UTC timestamp (`scheduled_at`).
- **Result Storage with TTL**: Configurable expiration periods with automated cleanup of stale task records.
- **Metrics Dashboard API**: Real-time throughput (last hour), average completion duration, failure rates, and status distributions.
- **Worker Health Monitoring**: Inspection endpoint reporting live Celery worker node availability, current active jobs, and registered task signatures.

---

## Tech Stack

| Component | Technology | Description |
|---|---|---|
| **API Framework** | FastAPI | High-performance asynchronous REST & WebSocket framework |
| **Task Queue** | Celery 5.3+ | Distributed task execution and canvas orchestration |
| **Message Broker** | Redis 7 | High-speed broker, result backend, and Pub/Sub event transport |
| **Database** | PostgreSQL 16 | Relational storage for task records, lifecycles, and results |
| **ORM & Database Client** | SQLAlchemy 2.0 & asyncpg | Fully typed asynchronous ORM mapping |
| **Schema Migrations** | Alembic | Version-controlled schema migration tool |
| **Validation & Settings** | Pydantic v2 & Pydantic-Settings | Strict request/response validation and environment config |
| **Code Quality** | Ruff & Mypy (strict mode) | Modern linting, formatting, and strict type verification |
| **Testing** | pytest & pytest-asyncio | Over 50 unit and integration tests with **92% code coverage** |
| **Containerization** | Docker & Docker Compose | Multi-stage Docker image and complete 4-container local stack |

---

## Architecture & Project Structure

The codebase strictly adheres to clean layered architecture:
- `main.py` is reserved exclusively for app lifecycle and middleware initialization — no business logic.
- `celery_app.py` is dedicated to Celery configuration and queue topology.
- API route handlers contain no business logic and delegate to domain services.
- Domain services coordinate business rules and communicate with the database.

```
task-queue/
├── src/task_queue/
│   ├── __init__.py
│   ├── main.py              # FastAPI app initialization & lifecycle
│   ├── config.py            # Pydantic Settings & environment config
│   ├── celery_app.py        # Celery broker & queue topology setup
│   ├── exceptions.py        # Custom domain exceptions
│   ├── api/                 # HTTP & WebSocket endpoints
│   │   ├── __init__.py
│   │   ├── router.py        # Aggregated API router
│   │   ├── tasks.py         # Task submission, retry, and cancellation
│   │   ├── status.py        # Status queries, results, and metrics
│   │   └── websocket.py     # Real-time WebSocket connection manager
│   ├── tasks/               # Celery task definitions
│   │   ├── __init__.py
│   │   ├── base.py          # BaseTask with lifecycle hooks & retries
│   │   ├── email.py         # Email delivery task
│   │   ├── report.py        # Analytical report task
│   │   └── data_processing.py # Batch cleaning & aggregation tasks
│   ├── models/              # SQLAlchemy ORM models
│   │   ├── __init__.py
│   │   └── task_record.py   # Persistent TaskRecord model & Enums
│   ├── schemas/             # Pydantic validation models
│   │   ├── __init__.py
│   │   └── task.py          # Request & response schemas
│   ├── services/            # Business logic layer
│   │   ├── __init__.py
│   │   ├── task_service.py  # Task orchestration & persistence
│   │   └── metrics.py       # Metrics aggregation & worker inspection
│   └── db/                  # Database connectivity
│       ├── __init__.py
│       └── session.py       # Async & sync engine session factories
├── alembic/                 # Database migrations
│   ├── env.py
│   ├── script.py.mako
│   └── versions/
│       └── 001_initial_task_records.py
├── tests/                   # Test suite mirroring src layout
│   ├── __init__.py
│   ├── conftest.py          # Fixtures, test DB & eager Celery setup
│   ├── test_api/            # API integration tests
│   ├── test_tasks/          # Celery task unit tests
│   ├── test_services/       # Business service unit tests
│   └── test_db_and_config.py# Database and config unit tests
├── docker-compose.yml       # Production stack: App + Worker + Redis + Postgres
├── Dockerfile               # Multi-stage security-hardened container build
├── Makefile                 # Development automation commands
├── pyproject.toml           # Project dependencies & tool configs
├── README.md                # English documentation
└── README_RU.md             # Russian documentation
```

---

## Getting Started

### Prerequisites

- Python 3.11+
- Redis (optional for unit tests, required for distributed worker execution)
- PostgreSQL (optional for unit tests, required for distributed worker execution)
- Docker & Docker Compose (optional for containerized deployment)

### Local Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/CHUKEPC/task-queue.git
   cd task-queue
   ```

2. **Create virtual environment and install dependencies:**
   ```bash
   make install
   ```

3. **Configure environment variables:**
   ```bash
   cp .env.example .env
   ```

4. **Run database migrations:**
   ```bash
   alembic upgrade head
   ```

5. **Start services:**
   - In terminal 1 (FastAPI Web Server):
     ```bash
     make run
     ```
   - In terminal 2 (Celery Worker):
     ```bash
     make worker
     ```

### Docker Compose Deployment

Run the complete stack (FastAPI web server, Celery worker, PostgreSQL, and Redis) with a single command:
```bash
docker compose up -d
```

The API will be accessible at `http://localhost:8000` with interactive Swagger UI documentation at `http://localhost:8000/docs`.

---

## API Reference

### 1. Task Submission

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/v1/tasks/email` | Submit an email delivery task |
| `POST` | `/api/v1/tasks/report` | Submit an analytical report generation task |
| `POST` | `/api/v1/tasks/data-processing` | Submit a batch data processing task |
| `POST` | `/api/v1/tasks/chain` | Submit a sequential task chain workflow |
| `POST` | `/api/v1/tasks/group` | Submit a parallel task group workflow |

#### Example: Submit Email Task
```bash
curl -X POST "http://localhost:8000/api/v1/tasks/email" \
  -H "Content-Type: application/json" \
  -d '{
    "recipient": "user@example.com",
    "subject": "Order Confirmation",
    "body": "Thank you for your purchase!",
    "priority": "high",
    "countdown_seconds": 10
  }'
```

#### Example: Submit Batch Data Processing Task
```bash
curl -X POST "http://localhost:8000/api/v1/tasks/data-processing" \
  -H "Content-Type: application/json" \
  -d '{
    "dataset_name": "q3_transactions",
    "records": [
      {"id": 1, "item": "Widget", "amount": 25.50},
      {"id": 2, "item": "Gadget", "amount": 80.00}
    ],
    "operations": ["clean", "deduplicate", "aggregate"],
    "priority": "critical"
  }'
```

---

### 2. Task Control & Query

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/tasks` | List tasks with filtering (`status`, `task_type`, `priority`) and pagination |
| `GET` | `/api/v1/tasks/{task_id}` | Retrieve comprehensive task details and lifecycle state |
| `GET` | `/api/v1/tasks/{task_id}/result` | Fetch task output, execution duration, and error traces |
| `POST` | `/api/v1/tasks/{task_id}/retry` | Manually re-enqueue a failed or cancelled task |
| `POST` | `/api/v1/tasks/{task_id}/cancel` | Cancel in-flight or pending execution and revoke worker process |
| `DELETE` | `/api/v1/tasks/expired` | Purge expired task records according to configured TTL |

---

### 3. Monitoring & Analytics

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/metrics` | System analytics: throughput, error rates, average duration |
| `GET` | `/api/v1/workers/health` | Inspect Celery worker nodes, active load, and registered tasks |
| `GET` | `/health` | Application healthcheck endpoint for orchestrators |

#### Example Metrics Response
```json
{
  "total_tasks": 1250,
  "status_counts": {
    "pending": 4,
    "running": 12,
    "success": 1210,
    "failed": 18,
    "retrying": 3,
    "cancelled": 3
  },
  "type_counts": {
    "email": 800,
    "report": 250,
    "data_processing": 200
  },
  "priority_counts": {
    "low": 100,
    "medium": 750,
    "high": 300,
    "critical": 100
  },
  "throughput_last_hour": 185,
  "avg_duration_seconds": 0.42,
  "failure_rate_percentage": 1.44,
  "active_tasks": 16
}
```

---

### 4. Real-Time WebSockets

| Channel | Description |
|---|---|
| `ws://localhost:8000/api/v1/ws/tasks` | Stream real-time status updates across all tasks |
| `ws://localhost:8000/api/v1/ws/tasks/{task_id}` | Stream updates for a specific task |

#### Example Event Payload
```json
{
  "event": "task_success",
  "task_id": "b054f858-9b80-413a-a8bb-c947dfe6cbff",
  "status": "success",
  "timestamp": "2026-10-21T00:45:00.000Z",
  "data": {
    "celery_task_id": "c1f73b88-1fa9-4a90-8809-5e72d3f7bb40",
    "task_name": "tasks.send_email",
    "result": {
      "status": "delivered",
      "recipient": "user@example.com",
      "message_id": "msg_9f2bc719dae1"
    },
    "duration_seconds": 0.23,
    "retry_count": 0
  }
}
```

---

## Development & Testing

All quality commands are automated via `Makefile`:

```bash
# Run automated code formatters
make format

# Run linter checks (Ruff)
make lint

# Run strict type checking (Mypy)
make type-check

# Run test suite with coverage report
make test

# Execute all validation gates (lint, type-check, test)
make all
```

---

## License

This project is open-source software licensed under the [MIT License](LICENSE).
