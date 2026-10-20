# Task Queue System (Распределенная система очередей задач)

[![CI](https://github.com/CHUKEPC/task-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/CHUKEPC/task-queue/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688.svg?style=flat&logo=FastAPI&logoColor=white)](https://fastapi.tiangolo.com)
[![Celery](https://img.shields.io/badge/Celery-5.3+-37814A.svg?style=flat&logo=Celery&logoColor=white)](https://docs.celeryq.dev/)
[![Redis](https://img.shields.io/badge/Redis-7.0+-DC382D.svg?style=flat&logo=Redis&logoColor=white)](https://redis.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16+-4169E1.svg?style=flat&logo=PostgreSQL&logoColor=white)](https://www.postgresql.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Масштабируемая распределенная система управления очередями фоновых задач с приоритетной диспетчеризацией, автоматическими повторами с экспоненциальной задержкой (exponential backoff), real-time обновлениями статусов через WebSocket, поддержкой сложных пайплайнов Celery Canvas (цепочки и параллельные группы), дашбордом метрик и мониторингом состояния воркеров. Реализована на стеке **Celery**, **Redis**, **FastAPI**, **PostgreSQL** и **SQLAlchemy 2.0** в соответствии с принципами чистой многослойной архитектуры.

---

## Возможности

- **Приоритетная диспетчеризация**: 4 уровня приоритетов (`critical`, `high`, `medium`, `low`), привязанных к очередям Redis и числовым весам приоритета (0–9).
- **Различные типы задач**:
  - `email`: Отправка транзакционных писем с валидацией адресата и формированием квитанции о доставке.
  - `report`: Фоновая генерация аналитических и финансовых отчетов с поддержкой различных форматов экспорта (`json`, `csv`, `pdf`).
  - `data_processing`: Пакетная обработка записей (очистка, дедупликация, статистическая агрегация, вычисление контрольной суммы SHA-256).
- **Политика повторных попыток (Retry)**: Автоматический экспоненциальный backoff (`retry_backoff=True`), рандомизация задержек (jitter), ограничение максимального числа попыток, а также API для ручного перезапуска упавших задач.
- **Полный жизненный цикл задачи**: `pending` → `running` → `success` / `failed` / `retrying` / `cancelled`.
- **Real-Time обновления через WebSocket**: Трансляция событий жизненного цикла в реальном времени в общий канал (`/ws/tasks`) или канал конкретной задачи (`/ws/tasks/{task_id}`) на базе Redis Pub/Sub.
- **Сложные сценарии Celery Canvas**: Поддержка последовательных цепочек (`chains`) и параллельных групп (`groups`).
- **Отложенный запуск и планирование**: Запуск через заданный интервал (`countdown_seconds`) или в конкретное время UTC (`scheduled_at`).
- **Хранение результатов с TTL**: Автоматическое удаление устаревших записей по истечении заданного времени жизни.
- **Дашборд метрик**: Пропускная способность за последний час, средняя продолжительность успешных задач, процент отказов и распределение по статусам и типам.
- **Мониторинг воркеров**: Инспекция активных воркеров Celery, текущей нагрузки и списка зарегистрированных задач.

---

## Стек технологий

| Компонент | Технология | Описание |
|---|---|---|
| **API Фреймворк** | FastAPI | Асинхронный веб-фреймворк с поддержкой REST и WebSocket |
| **Очередь задач** | Celery 5.3+ | Распределенный исполнитель фоновых задач и оркестратор Canvas |
| **Брокер сообщений** | Redis 7 | Брокер сообщений, бэкенд результатов и Pub/Sub транспорт |
| **База данных** | PostgreSQL 16 | Реляционное хранилище задач, метаданных и результатов |
| **ORM и драйвер** | SQLAlchemy 2.0 & asyncpg | Строго типизированный асинхронный ORM |
| **Миграции** | Alembic | Управление версионированием схемы БД |
| **Валидация и настройки** | Pydantic v2 & Pydantic-Settings | Валидация входных данных и конфигурация через переменные окружения |
| **Качество кода** | Ruff & Mypy (strict) | Линтер, автоформатер и строгая статическая типизация |
| **Тестирование** | pytest & pytest-asyncio | Более 50 модульных и интеграционных тестов с **покрытием 92%** |
| **Контейнеризация** | Docker & Docker Compose | Многоэтапный Dockerfile и готовый docker-compose стек из 4 сервисов |

---

## Архитектура и структура проекта

Проект строго следует принципам многослойной архитектуры:
- `main.py` предназначен исключительно для инициализации приложения и подключения роутеров — никакой бизнес-логики.
- `celery_app.py` отвечает только за конфигурацию Celery и топологию очередей.
- Хэндлеры API делегируют выполнение сервисным слоям.
- Сервисный слой управляет бизнес-логикой и взаимодействует с базой данных и брокером.

```
task-queue/
├── src/task_queue/
│   ├── __init__.py
│   ├── main.py              # Точка входа и инициализация FastAPI
│   ├── config.py            # Настройки Pydantic Settings и окружения
│   ├── celery_app.py        # Настройка Celery и очередей
│   ├── exceptions.py        # Кастомные доменные исключения
│   ├── api/                 # Эндпоинты HTTP и WebSocket
│   │   ├── __init__.py
│   │   ├── router.py        # Сборный роутер API
│   │   ├── tasks.py         # Создание, перезапуск и отмена задач
│   │   ├── status.py        # Запрос статусов, результатов и метрик
│   │   └── websocket.py     # Менеджер WebSocket-соединений
│   ├── tasks/               # Определение задач Celery
│   │   ├── __init__.py
│   │   ├── base.py          # Базовый класс задачи с хуками и retry
│   │   ├── email.py         # Задача отправки email
│   │   ├── report.py        # Задача генерации отчетов
│   │   └── data_processing.py # Пакетная обработка данных
│   ├── models/              # Модели SQLAlchemy 2.0
│   │   ├── __init__.py
│   │   └── task_record.py   # Модель TaskRecord и энумы
│   ├── schemas/             # Pydantic-схемы
│   │   ├── __init__.py
│   │   └── task.py          # Схемы запросов и ответов
│   ├── services/            # Слой бизнес-логики
│   │   ├── __init__.py
│   │   ├── task_service.py  # Оркестрация и управление задачами
│   │   └── metrics.py       # Расчет метрик и инспекция воркеров
│   └── db/                  # Настройка базы данных
│       ├── __init__.py
│       └── session.py       # Фабрики асинхронных и синхронных сессий
├── alembic/                 # Миграции базы данных
│   ├── env.py
│   ├── script.py.mako
│   └── versions/
│       └── 001_initial_task_records.py
├── tests/                   # Набор тестов (повторяет структуру src/)
│   ├── __init__.py
│   ├── conftest.py          # Фикстуры, тестовая БД и Celery eager mode
│   ├── test_api/            # Тесты API эндпоинтов
│   ├── test_tasks/          # Тесты задач Celery
│   ├── test_services/       # Тесты сервисов
│   └── test_db_and_config.py# Тесты БД и конфигурации
├── docker-compose.yml       # Стек для запуска: App + Worker + Redis + Postgres
├── Dockerfile               # Многоэтапный защищенный Dockerfile
├── Makefile                 # Команды автоматизации разработки
├── pyproject.toml           # Зависимости и конфигурация инструментов
├── README.md                # Документация на английском языке
└── README_RU.md             # Документация на русском языке
```

---

## Быстрый старт

### Требования

- Python 3.11+
- Redis и PostgreSQL (для распределенного запуска)
- Docker и Docker Compose (опционально)

### Локальная установка

1. **Клонируйте репозиторий:**
   ```bash
   git clone https://github.com/CHUKEPC/task-queue.git
   cd task-queue
   ```

2. **Создайте виртуальное окружение и установите зависимости:**
   ```bash
   make install
   ```

3. **Создайте файл конфигурации окружения:**
   ```bash
   cp .env.example .env
   ```

4. **Примените миграции базы данных:**
   ```bash
   alembic upgrade head
   ```

5. **Запустите приложение и воркер:**
   - Терминал 1 (Веб-сервер FastAPI):
     ```bash
     make run
     ```
   - Терминал 2 (Celery воркер):
     ```bash
     make worker
     ```

### Запуск через Docker Compose

Запуск всей инфраструктуры (FastAPI, Celery воркер, PostgreSQL, Redis) в изолированных контейнерах:
```bash
docker compose up -d
```

Интерактивная документация Swagger UI доступна по адресу `http://localhost:8000/docs`.

---

## Обзор API

### 1. Отправка задач

| Метод | Эндпоинт | Описание |
|---|---|---|
| `POST` | `/api/v1/tasks/email` | Отправить задачу на доставку email |
| `POST` | `/api/v1/tasks/report` | Отправить задачу на генерацию отчета |
| `POST` | `/api/v1/tasks/data-processing` | Отправить задачу пакетной обработки данных |
| `POST` | `/api/v1/tasks/chain` | Запустить последовательную цепочку задач (Chain) |
| `POST` | `/api/v1/tasks/group` | Запустить параллельную группу задач (Group) |

#### Пример: Отправка задачи Email
```bash
curl -X POST "http://localhost:8000/api/v1/tasks/email" \
  -H "Content-Type: application/json" \
  -d '{
    "recipient": "user@example.com",
    "subject": "Подтверждение заказа",
    "body": "Спасибо за оформление заказа!",
    "priority": "high",
    "countdown_seconds": 5
  }'
```

---

### 2. Управление и запрос статуса

| Метод | Эндпоинт | Описание |
|---|---|---|
| `GET` | `/api/v1/tasks` | Список задач с фильтрацией (`status`, `task_type`, `priority`) и пагинацией |
| `GET` | `/api/v1/tasks/{task_id}` | Получить подробную информацию о состоянии задачи |
| `GET` | `/api/v1/tasks/{task_id}/result` | Получить результат выполнения и время работы |
| `POST` | `/api/v1/tasks/{task_id}/retry` | Перезапустить упавшую или отмененную задачу |
| `POST` | `/api/v1/tasks/{task_id}/cancel` | Отменить выполняющуюся или ожидающую задачу |
| `DELETE` | `/api/v1/tasks/expired` | Удалить устаревшие задачи согласно TTL |

---

### 3. Мониторинг и метрики

| Метод | Эндпоинт | Описание |
|---|---|---|
| `GET` | `/api/v1/metrics` | Аналитика: пропускная способность, процент ошибок, средняя длительность |
| `GET` | `/api/v1/workers/health` | Проверка доступности нод Celery воркеров и их загрузки |
| `GET` | `/health` | Базовый статус готовности сервиса |

---

### 4. Real-Time обновления через WebSocket

| Канал | Описание |
|---|---|
| `ws://localhost:8000/api/v1/ws/tasks` | Поток обновлений по всем задачам в системе |
| `ws://localhost:8000/api/v1/ws/tasks/{task_id}` | Поток обновлений по конкретной задаче |

---

## Тестирование и контроль качества

Все проверки запускаются через единый `Makefile`:

```bash
# Форматирование кода
make format

# Проверка линтером (Ruff)
make lint

# Проверка статической типизации (Mypy)
make type-check

# Запуск тестов с замером покрытия
make test

# Комплексный прогон всех проверок
make all
```

---

## Лицензия

Проект распространяется под открытой лицензией [MIT](LICENSE).
