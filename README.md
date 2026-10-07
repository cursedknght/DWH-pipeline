# DWH Pipeline

Учебный конвейер данных для финтеха: Airflow каждый час генерирует платёжные транзакции, раскладывает их по слоям хранилища (stg → dds → mart) в PostgreSQL и выгружает в ClickHouse для быстрой аналитики.

## Архитектура

```
Airflow (по расписанию)
   │
   ├─ :00  fin_01_generate_transactions ──▶ PostgreSQL  stg.transactions_raw
   │
   ├─ :15  fin_02_build_dds ─────────────▶ PostgreSQL  dds  (звезда)
   │                                          ├─ fact_transactions
   │                                          ├─ dim_client
   │                                          └─ dim_merchant
   │
   ├─ :30  fin_03_build_marts ───────────▶ PostgreSQL  mart (витрины)
   │                                          ├─ daily_city_category
   │                                          └─ client_stats
   │
   └─ :45  fin_04_load_clickhouse ───────▶ ClickHouse  dwh.transactions_wide
```

## Стек

| Инструмент | Роль |
|---|---|
| Apache Airflow 3 | Оркестрация, расписание, зависимости задач |
| PostgreSQL 16 | Слои хранилища stg, dds, mart |
| ClickHouse | Аналитическое хранилище (широкая денормализованная таблица) |
| Docker Compose | Запуск всей инфраструктуры локально |
| Python, SQL | Логика загрузки и трансформаций |

## Слои данных

| Слой | Таблицы | Описание |
|---|---|---|
| **stg** | `transactions_raw` | Сырые транзакции «как пришли» из источника |
| **dds** | `fact_transactions`, `dim_client`, `dim_merchant` | Модель «звезда»: факты и измерения, пересчёт валют в рубли |
| **mart** | `daily_city_category`, `client_stats` | Готовые витрины для аналитиков |
| **ClickHouse** | `transactions_wide` | Факты, денормализованные со справочниками, на движке MergeTree |

## Что реализовано

- **Слоистая архитектура DWH** (stg → dds → mart) и модель «звезда».
- **Идемпотентность:** повторный запуск любого DAG'а не создаёт дублей (`IF NOT EXISTS`, `ON CONFLICT DO NOTHING`, delete-insert).
- **Инкрементальная загрузка** по watermark (`max(tx_id)`): переносятся только новые строки.
- **Два способа обновления витрин:** пересчёт окна последних 7 дней и полная перезаливка.
- **Проверки качества данных:** сверка количества строк и сумм между слоями; при расхождении задача падает.
- **Атомарность:** витрины пересчитываются в одной транзакции и не бывают «наполовину пустыми».
- **ClickHouse:** MergeTree, партиционирование по месяцам, ключ сортировки под частые фильтры, `LowCardinality`.
- **Безопасность:** параметризованные SQL-запросы, учётные данные вынесены в подключения Airflow.

## Как запустить

Нужны Docker и Docker Compose.

```bash
git clone git@github.com:cursedknght/fintech-dwh-pipeline.git
cd fintech-dwh-pipeline

# 1. PostgreSQL и ClickHouse
docker compose up -d

# 2. Airflow
cd airflow
cp .env.example .env
sed -i "s/AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
mkdir -p logs plugins config
docker compose up airflow-init
docker compose up -d
```

Веб-интерфейс Airflow: http://localhost:8080 (логин и пароль `airflow`). Включите DAG'и с тегом `pet-project`.

Подключения к базам (для DBeaver или psql):

| База | Хост и порт | База данных | Логин / пароль |
|---|---|---|---|
| PostgreSQL | localhost:5432 | dwh | de / de |
| ClickHouse | localhost:8123 | dwh | de / de |

> Пароли учебные, только для локального запуска.

## Примеры запросов

Топ-10 клиентов по тратам (PostgreSQL):

```sql
SELECT client_id, full_name, city, segment, tx_count, total_rub
FROM mart.client_stats
ORDER BY total_rub DESC
LIMIT 10;
```

Траты по городам и категориям (ClickHouse):

```sql
SELECT city, category, count() AS tx, sum(amount_rub) AS sum_rub
FROM dwh.transactions_wide
WHERE status = 'success'
GROUP BY city, category
ORDER BY sum_rub DESC;
```

## Планы развития

- Поток транзакций через Kafka вместо пакетной генерации.
- SCD Type 2 для истории изменений клиентов.
- Зависимости между DAG'ами через Airflow Assets вместо сдвига по времени.
- Тесты для DAG'ов.