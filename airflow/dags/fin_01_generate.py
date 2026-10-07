import random                                                       # для генерации случайных чисел 
from datetime import datetime, timedelta                            # дата и время, промежуток времени

from airflow.sdk import dag, task                                   # импорьтируем декораторы dag и task из airflow.sdk
from airflow.providers.postgres.hooks.postgres import PostgresHook  # подключение к Postgres

CONN_ID = "dwh_postgres"                                            # имя подключения к базе  


@dag(                                               # эта функция собирает конвейер:
    dag_id="fin_01_generate_transactions",              # уникальное имя дага 
    schedule="@hourly",                                 # запускать каждый час
    start_date=datetime(2026, 10, 1),                   # дата начала
    catchup=False,                                      # не запускать пропущенные даги
    tags=["pet-project"],                               # метки для поиска и фильтрации в веб-интерфейсе
)
def generate_transactions():
    # Задача1: создаем таблицу 
    @task # превращает функцию в задачу airflow 
    def create_table(): 
        hook = PostgresHook(postgres_conn_id=CONN_ID) # создает подключение к базе по имени
        hook.run("""
            CREATE SCHEMA IF NOT EXISTS stg;                    -- слой сырых данных  
            CREATE TABLE IF NOT EXISTS stg.transactions_raw (
                tx_id       bigserial PRIMARY KEY,              -- уникальный идентификатор транзакции   
                client_id   int,                                
                merchant_id int,
                amount      numeric(12, 2),                     -- сумма транзакции
                currency    text,                               -- валюта
                status      text,                               -- статус
                tx_ts       timestamp,                          -- время транзакции
                loaded_at   timestamp DEFAULT now()             -- время загрузки в таблицу
            );
        """) 
        # выполняет SQL, """ позволяет писать многострочный текст   
        # создать схему stg, если она не существует, и таблицу transactions_raw с указанными полями

    # Задача2: генерируем случайные транзакции и вставляем их в таблицу
    @task # превращает функцию в задачу airflow
    def generate(n: int = 1000) -> int: # сколько транзакций генерировать, по умолчанию будет 1000 
        now = datetime.now() # текущая дата и время
        rows = []
        for _ in range(n):
            rows.append((
                random.randint(1, 10000),                               # клиент 
                random.randint(1, 500),                                 # магазин 
                round(random.uniform(10, 10000), 2),                    # сумма  
                random.choice(["RUB", "RUB", "RUB", "USD", "EUR"]),     # валюта 
                "success" if random.random() < 0.97 else "declined",    # статус  
                now - timedelta(minutes=random.randint(0, 59)),         # время  
            ))                                          # генерируем случайные значения для каждой транзакции: client_id, merchant_id, amount, currency, status и tx_ts
        hook = PostgresHook(postgres_conn_id=CONN_ID)   # создает подключение к базе по имени
        hook.insert_rows(
            table="stg.transactions_raw",
            rows=rows,
            target_fields=["client_id", "merchant_id", "amount",
                           "currency", "status", "tx_ts"],
        )                   # вставляет сгенерированные строки в таблицу transactions_raw, указывая поля, в которые вставлять данные
        return len(rows)    # возвращает количество вставленных строк, попадает в XCom  

    # порядок выполнения задач: сначала создаем таблицу, затем генерируем транзакции
    create_table() >> generate()

# регистрируем даг в airflow, чтобы он был виден в веб-интерфейсе и мог выполняться по расписанию
generate_transactions()