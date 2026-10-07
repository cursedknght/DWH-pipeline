from datetime import datetime  

from airflow.sdk import dag, task, BaseHook
# от basehook нужна только одна функция: достать из Airflow сохраненное подключение (адрес, логин, пароль)  
from airflow.providers.postgres.hooks.postgres import PostgresHook 

PG_CONN_ID = "dwh_postgres"  # имя подключения к Postgres  
CH_CONN_ID = "dwh_clickhouse"  # имя подключения к ClickHouse

# колонки, которые мы передаем в ClickHouse (в том же порядке, что в SELECT ниже)
COLUMNS = ["tx_id",
           "tx_ts",
           "tx_date",
           "client_id",
           "city",
           "segment",
           "merchant_id",
           "category",
           "amount",
           "currency",
           "amount_rub",
           "status"] 
def get_ch_client():
    """создает клиент ClickHouse, по данным из подключения Airflow"""
    import clickhouse_connect # импорт внутри функции 
    conn = BaseHook.get_connection(CH_CONN_ID) # адрес, логин, пароль из AIRFLOW_CONN_DWH_CLICKHOUSE
    return clickhouse_connect.get_client(
        host = conn.host,               # host.docker.internal 
        port = conn.port,               # 8123
        username = conn.login,          # de 
        password = conn.password,       # de 
        database = conn.schema,         # dwh
        compress = False,               # без сжатия      
    )

@dag(
    dag_id = "fin_04_load_clickhouse",
    schedule = "45 * * * *",            # каждый час в :45
    start_date = datetime(2026, 10, 1),
    catchup = False,
    max_active_runs = 1,
    tags = ["pet-project"],
)
def load_clickhouse():
    # Задача1: создать таблицу в кликхаус
    @task
    def create_table():
        client = get_ch_client()
        client.command("""
            CREATE TABLE IF NOT EXISTS dwh.transactions_wide (
                tx_id       UInt64,                     -- целое без знака
                tx_ts       DateTime,
                tx_date     Date,
                client_id   UInt32,
                city        LowCardinality(String),     -- мало разных значений → словарь
                segment     LowCardinality(String),
                merchant_id UInt32,
                category    LowCardinality(String),
                amount      Decimal(12, 2),             -- деньги: Decimal, не Float
                currency    LowCardinality(String),
                amount_rub  Decimal(14, 2),
                status      LowCardinality(String),
                loaded_at   DateTime DEFAULT now()      -- заполнится само
            )
            ENGINE = MergeTree                          -- основной движок ClickHouse
            PARTITION BY toYYYYMM(tx_date)              -- части таблицы по месяцам
            ORDER BY (tx_date, city, category, tx_id)   -- порядок хранения = ключ разреженного индекса
        """)

    # Задача2: перенести новые транзакции 
    @task
    def load_increment() -> int:
        client = get_ch_client() 

        # watermark: самый большой tx_id, который уже есть в ClickHouse
        # (у пустой таблицы ClickHouse вернёт 0)
        last_id = int(client.command("SELECT max(tx_id) FROM dwh.transactions_wide") or 0)
        print(f"В ClickHouse уже есть транзакции до tx_id = {last_id}")

        # берём из Postgres только новые транзакции, сразу со справочниками (денормализация)
        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        rows = pg.get_records(
            """
            SELECT f.tx_id, f.tx_ts, f.tx_date, f.client_id, c.city, c.segment,
                   f.merchant_id, m.category, f.amount, f.currency, f.amount_rub, f.status
            FROM dds.fact_transactions f
            JOIN dds.dim_client   c ON c.client_id   = f.client_id
            JOIN dds.dim_merchant m ON m.merchant_id = f.merchant_id
            WHERE f.tx_id > %s
            ORDER BY f.tx_id
            """,
            parameters=(last_id,),                  # %s заменится на last_id безопасно
        )

        if not rows:
            print("Новых транзакций нет")
            return 0

        client.insert("transactions_wide", rows, column_names=COLUMNS)
        print(f"Загружено строк: {len(rows)}")
        return len(rows) 

    # Задача3: сверка количества строк
    @task 
    def check_counts():
        pg = PostgresHook(postgres_conn_id = PG_CONN_ID)
        pg_count = pg.get_first("SELECT count(*) FROM dds.fact_transactions")[0] 
        ch_count = int(get_ch_client().command("SELECT count() FROM dwh.transactions_wide"))
        print(f"Postgres dds: {pg_count}, ClickHouse: {ch_count}")
        if pg_count != ch_count:
            raise ValueError(f"Расхождение: Postgres {pg_count}, ClickHouse {ch_count}")

    create_table() >> load_increment() >> check_counts() 


load_clickhouse()




