from datetime import datetime                                           # дата и время 

from airflow.sdk import dag, task                                       # импортируем декораторы dag и task из airflow.sdk
from airflow.providers.postgres.hooks.postgres import PostgresHook      # подключение к Postgres

CONN_ID = "dwh_postgres"                                                # имя подключения к базе


def run_sql(sql: str): 
    PostgresHook(postgres_conn_id=CONN_ID).run(sql)  # выполняет SQL-запрос в базе данных, используя подключение по имени


@dag(
    dag_id="fin_02_build_dds",                                         # уникальное имя дага
    schedule="15 * * * *",                                             # запускать каждый час в :15
                                                                       # (fin_01 генерирует в :00, даём ему закончить)
    start_date=datetime(2026, 10, 1),                                  # дата начала
    catchup=False,                                                     # не догонять пропущенные даги
    tags=["pet-project"],                                              # метки для поиска и фильтрации в веб-интерфейсе
)
# создание звезды
def build_dds(): 

    @task                                                           # задача1: создать схемы и таблицы в слое dds
    def create_tables():                                            
        run_sql("""
            CREATE SCHEMA IF NOT EXISTS dds;                        -- схема для слоя dds (data warehouse)

            CREATE TABLE IF NOT EXISTS dds.dim_client (             -- измерение: справочник клиентов (кто)
                client_id     int PRIMARY KEY,                    -- уникальный номер клиента
                full_name     text,                               -- полное имя
                city          text,                               -- город
                segment       text,                               -- сегмент mass/premium/vip
                registered_at date                                -- дата регистрации
            );

            CREATE TABLE IF NOT EXISTS dds.dim_merchant (           -- измерение: справочник магазинов (где)
                merchant_id   int PRIMARY KEY,                    -- уникальный номер магазина
                merchant_name text,                               -- название магазина
                category      text                                -- категория
            );

            CREATE TABLE IF NOT EXISTS dds.fact_transactions (      -- факт: транзакции (что произошло)
                tx_id       bigint PRIMARY KEY,                   -- номер транзакции переносим из stg  
                client_id   int REFERENCES dds.dim_client,        -- внешний ключ: клиент должен быть в справочнике  
                merchant_id int REFERENCES dds.dim_merchant,      -- внешний ключ: магазин должен быть в справочнике
                tx_ts       timestamp,                            -- время транзакции  
                tx_date     date,                                 -- дата транзакции (для удобства агрегации по дням)
                amount      numeric(12, 2),                       -- сумма транзакции в исходной валюте 
                currency    text,                                 -- исходная валюта транзакции
                amount_rub  numeric(14, 2),                       -- сумма транзакции в рублях (для удобства агрегации по валютам)
                status      text,                                 -- статус транзакции (success/declined)
                loaded_at   timestamp DEFAULT now()               -- когда попало в dds (для отладки и мониторинга)
            );
        """)

    @task                                                           # задача2: заполнить справочник клиентов  
                                                                    # имитируем выгрузку из срм. Заполняется один раз 
                                                                    # при следующих запусках ON CONFLICT пропустит уже существующих  
    def load_dim_client():
         run_sql("""
            INSERT INTO dds.dim_client
            SELECT g,                                                                               -- номер клиента 1..10000
                   'Клиент ' || g,                                                                  -- || склеивает строки   
                   (ARRAY['Москва','Санкт-Петербург','Казань','Новосибирск','Екатеринбург'])
                       [1 + floor(random() * 5)::int],                                              -- случайный город (индекс 1..5)
                   (ARRAY['mass','premium','vip'])[1 + floor(random() * 3)::int],                   -- случайный сегмент (индекс 1..3) 
                   date '2020-01-01' + (random() * 1800)::int                                       -- случайная дата за примерно 5 лет  
            FROM generate_series(1, 10000) g                                                        -- генерируем 10000 клиентов с числами 1..10000
            ON CONFLICT (client_id) DO NOTHING;                                                     -- уже есть такой клиент? пропустить  
        """)
         
    @task                                                           # задача3: заполнить справочник магазинов
    def load_dim_merchant():
        run_sql("""
            INSERT INTO dds.dim_merchant
            SELECT g,                                                                       -- номер магазина 1..500
                   'Магазин ' || g,
                   (ARRAY['Продукты','Кафе','Транспорт','Электроника','Одежда','Аптеки'])   
                       [1 + floor(random() * 6)::int]                                       -- случайная категория (индекс 1..6)
            FROM generate_series(1, 500) g                                                  -- генерируем 500 магазинов с числами 1..500
            ON CONFLICT (merchant_id) DO NOTHING;                                           -- уже есть такой магазин? пропустить
        """) 

    @task                                                           # задача4: перенести НОВЫЕ транзакции из слоя stg в dds
    def load_fact():
        run_sql("""
            INSERT INTO dds.fact_transactions
                (tx_id, client_id, merchant_id, tx_ts, tx_date,
                 amount, currency, amount_rub, status)
            SELECT r.tx_id,
                   r.client_id,
                   r.merchant_id,
                   r.tx_ts,
                   r.tx_ts::date,                                                           -- ::меняет тип: оставляем только дату, без времени
                   r.amount,
                   r.currency,
                   round(r.amount * CASE r.currency
                                        WHEN 'USD' THEN 90              -- курс доллара к рублю
                                        WHEN 'EUR' THEN 100             -- курс евро к рублю
                                        ELSE 1                          -- рубли остаются как есть 
                                    END, 2),                            -- округляем до копеек 
                   r.status
            FROM stg.transactions_raw r
                                            -- ИНКРЕМЕНТАЛЬНАЯ ЗАГРУЗКА: берём только транзакции новее уже загруженных.
                                            -- coalesce(..., 0) нужен для первого запуска, когда таблица пустая и max = NULL
            WHERE r.tx_id > (SELECT coalesce(max(tx_id), 0) FROM dds.fact_transactions)
            ON CONFLICT (tx_id) DO NOTHING; -- страховка от дублей  
        """)

    @task                                                           # задача5: проверить качество данных  
    def check_counts():
        hook = PostgresHook(postgres_conn_id=CONN_ID)
                                            # get_first возвращает первую строку результата, например (2000,); [0] достаёт число
        stg = hook.get_first("SELECT count(*) FROM stg.transactions_raw")[0]
        dds = hook.get_first("SELECT count(*) FROM dds.fact_transactions")[0]
        print(f"stg: {stg}, dds: {dds}")    # это будет видно в журналах задачи  
        if stg != dds:
                                            # raise роняет задачу: она станет красной и мы узнаем о проблеме  
            raise ValueError(f"Расхождение: в stg {stg} строк, в dds {dds}")


                                                                    #порядок выполнения задач  
    t_create = create_tables()
    t_client = load_dim_client()
    t_merchant = load_dim_merchant()
    t_fact = load_fact()
    t_check = check_counts()

    t_create >> [t_client, t_merchant] >> t_fact >> t_check   


build_dds()       # регистрируем даг в airflow, чтобы он был виден в веб-интерфейсе и мог выполняться по расписанию
 