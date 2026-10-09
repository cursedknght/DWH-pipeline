from datetime import datetime  

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook 

CONN_ID = "dwh_postgres"


def run_sql(sql: str):
    PostgresHook(postgres_conn_id=CONN_ID).run(sql)     # выполняет SQL-запрос в базе данных, используя подключение по имени

 
@dag(                                                   # настройка дага                      
    dag_id = "fin_03_build_marts",          # уникальное имя дага
    schedule = "30 * * * *",                # запускать каждый час в :30
    start_date = datetime(2026, 10, 1),     # дата начала
    catchup = False,                        # не догонять пропущенные даги
    max_active_runs = 1,                    # <= 1  если предыдущий запуск ещё не завершился, то новый не стартует
    tags = ["pet-project"],                 # метки для поиска и фильтрации в веб-интерфейсе
)
def build_marts():
    # Задача1: создать схему и таблицы витрин  
    @task 
    def create_tables():  
        run_sql("""
            CREATE SCHEMA IF NOT EXISTS mart;

            -- Витрина 1: день × город × категория
            CREATE TABLE IF NOT EXISTS mart.daily_city_category (
                report_date     date,
                city            text,
                category        text,
                tx_total        int,                        -- всего транзакций
                tx_success      int,                        -- успешных
                tx_declined     int,                        -- отклонённых
                sum_rub         numeric(16, 2),             -- сумма успешных, руб.
                avg_check_rub   numeric(14, 2),             -- средний чек, руб.
                declined_share  numeric(5, 2),              -- доля отклонённых, %
                updated_at      timestamp DEFAULT now(),
                PRIMARY KEY (report_date, city, category)   -- составной ключ из трёх полей
            );

            -- Витрина 2: сводка по клиенту
            CREATE TABLE IF NOT EXISTS mart.client_stats (
                client_id       int PRIMARY KEY,
                full_name       text,
                city            text,
                segment         text,
                tx_count        int,                        -- сколько всего транзакций
                total_rub       numeric(16, 2),             -- сколько потратил (успешные)
                avg_check_rub   numeric(14, 2),             -- средний чек
                declined_share  numeric(5, 2),              -- доля отклонённых, %
                last_tx_ts      timestamp,                  -- когда была последняя транзакция
                updated_at      timestamp DEFAULT now()
            );
        """)    

    # Задача2: дневная витрина (пересчет окна 7 дней)  
    @task 
    def build_daily():      
        run_sql("""
            -- 1) удаляем последние 7 дней...
            DELETE FROM mart.daily_city_category
            WHERE report_date >= current_date - 7;

            -- 2) ...и считаем их заново из dds
            INSERT INTO mart.daily_city_category
                (report_date, city, category, tx_total, tx_success,
                 tx_declined, sum_rub, avg_check_rub, declined_share)
            SELECT f.tx_date,
                   c.city,
                   m.category,
                   count(*),
                   count(*) FILTER (WHERE f.status = 'success'),
                   count(*) FILTER (WHERE f.status = 'declined'),
                   coalesce(sum(f.amount_rub) FILTER (WHERE f.status = 'success'), 0),
                   round(avg(f.amount_rub) FILTER (WHERE f.status = 'success'), 2),
                   round(100.0 * count(*) FILTER (WHERE f.status = 'declined') / count(*), 2)
            FROM dds.fact_transactions f
            JOIN dds.dim_client   c ON c.client_id   = f.client_id
            JOIN dds.dim_merchant m ON m.merchant_id = f.merchant_id
            WHERE f.tx_date >= current_date - 7
            GROUP BY f.tx_date, c.city, m.category;
        """)

    # Задача3: витрина по клиентам (полная перезаливка)  
    @task
    def build_clients():
        run_sql("""
            -- 1) очищаем таблицу целиком...
            TRUNCATE mart.client_stats;

            -- 2) ...и считаем заново по всей истории
            INSERT INTO mart.client_stats
                (client_id, full_name, city, segment, tx_count,
                 total_rub, avg_check_rub, declined_share, last_tx_ts)
            SELECT c.client_id,
                   c.full_name,
                   c.city,
                   c.segment,
                   count(f.tx_id),
                   coalesce(sum(f.amount_rub) FILTER (WHERE f.status = 'success'), 0),
                   round(avg(f.amount_rub) FILTER (WHERE f.status = 'success'), 2),
                   round(100.0 * count(*) FILTER (WHERE f.status = 'declined')
                         / nullif(count(f.tx_id), 0), 2),
                   max(f.tx_ts)
            FROM dds.dim_client c
            LEFT JOIN dds.fact_transactions f ON f.client_id = c.client_id
            GROUP BY c.client_id, c.full_name, c.city, c.segment;
        """)

    # Задача4: проверка что суммы в витринах сходятся с dds  
    @task
    def check_sums(): 
        hook = PostgresHook(postgres_conn_id = CONN_ID) 
        dds = hook.get_first("""
            SELECT coalesce(sum(amount_rub), 0)
            FROM dds.fact_transactions WHERE status = 'success'
        """)[0]
        daily = hook.get_first("SELECT coalesce(sum(sum_rub), 0) FROM mart.daily_city_category")[0]
        clients = hook.get_first("SELECT coalesce(sum(total_rub), 0) FROM mart.client_stats")[0]
        print(f"dds: {dds}, daily: {daily}, clients: {clients}")
        if not (dds == daily == clients):
            raise ValueError(f"Суммы не сходятся: dds={dds}, daily={daily}, clients={clients}")

    t_create = create_tables()
    t_daily = build_daily()
    t_clients = build_clients()
    t_check = check_sums()

    t_create >> [t_daily, t_clients] >> t_check


build_marts()         