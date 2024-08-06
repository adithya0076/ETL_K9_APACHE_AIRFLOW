from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts.k9_etl import extract_and_transform_data, load_data, start_streamlit_app


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 5),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "fetch_and_store_k9_data",
    default_args=default_args,
    description="A Dag to fetch and store K9 data",
    schedule_interval=timedelta(days=1),
)

fetch_k9_data_task = PythonOperator(
    task_id="fetch_k9_data",
    python_callable=extract_and_transform_data,
    dag=dag,
)

create_fact_table_task = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="k9_connection",
    sql="""
    CREATE TABLE IF NOT EXISTS facts (
        id SERIAL PRIMARY KEY,
        fact TEXT UNIQUE NOT NULL,
        category VARCHAR(20) NOT NULL,
        created_date TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

create_fact_versions_table_task = PostgresOperator(
    task_id="create_fact_versions_table",
    postgres_conn_id="k9_connection",
    sql="""
    CREATE TABLE IF NOT EXISTS fact_versions (
        id SERIAL PRIMARY KEY,
        fact_id INTEGER NOT NULL REFERENCES facts(id),
        fact TEXT NOT NULL,
        version INTEGER NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(fact_id, version)
    );
    """,
    dag=dag,
)

insert_k9_data_task = PythonOperator(
    task_id="insert_k9_data",
    python_callable=load_data,
    dag=dag,
)

# dependencies
(
    fetch_k9_data_task
    >> create_fact_table_task
    >> create_fact_versions_table_task
    >> insert_k9_data_task
)
