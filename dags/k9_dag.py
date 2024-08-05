from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def categorize_fact(fact):
    if any(char.isdigit() for char in fact):
        return "with_numbers"
    else:
        return "without_numbers"


# fetch data from the K9 data source
def extract_and_transform_data(ti):
    url = "https://raw.githubusercontent.com/vetstoria/random-k9-etl/main/source_data.json"
    response = requests.get(url)
    data = response.json()

    # Convert the JSON data into a pandas DataFrame
    df = pd.DataFrame(data)

    # Remove duplicates
    df_unique = df.drop_duplicates(subset=["fact", "created_date"])

    # Apply the categorization function
    df_unique["category"] = df_unique["fact"].apply(categorize_fact)

    # Push the transformed data to XCom
    ti.xcom_push(key="k9_data", value=df_unique.to_dict("records"))


# insert data into the Postgres database
def load_data(ti):
    k9_data = ti.xcom_pull(task_ids="fetch_k9_data", key="k9_data")
    if not k9_data:
        raise ValueError("No k9 data found")

    pg_hook = PostgresHook(postgres_conn_id="k9_connection")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    for data in k9_data:
        fact = data["fact"]
        category = data["category"]
        created_date = data["created_date"]

        # Insert or update 'facts' table and get the fact ID
        cursor.execute(
            """
            INSERT INTO facts (fact, category, created_date)
            VALUES (%s, %s, %s)
            ON CONFLICT (fact) DO UPDATE
            SET category = EXCLUDED.category,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id;
        """,
            (fact, category, created_date),
        )

        fact_id = cursor.fetchone()[0]

        # Insert into 'fact_versions' table
        cursor.execute(
            """
            INSERT INTO fact_versions (fact_id, fact, version, created_at)
            SELECT %s, %s, COALESCE(MAX(version), 0) + 1, CURRENT_TIMESTAMP
            FROM fact_versions
            WHERE fact_id = %s
        """,
            (fact_id, fact, fact_id),
        )

    connection.commit()
    cursor.close()
    connection.close()


# dag

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
