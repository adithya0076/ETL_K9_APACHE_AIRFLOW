import requests
import pandas as pd
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