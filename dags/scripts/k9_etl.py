import json
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
    url = "https://raw.githubusercontent.com/adithya0076/random-k9-etl/main/source_data.json"
    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame(data)

    # Remove duplicates
    df_unique = df.drop_duplicates(subset=["fact", "created_date"])

    df_unique["category"] = df_unique["fact"].apply(categorize_fact)

    ti.xcom_push(key="k9_data", value=df_unique.to_dict("records"))


# insert data into the Postgres database
def load_data(ti):
    k9_data = ti.xcom_pull(task_ids="fetch_k9_data", key="k9_data")
    if not k9_data:
        raise ValueError("No k9 data found")

    pg_hook = PostgresHook(postgres_conn_id="k9_connection")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    # try:
    cursor.execute("BEGIN;")
    cursor.execute("SELECT created_date FROM facts;")
    existing_ids = set(row[0] for row in cursor.fetchall())
    print("existing_ids", existing_ids)

    new_ids = set(data["created_date"] for data in k9_data)
    print("new ids", new_ids)

    filtered_ids = new_ids - existing_ids
    print("filtered_ids", filtered_ids)

    ## insert function
    for i in filtered_ids:
        for item in k9_data:
            if item["created_date"] == i:
                fact = item["fact"]
                category = item["category"]
                created_date = item["created_date"]
                print("inserting new data")
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

    # update flow
    for i in existing_ids:
        for data in k9_data:
            fact = data["fact"]
            category = data["category"]
            created_date = data["created_date"]

            if item["created_date"] == i:
                cursor.execute(
                    """
                    SELECT fact FROM facts WHERE created_date = %s;
                    """,
                    (created_date,),
                )
                existing_fact = cursor.fetchone()
                if existing_fact:
                    existing_fact = existing_fact[0]
                    if existing_fact != fact:
                        print("updating new data")
                        cursor.execute(
                            """
                            UPDATE facts
                            SET fact = %s,
                                category = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE created_date = %s
                            RETURNING id;
                            """,
                            (fact, category, created_date),
                        )

                        fact_id = cursor.fetchone()[0]

                        cursor.execute(
                            """
                            INSERT INTO fact_versions (fact_id, fact, version, created_at)
                            SELECT %s, %s, COALESCE(MAX(version), 0) + 1, CURRENT_TIMESTAMP
                            FROM fact_versions
                            WHERE fact_id = %s;
                            """,
                            (fact_id, fact, fact_id),
                        )
                print("existing_fact", existing_fact)

    connection.commit()

    # delete flow
    for i in existing_ids:
        if i not in new_ids:
            print("deleting data")
            cursor.execute(
                """
                DELETE FROM facts
                WHERE created_date = %s;
                """,
                (i,),
            )

    connection.commit()
    cursor.close()
    connection.close()
