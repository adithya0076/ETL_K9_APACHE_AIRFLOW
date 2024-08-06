import pytest
from datetime import datetime
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
from airflow.operators.python import PythonOperator
from dags.scripts.k9_etl import extract_and_transform_data, load_data


@pytest.fixture()
def dag():
    dagbag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    return dagbag


def test_extract_and_transform_data(dag):
    ti = TaskInstance(
        task=PythonOperator(
            task_id="fetch_k9_data", python_callable=extract_and_transform_data, dag=dag
        ),
        execution_date=datetime.now(),
    )

    extract_and_transform_data(ti)

    xcom_value = ti.xcom_pull(task_ids="fetch_k9_data", key="k9_data")
    assert xcom_value is not None
    assert isinstance(xcom_value, list)
    assert all(isinstance(item, dict) for item in xcom_value)
