from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils import timezone

import pandas as pd

default_args ={
    "start_date": timezone.datetime(2024, 2,25),
    "owner": "Surasak Suksathit",
}

# function for task 1
def _extract_from_mysql():
    hook = MySqlHook(mysql_conn_id="pim_mysql_conn")
    # hook.bulk_dump("titanic", "/opt/airflow/dags/titanic_dump.tsv")
    conn = hook.get_conn()

    df = pd.read_sql("select * from titanic", con=conn)
    print(df.head())
    df.to_csv("/opt/airflow/dags/titanic_dump.csv", header=None, index=False)

with DAG(
    "titanic_sql_to_bigquery_pipeline",
    default_args=default_args,
    schedule=None,
    tags=["titanic","mysql","bigqurey"],
):

    # task 1
    extract_from_mysql = PythonOperator(
        task_id="extract_from_mysql",
        python_callable=_extract_from_mysql,
    )

    # task 2
    load_to_gcs = EmptyOperator(task_id="load_to_gcs")
    load_from_gcs_to_bigquery = EmptyOperator(task_id="load_fromgcs_to_bigqurey")

    # task 3
    extract_from_mysql >> load_to_gcs >> load_from_gcs_to_bigquery