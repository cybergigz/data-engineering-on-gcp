from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from google.cloud import bigquery, storage
from google.oauth2 import service_account

import json


def _load_transactions_to_gcs(dags_folder="/opt/airflow/dags"):
    DATA_FOLDER = "data"
    BUSINESS_DOMAIN = "breakfast"
    location = "asia-southeast1"

    # Prepare and Load Credentials to Connect to GCP Services
    keyfile_gcs = f"{dags_folder}/data-engineering-on-gcp-409709-46c582d6f434.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    project_id = "data-engineering-on-gcp-409709"

    # Load data from Local to GCS
    bucket_name = "my_workshop_43"
    storage_client = storage.Client(
        project=project_id,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    data = "transactions"
    # file_path = f"{DATA_FOLDER}/{data}.csv"
    file_path = f"{dags_folder}/breakfast_transactions.csv"
    destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"

    # YOUR CODE HERE TO LOAD DATA TO GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

    return destination_blob_name


def _load_transactions_from_gcs_to_bigquery(dags_folder="/opt/airflow/dags", **context):
    DATA_FOLDER = "data"
    BUSINESS_DOMAIN = "breakfast"
    location = "asia-southeast1"

    bucket_name = "my_workshop_43"
    data = "transactions"
    # destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"
    destination_blob_name = context["ti"].xcom_pull(task_ids="load_transactions_to_gcs", key="return_value")

    keyfile_bigquery = f"{dags_folder}/data-engineering-on-gcp-409709-bigquery-and-gcs-795734ea31e4.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    project_id = "data-engineering-on-gcp-409709"

    # # Load data from GCS to BigQuery
    bigquery_client = bigquery.Client(
        project=project_id,
        credentials=credentials_bigquery,
        location=location,
    )
    table_id = f"{project_id}.breakfast.{data}"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=location,
    )
    job.result()


with DAG(
    "breakfast_xcom_pipeline",
    schedule="@daily",
    start_date=timezone.datetime(2024, 2, 4),
    tags=["breakfast", "PIM"],
):
    start = EmptyOperator(task_id="start")

    load_transactions_to_gcs = PythonOperator(
        task_id="load_transactions_to_gcs",
        python_callable=_load_transactions_to_gcs,
    )

    load_transactions_from_gcs_to_bigquery = PythonOperator(
        task_id="load_transactions_from_gcs_to_bigquery",
        python_callable=_load_transactions_from_gcs_to_bigquery,
    )

    end = EmptyOperator(task_id="end")

    start >> load_transactions_to_gcs >> load_transactions_from_gcs_to_bigquery >> end