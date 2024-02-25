from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.operators.python import PythonOperator

import json
import os

from google.cloud import bigquery, storage
from google.oauth2 import service_account

DAGS_FOLDER = "/opt/airflow/dags/"
DATA_FOLDER = "data"
BUSINESS_DOMAIN = "breakfast"
project_id = "mypim-410508"
location = "asia-southeast1"
data = "products"
file_path = "{DAGS_FOLDER}breakfast_products.csv"
bucket_name = "my_workshop_26"
destination_blob_name = "{BUSINESS_DOMAIN}/{data}/{data}.csv"

def _load_products_to_gcs():

    # Prepare and Load Credentials to Connect to GCP Services
    keyfile_gcs = f"{DAGS_FOLDER}mypim-410508-gcs.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    # Load data from Local to GCS
    storage_client = storage.Client(
        project=project_id,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    # data = "products"
    # file_path = f"{DATA_FOLDER}/{data}.csv"
    # file_path = f"{DAGS_FOLDER}breakfast_products.csv"
    # destination_blob_name = f"{BUSINESS_DOMAIN}/{data}/{data}.csv"

    # YOUR CODE HERE TO LOAD DATA TO GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)


def _load_products_to_bigquery():

    keyfile_bigquery = f"{DAGS_FOLDER}mypim-410508-gcs-bigquery-admin.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    # Load data from GCS to BigQuery
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

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

with DAG(
    dag_id="breakfast_product_pipeline",
    #date time object, daylight Saving Time
    start_date=timezone.datetime(2024,1,28), 
    # schedule=none,
    schedule = "0 0 * * *",
    tags=["breakfast","PIM"]
): #Dag context
    start = EmptyOperator(task_id="start") #Operator
    
    load_products_to_gcs = PythonOperator(
        task_id="load_products_to_gcs",
        python_callable=_load_products_to_gcs,
    )

    load_products_to_bigquery = PythonOperator(
        task_id="load_products_to_bigquery",
        python_callable=_load_products_to_bigquery,
    )
    
    end = EmptyOperator(task_id="end")

    start >> load_products_to_gcs >> load_products_to_bigquery >> end