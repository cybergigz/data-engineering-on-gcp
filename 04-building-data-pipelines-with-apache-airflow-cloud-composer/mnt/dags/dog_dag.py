from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import json
import requests

DAGS_FOLDER = "/"


def _get_dog_image_url():
    url = "https://dog.ceo/api/breeds/image/random"
    response = requests.get(url)
    data = response.json()

    # Your code here
    # print(data)

    with open("/opt/airflow/dags/dogs.json", "w") as f:
        json.dump(data,f)

with DAG(
    dag_id="dog_dag",
    #date time object, daylight Saving Time
    start_date=timezone.datetime(2024,1,28), 
    # schedule=none,
    schedule = "*/30 * * * *",
    catchup=False,
): #Dag context

    start = EmptyOperator(task_id="start_task") #Operator

    get_dog_image_url = PythonOperator(
        task_id="get_dog_image_url",
        python_callable=_get_dog_image_url,
    )
    
    load_to_jsonbin = BashOperator(
        task_id="load_to_jsonbin",
        bash_command="""
            # API_KEY='$2a$10$XrPmvZkaJ0sXYbP.vIfQaOi7QQ1hoip.5//mFNa696/Cs1R58L25q'
            # COLLECTION_ID='659a4d15266cfc3fde739a64'
            API_KEY={{{{var.value.jsonbin_api_key}}}}
            COLLECTION_ID={{{{var.value.jsonbin_dog_collection_id}}}}

            curl -XPOST \
                -H "Content-type: application/json" \
                -H "X-Master-Key: $API_KEY" \
                -H "X-Collection-Id: $COLLECTION_ID" \
                -d @{DAGS_FOLDER}dogs.json \
                "https://api.jsonbin.io/v3/b"
            """,
    )

    end = EmptyOperator(task_id="end_task")

    start >> get_dog_image_url >> load_to_jsonbin >> end