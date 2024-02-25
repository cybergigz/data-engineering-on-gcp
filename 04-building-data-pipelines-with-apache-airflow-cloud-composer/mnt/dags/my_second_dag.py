from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

def _world():
    print("world");

with DAG(
    dag_id="my_second_dag",
    #date time object, daylight Saving Time
    start_date=timezone.datetime(2024,1,22), 
    # schedule=none,
    schedule = "0 0 * * *"
): #Dag context
    start = EmptyOperator(task_id="start") #Operator
    
    hello = BashOperator(
            task_id="hello",
            bash_command="echo 'hello'",
    )
    
    world = PythonOperator(
            task_id="world",
            python_callable=_world,
    )

    end = EmptyOperator(task_id="end")
    
    #start >> hello >> world >> end
    # start >> hello >> end
    # start >> world >> end
    start >> [hello, world] >> end