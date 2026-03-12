from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="MyFirstDag",
    description="A simple tutorial DAG",
    schedule=None,
    start_date=datetime(2026,3,7),
    catchup=False,
) as dag:
    
    t1 = BashOperator(
        task_id="first_task",
        bash_command="echo hello from the first task",
    )
    
    t2 = BashOperator(
        task_id="second_task",
        bash_command="echo hello from the second task",
    )
    
    t1 >> t2