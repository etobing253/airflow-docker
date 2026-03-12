from airflow.decorators import dag,task
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import random

@dag(
    dag_id="dag_with_python_operator_v2",
    start_date=datetime(2026,3,7),
    catchup=False,
    schedule=None,
)
def tutorial_python_operator():
    
    start = EmptyOperator(task_id="start")
        
    @task
    def get_name():
        return("Elfraera")
    
    @task
    def get_age():
        raise Exception
        return(23)
    
    @task(trigger_rule="one_success") # ada 1 gagal tetep jalan
    def greet(name:str, age:int):
        print(f"I am {name} and age {age}")
                
    name_value=get_name() #stored di XCOM   
    age_value=get_age() #stored di XCOM
    
    start >> [name_value,age_value]
    
    greet_task=greet(name_value,age_value)
    
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ONE_SUCCESS)
    
    greet_task >> end
            
tutorial_python_operator()