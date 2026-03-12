from airflow.decorators import dag,task
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.http.operators.http import HttpOperator

@dag(
    dag_id="dag_with_http_operator",
    start_date=datetime(2026,3,9),
    catchup=False,
    schedule=None,
)
def tutorial_http_operator():
    call_api=HttpOperator(
        task_id="get_post",
        http_conn_id="http_default",
        endpoint="jsonplaceholder.typicode.com/todos/1",
        method="GET",
        response_filter=lambda response: response.json(),
        log_response=True,
    )
    
    @task
    def process_response(api_response):
        print(f"Judulnya: {api_response['title']}")
    
    start = EmptyOperator(task_id="start")
    start >> call_api
    process_task=process_response(call_api.output)       
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)
    process_task >> end            
                
tutorial_http_operator()
