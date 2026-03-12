from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="execute_shell_script",
    start_date=datetime(2026,3,7),
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    
    execute_script_first = BashOperator(
        task_id="execute_script_first",
        bash_command=r"{{'sh /opt/airflow/scripts/SampleShellScript.sh /opt/airflow/scripts/task_output1.txt'}}"
    )
    
    execute_script_second = BashOperator(
        task_id="execute_script_second",
        bash_command=r"{{'sh /opt/airflow/scripts/SampleShellScript.sh /opt/airflow/scripts/task_output2.txt'}}"
    )
    
    execute_script_first >> execute_script_second