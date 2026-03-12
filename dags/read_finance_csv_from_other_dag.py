from airflow.decorators import dag,task
from datetime import datetime
import pandas as pd
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

@dag(
    dag_id="dag_load_data_from_csv",
    start_date=datetime(2026,3,10),
    catchup=False,
    schedule=None,
)
def load_csv_file():
    file_path_all = "/opt/airflow/scripts/saham_indonesia_all.csv"
   
    @task
    def read_csv_file(file_path_all:str):
        df = pd.read_csv(file_path_all)
        
        print(df.head())
        return file_path_all
    
    start = EmptyOperator(task_id="start")
            
    load_csv_combined_task=read_csv_file(file_path_all) #stored di XCOM
    # notes: jgn simpan dataframe raksasa langsung ke xcom, memberatkan DB
    
    start >> load_csv_combined_task
    
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)
    
    load_csv_combined_task >> end
     
load_csv_file()