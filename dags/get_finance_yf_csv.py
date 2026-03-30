from airflow.decorators import dag, task, task_group
from datetime import date, datetime, timedelta, timezone
import pandas as pd
import yfinance as yf
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import os
import sys
import pendulum
from jinja2 import Environment, FileSystemLoader

# Add scripts directory to path for importing utilities
sys.path.append('/opt/airflow/scripts')
from finance_utils import validate_tickers, fetch_yf_data, save_partial_csv, combine_stock_csv, read_sql_file

# Constants
FILES = {
    'file_path_close': '/opt/airflow/scripts/saham_indonesia_close.csv',
    'file_path_high': '/opt/airflow/scripts/saham_indonesia_high.csv',
    'file_path_low': '/opt/airflow/scripts/saham_indonesia_low.csv',
    'file_path_open': '/opt/airflow/scripts/saham_indonesia_open.csv',
}

MISC = {
    'file_path_all': '/opt/airflow/scripts/saham_indonesia_all.csv',
}

local_tz = pendulum.timezone("Asia/Jakarta")

# Set up Jinja environment
env = Environment(
    loader=FileSystemLoader('/opt/airflow/scripts'),
    autoescape=False
)

template_create = env.get_template('create_table.sql')
# template_insert = env.get_template('upsert_stock.sql')
template_select = env.get_template('select_stocks.sql')

SQL_CREATE = template_create.render()
# SQL_INSERT = template_insert.render()
SQL_SELECT = template_select.render()

# SQL_CREATE = read_sql_file('/opt/airflow/scripts/create_table.sql')
# SQL_SELECT = read_sql_file('/opt/airflow/scripts/select_stocks.sql')
SQL_INSERT = read_sql_file('/opt/airflow/scripts/upsert_stock.sql')

@dag(
    dag_id="dag_get_data_from_yf",
    start_date=datetime(2026,3,1,tzinfo=local_tz),
    catchup=False,
    # template_searchpath=["/opt/airflow/scripts"],
    schedule="0 17 * * 1-5", # Setiap hari kerja pukul 17:00 WIB
    params={
        "tickers": Param(None, type=["null", "string"], description="""Masukkan kode saham.\n
                         Jika mau default, kosongkan.\n
                         Jika multiple, pisahkan dengan koma (misal: BMRI,BBCA,BBRI)."""),
        "start_date": Param(None, type=["null", "string"], format="date", description="Pilih start date"),
        "end_date": Param(None, type=["null", "string"], format="date", description="Pilih end date")
    },
)
def get_data_from_yf_to_csv():
    
    start = EmptyOperator(task_id="start")    
    
    @task
    def get_data_from_yf(**context):
        tickers_input = context['params'].get('tickers')
        start_date_input = context['params'].get('start_date')
        end_date_input = context['params'].get('end_date')
        # logical_date = context['logical_date']
        start_date_scheduled = datetime.now(local_tz)

        print(f"Ini tickers user: {tickers_input}")
        print(f"Ini start date user: {start_date_input}")
        print(f"Ini end date user: {end_date_input}")

        # Validate tickers using utility function
        valid_tickers, invalid_tickers = validate_tickers(tickers_input)

        if invalid_tickers:
            return {
                "status": "error",
                "message": f"Ticker tidak ditemukan di Yahoo Finance: {', '.join(invalid_tickers)}",
                "invalid_count": len(invalid_tickers)
            }

        # Fetch data using utility function
        data = fetch_yf_data(valid_tickers, start_date_input, end_date_input, start_date_scheduled)
        print(f"ingest dari yf: {valid_tickers}")
        return data
    
    @task_group(group_id='olhc_branching')
    def olhc_branching(data_from_yf: dict):
        @task
        def get_csv_open(data_dict: dict):
            if data_dict is None:
                raise ValueError("Data dictionary kosong!") # Hindari error jika input kosong

            file_path = save_partial_csv(data_dict, FILES["file_path_open"], 'Open')
            print(f"Data saved to {file_path}")

            # disimpan di xcom (postgres)
            return file_path

        @task
        def get_csv_low(data_dict: dict):
            if data_dict is None:
                raise ValueError("Data dictionary kosong!") # Hindari error jika input kosong

            file_path = save_partial_csv(data_dict, FILES["file_path_low"], 'Low')
            print(f"Data saved to {file_path}")

            # disimpan di xcom (postgres)
            return file_path

        @task
        def get_csv_high(data_dict: dict):
            if data_dict is None:
                raise ValueError("Data dictionary kosong!") # Hindari error jika input kosong

            file_path = save_partial_csv(data_dict, FILES["file_path_high"], 'High')
            print(f"Data saved to {file_path}")

            # disimpan di xcom (postgres)
            return file_path

        @task
        def get_csv_close(data_dict: dict):
            if data_dict is None:
                raise ValueError("Data dictionary kosong!") # Hindari error jika input kosong

            file_path = save_partial_csv(data_dict, FILES["file_path_close"], 'Close')
            print(f"Data saved to {file_path}")

            # disimpan di xcom (postgres)
            return file_path

        get_csv_open(data_from_yf) #stored di XCOM as csv
        get_csv_low(data_from_yf) #stored di XCOM as csv
        get_csv_high(data_from_yf) #stored di XCOM as csv
        get_csv_close(data_from_yf) #stored di XCOM as csv
    
    @task(trigger_rule="all_success") # all branch di upstream sukses dulu
    def combine_csv():
        # VALIDASI: Cek jika ada path yang None
        for key, path in FILES.items():
            if path is None:
                raise ValueError(f"Task get_csv_{key} tidak mengirimkan path (None). Cek logs task tersebut!")
            if not os.path.exists(path):
                raise FileNotFoundError(f"File {path} tidak ditemukan di disk.")

        # Use utility function to combine CSV files
        combined_file = combine_stock_csv(FILES, MISC["file_path_all"])
        print(f"Berhasil menggabungkan data ke {combined_file}")

        return combined_file 
    
    query_create = SQLExecuteQueryOperator(
        task_id='create_table_task',
        conn_id="db_custom",
        sql=SQL_CREATE,
    )

    query_select = SQLExecuteQueryOperator(
        task_id='select_data_task',
        conn_id="db_custom",
        sql=SQL_SELECT,
    )  
               
    get_data_task = get_data_from_yf()
    
    olhc_branching_task = olhc_branching(get_data_task)
    
    combine_csv_task = combine_csv()
    
    def prepare_data_csv_all(trigger_from_combine):
        csv_all = MISC["file_path_all"]
        file_exists = os.path.isfile(csv_all)
        if file_exists:
            df = pd.read_csv(csv_all)
            return df.to_dict(orient='records')

        return []
    
    csv_ready = prepare_data_csv_all(combine_csv_task)
    
    query_insert = SQLExecuteQueryOperator.partial(
        task_id='insert_data_task',
        conn_id="db_custom",
        sql=SQL_INSERT,
        map_index_template="{{ task.params.Ticker }} - {{ task.params.Date }}",
    ).expand(params=csv_ready) #expand: paramnya ticker-date
                      
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)
    
    start >> get_data_task >> olhc_branching_task >> combine_csv_task >> query_create >> query_insert >> query_select >> end
    
get_data_from_yf_to_csv()
