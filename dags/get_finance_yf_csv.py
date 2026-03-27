from airflow.decorators import dag,task,task_group
from datetime import date, datetime, timedelta, timezone
import pandas as pd
import yfinance as yf
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import os

@dag(
    dag_id="dag_get_data_from_yf",
    start_date=datetime(2026,3,1),
    catchup=False,
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
    files = {
        'file_path_close': '/opt/airflow/scripts/saham_indonesia_close.csv',
        'file_path_high': '/opt/airflow/scripts/saham_indonesia_high.csv',
        'file_path_low': '/opt/airflow/scripts/saham_indonesia_low.csv',
        'file_path_open': '/opt/airflow/scripts/saham_indonesia_open.csv',
    }
    
    misc = {
        'file_path_all': '/opt/airflow/scripts/saham_indonesia_all.csv',
    }
    
    start = EmptyOperator(task_id="start")    
    
    def get_data_from_yf_child(ticker_list:list, start_date:date, end_date:date, **context):
        data = None
        # original_schedule_date = datetime.now(timezone(timedelta(hours=7)))
        original_schedule_date = datetime.now()
                
        if start_date is None and end_date is None: #scheduled event
            data = yf.download(ticker_list, start=original_schedule_date.strftime('%Y-%m-%d'), group_by='column')
            print("scheduled")
            print(f"DEBUG: Row count fetched: {len(data)}")
            print(f"DEBUG: Data content: {data}")
        elif start_date is not None and end_date is not None: #triggered event
            end_inclusive = (datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            data = yf.download(ticker_list, start=start_date, end=end_inclusive, group_by='column')
            print("triggered")
            print(f"DEBUG: Row count fetched: {len(data)}")
            print(f"DEBUG: Data content: {data}")
                            
        data_filtered = data[['Close', 'High', 'Low', 'Open']]
        print("data_filtered: ", data_filtered)
        data_long = data_filtered.stack(level=1).reset_index()
        data_long.columns = ['Date', 'Ticker', 'Close', 'High', 'Low', 'Open']
        print("data_long.columns before Inserted_at_timestamp: ", data_long.columns)
        data_long['Inserted_at_timestamp_WIB'] = original_schedule_date.strftime('%Y-%m-%d %H:%M:%S') 
        
        data_long['Date'] = data_long['Date'].dt.strftime('%Y-%m-%d')
        print("data_long.columns: ", data_long.columns)
        
        return data_long.to_dict(orient='records')
        
    @task
    def get_data_from_yf(**context): 
        tickers_input = context['params'].get('tickers')
        start_date_input = context['params'].get('start_date')
        end_date_input = context['params'].get('end_date')
        print(f"Ini tickers user: {tickers_input}")
        print(f"Ini start date user: {start_date_input}") 
        print(f"Ini end date user: {end_date_input}")            
        
        if tickers_input is None: #saham default
            get_tickers_default = get_data_from_yf_child(["BMRI.JK", "BBRI.JK", "BBCA.JK"], start_date_input, end_date_input, **context)
            return get_tickers_default
        
        #saham custom
        #handle saham not exists di yf dulu
        ticker_list = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]
        idx_tickers = [ticker + ".JK" for ticker in ticker_list]
        invalid_tickers = []
        
        for ticker_symbol in idx_tickers:
            ticker_data = yf.Ticker(ticker_symbol)
            
            # Cara paling valid mengecek keberadaan ticker adalah dengan mencoba 
            # mengambil data history singkat (misal 1 hari). 
            # Jika kosong, berarti ticker tidak ditemukan atau tidak aktif.
            history = ticker_data.history()
            
            if history.empty:
                invalid_tickers.append(ticker_symbol)    
        
        if invalid_tickers:
        # Menghentikan eksekusi jika ada ticker yang bermasalah
            return {
                "status": "error",
                "message": f"Ticker tidak ditemukan di Yahoo Finance: {', '.join(invalid_tickers)}",
                "invalid_count": len(invalid_tickers)
            }
        
        get_tickers_custom = get_data_from_yf_child(idx_tickers, start_date_input, end_date_input, **context)
        print("ingest dari yf: ", idx_tickers)
        return get_tickers_custom
    
    @task_group(group_id='olhc_branching')
    def olhc_branching(data_from_yf: dict):    
        @task
        def get_csv_open(data_dict: dict):
            if data_dict is None:
                raise ValueError("Data dictionary kosong!") # Hindari error jika input kosong
            
            df = pd.DataFrame(data_dict)
            file_exists = os.path.isfile(files["file_path_open"])
            df[['Date', 'Ticker', 'Open', 'Inserted_at_timestamp_WIB']].to_csv(files["file_path_open"], mode='a', index=False, header=not file_exists)
            print(f"Data saved to {files["file_path_open"]}")
            
            # disimpan di xcom (postgres)
            return files["file_path_open"]
        
        @task
        def get_csv_low(data_dict: dict):
            if data_dict is None:
                raise ValueError("Data dictionary kosong!") # Hindari error jika input kosong
            
            df = pd.DataFrame(data_dict)
            file_exists = os.path.isfile(files["file_path_low"])
            df[['Date', 'Ticker', 'Low', 'Inserted_at_timestamp_WIB']].to_csv(files["file_path_low"], mode='a', index=False, header=not file_exists)
            print(f"Data saved to {files["file_path_low"]}")
            
            # disimpan di xcom (postgres)
            return files["file_path_low"]
        
        @task
        def get_csv_high(data_dict: dict):
            if data_dict is None:
                raise ValueError("Data dictionary kosong!") # Hindari error jika input kosong
            
            df = pd.DataFrame(data_dict)
            file_exists = os.path.isfile(files["file_path_high"])
            df[['Date', 'Ticker', 'High', 'Inserted_at_timestamp_WIB']].to_csv(files["file_path_high"], mode='a', index=False, header=not file_exists)
            print(f"Data saved to {files["file_path_high"]}")
            
            # disimpan di xcom (postgres)
            return files["file_path_high"]
        
        @task
        def get_csv_close(data_dict: dict):
            if data_dict is None:
                raise ValueError("Data dictionary kosong!") # Hindari error jika input kosong
    
            df = pd.DataFrame(data_dict)
            file_exists = os.path.isfile(files["file_path_close"])
            df[['Date', 'Ticker', 'Close', 'Inserted_at_timestamp_WIB']].to_csv(files["file_path_close"], mode='a', index=False, header=not file_exists)
            print(f"Data saved to {files["file_path_close"]}")
            
            # disimpan di xcom (postgres)
            return files["file_path_close"]

        get_csv_open(data_from_yf) #stored di XCOM as csv
        get_csv_low(data_from_yf) #stored di XCOM as csv
        get_csv_high(data_from_yf) #stored di XCOM as csv
        get_csv_close(data_from_yf) #stored di XCOM as csv
    
    @task(trigger_rule="all_success") # all branch di upstream sukses dulu
    def combine_csv(): 
        # VALIDASI: Cek jika ada path yang None
        for key, path in files.items():
            if path is None:
                raise ValueError(f"Task get_csv_{key} tidak mengirimkan path (None). Cek logs task tersebut!")
            if not os.path.exists(path):
                raise FileNotFoundError(f"File {path} tidak ditemukan di disk.")
        
        # Baca data dan BERSIHKAN header duplikat yang mungkin masuk di file individu
        def read_and_clean(p):
            temp_df = pd.read_csv(p)
            # Menghapus baris yang isinya nama kolom (akibat double header)
            return temp_df[temp_df['Date'] != 'Date']
        
        df_open = read_and_clean(files["file_path_open"])
        df_low = read_and_clean(files["file_path_low"])
        df_high = read_and_clean(files["file_path_high"])
        df_close = read_and_clean(files["file_path_close"])
        
        merged = pd.merge(df_open, df_low, on=['Date', 'Ticker', 'Inserted_at_timestamp_WIB'], how='outer')
        merged = pd.merge(merged, df_high, on=['Date', 'Ticker', 'Inserted_at_timestamp_WIB'], how='outer')
        merged = pd.merge(merged, df_close, on=['Date', 'Ticker', 'Inserted_at_timestamp_WIB'], how='outer')
        cols_order = ['Date', 'Ticker', 'Close', 'High', 'Low', 'Open', 'Inserted_at_timestamp_WIB']
        merged = merged[cols_order]
        
        file_exists = os.path.isfile(misc["file_path_all"])
        if file_exists:
            existing_df = pd.read_csv(misc["file_path_all"])
            existing_df = existing_df[existing_df['Date'] != 'Date'] # Bersihkan jika ada header di selain baris 0
            final_df = pd.concat([existing_df, merged]).drop_duplicates(subset=['Date', 'Ticker'])
            final_df.to_csv(misc["file_path_all"], index=False)
        else:            
            merged.to_csv(misc["file_path_all"], index=False)

        print(f"Berhasil menggabungkan data ke {misc["file_path_all"]}")
        
        return misc["file_path_all"] 
    
    query_create = SQLExecuteQueryOperator(
        task_id='create_table_task',
        conn_id="db_custom",
        sql=""" CREATE TABLE IF NOT EXISTS indonesian_stocks (
                date DATE,
                ticker VARCHAR(7),
                close FLOAT,
                high FLOAT,
                low FLOAT,
                open FLOAT,
                inserted_at_timestamp_wib TIMESTAMP,
                PRIMARY KEY (date, ticker)
            ); """,
    )
          
    query_select = SQLExecuteQueryOperator(
        task_id='select_data_task',
        conn_id="db_custom",
        sql=" SELECT * FROM indonesian_stocks; ",
    )  
               
    get_data_task = get_data_from_yf()
    
    olhc_branching_task = olhc_branching(get_data_task)
    
    combine_csv_task = combine_csv()
    
    def prepare_data_csv_all(trigger_from_combine):
        csv_all = misc["file_path_all"]
        file_exists = os.path.isfile(csv_all)
        if file_exists:
            df = pd.read_csv(csv_all)
            return df.to_dict(orient='records')
        
        return []
    
    csv_ready = prepare_data_csv_all(combine_csv_task)
    
    query_insert = SQLExecuteQueryOperator.partial(
        task_id='insert_data_task',
        conn_id="db_custom",
        sql=""" INSERT INTO indonesian_stocks (date, ticker, close, high, low, open, inserted_at_timestamp_wib) 
                VALUES ('{{ params.Date }}', '{{ params.Ticker }}', '{{ params.Close }}', '{{ params.High }}', '{{ params.Low }}', '{{ params.Open }}', '{{ params.Inserted_at_timestamp_WIB }}' )
                ON CONFLICT (date, ticker) DO NOTHING; """,
    ).expand(params=csv_ready)
                      
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)
    
    start >> get_data_task >> olhc_branching_task >> combine_csv_task >> query_create >> query_insert >> query_select >> end
    
get_data_from_yf_to_csv()