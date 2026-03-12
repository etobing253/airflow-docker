from airflow.decorators import dag,task
from datetime import date, datetime, timedelta
import pandas as pd
import yfinance as yf
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from io import StringIO
from airflow.models.param import Param

@dag(
    dag_id="dag_get_data_from_yf",
    start_date=datetime(2026,3,11),
    catchup=False,
    schedule=None, #"*/1 * * * *",
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
        'raw_data_path': '/opt/airflow/scripts/saham_indonesia_raw.csv',
    }
    
    labels = ['Close', 'High', 'Low', 'Open']
    
    start = EmptyOperator(task_id="start")      
    
    def get_data_from_yf_child(ticker_list:list, start_date:date, end_date:date):
        if start_date is None and end_date is None: #jika start & end kosong: periode 1 thn terakhir
            data = yf.download(ticker_list, period="1y", group_by='column')
            data = data.drop(columns=['Volume'], level=0)
            raw_data=data.to_csv(misc["raw_data_path"], index=True)
            print(raw_data)
            return data
        elif start_date is not None and end_date is None: #jika end saja yang kosong: start-now
            data = yf.download(ticker_list, start=start_date, group_by='column')
            data = data.drop(columns=['Volume'], level=0)
            raw_data=data.to_csv(misc["raw_data_path"], index=True)
            print(raw_data)
            return data
        elif start_date is not None and end_date is not None: #jika start & end tidak kosong: start-end
            end_inclusive = (datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            data = yf.download(ticker_list, start=start_date, end=end_inclusive, group_by='column')
            data = data.drop(columns=['Volume'], level=0)
            raw_data=data.to_csv(misc["raw_data_path"], index=True)
            print(raw_data)
            return data
        
    @task
    def get_data_from_yf(**context): 
        tickers_input = context['params'].get('tickers')
        start_date_input = context['params'].get('start_date')
        end_date_input = context['params'].get('end_date')
        print(f"Ini tickers user: {tickers_input}")
        print(f"Ini start date user: {start_date_input}") 
        print(f"Ini end date user: {end_date_input}")            
        
        if tickers_input is None: #saham default
            get_tickers_default = get_data_from_yf_child(["BMRI.JK", "BBRI.JK", "BBCA.JK"], start_date_input, end_date_input)
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
        
        get_tickers_custom = get_data_from_yf_child(idx_tickers, start_date_input, end_date_input)
        return get_tickers_custom
        
    @task
    def get_csv_open(data:pd.DataFrame):
        csv_string_open=data['Open'].to_csv(files["file_path_open"], index=True)
        print(csv_string_open)
        
        # disimpan di xcom (postgres)
        return files["file_path_open"]
    
    @task
    def get_csv_low(data:pd.DataFrame):
        csv_string_low=data['Low'].to_csv(files["file_path_low"], index=True)
        print(csv_string_low)
        
        # disimpan di xcom (postgres)
        return files["file_path_low"]
    
    @task
    def get_csv_high(data:pd.DataFrame):
        csv_string_high=data['High'].to_csv(files["file_path_high"], index=True)
        print(csv_string_high)
        
        # disimpan di xcom (postgres)
        return files["file_path_high"]
    
    @task
    def get_csv_close(data:pd.DataFrame):
        csv_string_close=data['Close'].to_csv(files["file_path_close"], index=True)
        print(csv_string_close)
        
        # disimpan di xcom (postgres)
        return files["file_path_close"]
    
    @task(trigger_rule="all_success") # all branch sukses dulu
    def combine_csv():
        dfs = []
        
        for f in files:
            df = pd.read_csv(files[f], index_col=0) #Date di kolom pertama dan jadi index
            dfs.append(df)
            
        combined_df = pd.concat(dfs, axis=1, keys=labels)
        combined_df.columns.names = ['Price', 'Ticker']
        combined_df.index.name = 'Date'
        
        csv_string_all = combined_df.to_csv(misc["file_path_all"], index=True)

        print(csv_string_all)
        
        return misc["file_path_all"]

    # xcom simpan path aja
    # menunggu DAG pertama selesai
    trigger_dag2 = TriggerDagRunOperator(
        task_id='trigger_load_data_dag',
        trigger_dag_id='dag_load_data_from_csv',
        wait_for_completion=False, # lgsg done after trigger, gk nunggu sampai dag 2 selesai, hemat resource/slot worker
        poke_interval=60 # cek setiap 1 mnt
    )
       
    get_data_task = get_data_from_yf()
    
    csv_open=get_csv_open(get_data_task) #stored di XCOM as csv
    csv_low=get_csv_low(get_data_task) #stored di XCOM as csv
    csv_high=get_csv_high(get_data_task) #stored di XCOM as csv
    csv_close=get_csv_close(get_data_task) #stored di XCOM as csv
    
    combine_csv_task = combine_csv()
                   
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)
    
    start >> get_data_task >> [csv_close,csv_high,csv_low,csv_open] >> combine_csv_task >> trigger_dag2 >> end
    
    # XCOM disimpan di postgres, tapi ada batasan ukuran
    
get_data_from_yf_to_csv()