import pandas as pd
import yfinance as yf
import os
from datetime import timedelta, datetime
 
def validate_tickers(tickers_input):
    """Memvalidasi ticker dan menambahkan suffix .JK."""
    if not tickers_input:
        return ["BMRI.JK", "BBRI.JK", "BBCA.JK"], []
    ticker_list = [t.strip().upper() for t in tickers_input.split(',') if t.strip()]
    idx_tickers = [t if t.endswith(".JK") else t + ".JK" for t in ticker_list]
    invalid_tickers = []
    for symbol in idx_tickers:
        if yf.Ticker(symbol).history(period="1d").empty:
            invalid_tickers.append(symbol)
    return idx_tickers, invalid_tickers
 
def fetch_yf_data(ticker_list, start_date, end_date, logical_date):
    """Mengambil data dari yfinance dan mengolahnya ke format long."""
    if not start_date or not end_date:
        # Menggunakan logical_date (data_interval_start) untuk konsistensi
        start_fetch = logical_date.strftime('%Y-%m-%d')
        data = yf.download(ticker_list, start=start_fetch, group_by='column')
    else:
        end_inclusive = (datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
        data = yf.download(ticker_list, start=start_date, end=end_inclusive, group_by='column')
 
    if data.empty:
        return []
 
    # Transformasi data
    data_filtered = data[['Close', 'High', 'Low', 'Open']]
    data_long = data_filtered.stack(level=1).reset_index()
    data_long.columns = ['Date', 'Ticker', 'Close', 'High', 'Low', 'Open']
    # Gunakan logical_date untuk timestamp agar idempotent
    data_long['Inserted_at_timestamp_WIB'] = logical_date.strftime('%Y-%m-%d %H:%M:%S')
    data_long['Date'] = data_long['Date'].dt.strftime('%Y-%m-%d')
    return data_long.to_dict(orient='records')
 
def save_partial_csv(data_dict, file_path, column_name):
    """Menyimpan kolom spesifik ke CSV parsial."""
    if not data_dict:
        return None
    df = pd.DataFrame(data_dict)
    cols = ['Date', 'Ticker', column_name, 'Inserted_at_timestamp_WIB']
    file_exists = os.path.isfile(file_path)
    df[cols].to_csv(file_path, mode='a', index=False, header=not file_exists)
    return file_path
 
def combine_stock_csv(file_paths, output_path):
    """Menggabungkan semua CSV parsial dan menghapus duplikat."""
    dfs = []
    for path in file_paths.values():
        temp_df = pd.read_csv(path)
        dfs.append(temp_df[temp_df['Date'] != 'Date']) # Bersihkan header ganda
 
    # Merge logic
    merged = dfs[0]
    for next_df in dfs[1:]:
        merged = pd.merge(merged, next_df, on=['Date', 'Ticker', 'Inserted_at_timestamp_WIB'], how='outer')
 
    cols_order = ['Date', 'Ticker', 'Close', 'High', 'Low', 'Open', 'Inserted_at_timestamp_WIB']
    merged = merged[cols_order]
 
    if os.path.isfile(output_path):
        existing_df = pd.read_csv(output_path)
        existing_df = existing_df[existing_df['Date'] != 'Date']
        merged = pd.concat([existing_df, merged]).drop_duplicates(subset=['Date', 'Ticker'])
 
    merged.to_csv(output_path, index=False)
    return output_path