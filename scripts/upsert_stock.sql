INSERT INTO indonesian_stocks (
    date, 
    ticker, 
    close, 
    high, 
    low, 
    open, 
    inserted_at_timestamp_wib
) 
VALUES (
    '{{ params.Date }}', 
    '{{ params.Ticker }}', 
    {{ params.Close }}, 
    {{ params.High }}, 
    {{ params.Low }}, 
    {{ params.Open }}, 
    '{{ params.Inserted_at_timestamp_WIB }}' 
)
ON CONFLICT (date, ticker) 
DO NOTHING;