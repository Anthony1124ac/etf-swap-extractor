import pandas as pd
from etf_swap_extractor_manual import ETFSwapDataExtractor

# Load tickers from CSV
csv_path = 'etf_tickers.csv'
df = pd.read_csv(csv_path)
tickers = df['Ticker'].astype(str).str.upper().tolist()
ciks = df['CIK'].astype(str).str.zfill(10).tolist()
series_ids = df['Series'].astype(str).tolist()

extractor = ETFSwapDataExtractor()

for ticker, cik, series_id in zip(tickers, ciks, series_ids):
    print(f"Processing {ticker} (CIK: {cik}, Series: {series_id})")
    try:
        extractor.process_ticker(ticker, cik, series_id=series_id)
    except Exception as e:
        print(f"Error processing {ticker}: {e}") 