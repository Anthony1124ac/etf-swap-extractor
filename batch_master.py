import pandas as pd
import subprocess
import time

CSV_PATH = 'etf_tickers.csv'
LOG_PATH = 'batch_master_results.log'

# Read tickers from CSV
print(f"Reading tickers from {CSV_PATH}...")
df = pd.read_csv(CSV_PATH)
results = []

for idx, row in df.iterrows():
    ticker = str(row['Ticker']).upper()
    cik = str(row['CIK']).zfill(10)
    series_id = str(row['Series'])
    print(f"\nProcessing {ticker} ({idx+1}/{len(df)})...")
    try:
        result = subprocess.run(
            ['python', 'single_ticker_loader.py', ticker, cik, series_id],
            capture_output=True, text=True, timeout=1800  # 30 min per ticker
        )
        print(result.stdout)
        results.append((ticker, 'SUCCESS' if 'SUCCESS' in result.stdout else 'FAIL', result.stdout.strip()))
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT: {ticker}")
        results.append((ticker, 'TIMEOUT', ''))
    except Exception as e:
        print(f"ERROR: {ticker} - {e}")
        results.append((ticker, 'ERROR', str(e)))
    # Optional: delay to avoid SEC rate limits
    time.sleep(2)

# Write results to log file
with open(LOG_PATH, 'w') as f:
    for ticker, status, msg in results:
        f.write(f"{ticker},{status},{msg}\n")

print(f"\nBatch processing complete. Results written to {LOG_PATH}") 