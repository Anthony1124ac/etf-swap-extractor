#!/usr/bin/env python3
import sys
import os
import time
from datetime import datetime

print("=" * 60)
print("DEBUG WORKER STARTING")
print("=" * 60)
print(f"Time: {datetime.now()}")

# Step 1: Basic imports
print("Step 1: Testing basic imports...")
try:
    import pandas as pd
    print("✓ pandas imported")
except Exception as e:
    print(f"✗ pandas import failed: {e}")
    sys.exit(1)

# Step 2: Load CSV
print("Step 2: Loading CSV...")
try:
    csv_path = 'etf_tickers.csv'
    print(f"Looking for CSV at: {csv_path}")
    print(f"Current directory: {os.getcwd()}")
    print(f"Files in directory: {os.listdir('.')}")
    
    if os.path.exists(csv_path):
        print("✓ CSV file found")
        df = pd.read_csv(csv_path)
        print(f"✓ CSV loaded: {len(df)} rows")
        tickers = df['Ticker'].astype(str).str.upper().tolist()
        ciks = df['CIK'].astype(str).str.zfill(10).tolist()
        series_ids = df['Series'].astype(str).tolist()
        print(f"✓ Data extracted: {len(tickers)} tickers")
    else:
        print("✗ CSV file not found")
        sys.exit(1)
except Exception as e:
    print(f"✗ CSV loading failed: {e}")
    sys.exit(1)

# Step 3: Test database connection
print("Step 3: Testing database connection...")
try:
    from etf_db import get_db_connection
    conn = get_db_connection()
    print("✓ Postgres connection successful")
    conn.close()
except Exception as e:
    print(f"✗ Postgres connection failed: {e}")
    sys.exit(1)

# Step 4: Test extractor import
print("Step 4: Testing extractor import...")
try:
    from etf_swap_extractor_manual import ETFSwapDataExtractor
    print("✓ ETFSwapDataExtractor imported")
except Exception as e:
    print(f"✗ ETFSwapDataExtractor import failed: {e}")
    sys.exit(1)

# Step 5: Test extractor initialization
print("Step 5: Testing extractor initialization...")
try:
    extractor = ETFSwapDataExtractor()
    print("✓ Extractor initialized")
except Exception as e:
    print(f"✗ Extractor initialization failed: {e}")
    sys.exit(1)

# Step 6: Test processing first ticker
print("Step 6: Testing first ticker processing...")
try:
    first_ticker = tickers[0]
    first_cik = ciks[0]
    first_series = series_ids[0]
    
    print(f"Testing with: {first_ticker} (CIK: {first_cik}, Series: {first_series})")
    
    # Clear data first
    print("Clearing existing data...")
    extractor.clear_ticker_data(first_ticker)
    print("✓ Data cleared")
    
    # Process ticker
    print("Starting ticker processing...")
    extractor.process_ticker(first_ticker, first_cik, series_id=first_series)
    print(f"✓ Successfully processed {first_ticker}")
    
except Exception as e:
    print(f"✗ First ticker processing failed: {e}")
    import traceback
    print(f"Traceback: {traceback.format_exc()}")
    sys.exit(1)

print("=" * 60)
print("DEBUG WORKER COMPLETE - ALL TESTS PASSED")
print("=" * 60) 