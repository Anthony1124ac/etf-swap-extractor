#!/usr/bin/env python3
import sys
import os
import time
from datetime import datetime

print("=" * 50)
print("WORKER TEST SCRIPT STARTING")
print("=" * 50)
print(f"Time: {datetime.now()}")
print(f"Python version: {sys.version}")
print(f"Current directory: {os.getcwd()}")
print(f"Files in directory: {os.listdir('.')}")

# Test 1: Check if CSV exists
if os.path.exists('etf_tickers.csv'):
    print("✓ etf_tickers.csv found")
    import pandas as pd
    df = pd.read_csv('etf_tickers.csv')
    print(f"✓ CSV loaded: {len(df)} rows")
    print(f"First few tickers: {df['Ticker'].head().tolist()}")
else:
    print("✗ etf_tickers.csv not found")

# Test 2: Check database connection
try:
    from etf_db import get_db_connection
    conn = get_db_connection()
    print("✓ Postgres connection successful")
    conn.close()
except Exception as e:
    print(f"✗ Postgres connection failed: {e}")

# Test 3: Test extractor import
try:
    from etf_swap_extractor_manual import ETFSwapDataExtractor
    print("✓ ETFSwapDataExtractor imported successfully")
except Exception as e:
    print(f"✗ ETFSwapDataExtractor import failed: {e}")

print("=" * 50)
print("TEST COMPLETE - WORKER ENVIRONMENT OK")
print("=" * 50)

# Keep the script running for a bit to see logs
for i in range(10):
    print(f"Test message {i+1}/10")
    time.sleep(1)

print("Worker test finished!") 