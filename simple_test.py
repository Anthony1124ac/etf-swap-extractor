#!/usr/bin/env python3
import sys
import os
import logging
from datetime import datetime

# Set up basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("=" * 50)
    logger.info("SIMPLE TEST SCRIPT STARTING")
    logger.info("=" * 50)
    
    # Test 1: Basic environment
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Current directory: {os.getcwd()}")
    logger.info(f"Files in directory: {os.listdir('.')}")
    
    # Test 2: Check if CSV exists
    if os.path.exists('etf_tickers.csv'):
        logger.info("✓ etf_tickers.csv found")
        import pandas as pd
        df = pd.read_csv('etf_tickers.csv')
        logger.info(f"✓ CSV loaded successfully: {len(df)} rows")
        logger.info(f"First few tickers: {df['Ticker'].head().tolist()}")
    else:
        logger.error("✗ etf_tickers.csv not found")
        return
    
    # Test 3: Check database connection
    try:
        from etf_db import get_db_connection
        conn = get_db_connection()
        logger.info("✓ Postgres connection successful")
        conn.close()
    except Exception as e:
        logger.error(f"✗ Postgres connection failed: {e}")
    
    # Test 4: Test extractor import
    try:
        from etf_swap_extractor_manual import ETFSwapDataExtractor
        extractor = ETFSwapDataExtractor()
        logger.info("✓ ETFSwapDataExtractor imported successfully")
    except Exception as e:
        logger.error(f"✗ ETFSwapDataExtractor import failed: {e}")
        return
    
    # Test 5: Process one ticker
    logger.info("Testing with first ticker...")
    try:
        first_ticker = df.iloc[0]
        ticker = first_ticker['Ticker']
        cik = str(first_ticker['CIK']).zfill(10)
        series_id = str(first_ticker['Series'])
        
        logger.info(f"Processing: {ticker} (CIK: {cik}, Series: {series_id})")
        extractor.process_ticker(ticker, cik, series_id=series_id)
        logger.info(f"✓ Successfully processed {ticker}")
        
    except Exception as e:
        logger.error(f"✗ Failed to process first ticker: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    logger.info("=" * 50)
    logger.info("SIMPLE TEST COMPLETE")
    logger.info("=" * 50)

if __name__ == "__main__":
    main() 