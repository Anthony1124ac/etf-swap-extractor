import pandas as pd
import logging
import time
import sys
from datetime import datetime
import traceback
from etf_swap_extractor_manual import ETFSwapDataExtractor

# Set up logging for background worker
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('simple_batch_worker.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Main function for simple batch processing without timeouts"""
    logger.info("=" * 80)
    logger.info("SIMPLE BATCH WORKER STARTING")
    logger.info("=" * 80)
    
    # Load tickers from CSV
    csv_path = 'etf_tickers.csv'
    logger.info(f"Loading tickers from {csv_path}")
    
    try:
        df = pd.read_csv(csv_path)
        tickers = df['Ticker'].astype(str).str.upper().tolist()
        ciks = df['CIK'].astype(str).str.zfill(10).tolist()
        series_ids = df['Series'].astype(str).tolist()
        logger.info(f"Loaded {len(tickers)} tickers from CSV")
    except Exception as e:
        logger.error(f"Error loading CSV file: {e}")
        return

    extractor = ETFSwapDataExtractor()
    
    # Only process the second ticker (LBJ) for debugging
    i = 2
    ticker = tickers[1]
    cik = ciks[1]
    series_id = series_ids[1]
    logger.info(f"Processing ONLY ticker {i}/{len(tickers)}: {ticker} (CIK: {cik}, Series: {series_id})")
    try:
        logger.info(f"Clearing existing data for {ticker}")
        extractor.clear_ticker_data(ticker)
        logger.info(f"Starting processing for {ticker}")
        try:
            extractor.process_ticker(ticker, cik, series_id=series_id)
            logger.info(f"✓ Successfully processed {ticker}")
        except Exception as e:
            logger.error(f"✗ Error processing {ticker}: {e}")
            logger.error(traceback.format_exc())
    except Exception as e:
        logger.error(f"✗ Fatal error for {ticker}: {e}")
        logger.error(traceback.format_exc())
    logger.info("=" * 80)
    logger.info("Worker finished!")

if __name__ == "__main__":
    main() 