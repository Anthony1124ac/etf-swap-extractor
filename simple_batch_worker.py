import pandas as pd
import logging
import time
import sys
from datetime import datetime
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
    
    # Track progress
    total_tickers = len(tickers)
    successful_tickers = 0
    failed_tickers = 0
    failed_list = []
    
    logger.info(f"Starting batch processing of {total_tickers} tickers")
    start_time = datetime.now()
    
    # Process tickers sequentially
    for i, (ticker, cik, series_id) in enumerate(zip(tickers, ciks, series_ids), 1):
        logger.info(f"Processing ticker {i}/{total_tickers}: {ticker} (CIK: {cik}, Series: {series_id})")
        
        try:
            # Add delay to be respectful to SEC servers
            if i > 1:
                time.sleep(2)  # 2 second delay between requests
            
            # Clear existing data for this ticker first
            logger.info(f"Clearing existing data for {ticker}")
            extractor.clear_ticker_data(ticker)
            
            # Process the ticker (no timeout - let it run naturally)
            logger.info(f"Starting processing for {ticker}")
            extractor.process_ticker(ticker, cik, series_id=series_id)
            
            successful_tickers += 1
            logger.info(f"✓ Successfully processed {ticker} ({successful_tickers}/{total_tickers})")
            
            # Log progress every 5 tickers
            if i % 5 == 0:
                elapsed = datetime.now() - start_time
                rate = i / elapsed.total_seconds() * 60  # tickers per minute
                eta_minutes = (total_tickers - i) / rate if rate > 0 else 0
                logger.info(f"Progress: {i}/{total_tickers} ({i/total_tickers*100:.1f}%) - Rate: {rate:.1f} tickers/min - ETA: {eta_minutes:.1f} min")
                logger.info(f"Success: {successful_tickers}, Failed: {failed_tickers}")
            
        except Exception as e:
            failed_tickers += 1
            failed_list.append(ticker)
            logger.error(f"✗ Error processing {ticker}: {e}")
            logger.error(f"Continuing with next ticker...")
            continue
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Final summary
    logger.info("=" * 80)
    logger.info("BATCH PROCESSING COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total tickers: {total_tickers}")
    logger.info(f"Successful: {successful_tickers}")
    logger.info(f"Failed: {failed_tickers}")
    logger.info(f"Success rate: {(successful_tickers/total_tickers)*100:.1f}%")
    logger.info(f"Duration: {duration}")
    
    if failed_list:
        logger.info(f"Failed tickers: {', '.join(failed_list)}")
    
    logger.info("=" * 80)
    logger.info("Worker finished successfully!")

if __name__ == "__main__":
    main() 