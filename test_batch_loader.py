import pandas as pd
import logging
import time
from datetime import datetime
from etf_swap_extractor_manual import ETFSwapDataExtractor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_batch_load.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def test_batch_loader():
    """Test the batch loader with first 3 tickers"""
    # Load tickers from CSV
    csv_path = 'etf_tickers.csv'
    logger.info(f"Loading tickers from {csv_path}")
    
    try:
        df = pd.read_csv(csv_path)
        # Take only first 3 tickers for testing
        df_test = df.head(3)
        tickers = df_test['Ticker'].astype(str).str.upper().tolist()
        ciks = df_test['CIK'].astype(str).str.zfill(10).tolist()
        series_ids = df_test['Series'].astype(str).tolist()
        logger.info(f"Testing with {len(tickers)} tickers: {tickers}")
    except Exception as e:
        logger.error(f"Error loading CSV file: {e}")
        return

    extractor = ETFSwapDataExtractor()
    
    # Track progress
    total_tickers = len(tickers)
    successful_tickers = 0
    failed_tickers = 0
    failed_list = []
    
    logger.info(f"Starting test batch processing of {total_tickers} tickers")
    start_time = datetime.now()
    
    for i, (ticker, cik, series_id) in enumerate(zip(tickers, ciks, series_ids), 1):
        logger.info(f"Processing ticker {i}/{total_tickers}: {ticker} (CIK: {cik}, Series: {series_id})")
        
        try:
            # Add a small delay to be respectful to SEC servers
            if i > 1:
                time.sleep(2)  # Longer delay for testing
            
            extractor.process_ticker(ticker, cik, series_id=series_id)
            successful_tickers += 1
            logger.info(f"✓ Successfully processed {ticker}")
            
        except Exception as e:
            failed_tickers += 1
            failed_list.append(ticker)
            logger.error(f"✗ Error processing {ticker}: {e}")
            logger.error(f"Continuing with next ticker...")
            continue
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    # Summary
    logger.info("=" * 60)
    logger.info("TEST BATCH PROCESSING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total tickers: {total_tickers}")
    logger.info(f"Successful: {successful_tickers}")
    logger.info(f"Failed: {failed_tickers}")
    logger.info(f"Success rate: {(successful_tickers/total_tickers)*100:.1f}%")
    logger.info(f"Duration: {duration}")
    
    if failed_list:
        logger.info(f"Failed tickers: {', '.join(failed_list)}")
    
    logger.info("=" * 60)
    
    if successful_tickers == total_tickers:
        logger.info("✓ Test passed! All tickers processed successfully.")
        logger.info("You can now run the full batch loader.")
    else:
        logger.info("✗ Test failed! Some tickers failed to process.")
        logger.info("Check the logs for details before running the full batch.")

if __name__ == "__main__":
    test_batch_loader() 