import sys
import logging
from etf_swap_extractor_manual import ETFSwapDataExtractor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

def main():
    if len(sys.argv) < 4:
        print("Usage: python single_ticker_loader.py TICKER CIK SERIES_ID")
        sys.exit(1)
    ticker, cik, series_id = sys.argv[1], sys.argv[2], sys.argv[3]
    logger.info(f"Processing ticker: {ticker}, CIK: {cik}, Series: {series_id}")
    extractor = ETFSwapDataExtractor()
    try:
        extractor.process_ticker(ticker, cik, series_id=series_id)
        logger.info(f"SUCCESS: {ticker}")
        print(f"SUCCESS: {ticker}")
    except Exception as e:
        logger.error(f"FAIL: {ticker} - {e}")
        print(f"FAIL: {ticker} - {e}")

if __name__ == "__main__":
    main() 