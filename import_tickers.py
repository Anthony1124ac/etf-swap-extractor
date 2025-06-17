from etf_swap_extractor_manual import ETFSwapDataExtractor
import pandas as pd

def main():
    # Initialize the extractor
    extractor = ETFSwapDataExtractor()
    
    # Import tickers from CSV
    csv_path = "ETF Tickers CIK_SERIES_6_16_25 - CIK_SERIES.csv"
    print(f"Reading CSV file: {csv_path}")
    
    # Read and display first few rows of CSV
    df = pd.read_csv(csv_path)
    print("\nFirst few rows of CSV:")
    print(df.head())
    print(f"\nTotal rows in CSV: {len(df)}")
    
    # Import tickers
    print(f"\nImporting tickers from {csv_path}...")
    extractor.import_tickers_from_csv(csv_path)
    
    # Verify database contents after import
    import sqlite3
    conn = sqlite3.connect("etf_swap_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ticker_mappings")
    count = cursor.fetchone()[0]
    print(f"\nNumber of tickers in database after import: {count}")
    
    # Show some sample tickers from database
    cursor.execute("SELECT ticker, cik, series_id FROM ticker_mappings LIMIT 5")
    print("\nSample tickers in database:")
    for row in cursor.fetchall():
        print(row)
    
    conn.close()

if __name__ == "__main__":
    main() 