import sqlite3
import pandas as pd
from etf_db import query_swap_data, get_db_connection

def check_sqlite_status():
    """Check the status of the SQLite database"""
    print("=" * 60)
    print("SQLITE DATABASE STATUS")
    print("=" * 60)
    
    try:
        conn = sqlite3.connect("etf_swap_data.db")
        cursor = conn.cursor()
        
        # Check if tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"Tables in database: {[table[0] for table in tables]}")
        
        if ('swap_data',) in tables:
            # Count total records
            cursor.execute("SELECT COUNT(*) FROM swap_data")
            total_records = cursor.fetchone()[0]
            print(f"Total swap records: {total_records}")
            
            # Count unique tickers
            cursor.execute("SELECT COUNT(DISTINCT ticker) FROM swap_data")
            unique_tickers = cursor.fetchone()[0]
            print(f"Unique tickers: {unique_tickers}")
            
            # List all tickers
            cursor.execute("SELECT DISTINCT ticker FROM swap_data ORDER BY ticker")
            tickers = [row[0] for row in cursor.fetchall()]
            print(f"Tickers in database: {', '.join(tickers[:10])}{'...' if len(tickers) > 10 else ''}")
            
            # Show recent filings
            cursor.execute("""
                SELECT ticker, filing_date, COUNT(*) as records 
                FROM swap_data 
                GROUP BY ticker, filing_date 
                ORDER BY filing_date DESC 
                LIMIT 10
            """)
            recent = cursor.fetchall()
            print("\nRecent filings:")
            for ticker, date, count in recent:
                print(f"  {ticker} - {date}: {count} records")
        
        conn.close()
        
    except Exception as e:
        print(f"Error checking SQLite database: {e}")

def check_postgres_status():
    """Check the status of the Postgres database"""
    print("\n" + "=" * 60)
    print("POSTGRES DATABASE STATUS")
    print("=" * 60)
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'swap_data'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if table_exists:
            # Count total records
            cursor.execute("SELECT COUNT(*) FROM swap_data")
            total_records = cursor.fetchone()[0]
            print(f"Total swap records: {total_records}")
            
            # Count unique tickers
            cursor.execute("SELECT COUNT(DISTINCT ticker) FROM swap_data")
            unique_tickers = cursor.fetchone()[0]
            print(f"Unique tickers: {unique_tickers}")
            
            # List all tickers
            cursor.execute("SELECT DISTINCT ticker FROM swap_data ORDER BY ticker")
            tickers = [row[0] for row in cursor.fetchall()]
            print(f"Tickers in database: {', '.join(tickers[:10])}{'...' if len(tickers) > 10 else ''}")
            
            # Show recent filings
            cursor.execute("""
                SELECT ticker, filing_date, COUNT(*) as records 
                FROM swap_data 
                GROUP BY ticker, filing_date 
                ORDER BY filing_date DESC 
                LIMIT 10
            """)
            recent = cursor.fetchall()
            print("\nRecent filings:")
            for ticker, date, count in recent:
                print(f"  {ticker} - {date}: {count} records")
        else:
            print("swap_data table does not exist in Postgres")
        
        conn.close()
        
    except Exception as e:
        print(f"Error checking Postgres database: {e}")

def compare_with_csv():
    """Compare what's in the database vs what should be in the CSV"""
    print("\n" + "=" * 60)
    print("COMPARISON WITH CSV")
    print("=" * 60)
    
    try:
        # Load CSV
        df = pd.read_csv('etf_tickers.csv')
        csv_tickers = set(df['Ticker'].astype(str).str.upper().tolist())
        print(f"Tickers in CSV: {len(csv_tickers)}")
        
        # Get tickers from SQLite
        conn = sqlite3.connect("etf_swap_data.db")
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT ticker FROM swap_data")
        sqlite_tickers = set([row[0] for row in cursor.fetchall()])
        conn.close()
        
        # Get tickers from Postgres
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT ticker FROM swap_data")
            postgres_tickers = set([row[0] for row in cursor.fetchall()])
            conn.close()
        except:
            postgres_tickers = set()
        
        print(f"Tickers in SQLite: {len(sqlite_tickers)}")
        print(f"Tickers in Postgres: {len(postgres_tickers)}")
        
        # Show missing tickers
        missing_from_sqlite = csv_tickers - sqlite_tickers
        missing_from_postgres = csv_tickers - postgres_tickers
        
        print(f"\nMissing from SQLite: {len(missing_from_sqlite)}")
        if missing_from_sqlite:
            print(f"  {', '.join(sorted(list(missing_from_sqlite))[:10])}{'...' if len(missing_from_sqlite) > 10 else ''}")
        
        print(f"Missing from Postgres: {len(missing_from_postgres)}")
        if missing_from_postgres:
            print(f"  {', '.join(sorted(list(missing_from_postgres))[:10])}{'...' if len(missing_from_postgres) > 10 else ''}")
        
    except Exception as e:
        print(f"Error comparing with CSV: {e}")

if __name__ == "__main__":
    check_sqlite_status()
    check_postgres_status()
    compare_with_csv() 