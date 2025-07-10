import os
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ['DATABASE_URL']

CREATE_TABLE_SQL = '''
CREATE TABLE IF NOT EXISTS swap_data (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    filing_date DATE NOT NULL,
    period_of_report DATE NOT NULL,
    designated_reference_portfolio TEXT,
    index_identifier TEXT,
    counterparty_name TEXT,
    fixed_or_floating TEXT,
    floating_rt_index TEXT,
    floating_rt_spread FLOAT,
    notional_amt FLOAT,
    filing_url TEXT,
    extracted_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, filing_date, counterparty_name, notional_amt)
);
'''

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def get_db_connection():
    """Alias for compatibility with other scripts."""
    return get_conn()

def create_table():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            conn.commit()

def insert_swap_data(swap):
    sql = '''
    INSERT INTO swap_data (
        ticker, filing_date, period_of_report, designated_reference_portfolio, index_identifier,
        counterparty_name, fixed_or_floating, floating_rt_index, floating_rt_spread, notional_amt, filing_url
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ticker, filing_date, counterparty_name, notional_amt) DO NOTHING;
    '''
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (
                swap['ticker'],
                swap['filing_date'],
                swap['period_of_report'],
                swap.get('Designated Reference Portfolio'),
                swap.get('index_identifier'),
                swap.get('counterparty_name'),
                swap.get('fixed_or_floating'),
                swap.get('floating_rt_index'),
                swap.get('floating_rt_spread'),
                swap.get('notional_amt'),
                swap.get('filing_url')
            ))
            conn.commit()

def query_swap_data(ticker=None, limit=100):
    sql = 'SELECT * FROM swap_data'
    params = []
    if ticker:
        sql += ' WHERE ticker = %s'
        params.append(ticker)
    sql += ' ORDER BY filing_date DESC LIMIT %s'
    params.append(limit)
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()

if __name__ == "__main__":
    create_table()
    print("Table created!")
    # Example: print(query_swap_data('TSLL', 5)) 