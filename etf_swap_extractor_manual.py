import requests
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import time
import re
import os
from typing import Dict, List, Optional, Tuple
import json
import pandas as pd
from dateutil.parser import parse
import logging
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

class ETFSwapDataExtractor:
    def __init__(self, db_path: str = "etf_swap_data.db"):
        """Initialize the ETF Swap Data Extractor"""
        self.db_path = db_path
        self.base_url = "https://www.sec.gov"
        self.headers = {
            'User-Agent': 'VegaShares Anthony Crinieri tonycrinieri@gmail.com'  # REQUIRED by SEC
        }
        self.setup_database()
    
    def setup_database(self):
        """Create the database and tables for storing swap data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Drop existing tables to ensure clean schema
        cursor.execute('DROP TABLE IF EXISTS swap_data')
        cursor.execute('DROP TABLE IF EXISTS ticker_mappings')
        
        # Create tables with updated schema
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS swap_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                filing_date TEXT NOT NULL,
                period_of_report TEXT NOT NULL,
                index_name TEXT,
                index_identifier TEXT,
                counterparty_name TEXT,
                fixed_or_floating TEXT,
                floating_rt_index TEXT,
                floating_rt_spread REAL,
                notional_amt REAL,
                filing_url TEXT,
                extracted_date TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, filing_date, counterparty_name, notional_amt)
            )
        ''')
        
        # Create table for storing ticker to CIK mappings if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticker_mappings (
                ticker TEXT PRIMARY KEY,
                cik TEXT NOT NULL,
                company_name TEXT,
                series_id TEXT,
                start_date TEXT,
                last_updated TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()

    def get_ticker_mapping(self, ticker: str) -> Optional[Dict]:
        """Get the CIK and Series ID for a ticker from the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT ticker, cik, company_name, series_id, start_date
            FROM ticker_mappings
            WHERE ticker = ?
        ''', (ticker,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                'ticker': result[0],
                'cik': result[1],
                'company_name': result[2],
                'series_id': result[3],
                'start_date': result[4]
            }
        return None

    def add_ticker_mapping(self, ticker: str, cik: str, series_id: str, company_name: str = None, start_date: str = None):
        """Add a new ticker mapping to the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO ticker_mappings 
            (ticker, cik, company_name, series_id, start_date)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            ticker,
            cik,
            company_name,
            series_id,
            start_date or "2022-01-01"
        ))
        
        conn.commit()
        conn.close()

    def process_ticker_xml(self, ticker: str, xml_url: str, filing_date: str = None, series_id: str = None):
        logger.info(f"[process_ticker_xml] Start: ticker={ticker}, xml_url={xml_url}, filing_date={filing_date}, series_id={series_id}")
        if not filing_date:
            date_match = re.search(r'(\d{4})(\d{2})(\d{2})', xml_url)
            if date_match:
                filing_date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
            else:
                filing_date = datetime.now().strftime("%Y-%m-%d")
        try:
            logger.info(f"[process_ticker_xml] Fetching XML from {xml_url}")
            response = requests.get(xml_url, headers=self.headers, timeout=30)
            logger.info(f"[process_ticker_xml] Fetched XML, status_code={response.status_code}")
            if response.status_code == 200:
                try:
                    logger.info(f"[process_ticker_xml] Parsing XML content")
                    swap_data = self._parse_nport_xml_specific(response.content, ticker, xml_url, series_id)
                    logger.info(f"[process_ticker_xml] Parsed XML, found {len(swap_data) if swap_data else 0} swap records")
                    if swap_data:
                        root = ET.fromstring(response.content)
                        period_of_report = None
                        try:
                            period_elem = root.find('.//nport:repPdEndDt', namespaces={'nport': 'http://www.sec.gov/edgar/nport'})
                            if period_elem is not None and period_elem.text:
                                period_of_report = period_elem.text.strip()
                        except Exception as e:
                            logger.error(f"Error extracting period_of_report: {e}")
                        batch_size = 100
                        for i in range(0, len(swap_data), batch_size):
                            batch = swap_data[i:i + batch_size]
                            logger.info(f"[process_ticker_xml] Saving batch {i//batch_size+1} of swap data")
                            self.save_swap_data_specific(batch, filing_date, period_of_report)
                    return swap_data
                except ET.ParseError as e:
                    logger.error(f"XML parsing error for {ticker}: {e}")
                    return []
                except Exception as e:
                    logger.error(f"Error processing XML for {ticker}: {e}")
                    return []
            else:
                logger.error(f"Failed to fetch XML for {ticker}. Status code: {response.status_code}")
                return []
        except requests.Timeout:
            logger.error(f"Timeout while fetching XML for {ticker}")
            return []
        except requests.RequestException as e:
            logger.error(f"Request error for {ticker}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error fetching XML for {ticker}: {e}")
            return []
    
    def _parse_nport_xml_specific(self, xml_content: bytes, ticker: str, filing_url: str, series_id: str = None) -> List[Dict]:
        """Parse N-PORT XML content to extract specific swap data outputs for any ticker"""
        swap_data = []
        
        try:
            if isinstance(xml_content, bytes):
                xml_str = xml_content.decode('utf-8')
            else:
                xml_str = xml_content
            
            # Parse XML with explicit namespace handling
            root = ET.fromstring(xml_content)
            
            # Define namespaces
            namespaces = {
                '': 'http://www.sec.gov/edgar/nport',
                'nport': 'http://www.sec.gov/edgar/nport',
                'com': 'http://www.sec.gov/edgar/common',
                'ncom': 'http://www.sec.gov/edgar/nportcommon',
                'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
            }
            
            # First check if this filing is for the correct series
            if series_id:
                series_elem = root.find('.//nport:seriesId', namespaces)
                if series_elem is None or series_elem.text != series_id:
                    return []
            
            # Extract period_of_report from <repPdEndDt>
            period_of_report = None
            try:
                period_elem = root.find('.//nport:repPdEndDt', namespaces)
                if period_elem is not None and period_elem.text:
                    period_of_report = period_elem.text.strip()
            except Exception as e:
                logger.error(f"Error extracting period_of_report: {e}")
            
            # Extract index information from varInfo section
            var_section = root.find('.//nport:varInfo', namespaces)
            index_name = None
            index_identifier = None
            
            if var_section is not None:
                # Find designated index name - try multiple paths
                index_name_elem = var_section.find('.//nport:nameDesignatedIndex', namespaces)
                if index_name_elem is None:
                    index_name_elem = var_section.find('.//nameDesignatedIndex')
                if index_name_elem is not None:
                    index_name = index_name_elem.text.strip()
                
                # Find index identifier - try multiple paths
                index_id_elem = var_section.find('.//nport:indexIdentifier', namespaces)
                if index_id_elem is None:
                    index_id_elem = var_section.find('.//indexIdentifier')
                if index_id_elem is not None:
                    index_identifier = index_id_elem.text.strip()
            
            # Look for investment securities and derivatives
            investment_paths = [
                './/nport:invstOrSec',
                './/nport:derivativeInstrument',
                './/nport:derivative',
                './/nport:investment',
                './/nport:security',
                './/nport:holding'
            ]
            
            # Process elements in chunks to avoid memory issues
            for path in investment_paths:
                try:
                    elements = root.findall(path, namespaces)
                    if elements:
                        for element in elements:
                            try:
                                swap_info = self._extract_specific_swap_info(element, namespaces, ticker, filing_url)
                                if swap_info:
                                    swap_info['period_of_report'] = period_of_report
                                    swap_info['index_name'] = index_name
                                    swap_info['index_identifier'] = index_identifier
                                    swap_data.append(swap_info)
                            except Exception as e:
                                logger.error(f"Error processing element: {e}")
                                continue
                except Exception as e:
                    logger.error(f"Error processing path {path}: {str(e)}")
                    continue
                        
        except ET.ParseError as e:
            logger.error(f"XML parsing error: {e}")
        except Exception as e:
            logger.error(f"Error parsing N-PORT XML: {e}")
        
        return swap_data
    
    def _extract_specific_swap_info(self, element: ET.Element, namespaces: Dict, ticker: str, filing_url: str) -> Optional[Dict]:
        """Extract the 7 specific swap information fields for any designated index"""
        
        # Get all text content from this element
        element_text = ET.tostring(element, encoding='unicode')
        
        # Check if this element contains swap/derivative information
        swap_indicators = ['swap', 'derivative', 'deriv', 'forward', 'future', 'option', 
                          'floatingRtIndex', 'fixedOrFloating', 'counterparty', 'notional']
        
        has_swap_indicators = any(indicator.lower() in element_text.lower() for indicator in swap_indicators)
        
        if not has_swap_indicators:
            return None
        
        swap_info = {
            'ticker': ticker,
            'filing_url': filing_url
        }
        
        try:
            # Field mapping for your specific requirements
            field_mappings = {
                'index_name': ['indexName', 'indexTitle', 'index', 'benchmarkName', 'name', 'title', 
                              'desc', 'description', 'issuerName', 'securityName'],
                'index_identifier': ['indexIdentifier', 'indexId', 'benchmarkId', 'identifier', 'id',
                                   'securityId', 'cusip', 'isin'],
                'counterparty_name': ['counterpartyName', 'ctrPtyName', 'counterparty', 'ctrPty',
                                    'cptyName', 'partyName'],
                'notional_amt': ['notionalAmt', 'notional', 'notionalAmount', 'amt', 'principalAmt',
                               'nominalAmt', 'faceAmt']
            }
            
            def find_field_value(field_variations, element, namespaces):
                for field in field_variations:
                    for ns_prefix in ['', 'nport', 'com', 'ncom']:
                        if ns_prefix:
                            path = f'.//{ns_prefix}:{field}'
                        else:
                            path = f'.//{field}'
                        try:
                            elem = element.find(path, namespaces)
                            if elem is not None and elem.text:
                                return elem.text.strip()
                        except Exception:
                            continue
                    try:
                        elem = element.find(f'.//{field}')
                        if elem is not None and elem.text:
                            return elem.text.strip()
                    except Exception:
                        continue
                return None
            
            # Extract each required field
            for output_field, field_variations in field_mappings.items():
                value = find_field_value(field_variations, element, namespaces)
                if value:
                    if output_field in ['notional_amt']:
                        try:
                            clean_value = re.sub(r'[,$%]', '', value)
                            swap_info[output_field] = float(clean_value)
                        except ValueError:
                            swap_info[output_field] = value
                    else:
                        swap_info[output_field] = value
                    
            # Extract fixedOrFloating, floatingRtIndex, floatingRtSpread
            floating_pmnt_desc = None
            try:
                floating_pmnt_desc = element.find('.//nport:derivativeInfo/nport:swapDeriv/nport:floatingPmntDesc', namespaces)
                if floating_pmnt_desc is None:
                    floating_pmnt_desc = element.find('.//derivativeInfo/swapDeriv/floatingPmntDesc')
            except Exception:
                pass
            
            if floating_pmnt_desc is not None:
                swap_info['fixed_or_floating'] = floating_pmnt_desc.attrib.get('fixedOrFloating')
                
                floating_rt_index = floating_pmnt_desc.attrib.get('floatingRtIndex', '')
                if floating_rt_index:
                    valid_indices = ['1 month Sofr + spread', 'OBFR01', 'FEDL01', 'OBFR']
                    if floating_rt_index in valid_indices:
                        swap_info['floating_rt_index'] = floating_rt_index
                    else:
                        swap_info['floating_rt_index'] = '1 month Sofr + spread'
                
                spread_val = floating_pmnt_desc.attrib.get('floatingRtSpread')
                if spread_val is not None:
                    try:
                        swap_info['floating_rt_spread'] = float(spread_val)
                    except ValueError:
                        swap_info['floating_rt_spread'] = spread_val
            
            # Only return if we have at least some meaningful swap data
            required_fields = ['counterparty_name', 'fixed_or_floating', 'floating_rt_index', 'notional_amt']
            has_swap_data = all(field in swap_info for field in required_fields)
            
            if has_swap_data:
                return swap_info
                
        except Exception as e:
            logger.error(f"Error extracting specific swap info: {e}")
        
        return None
    
    def save_swap_data_specific(self, swap_data: List[Dict], filing_date: str, period_of_report: str = None):
        """Save swap data to the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for swap in swap_data:
            try:
                # Use period_of_report from swap data if available, otherwise use the provided one or filing_date
                swap_period = swap.get('period_of_report') or period_of_report or filing_date
                
                cursor.execute('''
                    INSERT OR REPLACE INTO swap_data 
                    (ticker, filing_date, period_of_report, index_name, index_identifier,
                    counterparty_name, fixed_or_floating, floating_rt_index, floating_rt_spread,
                    notional_amt, filing_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    swap['ticker'],
                    filing_date,
                    swap_period,  # Use the determined period
                    swap.get('index_name'),
                    swap.get('index_identifier'),
                    swap.get('counterparty_name'),
                    swap.get('fixed_or_floating'),
                    swap.get('floating_rt_index'),
                    swap.get('floating_rt_spread'),
                    swap.get('notional_amt'),
                    swap.get('filing_url')
                ))
            except Exception as e:
                logger.error(f"Error saving swap data: {e}")
                continue
        
        conn.commit()
        conn.close()
    
    def get_ticker_data_specific(self, ticker: str) -> List[Dict]:
        """Get all swap data for a specific ticker"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT filing_date, period_of_report, index_name, index_identifier,
                   counterparty_name, fixed_or_floating, floating_rt_index, 
                   floating_rt_spread, notional_amt, filing_url
            FROM swap_data 
            WHERE ticker = ? 
            ORDER BY filing_date DESC
        ''', (ticker,))
        
        results = cursor.fetchall()
        conn.close()
        
        return [{
            'filing_date': row[0],
            'period_of_report': row[1],
            'index_name': row[2],
            'index_identifier': row[3],
            'counterparty_name': row[4],
            'fixed_or_floating': row[5],
            'floating_rt_index': row[6],
            'floating_rt_spread': row[7],
            'notional_amt': row[8],
            'filing_url': row[9]
        } for row in results]

    def get_historical_filings(self, cik: str, start_date: str = None, end_date: str = None) -> List[Dict]:
        logger.info(f"[get_historical_filings] Start: cik={cik}, start_date={start_date}, end_date={end_date}")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"[get_historical_filings] Using start_date={start_date}, end_date={end_date}")
        try:
            url = f"https://data.sec.gov/submissions/CIK{cik.zfill(10)}.json"
            logger.info(f"[get_historical_filings] Requesting SEC URL: {url}")
            response = requests.get(url, headers=self.headers, timeout=30)
            logger.info(f"[get_historical_filings] SEC response status: {response.status_code}")
            if response.status_code != 200:
                logger.error(f"Error fetching filings: {response.status_code}")
                return []
            data = response.json()
            nport_filings = []
            recent = data.get('filings', {}).get('recent', {})
            if recent:
                forms = recent.get('form', [])
                accession_numbers = recent.get('accessionNumber', [])
                filing_dates = recent.get('filingDate', [])
                for i, form in enumerate(forms):
                    if form == 'NPORT-P':
                        filing_date = filing_dates[i]
                        filing_dt = parse(filing_date)
                        start_date_dt = parse(start_date)
                        end_date_dt = parse(end_date)
                        if start_date_dt <= filing_dt <= end_date_dt:
                            acc_no = accession_numbers[i].replace('-', '')
                            if acc_no:
                                xml_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_no}/primary_doc.xml"
                                nport_filings.append({
                                    'filing_date': filing_date,
                                    'filing_url': xml_url
                                })
            files = data.get('filings', {}).get('files', [])
            for file_info in files:
                if file_info.get('form') == 'NPORT-P':
                    filing_date = file_info.get('filingDate')
                    if filing_date:
                        filing_dt = parse(filing_date)
                        start_date_dt = parse(start_date)
                        end_date_dt = parse(end_date)
                        if start_date_dt <= filing_dt <= end_date_dt:
                            acc_no = file_info.get('accessionNumber', '').replace('-', '')
                            if acc_no:
                                xml_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_no}/primary_doc.xml"
                                nport_filings.append({
                                    'filing_date': filing_date,
                                    'filing_url': xml_url
                                })
            logger.info(f"[get_historical_filings] Found {len(nport_filings)} NPORT-P filings")
            nport_filings.sort(key=lambda x: x['filing_date'], reverse=True)
            logger.info(f"[get_historical_filings] End: returning {len(nport_filings)} filings")
            return nport_filings
        except requests.Timeout:
            logger.error(f"Timeout while fetching filings for CIK {cik}")
            return []
        except requests.RequestException as e:
            logger.error(f"Request error for CIK {cik}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error fetching historical filings: {e}")
            return []
    
    def process_ticker(self, ticker: str, cik: str, start_date: str = None, end_date: str = None, series_id: str = None):
        logger.info(f"[process_ticker] Start: ticker={ticker}, cik={cik}, start_date={start_date}, end_date={end_date}, series_id={series_id}")
        self.clear_ticker_data(ticker)
        if not start_date:
            start_date = "2022-01-01"
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"[process_ticker] Fetching historical filings for {ticker}")
        filings = self.get_historical_filings(cik, start_date, end_date)
        logger.info(f"[process_ticker] Found {len(filings)} filings for {ticker}")
        batch_size = 5
        for i in range(0, len(filings), batch_size):
            batch = filings[i:i + batch_size]
            logger.info(f"[process_ticker] Processing batch {i//batch_size + 1} of {(len(filings) + batch_size - 1)//batch_size} for {ticker}")
            for filing in batch:
                try:
                    logger.info(f"[process_ticker] Processing filing {filing['filing_date']} for {ticker}")
                    self.process_ticker_xml(ticker, filing['filing_url'], filing['filing_date'], series_id)
                    time.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error processing filing for {ticker}: {e}")
                    continue
            time.sleep(0.5)
        logger.info(f"[process_ticker] Finished processing {ticker}")

    def export_to_csv(self, output_file: str = "etf_swap_data.csv", ticker: str = None):
        """Export swap data to CSV file"""
        conn = sqlite3.connect(self.db_path)
        
        query = '''
            SELECT ticker, filing_date, period_of_report, index_name, index_identifier,
                   counterparty_name, fixed_or_floating, floating_rt_index,
                   floating_rt_spread, notional_amt, filing_url
            FROM swap_data
        '''
        
        if ticker:
            query += ' WHERE ticker = ?'
            df = pd.read_sql_query(query, conn, params=(ticker,))
        else:
            df = pd.read_sql_query(query, conn)
        
        conn.close()
        
        # Save to CSV
        df.to_csv(output_file, index=False)
        logger.info(f"Data exported to {output_file}")

    def import_tickers_from_csv(self, csv_path: str):
        """Import ticker mappings from a CSV file
        
        Supports either:
        - ticker,cik,series_id,company_name,start_date
        - CIK,Series,Name,Ticker
        """
        try:
            # Read the CSV file
            df = pd.read_csv(csv_path)
            
            # Detect and map columns if using alternate header
            if set(['CIK', 'Series', 'Name', 'Ticker']).issubset(df.columns):
                df = df.rename(columns={
                    'CIK': 'cik',
                    'Series': 'series_id',
                    'Name': 'company_name',
                    'Ticker': 'ticker'
                })
            
            # Validate required columns
            required_columns = ['ticker', 'cik', 'series_id']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
            
            # Add optional columns if not present
            if 'company_name' not in df.columns:
                df['company_name'] = None
            if 'start_date' not in df.columns:
                df['start_date'] = '2022-01-01'
            
            # Connect to database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Insert each row
            for _, row in df.iterrows():
                cursor.execute('''
                    INSERT OR REPLACE INTO ticker_mappings 
                    (ticker, cik, company_name, series_id, start_date)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    str(row['ticker']).upper(),
                    str(row['cik']).zfill(10),  # Ensure CIK is 10 digits
                    row['company_name'],
                    row['series_id'],
                    row['start_date']
                ))
                print(f"Added {str(row['ticker']).upper()} to database")
            
            conn.commit()
            conn.close()
            print(f"\nSuccessfully imported {len(df)} tickers from {csv_path}")
            
        except Exception as e:
            print(f"Error importing tickers from CSV: {str(e)}")

    def clear_ticker_data(self, ticker: str):
        """Clear all swap data for a specific ticker from the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM swap_data WHERE ticker = ?', (ticker,))
        conn.commit()
        conn.close()
        print(f"Cleared existing data for {ticker}")

def main():
    """Main function to run the ETF Swap Data Extractor"""
    import sys
    import pandas as pd
    
    if len(sys.argv) < 2:
        print("Please provide a ticker symbol as a command-line argument")
        print("Usage: python etf_swap_extractor_manual.py TICKER")
        sys.exit(1)
    
    ticker = sys.argv[1].upper()
    print(f"Processing ticker: {ticker}")
    
    # Load ticker mappings from CSV
    try:
        csv_path = "etf_tickers.csv"
        ticker_mappings = pd.read_csv(csv_path)
        # Convert CIK numbers to 10-digit strings with leading zeros
        ticker_mappings['CIK'] = ticker_mappings['CIK'].astype(str).str.zfill(10)
        
        # Find the ticker in the mappings
        ticker_data = ticker_mappings[ticker_mappings['Ticker'] == ticker]
        if ticker_data.empty:
            print(f"Error: Ticker {ticker} not found in the database")
            sys.exit(1)
        
        cik = ticker_data['CIK'].iloc[0]
        series_id = ticker_data['Series'].iloc[0]
        start_date = ticker_data['Start Date'].iloc[0] if 'Start Date' in ticker_data.columns else "2019-01-01"
        print(f"Found CIK: {cik}")
        print(f"Found Series ID: {series_id}")
        print(f"Using start date: {start_date}")
        
        # Initialize the extractor
        extractor = ETFSwapDataExtractor()
        
        # Process the ticker with the specific start date
        extractor.process_ticker(ticker, cik, start_date=start_date, series_id=series_id)
        
        # Export to CSV
        csv_path = f"{ticker.lower()}_swap_data.csv"
        extractor.export_to_csv(csv_path, ticker)
        print(f"Data exported to {csv_path}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
