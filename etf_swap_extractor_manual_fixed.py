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
        """Process XML file for any ticker to extract swap data"""
        if not filing_date:
            # Try to extract date from URL
            date_match = re.search(r'(\d{4})(\d{2})(\d{2})', xml_url)
            if date_match:
                filing_date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
            else:
                filing_date = datetime.now().strftime("%Y-%m-%d")
        
        try:
            # Add timeout to the request
            response = requests.get(xml_url, headers=self.headers, timeout=30)
            
            if response.status_code == 200:
                try:
                    # Parse the XML for specific outputs with timeout handling
                    swap_data = self._parse_nport_xml_specific(response.content, ticker, xml_url, series_id)
                    
                    # Save to database
                    if swap_data:
                        # Extract period_of_report from XML
                        root = ET.fromstring(response.content)
                        period_of_report = None
                        try:
                            period_elem = root.find('.//nport:repPdEndDt', namespaces={'nport': 'http://www.sec.gov/edgar/nport'})
                            if period_elem is not None and period_elem.text:
                                period_of_report = period_elem.text.strip()
                        except Exception as e:
                            logger.error(f"Error extracting period_of_report: {e}")
                        
                        # Save data in smaller batches
                        batch_size = 100
                        for i in range(0, len(swap_data), batch_size):
                            batch = swap_data[i:i + batch_size]
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
                    swap_info['floating_rt_index'] = floating_rt_index
                
                floating_rt_spread = floating_pmnt_desc.attrib.get('floatingRtSpread', '')
                if floating_rt_spread:
                    try:
                        swap_info['floating_rt_spread'] = float(floating_rt_spread)
                    except ValueError:
                        swap_info['floating_rt_spread'] = floating_rt_spread
            
            return swap_info
            
        except Exception as e:
            logger.error(f"Error extracting swap info: {e}")
            return None
    
    def save_swap_data_specific(self, swap_data: List[Dict], filing_date: str, period_of_report: str = None):
        """Save the specific swap data to the database"""
        if not swap_data:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            for data in swap_data:
                cursor.execute('''
                    INSERT OR REPLACE INTO swap_data (
                        ticker, filing_date, period_of_report, index_name, index_identifier,
                        counterparty_name, fixed_or_floating, floating_rt_index,
                        floating_rt_spread, notional_amt, filing_url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    data.get('ticker'),
                    filing_date,
                    period_of_report or data.get('period_of_report'),
                    data.get('index_name'),
                    data.get('index_identifier'),
                    data.get('counterparty_name'),
                    data.get('fixed_or_floating'),
                    data.get('floating_rt_index'),
                    data.get('floating_rt_spread'),
                    data.get('notional_amt'),
                    data.get('filing_url')
                ))
            
            conn.commit()
        except Exception as e:
            logger.error(f"Error saving swap data: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def get_ticker_data_specific(self, ticker: str) -> List[Dict]:
        """Get all swap data for a specific ticker"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT 
                    ticker, filing_date, period_of_report, index_name, index_identifier,
                    counterparty_name, fixed_or_floating, floating_rt_index,
                    floating_rt_spread, notional_amt, filing_url
                FROM swap_data
                WHERE ticker = ?
                ORDER BY filing_date DESC
            ''', (ticker,))
            
            columns = [description[0] for description in cursor.description]
            results = []
            
            for row in cursor.fetchall():
                result = dict(zip(columns, row))
                results.append(result)
            
            return results
        except Exception as e:
            logger.error(f"Error getting ticker data: {e}")
            return []
        finally:
            conn.close()
    
    def get_historical_filings(self, cik: str, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get historical N-PORT filings for a CIK"""
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        try:
            # Get the submissions index
            url = f"{self.base_url}/data/edgar/submissions/CIK{cik}.json"
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"Failed to get submissions index. Status code: {response.status_code}")
                return []
            
            data = response.json()
            
            # Get recent filings
            recent_filings = data.get('filings', {}).get('recent', {})
            if not recent_filings:
                return []
            
            # Get form types and filing dates
            form_types = recent_filings.get('form', [])
            filing_dates = recent_filings.get('filingDate', [])
            primary_docs = recent_filings.get('primaryDocument', [])
            primary_doc_descriptions = recent_filings.get('primaryDocDescription', [])
            
            # Filter for N-PORT-P filings within date range
            nport_filings = []
            for i in range(len(form_types)):
                if form_types[i] == 'NPORT-P':
                    filing_date = filing_dates[i]
                    if start_date <= filing_date <= end_date:
                        nport_filings.append({
                            'filing_date': filing_date,
                            'primary_doc': primary_docs[i],
                            'description': primary_doc_descriptions[i]
                        })
            
            return nport_filings
            
        except requests.Timeout:
            logger.error("Timeout while fetching historical filings")
            return []
        except requests.RequestException as e:
            logger.error(f"Request error: {e}")
            return []
        except Exception as e:
            logger.error(f"Error getting historical filings: {e}")
            return []
    
    def process_ticker(self, ticker: str, cik: str, start_date: str = None, end_date: str = None, series_id: str = None):
        """Process a ticker to extract swap data"""
        try:
            # Get historical filings
            filings = self.get_historical_filings(cik, start_date, end_date)
            
            if not filings:
                logger.error(f"No N-PORT-P filings found for {ticker}")
                return
            
            # Process each filing
            for filing in filings:
                try:
                    # Construct the XML URL
                    filing_date = filing['filing_date']
                    primary_doc = filing['primary_doc']
                    xml_url = f"{self.base_url}/Archives/edgar/data/{cik}/{primary_doc}"
                    
                    # Process the XML file
                    self.process_ticker_xml(ticker, xml_url, filing_date, series_id)
                    
                    # Respect SEC rate limits
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Error processing filing for {ticker}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error processing ticker {ticker}: {e}")
    
    def export_to_csv(self, output_file: str = "etf_swap_data.csv", ticker: str = None):
        """Export the swap data to a CSV file"""
        conn = sqlite3.connect(self.db_path)
        
        try:
            query = '''
                SELECT 
                    ticker, filing_date, period_of_report, index_name, index_identifier,
                    counterparty_name, fixed_or_floating, floating_rt_index,
                    floating_rt_spread, notional_amt, filing_url
                FROM swap_data
            '''
            
            if ticker:
                query += ' WHERE ticker = ?'
                df = pd.read_sql_query(query, conn, params=(ticker,))
            else:
                df = pd.read_sql_query(query, conn)
            
            # Format the data
            df['filing_date'] = pd.to_datetime(df['filing_date']).dt.strftime('%Y-%m-%d')
            df['period_of_report'] = pd.to_datetime(df['period_of_report']).dt.strftime('%Y-%m-%d')
            
            # Save to CSV
            df.to_csv(output_file, index=False)
            
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")
        finally:
            conn.close()
    
    def import_tickers_from_csv(self, csv_path: str):
        """Import ticker mappings from a CSV file"""
        try:
            df = pd.read_csv(csv_path)
            
            # Ensure required columns exist
            required_columns = ['Ticker', 'CIK', 'Series']
            if not all(col in df.columns for col in required_columns):
                logger.error(f"CSV file must contain columns: {', '.join(required_columns)}")
                return
            
            # Process each row
            for _, row in df.iterrows():
                try:
                    ticker = str(row['Ticker']).strip()
                    cik = str(row['CIK']).strip().zfill(10)  # Ensure 10-digit CIK
                    series_id = str(row['Series']).strip()
                    
                    # Add to database
                    self.add_ticker_mapping(ticker, cik, series_id)
                    
                except Exception as e:
                    logger.error(f"Error processing row for ticker {row.get('Ticker', 'unknown')}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error importing tickers from CSV: {e}")
    
    def clear_ticker_data(self, ticker: str):
        """Clear all data for a specific ticker"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('DELETE FROM swap_data WHERE ticker = ?', (ticker,))
            conn.commit()
        except Exception as e:
            logger.error(f"Error clearing ticker data: {e}")
            conn.rollback()
        finally:
            conn.close()

def main():
    # Example usage
    extractor = ETFSwapDataExtractor()
    
    # Import tickers from CSV
    extractor.import_tickers_from_csv('ETF Tickers CIK_SERIES_6_16_25 - CIK_SERIES.csv')
    
    # Process a ticker
    ticker = "TSLL"
    cik = "0001424958"
    series_id = "S000007784"
    
    extractor.process_ticker(ticker, cik, series_id=series_id)
    
    # Export to CSV
    extractor.export_to_csv(f"{ticker.lower()}_swap_data.csv", ticker)

if __name__ == "__main__":
    main() 