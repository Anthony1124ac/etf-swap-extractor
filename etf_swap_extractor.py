import requests
import sqlite3
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import time
import re
import os
from typing import Dict, List, Optional, Tuple
import json

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
        
        # Create table for storing swap data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS swap_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                filing_date TEXT NOT NULL,
                report_date TEXT NOT NULL,
                swap_counterparty TEXT,
                notional_amount REAL,
                fixed_or_floating TEXT,
                floating_rate_index TEXT,
                floating_rate_spread REAL,
                drp REAL,
                filing_url TEXT,
                extracted_date TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(ticker, filing_date, swap_counterparty, notional_amount)
            )
        ''')
        
        # Create table for storing ticker to CIK mappings
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ticker_mappings (
                ticker TEXT PRIMARY KEY,
                cik TEXT NOT NULL,
                company_name TEXT,
                series_id TEXT,
                last_updated TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        print("Database initialized successfully")
    
    def get_cik_from_ticker(self, ticker: str) -> Optional[Tuple[str, str]]:
        """Get CIK and Series ID from ticker symbol"""
        # First check if we have it cached
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT cik, series_id FROM ticker_mappings WHERE ticker = ?", (ticker,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return result[0], result[1]
        
        # Try multiple approaches to find CIK
        
        # Method 1: Try the company search JSON API (more reliable)
        try:
            search_url = "https://www.sec.gov/files/company_tickers.json"
            response = requests.get(search_url, headers=self.headers)
            time.sleep(0.1)
            
            if response.status_code == 200:
                data = response.json()
                for entry in data.values():
                    if entry.get('ticker', '').upper() == ticker.upper():
                        cik = str(entry['cik_str']).zfill(10)
                        company_name = entry['title']
                        
                        # Find series ID
                        series_id = self._find_series_id(cik, ticker)
                        
                        # Cache the result
                        conn = sqlite3.connect(self.db_path)
                        cursor = conn.cursor()
                        cursor.execute('''
                            INSERT OR REPLACE INTO ticker_mappings 
                            (ticker, cik, company_name, series_id) 
                            VALUES (?, ?, ?, ?)
                        ''', (ticker, cik, company_name, series_id))
                        conn.commit()
                        conn.close()
                        
                        return cik, series_id
        except Exception as e:
            print(f"Method 1 failed: {e}")
        
        # Method 2: Try EDGAR search with HTML parsing
        try:
            search_url = f"{self.base_url}/cgi-bin/browse-edgar"
            params = {
                'action': 'getcompany',
                'ticker': ticker,
                'owner': 'exclude',
                'count': '10'
            }
            
            response = requests.get(search_url, params=params, headers=self.headers)
            time.sleep(0.1)
            
            if response.status_code == 200:
                content = response.text
                
                # Look for CIK in the HTML
                cik_match = re.search(r'CIK=(\d+)', content)
                if cik_match:
                    cik = cik_match.group(1).zfill(10)
                    
                    # Find company name
                    name_match = re.search(r'<h1>([^<]+)</h1>', content)
                    company_name = name_match.group(1) if name_match else ""
                    
                    # Find series ID
                    series_id = self._find_series_id(cik, ticker)
                    
                    # Cache the result
                    conn = sqlite3.connect(self.db_path)
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT OR REPLACE INTO ticker_mappings 
                        (ticker, cik, company_name, series_id) 
                        VALUES (?, ?, ?, ?)
                    ''', (ticker, cik, company_name, series_id))
                    conn.commit()
                    conn.close()
                    
                    return cik, series_id
        except Exception as e:
            print(f"Method 2 failed: {e}")
        
        # Method 3: Manual lookup for common leveraged ETFs
        manual_mappings = {
            'TSLL': ('0001689873', 'S000076344'),  # Direxion - CORRECTED
            'TQQQ': ('0001424958', None),  # ProShares  
            'SOXL': ('0001593063', None),  # Direxion
            'FNGU': ('0001593063', None),  # Direxion
            'TECL': ('0001593063', None),  # Direxion
            'UPRO': ('0001424958', None),  # ProShares
            'SPXL': ('0001593063', None),  # Direxion
        }
        
        if ticker.upper() in manual_mappings:
            cik, series_id = manual_mappings[ticker.upper()]
            
            # Cache the result
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO ticker_mappings 
                (ticker, cik, company_name, series_id) 
                VALUES (?, ?, ?, ?)
            ''', (ticker, cik, f"Manual mapping for {ticker}", series_id))
            conn.commit()
            conn.close()
            
            return cik, series_id
        
        print(f"Could not find CIK for ticker {ticker} using any method")
        return None, None
    
    def _find_series_id(self, cik: str, ticker: str) -> Optional[str]:
        """Find series ID for ETF from recent N-PORT filings"""
        # Get recent filings to find series ID
        filings_url = f"{self.base_url}/cgi-bin/browse-edgar"
        params = {
            'action': 'getcompany',
            'CIK': cik,
            'type': 'N-PORT',
            'count': '10',
            'output': 'xml'
        }
        
        try:
            response = requests.get(filings_url, params=params, headers=self.headers)
            time.sleep(0.1)
            
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                
                # Look for the first N-PORT filing
                for filing in root.findall('.//filing'):
                    filing_href = filing.find('filingHREF')
                    if filing_href is not None:
                        # Get the filing details to extract series ID
                        filing_url = self.base_url + filing_href.text
                        series_id = self._extract_series_from_filing(filing_url, ticker)
                        if series_id:
                            return series_id
                            
        except Exception as e:
            print(f"Error finding series ID for {ticker}: {e}")
        
        return None
    
    def _extract_series_from_filing(self, filing_url: str, ticker: str) -> Optional[str]:
        """Extract series ID from a specific N-PORT filing"""
        try:
            response = requests.get(filing_url, headers=self.headers)
            time.sleep(0.1)
            
            if response.status_code == 200:
                # Look for series information in the filing
                content = response.text
                
                # Common patterns for series ID in N-PORT filings
                series_patterns = [
                    r'<seriesId>([^<]+)</seriesId>',
                    r'Series ID[:\s]*([A-Za-z0-9\-]+)',
                    r'series[^>]*>([A-Za-z0-9\-]+)'
                ]
                
                for pattern in series_patterns:
                    match = re.search(pattern, content, re.IGNORECASE)
                    if match:
                        return match.group(1).strip()
                        
        except Exception as e:
            print(f"Error extracting series from filing: {e}")
        
        return None
    
    def get_nport_filings(self, cik: str, ticker: str, years_back: int = 3) -> List[Dict]:
        """Get all N-PORT filings for a given CIK over specified time period"""
        filings = []
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years_back * 365)
        
        # Search for N-PORT filings
        search_url = f"{self.base_url}/cgi-bin/browse-edgar"
        params = {
            'action': 'getcompany',
            'CIK': cik,
            'type': 'N-PORT',
            'dateb': end_date.strftime('%Y%m%d'),
            'count': '100',
            'output': 'xml'
        }
        
        try:
            response = requests.get(search_url, params=params, headers=self.headers)
            time.sleep(0.1)
            
            if response.status_code == 200:
                root = ET.fromstring(response.content)
                
                for filing in root.findall('.//filing'):
                    filing_date_elem = filing.find('filingDate')
                    filing_href_elem = filing.find('filingHREF')
                    
                    if filing_date_elem is not None and filing_href_elem is not None:
                        filing_date = datetime.strptime(filing_date_elem.text, '%Y-%m-%d')
                        
                        if filing_date >= start_date:
                            filings.append({
                                'date': filing_date_elem.text,
                                'url': self.base_url + filing_href_elem.text
                            })
                            
        except Exception as e:
            print(f"Error retrieving N-PORT filings for {ticker}: {e}")
        
        return sorted(filings, key=lambda x: x['date'], reverse=True)
    
    def extract_swap_data_from_filing(self, filing_url: str, ticker: str) -> List[Dict]:
        """Extract swap data from a single N-PORT filing"""
        swap_data = []
        
        try:
            # Get the filing page first
            response = requests.get(filing_url, headers=self.headers)
            time.sleep(0.1)
            
            if response.status_code != 200:
                return swap_data
            
            # Find XML file link in the filing
            content = response.text
            xml_links = re.findall(r'<a[^>]*href="([^"]*\.xml)"[^>]*>', content, re.IGNORECASE)
            
            for xml_link in xml_links:
                if not xml_link.startswith('http'):
                    xml_url = self.base_url + xml_link
                else:
                    xml_url = xml_link
                
                # Download and parse XML
                xml_response = requests.get(xml_url, headers=self.headers)
                time.sleep(0.1)
                
                if xml_response.status_code == 200:
                    swap_data.extend(self._parse_nport_xml(xml_response.content, ticker, filing_url))
                    
        except Exception as e:
            print(f"Error extracting swap data from filing: {e}")
        
        return swap_data
    
    def _parse_nport_xml(self, xml_content: bytes, ticker: str, filing_url: str) -> List[Dict]:
        """Parse N-PORT XML content to extract swap data"""
        swap_data = []
        
        try:
            root = ET.fromstring(xml_content)
            
            # Define namespace mappings (N-PORT filings use namespaces)
            namespaces = {
                'ns1': 'http://www.sec.gov/edgar/nport',
                'com': 'http://www.sec.gov/edgar/common'
            }
            
            # Try to find namespace in the XML if not predefined
            if root.tag.startswith('{'):
                default_ns = root.tag.split('}')[0][1:]
                namespaces[''] = default_ns
            
            # Look for swap/derivative instruments
            # N-PORT uses various tags for derivatives
            derivative_paths = [
                './/derivativeInstrument',
                './/derivative',
                './/swap',
                './/swapInstrument',
                './/otherDerivInst'
            ]
            
            for path in derivative_paths:
                derivatives = root.findall(path, namespaces)
                
                for derivative in derivatives:
                    swap_info = self._extract_swap_info(derivative, namespaces)
                    if swap_info:
                        swap_info['ticker'] = ticker
                        swap_info['filing_url'] = filing_url
                        swap_data.append(swap_info)
            
            # Also check for investment-level data
            investments = root.findall('.//invstOrSec', namespaces)
            for investment in investments:
                # Check if this is a derivative/swap
                asset_cat = investment.find('.//assetCat', namespaces)
                if asset_cat is not None and asset_cat.text in ['DC', 'DE']:  # Derivative categories
                    swap_info = self._extract_swap_info(investment, namespaces)
                    if swap_info:
                        swap_info['ticker'] = ticker
                        swap_info['filing_url'] = filing_url
                        swap_data.append(swap_info)
                        
        except ET.ParseError as e:
            print(f"XML parsing error: {e}")
        except Exception as e:
            print(f"Error parsing N-PORT XML: {e}")
        
        return swap_data
    
    def _extract_swap_info(self, element: ET.Element, namespaces: Dict) -> Optional[Dict]:
        """Extract swap information from XML element"""
        swap_info = {}
        
        try:
            # Extract counterparty information
            counterparty_elem = element.find('.//counterpartyName', namespaces)
            if counterparty_elem is None:
                counterparty_elem = element.find('.//ctrPtyName', namespaces)
            if counterparty_elem is not None:
                swap_info['counterparty'] = counterparty_elem.text
            
            # Extract notional amount
            notional_elem = element.find('.//notionalAmt', namespaces)
            if notional_elem is None:
                notional_elem = element.find('.//notional', namespaces)
            if notional_elem is not None:
                try:
                    swap_info['notional_amount'] = float(notional_elem.text)
                except ValueError:
                    pass
            
            # Extract fixed or floating rate type
            rate_type_elem = element.find('.//fixedOrFloating', namespaces)
            if rate_type_elem is None:
                rate_type_elem = element.find('.//rateType', namespaces)
            if rate_type_elem is not None:
                swap_info['fixed_or_floating'] = rate_type_elem.text
            
            # Extract floating rate index
            rate_index_elem = element.find('.//floatingRtIndex', namespaces)
            if rate_index_elem is None:
                rate_index_elem = element.find('.//floatingRateIndex', namespaces)
            if rate_index_elem is not None:
                swap_info['floating_rate_index'] = rate_index_elem.text
            
            # Extract floating rate spread  
            rate_spread_elem = element.find('.//floatingrtspread', namespaces)
            if rate_spread_elem is None:
                rate_spread_elem = element.find('.//floatingRateSpread', namespaces)
            if rate_spread_elem is not None:
                try:
                    swap_info['floating_rate_spread'] = float(rate_spread_elem.text)
                except ValueError:
                    pass
            
            # Extract DRP (Delta Risk Premium or similar)
            drp_elem = element.find('.//drp', namespaces)
            if drp_elem is None:
                drp_elem = element.find('.//deltaRiskPremium', namespaces)
            if drp_elem is not None:
                try:
                    swap_info['drp'] = float(drp_elem.text)
                except ValueError:
                    pass
            
            # Only return if we have meaningful data
            if len(swap_info) > 0:
                return swap_info
                
        except Exception as e:
            print(f"Error extracting swap info: {e}")
        
        return None
    
    def save_swap_data(self, swap_data: List[Dict], filing_date: str, report_date: str = None):
        """Save swap data to database"""
        if not swap_data:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for data in swap_data:
            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO swap_data 
                    (ticker, filing_date, report_date, swap_counterparty, notional_amount, 
                     fixed_or_floating, floating_rate_index, floating_rate_spread, drp, filing_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    data.get('ticker'),
                    filing_date,
                    report_date or filing_date,
                    data.get('counterparty'),
                    data.get('notional_amount'),
                    data.get('fixed_or_floating'),
                    data.get('floating_rate_index'),
                    data.get('floating_rate_spread'),
                    data.get('drp'),
                    data.get('filing_url')
                ))
            except sqlite3.Error as e:
                print(f"Database error: {e}")
        
        conn.commit()
        conn.close()
    
    def process_ticker(self, ticker: str, years_back: int = 3) -> bool:
        """Main function to process a ticker and extract all historical swap data"""
        print(f"\nProcessing ticker: {ticker}")
        
        # Get CIK and Series ID
        cik, series_id = self.get_cik_from_ticker(ticker)
        if not cik:
            print(f"Could not find CIK for ticker {ticker}")
            return False
        
        print(f"Found CIK: {cik}")
        if series_id:
            print(f"Series ID: {series_id}")
        
        # Get all N-PORT filings
        filings = self.get_nport_filings(cik, ticker, years_back)
        print(f"Found {len(filings)} N-PORT filings")
        
        total_swaps = 0
        for filing in filings:
            print(f"Processing filing from {filing['date']}...")
            
            # Extract swap data from this filing
            swap_data = self.extract_swap_data_from_filing(filing['url'], ticker)
            
            if swap_data:
                print(f"  Found {len(swap_data)} swap records")
                self.save_swap_data(swap_data, filing['date'])
                total_swaps += len(swap_data)
            else:
                print("  No swap data found")
            
            # Be respectful to SEC servers
            time.sleep(0.2)
        
        print(f"Completed processing {ticker}. Total swap records: {total_swaps}")
        return True
    
    def get_ticker_data(self, ticker: str) -> List[Dict]:
        """Retrieve all stored data for a specific ticker"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT filing_date, report_date, swap_counterparty, notional_amount,
                   fixed_or_floating, floating_rate_index, floating_rate_spread, drp
            FROM swap_data 
            WHERE ticker = ? 
            ORDER BY filing_date DESC
        ''', (ticker,))
        
        columns = [description[0] for description in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return results
    
    def export_ticker_data(self, ticker: str, filename: str = None):
        """Export ticker data to CSV"""
        import csv
        
        data = self.get_ticker_data(ticker)
        if not data:
            print(f"No data found for ticker {ticker}")
            return
        
        if not filename:
            filename = f"{ticker}_swap_data_{datetime.now().strftime('%Y%m%d')}.csv"
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            if data:
                fieldnames = data[0].keys()
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
        
        print(f"Data exported to {filename}")

def main():
    """Main function to run the ETF swap data extractor"""
    print("ETF Swap Data Extractor")
    print("======================")
    
    # Initialize the extractor
    extractor = ETFSwapDataExtractor()
    
    while True:
        print("\nOptions:")
        print("1. Process a ticker (extract swap data)")
        print("2. View data for a ticker")
        print("3. Export data to CSV")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == '1':
            ticker = input("Enter ticker symbol (e.g., TSLL): ").strip().upper()
            if ticker:
                years = input("Enter years of history to retrieve (default 3): ").strip()
                years = int(years) if years.isdigit() else 3
                
                print(f"\nProcessing {ticker} for {years} years of history...")
                print("This may take several minutes due to SEC rate limits...")
                
                success = extractor.process_ticker(ticker, years)
                if success:
                    print(f"\n✓ Successfully processed {ticker}")
                else:
                    print(f"\n✗ Failed to process {ticker}")
        
        elif choice == '2':
            ticker = input("Enter ticker symbol: ").strip().upper()
            if ticker:
                data = extractor.get_ticker_data(ticker)
                if data:
                    print(f"\nSwap data for {ticker}:")
                    print("-" * 80)
                    for record in data[:10]:  # Show first 10 records
                        print(f"Date: {record['filing_date']}")
                        print(f"Counterparty: {record['swap_counterparty']}")
                        print(f"Notional: ${record['notional_amount']:,.2f}" if record['notional_amount'] else "N/A")
                        print(f"Type: {record['fixed_or_floating']}")
                        print(f"Index: {record['floating_rate_index']}")
                        print(f"Spread: {record['floating_rate_spread']}")
                        print(f"DRP: {record['drp']}")
                        print("-" * 40)
                    
                    if len(data) > 10:
                        print(f"... and {len(data) - 10} more records")
                else:
                    print(f"No data found for {ticker}")
        
        elif choice == '3':
            ticker = input("Enter ticker symbol: ").strip().upper()
            if ticker:
                filename = input("Enter filename (press Enter for default): ").strip()
                extractor.export_ticker_data(ticker, filename if filename else None)
        
        elif choice == '4':
            print("Goodbye!")
            break
        
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()