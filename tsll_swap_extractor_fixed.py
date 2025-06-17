import requests
import xml.etree.ElementTree as ET
import time
import re
from datetime import datetime, timedelta
import json

def get_tsll_swap_data():
    """Extract swap data from TSLL N-PORT filings"""
    
    # Correct TSLL identifiers
    cik = "0001424958"
    series_id = "S000072483"
    class_id = "C000228774"
    ticker = "TSLL"
    
    # IMPORTANT: Replace with your actual email address
    headers = {
        'User-Agent': 'AnthonyCrinieri tonycrinieri@gmail.com'
    }
    
    print(f"🔍 Extracting swap data for {ticker}")
    print(f"   CIK: {cik} | Series: {series_id} | Class: {class_id}")
    print("=" * 60)
    
    try:
        # Get company filings
        company_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        response = requests.get(company_url, headers=headers)
        
        if response.status_code != 200:
            print(f"❌ Failed to get company data: {response.status_code}")
            return
            
        company_data = response.json()
        print(f"✅ Found: {company_data.get('name', 'N/A')}")
        
        # Get recent filings
        recent_filings = company_data.get('filings', {}).get('recent', {})
        forms = recent_filings.get('form', [])
        dates = recent_filings.get('filingDate', [])
        access_numbers = recent_filings.get('accessionNumber', [])
        
        # Find NPORT-P filings (corrected form type)
        nport_filings = []
        for i, form in enumerate(forms):
            if form == 'NPORT-P':  # Exact match for NPORT-P
                nport_filings.append({
                    'form': form,
                    'date': dates[i],
                    'accession': access_numbers[i],
                    'date_obj': datetime.strptime(dates[i], '%Y-%m-%d')
                })
        
        # Sort by date (most recent first)
        nport_filings.sort(key=lambda x: x['date_obj'], reverse=True)
        
        print(f"📄 Found {len(nport_filings)} NPORT-P filings")
        
        if not nport_filings:
            print("❌ No NPORT-P filings found")
            return
        
        # Process recent filings
        swap_data = []
        processed_count = 0
        
        for filing in nport_filings[:5]:  # Process 5 most recent
            print(f"\n📋 Processing: {filing['form']} - {filing['date']}")
            
            try:
                # Download filing
                accession_clean = filing['accession'].replace('-', '')
                filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/{filing['accession']}.txt"
                
                print(f"   📥 Downloading from: {filing_url}")
                
                time.sleep(0.1)  # Rate limiting
                filing_response = requests.get(filing_url, headers=headers)
                
                if filing_response.status_code != 200:
                    print(f"   ❌ Download failed: {filing_response.status_code}")
                    continue
                
                content = filing_response.text
                print(f"   ✅ Downloaded ({len(content)} characters)")
                
                # Extract XML from the filing
                xml_match = re.search(r'<\?xml.*?</nport-p>', content, re.DOTALL | re.IGNORECASE)
                if not xml_match:
                    # Try alternative XML patterns
                    xml_match = re.search(r'<nport-p.*?</nport-p>', content, re.DOTALL | re.IGNORECASE)
                
                if not xml_match:
                    print(f"   ⚠️  No XML content found")
                    continue
                
                xml_content = xml_match.group(0)
                print(f"   📄 Found XML content ({len(xml_content)} characters)")
                
                # Parse XML
                try:
                    root = ET.fromstring(xml_content)
                    print(f"   ✅ XML parsed successfully")
                    
                    # Look for series-specific data
                    series_data = find_series_data(root, series_id, class_id)
                    if series_data:
                        print(f"   🎯 Found series data for {series_id}")
                        
                        # Extract swap information
                        filing_swaps = extract_swap_data(series_data, filing['date'])
                        if filing_swaps:
                            swap_data.extend(filing_swaps)
                            print(f"   💰 Found {len(filing_swaps)} swap positions")
                        else:
                            print(f"   ⚠️  No swap data found in this filing")
                    else:
                        print(f"   ⚠️  Series {series_id} not found in this filing")
                        
                    processed_count += 1
                    
                except ET.ParseError as e:
                    print(f"   ❌ XML parsing error: {e}")
                    continue
                    
            except Exception as e:
                print(f"   ❌ Error processing filing: {e}")
                continue
        
        print(f"\n" + "=" * 60)
        print(f"🎯 EXTRACTION COMPLETE")
        print(f"📊 Processed {processed_count} filings")
        print(f"💰 Total swap positions found: {len(swap_data)}")
        
        if swap_data:
            print(f"\n📋 Sample swap data:")
            for i, swap in enumerate(swap_data[:3]):  # Show first 3
                print(f"   {i+1}. {swap.get('description', 'N/A')}")
                print(f"      Value: ${swap.get('value', 0):,.2f}")
                print(f"      Date: {swap.get('date', 'N/A')}")
                
            # Save to file
            save_swap_data(swap_data, ticker)
        else:
            print("⚠️  No swap data found across all processed filings")
            
        return swap_data
        
    except Exception as e:
        print(f"❌ Error in main extraction: {e}")
        return []

def find_series_data(root, series_id, class_id):
    """Find data for specific series and class"""
    try:
        # Look for series information in the XML
        for elem in root.iter():
            # Check if this element contains our series ID
            if elem.text and series_id in str(elem.text):
                # Found our series, now look for parent or nearby class data
                parent = elem.getparent() if hasattr(elem, 'getparent') else None
                if parent is not None:
                    # Check if class ID is nearby
                    for child in parent.iter():
                        if child.text and class_id in str(child.text):
                            return parent
                            
            # Also check for class ID directly
            if elem.text and class_id in str(elem.text):
                parent = elem.getparent() if hasattr(elem, 'getparent') else None
                if parent is not None:
                    return parent
                    
        # If specific IDs not found, return root for broader search
        return root
        
    except Exception as e:
        print(f"   ⚠️  Error finding series data: {e}")
        return root

def extract_swap_data(xml_element, filing_date):
    """Extract swap data from XML element"""
    swaps = []
    
    try:
        # Common swap-related XML tags in N-PORT filings
        swap_tags = [
            'derivativeInstrument',
            'swap',
            'totalReturnSwap',
            'equitySwap',
            'instrument'
        ]
        
        for tag in swap_tags:
            for elem in xml_element.iter():
                if tag.lower() in elem.tag.lower():
                    # Found potential swap element
                    swap_info = parse_swap_element(elem, filing_date)
                    if swap_info:
                        swaps.append(swap_info)
                        
        # Also look for text content mentioning swaps
        swap_keywords = ['total return swap', 'equity swap', 'return swap', 'swap agreement']
        
        for elem in xml_element.iter():
            if elem.text:
                text = elem.text.lower()
                for keyword in swap_keywords:
                    if keyword in text:
                        # Found swap mention in text
                        swap_info = parse_text_swap_reference(elem, filing_date)
                        if swap_info:
                            swaps.append(swap_info)
                            
    except Exception as e:
        print(f"   ⚠️  Error extracting swap data: {e}")
        
    return swaps

def parse_swap_element(elem, filing_date):
    """Parse individual swap element"""
    try:
        swap_data = {
            'date': filing_date,
            'description': elem.text or 'Unknown swap',
            'value': 0,
            'tag': elem.tag
        }
        
        # Look for value information in child elements
        for child in elem:
            if 'value' in child.tag.lower() or 'amount' in child.tag.lower():
                try:
                    swap_data['value'] = float(child.text or 0)
                except (ValueError, TypeError):
                    pass
                    
            if 'description' in child.tag.lower() or 'name' in child.tag.lower():
                swap_data['description'] = child.text or swap_data['description']
                
        return swap_data
        
    except Exception as e:
        return None

def parse_text_swap_reference(elem, filing_date):
    """Parse swap reference found in text"""
    try:
        return {
            'date': filing_date,
            'description': elem.text[:100] + '...' if len(elem.text) > 100 else elem.text,
            'value': 0,
            'tag': elem.tag,
            'type': 'text_reference'
        }
    except Exception as e:
        return None

def save_swap_data(swap_data, ticker):
    """Save swap data to file"""
    try:
        filename = f"{ticker}_swap_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(filename, 'w') as f:
            json.dump(swap_data, f, indent=2, default=str)
            
        print(f"💾 Swap data saved to: {filename}")
        
    except Exception as e:
        print(f"⚠️  Error saving data: {e}")

if __name__ == "__main__":
    # IMPORTANT: Update the email in the headers before running
    print("🚀 TSLL Swap Data Extractor")
    print("⚠️  Make sure to update your email address in the headers!")
    print()
    
    swap_data = get_tsll_swap_data()
    
    if swap_data:
        print(f"\n✅ Successfully extracted {len(swap_data)} swap positions!")
    else:
        print(f"\n❌ No swap data found. Check the diagnostic output above.")