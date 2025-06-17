import requests
import xml.etree.ElementTree as ET
import time
import re
from datetime import datetime, timedelta
import json

def test_tsll_filings():
    """Test script to diagnose TSLL N-PORT filing issues"""
    
    cik = "0001424958"  # Correct CIK for TSLL
    series_id = "S000072483"  # Series ID
    class_id = "C000228774"   # Class/Contract ID
    ticker = "TSLL"
    
    # IMPORTANT: Replace with your actual email address
    headers = {
        'User-Agent': 'VegaShares tonycrinieri@gmail.com'
    }
    
    print(f"🔍 Diagnosing N-PORT filings for {ticker}")
    print(f"   CIK: {cik}")
    print(f"   Series: {series_id}")
    print(f"   Class: {class_id}")
    print("=" * 60)
    
    # Test 1: Check company information
    print("\n📋 TEST 1: Company Information")
    try:
        company_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        response = requests.get(company_url, headers=headers)
        
        if response.status_code == 200:
            company_data = response.json()
            print(f"✅ Company found: {company_data.get('name', 'N/A')}")
            print(f"   Trading symbol: {company_data.get('tickers', ['N/A'])[0] if company_data.get('tickers') else 'N/A'}")
            print(f"   SIC: {company_data.get('sic', 'N/A')}")
            print(f"   SIC Description: {company_data.get('sicDescription', 'N/A')}")
            
            # Check recent filings
            recent_filings = company_data.get('filings', {}).get('recent', {})
            forms = recent_filings.get('form', [])
            dates = recent_filings.get('filingDate', [])
            
            print(f"\n📊 Recent filings summary:")
            form_counts = {}
            for form in forms:
                form_counts[form] = form_counts.get(form, 0) + 1
            
            for form, count in sorted(form_counts.items()):
                print(f"   {form}: {count} filings")
                
        else:
            print(f"❌ Failed to get company data: {response.status_code}")
            return
            
    except Exception as e:
        print(f"❌ Error getting company data: {e}")
        return
    
    time.sleep(0.1)  # Rate limiting
    
    # Test 2: Search for N-PORT filings specifically
    print("\n📋 TEST 2: N-PORT Filing Search")
    try:
        recent_filings = company_data.get('filings', {}).get('recent', {})
        forms = recent_filings.get('form', [])
        dates = recent_filings.get('filingDate', [])
        access_numbers = recent_filings.get('accessionNumber', [])
        
        nport_filings = []
        for i, form in enumerate(forms):
            if 'N-PORT' in form.upper():
                nport_filings.append({
                    'form': form,
                    'date': dates[i],
                    'accession': access_numbers[i]
                })
        
        print(f"🎯 Found {len(nport_filings)} N-PORT filings:")
        if nport_filings:
            for filing in nport_filings[:5]:  # Show first 5
                print(f"   📄 {filing['form']} - {filing['date']} - {filing['accession']}")
                
            # Test downloading the most recent N-PORT
            most_recent = nport_filings[0]
            print(f"\n🔬 Testing download of most recent N-PORT:")
            print(f"   Form: {most_recent['form']}")
            print(f"   Date: {most_recent['date']}")
            print(f"   Accession: {most_recent['accession']}")
            
            # Try to download and parse
            accession_clean = most_recent['accession'].replace('-', '')
            filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/{most_recent['accession']}.txt"
            
            print(f"   📥 Attempting download from: {filing_url}")
            
            filing_response = requests.get(filing_url, headers=headers)
            if filing_response.status_code == 200:
                content = filing_response.text
                print(f"   ✅ Downloaded successfully ({len(content)} characters)")
                
                # Look for XML content
                xml_match = re.search(r'<\?xml.*?</nport-p>', content, re.DOTALL | re.IGNORECASE)
                if xml_match:
                    xml_content = xml_match.group(0)
                    print(f"   📄 Found XML content ({len(xml_content)} characters)")
                    
                    # Try to parse XML
                    try:
                        root = ET.fromstring(xml_content)
                        print(f"   ✅ XML parsed successfully")
                        print(f"   📋 Root element: {root.tag}")
                        
                        # Look for swap-related elements
                        swap_keywords = ['swap', 'derivative', 'total return', 'return swap']
                        found_swaps = []
                        
                        for elem in root.iter():
                            if elem.text and any(keyword in elem.text.lower() for keyword in swap_keywords):
                                found_swaps.append(f"{elem.tag}: {elem.text[:100]}...")
                        
                        print(f"   🔍 Found {len(found_swaps)} potential swap-related elements:")
                        for swap in found_swaps[:3]:  # Show first 3
                            print(f"      {swap}")
                            
                    except ET.ParseError as e:
                        print(f"   ❌ XML parsing error: {e}")
                        
                else:
                    print(f"   ⚠️  No XML content found in filing")
                    # Show first 500 chars to see format
                    print(f"   📄 First 500 characters:")
                    print(f"      {content[:500]}...")
                    
            else:
                print(f"   ❌ Failed to download filing: {filing_response.status_code}")
        else:
            print("   ⚠️  No N-PORT filings found!")
            
    except Exception as e:
        print(f"❌ Error in N-PORT search: {e}")
    
    # Test 3: Alternative search methods
    print("\n📋 TEST 3: Alternative Search Methods")
    
    # Try different form variations
    form_variations = ['N-PORT', 'NPORT', 'N-PORTEX', 'N-PORT/A']
    
    for form_var in form_variations:
        matching_filings = []
        for i, form in enumerate(forms):
            if form_var.upper() in form.upper():
                matching_filings.append({
                    'form': form,
                    'date': dates[i],
                    'accession': access_numbers[i]
                })
        print(f"   🔍 '{form_var}' search: {len(matching_filings)} matches")
    
    # Test 4: Manual verification URLs
    print("\n📋 TEST 4: Manual Verification URLs")
    print("   🌐 EDGAR Browse URL:")
    print(f"      https://www.sec.gov/edgar/browse/?CIK={cik}&owner=exclude")
    print("   🌐 SEC Search URL:")
    print(f"      https://www.sec.gov/edgar/search/#/q=N-PORT&ciks={cik}")
    print("   🌐 Series-Specific Search:")
    print(f"      https://www.sec.gov/edgar/search/#/q={series_id}")
    print(f"      https://www.sec.gov/edgar/search/#/q={class_id}")
    
    # Test 5: Series-specific N-PORT search
    print("\n📋 TEST 5: Series-Specific N-PORT Search")
    try:
        # For series funds, N-PORT filings might be filed under the series ID
        print(f"   🔍 Searching for N-PORT filings with Series ID: {series_id}")
        
        # Look through filings for series-specific references
        series_filings = []
        for i, form in enumerate(forms):
            if 'N-PORT' in form.upper():
                # Check if this filing might be for our specific series
                accession = access_numbers[i]
                series_filings.append({
                    'form': form,
                    'date': dates[i],
                    'accession': accession,
                    'series_match': series_id in accession or class_id in accession
                })
        
        if series_filings:
            print(f"   📄 Found {len(series_filings)} N-PORT filings to check:")
            for filing in series_filings:
                match_indicator = "✅" if filing['series_match'] else "❓"
                print(f"      {match_indicator} {filing['form']} - {filing['date']} - {filing['accession']}")
        else:
            print("   ⚠️  No N-PORT filings found for this series")
            
    except Exception as e:
        print(f"❌ Error in series-specific search: {e}")
    
    # Test 6: Check if it's a series fund
    print("\n📋 TEST 6: Series Fund Structure Check")
    try:
        # Look for series information in company data
        if 'addresses' in company_data:
            print("   📍 Company has address information - might be series fund")
            
        # Check for multiple class/series filings
        class_filings = [f for f in forms if any(keyword in f.upper() for keyword in ['485BPOS', 'N-1A', 'N-CSR'])]
        print(f"   📊 Fund-related filings found: {len(class_filings)}")
        
        if class_filings:
            print("   ✅ This appears to be a fund - N-PORT filings should exist")
        else:
            print("   ⚠️  Limited fund filings - might not be required to file N-PORT")
            
    except Exception as e:
        print(f"❌ Error in series fund check: {e}")
    
    print("\n" + "=" * 60)
    print("🎯 DIAGNOSIS COMPLETE")
    print("\n📋 Next Steps:")
    print("1. Check the manual URLs above in your browser")
    print("2. Look for N-PORT filings in the EDGAR interface")
    print("3. Report back what you find!")
    print("\n💡 If you see N-PORT filings manually but script doesn't find them,")
    print("   we'll need to adjust the search parameters or parsing logic.")

if __name__ == "__main__":
    test_tsll_filings()