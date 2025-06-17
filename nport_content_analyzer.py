import requests
import time
import re
from datetime import datetime

def analyze_nport_structure():
    """Analyze the actual structure of TSLL's N-PORT filings"""
    
    cik = "0001424958"
    series_id = "S000072483"
    class_id = "C000228774"
    
    # IMPORTANT: Replace with your actual email address
    headers = {
        'User-Agent': 'AnthonyCrinieri tonycrinieri@gmail.com'
    }
    
    print("🔬 ANALYZING N-PORT FILING STRUCTURE")
    print("=" * 60)
    
    try:
        # Get company filings
        company_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
        response = requests.get(company_url, headers=headers)
        
        company_data = response.json()
        recent_filings = company_data.get('filings', {}).get('recent', {})
        forms = recent_filings.get('form', [])
        dates = recent_filings.get('filingDate', [])
        access_numbers = recent_filings.get('accessionNumber', [])
        
        # Find most recent NPORT-P filing
        nport_filing = None
        for i, form in enumerate(forms):
            if form == 'NPORT-P':
                nport_filing = {
                    'form': form,
                    'date': dates[i],
                    'accession': access_numbers[i]
                }
                break
        
        if not nport_filing:
            print("❌ No NPORT-P filing found")
            return
            
        print(f"📄 Analyzing: {nport_filing['form']} - {nport_filing['date']}")
        print(f"   Accession: {nport_filing['accession']}")
        
        # Download the filing
        accession_clean = nport_filing['accession'].replace('-', '')
        filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_clean}/{nport_filing['accession']}.txt"
        
        print(f"📥 Downloading from: {filing_url}")
        
        time.sleep(0.1)
        filing_response = requests.get(filing_url, headers=headers)
        
        if filing_response.status_code != 200:
            print(f"❌ Download failed: {filing_response.status_code}")
            return
            
        content = filing_response.text
        print(f"✅ Downloaded ({len(content)} characters)")
        
        # Analyze the content structure
        print("\n🔍 CONTENT ANALYSIS:")
        
        # 1. Check for different XML patterns
        print("\n1️⃣ XML Pattern Search:")
        xml_patterns = [
            r'<\?xml.*?</nport-p>',
            r'<nport-p.*?</nport-p>',
            r'<NPORT-P.*?</NPORT-P>',
            r'<xml>.*?</xml>',
            r'<\?xml.*?<\/[^>]+>',
        ]
        
        for i, pattern in enumerate(xml_patterns, 1):
            matches = re.findall(pattern, content, re.DOTALL | re.IGNORECASE)
            print(f"   Pattern {i}: {len(matches)} matches")
            if matches:
                print(f"     First match preview: {matches[0][:200]}...")
        
        # 2. Look for document structure markers
        print("\n2️⃣ Document Structure:")
        structure_markers = [
            '<DOCUMENT>',
            '<TYPE>',
            '<FILENAME>',
            'NPORT-P',
            '<TEXT>',
            '</TEXT>',
            '</DOCUMENT>'
        ]
        
        for marker in structure_markers:
            count = content.upper().count(marker.upper())
            print(f"   {marker}: {count} occurrences")
        
        # 3. Find DOCUMENT sections
        print("\n3️⃣ Document Sections:")
        doc_pattern = r'<DOCUMENT>(.*?)</DOCUMENT>'
        documents = re.findall(doc_pattern, content, re.DOTALL | re.IGNORECASE)
        
        print(f"   Found {len(documents)} document sections")
        
        for i, doc in enumerate(documents):
            # Look for TYPE in each document
            type_match = re.search(r'<TYPE>(.*?)[\r\n]', doc, re.IGNORECASE)
            filename_match = re.search(r'<FILENAME>(.*?)[\r\n]', doc, re.IGNORECASE)
            
            doc_type = type_match.group(1).strip() if type_match else "Unknown"
            filename = filename_match.group(1).strip() if filename_match else "Unknown"
            
            print(f"   Doc {i+1}: Type='{doc_type}', Filename='{filename}'")
            
            # If this is an NPORT-P document, analyze its content
            if 'NPORT' in doc_type.upper():
                print(f"      📋 Analyzing NPORT document content:")
                
                # Look for TEXT section
                text_match = re.search(r'<TEXT>(.*?)</TEXT>', doc, re.DOTALL | re.IGNORECASE)
                if text_match:
                    text_content = text_match.group(1)
                    print(f"         TEXT section: {len(text_content)} characters")
                    
                    # Show first 500 characters of TEXT content
                    print(f"         Preview:")
                    print(f"         {text_content[:500]}...")
                    
                    # Look for our series/class IDs in this content
                    series_found = series_id in text_content
                    class_found = class_id in text_content
                    
                    print(f"         Series {series_id} found: {'✅' if series_found else '❌'}")
                    print(f"         Class {class_id} found: {'✅' if class_found else '❌'}")
                    
                    # Look for swap-related content
                    swap_keywords = ['swap', 'derivative', 'total return']
                    swap_mentions = []
                    
                    for keyword in swap_keywords:
                        matches = len(re.findall(keyword, text_content, re.IGNORECASE))
                        if matches > 0:
                            swap_mentions.append(f"{keyword}: {matches}")
                    
                    if swap_mentions:
                        print(f"         Swap mentions: {', '.join(swap_mentions)}")
                    else:
                        print(f"         No obvious swap mentions found")
                        
                else:
                    print(f"         No TEXT section found")
        
        # 4. Search for series-specific content
        print(f"\n4️⃣ Series-Specific Search:")
        print(f"   Searching for Series ID: {series_id}")
        print(f"   Searching for Class ID: {class_id}")
        
        series_positions = []
        for match in re.finditer(series_id, content):
            start = max(0, match.start() - 100)
            end = min(len(content), match.end() + 100)
            context = content[start:end].replace('\n', ' ').replace('\r', ' ')
            series_positions.append(context)
        
        print(f"   Series ID found {len(series_positions)} times")
        if series_positions:
            print(f"   First occurrence context:")
            print(f"      ...{series_positions[0]}...")
        
        class_positions = []
        for match in re.finditer(class_id, content):
            start = max(0, match.start() - 100)
            end = min(len(content), match.end() + 100)
            context = content[start:end].replace('\n', ' ').replace('\r', ' ')
            class_positions.append(context)
        
        print(f"   Class ID found {len(class_positions)} times")
        if class_positions:
            print(f"   First occurrence context:")
            print(f"      ...{class_positions[0]}...")
            
        print("\n" + "=" * 60)
        print("🎯 ANALYSIS COMPLETE")
        print("\n💡 Next steps based on findings:")
        print("1. If XML found: Update parsing logic")
        print("2. If no XML: Look for alternative formats (JSON, delimited, etc.)")
        print("3. If series/class found: Focus extraction on those sections")
        
    except Exception as e:
        print(f"❌ Error in analysis: {e}")

if __name__ == "__main__":
    analyze_nport_structure()