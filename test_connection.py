# test_connection.py
import gspread
from google.oauth2.service_account import Credentials
import sys
import os

def test_google_sheets_connection():
    """Test Google Sheets API connection"""
    
    print("🔧 Google Sheets API Connection Test")
    print("=" * 50)
    
    # File paths
    credentials_file = "credentials.json"
    sheet_id = "1cWCO0gKBv1kIrnWg_katFLoIqAcuLj869qXiJIdkzLE"
    
    try:
        print("📋 1. Checking credentials file...")
        
        # Define scope
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # Load credentials
        creds = Credentials.from_service_account_file(
            credentials_file, 
            scopes=scope
        )
        print("✅ Credentials loaded successfully")
        
        print("\n📋 2. Connecting to Google Sheets API...")
        # Create client
        client = gspread.authorize(creds)
        print("✅ API connection successful")
        
        print("\n📋 3. Testing sheet access...")
        # Open sheet
        sheet = client.open_by_key(sheet_id)
        print(f"✅ Sheet opened successfully: '{sheet.title}'")
        
        print("\n📋 4. Getting worksheet info...")
        # First worksheet
        worksheet = sheet.get_worksheet(0)
        print(f"✅ Worksheet: '{worksheet.title}'")
        print(f"📊 Size: {worksheet.row_count} rows x {worksheet.col_count} columns")
        
        print("\n📋 5. Testing sample data...")
        # Get first row (header)
        header_row = worksheet.row_values(1)
        print(f"✅ Header row: {len(header_row)} columns")
        print(f"📋 First 5 columns: {header_row[:5]}")
        
        print("\n📋 6. Checking Seri_Saat column...")
        if 'Seri_Saat' in header_row:
            seri_saat_col = header_row.index('Seri_Saat') + 1
            # Get sample values
            sample_values = worksheet.col_values(seri_saat_col)[1:4]  # Skip header, get 3 values
            
            print(f"✅ Seri_Saat column found (column {seri_saat_col})")
            print("📋 Sample values:")
            for i, time_val in enumerate(sample_values):
                print(f"   {i+1}. {time_val} (type: {type(time_val).__name__})")
        else:
            print("⚠️ Seri_Saat column not found")
        
        print(f"\n🎉 ALL TESTS PASSED!")
        print(f"🚀 You can run the main data extraction script")
        
        return True
        
    except FileNotFoundError:
        print("❌ credentials.json file not found")
        return False
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

if __name__ == "__main__":
    print("Python version:", sys.version)
    print("Current directory:", os.getcwd())
    print()
    
    success = test_google_sheets_connection()
    
    if success:
        print("\n✅ Test successful!")
    else:
        print("\n❌ Test failed.")