# extract_rowing_data.py
import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime
import os

def extract_rowing_data():
    """Extract Turkish Rowing Federation data"""
    
    print("🌟 ROWING DATA EXTRACTOR")
    print("=" * 50)
    
    # Configuration
    credentials_file = "credentials.json"
    sheet_id = "1cWCO0gKBv1kIrnWg_katFLoIqAcuLj869qXiJIdkzLE"
    
    try:
        print("📋 1. Connecting to Google Sheets...")
        
        # Credentials and client
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds = Credentials.from_service_account_file(credentials_file, scopes=scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(sheet_id)
        worksheet = sheet.get_worksheet(0)
        
        print(f"✅ Connection successful: {sheet.title}")
        
        print("\n📋 2. Extracting raw data...")
        
        # Get data with UNFORMATTED_VALUE to avoid format loss
        all_values = worksheet.get_all_values(
            value_render_option='UNFORMATTED_VALUE'
        )
        
        headers = all_values[0]
        data_rows = all_values[1:]
        
        print(f"✅ {len(data_rows)} rows of raw data extracted")
        print(f"📋 Columns ({len(headers)}): {', '.join(headers)}")
        
        print("\n📋 3. Creating DataFrame...")
        
        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=headers)
        
        # Replace empty strings with NaN
        df = df.replace('', None)
        
        print(f"✅ DataFrame created: {df.shape}")
        
        print("\n📋 4. Checking data quality...")
        
        # Basic data quality
        total_cells = df.shape[0] * df.shape[1]
        null_cells = df.isnull().sum().sum()
        
        print(f"📊 Total cells: {total_cells:,}")
        print(f"📊 Empty cells: {null_cells:,} ({null_cells/total_cells*100:.1f}%)")
        
        # Check critical columns
        critical_columns = ['Sporcu', 'Takim', 'Tarih', 'Seri_Saat']
        print(f"\n📋 Critical column analysis:")
        
        for col in critical_columns:
            if col in df.columns:
                non_null = df[col].notna().sum()
                percentage = non_null / len(df) * 100
                print(f"   {col}: {non_null}/{len(df)} ({percentage:.1f}% filled)")
            else:
                print(f"   ⚠️ {col}: Column not found!")
        
        print("\n📋 5. Analyzing Seri_Saat column...")
        
        if 'Seri_Saat' in df.columns:
            seri_saat_sample = df['Seri_Saat'].dropna().head(5)
            print("📋 Seri_Saat sample values:")
            for i, val in enumerate(seri_saat_sample):
                print(f"   {i+1}. '{val}' (type: {type(val).__name__})")
            
            # Check for Excel date format
            excel_date_check = df['Seri_Saat'].astype(str).str.contains('1899|1900', na=False).sum()
            if excel_date_check > 0:
                print(f"⚠️ {excel_date_check} Excel date formats detected")
            else:
                print("✅ No Excel date format, data is clean!")
        
        print("\n📋 6. Saving data...")
        
        # Filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"rowing_data_raw_{timestamp}.csv"
        
        # Save with UTF-8 encoding
        df.to_csv(filename, index=False, encoding='utf-8')
        
        file_size = os.path.getsize(filename)
        print(f"✅ Data saved: {filename}")
        print(f"📁 File size: {file_size:,} bytes ({file_size/1024:.1f} KB)")
        
        print("\n📋 7. Summary report...")
        print(f"🎯 Process Summary:")
        print(f"   📊 Extracted rows: {len(df):,}")
        print(f"   📋 Column count: {len(df.columns)}")
        print(f"   📁 Output file: {filename}")
        print(f"   🕐 Process time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Show first 3 rows
        print(f"\n👀 First 3 rows preview:")
        preview_columns = ['Sporcu', 'Takim', 'Tarih', 'Seri_Saat', 'Derece']
        available_columns = [col for col in preview_columns if col in df.columns]
        
        if available_columns:
            preview_df = df[available_columns].head(3)
            for i, row in preview_df.iterrows():
                print(f"\n   Row {i+1}:")
                for col in available_columns:
                    print(f"     {col}: {row[col]}")
        
        print(f"\n🎉 DATA EXTRACTION COMPLETED!")
        print(f"🚀 File ready: {filename}")
        
        return df, filename
        
    except Exception as e:
        print(f"❌ Error occurred: {str(e)}")
        print(f"🔍 Error type: {type(e).__name__}")
        return None, None

if __name__ == "__main__":
    df, filename = extract_rowing_data()
    
    if df is not None:
        print(f"\n✅ Process successful!")
        print(f"📊 Data size: {df.shape}")
        print(f"📁 File: {filename}")
    else:
        print(f"\n❌ Process failed!")