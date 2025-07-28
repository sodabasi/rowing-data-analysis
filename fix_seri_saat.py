# fix_seri_saat.py
import pandas as pd
from datetime import datetime, timedelta

def decimal_to_time(decimal_value):
    """Decimal değeri HH:MM formatına çevir"""
    if pd.isna(decimal_value) or decimal_value == '':
        return None
    
    try:
        # Decimal değeri float'a çevir
        decimal_float = float(decimal_value)
        
        # 0.375 = 9:00 AM, 0.5 = 12:00 PM gibi
        # Decimal * 24 = saat
        total_hours = decimal_float * 24
        
        # Saat ve dakikayı ayır
        hours = int(total_hours)
        minutes = int((total_hours - hours) * 60)
        
        # HH:MM formatında dön
        return f"{hours:02d}:{minutes:02d}"
        
    except (ValueError, TypeError):
        return str(decimal_value)  # Dönüştürülemezse olduğu gibi dön

def fix_rowing_data():
    """Seri_Saat kolonunu düzelt"""
    
    print("🔧 SERİ_SAAT FORMAT DÜZELTİCİ")
    print("=" * 50)
    
    # Dosyayı oku
    filename = 'rowing_data_raw_20250724_110546.csv'
    df = pd.read_csv(filename)
    
    print(f"📊 Orijinal veri: {df.shape}")
    
    if 'Seri_Saat' in df.columns:
        print(f"\n🔍 Mevcut Seri_Saat örnekleri:")
        sample_original = df['Seri_Saat'].dropna().head(5).tolist()
        for i, val in enumerate(sample_original):
            print(f"   {i+1}. {val}")
        
        print(f"\n🔧 Decimal → HH:MM dönüşümü yapılıyor...")
        
        # Seri_Saat kolonunu düzelt
        df['Seri_Saat'] = df['Seri_Saat'].apply(decimal_to_time)
        
        print(f"\n✅ Düzeltilmiş Seri_Saat örnekleri:")
        sample_fixed = df['Seri_Saat'].dropna().head(5).tolist()
        for i, val in enumerate(sample_fixed):
            print(f"   {i+1}. {val}")
        
        # Düzeltilmiş dosyayı kaydet
        new_filename = filename.replace('.csv', '_fixed.csv')
        df.to_csv(new_filename, index=False, encoding='utf-8')
        
        print(f"\n💾 Düzeltilmiş veri kaydedildi: {new_filename}")
        
        # Kontrol
        print(f"\n📊 Saat formatı kontrolü:")
        time_pattern_count = df['Seri_Saat'].astype(str).str.match(r'\d{2}:\d{2}').sum()
        total_non_null = df['Seri_Saat'].notna().sum()
        print(f"   HH:MM formatında: {time_pattern_count}/{total_non_null}")
        
        return new_filename
    else:
        print("❌ Seri_Saat kolonu bulunamadı!")
        return None

if __name__ == "__main__":
    fixed_file = fix_rowing_data()
    
    if fixed_file:
        print(f"\n🎉 İşlem tamamlandı!")
        print(f"📁 Yeni dosya: {fixed_file}")
        print(f"🚀 Bu dosyayla FACT table oluşturabilirsiniz!")