# quick_check.py
import pandas as pd

# Düzeltilmiş dosyayı oku
df = pd.read_csv('rowing_data_raw_20250724_110546_fixed.csv')

print("📊 DÜZELTME SONRASI VERİ KONTROLÜ")
print("=" * 50)
print(f"📏 Boyut: {df.shape}")

# Seri_Saat kontrolü
if 'Seri_Saat' in df.columns:
    print(f"\n⏰ Düzeltilmiş Seri_Saat örnekleri:")
    sample_times = df['Seri_Saat'].dropna().head(10).tolist()
    for i, time_val in enumerate(sample_times):
        print(f"   {i+1}. {time_val}")
    
    print(f"\n🕐 Saat formatı dağılımı:")
    print(f"   Toplam dolu: {df['Seri_Saat'].notna().sum()}")
    print(f"   HH:MM formatı: {df['Seri_Saat'].astype(str).str.match(r'\d{2}:\d{2}').sum()}")

print(f"\n🎯 İlk 5 satır:")
print(df[['Sporcu', 'Takim', 'Tarih', 'Seri_Saat', 'Derece']].head(5))