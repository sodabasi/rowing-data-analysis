# clean_data.py
import pandas as pd

def clean_rowing_data():
    """Rowing data'yı temizle - header'ları ve hatalı kayıtları kaldır"""
    
    print("🧹 ROWING DATA CLEANER")
    print("=" * 40)
    
    # Dosyayı oku
    filename = 'rowing_data_raw_20250724_110546_fixed.csv'
    df = pd.read_csv(filename)
    
    print(f"📊 Orijinal veri: {df.shape}")
    print(f"📋 Kolonlar: {list(df.columns)}")
    
    # İlk 10 satıra bakalım
    print(f"\n👀 İlk 10 satır:")
    for i in range(min(10, len(df))):
        sporcu = df.iloc[i]['Sporcu']
        takim = df.iloc[i]['Takim']
        print(f"   {i+1}. Sporcu: '{sporcu}', Takim: '{takim}'")
    
    print(f"\n🔍 Problem kayıtları tespit ediliyor...")
    
    # Header kayıtlarını bul
    header_conditions = [
        df['Sporcu'] == 'Sporcu',
        df['Takim'] == 'Takim', 
        df['Tarih'] == 'Tarih',
        df['Yaris_Adi'] == 'Yaris_Adi',
        df['Sporcu'].str.contains('Sporcu', na=False),
        df['Takim'].str.contains('Takim', na=False)
    ]
    
    # Tüm header condition'larını birleştir
    is_header_row = pd.Series([False] * len(df))
    for condition in header_conditions:
        is_header_row = is_header_row | condition
    
    header_count = is_header_row.sum()
    print(f"⚠️ {header_count} adet header satırı bulundu")
    
    # Header satırlarını göster
    if header_count > 0:
        print(f"\n📋 Header satırları:")
        header_rows = df[is_header_row]
        for i, (idx, row) in enumerate(header_rows.iterrows()):
            print(f"   {i+1}. Satır {idx}: Sporcu='{row['Sporcu']}', Takim='{row['Takim']}'")
    
    # Boş kayıtları bul
    empty_conditions = [
        df['Sporcu'].isna(),
        df['Takim'].isna(),
        df['Sporcu'] == '',
        df['Takim'] == '',
        df['Sporcu'].str.strip() == '',
        df['Takim'].str.strip() == ''
    ]
    
    is_empty_row = pd.Series([False] * len(df))
    for condition in empty_conditions:
        is_empty_row = is_empty_row | condition
    
    empty_count = is_empty_row.sum()
    print(f"⚠️ {empty_count} adet boş/geçersiz satır bulundu")
    
    # Temizleme işlemi
    print(f"\n🧹 Temizleme işlemi başlıyor...")
    
    # Header ve boş satırları kaldır
    to_remove = is_header_row | is_empty_row
    df_clean = df[~to_remove].copy()
    
    # String kolonları trim et
    string_columns = ['Sporcu', 'Takim', 'Tarih', 'Yaris_Adi', 'Yaris_Adi2', 'Seri_Adi', 'Yaris_Yeri']
    for col in string_columns:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()
    
    # Çok kısa sporcu/takım isimlerini kaldır
    df_clean = df_clean[
        (df_clean['Sporcu'].str.len() > 2) & 
        (df_clean['Takim'].str.len() > 1)
    ]
    
    print(f"✅ Temizleme tamamlandı")
    print(f"📊 Temiz veri: {df_clean.shape}")
    print(f"🗑️ Kaldırılan satır: {len(df) - len(df_clean)}")
    
    # Temiz veri özeti
    print(f"\n📈 Temiz Veri Özeti:")
    print(f"   📊 Toplam satır: {len(df_clean):,}")
    print(f"   👤 Benzersiz sporcu: {df_clean['Sporcu'].nunique():,}")
    print(f"   🏅 Benzersiz takım: {df_clean['Takim'].nunique():,}")
    print(f"   🏆 Benzersiz yarış: {df_clean['Yaris_Adi'].nunique():,}")
    print(f"   🎯 Benzersiz kategori: {df_clean['Seri_Adi'].nunique():,}")
    
    # Sample data göster
    print(f"\n👀 Temiz veri örnekleri:")
    sample_df = df_clean[['Sporcu', 'Takim', 'Tarih', 'Seri_Saat']].head(5)
    for i, (idx, row) in enumerate(sample_df.iterrows()):
        print(f"   {i+1}. {row['Sporcu']} | {row['Takim']} | {row['Tarih']} | {row['Seri_Saat']}")
    
    # Temiz dosyayı kaydet
    clean_filename = filename.replace('.csv', '_cleaned.csv')
    df_clean.to_csv(clean_filename, index=False, encoding='utf-8')
    
    print(f"\n💾 Temiz veri kaydedildi: {clean_filename}")
    
    return df_clean, clean_filename

if __name__ == "__main__":
    df_clean, filename = clean_rowing_data()
    
    if df_clean is not None:
        print(f"\n🎉 Veri temizleme başarılı!")
        print(f"📁 Temiz dosya: {filename}")
        print(f"🚀 Artık FACT table oluşturabilirsiniz!")