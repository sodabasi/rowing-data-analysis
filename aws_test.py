import boto3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

print("🔍 AWS Data Analyst - Tam Sistem Testi")
print("=" * 50)

# 1. AWS S3 Bağlantı Testi
try:
    s3 = boto3.client('s3')
    buckets = s3.list_buckets()
    print(f"✅ AWS S3 bağlantısı başarılı!")
    print(f"📁 Erişilebilir bucket sayısı: {len(buckets['Buckets'])}")
    
    # Bucket listesi
    for bucket in buckets['Buckets']:
        print(f"  📁 {bucket['Name']}")
        
except Exception as e:
    print(f"❌ AWS bağlantı hatası: {e}")

# 2. Data Science Paketleri Testi
print("\n📊 Data Science Paketleri:")
try:
    df = pd.DataFrame({
        'tarih': pd.date_range('2024-01-01', periods=100),
        'deger': np.random.randn(100).cumsum()
    })
    print(f"✅ Pandas: DataFrame oluşturuldu ({df.shape})")
    print(f"✅ NumPy: Random data üretildi")
    
    # İstatistikler
    print(f"📈 Ortalama: {df['deger'].mean():.2f}")
    print(f"📊 Standart sapma: {df['deger'].std():.2f}")
    
except Exception as e:
    print(f"❌ Data Science hata: {e}")

# 3. AWS Services Testi
print("\n☁️ AWS Services Testi:")
try:
    # AWS kimlik bilgileri
    sts = boto3.client('sts')
    identity = sts.get_caller_identity()
    print(f"✅ AWS Account ID: {identity['Account']}")
    print(f"✅ AWS ARN: {identity['Arn']}")
    
except Exception as e:
    print(f"❌ AWS kimlik hatası: {e}")

print("\n🎯 SİSTEM DURUMU: TAMAMEN HAZIR! 🚀")
print("🔹 Jupyter Lab başlatmak için: jupyter lab")
print("🔹 Streamlit app için: streamlit run app.py")
print("🔹 Git operations için: git status")