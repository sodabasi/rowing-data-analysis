# create_fact_table_v3.py
import pandas as pd
import numpy as np
from datetime import datetime
import re

def create_fact_race_results_v3():
    """FACT_RACE_RESULTS tablosu - Düzeltilmiş parsing"""
    
    print("🌟 FACT_RACE_RESULTS CREATOR V3")
    print("=" * 50)
    
    # Temiz veriyi oku
    filename = 'rowing_data_raw_20250724_110546_fixed_cleaned.csv'
    df = pd.read_csv(filename)
    
    print(f"📊 Temiz veri yüklendi: {df.shape}")
    
    # =============================================
    # VERİ YAPISI ANALİZİ
    # =============================================
    print(f"\n🔍 Veri yapısı analizi...")
    print(f"📋 Kolonlar: {list(df.columns)}")
    
    # İlk 10 satırı incele
    print(f"\n👀 İlk 10 satır (Derece ve Sonuc):")
    for i in range(min(10, len(df))):
        derece = df.iloc[i]['Derece']
        sonuc = df.iloc[i]['Sonuc']
        print(f"   {i+1}. Derece: '{derece}' | Sonuc: '{sonuc}'")
    
    # Unique değerler
    print(f"\n📊 Derece unique değerleri (ilk 20):")
    derece_unique = df['Derece'].dropna().unique()[:20]
    for val in derece_unique:
        print(f"   '{val}'")
    
    print(f"\n📊 Sonuc unique değerleri (ilk 20):")
    sonuc_unique = df['Sonuc'].dropna().unique()[:20]
    for val in sonuc_unique:
        print(f"   '{val}'")
    
    # =============================================
    # DOĞRU PARSING LOGIC
    # =============================================
    
    # SONUC = rank position (1, 2, 3, etc.)
    # DERECE = race time (1.41,97 = 1:41.97)
    
    def parse_rank_position(sonuc_value):
        """Sonuc kolonunu rank position olarak parse et"""
        if pd.isna(sonuc_value) or sonuc_value == '':
            return {
                'rank_position': None,
                'has_valid_rank': False,
                'rank_notes': 'No rank data'
            }
        
        try:
            rank_int = int(float(str(sonuc_value).strip()))
            if 1 <= rank_int <= 100:  # Reasonable rank range
                return {
                    'rank_position': rank_int,
                    'has_valid_rank': True,
                    'rank_notes': None
                }
            else:
                return {
                    'rank_position': None,
                    'has_valid_rank': False,
                    'rank_notes': f'Invalid rank: {sonuc_value}'
                }
        except (ValueError, TypeError):
            return {
                'rank_position': None,
                'has_valid_rank': False,
                'rank_notes': f'Unparseable rank: {sonuc_value}'
            }
    
    def parse_race_time_with_flags(derece_value):
        """Derece kolonunu race time + flags olarak parse et"""
        if pd.isna(derece_value) or derece_value == '':
            return {
                'race_time_seconds': None,
                'dnf_flag': False,
                'dns_flag': False,
                'dsq_flag': False,
                'has_valid_time': False,
                'time_notes': 'No time data'
            }
        
        derece_str = str(derece_value).strip().upper()
        
        # DNF check
        if any(pattern in derece_str for pattern in ['DNF', 'BİTİREMEDİ', 'BITIREMEDI']):
            return {
                'race_time_seconds': None,
                'dnf_flag': True,
                'dns_flag': False,
                'dsq_flag': False,
                'has_valid_time': False,
                'time_notes': 'Did Not Finish'
            }
        
        # DNS check  
        if any(pattern in derece_str for pattern in ['DNS', 'KATILMADI', 'BAŞLAMADI']):
            return {
                'race_time_seconds': None,
                'dnf_flag': False,
                'dns_flag': True,
                'dsq_flag': False,
                'has_valid_time': False,
                'time_notes': 'Did Not Start'
            }
        
        # DSQ check
        if any(pattern in derece_str for pattern in ['DSQ', 'DİSKALİFİYE', 'DISKALIFIYE', 'YARISHARICI']):
            return {
                'race_time_seconds': None,
                'dnf_flag': False,
                'dns_flag': False,
                'dsq_flag': True,
                'has_valid_time': False,
                'time_notes': 'Disqualified'
            }
        
        # Time parsing: 1.41,97 -> 1:41.97 -> 101.97 seconds
        try:
            # Replace comma with dot for decimal
            time_clean = derece_str.replace(',', '.')
            
            # Pattern 1: MM.SS.CS (1.41.97)
            pattern1 = r'^(\d+)\.(\d{2})\.(\d{2})$'
            match1 = re.match(pattern1, time_clean)
            if match1:
                minutes = int(match1.group(1))
                seconds = int(match1.group(2))
                centiseconds = int(match1.group(3))
                total_seconds = minutes * 60 + seconds + centiseconds / 100
                
                if 30.0 <= total_seconds <= 1800.0:  # Reasonable time range
                    return {
                        'race_time_seconds': total_seconds,
                        'dnf_flag': False,
                        'dns_flag': False,
                        'dsq_flag': False,
                        'has_valid_time': True,
                        'time_notes': None
                    }
            
            # Pattern 2: SS.CS (41.97) - seconds only
            pattern2 = r'^(\d{1,2})\.(\d{1,2})$'
            match2 = re.match(pattern2, time_clean)
            if match2:
                seconds = int(match2.group(1))
                centiseconds = int(match2.group(2))
                total_seconds = seconds + centiseconds / 100
                
                if 30.0 <= total_seconds <= 300.0:  # Sprint times
                    return {
                        'race_time_seconds': total_seconds,
                        'dnf_flag': False,
                        'dns_flag': False,
                        'dsq_flag': False,
                        'has_valid_time': True,
                        'time_notes': None
                    }
            
            # Pattern 3: Pure decimal (101.97)
            try:
                total_seconds = float(time_clean)
                if 30.0 <= total_seconds <= 1800.0:
                    return {
                        'race_time_seconds': total_seconds,
                        'dnf_flag': False,
                        'dns_flag': False,
                        'dsq_flag': False,
                        'has_valid_time': True,
                        'time_notes': None
                    }
            except:
                pass
            
            # If no pattern matches
            return {
                'race_time_seconds': None,
                'dnf_flag': False,
                'dns_flag': False,
                'dsq_flag': False,
                'has_valid_time': False,
                'time_notes': f'Unparseable time: {derece_value}'
            }
                       
        except Exception as e:
            return {
                'race_time_seconds': None,
                'dnf_flag': False,
                'dns_flag': False,
                'dsq_flag': False,
                'has_valid_time': False,
                'time_notes': f'Parse error: {derece_value}'
            }
    
    # Apply parsing
    print(f"\n🔧 Parsing ranks and times...")
    
    # Parse ranks (Sonuc column)
    parsed_ranks = df['Sonuc'].apply(parse_rank_position)
    df['rank_position'] = [p['rank_position'] for p in parsed_ranks]
    df['has_valid_rank'] = [p['has_valid_rank'] for p in parsed_ranks]
    df['rank_notes'] = [p['rank_notes'] for p in parsed_ranks]
    
    # Parse times (Derece column)
    parsed_times = df['Derece'].apply(parse_race_time_with_flags)
    df['race_time_seconds'] = [p['race_time_seconds'] for p in parsed_times]
    df['dnf_flag'] = [p['dnf_flag'] for p in parsed_times]
    df['dns_flag'] = [p['dns_flag'] for p in parsed_times]
    df['dsq_flag'] = [p['dsq_flag'] for p in parsed_times]
    df['has_valid_time'] = [p['has_valid_time'] for p in parsed_times]
    df['time_notes'] = [p['time_notes'] for p in parsed_times]
    
    # Parsing sonuçları
    print(f"✅ Parsing tamamlandı:")
    print(f"   Valid ranks: {df['has_valid_rank'].sum()} ({df['has_valid_rank'].mean()*100:.1f}%)")
    print(f"   Valid times: {df['has_valid_time'].sum()} ({df['has_valid_time'].mean()*100:.1f}%)")
    print(f"   DNF: {df['dnf_flag'].sum()}")
    print(f"   DNS: {df['dns_flag'].sum()}")
    print(f"   DSQ: {df['dsq_flag'].sum()}")
    
    # Sample parsed data
    print(f"\n👀 Parsed örnekleri:")
    sample_indices = [0, 1, 2, 3, 4]
    for i in sample_indices:
        if i < len(df):
            row = df.iloc[i]
            print(f"   {i+1}. Rank: {row['rank_position']} | Time: {row['race_time_seconds']:.2f}s | DNF: {row['dnf_flag']}")
    
    # =============================================
    # DIM_ATHLETE (Fixed merging)
    # =============================================
    print(f"\n👤 Creating DIM_ATHLETE...")
    
    # Only use valid ranks for statistics
    valid_rank_df = df[df['has_valid_rank']].copy()
    
    if len(valid_rank_df) > 0:
        athlete_base_stats = valid_rank_df.groupby('Sporcu').agg({
            'rank_position': ['count', 'mean', 'min'],
            'race_time_seconds': 'count'
        }).reset_index()
        
        athlete_base_stats.columns = ['athlete_name', 'total_races_with_rank', 'avg_rank', 'best_rank', 'races_with_time']
    else:
        # If no valid ranks, create empty structure
        athlete_base_stats = pd.DataFrame(columns=['athlete_name', 'total_races_with_rank', 'avg_rank', 'best_rank', 'races_with_time'])
    
    # Total races for all athletes
    total_races = df.groupby('Sporcu').size().reset_index(name='total_races')
    total_races.columns = ['athlete_name', 'total_races']
    
    # Start with total races as base
    athlete_stats = total_races.copy()
    
    # Merge base stats
    athlete_stats = athlete_stats.merge(athlete_base_stats, on='athlete_name', how='left')
    
    # Wins and podiums
    if len(valid_rank_df) > 0:
        wins = valid_rank_df[valid_rank_df['rank_position'] == 1].groupby('Sporcu').size().reset_index()
        wins.columns = ['athlete_name', 'total_wins']
        
        podiums = valid_rank_df[valid_rank_df['rank_position'].isin([1, 2, 3])].groupby('Sporcu').size().reset_index()
        podiums.columns = ['athlete_name', 'total_podiums']
    else:
        wins = pd.DataFrame(columns=['athlete_name', 'total_wins'])
        podiums = pd.DataFrame(columns=['athlete_name', 'total_podiums'])
    
    athlete_stats = athlete_stats.merge(wins, on='athlete_name', how='left')
    athlete_stats = athlete_stats.merge(podiums, on='athlete_name', how='left')
    
    # DNF/DNS/DSQ counts
    dnf_counts = df[df['dnf_flag']].groupby('Sporcu').size().reset_index()
    dnf_counts.columns = ['athlete_name', 'total_dnf']
    
    dns_counts = df[df['dns_flag']].groupby('Sporcu').size().reset_index()
    dns_counts.columns = ['athlete_name', 'total_dns']
    
    dsq_counts = df[df['dsq_flag']].groupby('Sporcu').size().reset_index()
    dsq_counts.columns = ['athlete_name', 'total_dsq']
    
    athlete_stats = athlete_stats.merge(dnf_counts, on='athlete_name', how='left')
    athlete_stats = athlete_stats.merge(dns_counts, on='athlete_name', how='left')  
    athlete_stats = athlete_stats.merge(dsq_counts, on='athlete_name', how='left')
    
    # Fill NaN values
    fill_columns = ['total_races_with_rank', 'races_with_time', 'total_wins', 'total_podiums', 'total_dnf', 'total_dns', 'total_dsq']
    for col in fill_columns:
        if col in athlete_stats.columns:
            athlete_stats[col] = athlete_stats[col].fillna(0).astype(int)
    
    # Keys
    athlete_stats['athlete_key'] = range(1, len(athlete_stats) + 1)
    athlete_stats['athlete_id'] = athlete_stats['athlete_name'].str.replace(' ', '_').str.upper()
    
    # Final columns
    final_athlete_cols = ['athlete_key', 'athlete_id', 'athlete_name', 'total_races', 'total_races_with_rank', 
                         'total_wins', 'total_podiums', 'best_rank', 'avg_rank', 'total_dnf', 'total_dns', 'total_dsq']
    
    # Only select columns that exist
    existing_cols = [col for col in final_athlete_cols if col in athlete_stats.columns]
    dim_athlete = athlete_stats[existing_cols].copy()
    
    print(f"✅ DIM_ATHLETE: {len(dim_athlete)} athletes")
    
    # =============================================
    # BASIT FACT TABLE
    # =============================================
    print(f"\n🌟 Creating FACT_RACE_RESULTS...")
    
    # Athlete mapping
    athlete_mapping = dim_athlete.set_index('athlete_name')['athlete_key'].to_dict()
    
    # Fact table preparation
    fact_df = df.copy()
    fact_df['athlete_key'] = fact_df['Sporcu'].map(athlete_mapping)
    fact_df['result_id'] = range(1, len(fact_df) + 1)
    
    # Other measures
    fact_df['lane_number'] = pd.to_numeric(fact_df['Parkur_No'], errors='coerce')
    fact_df['series_number'] = pd.to_numeric(fact_df['Seri_No'], errors='coerce')
    fact_df['warning_points'] = pd.to_numeric(fact_df['ihtar'], errors='coerce').fillna(0)
    fact_df['series_time'] = fact_df['Seri_Saat']
    
    # Final fact table columns
    fact_columns = ['result_id', 'athlete_key', 'rank_position', 'race_time_seconds',
                   'lane_number', 'series_number', 'series_time', 'warning_points',
                   'has_valid_rank', 'has_valid_time', 'dnf_flag', 'dns_flag', 'dsq_flag',
                   'rank_notes', 'time_notes']
    
    # Only select columns that exist
    existing_fact_cols = [col for col in fact_columns if col in fact_df.columns]
    fact_race_results = fact_df[existing_fact_cols].copy()
    
    print(f"✅ FACT_RACE_RESULTS: {len(fact_race_results)} records")
    
    # Quality report
    print(f"\n📊 FINAL QUALITY REPORT:")
    print(f"   📊 Total records: {len(fact_race_results):,}")
    print(f"   ✅ Valid ranks: {fact_race_results['has_valid_rank'].sum():,} ({fact_race_results['has_valid_rank'].mean()*100:.1f}%)")
    print(f"   ⏱️ Valid times: {fact_race_results['has_valid_time'].sum():,} ({fact_race_results['has_valid_time'].mean()*100:.1f}%)")
    print(f"   🚩 DNF: {fact_race_results['dnf_flag'].sum()}")
    print(f"   🚩 DNS: {fact_race_results['dns_flag'].sum()}")
    print(f"   🚩 DSQ: {fact_race_results['dsq_flag'].sum()}")
    
    # Save files
    print(f"\n💾 Saving final tables...")
    
    dim_athlete.to_csv('dim_athlete_final.csv', index=False, encoding='utf-8')
    fact_race_results.to_csv('fact_race_results_final.csv', index=False, encoding='utf-8')
    
    print(f"\n🎉 FINAL STAR SCHEMA CREATED!")
    print(f"📁 Final files:")
    print(f"   • dim_athlete_final.csv ({len(dim_athlete)} athletes)")
    print(f"   • fact_race_results_final.csv ({len(fact_race_results)} results)")
    
    return {
        'dim_athlete': dim_athlete,
        'fact_race_results': fact_race_results
    }

if __name__ == "__main__":
    tables = create_fact_race_results_v3()
    print(f"\n🚀 Ready for PostgreSQL!")