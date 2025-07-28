# create_fact_table_v2.py
import pandas as pd
import numpy as np
from datetime import datetime
import re

def create_fact_race_results_v2():
    """FACT_RACE_RESULTS tablosu - DNF/DNS/DSQ flag'ları ile"""
    
    print("🌟 FACT_RACE_RESULTS CREATOR V2")
    print("=" * 50)
    
    # Temiz veriyi oku
    filename = 'rowing_data_raw_20250724_110546_fixed_cleaned.csv'
    df = pd.read_csv(filename)
    
    print(f"📊 Temiz veri yüklendi: {df.shape}")
    
    # =============================================
    # ÖZEL DURUM ANALİZİ
    # =============================================
    print(f"\n🚩 Özel Durum Analizi...")
    
    def analyze_special_cases(series):
        """Derece kolonundaki özel durumları analiz et"""
        special_cases = {}
        
        for value in series.dropna().unique():
            value_str = str(value).strip().upper()
            
            # DNF patterns
            if any(pattern in value_str for pattern in ['DNF', 'BİTİREMEDİ', 'BITIREMEDI', 'FINISH']):
                special_cases[value] = 'DNF'
            # DNS patterns  
            elif any(pattern in value_str for pattern in ['DNS', 'KATILMADI', 'START', 'BAŞLAMADI']):
                special_cases[value] = 'DNS'
            # DSQ patterns
            elif any(pattern in value_str for pattern in ['DSQ', 'DİSKALİFİYE', 'DISKALIFIYE', 'YARISHARICI', 'YARIS HARICI']):
                special_cases[value] = 'DSQ'
            # Numeric check
            else:
                try:
                    float_val = float(str(value).replace(',', '.'))
                    if float_val > 0:
                        special_cases[value] = 'VALID_RANK'
                except:
                    special_cases[value] = 'OTHER'
        
        return special_cases
    
    # Derece kolonu analizi
    derece_cases = analyze_special_cases(df['Derece'])
    
    print(f"📋 Derece kolonu özel durumları:")
    case_counts = {}
    for value, case_type in derece_cases.items():
        if case_type not in case_counts:
            case_counts[case_type] = 0
        case_counts[case_type] += (df['Derece'] == value).sum()
    
    for case_type, count in case_counts.items():
        print(f"   {case_type}: {count} adet")
        if case_type != 'VALID_RANK':
            examples = [k for k, v in derece_cases.items() if v == case_type][:3]
            print(f"     Örnekler: {examples}")
    
    # =============================================
    # DERECE PARSING VE FLAGS
    # =============================================
    def parse_rank_with_flags(derece_value):
        """Derece değerini parse et ve flag'ları belirle"""
        if pd.isna(derece_value) or derece_value == '':
            return {
                'rank_position': None,
                'dnf_flag': False,
                'dns_flag': False, 
                'dsq_flag': False,
                'has_valid_rank': False,
                'rank_notes': None
            }
        
        derece_str = str(derece_value).strip().upper()
        
        # DNF check  
        if any(pattern in derece_str for pattern in ['DNF', 'BİTİREMEDİ', 'BITIREMEDI']):
            return {
                'rank_position': None,
                'dnf_flag': True,
                'dns_flag': False,
                'dsq_flag': False,
                'has_valid_rank': False,
                'rank_notes': 'Did Not Finish'
            }
        
        # DNS check
        if any(pattern in derece_str for pattern in ['DNS', 'KATILMADI', 'BAŞLAMADI']):
            return {
                'rank_position': None,
                'dnf_flag': False,
                'dns_flag': True,
                'dsq_flag': False,
                'has_valid_rank': False,
                'rank_notes': 'Did Not Start'
            }
        
        # DSQ check  
        if any(pattern in derece_str for pattern in ['DSQ', 'DİSKALİFİYE', 'DISKALIFIYE', 'YARISHARICI']):
            return {
                'rank_position': None,
                'dnf_flag': False,
                'dns_flag': False,
                'dsq_flag': True,
                'has_valid_rank': False,
                'rank_notes': 'Disqualified'
            }
        
        # Numeric rank check
        try:
            # Comma to dot conversion for Turkish decimal format
            numeric_str = derece_str.replace(',', '.')
            rank_value = float(numeric_str)
            
            if rank_value > 0 and rank_value == int(rank_value):  # Valid integer rank
                return {
                    'rank_position': int(rank_value),
                    'dnf_flag': False,
                    'dns_flag': False,
                    'dsq_flag': False,
                    'has_valid_rank': True,
                    'rank_notes': None
                }
            else:
                return {
                    'rank_position': None,
                    'dnf_flag': False,
                    'dns_flag': False,
                    'dsq_flag': False,
                    'has_valid_rank': False,
                    'rank_notes': f'Invalid rank: {derece_value}'
                }
                
        except (ValueError, TypeError):
            return {
                'rank_position': None,
                'dnf_flag': False,
                'dns_flag': False,
                'dsq_flag': False,
                'has_valid_rank': False,
                'rank_notes': f'Unparseable: {derece_value}'
            }
    
    # Apply parsing
    print(f"\n🔧 Derece parsing...")
    parsed_ranks = df['Derece'].apply(parse_rank_with_flags)
    
    # Extract parsed data
    df['rank_position'] = [p['rank_position'] for p in parsed_ranks]
    df['dnf_flag'] = [p['dnf_flag'] for p in parsed_ranks]
    df['dns_flag'] = [p['dns_flag'] for p in parsed_ranks]  
    df['dsq_flag'] = [p['dsq_flag'] for p in parsed_ranks]
    df['has_valid_rank'] = [p['has_valid_rank'] for p in parsed_ranks]
    df['rank_notes'] = [p['rank_notes'] for p in parsed_ranks]
    
    # Parsing sonuçları
    print(f"✅ Parsing tamamlandı:")
    print(f"   Valid rank: {df['has_valid_rank'].sum()} ({df['has_valid_rank'].mean()*100:.1f}%)")
    print(f"   DNF: {df['dnf_flag'].sum()}")
    print(f"   DNS: {df['dns_flag'].sum()}")  
    print(f"   DSQ: {df['dsq_flag'].sum()}")
    print(f"   Diğer: {(~df['has_valid_rank'] & ~df['dnf_flag'] & ~df['dns_flag'] & ~df['dsq_flag']).sum()}")
    
    # =============================================
    # SONUC (RACE TIME) PARSING
    # =============================================
    def parse_race_time(sonuc_value):
        """Sonuc değerini race time olarak parse et"""
        if pd.isna(sonuc_value) or sonuc_value == '':
            return None
        
        try:
            # String'e çevir ve temizle
            time_str = str(sonuc_value).strip().replace(',', '.')
            
            # Sadece sayısal değer kontrolü
            time_float = float(time_str)
            
            # Sanity check - yarış süreleri genelde 30 saniye - 30 dakika arası
            if 30.0 <= time_float <= 1800.0:  # 30 saniye - 30 dakika
                return time_float
            else:
                return None
                
        except (ValueError, TypeError):
            return None
    
    print(f"\n⏱️ Race time parsing...")
    df['race_time_seconds'] = df['Sonuc'].apply(parse_race_time)
    df['has_valid_time'] = df['race_time_seconds'].notna()
    
    valid_times = df['has_valid_time'].sum()
    print(f"✅ Valid race times: {valid_times} ({valid_times/len(df)*100:.1f}%)")
    
    # =============================================
    # DIM_ATHLETE (Fixed)
    # =============================================
    print(f"\n👤 1. Creating DIM_ATHLETE...")
    
    # Only use valid ranks for statistics
    valid_rank_df = df[df['has_valid_rank']].copy()
    
    athlete_stats = valid_rank_df.groupby('Sporcu').agg({
        'rank_position': ['count', 'mean', 'min'],
        'race_time_seconds': 'count'
    }).reset_index()
    
    athlete_stats.columns = ['athlete_name', 'total_races_with_rank', 'avg_rank', 'best_rank', 'races_with_time']
    
    # Total races (including DNF/DNS/DSQ)
    total_races = df.groupby('Sporcu').size().reset_index(name='total_races')
    athlete_stats = athlete_stats.merge(total_races, left_on='athlete_name', right_on='Sporcu', how='right')
    
    # Wins (1st place)
    wins = valid_rank_df[valid_rank_df['rank_position'] == 1].groupby('Sporcu').size().reset_index(name='total_wins')
    athlete_stats = athlete_stats.merge(wins, left_on='athlete_name', right_on='Sporcu', how='left')
    
    # Podiums (1st, 2nd, 3rd)
    podiums = valid_rank_df[valid_rank_df['rank_position'].isin([1, 2, 3])].groupby('Sporcu').size().reset_index(name='total_podiums')
    athlete_stats = athlete_stats.merge(podiums, left_on='athlete_name', right_on='Sporcu', how='left')
    
    # DNF/DNS/DSQ counts
    dnf_counts = df[df['dnf_flag']].groupby('Sporcu').size().reset_index(name='total_dnf')
    dns_counts = df[df['dns_flag']].groupby('Sporcu').size().reset_index(name='total_dns')
    dsq_counts = df[df['dsq_flag']].groupby('Sporcu').size().reset_index(name='total_dsq')
    
    athlete_stats = athlete_stats.merge(dnf_counts, left_on='athlete_name', right_on='Sporcu', how='left')
    athlete_stats = athlete_stats.merge(dns_counts, left_on='athlete_name', right_on='Sporcu', how='left')
    athlete_stats = athlete_stats.merge(dsq_counts, left_on='athlete_name', right_on='Sporcu', how='left')
    
    # Fill NaN values
    fill_columns = ['total_races_with_rank', 'races_with_time', 'total_wins', 'total_podiums', 'total_dnf', 'total_dns', 'total_dsq']
    for col in fill_columns:
        athlete_stats[col] = athlete_stats[col].fillna(0).astype(int)
    
    # Keys and cleanup
    athlete_stats['athlete_key'] = range(1, len(athlete_stats) + 1)
    athlete_stats['athlete_id'] = athlete_stats['athlete_name'].str.replace(' ', '_').str.upper()
    
    # Final columns
    dim_athlete = athlete_stats[['athlete_key', 'athlete_id', 'athlete_name', 'total_races', 'total_races_with_rank', 
                                'total_wins', 'total_podiums', 'best_rank', 'avg_rank', 'total_dnf', 'total_dns', 'total_dsq']].copy()
    
    print(f"✅ DIM_ATHLETE: {len(dim_athlete)} athletes")
    
    # =============================================
    # DİĞER DIMENSION'LAR (Aynı kalsın)
    # =============================================
    print(f"\n🏅 2. Creating DIM_TEAM...")
    
    team_stats = df.groupby('Takim').agg({
        'Sporcu': 'nunique',
        'rank_position': 'count'
    }).reset_index()
    
    team_stats.columns = ['team_name', 'total_athletes', 'total_races']
    
    # Championships (only valid ranks)
    championships = valid_rank_df[valid_rank_df['rank_position'] == 1].groupby('Takim').size().reset_index(name='championship_count')
    team_stats = team_stats.merge(championships, left_on='team_name', right_on='Takim', how='left')
    team_stats['championship_count'] = team_stats['championship_count'].fillna(0).astype(int)
    
    # Team type classification
    def classify_team_type(team_name):
        team_upper = team_name.upper()
        if any(club in team_upper for club in ['FENERBAHÇE', 'GALATASARAY', 'BEŞIKTAŞ']):
            return 'Büyük Kulüp'
        elif 'KÜREK' in team_upper:
            return 'Kürek Kulübü'
        elif 'SPOR' in team_upper or 'SK' in team_upper:
            return 'Spor Kulübü'
        else:
            return 'Diğer'
    
    team_stats['team_type'] = team_stats['team_name'].apply(classify_team_type)
    team_stats['team_key'] = range(1, len(team_stats) + 1)
    team_stats['team_id'] = team_stats['team_name'].str.replace(' ', '_').str.upper()
    
    dim_team = team_stats[['team_key', 'team_id', 'team_name', 'team_type', 'total_athletes', 'total_races', 'championship_count']].copy()
    
    print(f"✅ DIM_TEAM: {len(dim_team)} teams")
    
    # DIM_RACE, DIM_CATEGORY, DIM_LOCATION, DIM_DATE - aynı kalsın
    # (Kısaltma için burayı atlıyorum, tam versiyonda hepsi olacak)
    
    # =============================================
    # FACT_RACE_RESULTS (Updated)  
    # =============================================
    print(f"\n🌟 Creating FACT_RACE_RESULTS with flags...")
    
    # Basit version - sadece temel mappings
    athlete_mapping = dim_athlete.set_index('athlete_name')['athlete_key'].to_dict()
    team_mapping = dim_team.set_index('team_name')['team_key'].to_dict()
    
    # Fact table preparation
    fact_df = df.copy()
    fact_df['athlete_key'] = fact_df['Sporcu'].map(athlete_mapping)
    fact_df['team_key'] = fact_df['Takim'].map(team_mapping)
    
    # Other measures
    fact_df['lane_number'] = pd.to_numeric(fact_df['Parkur_No'], errors='coerce')
    fact_df['series_number'] = pd.to_numeric(fact_df['Seri_No'], errors='coerce')
    fact_df['warning_points'] = pd.to_numeric(fact_df['ihtar'], errors='coerce').fillna(0)
    fact_df['series_time'] = fact_df['Seri_Saat']
    fact_df['result_id'] = range(1, len(fact_df) + 1)
    
    # Final fact table with flags
    fact_columns = ['result_id', 'athlete_key', 'team_key', 'rank_position', 'race_time_seconds',
                   'lane_number', 'series_number', 'series_time', 'warning_points',
                   'has_valid_rank', 'has_valid_time', 'dnf_flag', 'dns_flag', 'dsq_flag', 'rank_notes']
    
    fact_race_results = fact_df[fact_columns].copy()
    
    print(f"✅ FACT_RACE_RESULTS: {len(fact_race_results)} records")
    
    # Quality report
    print(f"\n📊 FACT TABLE QUALITY REPORT:")
    print(f"   📊 Total records: {len(fact_race_results):,}")
    print(f"   ✅ Valid ranks: {fact_race_results['has_valid_rank'].sum():,} ({fact_race_results['has_valid_rank'].mean()*100:.1f}%)")
    print(f"   ⏱️ Valid times: {fact_race_results['has_valid_time'].sum():,} ({fact_race_results['has_valid_time'].mean()*100:.1f}%)")
    print(f"   🚩 DNF: {fact_race_results['dnf_flag'].sum()}")
    print(f"   🚩 DNS: {fact_race_results['dns_flag'].sum()}")
    print(f"   🚩 DSQ: {fact_race_results['dsq_flag'].sum()}")
    
    # Save files
    print(f"\n💾 Saving tables...")
    
    dim_athlete.to_csv('dim_athlete_v2.csv', index=False, encoding='utf-8')
    dim_team.to_csv('dim_team_v2.csv', index=False, encoding='utf-8')
    fact_race_results.to_csv('fact_race_results_v2.csv', index=False, encoding='utf-8')
    
    print(f"\n🎉 ENHANCED STAR SCHEMA CREATED!")
    print(f"📁 Files with DNF/DNS/DSQ flags:")
    print(f"   • dim_athlete_v2.csv ({len(dim_athlete)} athletes)")
    print(f"   • dim_team_v2.csv ({len(dim_team)} teams)")  
    print(f"   • fact_race_results_v2.csv ({len(fact_race_results)} results)")
    
    return {
        'dim_athlete': dim_athlete,
        'dim_team': dim_team,
        'fact_race_results': fact_race_results
    }

if __name__ == "__main__":
    tables = create_fact_race_results_v2()
    print(f"\n🚀 Ready with enhanced data quality flags!")