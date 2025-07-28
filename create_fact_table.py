# create_fact_table.py
import pandas as pd
import numpy as np
from datetime import datetime

def create_fact_race_results():
    """FACT_RACE_RESULTS tablosu ve dimension'ları oluştur"""
    
    print("🌟 FACT_RACE_RESULTS CREATOR")
    print("=" * 50)
    
    # Temiz veriyi oku
    filename = 'rowing_data_raw_20250724_110546_fixed_cleaned.csv'
    df = pd.read_csv(filename)
    
    print(f"📊 Temiz veri yüklendi: {df.shape}")
    
    # =============================================
    # 1. DIM_ATHLETE
    # =============================================
    print(f"\n👤 1. Creating DIM_ATHLETE...")
    
    athlete_stats = df.groupby('Sporcu').agg({
        'Derece': ['count', 'mean', lambda x: (pd.to_numeric(x, errors='coerce') == 1).sum()],
        'Sonuc': 'count'
    }).reset_index()
    
    athlete_stats.columns = ['athlete_name', 'total_races', 'avg_rank', 'total_wins', 'race_count_check']
    athlete_stats['athlete_key'] = range(1, len(athlete_stats) + 1)
    athlete_stats['athlete_id'] = athlete_stats['athlete_name'].str.replace(' ', '_').str.upper()
    
    # Podium finishes (1st, 2nd, 3rd)
    podium_stats = df[pd.to_numeric(df['Derece'], errors='coerce').isin([1, 2, 3])].groupby('Sporcu').size().reset_index(name='total_podiums')
    athlete_stats = athlete_stats.merge(podium_stats, left_on='athlete_name', right_on='Sporcu', how='left')
    athlete_stats['total_podiums'] = athlete_stats['total_podiums'].fillna(0).astype(int)
    
    # Best rank
    best_ranks = df.groupby('Sporcu')['Derece'].apply(lambda x: pd.to_numeric(x, errors='coerce').min()).reset_index()
    best_ranks.columns = ['athlete_name', 'best_rank']
    athlete_stats = athlete_stats.merge(best_ranks, on='athlete_name', how='left')
    
    dim_athlete = athlete_stats[['athlete_key', 'athlete_id', 'athlete_name', 'total_races', 'total_wins', 'total_podiums', 'best_rank', 'avg_rank']].copy()
    
    print(f"✅ DIM_ATHLETE: {len(dim_athlete)} athletes")
    
    # =============================================
    # 2. DIM_TEAM  
    # =============================================
    print(f"\n🏅 2. Creating DIM_TEAM...")
    
    team_stats = df.groupby('Takim').agg({
        'Sporcu': 'nunique',
        'Derece': ['count', lambda x: (pd.to_numeric(x, errors='coerce') == 1).sum()]
    }).reset_index()
    
    team_stats.columns = ['team_name', 'total_athletes', 'total_races', 'championship_count']
    team_stats['team_key'] = range(1, len(team_stats) + 1)
    team_stats['team_id'] = team_stats['team_name'].str.replace(' ', '_').str.upper()
    
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
    
    dim_team = team_stats[['team_key', 'team_id', 'team_name', 'team_type', 'total_athletes', 'total_races', 'championship_count']].copy()
    
    print(f"✅ DIM_TEAM: {len(dim_team)} teams")
    
    # =============================================
    # 3. DIM_RACE
    # =============================================
    print(f"\n🏆 3. Creating DIM_RACE...")
    
    race_stats = df.groupby(['Yaris_Adi', 'Yaris_Adi2']).agg({
        'Sporcu': 'nunique',
        'Seri_Adi': 'nunique',
        'Derece': 'count'
    }).reset_index()
    
    race_stats.columns = ['race_name', 'race_name_secondary', 'total_participants', 'total_categories', 'total_results']
    race_stats['race_key'] = range(1, len(race_stats) + 1)
    
    # Race type classification
    def classify_race_type(race_name, race_name_2):
        full_name = f"{race_name} {race_name_2 or ''}".upper()
        if 'INTERNATIONAL' in full_name:
            return 'International'
        elif 'TÜRKİYE' in full_name or 'ŞAMPIYON' in full_name:
            return 'National Championship'
        elif 'KUPA' in full_name:
            return 'Cup'
        else:
            return 'Regional'
    
    race_stats['race_type'] = race_stats.apply(lambda x: classify_race_type(x['race_name'], x['race_name_secondary']), axis=1)
    race_stats['is_international'] = race_stats['race_type'] == 'International'
    race_stats['is_championship'] = race_stats['race_type'].str.contains('Championship')
    race_stats['race_id'] = race_stats['race_name'].str.replace(' ', '_').str.upper()
    
    dim_race = race_stats[['race_key', 'race_id', 'race_name', 'race_name_secondary', 'race_type', 'is_international', 'is_championship', 'total_participants', 'total_categories']].copy()
    
    print(f"✅ DIM_RACE: {len(dim_race)} races")
    
    # =============================================
    # 4. DIM_CATEGORY
    # =============================================
    print(f"\n🎯 4. Creating DIM_CATEGORY...")
    
    category_stats = df.groupby('Seri_Adi').agg({
        'Sporcu': 'nunique'
    }).reset_index()
    
    category_stats.columns = ['category_name', 'total_participants']
    category_stats['category_key'] = range(1, len(category_stats) + 1)
    category_stats['category_id'] = category_stats['category_name'].str.replace(' ', '_').str.upper()
    
    # Parse category details
    def parse_category(category_name):
        if not category_name:
            return {'gender': 'Unknown', 'age_group': 'Unknown', 'boat_type': 'Unknown'}
        
        category_upper = category_name.upper()
        
        # Gender
        if any(word in category_upper for word in ['BAYAN', 'KADIN', 'WOMEN']):
            gender = 'Kadın'
        elif any(word in category_upper for word in ['ERKEK', 'MEN']):
            gender = 'Erkek'  
        elif 'MIX' in category_upper:
            gender = 'Karışık'
        else:
            gender = 'Bilinmiyor'
        
        # Age group
        if any(word in category_upper for word in ['GENÇ', 'JUNIOR']):
            age_group = 'Genç'
        elif 'MASTER' in category_upper:
            age_group = 'Master'
        elif any(word in category_upper for word in ['SENIOR', 'BÜYÜK']):
            age_group = 'Senior'
        else:
            age_group = 'Bilinmiyor'
        
        # Boat type
        if '1X' in category_upper:
            boat_type = '1x (Single)'
        elif '2X' in category_upper:
            boat_type = '2x (Double)'
        elif '4X+' in category_upper:
            boat_type = '4x+ (Quad with Cox)'
        elif '2-' in category_upper:
            boat_type = '2- (Pair)'
        elif '4-' in category_upper:
            boat_type = '4- (Four)'
        elif '8+' in category_upper:
            boat_type = '8+ (Eight)'
        else:
            boat_type = 'Diğer'
        
        return {'gender': gender, 'age_group': age_group, 'boat_type': boat_type}
    
    # Apply parsing
    parsed_categories = category_stats['category_name'].apply(parse_category)
    category_stats['gender'] = [cat['gender'] for cat in parsed_categories]
    category_stats['age_group'] = [cat['age_group'] for cat in parsed_categories]  
    category_stats['boat_type'] = [cat['boat_type'] for cat in parsed_categories]
    
    dim_category = category_stats[['category_key', 'category_id', 'category_name', 'gender', 'age_group', 'boat_type', 'total_participants']].copy()
    
    print(f"✅ DIM_CATEGORY: {len(dim_category)} categories")
    
    # =============================================
    # 5. DIM_LOCATION
    # =============================================
    print(f"\n🏟️ 5. Creating DIM_LOCATION...")
    
    location_stats = df.groupby('Yaris_Yeri').agg({
        'Sporcu': 'nunique',
        'Derece': 'count'
    }).reset_index()
    
    location_stats.columns = ['location_name', 'total_participants', 'total_events']
    location_stats['location_key'] = range(1, len(location_stats) + 1)
    location_stats['location_id'] = location_stats['location_name'].str.replace(' ', '_').str.upper()
    
    # Region classification
    def classify_region(location_name):
        location_upper = location_name.upper()
        if any(city in location_upper for city in ['İSTANBUL', 'KALAMIS', 'HALİÇ', 'BEYKOZ']):
            return 'İstanbul'
        elif 'ANKARA' in location_upper:
            return 'Ankara'
        elif any(city in location_upper for city in ['SAPANCA', 'KOCAELİ', 'GEBZE']):
            return 'Marmara'
        elif 'EDİRNE' in location_upper:
            return 'Trakya'
        else:
            return 'Diğer'
    
    location_stats['region'] = location_stats['location_name'].apply(classify_region)
    location_stats['venue_type'] = 'Kürek Pisti'
    
    dim_location = location_stats[['location_key', 'location_id', 'location_name', 'region', 'venue_type', 'total_events', 'total_participants']].copy()
    
    print(f"✅ DIM_LOCATION: {len(dim_location)} locations")
    
    # =============================================
    # 6. DIM_DATE  
    # =============================================
    print(f"\n📅 6. Creating DIM_DATE...")
    
    # Parse Turkish dates
    def parse_turkish_date(date_str):
        if not date_str:
            return None
        
        months = {
            'Ocak': '01', 'Şubat': '02', 'Mart': '03', 'Nisan': '04',
            'Mayıs': '05', 'Haziran': '06', 'Temmuz': '07', 'Ağustos': '08',
            'Eylül': '09', 'Ekim': '10', 'Kasım': '11', 'Aralık': '12'
        }
        
        date_clean = date_str.strip()
        for tr_month, num_month in months.items():
            date_clean = date_clean.replace(tr_month, num_month)
        
        try:
            return pd.to_datetime(date_clean, format='%d %m %Y')
        except:
            return None
    
    # Get unique dates
    df['parsed_date'] = df['Tarih'].apply(parse_turkish_date)
    unique_dates = df['parsed_date'].dropna().unique()
    
    dim_date = pd.DataFrame({'full_date': unique_dates})
    dim_date['year'] = dim_date['full_date'].dt.year
    dim_date['quarter'] = dim_date['full_date'].dt.quarter
    dim_date['month'] = dim_date['full_date'].dt.month
    dim_date['month_name'] = dim_date['full_date'].dt.strftime('%B')
    dim_date['day'] = dim_date['full_date'].dt.day
    dim_date['day_of_week'] = dim_date['full_date'].dt.dayofweek + 1
    dim_date['day_name'] = dim_date['full_date'].dt.strftime('%A')
    dim_date['is_weekend'] = dim_date['full_date'].dt.dayofweek >= 5
    
    # Season  
    def get_season(month):
        if month in [12, 1, 2]:
            return 'Kış'
        elif month in [3, 4, 5]:
            return 'İlkbahar'
        elif month in [6, 7, 8]:
            return 'Yaz'
        else:
            return 'Sonbahar'
    
    dim_date['season'] = dim_date['month'].apply(get_season)
    dim_date['date_key'] = dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
    
    dim_date = dim_date[['date_key', 'full_date', 'year', 'quarter', 'month', 'month_name', 'day', 'day_of_week', 'day_name', 'is_weekend', 'season']].sort_values('date_key')
    
    print(f"✅ DIM_DATE: {len(dim_date)} dates")
    
    # =============================================
    # 7. FACT_RACE_RESULTS
    # =============================================
    print(f"\n🌟 7. Creating FACT_RACE_RESULTS...")
    
    fact_df = df.copy()
    
    # Foreign key mappings
    athlete_mapping = dim_athlete.set_index('athlete_name')['athlete_key'].to_dict()
    team_mapping = dim_team.set_index('team_name')['team_key'].to_dict()
    category_mapping = dim_category.set_index('category_name')['category_key'].to_dict()
    location_mapping = dim_location.set_index('location_name')['location_key'].to_dict()
    
    # Race mapping (composite key)
    race_mapping = {}
    for _, row in dim_race.iterrows():
        key = (row['race_name'], row['race_name_secondary'])
        race_mapping[key] = row['race_key']
    
    # Date mapping
    date_mapping = dim_date.set_index('full_date')['date_key'].to_dict()
    
    # Map foreign keys
    fact_df['athlete_key'] = fact_df['Sporcu'].map(athlete_mapping)
    fact_df['team_key'] = fact_df['Takim'].map(team_mapping)
    fact_df['category_key'] = fact_df['Seri_Adi'].map(category_mapping)
    fact_df['location_key'] = fact_df['Yaris_Yeri'].map(location_mapping)
    
    # Race mapping
    fact_df['race_combo'] = list(zip(fact_df['Yaris_Adi'], fact_df['Yaris_Adi2']))  
    fact_df['race_key'] = fact_df['race_combo'].map(race_mapping)
    
    # Date mapping
    fact_df['date_key'] = fact_df['parsed_date'].map(date_mapping)
    
    # Measures
    fact_df['rank_position'] = pd.to_numeric(fact_df['Derece'], errors='coerce')
    fact_df['race_time_decimal'] = pd.to_numeric(fact_df['Sonuc'], errors='coerce')
    fact_df['lane_number'] = pd.to_numeric(fact_df['Parkur_No'], errors='coerce')
    fact_df['series_number'] = pd.to_numeric(fact_df['Seri_No'], errors='coerce')
    fact_df['warning_points'] = pd.to_numeric(fact_df['ihtar'], errors='coerce').fillna(0)
    fact_df['series_time'] = fact_df['Seri_Saat']
    
    # Data quality flags
    fact_df['has_valid_time'] = fact_df['race_time_decimal'].notna()
    fact_df['has_valid_rank'] = fact_df['rank_position'].notna()
    
    # Result ID
    fact_df['result_id'] = range(1, len(fact_df) + 1)
    
    # Final fact table
    fact_columns = ['result_id', 'athlete_key', 'team_key', 'race_key', 'category_key', 
                   'location_key', 'date_key', 'rank_position', 'race_time_decimal',
                   'lane_number', 'series_number', 'series_time', 'warning_points',
                   'has_valid_time', 'has_valid_rank']
    
    fact_race_results = fact_df[fact_columns].copy()
    
    print(f"✅ FACT_RACE_RESULTS: {len(fact_race_results)} records")
    
    # Data quality report
    print(f"\n📊 FOREIGN KEY COVERAGE:")
    for fk in ['athlete_key', 'team_key', 'race_key', 'category_key', 'location_key', 'date_key']:
        coverage = fact_race_results[fk].notna().sum() / len(fact_race_results) * 100
        print(f"   {fk}: {coverage:.1f}%")
    
    # Save all tables
    print(f"\n💾 8. Saving tables...")
    
    # Save dimensions
    dim_athlete.to_csv('dim_athlete.csv', index=False, encoding='utf-8')
    dim_team.to_csv('dim_team.csv', index=False, encoding='utf-8')
    dim_race.to_csv('dim_race.csv', index=False, encoding='utf-8')  
    dim_category.to_csv('dim_category.csv', index=False, encoding='utf-8')
    dim_location.to_csv('dim_location.csv', index=False, encoding='utf-8')
    dim_date.to_csv('dim_date.csv', index=False, encoding='utf-8')
    
    # Save fact table
    fact_race_results.to_csv('fact_race_results.csv', index=False, encoding='utf-8')
    
    print(f"\n🎉 STAR SCHEMA CREATED SUCCESSFULLY!")
    print(f"📁 Files created:")
    print(f"   • dim_athlete.csv ({len(dim_athlete)} records)")
    print(f"   • dim_team.csv ({len(dim_team)} records)")
    print(f"   • dim_race.csv ({len(dim_race)} records)")
    print(f"   • dim_category.csv ({len(dim_category)} records)")
    print(f"   • dim_location.csv ({len(dim_location)} records)")
    print(f"   • dim_date.csv ({len(dim_date)} records)")
    print(f"   • fact_race_results.csv ({len(fact_race_results)} records)")
    
    return {
        'dim_athlete': dim_athlete,
        'dim_team': dim_team,
        'dim_race': dim_race,
        'dim_category': dim_category,  
        'dim_location': dim_location,
        'dim_date': dim_date,
        'fact_race_results': fact_race_results
    }

if __name__ == "__main__":
    tables = create_fact_race_results()
    print(f"\n🚀 Ready for PostgreSQL upload!")