# upload_to_postgresql.py
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import numpy as np
from datetime import datetime

def upload_star_schema_to_postgresql():
    """Upload star schema to PostgreSQL"""
    
    print("🚀 POSTGRESQL UPLOAD - ROWING STAR SCHEMA")
    print("=" * 60)
    
    # Database connection parameters
    DB_CONFIG = {
        'host': 'my-dev-database.cqx0ougy8gof.us-east-1.rds.amazonaws.com',
        'port': 5432,
        'database': 'postgres',  # veya sizin database isminiz
        'username': 'postgres',   # veya sizin username'iniz
        'password': 'DataScience123!'  # ⚠️ Gerçek şifrenizi girin
    }
    
    print(f"📋 Connection Configuration:")
    print(f"   Host: {DB_CONFIG['host']}")
    print(f"   Port: {DB_CONFIG['port']}")
    print(f"   Database: {DB_CONFIG['database']}")
    print(f"   Username: {DB_CONFIG['username']}")
    
    try:
        # =============================================
        # 1. CONNECTION TEST
        # =============================================
        print(f"\n🔌 1. Testing PostgreSQL connection...")
        
        connection_string = f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version();"))
            version = result.fetchone()[0]
            print(f"✅ Connected successfully!")
            print(f"   PostgreSQL Version: {version[:50]}...")
        
        # =============================================
        # 2. CREATE SCHEMA
        # =============================================
        print(f"\n🏗️ 2. Creating schema...")
        
        with engine.connect() as conn:
            # Create schema if not exists
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS sports_analytics;"))
            conn.commit()
            print(f"✅ Schema 'sports_analytics' created/verified")
        
        # =============================================
        # 3. LOAD DATA FILES
        # =============================================
        print(f"\n📊 3. Loading data files...")
        
        dim_athlete = pd.read_csv('dim_athlete_final.csv')
        fact_results = pd.read_csv('fact_race_results_final.csv')
        
        print(f"✅ Data loaded:")
        print(f"   DIM_ATHLETE: {len(dim_athlete):,} records")
        print(f"   FACT_RACE_RESULTS: {len(fact_results):,} records")
        
        # =============================================
        # 4. UPLOAD DIMENSION TABLES
        # =============================================
        print(f"\n👤 4. Uploading DIM_ATHLETE...")
        
        # Upload dimension table
        dim_athlete.to_sql(
            'dim_athlete', 
            engine, 
            schema='sports_analytics',
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"✅ DIM_ATHLETE uploaded successfully")
        
        # =============================================
        # 5. UPLOAD FACT TABLE
        # =============================================
        print(f"\n🌟 5. Uploading FACT_RACE_RESULTS...")
        
        # Replace NaN with None for proper NULL handling
        fact_results_clean = fact_results.replace({np.nan: None})
        
        fact_results_clean.to_sql(
            'fact_race_results',
            engine,
            schema='sports_analytics', 
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        print(f"✅ FACT_RACE_RESULTS uploaded successfully")
        
        # =============================================
        # 6. CREATE INDEXES
        # =============================================
        print(f"\n⚡ 6. Creating indexes for performance...")
        
        index_queries = [
            # Primary key indexes (if not auto-created)
            "CREATE INDEX IF NOT EXISTS idx_dim_athlete_pk ON sports_analytics.dim_athlete(athlete_key);",
            "CREATE INDEX IF NOT EXISTS idx_fact_results_pk ON sports_analytics.fact_race_results(result_id);",
            
            # Foreign key indexes
            "CREATE INDEX IF NOT EXISTS idx_fact_athlete_fk ON sports_analytics.fact_race_results(athlete_key);",
            
            # Performance indexes
            "CREATE INDEX IF NOT EXISTS idx_fact_rank ON sports_analytics.fact_race_results(rank_position) WHERE rank_position IS NOT NULL;",
            "CREATE INDEX IF NOT EXISTS idx_fact_time ON sports_analytics.fact_race_results(race_time_seconds) WHERE race_time_seconds IS NOT NULL;",
            "CREATE INDEX IF NOT EXISTS idx_fact_flags ON sports_analytics.fact_race_results(dnf_flag, dns_flag, dsq_flag);",
            
            # Athlete name index for lookups
            "CREATE INDEX IF NOT EXISTS idx_athlete_name ON sports_analytics.dim_athlete(athlete_name);",
        ]
        
        with engine.connect() as conn:
            for i, query in enumerate(index_queries, 1):
                try:
                    conn.execute(text(query))
                    print(f"   ✅ Index {i}/{len(index_queries)} created")
                except Exception as e:
                    print(f"   ⚠️ Index {i} warning: {str(e)[:50]}...")
            
            conn.commit()
        
        print(f"✅ Indexes created successfully")
        
        # =============================================
        # 7. DATA VERIFICATION
        # =============================================
        print(f"\n🔍 7. Verifying uploaded data...")
        
        with engine.connect() as conn:
            # Count records
            athlete_count = conn.execute(text("SELECT COUNT(*) FROM sports_analytics.dim_athlete;")).fetchone()[0]
            fact_count = conn.execute(text("SELECT COUNT(*) FROM sports_analytics.fact_race_results;")).fetchone()[0]
            
            print(f"✅ Record counts verified:")
            print(f"   DIM_ATHLETE: {athlete_count:,} records")
            print(f"   FACT_RACE_RESULTS: {fact_count:,} records")
            
            # Data quality checks
            quality_checks = [
                ("Valid Ranks", "SELECT COUNT(*) FROM sports_analytics.fact_race_results WHERE has_valid_rank = true;"),
                ("Valid Times", "SELECT COUNT(*) FROM sports_analytics.fact_race_results WHERE has_valid_time = true;"),
                ("DNF Count", "SELECT COUNT(*) FROM sports_analytics.fact_race_results WHERE dnf_flag = true;"),
                ("DNS Count", "SELECT COUNT(*) FROM sports_analytics.fact_race_results WHERE dns_flag = true;"),
                ("DSQ Count", "SELECT COUNT(*) FROM sports_analytics.fact_race_results WHERE dsq_flag = true;"),
                ("Athletes with Wins", "SELECT COUNT(*) FROM sports_analytics.dim_athlete WHERE total_wins > 0;")
            ]
            
            print(f"\n   📊 Quality Verification:")
            for check_name, query in quality_checks:
                try:
                    result = conn.execute(text(query)).fetchone()[0]
                    print(f"     {check_name}: {result:,}")
                except Exception as e:
                    print(f"     {check_name}: Error - {str(e)[:30]}...")
        
        # =============================================
        # 8. SAMPLE ANALYTICAL QUERIES
        # =============================================
        print(f"\n📈 8. Testing sample analytical queries...")
        
        sample_queries = [
            ("Top 5 Athletes by Wins", """
                SELECT athlete_name, total_races, total_wins, total_podiums
                FROM sports_analytics.dim_athlete 
                WHERE total_wins > 0
                ORDER BY total_wins DESC, total_podiums DESC 
                LIMIT 5;
            """),
            
            ("Performance Distribution", """
                SELECT 
                    CASE 
                        WHEN rank_position = 1 THEN '1st Place'
                        WHEN rank_position BETWEEN 2 AND 3 THEN '2nd-3rd Place'
                        WHEN rank_position BETWEEN 4 AND 10 THEN '4th-10th Place'
                        ELSE 'Other'
                    END as position_group,
                    COUNT(*) as count
                FROM sports_analytics.fact_race_results 
                WHERE has_valid_rank = true
                GROUP BY position_group
                ORDER BY count DESC;
            """),
            
            ("Incident Summary", """
                SELECT 
                    'DNF' as incident_type, COUNT(*) as count
                FROM sports_analytics.fact_race_results WHERE dnf_flag = true
                UNION ALL
                SELECT 'DNS', COUNT(*) FROM sports_analytics.fact_race_results WHERE dns_flag = true
                UNION ALL  
                SELECT 'DSQ', COUNT(*) FROM sports_analytics.fact_race_results WHERE dsq_flag = true
                ORDER BY count DESC;
            """)
        ]
        
        with engine.connect() as conn:
            for query_name, query in sample_queries:
                try:
                    print(f"\n   🔍 {query_name}:")
                    result = conn.execute(text(query))
                    rows = result.fetchall()
                    
                    if rows:
                        for row in rows[:5]:  # Show max 5 rows
                            print(f"     {' | '.join(str(col) for col in row)}")
                    else:
                        print(f"     No results")
                        
                except Exception as e:
                    print(f"     Error: {str(e)[:50]}...")
        
        # =============================================
        # 9. CONNECTION INFO FOR TOOLS
        # =============================================
        print(f"\n🔗 9. Connection information for BI tools:")
        print(f"   Host: {DB_CONFIG['host']}")
        print(f"   Port: {DB_CONFIG['port']}")
        print(f"   Database: {DB_CONFIG['database']}")
        print(f"   Schema: sports_analytics")
        print(f"   Tables: dim_athlete, fact_race_results")
        
        # =============================================
        # 10. SUMMARY
        # =============================================
        print(f"\n🎉 POSTGRESQL UPLOAD COMPLETED SUCCESSFULLY!")
        
        summary = {
            'athletes_uploaded': athlete_count,
            'results_uploaded': fact_count,
            'schema': 'sports_analytics',
            'tables': ['dim_athlete', 'fact_race_results'],
            'indexes_created': len(index_queries),
            'connection_string': f"postgresql://{DB_CONFIG['username']}:***@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        }
        
        print(f"\n📊 Upload Summary:")
        for key, value in summary.items():
            if key != 'connection_string':
                print(f"   {key}: {value}")
        
        print(f"\n🚀 Ready for:")
        print(f"   • BI Tool connections (Tableau, Power BI)")
        print(f"   • Advanced analytics queries")
        print(f"   • Dashboard development")
        print(f"   • Performance monitoring")
        
        return summary
        
    except Exception as e:
        print(f"❌ Upload failed: {str(e)}")
        print(f"\n💡 Troubleshooting:")
        print(f"   1. Check PostgreSQL is running")
        print(f"   2. Verify credentials in DB_CONFIG")
        print(f"   3. Ensure database exists")
        print(f"   4. Check firewall/network settings")
        
        return None

if __name__ == "__main__":
    print("⚠️ BEFORE RUNNING:")
    print("1. Update DB_CONFIG with your PostgreSQL credentials")
    print("2. Ensure PostgreSQL is running")
    print("3. Create target database if needed")
    
    confirm = input("\nReady to upload? (y/N): ")
    
    if confirm.lower() == 'y':
        result = upload_star_schema_to_postgresql()
        
        if result:
            print(f"\n✅ Upload successful! Check DBeaver for your data.")
        else:
            print(f"\n❌ Upload failed. Check error messages above.")
    else:
        print("Upload cancelled.")