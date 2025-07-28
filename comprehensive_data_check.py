# comprehensive_data_check.py
import pandas as pd
import numpy as np
from datetime import datetime

def comprehensive_data_quality_check():
    """Comprehensive data quality assessment"""
    
    print("🔍 COMPREHENSIVE DATA QUALITY CHECK")
    print("=" * 60)
    
    try:
        # Load final files
        dim_athlete = pd.read_csv('dim_athlete_final.csv')
        fact_results = pd.read_csv('fact_race_results_final.csv')
        
        print(f"✅ Files loaded successfully")
        
        # =============================================
        # FILE OVERVIEW
        # =============================================
        print(f"\n📁 FILE OVERVIEW:")
        print(f"   📊 DIM_ATHLETE: {len(dim_athlete):,} records, {len(dim_athlete.columns)} columns")
        print(f"   🌟 FACT_RACE_RESULTS: {len(fact_results):,} records, {len(fact_results.columns)} columns")
        
        # =============================================
        # DIM_ATHLETE ANALYSIS
        # =============================================
        print(f"\n👤 DIM_ATHLETE DETAILED ANALYSIS:")
        print(f"   📋 Columns: {list(dim_athlete.columns)}")
        
        # Null analysis
        print(f"\n   📊 NULL Analysis:")
        for col in dim_athlete.columns:
            null_count = dim_athlete[col].isnull().sum()
            null_pct = null_count / len(dim_athlete) * 100
            print(f"     {col}: {null_count} nulls ({null_pct:.1f}%)")
        
        # Top performers
        print(f"\n   🏆 TOP 10 PERFORMERS:")
        if 'total_wins' in dim_athlete.columns and 'total_races' in dim_athlete.columns:
            top_performers = dim_athlete.nlargest(10, 'total_wins')
            for i, (_, athlete) in enumerate(top_performers.iterrows()):
                name = athlete['athlete_name'][:30]  # Truncate long names
                races = athlete.get('total_races', 0)
                wins = athlete.get('total_wins', 0)
                podiums = athlete.get('total_podiums', 0)
                print(f"     {i+1:2d}. {name:<30} | {races:3d} races | {wins:2d} wins | {podiums:2d} podiums")
        
        # Career statistics
        if 'total_races' in dim_athlete.columns:
            print(f"\n   📈 CAREER STATISTICS:")
            print(f"     Total Races - Min: {dim_athlete['total_races'].min()}, Max: {dim_athlete['total_races'].max()}, Avg: {dim_athlete['total_races'].mean():.1f}")
            
            if 'total_wins' in dim_athlete.columns:
                print(f"     Total Wins - Min: {dim_athlete['total_wins'].min()}, Max: {dim_athlete['total_wins'].max()}, Avg: {dim_athlete['total_wins'].mean():.1f}")
                
                # Win rate analysis
                dim_athlete['win_rate'] = dim_athlete['total_wins'] / dim_athlete['total_races'].replace(0, np.nan) * 100
                print(f"     Win Rate - Min: {dim_athlete['win_rate'].min():.1f}%, Max: {dim_athlete['win_rate'].max():.1f}%, Avg: {dim_athlete['win_rate'].mean():.1f}%")
        
        # DNF/DNS/DSQ analysis
        if all(col in dim_athlete.columns for col in ['total_dnf', 'total_dns', 'total_dsq']):
            print(f"\n   🚩 SPECIAL CASES SUMMARY:")
            print(f"     Total DNF incidents: {dim_athlete['total_dnf'].sum():,}")
            print(f"     Total DNS incidents: {dim_athlete['total_dns'].sum():,}")
            print(f"     Total DSQ incidents: {dim_athlete['total_dsq'].sum():,}")
            
            # Athletes with most incidents
            problem_athletes = dim_athlete[dim_athlete['total_dnf'] + dim_athlete['total_dns'] + dim_athlete['total_dsq'] > 0]
            if len(problem_athletes) > 0:
                print(f"     Athletes with incidents: {len(problem_athletes)} ({len(problem_athletes)/len(dim_athlete)*100:.1f}%)")
        
        # =============================================
        # FACT_RACE_RESULTS ANALYSIS
        # =============================================
        print(f"\n🌟 FACT_RACE_RESULTS DETAILED ANALYSIS:")
        print(f"   📋 Columns: {list(fact_results.columns)}")
        
        # Null analysis
        print(f"\n   📊 NULL Analysis:")
        for col in fact_results.columns:
            null_count = fact_results[col].isnull().sum()
            null_pct = null_count / len(fact_results) * 100
            print(f"     {col}: {null_count} nulls ({null_pct:.1f}%)")
        
        # Data quality flags
        if all(col in fact_results.columns for col in ['has_valid_rank', 'has_valid_time']):
            print(f"\n   ✅ DATA QUALITY FLAGS:")
            valid_rank_count = fact_results['has_valid_rank'].sum()
            valid_time_count = fact_results['has_valid_time'].sum()
            print(f"     Valid Ranks: {valid_rank_count:,} ({valid_rank_count/len(fact_results)*100:.1f}%)")
            print(f"     Valid Times: {valid_time_count:,} ({valid_time_count/len(fact_results)*100:.1f}%)")
            
            # Both valid
            both_valid = (fact_results['has_valid_rank'] & fact_results['has_valid_time']).sum()
            print(f"     Both Valid: {both_valid:,} ({both_valid/len(fact_results)*100:.1f}%)")
        
        # Special flags analysis
        if all(col in fact_results.columns for col in ['dnf_flag', 'dns_flag', 'dsq_flag']):
            print(f"\n   🚩 SPECIAL FLAGS DISTRIBUTION:")
            dnf_count = fact_results['dnf_flag'].sum()
            dns_count = fact_results['dns_flag'].sum()
            dsq_count = fact_results['dsq_flag'].sum()
            
            print(f"     DNF (Did Not Finish): {dnf_count:,} ({dnf_count/len(fact_results)*100:.1f}%)")
            print(f"     DNS (Did Not Start): {dns_count:,} ({dns_count/len(fact_results)*100:.1f}%)")
            print(f"     DSQ (Disqualified): {dsq_count:,} ({dsq_count/len(fact_results)*100:.1f}%)")
            
            total_incidents = dnf_count + dns_count + dsq_count
            print(f"     Total Incidents: {total_incidents:,} ({total_incidents/len(fact_results)*100:.1f}%)")
        
        # Rank analysis
        if 'rank_position' in fact_results.columns:
            valid_ranks = fact_results['rank_position'].dropna()
            if len(valid_ranks) > 0:
                print(f"\n   🏆 RANK ANALYSIS:")
                print(f"     Rank Range: {valid_ranks.min():.0f} - {valid_ranks.max():.0f}")
                print(f"     Average Rank: {valid_ranks.mean():.1f}")
                print(f"     Median Rank: {valid_ranks.median():.1f}")
                
                # Rank distribution
                rank_dist = valid_ranks.value_counts().head(10).sort_index()
                print(f"     Top 10 Rank Distribution:")
                for rank, count in rank_dist.items():
                    print(f"       Rank {rank:.0f}: {count:,} times")
        
        # Time analysis
        if 'race_time_seconds' in fact_results.columns:
            valid_times = fact_results['race_time_seconds'].dropna()
            if len(valid_times) > 0:
                print(f"\n   ⏱️ RACE TIME ANALYSIS:")
                print(f"     Time Range: {valid_times.min():.2f}s - {valid_times.max():.2f}s")
                print(f"     Average Time: {valid_times.mean():.2f}s ({valid_times.mean()/60:.1f} min)")
                print(f"     Median Time: {valid_times.median():.2f}s ({valid_times.median()/60:.1f} min)")
                
                # Time distribution by ranges
                print(f"     Time Distribution:")
                time_ranges = [
                    (0, 60, "Under 1 min"),
                    (60, 120, "1-2 min"),
                    (120, 300, "2-5 min"),
                    (300, 600, "5-10 min"),
                    (600, float('inf'), "Over 10 min")
                ]
                
                for min_time, max_time, label in time_ranges:
                    count = ((valid_times >= min_time) & (valid_times < max_time)).sum()
                    pct = count / len(valid_times) * 100
                    print(f"       {label}: {count:,} ({pct:.1f}%)")
        
        # =============================================
        # FOREIGN KEY INTEGRITY
        # =============================================
        print(f"\n🔗 FOREIGN KEY INTEGRITY:")
        
        if 'athlete_key' in fact_results.columns and 'athlete_key' in dim_athlete.columns:
            # Check athlete key integrity
            fact_athlete_keys = set(fact_results['athlete_key'].dropna().astype(int))
            dim_athlete_keys = set(dim_athlete['athlete_key'].astype(int))
            
            orphaned_keys = fact_athlete_keys - dim_athlete_keys
            unused_keys = dim_athlete_keys - fact_athlete_keys
            
            print(f"   👤 Athlete Keys:")
            print(f"     Fact table has: {len(fact_athlete_keys):,} unique athlete keys")
            print(f"     Dim table has: {len(dim_athlete_keys):,} unique athlete keys")
            print(f"     Orphaned keys in fact: {len(orphaned_keys)} (should be 0)")
            print(f"     Unused keys in dim: {len(unused_keys)} ({len(unused_keys)/len(dim_athlete_keys)*100:.1f}%)")
        
        # =============================================
        # SAMPLE DATA PREVIEW
        # =============================================
        print(f"\n👀 SAMPLE DATA PREVIEW:")
        
        # Sample athletes
        print(f"\n   👤 Sample Athletes (first 5):")
        athlete_cols = ['athlete_key', 'athlete_name', 'total_races', 'total_wins', 'total_podiums']
        available_athlete_cols = [col for col in athlete_cols if col in dim_athlete.columns]
        
        sample_athletes = dim_athlete[available_athlete_cols].head(5)
        print(sample_athletes.to_string(index=False))
        
        # Sample fact records
        print(f"\n   🌟 Sample Fact Records (first 5):")
        fact_cols = ['result_id', 'athlete_key', 'rank_position', 'race_time_seconds', 'dnf_flag', 'dns_flag', 'dsq_flag']
        available_fact_cols = [col for col in fact_cols if col in fact_results.columns]
        
        sample_facts = fact_results[available_fact_cols].head(5)
        print(sample_facts.to_string(index=False))
        
        # =============================================
        # DATA ISSUES SUMMARY
        # =============================================
        print(f"\n⚠️ POTENTIAL DATA ISSUES:")
        
        issues = []
        
        # Check for high null rates
        for col in dim_athlete.columns:
            null_pct = dim_athlete[col].isnull().sum() / len(dim_athlete) * 100
            if null_pct > 50:
                issues.append(f"DIM_ATHLETE.{col} has {null_pct:.1f}% nulls")
        
        for col in fact_results.columns:
            null_pct = fact_results[col].isnull().sum() / len(fact_results) * 100
            if null_pct > 50:
                issues.append(f"FACT_RACE_RESULTS.{col} has {null_pct:.1f}% nulls")
        
        # Check data quality flags
        if 'has_valid_rank' in fact_results.columns:
            valid_rank_pct = fact_results['has_valid_rank'].mean() * 100
            if valid_rank_pct < 70:
                issues.append(f"Only {valid_rank_pct:.1f}% of records have valid ranks")
        
        if 'has_valid_time' in fact_results.columns:
            valid_time_pct = fact_results['has_valid_time'].mean() * 100
            if valid_time_pct < 70:
                issues.append(f"Only {valid_time_pct:.1f}% of records have valid times")
        
        # Check for extreme values
        if 'race_time_seconds' in fact_results.columns:
            valid_times = fact_results['race_time_seconds'].dropna()
            if len(valid_times) > 0:
                if valid_times.min() < 30:
                    issues.append(f"Suspiciously fast times: minimum {valid_times.min():.1f}s")
                if valid_times.max() > 1800:
                    issues.append(f"Suspiciously slow times: maximum {valid_times.max():.1f}s")
        
        if len(issues) == 0:
            print(f"   ✅ No significant data quality issues detected!")
        else:
            for i, issue in enumerate(issues, 1):
                print(f"   {i}. {issue}")
        
        # =============================================
        # RECOMMENDATIONS
        # =============================================
        print(f"\n💡 RECOMMENDATIONS FOR POSTGRESQL:")
        
        recommendations = [
            "✅ Data is ready for PostgreSQL upload",
            "📊 Create indexes on athlete_key for fast joins",
            "🔍 Consider partitioning fact table by date if you add date dimension",
            "📈 Set up materialized views for common analytics queries",
            "🚩 Monitor DNF/DNS/DSQ rates for data quality trends"
        ]
        
        for rec in recommendations:
            print(f"   {rec}")
        
        # =============================================
        # SUMMARY SCORE
        # =============================================
        print(f"\n🎯 DATA QUALITY SCORE:")
        
        score_factors = []
        
        # Completeness score
        if 'has_valid_rank' in fact_results.columns:
            rank_completeness = fact_results['has_valid_rank'].mean() * 100
            score_factors.append(('Rank Completeness', rank_completeness))
        
        if 'has_valid_time' in fact_results.columns:
            time_completeness = fact_results['has_valid_time'].mean() * 100
            score_factors.append(('Time Completeness', time_completeness))
        
        # Foreign key integrity
        if len(orphaned_keys) == 0:
            score_factors.append(('FK Integrity', 100.0))
        else:
            fk_score = max(0, 100 - len(orphaned_keys) / len(fact_athlete_keys) * 100)
            score_factors.append(('FK Integrity', fk_score))
        
        if score_factors:
            overall_score = sum(score for _, score in score_factors) / len(score_factors)
            
            print(f"   📊 Score Breakdown:")
            for factor, score in score_factors:
                print(f"     {factor}: {score:.1f}%")
            
            print(f"\n   🎖️ OVERALL QUALITY SCORE: {overall_score:.1f}%")
            
            if overall_score >= 90:
                print(f"   🎉 EXCELLENT - Ready for production!")
            elif overall_score >= 80:
                print(f"   ✅ GOOD - Ready for PostgreSQL with minor monitoring")
            elif overall_score >= 70:
                print(f"   ⚠️ ACCEPTABLE - Consider data cleanup")
            else:
                print(f"   ❌ NEEDS IMPROVEMENT - Address data quality issues")
        
        print(f"\n🎉 COMPREHENSIVE DATA CHECK COMPLETED!")
        
        return {
            'dim_athlete': dim_athlete,
            'fact_results': fact_results,
            'issues': issues,
            'overall_score': overall_score if 'overall_score' in locals() else None
        }
        
    except Exception as e:
        print(f"❌ Error during data check: {str(e)}")
        return None

if __name__ == "__main__":
    result = comprehensive_data_quality_check()
    
    if result:
        print(f"\n🚀 Data check completed successfully!")
        if result['overall_score'] and result['overall_score'] >= 80:
            print(f"✅ Ready to proceed with PostgreSQL upload!")
        else:
            print(f"⚠️ Consider addressing data quality issues before upload.")