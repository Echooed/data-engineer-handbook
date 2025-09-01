from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import builtins  # gives access to Python's built-in round()

def create_spark_session():
    """Create and configure Spark session with optimizations"""
    spark = SparkSession.builder \
        .appName("Gaming Data Analysis - Optimized") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .getOrCreate()
    
    # Disable automatic broadcast join threshold as required
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    
    return spark

def get_directory_size(path):
    """Calculate directory size in MB"""
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return builtins.round(total / (1024*1024), 2)  # MB

def load_and_inspect_data(spark):
    """Load all CSV files and perform initial inspection"""
    base_path = "/home/dataspiro/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data/"
    
    # Load all datasets
    datasets = {
        'maps': 'maps.csv',
        'matches': 'matches.csv', 
        'medals': 'medals.csv',
        'match_details': 'match_details.csv',
        'medals_matches_players': 'medals_matches_players.csv'
    }
    
    dfs = {}
    for name, filename in datasets.items():
        dfs[name] = spark.read.csv(
            f"{base_path}{filename}",
            header=True,
            inferSchema=True
        )
        print(f"\n=== {name.upper()} DATASET ===")
        dfs[name].show(5)
        print(f"Row count: {dfs[name].count()}")
        print(f"Columns: {dfs[name].columns}")
    
    return dfs

def setup_optimized_tables(spark, dfs):
    """Setup bucketed and broadcast tables for optimal joins"""
    
    # Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS gaming_analysis")
    spark.sql("USE gaming_analysis")
    
    # Enable bucketing
    spark.conf.set("spark.sql.sources.bucketing.enabled", "true")
    spark.conf.set("spark.sql.sources.bucketing.autoBucketedScan.enabled", "true")
    
    # Bucket large tables on match_id (common join key)
    print("Creating bucketed tables...")
    dfs['match_details'].write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bucketed_match_details")
    dfs['matches'].write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bucketed_matches") 
    dfs['medals_matches_players'].write.mode("overwrite").bucketBy(16, "match_id").saveAsTable("bucketed_medals_matches_players")
    
    # Save smaller tables for broadcasting
    dfs['medals'].write.mode("overwrite").saveAsTable("broadcast_medals")
    dfs['maps'].write.mode("overwrite").saveAsTable("broadcast_maps")
    
    print("Tables created successfully!")

def create_main_analysis_table(spark):
    """Create the main analysis table with optimal joins"""
    
    # Load tables
    bucketed_matches = spark.table("bucketed_matches")
    bucketed_match_details = spark.table("bucketed_match_details") 
    bucketed_medals_matches_players = spark.table("bucketed_medals_matches_players")
    
    # Create main analysis table with bucketed joins
    match_player_table = spark.sql("""
        SELECT  
            m.match_id,
            md.player_gamertag,
            md.player_total_kills,
            m.mapid,
            m.playlist_id,
            mmp.medal_id
        FROM bucketed_matches m
        LEFT JOIN bucketed_match_details md 
               ON m.match_id = md.match_id
        LEFT JOIN bucketed_medals_matches_players mmp 
               ON md.match_id = mmp.match_id 
               AND md.player_gamertag = mmp.player_gamertag
    """)
    
    # Cache for multiple analyses
    match_player_table.cache()
    match_player_table.createOrReplaceTempView("match_player_table")
    
    return match_player_table

def analyze_player_performance(spark):
    """Analysis 1: Top players by average kills"""
    print("\n=== TOP PLAYERS BY AVERAGE KILLS ===")
    
    result = spark.sql("""
        SELECT 
            player_gamertag AS player_id,
            COUNT(DISTINCT match_id) AS games_played,
            ROUND(AVG(player_total_kills), 2) AS avg_total_kills,
            ROUND(SUM(player_total_kills), 2) AS total_kills
        FROM match_player_table
        WHERE player_gamertag IS NOT NULL
        GROUP BY player_gamertag
        HAVING games_played >= 5  -- Filter for players with meaningful sample size
        ORDER BY avg_total_kills DESC
        LIMIT 10
    """)
    
    result.show()
    return result

def analyze_playlist_popularity(spark):
    """Analysis 2: Most popular playlists"""
    print("\n=== PLAYLIST POPULARITY ANALYSIS ===")
    
    # 2a: By total plays
    playlist_plays = spark.sql("""
        SELECT 
            playlist_id,
            COUNT(*) AS total_plays
        FROM match_player_table
        WHERE playlist_id IS NOT NULL
        GROUP BY playlist_id
        ORDER BY total_plays DESC
        LIMIT 10
    """)
    
    print("Most played playlists (by total plays):")
    playlist_plays.show()
    
    # 2b: By unique matches and players
    playlist_engagement = spark.sql("""
        SELECT 
            playlist_id,
            COUNT(DISTINCT match_id) as total_matches,
            COUNT(DISTINCT player_gamertag) as unique_players,
            ROUND(COUNT(*) / COUNT(DISTINCT match_id), 2) as avg_players_per_match
        FROM match_player_table
        WHERE playlist_id IS NOT NULL
        GROUP BY playlist_id
        ORDER BY total_matches DESC
        LIMIT 10
    """)
    
    print("Playlist engagement metrics:")
    playlist_engagement.show()
    
    return playlist_plays, playlist_engagement

def analyze_map_popularity(spark):
    """Analysis 3: Most popular maps"""
    print("\n=== MAP POPULARITY ANALYSIS ===")
    
    # 3a: By total plays
    map_plays = spark.sql("""
        SELECT 
            mapid,
            COUNT(*) AS total_plays
        FROM match_player_table
        WHERE mapid IS NOT NULL
        GROUP BY mapid
        ORDER BY total_plays DESC
        LIMIT 10
    """)
    
    print("Most played maps (by total plays):")
    map_plays.show()
    
    # 3b: By unique matches and players  
    map_engagement = spark.sql("""
        SELECT 
            mapid,
            COUNT(DISTINCT match_id) as total_matches,
            COUNT(DISTINCT player_gamertag) as unique_players,
            ROUND(COUNT(*) / COUNT(DISTINCT match_id), 2) as avg_players_per_match
        FROM match_player_table
        WHERE mapid IS NOT NULL
        GROUP BY mapid
        ORDER BY total_matches DESC
        LIMIT 10
    """)
    
    print("Map engagement metrics:")
    map_engagement.show()
    
    return map_plays, map_engagement

def analyze_killing_spree_medals(spark):
    """Analysis 4: Top killing spree performance by map"""
    print("\n=== KILLING SPREE ANALYSIS BY MAP ===")
    
    # Load broadcast tables
    broadcast_medals = broadcast(spark.table("broadcast_medals"))
    
    # Join with broadcast medals for efficient lookup
    killing_spree_analysis = spark.sql("""
        SELECT 
            mpt.mapid AS map_id,
            mpt.player_gamertag,
            COUNT(*) AS killing_spree_count
        FROM match_player_table mpt
        JOIN broadcast_medals bcm ON mpt.medal_id = bcm.medal_id
        WHERE bcm.name = 'Killing Spree'
        GROUP BY mpt.mapid, mpt.player_gamertag
        ORDER BY killing_spree_count DESC
        LIMIT 10
    """)
    
    killing_spree_analysis.show()
    
    # Additional analysis: Killing sprees by map (aggregated)
    killing_spree_by_map = spark.sql("""
        SELECT 
            mpt.mapid AS map_id,
            COUNT(*) AS total_killing_sprees,
            COUNT(DISTINCT mpt.player_gamertag) AS unique_players_with_sprees,
            ROUND(COUNT(*) / COUNT(DISTINCT mpt.player_gamertag), 2) AS avg_sprees_per_player
        FROM match_player_table mpt
        JOIN broadcast_medals bcm ON mpt.medal_id = bcm.medal_id
        WHERE bcm.name = 'Killing Spree'
        GROUP BY mpt.mapid
        ORDER BY total_killing_sprees DESC
        LIMIT 10
    """)
    
    print("\nKilling spree summary by map:")
    killing_spree_by_map.show()
    
    return killing_spree_analysis, killing_spree_by_map

def save_sorted_datasets(spark, match_player_table):
    """Save datasets sorted by different columns for optimized future queries"""
    print("\n=== SAVING SORTED DATASETS ===")
    
    sort_columns = [
        ("mapid", "/tmp/sorted_by_mapid"),
        ("playlist_id", "/tmp/sorted_by_playlist"),
        ("player_gamertag", "/tmp/sorted_by_player_gamertag"),
        ("match_id", "/tmp/sorted_by_match_id"),
        ("medal_id", "/tmp/sorted_by_medal_id")
    ]
    
    results = []
    for sort_col, output_path in sort_columns:
        print(f"Sorting by {sort_col}...")
        
        # Sort within partitions for better performance
        sorted_df = match_player_table.sortWithinPartitions(sort_col)
        
        # Write to parquet
        sorted_df.write.mode("overwrite").parquet(output_path)
        
        # Calculate file size
        file_size = get_directory_size(output_path)
        results.append((sort_col, output_path, file_size))
        
        print(f"✓ {sort_col}: {output_path} ({file_size} MB)")
    
    return results

def run_comprehensive_analysis():
    """Run the complete gaming data analysis pipeline"""
    
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        print("Starting Gaming Data Analysis Pipeline...")
        print("=" * 50)
        
        # Step 1: Load and inspect data
        print("\n1. Loading and inspecting datasets...")
        dfs = load_and_inspect_data(spark)
        
        # Step 2: Setup optimized tables
        print("\n2. Setting up optimized tables...")
        setup_optimized_tables(spark, dfs)
        
        # Step 3: Create main analysis table
        print("\n3. Creating main analysis table...")
        match_player_table = create_main_analysis_table(spark)
        
        # Step 4: Run analyses
        print("\n4. Running performance analyses...")
        
        # Player performance analysis
        top_players = analyze_player_performance(spark)
        
        # Playlist popularity analysis
        playlist_plays, playlist_engagement = analyze_playlist_popularity(spark)
        
        # Map popularity analysis  
        map_plays, map_engagement = analyze_map_popularity(spark)
        
        # Killing spree analysis
        killing_spree_players, killing_spree_maps = analyze_killing_spree_medals(spark)
        
        # Step 5: Save optimized datasets
        print("\n5. Saving sorted datasets for future use...")
        sort_results = save_sorted_datasets(spark, match_player_table)
        
        print("\n" + "=" * 50)
        print("ANALYSIS COMPLETE!")
        print("=" * 50)
        
        # Summary
        print("\nFile size summary:")
        for sort_col, path, size in sort_results:
            print(f"  {sort_col}: {size} MB")
            
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        raise
    finally:
        # Clean up
        spark.stop()

# Alternative: Run individual components
def run_individual_analysis():
    """Alternative approach - run components individually for debugging"""
    
    spark = create_spark_session()
    
    # Just run the data loading and setup
    dfs = load_and_inspect_data(spark)
    setup_optimized_tables(spark, dfs)
    match_player_table = create_main_analysis_table(spark)
    
    # Run specific analysis
    analyze_player_performance(spark)
    
    return spark, match_player_table

# Uncomment to run:
# run_comprehensive_analysis()
# 
# OR for individual testing:
# spark, match_player_table = run_individual_analysis()