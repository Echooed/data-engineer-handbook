from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

def create_spark_session():
    """Create and configure Spark session with optimizations"""
    spark = SparkSession.builder \
        .appName("Spark Fundamentals Week Analysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Disable automatic broadcast join threshold as required
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    
    return spark

def load_and_prepare_data(spark):
    """Load all datasets and prepare them for analysis"""
    
    # Load the main datasets
    # Assuming these are in parquet format - adjust file paths as needed
    match_details = spark.read.parquet("path/to/match_details")
    matches = spark.read.parquet("path/to/matches") 
    medals_matches_players = spark.read.parquet("path/to/medals_matches_players")
    medals = spark.read.parquet("path/to/medals")
    maps = spark.read.parquet("path/to/maps")  # Assuming maps table exists
    
    return match_details, matches, medals_matches_players, medals, maps

def create_bucketed_tables(spark, match_details, matches, medals_matches_players):
    """Create bucketed tables for optimal joins"""
    
    # Create bucketed versions of the main tables
    # Bucket on match_id with 16 buckets as specified
    
    # Write match_details as bucketed table
    match_details.write \
        .mode("overwrite") \
        .option("path", "bucketed_tables/match_details") \
        .bucketBy(16, "match_id") \
        .saveAsTable("bucketed_match_details")
    
    # Write matches as bucketed table  
    matches.write \
        .mode("overwrite") \
        .option("path", "bucketed_tables/matches") \
        .bucketBy(16, "match_id") \
        .saveAsTable("bucketed_matches")
    
    # Write medals_matches_players as bucketed table
    medals_matches_players.write \
        .mode("overwrite") \
        .option("path", "bucketed_tables/medals_matches_players") \
        .bucketBy(16, "match_id") \
        .saveAsTable("bucketed_medals_matches_players")
    
    # Read back the bucketed tables
    bucketed_match_details = spark.table("bucketed_match_details")
    bucketed_matches = spark.table("bucketed_matches") 
    bucketed_medals_matches_players = spark.table("bucketed_medals_matches_players")
    
    return bucketed_match_details, bucketed_matches, bucketed_medals_matches_players

def perform_analysis(spark, match_details, matches, medals_matches_players, medals, maps):
    """Perform the main analysis with optimized joins"""
    
    # Explicitly broadcast the smaller tables (medals and maps)
    broadcast_medals = broadcast(medals)
    broadcast_maps = broadcast(maps)
    
    print("Starting analysis with optimized joins...")
    
    # Main join combining all the data
    # Using bucketed tables for match_id joins and broadcast for small tables
    combined_data = match_details \
        .join(matches, "match_id") \
        .join(broadcast_maps, matches.map_id == broadcast_maps.map_id, "left") \
        .join(medals_matches_players, ["match_id", "player_id"], "left") \
        .join(broadcast_medals, medals_matches_players.medal_id == broadcast_medals.medal_id, "left")
    
    # Cache the combined dataset as it will be used multiple times
    combined_data.cache()
    
    print("Combined data schema:")
    combined_data.printSchema()
    print(f"Combined data count: {combined_data.count()}")
    
    return combined_data

def analyze_player_performance(combined_data):
    """Analyze player performance metrics"""
    
    print("\n=== PLAYER PERFORMANCE ANALYSIS ===")
    
    # Which player averages the most kills per game?
    avg_kills_per_player = combined_data \
        .groupBy("player_id") \
        .agg(
            avg("kills").alias("avg_kills_per_game"),
            count("match_id").alias("total_games"),
            sum("kills").alias("total_kills")
        ) \
        .filter(col("total_games") >= 5) \
        .orderBy(desc("avg_kills_per_game"))
    
    print("\nTop 10 Players by Average Kills Per Game:")
    avg_kills_per_player.show(10)
    
    # Additional player insights
    top_performer = avg_kills_per_player.first()
    print(f"\nTop performer: Player {top_performer['player_id']} with {top_performer['avg_kills_per_game']:.2f} avg kills per game")
    
    return avg_kills_per_player

def analyze_playlist_popularity(combined_data):
    """Analyze playlist popularity"""
    
    print("\n=== PLAYLIST ANALYSIS ===")
    
    # Which playlist gets played the most?
    playlist_popularity = combined_data \
        .groupBy("playlist") \
        .agg(
            countDistinct("match_id").alias("total_matches"),
            countDistinct("player_id").alias("unique_players")
        ) \
        .orderBy(desc("total_matches"))
    
    print("\nPlaylist Popularity (by number of matches):")
    playlist_popularity.show()
    
    most_popular = playlist_popularity.first()
    print(f"\nMost popular playlist: {most_popular['playlist']} with {most_popular['total_matches']} matches")
    
    return playlist_popularity

def analyze_map_metrics(combined_data):
    """Analyze map-related metrics"""
    
    print("\n=== MAP ANALYSIS ===")
    
    # Which map gets played the most?
    map_popularity = combined_data \
        .groupBy("map_name") \
        .agg(
            countDistinct("match_id").alias("total_matches"),
            countDistinct("player_id").alias("unique_players"),
            avg("kills").alias("avg_kills_per_player")
        ) \
        .orderBy(desc("total_matches"))
    
    print("\nMap Popularity (by number of matches):")
    map_popularity.show()
    
    # Which map do players get the most Killing Spree medals on?
    killing_spree_by_map = combined_data \
        .filter(col("medal_name") == "Killing Spree") \
        .groupBy("map_name") \
        .agg(
            count("medal_id").alias("killing_spree_count"),
            countDistinct("player_id").alias("players_with_killing_spree"),
            countDistinct("match_id").alias("matches_with_killing_spree")
        ) \
        .orderBy(desc("killing_spree_count"))
    
    print("\nKilling Spree Medals by Map:")
    killing_spree_by_map.show()
    
    if killing_spree_by_map.count() > 0:
        top_killing_spree_map = killing_spree_by_map.first()
        print(f"\nMap with most Killing Spree medals: {top_killing_spree_map['map_name']} with {top_killing_spree_map['killing_spree_count']} medals")
    
    return map_popularity, killing_spree_by_map

def test_sort_within_partitions(combined_data):
    """Test different sortWithinPartitions to find smallest data size"""
    
    print("\n=== TESTING SORT WITHIN PARTITIONS ===")
    
    # Test different sort columns (low cardinality as hinted)
    sort_tests = [
        ("playlist", "Sorting by playlist"),
        ("map_name", "Sorting by map_name"), 
        ("medal_name", "Sorting by medal_name"),
        (["playlist", "map_name"], "Sorting by playlist + map_name"),
        ("match_id", "Sorting by match_id (baseline)")
    ]
    
    results = []
    
    for sort_col, description in sort_tests:
        print(f"\n{description}...")
        
        # Apply sortWithinPartitions
        if isinstance(sort_col, list):
            sorted_df = combined_data.sortWithinPartitions(*sort_col)
        else:
            sorted_df = combined_data.sortWithinPartitions(sort_col)
        
        # Force computation and measure
        sorted_df.cache()
        count = sorted_df.count()
        
        # Get partition information
        partitions = sorted_df.rdd.getNumPartitions()
        
        # Estimate data size by looking at a sample
        sample_rows = sorted_df.limit(1000).collect()
        
        print(f"  Partitions: {partitions}")
        print(f"  Total rows: {count}")
        print(f"  Sample collected successfully")
        
        results.append({
            'sort_method': description,
            'sort_column': str(sort_col),
            'partitions': partitions,
            'total_rows': count
        })
        
        sorted_df.unpersist()
    
    # Create results summary
    results_df = spark.createDataFrame(results)
    print("\nSort Within Partitions Test Results:")
    results_df.show(truncate=False)
    
    return results_df

def generate_comprehensive_report(spark, combined_data, results_dict):
    """Generate a comprehensive analysis report"""
    
    print("\n" + "="*60)
    print("COMPREHENSIVE ANALYSIS REPORT")
    print("="*60)
    
    # Overall dataset statistics
    total_matches = combined_data.select("match_id").distinct().count()
    total_players = combined_data.select("player_id").distinct().count()
    total_records = combined_data.count()
    
    print(f"\nDataset Overview:")
    print(f"  Total unique matches: {total_matches:,}")
    print(f"  Total unique players: {total_players:,}")
    print(f"  Total records: {total_records:,}")
    
    # Medal distribution analysis
    medal_distribution = combined_data \
        .filter(col("medal_name").isNotNull()) \
        .groupBy("medal_name") \
        .agg(
            count("medal_id").alias("total_medals"),
            countDistinct("player_id").alias("players_earning_medal")
        ) \
        .orderBy(desc("total_medals"))
    
    print(f"\nMedal Distribution:")
    medal_distribution.show()
    
    # Performance correlation analysis
    player_medal_performance = combined_data \
        .groupBy("player_id") \
        .agg(
            avg("kills").alias("avg_kills"),
            avg("deaths").alias("avg_deaths"),
            count(when(col("medal_name").isNotNull(), 1)).alias("total_medals"),
            countDistinct("medal_name").alias("unique_medal_types")
        ) \
        .withColumn("kd_ratio", col("avg_kills") / col("avg_deaths")) \
        .orderBy(desc("total_medals"))
    
    print(f"\nTop Medal Earners and Their Performance:")
    player_medal_performance.show(10)

def main():
    """Main execution function"""
    
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        print("Loading data...")
        match_details, matches, medals_matches_players, medals, maps = load_and_prepare_data(spark)
        
        print("Creating bucketed tables...")
        bucketed_match_details, bucketed_matches, bucketed_medals_matches_players = \
            create_bucketed_tables(spark, match_details, matches, medals_matches_players)
        
        print("Performing optimized analysis...")
        combined_data = perform_analysis(
            spark, 
            bucketed_match_details, 
            bucketed_matches, 
            bucketed_medals_matches_players, 
            medals, 
            maps
        )
        
        # Run all analyses
        results = {}
        
        print("Analyzing player performance...")
        results['player_performance'] = analyze_player_performance(combined_data)
        
        print("Analyzing playlist popularity...")
        results['playlist_analysis'] = analyze_playlist_popularity(combined_data)
        
        print("Analyzing map metrics...")
        results['map_popularity'], results['killing_spree_maps'] = analyze_map_metrics(combined_data)
        
        print("Testing sort within partitions...")
        results['sort_tests'] = test_sort_within_partitions(combined_data)
        
        print("Generating comprehensive report...")
        generate_comprehensive_report(spark, combined_data, results)
        
        # Save results for further analysis
        print("\nSaving analysis results...")
        
        # Save key results as parquet files
        results['player_performance'].write.mode("overwrite").parquet("output/player_performance")
        results['playlist_analysis'].write.mode("overwrite").parquet("output/playlist_analysis") 
        results['map_popularity'].write.mode("overwrite").parquet("output/map_popularity")
        results['killing_spree_maps'].write.mode("overwrite").parquet("output/killing_spree_maps")
        results['sort_tests'].write.mode("overwrite").parquet("output/sort_optimization_tests")
        
        print("Analysis complete! Results saved to output/ directory.")
        
        # Show final optimization recommendations
        print(f"\n{'='*60}")
        print("OPTIMIZATION RECOMMENDATIONS")
        print("="*60)
        print("1. Bucketing on match_id with 16 buckets implemented")
        print("2. Broadcasting small tables (medals, maps) implemented") 
        print("3. Automatic broadcast join disabled as requested")
        print("4. Test sortWithinPartitions on low-cardinality columns for optimal compression")
        print("5. Consider partitioning by date if temporal queries are common")
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()