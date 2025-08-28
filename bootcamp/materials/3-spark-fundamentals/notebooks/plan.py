def load_all_data(spark, url, properties):
    """Load all required datasets"""
    
    print("Loading all datasets...")
    
    # Load NBA data from JDBC
    print("Loading NBA game details from database...")
    df_game_details = spark.read.jdbc(url=url, table="game_details", properties=properties)
    
    # Load gaming datasets from CSV files
    print("Loading gaming datasets from CSV files...")
    
    # Load maps data
    df_maps = spark.read.csv(
        "Echooed/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data/maps.csv",
        header=True,
        inferSchema=True
    )
    
    # Load other gaming datasets (update paths as needed)
    try:
        df_match_details = spark.read.csv(
            "Echooed/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data/match_details.csv",
            header=True,
            inferSchema=True
        )
        
        df_matches = spark.read.csv(
            "Echooed/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data/matches.csv",
            header=True,
            inferSchema=True
        )
        
        df_medals_matches_players = spark.read.csv(
            "Echooed/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data/medals_matches_players.csv",
            header=True,
            inferSchema=True
        )
        
        df_medals = spark.read.csv(
            "Echooed/data-engineer-handbook/bootcamp/materials/3-spark-fundamentals/data/medals.csv",
            header=True,
            inferSchema=True
        )
        
        print("All gaming datasets loaded successfully!")
        gaming_data_available = True
        
    except Exception as e:
        print(f"Gaming datasets not available: {e}")
        print("Will proceed with NBA analysis only")
        gaming_data_available = False
        df_match_details = None
        df_matches = None
        df_medals_matches_players = None
        df_medals = None
    
    return {
        'game_details': df_game_details,
        'maps': df_maps,
        'match_details': df_match_details,
        'matches': df_matches,
        'medals_matches_players': df_medals_matches_players,
        'medals': df_medals,
        'gaming_data_available': gaming_data_available
    }