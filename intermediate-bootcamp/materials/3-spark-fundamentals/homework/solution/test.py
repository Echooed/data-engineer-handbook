import sys
import os
import pytest
from pyspark.sql.functions import to_timestamp

# --- Make sure project root is in sys.path ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.append(project_root)

from src.jobs.job1_simple import create_spark_session, compute_daily_active_users


# --- Simple unit test ---
def test_daily_active_users_simple():
    spark = create_spark_session()

    # Fake events DataFrame
    events = spark.createDataFrame(
        [
            ("u1", "2023-01-01 10:00:00"),
            ("u2", "2023-01-01 11:00:00"),
            ("u1", "2023-01-02 09:00:00"),
        ],
        ["user_id", "event_time"]
    ).withColumn("event_time", to_timestamp("event_time"))

    # Run job logic
    result = compute_daily_active_users(spark, events)

    # Collect as dict for easy check
    output = {row["event_date"]: row["daily_active_users"] for row in result.collect()}

    assert output["2023-01-01"] == 2
    assert output["2023-01-02"] == 1

# show table in warehouse
spark.sql("SHOW TABLES").show()

# The optimized join strategy with aliases
optimized_join = bucketed_match_details.alias("md") \
    .join(bucketed_matches.alias("m"), "match_id") \
    .join(bucketed_medals_matches_players.alias("mmp"), ["match_id", "player_gamertag"], "left")

optimized_join.show(5)