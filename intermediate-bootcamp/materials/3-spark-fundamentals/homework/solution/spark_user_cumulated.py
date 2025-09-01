from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# 1 Create Spark session
def create_spark_session(app_name="job1_daily_dau"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    return spark

# 2 Compute daily active users from an events DataFrame or registered table
def compute_daily_active_users(spark, events_df=None, days=None, start_date=None):
    # register dataframe as a temp view
    if events_df is not None:
        events_df.createOrReplaceTempView("events_temp")
    else:
        # expect a table named 'events' if no dataframe provided
        spark.catalog.refreshTable("events")
        spark.sql("CREATE OR REPLACE TEMP VIEW events_temp AS SELECT * FROM events")

    # build filter clause
    if start_date:
        date_filter = f"DATE(event_time) >= DATE '{start_date}'"
    elif days is not None:
        date_filter = f"event_time >= DATE_SUB(CURRENT_DATE(), {int(days)})"
    else:
        date_filter = "1=1"

    # aggregation base
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW dau_base AS
        SELECT
            DATE(event_time) AS event_date,
            COUNT(DISTINCT user_id) AS daily_active_users,
            COUNT(*) AS total_events,
            COUNT(DISTINCT device_id) AS unique_devices
        FROM events_temp
        WHERE {date_filter} AND user_id IS NOT NULL
        GROUP BY DATE(event_time)
    """)

    # rolling 7-day average over total_events
    result_df = spark.sql("""
        SELECT
            event_date,
            daily_active_users,
            total_events,
            unique_devices,
            ROUND(AVG(total_events) OVER (
                ORDER BY event_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 2) AS seven_day_avg_events
        FROM dau_base
        ORDER BY event_date DESC
    """)

    return result_df

# 3 Save results to parquet path
def save_results(df, output_path):
    df.write.mode("overwrite").parquet(output_path)

# 4 Runner helper to run end-to-end
def run(events_path=None, output_path="output/daily_active_users", days=7, start_date=None):
    spark = create_spark_session()
    try:
        if events_path:
            events_df = spark.read.parquet(events_path)
            # ensure event_time is timestamp
            events_df = events_df.withColumn("event_time", to_timestamp("event_time"))
            result = compute_daily_active_users(spark, events_df=events_df, days=days, start_date=start_date)
        else:
            result = compute_daily_active_users(spark, events_df=None, days=days, start_date=start_date)

        save_results(result, output_path)
        return result
    finally:
        spark.stop()
