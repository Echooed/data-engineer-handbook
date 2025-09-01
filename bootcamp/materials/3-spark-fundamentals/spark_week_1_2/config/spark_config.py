"""Spark configuration settings"""

SPARK_CONFIG = {
    "spark.app.name": "Gaming Analytics Migration",
    "spark.master": "local[4]",
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.sources.bucketing.enabled": "true",
    "spark.sql.sources.bucketing.autoBucketedScan.enabled": "true",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
}

# Database settings
DATABASE_NAME = "gaming_analytics"
TABLE_PREFIX = "gaming"
