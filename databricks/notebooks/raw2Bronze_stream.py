from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType
)

# Source location
source_path = "/Volumes/workspace/default/wellness_volume"

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("steps", IntegerType(), True),
    StructField("heart_rate", IntegerType(), True),
    StructField("event_time", TimestampType(), True)
])

# Read incoming JSON events as a stream
bronze_df = (
    spark.readStream
    .format("json")
    .schema(schema)
    .load(source_path)
)

# Write raw data to Bronze
query = (
    bronze_df.writeStream
    .format("delta")
    .option(
        "checkpointLocation",
        "/Volumes/workspace/default/wellness_volume/checkpoints/wellness_bronze"
    )
    .trigger(availableNow=True)
    .toTable("workspace.default.wellness_bronze")
)

query.awaitTermination()