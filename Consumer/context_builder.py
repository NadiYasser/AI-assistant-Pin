from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, coalesce
from pyspark.sql.types import *

# -------------------------------
# 1. Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("MultimodalContextAggregator") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# 2. Schemas
# -------------------------------
video_schema = StructType([
    StructField("type", StringType()),
    StructField("timestamp", LongType()),
    StructField("description", StringType()),
    StructField("confidence", DoubleType())
])

audio_schema = StructType([
    StructField("type", StringType()),
    StructField("timestamp", LongType()),
    StructField("transcript", StringType()),
    StructField("confidence", DoubleType())
])

location_schema = StructType([
    StructField("type", StringType()),
    StructField("timestamp", LongType()),
    StructField("raw", ArrayType(DoubleType())),
    StructField("semantic", StringType()),
    StructField("confidence", DoubleType())
])

# -------------------------------
# 3. Read Kafka Streams
# -------------------------------
def read_kafka(topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

video_raw = read_kafka("video_stream")
audio_raw = read_kafka("audio_stream")
location_raw = read_kafka("location_stream")

# -------------------------------
# 4. Parse JSON
# -------------------------------
video_df = video_raw.select(
    from_json(col("value").cast("string"), video_schema).alias("data")
).select("data.*")

audio_df = audio_raw.select(
    from_json(col("value").cast("string"), audio_schema).alias("data")
).select("data.*")

location_df = location_raw.select(
    from_json(col("value").cast("string"), location_schema).alias("data")
).select("data.*")

# -------------------------------
# -------------------------------
# 7. Alias DataFrames
# -------------------------------
v = video_df.alias("v")
a = audio_df.alias("a")
l = location_df.alias("l")

# -------------------------------
# 8. FULL OUTER JOINS
# -------------------------------
va = v.join(
    a,
    v.ts_bucket == a.ts_bucket,
    "full_outer"
)

val = va.join(
    l,
    coalesce(v.ts_bucket, a.ts_bucket) == l.ts_bucket,
    "full_outer"
)

# -------------------------------
# 9. Build Context Object
# -------------------------------
context_df = val.select(
    coalesce(v.ts_bucket, a.ts_bucket, l.ts_bucket).alias("timestamp"),

    struct(
        v.description.alias("description"),
        v.confidence.alias("confidence")
    ).alias("video"),

    struct(
        a.transcript.alias("transcript"),
        a.confidence.alias("confidence")
    ).alias("audio"),

    struct(
        l.raw.alias("raw"),
        l.semantic.alias("semantic"),
        l.confidence.alias("confidence")
    ).alias("location")
)

# -------------------------------
# 10. Output (Console for now)
# -------------------------------
query = context_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
