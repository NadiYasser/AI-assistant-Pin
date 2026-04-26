import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, current_timestamp, date_format, expr, from_json, struct, to_timestamp
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BUCKET_SECONDS = 15
WATERMARK_DELAY = "30 seconds"

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
    StructField("source", StringType()),
    StructField("timestamp", StringType()),
    StructField("objects", ArrayType(StringType())),
    StructField("scene_description", StringType()),
    StructField("confidence", DoubleType()),
    StructField("media_ref", StringType()),
])

audio_schema = StructType([
    StructField("source", StringType()),
    StructField("timestamp", StringType()),
    StructField("transcript", StringType()),
    StructField("keywords", ArrayType(StringType())),
    StructField("confidence", DoubleType()),
    StructField("audio_ref", StringType()),
])

location_schema = StructType([
    StructField("source", StringType()),
    StructField("timestamp", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("place_label", StringType()),
    StructField("zone_type", StringType()),
])

# -------------------------------
# 3. Read Kafka Streams
# -------------------------------
def read_kafka(topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
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
# 5. Normalize timestamp (15s bucket)
# -------------------------------
def add_bucket(df):
    return df.withColumn(
        "event_time",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
    ).withColumn(
        "ts_bucket",
        expr(f"CAST(UNIX_TIMESTAMP(event_time) - (UNIX_TIMESTAMP(event_time) % {BUCKET_SECONDS}) AS BIGINT)")
    )

video_df = add_bucket(video_df)
audio_df = add_bucket(audio_df)
location_df = add_bucket(location_df)

# -------------------------------
# 6. Add Watermarks (handle late data)
# -------------------------------
video_df = video_df.withWatermark("event_time", WATERMARK_DELAY)
audio_df = audio_df.withWatermark("event_time", WATERMARK_DELAY)
location_df = location_df.withWatermark("event_time", WATERMARK_DELAY)

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
    v.event_time == a.event_time,
    "full_outer"
)

va = va.select(
    coalesce(col("v.ts_bucket"), col("a.ts_bucket")).alias("ts_bucket"),
    coalesce(col("v.event_time"), col("a.event_time")).alias("event_time"),
    col("v.timestamp").alias("vision_timestamp"),
    col("v.objects").alias("vision_objects"),
    col("v.scene_description").alias("vision_scene_description"),
    col("v.confidence").alias("video_confidence"),
    col("v.media_ref").alias("vision_media_ref"),
    col("a.timestamp").alias("audio_timestamp"),
    col("a.transcript").alias("audio_transcript"),
    col("a.keywords").alias("audio_keywords"),
    col("a.confidence").alias("audio_confidence"),
    col("a.audio_ref").alias("audio_ref"),
)

val = va.join(
    l,
    va.event_time == l.event_time,
    "full_outer"
)

# -------------------------------
# 9. Build Context Object
# -------------------------------
context_df = val.select(
    expr("concat('ctx_', lpad(cast(coalesce(va.ts_bucket, l.ts_bucket) as string), 12, '0'))").alias("context_id"),
    expr("'user_001'").alias("user_id"),
    date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss").alias("created_at"),

    struct(
        va.vision_timestamp.alias("timestamp"),
        va.vision_objects.alias("objects"),
        va.vision_scene_description.alias("scene_description"),
        va.video_confidence.alias("confidence"),
        va.vision_media_ref.alias("media_ref")
    ).alias("vision"),

    struct(
        va.audio_timestamp.alias("timestamp"),
        va.audio_transcript.alias("transcript"),
        va.audio_keywords.alias("keywords"),
        va.audio_confidence.alias("confidence"),
        va.audio_ref.alias("audio_ref")
    ).alias("audio"),

    struct(
        l.timestamp.alias("timestamp"),
        l.latitude.alias("latitude"),
        l.longitude.alias("longitude"),
        l.place_label.alias("place_label"),
        l.zone_type.alias("zone_type")
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
