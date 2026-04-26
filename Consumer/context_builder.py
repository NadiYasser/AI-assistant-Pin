import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    coalesce,
    col,
    current_timestamp,
    date_format,
    expr,
    first,
    from_json,
    lit,
    struct,
    to_timestamp,
)
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BUCKET_SECONDS = 15
WATERMARK_DELAY = "30 seconds"
USER_ID = os.getenv("CONTEXT_USER_ID", "user_001")


VISION_SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("objects", ArrayType(StringType()), True),
    StructField("scene_description", StringType(), True),
    StructField("confidence", DoubleType(), True),
    StructField("media_ref", StringType(), True),
])

SPEECH_SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("transcript", StringType(), True),
    StructField("keywords", ArrayType(StringType()), True),
    StructField("confidence", DoubleType(), True),
    StructField("audio_ref", StringType(), True),
])

GPS_SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("place_label", StringType(), True),
    StructField("zone_type", StringType(), True),
])


spark = SparkSession.builder.appName("MultimodalContextAggregator").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def read_kafka_topic(topic_name, schema):
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic_name)
        .option("startingOffsets", "latest")
        .load()
    )

    return raw_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")


def add_bucket_columns(df):
    return (
        df.withColumn(
            "event_time",
            to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
        )
        .withColumn(
            "bucket_ts",
            expr(
                f"CAST(UNIX_TIMESTAMP(event_time) - "
                f"(UNIX_TIMESTAMP(event_time) % {BUCKET_SECONDS}) AS BIGINT)"
            ),
        )
        .withWatermark("event_time", WATERMARK_DELAY)
    )


vision_df = add_bucket_columns(read_kafka_topic("video_stream", VISION_SCHEMA)).select(
    col("bucket_ts"),
    col("event_time"),
    col("timestamp").alias("vision_timestamp"),
    col("objects").alias("vision_objects"),
    col("scene_description").alias("vision_scene_description"),
    col("confidence").alias("vision_confidence"),
    col("media_ref").alias("vision_media_ref"),
    lit(None).cast(StringType()).alias("audio_timestamp"),
    lit(None).cast(StringType()).alias("audio_transcript"),
    lit(None).cast(ArrayType(StringType())).alias("audio_keywords"),
    lit(None).cast(DoubleType()).alias("audio_confidence"),
    lit(None).cast(StringType()).alias("audio_ref"),
    lit(None).cast(StringType()).alias("location_timestamp"),
    lit(None).cast(DoubleType()).alias("location_latitude"),
    lit(None).cast(DoubleType()).alias("location_longitude"),
    lit(None).cast(StringType()).alias("location_place_label"),
    lit(None).cast(StringType()).alias("location_zone_type"),
)

speech_df = add_bucket_columns(read_kafka_topic("audio_stream", SPEECH_SCHEMA)).select(
    col("bucket_ts"),
    col("event_time"),
    lit(None).cast(StringType()).alias("vision_timestamp"),
    lit(None).cast(ArrayType(StringType())).alias("vision_objects"),
    lit(None).cast(StringType()).alias("vision_scene_description"),
    lit(None).cast(DoubleType()).alias("vision_confidence"),
    lit(None).cast(StringType()).alias("vision_media_ref"),
    col("timestamp").alias("audio_timestamp"),
    col("transcript").alias("audio_transcript"),
    col("keywords").alias("audio_keywords"),
    col("confidence").alias("audio_confidence"),
    col("audio_ref").alias("audio_ref"),
    lit(None).cast(StringType()).alias("location_timestamp"),
    lit(None).cast(DoubleType()).alias("location_latitude"),
    lit(None).cast(DoubleType()).alias("location_longitude"),
    lit(None).cast(StringType()).alias("location_place_label"),
    lit(None).cast(StringType()).alias("location_zone_type"),
)

gps_df = add_bucket_columns(read_kafka_topic("location_stream", GPS_SCHEMA)).select(
    col("bucket_ts"),
    col("event_time"),
    lit(None).cast(StringType()).alias("vision_timestamp"),
    lit(None).cast(ArrayType(StringType())).alias("vision_objects"),
    lit(None).cast(StringType()).alias("vision_scene_description"),
    lit(None).cast(DoubleType()).alias("vision_confidence"),
    lit(None).cast(StringType()).alias("vision_media_ref"),
    lit(None).cast(StringType()).alias("audio_timestamp"),
    lit(None).cast(StringType()).alias("audio_transcript"),
    lit(None).cast(ArrayType(StringType())).alias("audio_keywords"),
    lit(None).cast(DoubleType()).alias("audio_confidence"),
    lit(None).cast(StringType()).alias("audio_ref"),
    col("timestamp").alias("location_timestamp"),
    col("latitude").alias("location_latitude"),
    col("longitude").alias("location_longitude"),
    col("place_label").alias("location_place_label"),
    col("zone_type").alias("location_zone_type"),
)


merged_df = vision_df.unionByName(speech_df).unionByName(gps_df)


context_df = (
    merged_df.groupBy("bucket_ts")
    .agg(
        first("vision_timestamp", ignorenulls=True).alias("vision_timestamp"),
        first("vision_objects", ignorenulls=True).alias("vision_objects"),
        first("vision_scene_description", ignorenulls=True).alias("vision_scene_description"),
        first("vision_confidence", ignorenulls=True).alias("vision_confidence"),
        first("vision_media_ref", ignorenulls=True).alias("vision_media_ref"),
        first("audio_timestamp", ignorenulls=True).alias("audio_timestamp"),
        first("audio_transcript", ignorenulls=True).alias("audio_transcript"),
        first("audio_keywords", ignorenulls=True).alias("audio_keywords"),
        first("audio_confidence", ignorenulls=True).alias("audio_confidence"),
        first("audio_ref", ignorenulls=True).alias("audio_ref"),
        first("location_timestamp", ignorenulls=True).alias("location_timestamp"),
        first("location_latitude", ignorenulls=True).alias("location_latitude"),
        first("location_longitude", ignorenulls=True).alias("location_longitude"),
        first("location_place_label", ignorenulls=True).alias("location_place_label"),
        first("location_zone_type", ignorenulls=True).alias("location_zone_type"),
    )
    .select(
        expr("concat('ctx_', lpad(cast(bucket_ts as string), 12, '0'))").alias("context_id"),
        lit(USER_ID).alias("user_id"),
        date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss").alias("created_at"),
        struct(
            col("vision_timestamp").alias("timestamp"),
            coalesce(col("vision_objects"), array().cast(ArrayType(StringType()))).alias("objects"),
            col("vision_scene_description").alias("scene_description"),
            col("vision_confidence").alias("confidence"),
            col("vision_media_ref").alias("media_ref"),
        ).alias("vision"),
        struct(
            col("audio_timestamp").alias("timestamp"),
            col("audio_transcript").alias("transcript"),
            coalesce(col("audio_keywords"), array().cast(ArrayType(StringType()))).alias("keywords"),
            col("audio_confidence").alias("confidence"),
            col("audio_ref").alias("audio_ref"),
        ).alias("audio"),
        struct(
            col("location_timestamp").alias("timestamp"),
            col("location_latitude").alias("latitude"),
            col("location_longitude").alias("longitude"),
            col("location_place_label").alias("place_label"),
            col("location_zone_type").alias("zone_type"),
        ).alias("location"),
    )
)


query = (
    context_df.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
