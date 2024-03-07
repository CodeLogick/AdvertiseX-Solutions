from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder.appName("AdvertiseXDataProcessing").master('local').getOrCreate()

# Read data from Kafka topics
ad_impressions_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "your_kafka_brokers").option("subscribe", "ad_impressions_topic").load()
click_conversion_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "your_kafka_brokers").option("subscribe", "click_conversion_topic").load()

# Transform and correlate data
ad_impressions_df = ad_impressions_stream.selectExpr("CAST(value AS STRING)").select(col("value").cast("json"))
click_conversion_df = click_conversion_stream.selectExpr("CAST(value AS STRING)").select(col("value").cast("csv"))

joined_df = ad_impressions_df.join(click_conversion_df, "user_id", "left_outer")

# Perform additional transformations and write to storage with Error Handling
try:
    query = joined_df.writeStream.format("parquet").option("path", "hdfs://your_s3_path").start()
    query.awaitTermination()
    logger.info("Data processing completed successfully")
except Exception as e:
    logger.error(f"Error in data processing: {e}")
finally:
    query.stop()
