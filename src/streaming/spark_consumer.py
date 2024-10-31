from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

class StockDataStreamProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("StockStreamProcessor") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
            .getOrCreate()
        
        self.logger = logging.getLogger(__name__)

    def create_streaming_query(self, kafka_topic):
        """Create and process streaming data from Kafka."""
        
        # Define schema for JSON data
        schema = StructType([
            StructField("symbol", StringType()),
            StructField("timestamp", StringType()),
            StructField("price", DoubleType()),
            StructField("volume", LongType()),
            StructField("high", DoubleType()),
            StructField("low", DoubleType()),
            StructField("change_percent", DoubleType())
        ])

        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", kafka_topic) \
            .load()

        # Parse JSON data
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        # Calculate real-time analytics
        windowed_stats = parsed_df \
            .withWatermark("timestamp", "1 minute") \
            .groupBy(
                window("timestamp", "5 minutes"),
                "symbol"
            ) \
            .agg(
                avg("price").alias("avg_price"),
                max("high").alias("period_high"),
                min("low").alias("period_low"),
                sum("volume").alias("total_volume"),
                avg("change_percent").alias("avg_change_percent")
            )

        return windowed_stats