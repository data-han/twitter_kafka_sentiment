import os
import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, from_json, to_json, struct, expr, udf, lit, when,
    regexp_replace, to_timestamp, from_unixtime
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from textblob import TextBlob
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TwitterSentimentSparkPipeline:
    """Spark Structured Streaming pipeline for Twitter sentiment analysis."""
    
    def __init__(self):
        """Initialize the pipeline with configuration from environment variables."""
        load_dotenv()
        
        # Kafka configuration
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.input_topic = os.getenv('KAFKA_TOPIC', 'twitterdata')
        self.output_topic = os.getenv('KAFKA_OUTPUT_TOPIC', 'twitterdata-clean')
        
        # InfluxDB configuration (for future use)
        self.influx_token = os.getenv('influxdb_token')
        self.influx_org = os.getenv('influxdb_org')
        self.influx_bucket = os.getenv('influxdb_bucket')
        
        # Spark configuration
        self.checkpoint_location = os.getenv(
            'SPARK_CHECKPOINT_DIR', 
            '/tmp/spark-checkpoint'
        )
        self.app_name = os.getenv('SPARK_APP_NAME', 'TwitterSentimentAnalysis')
        
        # Initialize Spark session
        self.spark: Optional[SparkSession] = None
        
        logger.info("Pipeline configuration loaded")
    
    def _get_twitter_schema(self) -> StructType:
        """
        Define the schema for incoming Twitter data.
        
        Returns:
            StructType schema for Twitter JSON data
        """
        return StructType([
            StructField("id_str", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("text", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("user_name", StringType(), True),
            StructField("user_followers", StringType(), True),
            StructField("retweet_count", StringType(), True),
            StructField("favorite_count", StringType(), True),
            StructField("lang", StringType(), True),
            StructField("source", StringType(), True),
            StructField("hashtags", StringType(), True),
            StructField("is_retweet", StringType(), True),
            # Legacy fields for backward compatibility
            StructField("id", StringType(), True),
            StructField("truncated", StringType(), True),
            StructField("in_reply_to_status_id", StringType(), True),
            StructField("in_reply_to_user_id", StringType(), True),
            StructField("in_reply_to_screen_name", StringType(), True),
            StructField("user", StringType(), True),
            StructField("coordinates", StringType(), True),
            StructField("place", StringType(), True),
            StructField("quoted_status_id", StringType(), True),
            StructField("is_quote_status", StringType(), True),
            StructField("quoted_status", StringType(), True),
            StructField("retweeted_status", StringType(), True),
            StructField("quote_count", StringType(), True),
            StructField("reply_count", StringType(), True),
            StructField("entities", StringType(), True),
            StructField("extended_entities", StringType(), True),
            StructField("favorited", StringType(), True),
            StructField("retweeted", StringType(), True),
            StructField("possibly_sensitive", StringType(), True),
            StructField("filter_level", StringType(), True),
            StructField("matching_rules", StringType(), True),
            StructField("name", StringType(), True),
            StructField("timestamp_ms", StringType(), True)
        ])
    
    def _initialize_spark_session(self) -> None:
        """Initialize Spark session with optimized configurations."""
        try:
            self.spark = (SparkSession.builder
                         .appName(self.app_name)
                         .config("spark.jars.packages", 
                                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
                         .config("spark.sql.streaming.checkpointLocation", 
                                self.checkpoint_location)
                         .config("spark.streaming.stopGracefullyOnShutdown", "true")
                         .config("spark.sql.shuffle.partitions", "4")
                         .config("spark.sql.streaming.schemaInference", "false")
                         .getOrCreate())
            
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info(f"Spark session initialized: {self.app_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark session: {e}")
            raise
    
    def _preprocess_text(self, df: DataFrame) -> DataFrame:
        """
        Preprocess tweet text by removing URLs, special characters, mentions, etc.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Preprocessed DataFrame
        """
        try:
            # Filter out null text
            df = df.filter(col('text').isNotNull())
            
            # Remove URLs
            df = df.withColumn('text', regexp_replace('text', r'http\S+', ''))
            df = df.withColumn('text', regexp_replace('text', r'https\S+', ''))
            
            # Remove non-ASCII characters
            df = df.withColumn('text', regexp_replace('text', r'[^\x00-\x7F]+', ''))
            
            # Remove newlines and carriage returns
            df = df.withColumn('text', regexp_replace('text', r'[\n\r]', ' '))
            
            # Remove mentions (@username)
            df = df.withColumn('text', regexp_replace('text', r'@\w+', ''))
            
            # Remove hashtag symbols (keep the word)
            df = df.withColumn('text', regexp_replace('text', '#', ''))
            
            # Remove 'RT' (retweet indicator)
            df = df.withColumn('text', regexp_replace('text', r'\bRT\b', ''))
            
            # Remove extra colons
            df = df.withColumn('text', regexp_replace('text', ':', ''))
            
            # Remove multiple spaces
            df = df.withColumn('text', regexp_replace('text', r'\s+', ' '))
            
            # Trim leading/trailing spaces
            df = df.withColumn('text', regexp_replace('text', r'^\s+|\s+$', ''))
            
            # Clean source field (remove HTML tags)
            df = df.withColumn('source', regexp_replace('source', '<a href="', ''))
            df = df.withColumn('source', regexp_replace('source', r'".*?>', ''))
            df = df.withColumn('source', regexp_replace('source', '</a>', ''))
            
            # Filter out empty text after preprocessing
            df = df.filter(col('text') != '')
            
            logger.debug("Text preprocessing completed")
            return df
            
        except Exception as e:
            logger.error(f"Error in text preprocessing: {e}")
            raise
    
    @staticmethod
    def _calculate_polarity(text: str) -> float:
        """
        Calculate sentiment polarity using TextBlob.
        
        Args:
            text: Input text
            
        Returns:
            Polarity score (-1 to 1)
        """
        try:
            return float(TextBlob(text).sentiment.polarity)
        except Exception:
            return 0.0
    
    @staticmethod
    def _calculate_subjectivity(text: str) -> float:
        """
        Calculate text subjectivity using TextBlob.
        
        Args:
            text: Input text
            
        Returns:
            Subjectivity score (0 to 1)
        """
        try:
            return float(TextBlob(text).sentiment.subjectivity)
        except Exception:
            return 0.0
    
    def _perform_sentiment_analysis(self, df: DataFrame) -> DataFrame:
        """
        Perform sentiment analysis on tweet text.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with sentiment analysis results
        """
        try:
            # Register UDFs
            polarity_udf = udf(self._calculate_polarity, FloatType())
            subjectivity_udf = udf(self._calculate_subjectivity, FloatType())
            
            # Calculate polarity
            df = df.withColumn("polarity_v", polarity_udf(col("text")))
            
            # Classify sentiment based on polarity
            df = df.withColumn(
                'polarity',
                when(col('polarity_v') > 0, lit('Positive'))
                .when(col('polarity_v') == 0, lit('Neutral'))
                .otherwise(lit('Negative'))
            )
            
            # Calculate subjectivity
            df = df.withColumn("subjectivity_v", subjectivity_udf(col("text")))
            
            # Add subjectivity classification
            df = df.withColumn(
                'subjectivity',
                when(col('subjectivity_v') >= 0.5, lit('Subjective'))
                .otherwise(lit('Objective'))
            )
            
            logger.debug("Sentiment analysis completed")
            return df
            
        except Exception as e:
            logger.error(f"Error in sentiment analysis: {e}")
            raise
    
    def _read_from_kafka(self) -> DataFrame:
        """
        Read streaming data from Kafka.
        
        Returns:
            Streaming DataFrame from Kafka
        """
        try:
            df = (self.spark
                  .readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                  .option("subscribe", self.input_topic)
                  .option("startingOffsets", "latest")
                  .option("failOnDataLoss", "false")
                  .load())
            
            logger.info(f"Reading from Kafka topic: {self.input_topic}")
            return df
            
        except Exception as e:
            logger.error(f"Error reading from Kafka: {e}")
            raise
    
    def _parse_kafka_messages(self, df: DataFrame) -> DataFrame:
        """
        Parse Kafka messages and extract tweet data.
        
        Args:
            df: Raw Kafka DataFrame
            
        Returns:
            Parsed DataFrame with tweet data
        """
        try:
            # Cast key and value to string
            df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            
            # Parse JSON value
            schema = self._get_twitter_schema()
            df = df.withColumn("data", from_json(col("value"), schema))
            df = df.select("data.*")
            
            # Add timestamp with watermark
            df = df.withColumn(
                "ts",
                to_timestamp(from_unixtime(expr("timestamp_ms/1000")))
            ).withWatermark("ts", "10 seconds")
            
            # Handle missing timestamp (use current timestamp)
            df = df.withColumn(
                "ts",
                when(col("ts").isNull(), from_unixtime(expr("unix_timestamp()")))
                .otherwise(col("ts"))
            )
            
            logger.debug("Kafka messages parsed successfully")
            return df
            
        except Exception as e:
            logger.error(f"Error parsing Kafka messages: {e}")
            raise
    
    def _prepare_output_data(self, df: DataFrame) -> DataFrame:
        """
        Prepare data for output to Kafka.
        
        Args:
            df: Processed DataFrame
            
        Returns:
            DataFrame formatted for Kafka output
        """
        try:
            # Select relevant fields for output
            output_df = df.select(
                to_json(struct("id_str")).alias('key'),
                to_json(struct(
                    'text', 'lang', 'ts', 
                    'polarity_v', 'polarity', 
                    'subjectivity_v', 'subjectivity',
                    'user_name', 'retweet_count', 'favorite_count'
                )).alias("value")
            )
            
            # Cast to string for Kafka
            output_df = output_df.selectExpr(
                "CAST(key AS STRING)", 
                "CAST(value AS STRING)"
            )
            
            logger.debug("Output data prepared")
            return output_df
            
        except Exception as e:
            logger.error(f"Error preparing output data: {e}")
            raise
    
    def _write_to_kafka(self, df: DataFrame) -> None:
        """
        Write processed data to Kafka output topic.
        
        Args:
            df: DataFrame to write
        """
        try:
            query = (df
                    .writeStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers)
                    .option("topic", self.output_topic)
                    .option("checkpointLocation", f"{self.checkpoint_location}/kafka")
                    .outputMode("append")
                    .start())
            
            logger.info(f"Writing to Kafka topic: {self.output_topic}")
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error writing to Kafka: {e}")
            raise
    
    def _write_to_console(self, df: DataFrame) -> None:
        """
        Write processed data to console for debugging.
        
        Args:
            df: DataFrame to write
        """
        try:
            query = (df
                    .writeStream
                    .outputMode("append")
                    .format("console")
                    .option("truncate", "false")
                    .option("numRows", 10)
                    .start())
            
            logger.info("Writing to console for debugging")
            query.awaitTermination()
            
        except Exception as e:
            logger.error(f"Error writing to console: {e}")
            raise
    
    def run(self, output_mode: str = "kafka") -> None:
        """
        Run the Spark streaming pipeline.
        
        Args:
            output_mode: Output destination ('kafka' or 'console')
        """
        try:
            # Initialize Spark session
            self._initialize_spark_session()
            
            logger.info("Starting Spark Structured Streaming pipeline...")
            
            # Read from Kafka
            raw_df = self._read_from_kafka()
            
            # Parse Kafka messages
            parsed_df = self._parse_kafka_messages(raw_df)
            
            # Preprocess text
            preprocessed_df = self._preprocess_text(parsed_df)
            
            # Perform sentiment analysis
            analyzed_df = self._perform_sentiment_analysis(preprocessed_df)
            
            # Prepare output data
            output_df = self._prepare_output_data(analyzed_df)
            
            # Write to destination
            if output_mode == "console":
                self._write_to_console(output_df)
            else:
                self._write_to_kafka(output_df)
            
        except KeyboardInterrupt:
            logger.info("Received shutdown signal...")
        except Exception as e:
            logger.error(f"Error in pipeline execution: {e}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self) -> None:
        """Clean up resources and stop Spark session."""
        logger.info("Cleaning up resources...")
        
        if self.spark:
            try:
                self.spark.stop()
                logger.info("Spark session stopped")
            except Exception as e:
                logger.error(f"Error stopping Spark session: {e}")


def main():
    """Main entry point for the script."""
    try:
        pipeline = TwitterSentimentSparkPipeline()
        
        # Run with Kafka output (default)
        pipeline.run(output_mode="kafka")
        
        # Uncomment below to run with console output for debugging
        # pipeline.run(output_mode="console")
        
    except Exception as e:
        logger.error(f"Failed to run pipeline: {e}")
        exit(1)


if __name__ == "__main__":
    main()