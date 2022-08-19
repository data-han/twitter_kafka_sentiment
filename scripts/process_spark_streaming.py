import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from textblob import TextBlob
from dotenv import load_dotenv

load_dotenv()

token = os.getenv('influxdb_token')
org = os.getenv('influxdb_org')
bucket = os.getenv('influxdb_bucket')

topic_name = 'twitterdata'
output_topic = 'twitterdata-clean'


class Sentiment:
    def get_schema():
        schema = StructType([
            StructField("created_at", StringType()),
            StructField("id", StringType()),
            StructField("text", StringType()),
            StructField("source", StringType()),
            StructField("truncated", StringType()),
            StructField("in_reply_to_status_id", StringType()),
            StructField("in_reply_to_user_id", StringType()),
            StructField("in_reply_to_screen_name", StringType()),
            StructField("user", StringType()),
            StructField("coordinates", StringType()),
            StructField("place", StringType()),
            StructField("quoted_status_id", StringType()),
            StructField("is_quote_status", StringType()),
            StructField("quoted_status", StringType()),
            StructField("retweeted_status", StringType()),
            StructField("quote_count", StringType()),
            StructField("reply_count", StringType()),
            StructField("retweet_count", StringType()),
            StructField("favorite_count", StringType()),
            StructField("entities", StringType()),
            StructField("extended_entities", StringType()),
            StructField("favorited", StringType()),
            StructField("retweeted", StringType()),
            StructField("possibly_sensitive", StringType()),
            StructField("filter_level", StringType()),
            StructField("lang", StringType()),
            StructField("matching_rules", StringType()),
            StructField("name", StringType()),
            StructField("timestamp_ms", StringType())
        ])
        return schema

    @staticmethod
    def preprocessing(df):
        # words = df.select(explode(split(df.text, " ")).alias("word"))
        df = df.filter(col('text').isNotNull())
        df = df.withColumn('text', regexp_replace('text', r'http\S+', ''))
        df = df.withColumn('text', regexp_replace('text', r'[^\x00-\x7F]+', ''))
        df = df.withColumn('text', regexp_replace('text', r'[\n\r]', ' '))
        df = df.withColumn('text', regexp_replace('text', '@\w+', ''))
        df = df.withColumn('text', regexp_replace('text', '#', ''))
        df = df.withColumn('text', regexp_replace('text', 'RT', ''))
        df = df.withColumn('text', regexp_replace('text', ':', ''))
        df = df.withColumn('source', regexp_replace('source', '<a href="' , ''))

        return df

    # text classification
    @staticmethod
    def polarity_detection(text):
        return TextBlob(text).sentiment.polarity

    @staticmethod
    def subjectivity_detection(text):
        return TextBlob(text).sentiment.subjectivity

    @staticmethod
    def text_classification(words):
        # polarity detection
        polarity_detection_udf = udf(Sentiment.polarity_detection, FloatType())
        words = words.withColumn("polarity_v", polarity_detection_udf("text"))
        words = words.withColumn(
            'polarity',
            when(col('polarity_v') > 0, lit('Positive'))
            .when(col('polarity_v') == 0, lit('Neutral'))
            .otherwise(lit('Negative'))
        )
        # subjectivity detection
        subjectivity_detection_udf = udf(Sentiment.subjectivity_detection, FloatType())
        words = words.withColumn("subjectivity_v", subjectivity_detection_udf("text"))
        return words


if __name__ == "__main__":
    # create Spark session
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") # Ignore INFO DEBUG output
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic_name) \
        .load()

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    df = df.withColumn("data", from_json(df.value, Sentiment.get_schema())).select("data.*")
    df = df \
        .withColumn("ts", to_timestamp(from_unixtime(expr("timestamp_ms/1000")))) \
        .withWatermark("ts", "1 seconds") # old data will be removed

    # Preprocess the data
    df = Sentiment.preprocessing(df)

    # text classification to define polarity and subjectivity
    df = Sentiment.text_classification(df)

    assert type(df) == pyspark.sql.dataframe.DataFrame

    row_df = df.select(
        to_json(struct("id")).alias('key'),
        to_json(struct('text', 'lang', 'ts', 'polarity_v', 'polarity', 'subjectivity_v')).alias("value")
    )

    # Writing to Console
    # query = df.writeStream.queryName("all_tweets")\
    #     .outputMode("append").format("console")\
    #     .start()

    # Writing to Kafka
    query = row_df\
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream\
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", output_topic) \
        .option("checkpointLocation", "file:/Users/user/tmp") \
        .start()

    # Writing to InfluxDB - doesn't work yet
    # query = df\
    #     .writeStream\
    #     .foreach(InfluxDBWriter())\
    #     .outputMode("append")\
    #     .start()

    query.awaitTermination()
