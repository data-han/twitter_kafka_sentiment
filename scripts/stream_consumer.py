import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from textblob import TextBlob
from nltk.tag import pos_tag
from nltk.corpus import stopwords
from nltk.stem.wordnet import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer
from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from dotenv import load_dotenv

load_dotenv()

token = os.getenv('influxdb_token')
org = "datahanorg"
bucket = "datahanbucket"


class Sentiment:
    def __init__(self):
        self._stopwords = stopwords.words('english')
        self._word_tokenizer = TweetTokenizer(
            preserve_case=True,
            reduce_len=False,
            strip_handles=False)

        self._lemmatizer = WordNetLemmatizer()

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
        words = df.select(explode(split(df.text, " ")).alias("word"))
        words = words.na.replace('', None)
        words = words.na.drop()
        words = words.withColumn('word', regexp_replace('word', r'http\S+', ''))
        words = words.withColumn('word', regexp_replace('word', '@\w+', ''))
        words = words.withColumn('word', regexp_replace('word', '#', ''))
        words = words.withColumn('word', regexp_replace('word', 'RT', ''))
        words = words.withColumn('word', regexp_replace('word', ':', ''))

        return words

    def _tokenize(self, tweet):
        return self._word_tokenizer.tokenize(tweet)

    def _is_noise(self, word):
        pattern = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+#]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+|(@[A-Za-z0-9_]+)'
        return word in punctuation \
            or word.lower() in self._stopwords \
            or re.search(pattern, word, re.IGNORECASE) != None

    def _tag2type(self, tag):
        """
        Take a tag and return a type
        Common tags are:
            - NNP: Noun, proper, singular
            - NN: Noun, common, singular or mass
            - IN: Preposition or conjunction, subordinating
            - VBG: Verb, gerund or present participle
            - VBN: Verb, past participle
            - JJ: adjective ‘big’
            - JJR: adjective, comparative ‘bigger’
            - JJS: adjective, superlative ‘biggest’
            - ...
        return 'n' for noun, 'v' for verb, and 'a' for any
        """
        if tag.startswith('NN'):
            return 'n'
        elif tag.startswith('VB'):
            return 'v'
        else:
            return 'a'

    def _lemmatize(self, tokens):
        return [
            self._lemmatizer.lemmatize(word, self._tag2type(tag)).lower()
            for word, tag in pos_tag(tokens) if not self._is_noise(word)
        ]

    def _classify(self, tweet):
        tokens = self._lemmatize(self._tokenize(tweet))
        return self._classifier.classify(dict([token, True] for token in tokens))

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
        polarity_detection_udf = udf(Sentiment.polarity_detection, StringType())
        words = words.withColumn("polarity", polarity_detection_udf("word"))
        # subjectivity detection
        subjectivity_detection_udf = udf(Sentiment.subjectivity_detection, StringType())
        words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
        return words

    @staticmethod
    def aggregation(words):
        words = words.withColumn('grp', lit('one'))
        polarity_agg = words.select('polarity').groupby().sum()

        return polarity_agg


class InfluxDBWriter:
    def __init__(self):
        self.client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def open(self, partition_id, epoch_id):
        print("Opened %d, %d" % (partition_id, epoch_id))
        return True

    def process(self, row):
        self.write_api.write(bucket, org, record=self._row_to_line_protocol(row))

    def close(self, error):
        self.write_api.__del__()
        self.client.__del__()
        print("Closed with error: %s" % str(error))

    def _row_to_line_protocol(self, row):

        return


if __name__ == "__main__":
    # create Spark session
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") # Ignore INFO DEBUG output
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "twitterdata") \
        .load()

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    df = df.withColumn("data", from_json(df.value, Sentiment.get_schema())).select("data.*")
    df = df \
        .withColumn("ts", to_timestamp(from_unixtime(expr("timestamp_ms / 1000")))) \
        .withWatermark("ts", "1 seconds")

    windowed_counts = df \
        .groupBy(window(df['ts'], "1 minute", "30 seconds")) \
        .count()


    # mySchema = StructType([StructField("text", StringType(), True)])
    # values = df.select(from_json(df.value.cast("string"), mySchema).alias("test"))
    # df1 = values.select("test.*")

    # Preprocess the data
    words = Sentiment.preprocessing(df)

    # text classification to define polarity and subjectivity
    words = Sentiment.text_classification(words)

    # polarity_agg = Sentiment.aggregation(words)
    polarity = Sentiment._classify(df.select(df.text))
    print(polarity)
    # df = df.withColumn('polarity', sum('polarity'))
    words = words.repartition(1)

    assert type(words) == pyspark.sql.dataframe.DataFrame

    query = polarity_agg.writeStream.queryName("all_tweets")\
        .outputMode("append").format("console")\
        .start()

    # query = words\
    #     .writeStream\
    #     .foreach(InfluxDBWriter())\
    #     .outputMode("append")\
    #     .start()

    query.awaitTermination()
