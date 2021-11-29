import sys
import json
import tweepy
from tweepy import Stream
from tweepy_access import *
from socket import socket
from threading import Thread
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from textblob import TextBlob


PORT_NUMBER = 9002


class Streamer(Stream):
    """ 
        Class to stream tweets to a port on the local host 
    """
    def __init__(self, keywords):
        self.port = PORT_NUMBER
        self.keywords = keywords
        self.open_socket()
        self.wait_client()
        super().__init__(
            CONSUMER_KEY,
            CONSUMER_SECRET,
            ACCESS_TOKEN,
            ACCESS_SECRET
        )
        self.stream()

    def open_socket(self):
        self.s = socket()
        self.s.bind(("localhost", self.port))
        self.s.listen(1)

    def wait_client(self):
        self.c_socket, self.c_addr = self.s.accept()

    def stream(self):
        self.filter(track=self.keywords, filter_level='low', languages=["en"])

    def on_data(self, data):
        data = json.loads(data)
        text_key = 'fulltext' if 'fulltext' in data else 'text'
        self.c_socket.send(f'{data[text_key]}__EOL__'.encode('utf-8'))
        print(data[text_key])
        # sys.exit()
        

class Processor():
    """
        Class to process tweet data from a port on the local host
    """
    def __init__(self):
        spark = SparkSession.builder.appName("ElonFTW").getOrCreate()

        # read the tweet data from socket
        lines = spark.readStream.format("socket").option("host", "localhost").option("port", PORT_NUMBER).load()
        # Preprocess the data
        words = self.preprocessing(lines)
        # text classification to define polarity and subjectivity
        words = self.text_classification(words)

        words = words.repartition(1)
        query = words.writeStream.queryName("all_tweets")\
            .outputMode("append").format("parquet")\
            .option("path", "./parc")\
            .option("checkpointLocation", "./check")\
            .trigger(processingTime='60 seconds').start()
        query.awaitTermination()

    def polarity_detection(self, text):
        return TextBlob(text).sentiment.polarity

    def subjectivity_detection(self, text):
        return TextBlob(text).sentiment.subjectivity

    def text_classification(self, words):
        # polarity detection
        polarity_detection_udf = udf(self.polarity_detection, StringType())
        words = words.withColumn("polarity", polarity_detection_udf("word"))
        # subjectivity detection
        subjectivity_detection_udf = udf(self.subjectivity_detection, StringType())
        words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
        return words

    def preprocessing(self, lines):
        words = lines.select(explode(split(lines.value, "__EOL__")).alias("word"))
        words = words.na.replace('', None)
        words = words.na.drop()
        words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
        words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
        words = words.withColumn('word', F.regexp_replace('word', '#', ''))
        words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
        words = words.withColumn('word', F.regexp_replace('word', ':', ''))
        return words



if __name__ == "__main__":
    stream_t = Thread(target=Streamer,  args=([["tesla", "elon", "musk"]]))
    stream_t.start()
    process_t = Thread(target=Processor)
    process_t.start()
    while stream_t.is_alive():
        stream_t.join(timeout=2.0)
        process_t.join(timeout=2.0)
    
