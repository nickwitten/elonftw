import time
import sys
import os
import json
import tweepy
from tweepy import Stream
from tweepy_access import *
from socket import socket
from threading import Thread
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, explode, split
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from textblob import TextBlob


PORT_NUMBER = 9002
APP_DIR = os.path.join('/home/ubuntu', 'elonftw')
DATA_DIR = os.path.join(APP_DIR, 'data')


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
        # print(data[text_key])
        

class Processor():
    """
        Class to process tweet data from a port on the local host
    """
    def __init__(self):
        self.session = self.create_session()
        self.df = self.create_dataframe()
        self.clean_df()
        self.process()

    def create_session(self):
        return SparkSession.builder.appName("ElonFTW").getOrCreate()

    def create_dataframe(self):
        options = {
            'host': 'localhost',
            'port': PORT_NUMBER,
        }
        df = self.session.readStream.format("socket").options(**options).load()
        return df.select(explode(split(df.value, "__EOL__")).alias("word"))

    def clean_df(self):
        tweets = self.df.na.replace('', None)
        tweets = tweets.na.drop()
        tweets = tweets.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
        tweets = tweets.withColumn('word', F.regexp_replace('word', '@\w+', ''))
        tweets = tweets.withColumn('word', F.regexp_replace('word', '#', ''))
        tweets = tweets.withColumn('word', F.regexp_replace('word', 'RT', ''))
        tweets = tweets.withColumn('word', F.regexp_replace('word', ':', ''))
        self.df = tweets

    def process(self):
        polarity_detection_udf = udf(self.polarity_detection, StringType())
        subjectivity_detection_udf = udf(self.subjectivity_detection, StringType())
        self.df = self.df.withColumn("polarity", polarity_detection_udf("word"))
        self.df = self.df.withColumn("subjectivity", subjectivity_detection_udf("word"))

        self.df = self.df.repartition(1)
        query = self.df.writeStream.queryName("tsla_tweets")\
            .outputMode("append").format("csv")\
            .option("path", DATA_DIR)\
            .option("checkpointLocation", "./checkpoint")\
            .trigger(processingTime='60 seconds').start()
        query.awaitTermination()

    @staticmethod
    def polarity_detection(text):
        return TextBlob(text).sentiment.polarity

    @staticmethod
    def subjectivity_detection(text):
        return TextBlob(text).sentiment.subjectivity


if __name__ == "__main__":
    stream_t = Thread(target=Streamer,  args=([["tesla", "elon", "musk"]]))
    stream_t.start()
    process_t = Thread(target=Processor)
    process_t.start()
    while stream_t.is_alive():
        stream_t.join(timeout=2.0)
        process_t.join(timeout=2.0)
    
