import tweepy
from tweepy_access import *

auth = tweepy.AppAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
api = tweepy.API(auth)
for tweet in tweepy.Cursor(api.search_tweets, q='tesla').items(10):
    print(tweet.text)
