#Import the necessary methods from tweepy library
# Fill the Twitter credentials and AWS access keys
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto.dynamodb2
from boto.dynamodb2.table import Table
from textblob import TextBlob
from decimal import *

#Variables that contains the user credentials to access Twitter API
consumer_key = ''
consumer_secret =''
access_token = ''
access_token_secret = ''


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        # decode json
        dict_data = json.loads(data)
        # pass tweet into TextBlob
        tweet = TextBlob(dict_data["text"]) if "text" in dict_data.keys() else None
        # output sentiment polarity

        if tweet:
            
            # determine if sentiment is positive, negative, or neutral
            if tweet.sentiment.polarity < 0:
                sentiment = "negative"
            elif tweet.sentiment.polarity == 0:
                sentiment = "neutral"
            else:
                sentiment = "positive"
            # output sentiment
            print(sentiment, tweet.sentiment.polarity, dict_data["text"])
            try:
                tweets.put_item(data={
                 'id': str(dict_data['id']),
                 'username': dict_data['user']['name'],
                 'screen_name': dict_data['user']['screen_name'],
                 'tweet': dict_data["text"]  if "text" in dict_data.keys() else "None",
                 'followers_count': dict_data['user']['followers_count'],
                 'location': str(dict_data['user']['location']),
                 'geo': str(dict_data['geo']),
                 'created_at': dict_data['created_at'],
                 'polarity': str(tweet.sentiment.polarity),
                 'subjectivity': str(tweet.sentiment.subjectivity),
                 'sentiment': sentiment
                 })
            except (AttributeError, Exception) as e:
                print (e)
            return True

    def on_error(self, status):
        print (status)






if __name__ == '__main__':

    conn = boto.dynamodb2.connect_to_region(region_name='us-east-1',
                                            aws_access_key_id='',
                                            aws_secret_access_key='')

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    tweets = Table('spark',connection=conn)

    #This line filter Twitter Streams to capture data by the keywords: 'terrorist attack','pedestrian struck','Toronto'
    stream.filter(track=['terrorist attack','pedestrian struck','Toronto'])
