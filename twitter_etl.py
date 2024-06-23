import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def run_twitter_etl():

    access_key = "3A3w0G65Xd6MlHKyZPEtgRXqq" 
    access_secret = "veFUFAPyrlUboftc4t3vDcoBQqJNtZvkrrOjyOamWwqu8mThVN" 
    consumer_key = "1314461515-g2StlXvcRx6EyMTBSyhyHCSw7MJNk01fP8cf6sW"
    consumer_secret = "VLPqOj3svKLysbdDsz70ieHbwLfgHuFj38EotwtXbUqKg"


    # Twitter authentication##
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 

    # # # Creating an API object ##
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk', 
                            count=200,
                            include_rts = False,
                            tweet_mode = 'extended'
                            )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('refined_tweets.csv')