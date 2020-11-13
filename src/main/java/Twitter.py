import re

import tweepy
from pymongo import MongoClient

consumer_key = "SvXCpXxVM2JzaBjJLHTFdJK61"
consumer_secret = "YBq37YK6d5pybt2eHtxnduxCSa8LkZRwBHVB2Q3N8zhxBEJunE"
access_key = "1322853776793894913-0iYSM3AcKSwNNxc6XknGJp5XZv83ok"
access_secret = "BaTRs4holXqs70GXuTA9HfQhXi175gcIr9yTnc5qmm1uv"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth, wait_on_rate_limit = True)
result = []


def clean(tweetStr):
    tweet_regex = ' '.join(re.sub("([^0-9A-Za-z \t]+)|(\w+:\/\/\S+)", " ", tweetStr).split())

    # to remove links that start with HTTP/HTTPS in the tweet

    tweet_regex = re.sub(r'https?:\/\/(www\.)?[-a-zA-Z0–9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0–9@:%_\+.~#?&//=]*)', '', tweet_regex, flags=re.MULTILINE)

    # to remove other url links

    tweet_regex = re.sub(r'[-a-zA-Z0–9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0–9@:%_\+.~#?&//=]*)', '', tweet_regex, flags=re.MULTILINE)
    return tweet_regex



for tweet in tweepy.Cursor(api.search, q = 'Storm OR Winter OR Canada OR Temperature OR Flu OR Snow OR Indoor OR Safety').items(2000):
    data = {}
    data['user_name'] = tweet.user.screen_name
    data['time'] = tweet.created_at
    data['text'] = tweet.text
    data['location'] = tweet.user.location
    data['retweet_count'] = tweet.retweet_count
    result.append(data)
    client = MongoClient("mongodb+srv://robindermongo:root@cluster0.hon6x.mongodb.net/test")
    db = client.RawDb
    db.RawData.insert_one(data)
    # print(one['text'])
    if data["user_name"] is not None:
        data['user_name'] = clean(data['user_name'])
    if data["text"] is not None:
        data['text'] = clean(data['text'])
    if data["location"] is not None:
        data['location'] = clean(data['location'])
    dbProcessed = client.ProcessedDb
    dbProcessed.ProcessedData.insert_one(data)

