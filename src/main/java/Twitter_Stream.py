import re
import tweepy
from pymongo import MongoClient

client = MongoClient("mongodb+srv://robindermongo:root@cluster0.hon6x.mongodb.net/test")
consumer_key = "SvXCpXxVM2JzaBjJLHTFdJK61"
consumer_secret = "YBq37YK6d5pybt2eHtxnduxCSa8LkZRwBHVB2Q3N8zhxBEJunE"
access_key = "1322853776793894913-0iYSM3AcKSwNNxc6XknGJp5XZv83ok"
access_secret = "BaTRs4holXqs70GXuTA9HfQhXi175gcIr9yTnc5qmm1uv"


class StreamListener(tweepy.StreamListener):
    count = 0

    def on_status(self, status):
        if status.retweeted:
            return
        self.count += 1
        if self.count == 1000:
            stream.disconnect()
        self.insert_data(status)

    def clean(self, tweetStr):
        tweet_regex = ' '.join(re.sub("([^0-9A-Za-z \t]+)|(\w+:\/\/\S+)", " ", tweetStr).split())


        tweet_regex = re.sub(
            r'https?:\/\/(www\.)?[-a-zA-Z0–9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0–9@:%_\+.~#?&//=]*)', '',
            tweet_regex, flags=re.MULTILINE)


        tweet_regex = re.sub(r'[-a-zA-Z0–9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0–9@:%_\+.~#?&//=]*)', '',
                             tweet_regex, flags=re.MULTILINE)
        return tweet_regex

    def insert_data(self, tweet):
        data = {}
        data['user_name'] = tweet.user.screen_name
        data['time'] = tweet.created_at
        data['text'] = tweet.text
        data['location'] = tweet.user.location
        data['retweet_count'] = tweet.retweet_count
        raw_db = client.RawDb
        raw_db.RawData.insert_one(data)
        if data["user_name"] is not None:
            data['user_name'] = self.clean(data['user_name'])
        if data["text"] is not None:
            data['text'] = self.clean(data['text'])
        if data["location"] is not None:
            data['location'] = self.clean(data['location'])

        processed_db = client.ProcessedDb
        processed_db.ProcessedData.insert_one(data)

    def on_error(self, status_code):
        if status_code == 420:
            return False


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)

stream_listener = StreamListener()
stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
stream.filter(track=["Storm", "Winter", "Canada", "Temperature", "Flu", "Snow", "Indoor", "Safety"])
print("completed")