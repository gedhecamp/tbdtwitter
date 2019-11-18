import tweepy
import json
from pymongo import MongoClient


MONGO_HOST = 'mongodb+srv://gedhe:raykingm@tbdcluster-l5fo8.gcp.mongodb.net/test?retryWrites=true&w=majority'

WORDS = ['fintech', 'ovo_id', 'gopayindonesia', 'danawallet']

CONSUMER_KEY = "IPWn8SHZvxMYgkSwzRbI6RtfA"
CONSUMER_SECRET = "CZWzor7NJDUkYLEuRAFAsEPF2K5oFECJiPalSAJZ7Y7nFy9RTu"
ACCESS_TOKEN = "765375062576803840-UjDpWJv5M51gyo54AZK8cd5QzsD71eR"
ACCESS_TOKEN_SECRET = "PUhOEaLSvrDHgA8JrNEnpHZD7ZTYZ4vxKnQGcuif1NiG6"


class StreamListener(tweepy.StreamListener):

    def on_connect(self):
        print("You are now connected to the streaming API.")

    def on_error(self, status_code):
        print('An Error has occured: ' + repr(status_code))
        return False

    def on_data(self, data):
        try:
            client = MongoClient(MONGO_HOST)

            # buat database
            db = client.twitterdb

            # load data json
            datajson = json.loads(data)

            # ambil attr created at di twitter
            created_at = datajson['created_at']

            # print timestamp
            print("Tweet collected at " + str(created_at))

            # insert data
            db.twitter_demo.insert_one(datajson)
        except Exception as e:
            print(e)


auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)


listener = StreamListener(api=tweepy.API(wait_on_rate_limit=True))
streamer = tweepy.Stream(auth=auth, listener=listener)
print("Tracking: " + str(WORDS))
streamer.filter(track=WORDS)