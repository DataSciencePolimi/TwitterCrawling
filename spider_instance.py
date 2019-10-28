import tweepy
from tweepy import User
from tweepy import TweepError



class Spider:
    def __init__(self,spider_id, consumer_key,consumer_secret,access_token,access_token_secret):
        self.spider_id = spider_id
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.access_token_secret = access_token_secret
        auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        auth.set_access_token(self.access_token, self.access_token_secret)
        self.api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    def get_api(self):
        return self.api
