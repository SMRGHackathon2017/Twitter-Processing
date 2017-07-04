import sys
import tweepy
import json
from bson import ObjectId
import twprocess as tw


def create_twitter_csv():
    """Main function to create csv."""


    collected_tweets = tw.read_twitter_json('employ.json')

    fields = ([
        'id',
        'text',
        'coordinates',
        'retweeted',
        'retweet_count',
        'user.followers_count',
        'user.statuses_count',
        'user.description',
        'user.friend_count',
        'user.location',
        'user.time_zone',
        'created_at',
    ])

    csv = tw.create_csv(collected_tweets, fields)

    write_file = tw.write_csv(csv, 'employ.csv')

    return True


if __name__ == "__main__":
    create_twitter_csv()

