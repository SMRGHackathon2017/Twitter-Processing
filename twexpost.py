import sys
import tweepy
import json
from bson import ObjectId
import twprocess as tw


def create_twitter_data():
    """Main function to perform analysis."""

    consumer_key = ''
    consumer_secret = ''

    api = tw.twitter_auth(consumer_key, consumer_secret)

    collected_tweets = tw.read_twitter_json('employ.json')

    if api == None:
        print "Can't Authenticate"
        sys.exit(-1)
    else:
        pass

    max_tweets = 10000
    tweets_per_query = 100

    since_id = None
    max_id = -1

    json_file = open('employ.json', 'a')

    searches = ([
        '(discrimination OR discriminated) ("at work" OR workplace)',
        '(harassment OR harassed) ("at work" OR workplace)',
        '(bullying OR bullied) ("at work" OR workplace)',
        '"unpaid wages" ("at work" OR workplace)',
        '"unfair treatment" ("at work" OR workplace)',
        '(sacked OR dismissed) (unfairly OR illegally)',
        'employment (dispute OR tribunal OR conciliation)'
    ])

    tweets_id = list()
    tweets_json = list()
    search_id = 0

    for tweet in collected_tweets:
        tweets_id.append(tweet['id'])

    for search in searches:
        print search
        since_id = tw.find_latest_id(collected_tweets, search_id)

        twitter_results = tw.twitter_search(
                a=api,
                q=search,
                t=tweets_per_query,
                x=max_tweets,
                s=since_id
        )

        print twitter_results[0]
        print len(twitter_results[1])
        for tweet in twitter_results[1]:
            if tweet.id not in tweets_id:
                tweet._json['search_id'] = search_id
                json_file.write(tw.JSONEncoder().encode(tweet._json)+'\n')
                tweets_id.append(tweet.id)

        search_id += 1

    json_file.close()

    return True

if __name__ == "__main__":
    create_twitter_data()

