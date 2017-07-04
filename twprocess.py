import tweepy
import json
import csv


class JSONEncoder(json.JSONEncoder):
    """Allows ObjectId to be JSON encoded."""
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)


def read_twitter_json(f):
    """Read twitter JSON file.

    Input
        f: path to json file
    Output
        d: list of dictionaries containing
    """
    tweets = list()
    with open(f) as json_file:
        for line in json_file:
            tweets.append(json.loads(line))
    return tweets


def twitter_auth(k, s):
    """Create authenticated API.

    Creates an App Only authenticated API allow better
    rate limits for searching

    Inputs
        k: API key
        s: API key_secret
    Output
        api: authenticated API interface
    """

    auth = tweepy.AppAuthHandler(k, s)
    api = tweepy.API(
        auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True
    )

    if (not api):
        return None
    else:
        return api


def twitter_search(a, q, x=10000000, t=100, s=None, m=-1):
    """Produce Twitter Search.

    Inputs
        a: Authenticated API interface
        q: search query
        x: maximun no of returned tweets (default: 10e8)
        t: tweet per query (default: 100 (api max)) (api: count)
        s: get tweets since id s (default: None) (api: since_id)
        m: last processed id (default: -1) next unprocessed tweet is
           m - 1 or smaller (api: max_id)
    Output
        tweets: Tuple (No of search results, List of tweets)
    """

    tweets = list()
    tweet_count = 0

    while tweet_count < x:
        try:
            if (m <= 0):
                if (not s):
                    new_tweets = a.search(q = q, count = t)
                else:
                    new_tweets = a.search(q = q, count = t, since_id = s)
            else:
                if (not s):
                    new_tweets = a.search(q = q, count = t, max_id = (m - 1))
                else:
                    new_tweets = a.search(q = q, count = t, max_id = (m - 1), since_id = s)

            if not new_tweets:
                break

            for tweet in new_tweets:
                tweets.append(tweet)

            tweet_count += len(new_tweets)
            m = new_tweets[-1].id

        except tweepy.TweepError as e:
            error = (-1, "error:" + str(e))
            return error

    search_results = (tweet_count, tweets)

    return search_results


def find_latest_id(d, s):
    """Find latest search tweet.

    Finds the latest tweet in the mongo data base for
    a particular search

    Input
        d: list of tweet dictionaries
        s: search_id of search
    Output
        m: id of latest tweet or None
    """

    selected_tweets = [t['id'] for t in d if t['search_id'] == s]

    if selected_tweets:
        m = max(selected_tweets)
    else:
        m = None
    return m


def write_csv(d, f):
    """Write csv file back to disk.

    Input
        d: tuple containing (header, data)
        f: filename for csv file.
    Output
        Fiel written to disk
    """
    with open(f, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(d[0])
        for row in d[1]:
            row_encode = list()
            for x in row:
                if type(x) == unicode:
                    row_encode.append(x.encode('utf8'))
                else:
                    row_encode.append(x)
            writer.writerow(row_encode)
    return True


def md_5_hash(i):
    """MD5 Hash values.

    Input
        i: input to be hashed
    Output
        h: hashed value
    """
    h = hashlib.md5(i.encode('utf-8')).hexdigest()
    return h


def extract_first_name(s):
    """Extract first name from string.

    Extracts the first name with a name string that
    contains 2 or more numbers.

    Input
        s: string containing name
    Output
        name: string containing first name (or None if not names > 1)
    """
    clean_name = re.sub(r'\s+', r' ', s).split()

    for name in clean_name:
        if len(name) > 1:
            return name.title()
        else:
            pass

    return None


def unicode_decode(text):
    """Convert to unicode.

    Input
        text: text for convertion
    Output
        converted text (not unicode left encoded)
    """

    try:
        return text.encode('utf-8').decode()
    except UnicodeDecodeError:
        return text.encode('utf-8')


def extract_value(k, d, f=''):
    """Extract value from dictionary if exists.

    Optional apply function to value once extracted

    Inputs
        k: key form key/value pair in dictionary
        d: dictionary to extract from
        f: (optional) function to apply to the value
    Output
        v: Value if key exists in dictionary or the empty string
    """
    if k in d:
        if f != '':
            p = f(d[k])
        else:
            p = d[k]

        if type(p) == str:
            v = unicode_decode(p)
        else:
            v = p
    else:
        v = unicode_decode('')
    return v


def extract_dict(d, f):
    """Extract value from dictionary chain.

    Inputs
        d: Top level dictionary
        f: List of dictionary element including final required value
           e.g. ['reactions', 'summary', 'total_count']
    Output:
        required value if at end of chain otherwise recursivly call function
        till rearch the end of the chain
    """
    if len(f) == 1:
        return extract_value(f[0], d)
    else:
        return extract_dict(d[f[0]], f[1:])


def create_csv(d, f):
    """Create a flattened csv from a python dictionary.

    Inputs
        d: dictionary of JSON object
        f: list of fields for csv file
           (use dots to extract from deeper within the dictionary
    Outputs
        csv: tuple of (list of headers, list of data rows)
    """
    csv_data = list()
    csv_head = [unicode(x) for x in f]

    for row in d:
        row_data = list()
        for field in f:
            fields = field.split('.')
            row_data.append(extract_dict(row, fields))
        csv_data.append(row_data)

    csv = (csv_head, csv_data)
    return csv
