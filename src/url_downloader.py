#!/usr/bin/env python3
"""
User impact in Social Networks: master thesis at BUT FIT, ac. year 2016/2017.
Author: Petr Jirout, xjirou07@stud.fit.vutbr.cz

Twitter URL downloader prototype
"""

import sys, os, urllib, copy, json, time, datetime

import twitter

from twitter_data_unit import TwitterDataUnit as DataUnit

# How many results to fetch in one request ('count' parameter)
RESULTS_FETCH_COUNT = 100  #TODO increase for real data taking
# For how many minutes this script will run
RUNTIME_DURATION = 120

# Where the downloaded data are stored
DATA_DIR = os.path.join('..', 'data')

# Twitter keys and secrets are obtained from the os env variables
CONSUMER_KEY        = os.getenv("CONSUMER_KEY", None)
CONSUMER_SECRET     = os.getenv("CONSUMER_SECRET", None)
ACCESS_TOKEN_KEY    = os.getenv("ACCESS_TOKEN_KEY", None)
ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET", None)

# Global Twitter API object
twitterApi = twitter.Api(consumer_key=CONSUMER_KEY,
                         consumer_secret=CONSUMER_SECRET,
                         access_token_key=ACCESS_TOKEN_KEY,
                         access_token_secret=ACCESS_TOKEN_SECRET,
                         sleep_on_rate_limit=True)



def CreateQuery():
    """ Create a raw query.

    We ask for 100 tweets, but Twitter usually filters it, so in reality
    we get a smaller count """

    #TODO Specify desired keywords
    searchStr = "article OR security OR IT OR technology filter:links"
    # URL encode the search string
    searchStr = urllib.parse.quote(searchStr)
    # Search parameters, not to be encoded
    parameters = [
        "lang=en",
        #"result_type=popular",
        "result_type=recent",
        "count=%d" % RESULTS_FETCH_COUNT,
    ]

    query = searchStr + '&' + '&'.join(parameters)

    print("Query:", query)
    return query


def CreateDataUnitsFromTweet(tweetStatus):
    """ Given twitterApi tweet Status object, extract the information from it.

    Returns a list of extracted DataUnit objects: URL with their associated user, id, retweet and favourite count.
    """

    tweet = tweetStatus.AsDict()
    results = []

    urls = tweet['urls']
    for urlObj in urls:
        url = TrimAndFilterUrl(urlObj['expanded_url'])
        if not url:  # url filtered, skip it
            continue

        dataUnit = DataUnit()
        dataUnit.initFromTweet(tweet, url)

        results.append(dataUnit)

    return results


def TrimAndFilterUrl(url):
    """ Remove unnecessary parts of the url and check if it isn't in the filter rules.

    Return domain + element path, e.g.: domain.com/my/resource.img
    """

    res = urllib.parse.urlparse(url)

    domain = res.netloc.replace("www.", "", 1)  # erase 'www.'

    # Filter Twitter backlinks to the status itself
    # I.e. when status with ID 123456 containts url of the status: twitter.com/i/web/status/123456
    if domain == "twitter.com" and res.path.startswith("/i/web/status/"):
        return None

    scheme = ""
    if res.scheme:
        scheme = res.scheme + '://'
    query = ""
    if res.query:
        query = '?' + res.query

    return scheme + domain + res.path + query


def ExtractDomainFromUrl(url):
    """ Extract just the domain name. """

    res = urllib.parse.urlparse(url)
    domain = res.netloc.replace("www.", "", 1)  # erase 'www.'

    return domain


def SaveResults(results):
    """ Save results to the DATA_DIR, in format 'data_NUM.json' """

    # Because we're using a custom class to store the data (DataUnit), we
    # need to provide a default data dumper for objects, which cannot be serialized
    # automatically (i.e. our DataUnits)
    def _objDumper(obj):
        try:
            return obj.getDict()
        except:
            return obj.__dict__

    # Create the directory if necessary
    if not os.path.isdir(DATA_DIR):
        if os.path.exists(DATA_DIR):  # it's a regular file, abort
            print("Data directory '%s' cannot be created, there's a file with the same name" % DATA_DIR)
            return False
        else:
            os.makedirs(DATA_DIR)

    counter = 0
    while True:
        filename = "data_%d.json" % counter
        filename = os.path.join(DATA_DIR, filename)
        if not os.path.exists(filename):
            break
        counter += 1

    with open(filename, 'w') as fp:
        json.dump(results, fp, default=_objDumper, indent=2)

    print("Saved results to", filename)
    return True


def GetTweets(query, max_id=None):
    """ Perform the Twitter API Search query and return the result.

    Params:
        query:  search query
        max_id: return tweets older than given ID (i.e. smaller ID) (optional)
    """

    if max_id:
        query += "&max_id=%s" % str(max_id)

    print("Making query:", query)
    tweets = twitterApi.GetSearch(raw_query="q="+query)
    return tweets


def Main():
    """ TODO """
    #print(api.VerifyCredentials())

    query = CreateQuery()

    startTime = datetime.datetime.now()
    print("Started at:", startTime)
    # How long should the script run
    runDuration = datetime.timedelta(minutes=RUNTIME_DURATION)

    results = []
    resultsIds = []
    try:
        maxId = None
        for i in range(360000): # maximum of 360 000 iterations (requests)
            tweets = GetTweets(query, maxId)

            for tweet in tweets:
                dus = CreateDataUnitsFromTweet(tweet)
                if dus:
                    # First iteration, initialize maxId
                    if maxId is None:
                        maxId = dus[0].getId()
                    # Append only if we already don't have these elements
                    for du in dus:
                        duId = du.getId()
                        # We want the smallest ID as the max_id parameter
                        maxId = str( min(int(maxId), int(duId)) )

                        if duId in resultsIds:  # we already have this tweet, skip it
                            continue

                        results.append(du)
                        resultsIds.append(duId)

            print("Total tweets:", len(results))

            time.sleep(1)
            print(i, end=' ', flush=True)
            nowTime = datetime.datetime.now()
            if startTime + runDuration < nowTime:  # time limit expired, terminate
                break

    finally:
        SaveResults(results)

    return 0


# Main function wrapper
if __name__ == "__main__":
    sys.exit(Main())
