"""
    Data Unit class for Tweets.
"""

import urllib

class TwitterDataUnit():
    """ Data unit (e.g. one downloaded Tweet or URL). """

    def __init__(self):
        self.url = None
        self.domain = None
        self.scheme = None
        self.id = None
        self.user = None
        self.favorite_count = 0
        self.retweet_count = 0

    def getDict(self):
        """ Return the object content as a dictionary. """

        return {
            'url': self.url,
            'domain': self.domain,
            'scheme': self.scheme,
            'id': self.id,
            'user': self.user,
            'favorite_count': self.favorite_count,
            'retweet_count': self.retweet_count,
        }

    def getId(self):
        return self.id

    def initFromTweet(self, tweetDict, url):
        """ Init data structures from a tweet.

        Params:
            tweetObj: Status object from the python-twitter library, as dictionary (obj.AsDict())
            url: URL you want to associate with this object
        """

        self.user = tweetDict['user']['screen_name']
        self.id = tweetDict.get('id_str', None)
        self.favorite_count = tweetDict.get('favorite_count', 0)
        self.retweet_count = tweetDict.get('retweet_count', 0)
        self.url = url

        parsedUrl = urllib.parse.urlparse(url)
        self.domain = parsedUrl.netloc.replace("www.", "", 1)  # erase 'www.'
        self.scheme = parsedUrl.scheme
