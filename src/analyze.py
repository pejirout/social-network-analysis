#!/usr/bin/env python3
"""
    Python interface for analysis. You need to provide an ElasticSearch instance with the data.
    All the data crunching is done in the ElasticSearch.
"""

import os, sys, json, argparse, copy, urllib.parse, itertools

import dateutil.parser

import matplotlib
# We need to activate the backend. Make sure it's present on the system. (python3-tk system pkg etc.)
matplotlib.use('TkAgg')
import matplotlib.pyplot as pyplot
import matplotlib.dates

import numpy.polynomial.polynomial as poly

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError as EsNotFoundError
from elasticsearch_dsl import Search, A, F


DEBUG = False

### Global variables for appropriate command line options

# List of users you want analyze
USERS = []
# Whether to strip domains to their top domains, e.g. 'blog.ihned.cz' --> 'ihned.cz'
STRIP_DOMAINS = None
# Print all available users in Elasticsearch
PRINT_USERS = None
# Print only latest posts for the given user(s)
PRINT_LATEST_POST = None
# Plot only graphs
ONLY_PLOTS = None
# Do only common stats
ONLY_STATS = None
# Elasticsearch address string
ES_ADDRESS = None
# Elasticsearch index
ES_INDEX = 'xjirou07'
# Percentage of how many interactions a user has to have to be considered a frequent interactor
MIN_INTERACTIONS = 0.05
# Number of posts to analyze
POST_COUNT = 1000

# Global pyplot settings
pyplot.rcParams['xtick.minor.visible'] = True
pyplot.rcParams['ytick.minor.visible'] = True


# Future possible queries
# Between popular posts, what percentage of it is a photo (or link)
# Most liked posts, most shared posts, most commented posts? (With text)
# Plot interactions per status (compound graph)
# Graph: likes distribution between users (majority has very few, minority makes most, Pareto rule)


def ParseArguments():
    """ Parse command line arguments and set appropriate global flags """

    def min_interaction_type(number):
        number = float(number)
        if number < 0.01 or number > 0.99:
            raise argparse.ArgumentTypeError("{0} not in range [0.1, 0.9]".format(number))
        return number


    global USERS, STRIP_DOMAINS, PRINT_USERS, PRINT_LATEST_POST, ONLY_PLOTS, ONLY_STATS, ES_ADDRESS, ES_INDEX
    global MIN_INTERACTIONS, POST_COUNT
    parser = argparse.ArgumentParser(description="Analyze data from the Elasticsearch instance")
    parser.add_argument('-d', '--strip-domains', dest='strip', action='store_true',
                        help="When analyzing most frequently published domains, strip all low level domains. E.g.: " +
                             "'blog.ihned.cz' or 'archive.ihned.cz' will be both listed under the domain 'ihned.cz'")
    parser.add_argument('-l', '--list-users', dest='print', action='store_true',
                        help="Print users available for analysis in the Elasticsearch instance")
    parser.add_argument('--latest-post', dest='latest_post', action='store_true',
                        help="Print datetime of the user(s) latest post in the Elasticsearch instance")
    parser.add_argument('-u', '--user', dest='user', action='append', default=[],
                        help="ID or username of the user you want to analyze. May be specified multiple times.")
    parser.add_argument('-p', '--only-plots', dest='plots', action='store_true',
                        help="Return only plots instead of full analysis")
    parser.add_argument('-s', '--only-stats', dest='stats', action='store_true',
                        help="Return only text statistics instead of full analysis")
    parser.add_argument('-a', '--es-address', dest='es_addr', default=None,
                        help="Elasticsearch address. String in the format of 'host[:port]'")
    parser.add_argument('-i', '--es-index', dest='es_index', default=ES_INDEX,
                        help="Index under which all data are stored in Elasticsearch.")
    parser.add_argument('-m', '--min-interactions', dest='min_ints', type=min_interaction_type, default=MIN_INTERACTIONS,
                        help="Percentage of how many interactions a user has to have to be considered a frequent " +
                             "interactor. Must be a float in range [0.01, 0.99].")
    parser.add_argument('-n', '--post-count', dest='post_cnt', type=int, default=POST_COUNT,
                        help="Number of posts to analyze (positive integer).")

    args = parser.parse_args()
    USERS = args.user
    STRIP_DOMAINS = args.strip
    PRINT_USERS = args.print
    PRINT_LATEST_POST = args.latest_post
    ONLY_PLOTS = args.plots
    ONLY_STATS = args.stats
    ES_ADDRESS = args.es_addr
    ES_INDEX = args.es_index
    MIN_INTERACTIONS = args.min_ints
    POST_COUNT = args.post_cnt

    if not USERS and not PRINT_USERS:
        parser.error("Specify user(s) to analyze or an operation (e.g. -l)")


class Analyzer:
    """ Perform various analyses on data stored in Elasticsearch """

    def __init__(self, index, data_source, es_address=None):
        if es_address:
            self.es = Elasticsearch(es_address)
        else:
            self.es = Elasticsearch()
        self.index = index
        # Do not access directly, only via the getter (otherwise you might break your request pipeline)
        self._es_search =  Search().using(self.es).index(index)

        # Line style counters
        self._lineTypeCounter = 0
        self._lineColourCounter = 0

        # Save shortened source string (e.g. 'fb')
        if data_source.lower() not in ('fb', 'facebook', 'tw', 'twitter'):
            raise ValueError("Unsupported data source")
        self.data_source = data_source.lower()

        if self.data_source == 'facebook':
            self.data_source = 'fb'
        elif self.data_source == 'twitter':
            self.data_source = 'tw'

        # Save Elasticsearch document types
        self.doc_type_post = '{0}_post'.format(self.data_source)
        self.doc_type_interaction = '{0}_interaction'.format(self.data_source)
        self.doc_type_user = '{0}_user'.format(self.data_source)


    def get_es_search(self):
        """ Return a copy of internal elasticsearch_dsl.Search instance """

        return copy.copy(self._es_search)


    @staticmethod
    def execute_es_request(es_search_instance, doc_type=None, es_filter=None, es_query=None):
        """Execute request on the given search instance

        :param es_search_instance: elasticsearch_dsl.Search instance you want to perform the request on
        :param doc_type: document type for the request (optional]
        :param es_filter: filter to apply (optional)
        :param es_query: query to apply (optional)
        :return elasticsearch_dsl.Response instance
        """

        if not isinstance(es_search_instance, Search):
            raise ValueError("Given object is not a elasticsearch_dsl.Search instance")

        if doc_type:
            es_search_instance = es_search_instance.doc_type(doc_type)
        if es_filter:
            es_search_instance = es_search_instance.filter(es_filter)
        if es_query:
            es_search_instance = es_search_instance.query(es_query)

        if DEBUG:
            print("Raw query:")
            print(json.dumps(es_search_instance.to_dict(), indent=2))

        return es_search_instance.execute()


    @staticmethod
    def _get_author_id_list(author_ids):
        """ Given either string or a list, ensure a list is returned.

        :param author_ids: list or string of author IDs
        :return: list with author IDs
        """

        authorList = author_ids
        if type(author_ids) is str:  # create a list so we can iterate over it
            authorList = [author_ids]
        elif not type(author_ids) is list and not type(author_ids) is tuple:
            raise ValueError("'author_ids' argument has to be either string, list or tuple")

        return authorList


    def _get_line_style(self, marker_style='', reset=False):
        """ Return unique line style each time this function is called

        :param marker_style: marker you want to use (string)
        :param reset: when True, set internal counters to zero
        :return: string with line format
        """

        # Line styles: solid, dashed, dash-dotted, dotted
        lineTypes = ('-', '--', '-.', ':')
        # Line colours: blue, green, red, black, magenta
        lineColours = ('b', 'g', 'r', 'k', 'm')

        # Add local static variables
        if reset:
            self._lineTypeCounter = 0
            self._lineColourCounter = 0

        if reset:  # return first colour, do not increment
            return "{0}{1}{2}".format(lineColours[0], lineTypes[0], marker_style)

        # First increment only line colour, then line styles
        if self._lineColourCounter > len(lineColours) - 1:
            self._lineColourCounter = 0
            if self._lineTypeCounter >= len(lineTypes) - 1:
                raise RuntimeError("No more unique line styles")
            self._lineTypeCounter += 1

        # Line with data point marker (+)
        style = "{0}{1}{2}".format(lineColours[self._lineColourCounter], lineTypes[self._lineTypeCounter], marker_style)

        self._lineColourCounter += 1
        return style


    def get_author_name(self, author_id, ensure_ascii=True):
        """ Return author's name.

        :param author_id: ID of the user
        :param ensure_ascii: whether you want the ascii version or full name
        :return: username (string)
        """

        userInfo = self.es.get(index=self.index, doc_type=self.doc_type_user, id=author_id)
        if ensure_ascii:
            return userInfo['_source']['name_ascii']
        else:
            return userInfo['_source']['name']


    def get_author_id(self, author_name):
        """ Return author's ID from the given name

        :param author_name: name of the user
        :return: user's ID (string
        """

        authors = self.get_authors_all()

        for author in authors:
            if author_name in (author['name'], author['name_ascii']):
                return author['id']

        raise RuntimeError('User {0} not found in Elasticsearch'.format(author_name))


    def get_author_id_from_string(self, author_string):
        """ Get author ID from a string, which can be either ID or username

        :param author_string: user's ID or username
        :return user's ID
        """

        try:
            # Check if we got author name
            authorId = self.get_author_id(author_string)
            return authorId  # we've got author's name in author_string
        except RuntimeError:
            # Check if we got author ID. If it throws, user doesn't exist
            authorName = self.get_author_name(author_string)
            return self.get_author_id(authorName)


    def get_authors_all(self):
        """ Get all available authors

        :return: list of author dicts
        """

        ess = self.get_es_search()
        ess = ess.params(size=10000)  # return all authors
        isAuthorFilter = F('term', is_author=True)

        response = self.execute_es_request(ess, doc_type=self.doc_type_user, es_filter=isAuthorFilter)

        if not response.success():
            raise RuntimeError('Elasticsearch request failed')

        authors = []
        for hit in response.hits:
            authors.append(hit.to_dict())

        return authors


    def get_author_fan_count(self, author_id):
        """ Return total fan count for the author's page (based on liking/following the page, not posts)

        :param author_id: user's ID
        :return fan count (int)
        """

        userData = self.es.get(index=self.index, doc_type=self.doc_type_user, id=author_id)
        return userData['_source']['fan_count']


    def get_post(self, post_id):
        """ Return the post document

        :param post_id: ID of the post you want to fetch
        :return: desired document as a Python dict
        """

        return self.es.get(index=self.index, doc_type=self.doc_type_post, id=post_id)


    def get_newest_post(self, author_id):
        """ Return given number of latest posts for the given author.

        :param author_id: user ID
        :return post as a Python dict
        """

        ess = self.get_es_search()
        ess = ess.params(size=1)  # only one post
        ess = ess.sort('-created_time')  # sort from newest to oldest

        authorFilter = F('term', author=author_id)
        response = self.execute_es_request(ess, doc_type=self.doc_type_post, es_filter=authorFilter)
        if not response.success():
            raise RuntimeError('Request failed')

        if response.hits:
            return response.hits[0].to_dict()
        else:
            raise RuntimeError('No posts')


    def get_posts(self, author_id, status_count=100):
        """ Return given number of latest posts for the given author.

        :param author_id: user ID
        :param status_count: number of post you want to return (default: 100)
        :return: list of post dictionaries
        """

        ess = self.get_es_search()
        ess = ess.params(size=status_count)
        ess = ess.sort('-created_time')  # sort from newest to oldest

        authorFilter = F('term', author=author_id)
        response = self.execute_es_request(ess, doc_type=self.doc_type_post, es_filter=authorFilter)
        if not response.success():
            raise RuntimeError('Request failed')

        posts = []
        for post in response.hits:
            posts.append(post.to_dict())

        return posts


    def get_interactions_for_post(self, post_id):
        """ Get all interactions for the given post

        :param post_id: ID of a post
        :return elasticsearch_dsl.Response instance
        """

        statusIdFilter = F('term', status_id=post_id)
        return self.execute_es_request(self.get_es_search(), doc_type=self.doc_type_interaction,
                                       es_filter=statusIdFilter)


    def get_likes_for_post(self, post_id):
        """ Get all likes for the given post

        :param post_id: ID of a post
        :return elasticsearch_dsl.Response instance
        """

        statusIdFilter = F('term', status_id=post_id) & F('term', type='like')
        return self.execute_es_request(self.get_es_search(), doc_type=self.doc_type_interaction,
                                       es_filter=statusIdFilter)


    def get_shares_for_post(self, post_id):
        """ Get all shares for the given post

        :param post_id: ID of a post
        :return elasticsearch_dsl.Response instance
        """

        statusIdFilter = F('term', status_id=post_id) & F('term', type='share')
        return self.execute_es_request(self.get_es_search(), doc_type=self.doc_type_interaction,
                                       es_filter=statusIdFilter)


    def get_comments_for_post(self, post_id):
        """ Get all comments for the given post

        :param post_id: ID of a post
        :return List of comment objects
        """

        statusIdFilter = F('term', status_id=post_id) & F('term', type='comment')
        response = self.execute_es_request(self.get_es_search(), doc_type=self.doc_type_interaction,
                                       es_filter=statusIdFilter)
        if not response.success():
            raise RuntimeError('Request failed')

        comments = []
        for comment in response.hits:
            comments.append(comment.to_dict())

        return comments


    def get_sentiment_for_post(self, post_id):
        """ Get total sentiment of comments for the given post

        :param post_id: ID of a post
        :return total sentiment (int)
        """

        ess = self.get_es_search()
        ess = ess.params(fields=['message_sentiment'], size=10000)
        commentFilter =  F('term', status_id=post_id) & F('term', type='comment')\
                         & F('exists', field='message_sentiment')
        response = self.execute_es_request(ess, doc_type=self.doc_type_interaction, es_filter=commentFilter)
        if not response.success():
            raise RuntimeError('Request failed')

        totalSentiment = 0
        for comment in response.hits:
            sent = comment['message_sentiment'][0]
            if sent == 'p':  # positive
                totalSentiment += 1
            elif sent == 'n':  # negative
                totalSentiment += 1

        return totalSentiment


    def get_count_likes_for_post(self, post_id):
        """ Get count of all likes for the given post

        :param post_id: ID of a post
        :return number of likes (int)
        """

        response = self.get_likes_for_post(post_id)
        if not response.success():
            raise RuntimeError('Request failed')
        return response.hits.total


    def get_count_shares_for_post(self, post_id):
        """ Get count of all shares for the given post

        :param post_id: ID of a post
        :return number of shares (int)
        """

        response = self.get_shares_for_post(post_id)
        if not response.success():
            raise RuntimeError('Request failed')
        return response.hits.total


    def get_count_comments_for_post(self, post_id):
        """ Get number of comments for the given post

        :param post_id: post you want to analyze
        :return: number of comments
        """

        ess = self.get_es_search()
        ess = ess.params(size=0)  # do not return hits, just the count
        commentFilter =  F('term', status_id=post_id) & F('term', type='comment')
        response = self.execute_es_request(ess, doc_type=self.doc_type_interaction, es_filter=commentFilter)
        if not response.success():
            raise RuntimeError('Request failed')

        return response.hits.total


    def get_average_comment_len_for_post(self, post_id):
        """ Get average length of a comment for a post

        :param post_id: post you want to analyze
        :return: average length of a comment for post
        """

        ess = self.get_es_search()
        ess = ess.params(fields=['message_len'], size=10000)  # limit the number of analyzed comments
        commentFilter = F('term', status_id=post_id) & F('term', type='comment') \
                        & F('exists', field='message_len')
        response = self.execute_es_request(ess, doc_type=self.doc_type_interaction, es_filter=commentFilter)
        if not response.success():
            raise RuntimeError('Request failed')

        commentCount = response.hits.total or 1
        totalLength = 0
        for comment in response.hits:
            length = comment.to_dict().get('message_len')[0]  # Returns the first element from a list
            totalLength += length

        return totalLength / commentCount


    def get_count_all_posts(self, author_id):
        """ Get number of all posts the author has published

        :param author_id: author's ID
        :return: total post count
        """

        ess = self.get_es_search()
        ess = ess.params(size=0)  # do not return hits, just the count
        authorFilter = F('term', author=author_id)

        response = self.execute_es_request(ess, doc_type=self.doc_type_post, es_filter=authorFilter)
        if not response.success():
            raise RuntimeError('Request failed')

        return response.hits.total


    def get_count_all_likes(self, author_id):
        """ Get count of all likes the author has received

        :param author_id: author's ID
        :return: total like count
        """

        ess = self.get_es_search()
        ess = ess.params(size=0)  # do not return hits, just the count
        statusAuthorLikeFilter = F('term', status_author=author_id) & F('term', type='like')

        response = self.execute_es_request(ess, doc_type=self.doc_type_interaction, es_filter=statusAuthorLikeFilter)
        if not response.success():
            raise RuntimeError('Request failed')

        return response.hits.total


    def get_count_all_comments(self, author_id):
        """ Get count of all comments the author has received

        :param author_id: author's ID
        :return: total like count
        """

        ess = self.get_es_search()
        ess = ess.params(size=0)  # do not return hits, just the count
        statusAuthorLikeFilter = F('term', status_author=author_id) & F('term', type='comment')

        response = self.execute_es_request(ess, doc_type=self.doc_type_interaction, es_filter=statusAuthorLikeFilter)
        if not response.success():
            raise RuntimeError('Request failed')

        return response.hits.total


    def get_count_all_shares(self, author_id):
        """ Get count of all shares the author has received

        :param author_id: author's ID
        :return: total share count (int)
        """

        ess = self.get_es_search()
        ess = ess.params(size=0)  # do not return hits, just the count
        statusAuthorFilter = F('term', author=author_id)

        aggregationName = 'sum_shares'
        sumShareAgg = A('sum', field='share_count')
        ess.aggs.bucket(aggregationName, sumShareAgg)

        response = self.execute_es_request(ess, doc_type=self.doc_type_post, es_filter=statusAuthorFilter)
        if not response.success():
            raise RuntimeError('Request failed')

        response = response.to_dict()
        return int(response['aggregations'][aggregationName]['value'])


    def get_average_likes(self, author_id):
        """ Get average like count for one status

        :param author_id: author's ID
        :return: average like count per status (float)
        """

        totalLikes = self.get_count_all_likes(author_id)
        statusCount = self.get_count_all_posts(author_id)

        return totalLikes / statusCount


    def get_average_shares(self, author_id):
        """ Get average share count for one status

        :param author_id: author's ID
        :return: average share count per status (float)
        """

        totalShares = self.get_count_all_shares(author_id)
        statusCount = self.get_count_all_posts(author_id)

        return totalShares / statusCount


    def get_average_comments(self, author_id):
        """ Get average comment count for one status

        :param author_id: author's ID
        :return: average comment count per status (float)
        """

        totalComments = self.get_count_all_comments(author_id)
        statusCount = self.get_count_all_posts(author_id)

        return totalComments / statusCount


    def get_followers_most_active(self, author_id, count=20):
        """ Get users who have the most interactions on posts made by the author

        :param author_id: user ID you want to analyze
        :param count: how many most active followers you want to return
        :return dictionary with users (ID as a key)
        """

        MIN_DOC_COUNT = 10  # return only results with more than 10 hits

        ess = self.get_es_search()

        aggregationName = 'terms_author'
        termsAuthorAgg = A('terms', field='author', min_doc_count=MIN_DOC_COUNT, size=count)
        ess.aggs.bucket(aggregationName, termsAuthorAgg)
        ess = ess.params(size=0)  # do not return hits, just the aggregations

        statusAuthorFilter = F('term', status_author=author_id)  # limit request to the given author

        response = self.execute_es_request(ess, doc_type=self.doc_type_interaction, es_filter=statusAuthorFilter)
        if not response.success():
            raise RuntimeError('Request failed')

        responseDict = response.to_dict()
        users = {}
        for bucket in responseDict['aggregations'][aggregationName]['buckets']:
            userId = bucket['key']
            users[userId] = bucket['doc_count']

        return users


    def get_followers_active(self, author_id, min_interactions=0.05, add_filter=None):
        """ Get users who have interacted on more than given percentage of posts made by the author

        :param author_id: user ID you want to analyze
        :param min_interactions: percentage of the minimum interactions over all posts (default: 0.05, i.e 5%)
        :param add_filter: additional filter you want to use
        :return dictionary of user objects (at most 10k)
        """

        ess = self.get_es_search()
        # Get all posts count and set a minimum interaction count
        postsCount = self.get_count_all_posts(author_id)
        minInteractions = int(min_interactions * postsCount)

        aggregationName = 'terms_author'
        termsAuthorAgg = A('terms', field='author', min_doc_count=minInteractions, size=10000)
        ess.aggs.bucket(aggregationName, termsAuthorAgg)
        ess = ess.params(size=0)  # do not return hits, just the aggregations

        statusAuthorFilter = F('term', status_author=author_id)  # limit request to the given author
        if add_filter:
            statusAuthorFilter &= add_filter

        response = self.execute_es_request(ess, doc_type=self.doc_type_interaction, es_filter=statusAuthorFilter)
        if not response.success():
            raise RuntimeError('Request failed')

        responseDict = response.to_dict()
        users = {}
        for bucket in responseDict['aggregations'][aggregationName]['buckets']:
            userId = bucket['key']
            users[userId] = bucket['doc_count']

        return users


    def get_followers_active_likes(self, author_id, min_interactions=0.05):
        """Get users who have liked on more than given percentage of posts made by the author

        :param author_id: user ID you want to analyze
        :param min_interactions: percentage of the minimum interactions over all posts (default: 0.05, i.e 5%)
        :return dictionary of user objects
        """

        likeFilter = F('term', type='like')
        return self.get_followers_active(author_id, min_interactions, likeFilter)


    def get_followers_active_shares(self, author_id, min_interactions=0.05):
        """Get users who have shared more than given percentage of posts made by the author

        :param author_id: user ID you want to analyze
        :param min_interactions: percentage of the minimum interactions over all posts (default: 0.05, i.e 5%)
        :return dictionary of user objects
        """

        shareFilter = F('term', type='share')
        return self.get_followers_active(author_id, min_interactions, shareFilter)


    def get_followers_active_comments(self, author_id, min_interactions=0.05):
        """Get users who have commented on more than given percentage of posts made by the author

        :param author_id: user ID you want to analyze
        :param min_interactions: percentage of the minimum interactions over all posts (default: 0.05, i.e 5%)
        :return dictionary of user objects
        """

        commentFilter = F('term', type='comment')
        return self.get_followers_active(author_id, min_interactions, commentFilter)


    def get_top_likers_commenters(self, author_id, min_interactions=0.05):
        """ Return people who are amongst top likers and top commenters (intersection)

        :param author_id: author's ID you want to analyze
        :param min_interactions: percentage of the minimum interactions over all posts (default: 0.05, i.e 5%)
        :return list of user IDs
        """

        likers = self.get_followers_active_likes(author_id, min_interactions)
        commenters = self.get_followers_active_comments(author_id, min_interactions)

        users = []
        for liker in likers:
            if liker in commenters:
                users.append(liker)

        return users


    def get_top_likers_sharers(self, author_id, min_interactions=0.05):
        """ Return people who are amongst top likers and top sharers (intersection)

        :param author_id: author's ID you want to analyze
        :param min_interactions: percentage of the minimum interactions over all posts (default: 0.05, i.e 5%)
        :return list of user IDs
        """

        likers = self.get_followers_active_likes(author_id, min_interactions)
        sharers = self.get_followers_active_shares(author_id, min_interactions)

        users = []
        for liker in likers:
            if liker in sharers:
                users.append(liker)

        return users


    def get_top_commenters_sharers(self, author_id, min_interactions=0.05):
        """ Return people who are amongst top commenters and top sharers (intersection)

        :param author_id: author's ID you want to analyze
        :param min_interactions: percentage of the minimum interactions over all posts (default: 0.05, i.e 5%)
        :return list of user IDs
        """

        commenters = self.get_followers_active_comments(author_id, min_interactions)
        sharers = self.get_followers_active_shares(author_id, min_interactions)

        users = []
        for commenter in commenters:
            if commenter in sharers:
                users.append(commenter)

        return users


    def get_top_likers_commenters_sharers(self, author_id, min_interactions=0.05):
        """ Return people who are amongst top commenters top likers and top sharers (intersection)

        :param author_id: author's ID you want to analyze
        :param min_interactions: percentage of the minimum interactions over all posts (default: 0.05, i.e 5%)
        :return list of user IDs
        """

        likers = self.get_followers_active_likes(author_id, min_interactions)
        commenters = self.get_followers_active_comments(author_id, min_interactions)
        sharers = self.get_followers_active_shares(author_id, min_interactions)

        users = []
        for liker in likers:
            if liker in sharers and liker in commenters:
                users.append(liker)

        return users


    def save_followers_cross_active(self, author_ids, min_interactions=0.05, add_filter=None, sub_dir=None,
                                    filename=None, headline=None):
        """ Save people who are amongst top interactors between these authors

        :param author_ids: list of at least two author IDs
        :param min_interactions: percentage of the minimum interactions over all posts (default: 0.05, i.e 5%)
        :param add_filter: additional filter you want to use
        :param sub_dir: where to save the report file
        :param filename: name of the report file
        :param headline: headline before the printed data
        """

        if len(author_ids) < 2:
            raise ValueError("You must specify 2 or more authors for cross activity check")

        if not filename:
            filename = "cross_active_people.txt"

        if sub_dir:
            os.makedirs(sub_dir, exist_ok=True)
            filename = os.path.join(sub_dir, filename)

        if not headline:
            headline = "Active people"

        # Save author names and their active users so we don't have to query it multiple times
        authorNames = {}
        authorFollowers = {}
        for authorId in author_ids:
            authorNames[authorId] = self.get_author_name(authorId)
            authorFollowers[authorId] = set(self.get_followers_active(authorId, min_interactions, add_filter))

        # List of sets with possible combinations with 2 or more elements
        combinations = []
        for l in range(2, len(author_ids)+1):
            for subset in itertools.combinations(author_ids, l):
                combinations.append(subset)

        with open(filename, 'w') as fp:
            for comb in combinations:
                authorString = " ".join([ authorNames[authorId] for authorId in comb ])
                fp.write("{0} on these authors: {1}\n".format(headline, authorString))
                users = authorFollowers[comb[0]]  # first author
                for i in range(1, len(comb)):
                    newUsers = authorFollowers[comb[i]]
                    users.intersection_update(newUsers)
                fp.write("    Count: {0}\n".format(len(users)))


    def save_followers_cross_likers(self, author_ids, min_interactions=0.05, sub_dir=None):
        """ Save people who are amongst top likers between these authors

        :param author_ids: list of at least two author IDs
        :param min_interactions: percentage of the minimum interactions over all posts (default: 0.05, i.e 5%)
        :param sub_dir: where to save the report file
        """

        likeFilter = F('term', type='like')
        filename = "cross_active_likers.txt"
        headline = "Active likers"
        return self.save_followers_cross_active(author_ids, min_interactions, likeFilter, sub_dir=sub_dir,
                                                filename=filename, headline=headline)


    def save_followers_cross_sharers(self, author_ids, min_interactions=0.05, sub_dir=None):
        """ Save people who are amongst top sharers between these authors

        :param author_ids: list of at least two author IDs
        :param min_interactions: percentage of the minimum interactions over all posts (default: 0.05, i.e 5%)
        :param sub_dir: where to save the report file
        """

        shareFilter = F('term', type='share')
        filename = "cross_active_sharers.txt"
        headline = "Active sharers"
        return self.save_followers_cross_active(author_ids, min_interactions, shareFilter, sub_dir=sub_dir,
                                                filename=filename, headline=headline)


    def save_followers_cross_commenters(self, author_ids, min_interactions=0.05, sub_dir=None):
        """ Save people who are amongst top commenters between these authors

        :param author_ids: list of at least two author IDs
        :param min_interactions: percentage of the minimum interactions over all posts (default: 0.05, i.e 5%)
        :param sub_dir: where to save the report file
        """

        commentFilter = F('term', type='comment')
        filename = "cross_active_commenters.txt"
        headline = "Active commenters"
        return self.save_followers_cross_active(author_ids, min_interactions, commentFilter, sub_dir=sub_dir,
                                                filename=filename, headline=headline)


    def get_posts_most_popular(self, author_id, count=20):
        """ Get most popular (likes, shares, comments) posts from the author.

        :param author_id: user ID you want to analyze
        :param count: how many most popular posts you want to return
        :return dictionary with post ID's as key and number of total interactions as value
        """

        # Create a terms aggregation with buckets by status_id, i.e. aggregate all interactions that
        # have the same status_id in one bucket

        ess = self.get_es_search()

        aggregationName = 'terms_status_id'
        termsStatusIdAgg = A('terms', field='status_id', size=count)
        ess.aggs.bucket(aggregationName, termsStatusIdAgg)
        ess = ess.params(size=0)  # we don't care about the hits

        statusAuthorFilter = F('term', status_author=author_id)

        # Results are in the aggregations
        response = self.execute_es_request(ess, doc_type=self.doc_type_interaction, es_filter=statusAuthorFilter)
        if not response.success():
            raise RuntimeError('Request failed')

        responseDict = response.to_dict()
        posts = {}
        # Iterate over fetched ids
        for bucket in responseDict['aggregations'][aggregationName]['buckets']:
            postId = bucket['key']
            try:
                posts[postId] = bucket['doc_count']
            except EsNotFoundError:  # maybe the post is not in the ES, ignore
                continue

        return posts


    def get_links_most_popular(self, author_id, count=20):
        """ Get most popular links published by the author

        :param author_id: user ID you want to analyze
        :param count: how many most popular links you want to return
        :return dict with links as keys and number of occurrences as values
        """

        ess = self.get_es_search()
        #Algorithm: fetch most popular posts, fetch one full post, check if it has a link field, append and continue

        # Be optimistic and expect at least one fifth of the author's posts are links
        aggregationName = 'terms_status_id'
        termsStatusIdAgg = A('terms', field='status_id', size=5*count)
        ess.aggs.bucket(aggregationName, termsStatusIdAgg)
        ess = ess.params(size=0)  # we don't care about the hits

        statusAuthorFilter = F('term', status_author=author_id)

        response = self.execute_es_request(ess, doc_type=self.doc_type_interaction, es_filter=statusAuthorFilter)
        if not response.success():
            raise RuntimeError('Request failed')
        responseDict = response.to_dict()
        popularLinks = {}

        # Iterate over fetched ids and download full posts
        for bucket in responseDict['aggregations'][aggregationName]['buckets']:
            postId = bucket['key']
            try:
                post = self.get_post(postId)
                link = post['_source'].get('link', None)
                if link:  # given post has a link, save it
                    popularLinks[link] = bucket['doc_count']
            except EsNotFoundError:  # maybe the post is not in the ES, ignore
                continue

            if len(popularLinks) >= count:  # we've got our desired count
                break

        return popularLinks


    def get_domains_most_published(self, author_id, status_count=1000):
        """ Get most published domains (from links) by the author

        :param author_id: user ID you want to analyze
        :param status_count: how many last statuses you want to analyze for links
        :return unordered dict where domains are keys, values are number of occurrences
        """

        # Fetch all author's posts and analyze the results here
        ess = self.get_es_search()

        # Fetch only the 'link' field
        ess = ess.params(fields=['link'], size=status_count)
        ess = ess.sort('-created_time')  # Fetch only the newest items

        authorFilter = F('term', author=author_id) & F('exists', field='link')

        response = self.execute_es_request(ess, doc_type=self.doc_type_post, es_filter=authorFilter)
        if not response.success():
            raise RuntimeError('Unable to complete the request')

        domains = {}
        for post in response.hits:
            # Parse the url and extract the domain
            res = urllib.parse.urlparse(post['link'][0])  # '[0]' because a list is returned
            domain = res.netloc.replace("www.", "", 1)  # erase 'www.'

            if STRIP_DOMAINS:  # Strip to a top level domain, e.g. blog.ihned.cz --> ihned.cz
                parts = len(domain.split('.'))  # count of parts delimited by dot
                if parts > 2:  # we had some subdomains
                    splitDomain = domain.split('.')
                    domain = "{0}.{1}".format(splitDomain[-2], splitDomain[-1])  # take two top level elements
            if domain not in domains:
                domains[domain] = 1
            else:
                domains[domain] += 1

        return domains


    def get_average_interactions_per_fan(self, author_id):
        """ Return ratio between number of author's interactions and fan count

        :param author_id: user ID you want to analyze
        :return: number of average interactions per fan (float)
        """

        fans = self.get_author_fan_count(author_id)
        interactions = self.get_count_interactions(author_id)

        return interactions / fans


    def get_average_likes_per_fan(self, author_id):
        """ Return ratio between number of author's likes and fan count

        :param author_id: user ID you want to analyze
        :return: number of average likes per fan (float)
        """

        fans = self.get_author_fan_count(author_id)
        likes = self.get_count_all_likes(author_id)

        return likes / fans


    def get_average_comments_per_fan(self, author_id):
        """ Return ratio between number of comments and fan count

        :param author_id: user ID you want to analyze
        :return: number of average likes per fan (float)
        """

        fans = self.get_author_fan_count(author_id)
        comments = self.get_count_all_comments(author_id)

        return comments / fans


    def get_average_shares_per_fan(self, author_id):
        """ Return ratio between number of author's shares and fan count

        :param author_id: user ID you want to analyze
        :return: number of average interactions per fan (float)
        """

        fans = self.get_author_fan_count(author_id)
        shares = self.get_count_all_shares(author_id)

        return shares / fans


    def get_count_interactions(self, author_id):
        """ Return number of people, who interacted with the author's content

        :param author_id: user ID you want to analyze
        :return total interaction count (int)
        """

        ess = self.get_es_search()
        # Get number of interactions, where the status_author is our given one
        ess = ess.params(size=0)  # do not return hits, just the count
        statusAuthorFilter = F('term', status_author=author_id)

        response = self.execute_es_request(ess, doc_type=self.doc_type_interaction, es_filter=statusAuthorFilter)

        return response.hits.total


    def get_count_post_reach(self, post_id):
        """ Return number of people, who interacted with the author's content

        :param author_id: user ID you want to analyze
        :return total number of unique people who interacted
        """

        #TODO fix
        ess = self.get_es_search()
        # Get number of interactions, where the status_author is our given one
        ess = ess.params(size=0)  # do not return hits, just the count
        statusAuthorFilter = F('term', status_author=post_id)

        response = self.execute_es_request(ess, doc_type=self.doc_type_interaction, es_filter=statusAuthorFilter)

        return response.hits.total


    def get_comments_time_deltas(self, author_id, status_count=1000):
        """ Return deltas between post publication and comment creation

        :param author_id: author's ID
        :param status_count: how many last statuses you want to analyze
        :return: list of deltas (in seconds)
        """

        posts = self.get_posts(author_id, status_count)

        deltas = []
        for post in posts:
            postCreated = dateutil.parser.parse(post['created_time'])
            comments = self.get_comments_for_post(post['id'])
            for comment in comments:
                commentCreated = dateutil.parser.parse(comment['created_time'])
                delta = commentCreated - postCreated
                if delta.total_seconds() < 0:  # invalid value, ignore it
                    continue
                deltas.append(delta.total_seconds())

        return deltas


    def get_average_comments_time_delta(self, author_id, status_count=1000):
        """ Return average time after which comments are published after post's publication

        :param author_id: author's ID
        :param status_count: how many last statuses you want to analyze
        :return: average in seconds
        """

        deltas = self.get_comments_time_deltas(author_id, status_count)
        return sum(deltas) / len(deltas)


    def get_posts_time_distribution(self, author_id, status_count=1000):
        """ Get normalized time distribution of all posts the author has published

        :param author_id: author's ID
        :param status_count: how many last statuses you want to analyze
        :return: tuple(day_distribution, hour_distribution, minute_distribution) (lists)
        """

        postCount = self.get_count_all_posts(author_id)

        ess = self.get_es_search()
        ess = ess.sort('-created_time')  # sort from newest to oldest
        ess = ess.params(fields=['created_time'], size=status_count)
        statusAuthorLikeFilter = F('term', author=author_id)

        response = self.execute_es_request(ess, doc_type=self.doc_type_post, es_filter=statusAuthorLikeFilter)

        if not response.success():
            raise RuntimeError('Request failed')

        times = []
        for hit in response.hits:
            times.append(hit['created_time'][0])  # '[0]' because a list is returned

        # Initialize datetime lists to zero
        days = 7 * [0]
        hours = 24 * [0]
        minutes = 60 * [0]

        for time in times:
            time = dateutil.parser.parse(time)
            days[time.weekday()] += 1
            hours[time.hour] += 1
            minutes[time.minute] += 1

        # Normalize the values
        days = [ v / postCount for v in days]
        hours = [v / postCount for v in hours]
        minutes = [v / postCount for v in minutes]

        return days, hours, minutes


    def plot_comments_delta_distribution(self, author_ids, status_count=1000, sub_dir=None):
        """ Plot time delta distribution of comments on author's posts

        :param author_ids: ID of the author(s) (string or list/tuple)
        :param status_count: how many last statuses you want to analyze
        :param sub_dir: sub directory to save the plot in, created if necessary
        """

        author_ids = self._get_author_id_list(author_ids)
        if len(author_ids) > 20:
            print("Too many authors given, analyzing only first 20")
            author_ids = author_ids[:20]

        pyplot.figure(figsize=(10,10))
        pyplot.suptitle('When comments are published')

        self._get_line_style(reset=True)

        # For each author, fetch the data and plot the line into subplots
        authorNamesAscii = []
        for authorId in author_ids:
            authorName = self.get_author_name(authorId, ensure_ascii=False)
            authorNamesAscii.append(self.get_author_name(authorId, ensure_ascii=True))
            lineStyle = self._get_line_style()

            splot = pyplot.subplot(111)

            # Get deltas in seconds and create hourly buckets
            deltas = self.get_comments_time_deltas(authorId, status_count)
            # Fill buckets
            maxHour = 24*3  # three days
            buckets = [0] * maxHour
            overValues = 0

            for delta in deltas:
                hours = int(delta / (60*60))
                if hours >= maxHour:  # crop too high values
                    overValues += 1
                    continue
                try:
                    buckets[hours] += 1
                except IndexError:
                    continue  # ignore potential off values

            buckets.append(overValues)

            # Transform values into percentages
            bucketSum = sum(buckets)
            for i, value in enumerate(buckets):
                buckets[i] = 100 * value / bucketSum

            xAxis = [x for x in range(len(buckets))]
            xTicks = [ str(x) for x in range(len(buckets)) ]
            xTicks[-1] = ">{0}".format(maxHour)

            pyplot.ylabel('Percentage of comments')
            pyplot.xlabel("Hours after post's publication")
            pyplot.xticks(xAxis, xTicks)
            pyplot.plot(xAxis, buckets, lineStyle, label=authorName)

            # Hide all ticks but multiplies of 5 and the last one
            for label in splot.get_xticklabels():
                label.set_visible(False)
            for label in splot.get_xticklabels()[::5]:
                label.set_visible(True)
            splot.get_xticklabels()[-1].set_visible(True)

            pyplot.ylim(ymin=0)  # start y-axis at 0
            pyplot.legend()

        pyplot.tight_layout()
        pyplot.subplots_adjust(top=0.88)  # make space for the title

        filename = "comments_time_delta_distribution.svg"
        if sub_dir:
            os.makedirs(sub_dir, exist_ok=True)
            filename = os.path.join(sub_dir, filename)

        pyplot.savefig(filename, dpi=600)
        pyplot.close(pyplot.gcf())


    def plot_posts_time_distribution(self, author_ids, status_count=1000, sub_dir=None):
        """ Plot time distribution of author's posts

        :param author_ids: ID of the author(s) (string or list/tuple)
        :param status_count: how many last statuses you want to analyze
        :param sub_dir: sub directory to save the plot in, created if necessary
        """

        author_ids = self._get_author_id_list(author_ids)
        if len(author_ids) > 20:
            print("Too many authors given, analyzing only first 20")
            author_ids = author_ids[:20]

        pyplot.figure(figsize=(10,10))
        pyplot.suptitle('Post time distribution')

        self._get_line_style(reset=True)

        # For each author, fetch the data and plot the line into subplots
        authorNamesAscii = []
        for authorId in author_ids:
            authorName = self.get_author_name(authorId, ensure_ascii=False)
            authorNamesAscii.append(self.get_author_name(authorId, ensure_ascii=True))
            lineStyle = self._get_line_style(marker_style='+')
            days, hours, minutes = self.get_posts_time_distribution(authorId, status_count)

            # Days subplot
            pyplot.subplot(311)
            xTicksDays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            xAxis = [ x for x in range(7) ]
            yAxis = [ v for v in days ]
            pyplot.ylabel('Posts published (%)')
            pyplot.xlabel('Day of the week')
            pyplot.plot(xAxis, yAxis, lineStyle, label=authorName)
            pyplot.legend()
            pyplot.xticks(xAxis, xTicksDays)

            # Hours subplot
            pyplot.subplot(312)
            xAxis = [ x for x in range(24) ]
            yAxis = [ v for v in hours ]
            pyplot.ylabel('Posts published (%)')
            pyplot.xlabel('Hour of the day')
            pyplot.xticks(xAxis, xAxis)
            pyplot.plot(xAxis, yAxis, lineStyle, label=authorName)
            pyplot.legend()

            # Minutes subplot
            pyplot.subplot(313)
            xAxis = [ x for x in range(60) ]
            yAxis = [ v for v in minutes ]
            pyplot.ylabel('Posts published (%)')
            pyplot.xlabel('Minute of the day')
            pyplot.plot(xAxis, yAxis, lineStyle, label=authorName)
            pyplot.legend()

        pyplot.tight_layout()
        pyplot.subplots_adjust(top=0.88)  # make space for the title

        filename = "posts_time_distribution.svg"
        if sub_dir:
            os.makedirs(sub_dir, exist_ok=True)
            filename = os.path.join(sub_dir, filename)

        pyplot.savefig(filename, dpi=600)
        pyplot.close(pyplot.gcf())


    def plot_likes_distribution(self, author_ids, status_count=1000, sub_dir=None):
        """ Plot likes distribution over posts

        :param author_ids: ID of the author(s) (string or list/tuple)
        :param status_count: how many last statuses you want to analyze
        :param sub_dir: sub directory to save the plot in, created if necessary
        """

        author_ids = self._get_author_id_list(author_ids)
        if len(author_ids) > 20:
            print("Too many authors given, analyzing only first 20")
            author_ids = author_ids[:20]

        self._get_line_style(reset=True)

        pyplot.figure(figsize=(10,10))
        pyplot.suptitle('Post likes distribution')

        # Get all posts (sorted by time), get like count for each plot (y axis) and plot
        authorNamesAscii = []
        for authorId in author_ids:
            authorName = self.get_author_name(authorId, ensure_ascii=False)
            authorNamesAscii.append(self.get_author_name(authorId, ensure_ascii=True))
            lineStyle = self._get_line_style()

            posts = self.get_posts(authorId, status_count)
            postLikes = []
            postDates = []
            for post in posts:
                likes = self.get_count_likes_for_post(post['id'])
                postLikes.append(likes)
                postDates.append(dateutil.parser.parse(post['created_time']))

            # We received the posts in newest to oldest order, but we want to plot the oldest first --> reverse the list
            postLikes.reverse()
            postDates.reverse()

            # Plot posts with date stamps
            pyplot.subplot(211)
            pyplot.title('Post likes in time')
            pyplot.xlabel('Date')
            pyplot.ylabel('Likes')
            postDates = matplotlib.dates.date2num(postDates)
            pyplot.plot_date(postDates, postLikes, lineStyle, label=authorName)
            pyplot.legend()

            # Plot posts evenly distributed
            pyplot.subplot(212)
            xAxis = [ x for x in range(len(postLikes)) ]
            pyplot.title('Change of post likes')
            pyplot.xlabel('Post number')
            pyplot.ylabel('Likes')
            pyplot.plot(xAxis, postLikes, lineStyle, label=authorName)

            # Linear regression curve
            coefs = poly.polyfit(xAxis, postLikes, 1)
            ffit = poly.polyval(xAxis, coefs)
            linRegLabel = "{0} lin. reg. ({1:.2f}x{2:+.2f})".format(authorName, coefs[1], coefs[0])
            pyplot.plot(xAxis, ffit, self._get_line_style(), label=linRegLabel)

            pyplot.legend()

        pyplot.tight_layout()
        pyplot.subplots_adjust(top=0.88)  # make space for the title

        filename = "likes_distribution.svg"
        if sub_dir:
            os.makedirs(sub_dir, exist_ok=True)
            filename = os.path.join(sub_dir, filename)

        pyplot.savefig(filename, dpi=600)
        pyplot.close(pyplot.gcf())


    def plot_comments_distribution(self, author_ids, status_count=1000, sub_dir=None):
        """ Plot distribution of comments count for the given user(s)

        :param author_ids: ID of the author(s) (string or list/tuple)
        :param status_count: how many last statuses you want to analyze
        :param sub_dir: sub directory to save the plot in, created if necessary
        """

        author_ids = self._get_author_id_list(author_ids)

        # Get author's posts and for each post get comment count
        pyplot.figure(figsize=(10, 10))
        pyplot.suptitle('Number of post comments over time')

        self._get_line_style(reset=True)

        authorNamesAscii = []
        for authorId in author_ids:
            authorName = self.get_author_name(authorId, ensure_ascii=False)
            authorNamesAscii.append(self.get_author_name(authorId, ensure_ascii=True))
            lineStyle = self._get_line_style()

            posts = self.get_posts(authorId, status_count)
            postComments = []
            postDates = []
            for post in posts:
                commentCount = self.get_count_comments_for_post(post['id'])
                postComments.append(commentCount)
                postDates.append(dateutil.parser.parse(post['created_time']))

            # We received the posts in newest to oldest order, but we want to plot the oldest first --> reverse the list
            postComments.reverse()
            postDates.reverse()

            # Plot posts with date stamps
            pyplot.subplot(211)
            pyplot.title('Post comments in time')
            pyplot.xlabel('Date')
            pyplot.ylabel('Comments')
            postDates = matplotlib.dates.date2num(postDates)
            pyplot.plot_date(postDates, postComments, lineStyle, label=authorName)
            pyplot.legend()

            # Plot posts evenly distributed
            pyplot.subplot(212)
            xAxis = [ x for x in range(len(postComments)) ]
            pyplot.title('Change of post comments')
            pyplot.xlabel('Post number')
            pyplot.ylabel('Comments')
            pyplot.plot(xAxis, postComments, lineStyle, label=authorName)

            # Linear regression curve
            coefs = poly.polyfit(xAxis, postComments, 1)
            ffit = poly.polyval(xAxis, coefs)
            linRegLabel = "{0} lin. reg. ({1:.2f}x{2:+.2f})".format(authorName, coefs[1], coefs[0])
            pyplot.plot(xAxis, ffit, self._get_line_style(), label=linRegLabel)

            pyplot.legend()

        pyplot.tight_layout()
        pyplot.subplots_adjust(top=0.88)  # make space for the title

        filename = "comments_distribution.svg"
        if sub_dir:
            os.makedirs(sub_dir, exist_ok=True)
            filename = os.path.join(sub_dir, filename)

        pyplot.savefig(filename, dpi=600)
        pyplot.close(pyplot.gcf())


    def plot_comment_len_distribution(self, author_ids, status_count=1000, sub_dir=None):
        """ Plot distribution of average comment length for the given user(s)

        :param author_ids: ID of the author(s) (string or list/tuple)
        :param status_count: how many last statuses you want to analyze
        :param sub_dir: sub directory to save the plot in, created if necessary
        """

        author_ids = self._get_author_id_list(author_ids)

        # Get author's posts and for each post get comment count
        pyplot.figure(figsize=(10, 10))
        pyplot.suptitle('Average comment length over time')

        self._get_line_style(reset=True)

        authorNamesAscii = []
        for authorId in author_ids:
            authorName = self.get_author_name(authorId, ensure_ascii=False)
            authorNamesAscii.append(self.get_author_name(authorId, ensure_ascii=True))
            lineStyle = self._get_line_style()

            posts = self.get_posts(authorId, status_count)
            postComments = []
            postDates = []
            for post in posts:
                avgComment = self.get_average_comment_len_for_post(post['id'])
                postComments.append(avgComment)
                postDates.append(dateutil.parser.parse(post['created_time']))

            # We received the posts in newest to oldest order, but we want to plot the oldest first --> reverse the list
            postComments.reverse()
            postDates.reverse()

            # Plot posts with date stamps
            pyplot.subplot(211)
            pyplot.title('Average comment length in time')
            pyplot.xlabel('Date')
            pyplot.ylabel('Avg comment length')
            postDates = matplotlib.dates.date2num(postDates)
            pyplot.plot_date(postDates, postComments, lineStyle, label=authorName)
            pyplot.legend()

            # Plot posts evenly distributed
            pyplot.subplot(212)
            xAxis = [ x for x in range(len(postComments)) ]
            pyplot.title("Change of average comment's length")
            pyplot.xlabel('Post number')
            pyplot.ylabel('Avg comment length')
            pyplot.plot(xAxis, postComments, lineStyle, label=authorName)

            # Linear regression curve
            coefs = poly.polyfit(xAxis, postComments, 1)
            ffit = poly.polyval(xAxis, coefs)
            linRegLabel = "{0} lin. reg. ({1:.2f}x{2:+.2f})".format(authorName, coefs[1], coefs[0])
            pyplot.plot(xAxis, ffit, self._get_line_style(), label=linRegLabel)

            pyplot.legend()

        pyplot.tight_layout()
        pyplot.subplots_adjust(top=0.88)  # make space for the title

        filename = "comments_len_distribution.svg"
        if sub_dir:
            os.makedirs(sub_dir, exist_ok=True)
            filename = os.path.join(sub_dir, filename)

        pyplot.savefig(filename, dpi=600)
        pyplot.close(pyplot.gcf())


    def plot_comment_sentiment_distribution(self, author_ids, status_count=1000, sub_dir=None):
        """ Plot distribution of comments' sentiment for the given user(s)

        :param author_ids: ID of the author(s) (string or list/tuple)
        :param status_count: how many last statuses you want to analyze
        :param sub_dir: sub directory to save the plot in, created if necessary
        """

        author_ids = self._get_author_id_list(author_ids)

        # Get author's posts and for each post get comment count
        pyplot.figure(figsize=(10,10))
        pyplot.suptitle('Sentiment of comments over time')

        self._get_line_style(reset=True)

        authorNamesAscii = []
        for authorId in author_ids:
            authorName = self.get_author_name(authorId, ensure_ascii=False)
            authorNamesAscii.append(self.get_author_name(authorId, ensure_ascii=True))
            lineStyle = self._get_line_style()

            posts = self.get_posts(authorId, status_count)
            postSentiments = []
            postDates = []
            for post in posts:
                postSentiment = self.get_sentiment_for_post(post['id'])
                postSentiments.append(postSentiment)
                postDates.append(dateutil.parser.parse(post['created_time']))

            # We received the posts in newest to oldest order, but we want to plot the oldest first --> reverse the list
            postSentiments.reverse()
            postDates.reverse()

            # Plot posts with date stamps
            pyplot.subplot(211)
            pyplot.title("Comments' sentiment in time")
            pyplot.xlabel('Date')
            pyplot.ylabel('Sentiment')
            postDates = matplotlib.dates.date2num(postDates)
            pyplot.plot_date(postDates, postSentiments, lineStyle, label=authorName)
            pyplot.legend()

            # Plot posts evenly distributed
            pyplot.subplot(212)
            xAxis = [ x for x in range(len(postSentiments)) ]
            pyplot.title("Change of comments' sentiment")
            pyplot.xlabel('Post number')
            pyplot.ylabel('Sentiment')
            pyplot.plot(xAxis, postSentiments, lineStyle, label=authorName)

            # Linear regression curve
            coefs = poly.polyfit(xAxis, postSentiments, 1)
            ffit = poly.polyval(xAxis, coefs)
            linRegLabel = "{0} lin. reg. ({1:.2f}x{2:+.2f})".format(authorName, coefs[1], coefs[0])
            pyplot.plot(xAxis, ffit, self._get_line_style(), label=linRegLabel)

            pyplot.legend()

        pyplot.tight_layout()
        pyplot.subplots_adjust(top=0.88)  # make space for the title

        filename = "comment_sentiment_distribution.svg"
        if sub_dir:
            os.makedirs(sub_dir, exist_ok=True)
            filename = os.path.join(sub_dir, filename)

        pyplot.savefig(filename, dpi=600)
        pyplot.close(pyplot.gcf())


    def print_latest_post(self, author_id):
        print("Author:", self.get_author_name(author_id))
        post = self.get_newest_post(author_id)
        if post:
            print("\tLatest post:", post['created_time'])
        else:
            print("\tNo posts")


    def save_followers_most_active(self, author_id, filename=None, sub_dir=None):
        """ Save most published domains by the user

        :param author_id: user you want to analyze
        :param filename: output file
        :param sub_dir: sub directory to save the file in, it's created if necessary
        """

        if not filename:
            filename = 'followers_most_active.txt'  # default filename

        if sub_dir:
            os.makedirs(sub_dir, exist_ok=True)
            filename = os.path.join(sub_dir, filename)

        with open(filename, 'w') as fp:
            fp.write("Most active followers of user {0}\n\n".format(author_id))
            postCount = self.get_count_all_posts(author_id)
            fp.write("Total post count: {0}\n\n".format(postCount))

            followers = self.get_followers_most_active(author_id, 100)
            # Get the longest key, for alignment
            longest = max( map(len, followers) ) + 1

            # Print most active follower with a number of occurrences, e.g.: '1245636 507'
            for k in sorted(followers, key=followers.get, reverse=True):
                spaces = ' ' * (longest - len(k))
                fp.write("{0}{1}{2}\n".format(k, spaces, followers[k]))


    def save_domains_most_published(self, author_id, filename=None, sub_dir=None, status_count=1000):
        """ Save most published domains by the user

        :param author_id: user you want to analyze
        :param filename: output file
        :param sub_dir: sub directory to save the file in, it's created if necessary
        :param status_count: how many last statuses you want to analyze
        """

        if not filename:
            filename = 'domains_most_published.txt'  # default filename

        if sub_dir:
            os.makedirs(sub_dir, exist_ok=True)
            filename = os.path.join(sub_dir, filename)

        with open(filename, 'w') as fp:
            fp.write("Most published domains by user {0}\n\n".format(author_id))
            domains = self.get_domains_most_published(author_id, status_count=status_count)

            # Get the longest key, for alignment
            longest = max( map(len, domains) ) + 1

            # Print most published domain with a number of occurrences, e.g.: 'facebook.com 507'
            for k in sorted(domains, key=domains.get, reverse=True):
                spaces = ' ' * (longest - len(k))
                fp.write("{0}{1}{2}\n".format(k, spaces, domains[k]))


    def save_links_most_popular(self, author_id, filename=None, sub_dir=None):
        """ Save most popular links published by the user

        :param author_id: user you want to analyze
        :param filename: output file
        :param sub_dir: sub directory to save the file in, it's created if necessary
        """

        if not filename:
            filename = 'links_most_popular.txt'  # default filename

        if sub_dir:
            os.makedirs(sub_dir, exist_ok=True)
            filename = os.path.join(sub_dir, filename)

        with open(filename, 'w') as fp:
            fp.write("Most popular links published by user {0}\n\n".format(author_id))
            links = self.get_links_most_popular(author_id, 100)

            # Get the longest key, for alignment
            longest = max( map(len, links) ) + 1

            # Print most popular links with a number of occurrences, e.g.: 'facebook.com/1234 507'
            for k in sorted(links, key=links.get, reverse=True):
                spaces = ' ' * (longest - len(k))
                fp.write("{0}{1}{2}\n".format(k, spaces, links[k]))


    def save_posts_most_popular(self, author_id, filename=None, sub_dir=None):
        """ Save most popular posts published by the user

        :param author_id: user you want to analyze
        :param filename: output file
        :param sub_dir: sub directory to save the file in, it's created if necessary
        """

        if not filename:
            filename = 'posts_most_popular.txt'  # default filename

        if sub_dir:
            os.makedirs(sub_dir, exist_ok=True)
            filename = os.path.join(sub_dir, filename)

        with open(filename, 'w') as fp:
            fp.write("Most popular posts published by user {0}\n\n".format(author_id))
            posts = self.get_posts_most_popular(author_id, 100)

            # Get the longest key, for alignment
            longest = max( map(len, posts) ) + 1

            # Print most published domain with a number of occurrences, e.g.: 'facebook.com 507'
            for k in sorted(posts, key=posts.get, reverse=True):
                spaces = ' ' * (longest - len(k))
                fp.write("{0}{1}{2}\n".format(k, spaces, posts[k]))


    def save_overall_stats(self, author_id, filename=None, sub_dir=None):
        """ Fetch and save overall statistics for the given user

        :param author_id: user you want to analyze
        :param filename: output file
        :param sub_dir: sub directory to save the file in, it's created if necessary
        """

        if not filename:
            filename = 'stats_overall.txt'  # default filename

        if sub_dir:
            os.makedirs(sub_dir, exist_ok=True)
            filename = os.path.join(sub_dir, filename)

        with open(filename, 'w') as fp:
            fp.write("Overall statistics for user {0}\n\n".format(author_id))
            postCount = self.get_count_all_posts(author_id)
            fp.write("Total post count: {0}\n".format(postCount))
            likeCount = self.get_count_all_likes(author_id)
            fp.write("Total like count: {0}\n".format(likeCount))
            shareCount = self.get_count_all_shares(author_id)
            fp.write("Total share count: {0}\n".format(shareCount))
            avgLikes = self.get_average_likes(author_id)
            fp.write("Average like count per status: {0:.2f}\n".format(avgLikes))
            avgShares = self.get_average_shares(author_id)
            fp.write("Average share count per status: {0:.2f}\n".format(avgShares))
            avgComments = self.get_average_comments(author_id)
            fp.write("Average comment count per status: {0:.2f}\n".format(avgComments))
            avgCommentTime = self.get_average_comments_time_delta(author_id) / 60 / 60
            fp.write("\tAverage time of a comment creation after post's publication (hours): {0:.2f}\n".format(
                      avgCommentTime))


    def save_text_results(self, author_id, sub_dir=None, status_count=1000):
        """ Save text like results for the given user

        :param author_id: user you want to analyze
        :param sub_dir: sub directory to save the file in, it's created if necessary
        :param status_count: how many last statuses you want to analyze
        """

        print("Saving statistics for author {0}... ".format(self.get_author_name(author_id)), end='', flush=True)

        self.save_overall_stats(author_id, sub_dir=sub_dir)
        self.save_domains_most_published(author_id, sub_dir=sub_dir, status_count=status_count)
        self.save_links_most_popular(author_id, sub_dir=sub_dir)
        self.save_posts_most_popular(author_id, sub_dir=sub_dir)
        self.save_followers_most_active(author_id, sub_dir=sub_dir)

        print("done")


    def save_cross_activity_results(self, author_ids, sub_dir=None, min_interactions=0.05):
        """ Save cross activity results for the given users

        :param author_ids: list of users you want to analyze
        :param sub_dir: sub directory to save the file in, it's created if necessary
        :param min_interactions: percentage of the minimum interactions over all posts (default: 0.05, i.e 5%)
        """

        print("Saving cross activity statistics for specified authors... ", end='', flush=True)

        self.save_followers_cross_active(author_ids, sub_dir=sub_dir, min_interactions=min_interactions)
        self.save_followers_cross_likers(author_ids, sub_dir=sub_dir, min_interactions=min_interactions)
        self.save_followers_cross_sharers(author_ids, sub_dir=sub_dir, min_interactions=min_interactions)
        self.save_followers_cross_commenters(author_ids, sub_dir=sub_dir, min_interactions=min_interactions)

        print("done")


    def get_fan_activity_results(self, author_id, min_interactions=0.05):
        """ Get fan activity results for the given user

        :param author_id: user you want to analyze
        :param min_interactions: percentage of the minimum interactions over all posts (default: 0.05, i.e 5%)
        :return: string with fan activity report
        """

        authorName = self.get_author_name(author_id)
        fans = self.get_author_fan_count(author_id)
        reach = self.get_count_interactions(author_id)

        minIntPct = int(min_interactions * 100)

        res = "Author: {0}\n".format(authorName)
        res += "\tFan count: {0}\n".format(fans)
        res += "\tInteraction count: {0}\n".format(reach)
        res += "\tInteraction per fan: {0:.2f}\n".format(self.get_average_interactions_per_fan(author_id))
        res += "\tLikes per fan: {0:.2f}\n".format(self.get_average_likes_per_fan(author_id))
        res += "\tShares per fan: {0:.2f}\n".format(self.get_average_shares_per_fan(author_id))
        res += "\tComments per fan: {0:.2f}\n".format(self.get_average_comments_per_fan(author_id))

        activeUsersCnt = len(self.get_followers_active(author_id))
        res += "\nStats of users who interacted on more than {0}% of all author's posts\n".format(minIntPct)
        res += "User count: {0} ({1:.2f}% of fans)\n".format(activeUsersCnt, 100*activeUsersCnt / fans)

        likersCnt = len(self.get_followers_active_likes(author_id, min_interactions) )
        sharersCnt = len(self.get_followers_active_shares(author_id, min_interactions) )
        commentersCnt = len(self.get_followers_active_comments(author_id, min_interactions) )

        res += "\nLiked >{0}%: {1} ({2:.2f}% of fans)\n".format(minIntPct, likersCnt, 100*likersCnt / fans)
        res += "Commented >{0}%: {1} ({2:.2f}% of fans)\n".format(minIntPct, sharersCnt, 100*sharersCnt / fans)
        res += "Shared >{0}%: {1} ({2:.2f}% of fans)\n".format(minIntPct, commentersCnt, 100*commentersCnt / fans)

        likerCommentersCnt = len( self.get_top_likers_commenters(author_id, min_interactions) )
        likerSharersCnt = len( self.get_top_likers_sharers(author_id, min_interactions) )
        commenterSharersCnt = len( self.get_top_commenters_sharers(author_id, min_interactions) )
        likerCommenterSharerCnt = len( self.get_top_likers_commenters_sharers(author_id, min_interactions) )

        res += "\nLiked and commented >{0}%: {1} ({2:.2f}% of fans)\n".format(minIntPct, likerCommentersCnt,
                                                                           100*likerCommentersCnt / fans)
        res += "Liked and shared >{0}%: {1} ({2:.2f}% of fans)\n".format(minIntPct, likerSharersCnt,
                                                                      100*likerSharersCnt / fans)
        res += "Shared and commented >{0}%: {1} ({2:.2f}% of fans)\n".format(minIntPct, commenterSharersCnt,
                                                                          100*commenterSharersCnt / fans)
        res += "Liked, commented and shared >{0}%: {1} ({2:.2f}% of fans)\n".format(minIntPct, likerCommenterSharerCnt,
                                                                                 100*likerCommenterSharerCnt / fans)

        return res


    def save_fan_activity_results(self, author_ids, filename=None, sub_dir=None, min_interactions=0.05):
        """ Save fan activity results for the given user(s)

        :param author_ids: user(s) you want to analyze
        :param filename: name of the report file
        :param sub_dir: sub directory to save the file in, it's created if necessary
        :param min_interactions: percentage of the minimum interactions over all posts (default: 0.05, i.e 5%)
        """
        if not type(author_ids) is list and not type(author_ids) is tuple:
            author_ids = [author_ids]

        if not filename:
            filename = 'fan_activity.txt'  # default filename

        print("Saving fan activity statistics for specified author(s)... ", end='', flush=True)

        if sub_dir:
            os.makedirs(sub_dir, exist_ok=True)
            filename = os.path.join(sub_dir, filename)

        with open(filename, 'w') as fp:
            for authorId in author_ids:
                authorReport = self.get_fan_activity_results(authorId, min_interactions=min_interactions)
                fp.write(authorReport)
                fp.write('\n\n')

        print("done")

    def save_plots(self, author_ids, sub_dir, status_count=1000):
        """ Save all available plots for given author(s)

        :param author_ids: ID of the author(s) (string or list/tuple)
        :param sub_dir: where to save the results
        :param status_count: how many last statuses you want to analyze
        """

        print("Saving plots for specified author(s)... ", end='', flush=True)

        self.plot_posts_time_distribution(author_ids, status_count, sub_dir=sub_dir)
        self.plot_likes_distribution(author_ids, status_count, sub_dir=sub_dir)
        self.plot_comments_distribution(author_ids, status_count, sub_dir=sub_dir)
        self.plot_comment_len_distribution(author_ids, status_count, sub_dir=sub_dir)
        self.plot_comment_sentiment_distribution(author_ids, status_count, sub_dir=sub_dir)
        self.plot_comments_delta_distribution(author_ids, status_count, sub_dir=sub_dir)

        print("done")


def DumpResponse(response):
    """ Print given response object

    :param response: elasticsearch_dsl Response
    """

    if hasattr(response, 'to_dict') and callable(getattr(response, 'to_dict')):
        respStr = json.dumps(response.to_dict(), indent=2, ensure_ascii=False)
    elif type(response) is dict:
        respStr = json.dumps(response, indent=2, ensure_ascii=False)
    else:
        respStr = str(response)

    print("Response:\n", respStr)


def Main():
    ParseArguments()

    analyzer = Analyzer(index=ES_INDEX, data_source='fb', es_address=ES_ADDRESS)

    if PRINT_USERS:
        authors = analyzer.get_authors_all()
        for author in authors:
            print("{0}\n\tFull name: {1}\n\tID: {2}\n".format(author['name_ascii'], author['name'], author['id']))
        return 0

    authors = USERS

    # Check whether all given users exist and convert all potential usernames into IDs
    authorNames  = []
    for i, author in enumerate(authors):
        try:
            authors[i] = analyzer.get_author_id_from_string(author)
        except (RuntimeError, EsNotFoundError):
            print("Given user '{0}' does not exist in Elasticsearch".format(author))
            return 1

        # Convert author ID to author name
        authorId = authors[i]
        authorName = analyzer.get_author_name(authorId, ensure_ascii=True)
        authorNames.append(authorName)

        # Print latest post for all specified authors
        if PRINT_LATEST_POST:
            analyzer.print_latest_post(authorId)
            continue

        if not ONLY_PLOTS:
            # Save text results/stats for the given author
            authorDataDir = "stats_{0}".format(authorName)
            print("Results will be stored in the '{0}' directory".format(authorDataDir))
            analyzer.save_text_results(authorId, authorDataDir, status_count=POST_COUNT)


    if PRINT_LATEST_POST:  # nothing more to do, exit
        return 0

    # Where to save the compound results (for one author the directory is the same as for the text results)
    compoundDataDir = "stats_{0}".format('_'.join(authorNames))
    if len(authors) > 1:
        print("Compound results will be stored in the '{0}' directory".format(compoundDataDir))

    if not ONLY_STATS:  # we want plots
        analyzer.save_plots(authors, sub_dir=compoundDataDir, status_count=POST_COUNT)

    if not ONLY_PLOTS:  # we want stats
        analyzer.save_fan_activity_results(authors, sub_dir=compoundDataDir, min_interactions=MIN_INTERACTIONS)
        if len(authors) > 1:  # cross activity doesn't make sense for one author
            analyzer.save_cross_activity_results(authors, sub_dir=compoundDataDir, min_interactions=MIN_INTERACTIONS)

    return 0


if __name__ == "__main__":
    sys.exit(Main())
