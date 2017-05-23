#!/usr/bin/env python3
"""
    User impact in Social Networks: master thesis at BUT FIT, ac. year 2016/2017.
    Author: Petr Jirout, xjirou07@stud.fit.vutbr.cz

    Facebook downloader
"""

import sys, os, urllib.parse, json, re, copy, datetime, argparse

import requests

# How many bytes (approximately) a data file has (in bytes)
DATA_FILE_SIZE = 20 * 1024 * 1024  # 20 MB
# Where the downloaded data are stored
DATA_DIR = os.path.join('..', 'data')

# Global variables for appropriate command line options
# User you want analyze
USER = ''
# How many posts to fetch
POST_COUNT = 1000
# Download only posts published before this date. Use either unix timestamp or UTC time
# format, e.g. '2017-03-25T12:00:00+0000'
POSTS_PUBLISHED_UNTIL = None
# Download only posts published after this date.
POSTS_PUBLISHED_SINCE = None

# TODO specify CLI args: DATA_DESTINATION_DIR

# Facebook key and secret are obtained from the env variables
APP_ID     = os.getenv("FACEBOOK_APP_ID", None)
APP_SECRET = os.getenv("FACEBOOK_APP_SECRET", None)

# Which GraphAPI version to use
GRAPH_API_VERSION = '2.9'
# How many seconds to wait for the server to respond
REQUEST_TIMEOUT = 125


def ParseArguments():
    """ Parse command line arguments and set appropriate global flags """

    global USER, POST_COUNT, POSTS_PUBLISHED_UNTIL, POSTS_PUBLISHED_SINCE, APP_ID, APP_SECRET
    parser = argparse.ArgumentParser()
    parser.add_argument('USER', help="ID or username from which to download the data")
    parser.add_argument('-p', '--post-count', dest='post_cnt', type=int, default=1000,
                        help="How many user's post you want to download (default: 1000)")
    parser.add_argument('-u', '--published-until', dest='until', default=None,
                        help="Download only posts published before this date. Use either unix timestamp or UTC time " +
                             "format, e.g. '2017-03-25T12:00:00+0000'")
    parser.add_argument('-s', '--published-since', dest='since', default=None,
                        help="Download only posts published after this date. Use either unix timestamp or UTC time " +
                             "format, e.g. '2017-03-25T12:00:00+0000'")
    parser.add_argument('--app-id', dest='app_id', default=None,
                        help="Specify Facebook APP_ID via this parameter or via environment variable FACEBOOK_APP_ID")
    parser.add_argument('--app-secret', dest='app_secret', default=None,
                        help="Specify Facebook APP_SECRET via this parameter or via environment variable " +
                             "FACEBOOK_APP_SECRET")

    args = parser.parse_args()

    USER = args.USER
    POST_COUNT = args.post_cnt
    POSTS_PUBLISHED_UNTIL = args.until
    POSTS_PUBLISHED_SINCE = args.since

    if args.app_id:
        APP_ID = args.app_id
    if args.app_secret:
        APP_SECRET = args.app_secret


class GraphApi:
    """ TODO """

    @staticmethod
    def raw_request(request_url):
        """ Make a raw request on the given URL and return JSON.

        Return: JSON as a Python dictionary
        """

        try:
            r = requests.get(request_url.strip(), timeout=REQUEST_TIMEOUT)
        except:
            print("Exception for raw_request url:", request_url)
            raise
        return r.json()


    def __init__(self, app_id, app_secret):
        # Member variables
        self.appId = app_id
        self.appSecret = app_secret
        self.accessToken = None

        if not app_id or not app_secret:
            raise ValueError("Facebook GraphAPI APP_ID or APP_SECRET is missing")

        # Initialize the token
        self.accessToken = self.access_token_request()


    @staticmethod
    def get_interaction_list_size(list_obj):
        """ Return the approximate size of the prettified list with interaction objects. """

        ONE_INTERACTION_SIZE = 273  # approximate size of one prettified interaction JSON
        return ONE_INTERACTION_SIZE * len(list_obj)


    @staticmethod
    def get_post_list_size(list_obj):
        """ Return the approximate size of the prettified list with post objects.

        This is a really rough approximation.
        """

        ONE_POST_SIZE = 1010  # approximate size of one prettified post JSON
        return ONE_POST_SIZE * len(list_obj)


    @staticmethod
    def save_data(obj, data_subdir=None, filename_prefix=None):
        """ Save Python objects as JSON.

        :param obj: Python object you want to save
        :param data_subdir: name of the subdir in the DATA_DIR (string, optional)
        :param filename_prefix: prefix for the filename (string, optional)
        """

        dataDir = DATA_DIR
        if data_subdir:
            dataDir = os.path.join(dataDir, data_subdir)

        # Create the directory if necessary
        if not os.path.isdir(dataDir):
            if os.path.exists(dataDir):  # it's a regular file, abort
                print("Data directory '%s' cannot be created, there's a file with the same name" % dataDir)
                return False
            else:
                os.makedirs(dataDir)

        # Given prefix, append underscore
        if filename_prefix and not filename_prefix.endswith('_'):
            filename_prefix += "_"

        # Find an available filename
        counter = 0
        while True:
            filename = "{0}data_{1}.json".format(filename_prefix, counter)
            filename = os.path.join(dataDir, filename)
            if not os.path.exists(filename):
                break
            counter += 1

        with open(filename, 'w') as f:
            json.dump(obj, f, indent=2, ensure_ascii=False)  # Do not encode UTF-8 as ASCII
            f.flush()
            os.fsync(f.fileno())


    def append_interaction(self, interaction_list, interaction, data_subdir):
        """ Append interaction to the list and dumping them to a file when necessary

        :param interaction_list: list of interaction to which you want to append
        :param interaction: interaction object you want to append
        :param data_subdir: where to save the data
        :return: new interaction list
        """

        interaction_list.append(interaction)

        # Check if we need to dump the data
        if self.get_interaction_list_size(interaction_list) > DATA_FILE_SIZE:
            self.save_data(interaction_list, data_subdir=data_subdir, filename_prefix='interaction')
            interaction_list = []

        return interaction_list


    def append_user(self, user_list, user, data_subdir):
        """ Append user object to the list and dumping them to a file when necessary

        :param user_list: list of users to which you want to append
        :param user: user object you want to append
        :param data_subdir: where to save the data
        :return: new user list
        """

        user_list.append(user)

        # Check if we need to dump the data
        # One user data record is approximately the same as as interaction
        if self.get_interaction_list_size(user_list) > DATA_FILE_SIZE:
            self.save_data(user_list, data_subdir=data_subdir, filename_prefix='user')
            user_list = []

        return user_list


    def request(self, endpoint, params=None, raw_response=False):
        """ Make a GraphAPI request towards the given endpoint.

        :param endpoint: GraphAPI endpoint for the request (string). It may contain version
        :param params: request query parameters (dictionary, optional)
        :param raw_response: do not convert the response into Python dictionary (bool, default: False)
        :return: Request's response JSON (dictionary)
        """

        graphUrl = 'https://graph.facebook.com/'
        endpoint = endpoint.strip()

        if params is None:
            params = {}

        # If set, send the access token with the request
        token = self.accessToken
        if 'access_token' not in params and token is not None:
            params['access_token'] = token

        # If the endpoint doesn't start with version, append the default module version
        if not bool(re.search('^[/]?v[0-9]\.[0-9]/', endpoint)):
            graphUrl += 'v{0}/'.format(GRAPH_API_VERSION)

        try:
            r = requests.get(urllib.parse.urljoin(graphUrl, endpoint), params=params, timeout=REQUEST_TIMEOUT)
        except:
            print("Exception for graph_request endpoint:", endpoint)
            raise

        #print("Request url\n", r.url)

        if raw_response:
            return r.text

        return r.json()


    @staticmethod
    def response_has_error(response_dict, raise_exception=True):
        """ Checks whether GraphApi response contains error.

        :param response_dict: dictionary with the response
        :param raise_exception: whether to throw an exception on error found (bool, default: True)
        :return: None if no error found, dict with error description otherwise (if raise_exception is False)
        """

        if 'error' in response_dict:
            if not raise_exception:
                return response_dict['error']
            raise ValueError(response_dict['error'])

        return None


    def get_id_from_username(self, username):
        """ Get ID for a given FB username

        :param username: username (string)
        :return: ID of the user (string) or None on failure
        """

        params = { 'fields': 'id' }
        response = self.request(username, params)
        return response.get('id', None)


    def get_username_from_id(self, fb_id):
        """ Get facebook profile name (the one from the profile url) for a given ID

        :param fb_id: ID of the user (string)
        :return: username (string) or None on failure
        """

        params = {'fields': ','.join(('id', 'name', 'link'))}
        response = self.request(fb_id, params)

        profileName = urllib.parse.urlparse(response['link']).path  # 'extract path from the url
        profileName = profileName.replace('/', '')  # erase slashes
        profileName = profileName.replace(' ', '_')  # replace spaces with underscores

        if not profileName:
            return None
        return profileName


    def access_token_request(self):
        """ Retrieve the App token.

        :return Access token (string) or None on error
        """

        # Get access token
        params = {
            'client_id': APP_ID,
            'client_secret': APP_SECRET,
            'grant_type': 'client_credentials'
        }
        response = self.request('oauth/access_token', params=params, raw_response=True)

        # First versions of Graph API returned plain text response, later JSON. Let's try both.
        try:
            tokenJson = json.loads(response)
            return tokenJson.get('access_token', None)
        except ValueError:
            # We got plain text response in the format 'access_token=REAL_ACCESS_TOKEN'
            if 'access_token' in response:
                return response.replace('access_token=', '')  # extract the access token

        # Some other error, throw an exception
        err = response.get('error', None)
        if not err:
            err = response
        raise RuntimeError(err)


    def get_all_elements(self, endpoint, params=None):
        """ Return all elements on the given endpoint.

        Automatically iterates over pages, yielding one element at a time.
        """

        if params is None:
            params = {}

        if 'limit' not in params:
            params['limit'] = 100  # download maximum number of posts in one request to save network overhead

        # Get initial page
        page = self.request(endpoint.strip(), params)

        while not self.response_has_error(page) and page['data']:
            for element in page['data']:
                yield element

            nextUrl = page.get('paging', {}).get('next', None)

            if not nextUrl:  # no next URL, we reached the end
                return

            # Request next page
            page = self.raw_request(nextUrl.strip())  # nextUrl already has the previous params


    def save_users_details(self, user_ids, data_subdir):
        """ Download details for the given users

        :param user_ids: iterable of user ID's you want to download
        :param data_subdir: where to save the results
        """

        userInfoParams = {
            'fields': ','.join(('id', 'name', 'birthday', 'link', 'age_range', 'gender', 'first_name', 'middle_name',
                                'last_name', 'location', 'locale'))
        }

        users = []
        try:
            for userId in user_ids:
                userInfo = self.request(userId, userInfoParams)
                userInfo['origin'] = 'facebook'
                if userInfo.get('birthday', None):
                    userInfo['birthday_format'] = "MM/DD/YYYY"

                users = self.append_user(users, userInfo, data_subdir)
        finally:
            self.save_data(users, data_subdir=data_subdir, filename_prefix='user')


    def save_author_info(self, author_id, data_subdir):
        """ Save data about author (his page)

        :param author_id: ID of the page
        :param data_subdir: where to save the results
        """

        pageInfoParams = {
            'fields': ','.join(('id', 'name', 'birthday', 'link', 'location', 'about', 'fan_count',
                                'talking_about_count'))
        }

        authorName = self.get_username_from_id(author_id)
        if not authorName:
            authorName = author_id

        authorInfo = self.request(author_id, pageInfoParams)
        authorInfo['origin'] = 'facebook'
        authorInfo['is_author'] = True  # whether this user is an author (or just a person who interacts)
        authorInfo['name_ascii'] = authorName
        authorInfo['birthday_format'] = "MM/DD/YYYY"
        self.save_data(authorInfo, data_subdir=data_subdir, filename_prefix='user_page_info')


    def save_full_author_data(self, author_id, data_subdir):
        """ Download all author's data: posts, likes, shares, comments

        :param author_id: user ID
        :param data_subdir: where to save the results
        """
        # All available fields: https://developers.facebook.com/docs/graph-api/reference/v2.8/post
        postParams = {
            'fields': ','.join(('id', 'created_time', 'message', 'link', 'place', 'status_type', 'shares'))
        }
        shareParams = copy.deepcopy(postParams)
        shareParams['fields'] += ',from'  # we want to identify the user who shared the post

        # Select useful comment fields: https://developers.facebook.com/docs/graph-api/reference/v2.8/comment/
        commentParams = {
            'fields': ','.join(('id', 'created_time', 'message', 'from', 'like_count'))
        }

        if POSTS_PUBLISHED_UNTIL:
            print("Fetching posts published until", POSTS_PUBLISHED_UNTIL)
            postParams['until'] = POSTS_PUBLISHED_UNTIL  # user specified

        if POSTS_PUBLISHED_SINCE:
            print("Fetching posts published since", POSTS_PUBLISHED_SINCE)
            postParams['since'] = POSTS_PUBLISHED_SINCE


        interactionTemplate = {
            'id': None,
            'status_id': None,
            'status_author': author_id,
            'type': None,
            'author': None,
            'origin': 'facebook'
        }

        posts = []
        interactions = []
        postCount = 0
        commentCount = 0
        likeCount = 0
        shareCount = 0

        try:
            print("\nStarted at {0}\n".format(datetime.datetime.now().isoformat(sep=' ')))

            for post in self.get_all_elements("{0}/posts".format(author_id), postParams):
                postCount += 1
                postId = post['id']
                #userIds = set()

                # Save the post
                post['origin'] = 'facebook'
                post['author'] = author_id
                if 'shares' in post:  # flatten share count if present
                    post['share_count'] = post['shares'].get('count', 0)
                    del post['shares']
                else:
                    post['share_count'] = 0

                posts.append(post)
                # Dump if necessary
                if self.get_post_list_size(posts) > DATA_FILE_SIZE:
                    self.save_data(posts, data_subdir=data_subdir, filename_prefix='post')
                    posts = []

                # Fetch comments and create an interaction for each one
                for comment in self.get_all_elements('{0}/comments'.format(postId), commentParams):
                    commentCount += 1

                    interaction = copy.deepcopy(interactionTemplate)
                    interaction['type'] = 'comment'
                    interaction['id'] = comment['id']
                    interaction['status_id'] = postId
                    interaction['created_time'] = comment['created_time']
                    interaction['author'] = comment['from']['id']
                    interaction['message'] = comment['message']
                    interaction['like_count'] = comment['like_count']

                    interactions = self.append_interaction(interactions, interaction, data_subdir)

                    #userIds.add(comment['from']['id'])  # comment author

                # Fetch all likes and create an interaction record for all of them
                for like in self.get_all_elements('{0}/likes'.format(postId)):
                    likeCount += 1

                    # Likes does not have an id, so we need to generate it
                    # Returned (in the graph response) 'id' element is an id of the user who liked the post
                    interaction = copy.deepcopy(interactionTemplate)
                    interaction['id'] = "L_{0}_{1}".format(like['id'], postId)  # L_author_statusId
                    interaction['type'] = 'like'
                    interaction['status_id'] = postId
                    interaction['author'] = like['id']

                    interactions = self.append_interaction(interactions, interaction, data_subdir)

                    #userIds.add(like['id'])  # like author

                # Fetch all shares
                for share in self.get_all_elements('{0}/sharedposts'.format(postId), shareParams):
                    shareCount += 1

                    interaction = copy.deepcopy(interactionTemplate)
                    interaction['type'] = 'share'
                    interaction['id'] = share['id']
                    interaction['status_id'] = postId
                    interaction['created_time'] = share['created_time']
                    interaction['author'] = share['from']['id']
                    interaction['message'] = share.get('message', '')

                    interactions = self.append_interaction(interactions, interaction, data_subdir)

                    #userIds.add(share['from']['id'])  # share author

                if postCount % 10 == 0:
                    print("Posts downloaded: {0}/{1}".format(postCount, POST_COUNT))
                if postCount >= POST_COUNT:
                    break

        finally:
            # Save the final data
            if interactions:
                self.save_data(interactions, data_subdir=data_subdir, filename_prefix='interaction')
            if posts:
                self.save_data(posts, data_subdir=data_subdir, filename_prefix='posts')

            print("\nFinished at {0}\n".format(datetime.datetime.now().isoformat(sep=' ')))
            print("Total post count:", postCount)
            print("Total interaction count: {0} ({1} likes, {2} comments, {3} shares)".format(
                  likeCount + commentCount + shareCount, likeCount, commentCount, shareCount))



def Main():
    ParseArguments()

    #BABIS_ID = '214827221987263'
    #KLAUS_ML_ID = '277957209202178'
    #KALOUSEK_ID = '132141523484024'
    #SOBOTKA_ID = 'sobotka.bohuslav'
    #TOMIO_ID = 'tomio.cz'

    givenIdOrUsername = USER

    graph = GraphApi(APP_ID, APP_SECRET)

    # We don't know whether we got ID or username, so we query both endpoints
    authorId = graph.get_id_from_username(givenIdOrUsername)
    if not authorId:
        print("Unable to get author ID for the following string:", givenIdOrUsername)
        return 1

    authorName = graph.get_username_from_id(authorId)
    if not authorName:
        authorName = authorId

    # Where to save the downloaded data: 'facebook/user_USERNAME', e.g. 'facebook/user_AndrejBabis
    dataSubdir = os.path.join('facebook', "user_{0}".format(authorName))

    print("Downloading data from author:\n\t{0} (id: {1})".format(authorName, authorId))
    print("Data will be saved into the following directory:\n\t{0}".format(os.path.join(DATA_DIR, dataSubdir)))

    # Fetch and save user page info
    graph.save_author_info(authorId, dataSubdir)

    # Save all the data (posts, likes, comments, shares)
    graph.save_full_author_data(authorId, dataSubdir)

    return 0


# Main function wrapper
if __name__ == "__main__":
    sys.exit(Main())
