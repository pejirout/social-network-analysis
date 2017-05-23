#!/usr/bin/env python3
"""
    Load data into Elasticsearch.

    This loader does not transform or modify the data. You need to ensure that the data are in correct format
    with all required fields.
"""

import sys, os, json, argparse

from elasticsearch import Elasticsearch

from gender.genderize import AnalyzeGender

#TODO describe required fields + format (JSON) for interactions/posts

# Whether to add a sentiment field to data units
ADD_SENTIMENT = True

# Path to load
PATH = ''
# Elasticsearch address string
ES_ADDRESS = None
# Elasticsearch index
ES_INDEX = 'xjirou07'


if ADD_SENTIMENT:
    import sentiment
    CLASSIFIER = sentiment.LoadClassifier()


def ParseArguments():
    """ Parse command line arguments and set appropriate global flags """

    global ES_ADDRESS, PATH, ES_INDEX
    parser = argparse.ArgumentParser(description="Load data into the Elasticsearch instance")
    parser.add_argument('PATH', help="File or directory from which you want to load data")
    parser.add_argument('-a', '--es-address', dest='es_addr', default=None,
                        help="Elasticsearch address. String in the format of 'host[:port]'")
    parser.add_argument('-i', '--es-index', dest='es_index', default=ES_INDEX,
                        help="Index under which all data are stored in Elasticsearch.")

    args = parser.parse_args()
    PATH = args.PATH
    ES_ADDRESS = args.es_addr
    ES_INDEX = args.es_index


def GetDocTypeFromFilename(filename):
    """ Return a string representing given document type """

    interactionStrings = ('ints', 'interactions', 'interaction')
    postStrings = ('posts', 'post')

    if any(s in filename for s in interactionStrings):
        return 'interaction'
    elif any(s in filename for s in postStrings):
        return 'post'
    elif 'user_info' in filename or 'user_page_info' in filename:
        return 'user'
    else:
        return None


def GetOriginFromDataUnit(data_unit):
    """ Return a shortened origin string from the data unit

    E.g. 'fb' for Facebook, 'tw' for Twitter

    Returns: shortened origin (string)
    """

    origin = data_unit.get('origin', '').lower()
    if origin == 'facebook':
        origin = 'fb'
    elif origin == 'twitter':
        origin = 'tw'

    return origin


def AddExtraFieldsToUnit(data_unit):
    """ Add extra (computed) fields to the data unit """

    if data_unit.get('message', None):
        # Calculate length of the message
        data_unit['message_len'] = len(data_unit.get('message'))
        if ADD_SENTIMENT:
            # Store sentiment of the message
            data_unit['message_sentiment'] = sentiment.AnalyzeSentiment(data_unit.get('message'), CLASSIFIER)[0]

    if data_unit.get('first_name', None):  # user info data unit
        data_unit['gender'] = AnalyzeGender(data_unit['first_name'], data_unit.get('last_name', None))

    return data_unit


def Main():
    ParseArguments()

    # Prepare a list of files we want to load
    fileList = []

    # Append absolute paths to the file list
    if PATH:
        if not os.path.exists(PATH):
            raise ValueError("Path '{0}' does not exist".format(PATH))
        if os.path.isdir(PATH):
            print("Looking for data files in: {0}\n".format(PATH))
            for file in os.listdir(PATH):
                # Accept only files with '.json' suffix, not hidden and without 'IGNORE'
                if not file.endswith('.json') or file.startswith('.') or 'IGNORE' in file:
                    continue
                fileList.append( os.path.abspath(os.path.join(os.getcwd(), PATH, file)) )
        else:  # is a file
            fileList.append( os.path.abspath(os.path.join(os.getcwd(), PATH)) )

    # Create Elasticsearch interface
    if ES_ADDRESS:
        es = Elasticsearch(ES_ADDRESS)
    else:
        es = Elasticsearch()

    fileList.sort()

    for i, file in enumerate(fileList):
        docType = GetDocTypeFromFilename(os.path.basename(file))
        if not docType:
            print("Unknown data type, ignoring file: {0}".format(file))
            continue
        if not os.path.isfile(file):
            continue
        print('Processing data file [{0}/{1}]: {2}'.format(i+1, len(fileList), file))

        with open(file, 'r') as fp:
            # Data is expected to be a list of objects
            data = json.load(fp)
            if type(data) is not list and type(data) is not tuple:
                data = [data]
            # Save each unit into the index
            for unit in data:
                unitId = unit.get('id', None)
                if not unitId:
                    print("\tMissing id, ignoring element")
                    continue
                # Save the element with an origin prefix, e.g. 'fb_post' or 'tw_interaction'
                origin = GetOriginFromDataUnit(unit)
                unitDocType = "{0}_{1}".format(origin, docType)
                unit = AddExtraFieldsToUnit(unit)

                es.index(ES_INDEX, unitDocType, unit, unitId)

            print("\tInserted {0} '{1}' elements".format(len(data), docType))

    return 0

if __name__ == "__main__":
    sys.exit(Main())
