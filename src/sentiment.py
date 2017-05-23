#!/usr/bin/env python3
"""
    Analyze sentiment
"""

import sys, os, pickle

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import classification_report
from sklearn.pipeline import Pipeline
from sklearn import svm

# Training data
# Labels: 'p' (positive), 'n' (negative), '0' (neutral), 'b' (bipolar)
POSTS_FILE = os.path.join('fb_corpus', 'gold-posts.txt')
LABELS_FILE = os.path.join('fb_corpus', 'gold-labels.txt')

DEFAULT_CLASSIFIER_FILENAME = 'default_classifier.pkl'


def TrainClassifier():
    """ Train a sentiment classifier

    :return: trained classifier
    """

    print("Training sentiment classifier")

    # Read the data
    labels = []
    posts = []
    with open(POSTS_FILE, 'r') as pf, open(LABELS_FILE, 'r') as pl:
        allPosts = pf.read().strip()
        allLabels = pl.read().strip()

        posts += allPosts.split('\n')
        labels += allLabels.split('\n')

    if len(posts) != len(labels):
        print("Invalid corpus, different number of elements for labels/posts")
        return 1

    elemCnt = len(posts)
    print("Number of elements:", elemCnt)

    # Perform classification with SVM, linear kernel
    classifier = svm.LinearSVC()

    # Create feature vectors
    # Vectorizer for feature extraction
    vectorizer = TfidfVectorizer(sublinear_tf=True, use_idf=True)
    trainVectors = vectorizer.fit_transform(posts)

    classifier.fit(trainVectors, labels)

    #testVectors = VECTORIZER.transform(testPosts)
    #prediction = classifier.predict(testVectors)
    #print(classification_report(testLabels, prediction))

    pipeline = Pipeline([
        ('vectorizer', vectorizer),
        ('svc', classifier),
    ])

    return pipeline


def SaveClassifier(classifier, filename=DEFAULT_CLASSIFIER_FILENAME):
    """ Save given classifier into a file

    :param filename: where to save the classifier
    """

    # Save the classifier
    print("Saving the classifier")
    with open(filename, 'wb') as fp:
        pickle.dump(classifier, fp)


def LoadClassifier(filename=DEFAULT_CLASSIFIER_FILENAME):
    """ Load a classifier from the given file

    :param filename: file the pickled classifier
    :return: classifier
    """

    with open(filename, 'rb') as fp:
        return pickle.load(fp)


def AnalyzeSentiment(data, classifier=None):
    """ Analyse sentiment of the given string

    :param data: text you want to analyze
    :param classifier: classifier to use. If not specified, it loads the default classifier
    :return: array with sentiment prediction results
    """

    if type(data) is not list and type(data) is not tuple:
        data = [data]

    if not classifier:
        classifier = LoadClassifier()

    return classifier.predict(data)


def Main():
    classifier = TrainClassifier()

    data = [
        "velmi si užívám tyhle krátká videa, moc děkuji",
        "je šikovnej zvládne to.... takové mimi a jak dokáže bojovat držíme nejen palečky :-)",
        "špatná správa, mamka je březí.",
        "Také jsem je, bohužel, při žádné návštěvě ZOO neviděla...",
        "ten pohled mluví za vše.... ;-)",
        "Jojo :-) Už tomu rozumím :-) :-)",
        "Takový malý koloušek... jéééé",
    ]

    print("Analysing sentiment")
    ret = AnalyzeSentiment(data, classifier)
    zipped = list(zip(data, ret))
    print(zipped)

    SaveClassifier(classifier)

    return 0

if __name__ == "__main__":
    sys.exit(Main())
