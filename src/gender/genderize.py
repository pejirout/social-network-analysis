#!/usr/bin/env python3
"""
    Manually analyze the gender in the given file
"""

import sys, os, unicodedata

# Corpus files in the same directory
THIS_FILE_DIR = os.path.dirname(os.path.realpath(__file__))
NAME_FILE = os.path.join(THIS_FILE_DIR, "sorted_names.txt")
GENDER_FILE = os.path.join(THIS_FILE_DIR, "sorted_names_genders.txt")

# Global variable to keep the loaded names during runtime
NAME_DICT = {}


def InitNameDict():
    """ Initialize global name dictionary

    Called automatically on module import
    """

    global NAME_DICT

    with open(NAME_FILE, 'r') as nf, open(GENDER_FILE, 'r') as gf:
        allNames = nf.read().strip().split('\n')
        allGenders = gf.read().strip().split('\n')

        if len(allNames) != len(allGenders):
            raise ValueError("Invalid name or gender file: sizes don't match")

        for i, name in enumerate(allNames):
            NAME_DICT[name] = allGenders[i]

        # Now add normalized versions of names (without accents)
        for i, name in enumerate(allNames):
            normalized = NormalizeString(name)
            if normalized not in NAME_DICT:
                NAME_DICT[normalized] = NAME_DICT[name]


def NormalizeString(string):
    """ Normalize given string (remove accents etc.) """

    normalized = unicodedata.normalize('NFD', string)
    normalized = normalized.encode('ASCII', 'ignore')  # ignore non ascii chars
    normalized = normalized.decode('UTF-8')
    return normalized


def AnalyzeGender(first_name, last_name=None):
    """ Return letter describing gender: 'm' for male, 'f' for female, 'u' for unisex

    :param first_name: first name 
    :param last_name: last name (optional)
    :return: one letter specifying the gender, 'x' for unknown
    """

    if first_name in NAME_DICT:
        return NAME_DICT[first_name]
    # Try to look for a normalized version
    elif NormalizeString(first_name) in NAME_DICT:
        return NAME_DICT[NormalizeString(first_name)]
    # Try to analyze the last name
    elif last_name:
        normalized = NormalizeString(last_name)
        if normalized.endswith('ova'):  # Czech female surnames usually ends with 'ova'
            return 'f'

    return 'x'  # unable to determine the gender


def PromptForGender(string):
    """ Return letter describing gender: 'm' for male, 'f' for female, 'u' for unisex

    :param string: string with name you want to analyze
    :return: one letter specifying the gender 
    """

    while True:
        gender = input("Gender for: '{0}' [m/f/u]: ".format(string))
        if gender.lower() in ('m', 'f', 'u'):
            return gender.lower()

def Main():
    if len(sys.argv) > 1 and sys.argv[1] == '-c':
        # Create new gender file
        with open(NAME_FILE, 'r') as fp, open(GENDER_FILE, 'w') as op:
            allNames = fp.read().strip()
            names = allNames.split('\n')
            for name in names:
                gender = PromptForGender(name.strip())
                op.write("{0}\n".format(gender))
                op.flush()
    else:
        print("Doing nothing, specify '-c' to generate new gender files")

    return 0


InitNameDict()

if __name__ == "__main__":
    sys.exit(Main())
