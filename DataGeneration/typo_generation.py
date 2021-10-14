import random
from typing import List
import unicodedata
from numpy import random as nprandom
import re
import string

from DataGeneration import RANDOM_SEED

nprandom.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

# returns non-space character
def _get_similar_char(cs, position):
    if cs[position].isspace():
        # find a non-space character
        non_space = re.search(r"(\S)", ''.join(cs))
        if non_space is None:  # just pick an ascii character
            char = random.choice(string.ascii_letters)
        else:
            char = non_space.group()
    else:
        char = cs[position]
    code = ord(char)
    category = unicodedata.category(char)
    next_char = chr(code + 1)

    return next_char if unicodedata.category(next_char) == category else chr(code - 1)


def _omit_letter(cs: List[str], position):
    if len(cs) <= 1: return

    del cs[position]


def _double_letter(cs: List[str], position):
    cs.insert(position, cs[position])


def _insert_letter(cs: List[str], position):
    cs.insert(position, _get_similar_char(cs, position))


def _insert_space(cs: List[str], position):
    cs.insert(position, ' ')


def _replace_letter(cs: List[str], position):
    cs[position] = _get_similar_char(cs, position)


def _reverse_letters(cs: List[str], position):
    if len(cs) <= 1: return
    position = min(1, position)

    temp = cs[position - 1]
    cs[position - 1] = cs[position]
    cs[position] = temp


# All possible typo-error types
_ERROR_TYPES = [
    _omit_letter,
    _double_letter,
    _insert_letter,
    # _insert_space,
    _replace_letter,
    # _reverse_letters
]


# Takes a string as input and returns a misspelled version of it
def misspell(s: str, binomial_typo_param: float):
    s = list(s)

    number_of_typos = nprandom.binomial(len(s), binomial_typo_param)

    errors = random.choices(_ERROR_TYPES, k=number_of_typos)
    error_positions = nprandom.choice(len(s), size=number_of_typos, replace=False)

    for error, position in zip(errors, error_positions):
        error(s, min(position, len(s) - 1))

    result = "".join(s)
    return result
