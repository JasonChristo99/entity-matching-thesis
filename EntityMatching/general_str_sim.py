from strsimpy.jaro_winkler import JaroWinkler
from strsimpy.cosine import Cosine
from strsimpy.sorensen_dice import SorensenDice

# initialize similarity functions
cosine = Cosine(2)
jaro_winkler = JaroWinkler()
sor_dice = SorensenDice()


def cosine_similarity(s1, s2):
    result = cosine.similarity(s1, s2)
    return result


def jaro_winkler_similarity(s1, s2):
    result = jaro_winkler.similarity(s1, s2)
    return result


def sorensen_dice_similarity(s1, s2):
    result = sor_dice.similarity(s1, s2)
    return result
