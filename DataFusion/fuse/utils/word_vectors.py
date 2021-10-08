from gensim.models import KeyedVectors

filename = 'C:/Users/Iasonas/Downloads/GoogleNews-vectors-negative300.bin'


class WordVectors:
    __instance = None

    @staticmethod
    def getInstance():
        """ Static access method. """
        if WordVectors.__instance is None:
            WordVectors()
        return WordVectors.__instance

    def __init__(self):
        """ Virtually private constructor. """
        if WordVectors.__instance is not None:
            raise Exception("This class is a Singleton!")
        else:
            WordVectors.__instance = KeyedVectors.load_word2vec_format(filename, binary=True)
