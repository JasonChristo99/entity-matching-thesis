from gensim.models import KeyedVectors

WORD2VEC_FILEPATH = 'C:/Users/Iasonas/Downloads/GoogleNews-vectors-negative300.bin'
# WORD_LIMIT = 500000
WORD_LIMIT = 0  # test


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
            # WordVectors.__instance = KeyedVectors.load_word2vec_format(WORD2VEC_FILEPATH, binary=True, limit=WORD_LIMIT)
            WordVectors.__instance = KeyedVectors.load_word2vec_format(WORD2VEC_FILEPATH, binary=True) # without limit
            # WordVectors.__instance = WordVectorsStub() # for testing purposes


class WordVectorsStub:
    def __init__(self):
        pass

    def similarity(self, x, y):
        return 0
