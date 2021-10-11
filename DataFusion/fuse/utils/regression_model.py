import pickle


# filepath = '../../datasets/current/finalized_model.sav'
# filepath = '../../../datasets/current/finalized_model.sav'


class RegressionModel:
    __instance = None

    @staticmethod
    def getInstance(filepath='../../datasets/current/finalized_model.sav'):
        """ Static access method. """
        if RegressionModel.__instance is None:
            RegressionModel(filepath)
        return RegressionModel.__instance

    def __init__(self, filepath):
        """ Virtually private constructor. """
        if RegressionModel.__instance is not None:
            raise Exception("This class is a Singleton!")
        else:
            RegressionModel.__instance = pickle.load(open(filepath, 'rb'))
