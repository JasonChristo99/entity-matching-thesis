import pickle
filepath = '../../datasets/current/finalized_model.sav'
# filepath = '../../../datasets/current/finalized_model.sav'


class RegressionModel:
    __instance = None

    @staticmethod
    def getInstance():
        """ Static access method. """
        if RegressionModel.__instance is None:
            RegressionModel()
        return RegressionModel.__instance

    def __init__(self):
        """ Virtually private constructor. """
        if RegressionModel.__instance is not None:
            raise Exception("This class is a Singleton!")
        else:
            RegressionModel.__instance = pickle.load(open(filepath, 'rb'))
