from abc import abstractmethod, ABC


class MatchingStrategy(ABC):
    """
    The MatchingStrategy interface declares operations common to all supported matching strategies.
    """

    @abstractmethod
    def train(self, **kwargs):
        pass

    @abstractmethod
    def get_matches(self, **kwargs):
        pass
