from matching import MatchingStrategy
from matching.collective_matching_ahc.simrank import SimRank


class CollectiveMatchingAHC(MatchingStrategy):
    def __init__(self, dataset, attr):
        """
        Constructor for a fact matcher.
        :param dataset: An instance of the Dataset class.
        :param attr: The entity attribute associated with the matcher.
        """
        self.dataset = dataset
        self.attr = attr
        self.attributes = dataset.ent_attr_schema[attr]
        self.attribute_to_simrank_matcher = {}

    def train(self, **kwargs):
        # TODO
        print('TRAIN')
        # pass
        if self.attr == 'Working Experience':
            self.attribute_to_simrank_matcher['title'] = SimRank(self.dataset, self.attr, 'title')
        if self.attr == 'Education':
            self.attribute_to_simrank_matcher['degree'] = SimRank(self.dataset, self.attr, 'degree')
            self.attribute_to_simrank_matcher['university'] = SimRank(self.dataset, self.attr, 'university')

    def get_matches(self, facts, threshold=0.6):
        # TODO
        print('GETMATCHES')
        return []
        # pass
