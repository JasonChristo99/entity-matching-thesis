from matching import MatchingStrategy
from matching.ahc_matcher.simrank import SimRank

from strsimpy.jaro_winkler import JaroWinkler
from strsimpy.cosine import Cosine
from strsimpy.normalized_levenshtein import NormalizedLevenshtein
from strsimpy.damerau import Damerau

cosine = Cosine(2)
jarowinkler = JaroWinkler()
levenshtein = NormalizedLevenshtein()
damerau = Damerau()


class AgglomerativeHierarchicalClustering(MatchingStrategy):
    def __init__(self, dataset, attr, matcher_home='', threshold=0.6, constant_a=0.6):
        """
        Constructor for a fact matcher.
        :param dataset: An instance of the Dataset class.
        :param attr: The entity attribute associated with the matcher.
        """
        self.dataset = dataset
        self.attr = attr
        self.attributes = dataset.ent_attr_schema[attr]
        self.threshold = threshold
        self.constant_a = constant_a
        self.attribute_to_simrank_matcher = {}
        self.matcher_home = matcher_home
        self.init_state()

    def init_state(self):
        self.facts = dict()
        self.fact_records = dict()
        self.fact_clusters = dict()
        self.fact_pair_similarites = dict()
        self.formed_cluster_arrays = []

    def train(self, **kwargs):
        # print('TRAIN')
        if self.attr == 'Working Experience':
            self.attribute_to_simrank_matcher['title'] = SimRank(self.dataset, self.attr, 'title')
        if self.attr == 'Education':
            self.attribute_to_simrank_matcher['degree'] = SimRank(self.dataset, self.attr, 'degree')
            self.attribute_to_simrank_matcher['university'] = SimRank(self.dataset, self.attr, 'university')

    def get_matches(self, **kwargs):
        facts = kwargs.get("facts")
        fact_records = kwargs.get("fact_records")
        # TODO: never merge facts from the same source
        self.init_state()
        self.facts = facts
        self.fact_records = fact_records
        self.init_token_clusters()
        self.calculate_all_fact_pair_similarities()
        self.cluster_similar_facts()
        self.form_cluster_arrays()
        return self.formed_cluster_arrays
        # print('GETMATCHES')

    def cluster_similar_facts(self, threshold=0.45):
        # iteration = 1
        max_pair_similarity = self.get_max_cluster_pair_similarity()
        while max_pair_similarity["score"] > threshold and len(self.fact_clusters) > 1:
            # merge the most similar cluster pair
            self.merge_clusters(max_pair_similarity["pair"])
            # calculate new maximum similarity score
            max_pair_similarity = self.get_max_cluster_pair_similarity()

    def form_cluster_arrays(self):
        self.formed_cluster_arrays = []
        for cluster in self.fact_clusters:
            self.formed_cluster_arrays.append(list(cluster))

    @property
    def get_cluster_arrays(self):
        return self.formed_cluster_arrays

    def init_token_clusters(self):
        for key in self.fact_records:
            self.fact_clusters[frozenset({key})] = [key]

    def calculate_all_fact_pair_similarities(self):
        for x in self.fact_records:
            for y in self.fact_records:
                if frozenset({x, y}) in self.fact_pair_similarites:
                    continue
                similarity = self.get_fact_pair_similarity(self.fact_records[x], self.fact_records[y])
                self.fact_pair_similarites[frozenset({x, y})] = similarity

    def get_fact_pair_similarity(self, x: dict, y: dict):
        result = 0
        if x is y:
            return 1
        if self.attr == 'Name':
            result = self.get_name_fact_similarity(x, y)
        elif self.attr == 'Location':
            result = self.get_location_fact_similarity(x, y)
        elif self.attr == 'Education':
            result = self.get_education_fact_similarity(x, y)
        elif self.attr == 'Working Experience':
            result = self.get_working_experience_fact_similarity(x, y)
        elif self.attr == 'Skills':
            result = self.get_skills_fact_similarity(x, y)
        return result

    def get_name_fact_similarity(self, fact1, fact2):
        result = self.cosine_similarity(fact1['name'], fact2['name'])
        return result

    def get_location_fact_similarity(self, fact1, fact2):
        result = self.cosine_similarity(fact1['location'], fact2['location'])
        return result

    def get_education_fact_similarity(self, fact1, fact2):
        degree_sim = self.constant_a * self.cosine_similarity(fact1['degree'], fact2['degree']) + (
                1 - self.constant_a) * \
                     self.attribute_to_simrank_matcher['degree'].get_simrank_score_of_tokens(fact1['degree'],
                                                                                             fact2['degree'])
        university_sim = self.constant_a * self.cosine_similarity(fact1['university'], fact2['university']) + (
                1 - self.constant_a) * \
                         self.attribute_to_simrank_matcher['university'].get_simrank_score_of_tokens(
                             fact1['university'],
                             fact2['university'])
        year_sim: float
        try:
            diff = abs(int(fact1['year']) - int(fact2['year']))
            year_sim = 1 / (diff + 1)
        except:
            year_sim = self.cosine_similarity(fact1['year'], fact2['year'])

        result = 0.3 * degree_sim + 0.4 * university_sim + 0.2 * year_sim
        return result

    def get_working_experience_fact_similarity(self, fact1, fact2):
        title_sim = self.constant_a * self.cosine_similarity(fact1['title'], fact2['title']) + (1 - self.constant_a) * \
                    self.attribute_to_simrank_matcher['title'].get_simrank_score_of_tokens(fact1['title'],
                                                                                           fact2['title'])
        company_sim = self.cosine_similarity(fact1['company'], fact2['company'])

        years_sim: float
        try:
            y11 = int(fact1['years'].split('-')[0])
            y12 = int(fact1['years'].split('-')[1])
            y21 = int(fact2['years'].split('-')[0])
            y22 = int(fact2['years'].split('-')[1])
            diff = abs(y11 - y12) + abs(y21 - y22)
            years_sim = 1 / (diff + 1)
        except:
            years_sim = self.cosine_similarity(fact1['years'], fact2['years'])

        result = 0.3 * title_sim + 0.4 * company_sim + 0.2 * years_sim
        return result

    def get_skills_fact_similarity(self, fact1, fact2):
        result = self.cosine_similarity(fact1['skill'], fact2['skill'])
        return result

    def get_max_cluster_pair_similarity(self):
        clusters = list(self.fact_clusters.keys())
        max_pair_similarity = {"pair": frozenset({-1, -1}), "score": -1}
        for k, x in enumerate(clusters):
            for y in clusters[k + 1:]:
                if self.clusters_contain_facts_from_same_source(x, y): continue
                sim_score = self.get_cluster_pair_similarity(x, y)
                if sim_score > max_pair_similarity["score"]:
                    max_pair_similarity = {"pair": frozenset({x, y}), "score": sim_score}
        return max_pair_similarity

    def clusters_contain_facts_from_same_source(self, x, y):
        sources_of_r1 = [self.facts[r].src for r in list(x)]
        sources_of_r2 = [self.facts[r].src for r in list(y)]
        common_sources = len(set(sources_of_r1).intersection(sources_of_r2))
        return common_sources > 0

    def get_cluster_pair_similarity(self, x, y):
        # with maximum link
        max = -1
        for k, f1 in enumerate(self.fact_clusters[x]):
            for f2 in self.fact_clusters[y]:
                score = self.fact_pair_similarites[frozenset({f1, f2})]
                if score > max:
                    max = score
        return max

    def merge_clusters(self, cluster_pair: frozenset):
        clusters_list = list(cluster_pair)
        cluster_key_1: frozenset = clusters_list[0]
        cluster_key_2: frozenset = clusters_list[1]
        new_cluster_key = frozenset(cluster_key_1.union(cluster_key_2))
        # pop old keys, save values
        values_key_1 = self.fact_clusters.pop(cluster_key_1)
        values_key_2 = self.fact_clusters.pop(cluster_key_2)
        # add new key with values of old keys
        self.fact_clusters[new_cluster_key] = values_key_1 + values_key_2

    def cosine_similarity(self, x: str, y: str):
        return cosine.similarity(x.lower(), y.lower())
