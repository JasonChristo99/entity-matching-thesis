from typing import Union

from strsimpy.jaro_winkler import JaroWinkler
from strsimpy.cosine import Cosine
from strsimpy.normalized_levenshtein import NormalizedLevenshtein
from strsimpy.damerau import Damerau

cosine = Cosine(2)
jarowinkler = JaroWinkler()
levenshtein = NormalizedLevenshtein()
damerau = Damerau()


class SimRank:
    def __init__(self, dataset, attr, property, num_iterations=10):
        """
        This is a matcher for structural similarity of values of the same entity ettribute property
        Constructor for a fact matcher.
        :param dataset: An instance of the Dataset class.
        :param attr: The entity attribute associated with the matcher.
        """
        self.dataset = dataset
        self.attr = attr
        self.property = property
        self.attributes = dataset.ent_attr_schema[attr]
        self.token_clusters = dict()
        self.token_pair_similarites = dict()
        self.distinct_tokens = list()
        self.entity_to_cluster_graph = dict()
        self.cluster_to_enity_graph = dict()
        # key: iteration number, token: n^2 score table
        self.entity_simrank_buffer = dict()
        self.cluster_simrank_buffer = dict()
        self.num_iterations = num_iterations

        # 1. Cluster string-similar property values
        self.distinct_tokens = self.get_distinct_tokens_for_property()
        self.init_token_clusters()
        self.cluster_tokens()
        # 2. Get data to graph form of entity->token cluster
        self.form_entity_cluster_graph()
        # 3. Compute simrank for graph
        self.init_simrank_scores()
        self.compute_simrank_scores()

    def cluster_tokens(self, threshold=0.6):
        iteration = 1
        self.calculate_all_token_pair_similarities()
        max_pair_similarity = self.get_max_cluster_pair_similarity()
        while (max_pair_similarity["score"] > threshold):
            # merge the most similar cluster pair
            self.merge_clusters(max_pair_similarity["pair"])
            # calculate new maximum simlarity score
            max_pair_similarity = self.get_max_cluster_pair_similarity()

    def get_distinct_tokens_for_property(self):
        tokens_set = set()
        for fact_col in self.dataset.observed_fact_collections[self.attr]:
            for fact in fact_col.facts:
                token = fact.normalized[self.property] if fact.normalized[self.property] is not None else ""
                tokens_set.add(token)
        return list(tokens_set)

    def calculate_all_token_pair_similarities(self):
        for k, x in enumerate(self.distinct_tokens):
            for y in self.distinct_tokens[k + 1:]:
                # print(x, y)
                similarity = self.get_token_pair_similarity(x, y)
                self.token_pair_similarites[frozenset({x, y})] = similarity

    def get_token_pair_similarity(self, x: str, y: str):
        if self.attr == 'Education' and self.property == 'degree':
            # result = jarowinkler.similarity(x.lower(), y.lower())
            result = levenshtein.similarity(x.lower(), y.lower())
            # result = damerau.similarity(x.lower(), y.lower())
            # result = 1 - damerau.distance(x.lower(), y.lower()) / max(len(x), len(y))
            # result = cosine.similarity(x.lower(), y.lower())
            return result
        if self.attr == 'Education' and self.property == 'university':
            # result = 1 - damerau.distance(x.lower(), y.lower()) / max(len(x), len(y))
            result = cosine.similarity(x.lower(), y.lower())
            return result
        # result = 1 - damerau.distance(x.lower(), y.lower()) / max(len(x), len(y))
        result = cosine.similarity(x.lower(), y.lower())
        return result

    def init_token_clusters(self):
        for i, token in enumerate(self.distinct_tokens):
            self.token_clusters[frozenset({i})] = [token]

    def get_max_cluster_pair_similarity(self):
        clusters = list(self.token_clusters.keys())
        max_pair_similarity = {"pair": frozenset({-1, -1}), "score": -1}
        for k, x in enumerate(clusters):
            for y in clusters[k + 1:]:
                sim_score = self.get_cluster_pair_similarity(x, y)
                if sim_score > max_pair_similarity["score"]:
                    max_pair_similarity = {"pair": frozenset({x, y}), "score": sim_score}
        return max_pair_similarity

    def get_cluster_pair_similarity(self, x, y):
        # with maximum link
        max = -1
        for k, v1 in enumerate(self.token_clusters[x]):
            for v2 in self.token_clusters[y]:
                score = self.token_pair_similarites[frozenset({v1, v2})]
                if score > max:
                    max = score
        return max

    def merge_clusters(self, cluster_pair: frozenset):
        clusters_list = list(cluster_pair)
        cluster_key_1: frozenset = clusters_list[0]
        cluster_key_2: frozenset = clusters_list[1]
        new_cluster_key = frozenset(cluster_key_1.union(cluster_key_2))
        # pop old keys, save values
        values_key_1 = self.token_clusters.pop(cluster_key_1)
        values_key_2 = self.token_clusters.pop(cluster_key_2)
        # add new key with values of old keys
        self.token_clusters[new_cluster_key] = values_key_1 + values_key_2

    def form_entity_cluster_graph(self):
        for fact_col in self.dataset.observed_fact_collections[self.attr]:
            for fact in fact_col.facts:
                entity_of_fact = fact.eid
                token = fact.normalized[self.property] if fact.normalized[self.property] is not None else ""
                cluster_of_token = self.get_cluster_of_token(token)
                if entity_of_fact not in self.entity_to_cluster_graph:
                    self.entity_to_cluster_graph[entity_of_fact] = []

                self.entity_to_cluster_graph[entity_of_fact].append(cluster_of_token)

        # also build the inverse dictionary, for easy lookups
        for k, v in self.entity_to_cluster_graph.items():
            for x in v:
                self.cluster_to_enity_graph.setdefault(x, set()).add(k)

    def get_cluster_of_token(self, token: str):
        token_index = self.distinct_tokens.index(token)
        cluster: frozenset
        for key in self.token_clusters:
            if token_index in key:
                cluster = key
                break
        return cluster

    def init_simrank_scores(self):
        # dict like: iteration+pair-> score
        for entity1 in self.entity_to_cluster_graph:
            for entity2 in self.entity_to_cluster_graph:
                if entity1 != entity2:
                    self.entity_simrank_buffer[frozenset({0, frozenset({entity1, entity2})})] = 0
                else:
                    self.entity_simrank_buffer[frozenset({0, frozenset({entity1, entity2})})] = 1

        for cluster1 in self.cluster_to_enity_graph:
            for cluster2 in self.cluster_to_enity_graph:
                if cluster1 != cluster2:
                    self.cluster_simrank_buffer[frozenset({0, frozenset({cluster1, cluster2})})] = 0
                else:
                    self.cluster_simrank_buffer[frozenset({0, frozenset({cluster1, cluster2})})] = 1

    def compute_simrank_scores(self, constant_c1=0.8, constant_c2=0.8):
        # first calulate entity scores
        for i in range(self.num_iterations):
            # simrank formula
            for entity1 in self.entity_to_cluster_graph:
                for entity2 in self.entity_to_cluster_graph:
                    if entity1 != entity2:
                        outbound_neighbors_of_e1 = self.entity_to_cluster_graph[entity1]
                        outbound_neighbors_of_e2 = self.entity_to_cluster_graph[entity2]
                        double_sum = 0
                        for cluster1 in outbound_neighbors_of_e1:
                            for cluster2 in outbound_neighbors_of_e2:
                                double_sum += self.cluster_simrank_buffer[
                                    frozenset({i, frozenset({cluster1, cluster2})})]
                        self.entity_simrank_buffer[frozenset({i + 1, frozenset({entity1, entity2})})] = \
                            (constant_c1 * double_sum) / (len(outbound_neighbors_of_e1) * len(outbound_neighbors_of_e2))
                    else:
                        self.entity_simrank_buffer[frozenset({i + 1, frozenset({entity1, entity2})})] = 1

            # then calulate cluster scores
            # simrank formula
            for cluster1 in self.cluster_to_enity_graph:
                for cluster2 in self.cluster_to_enity_graph:
                    if cluster1 != cluster2:
                        inbound_neighbors_of_c1 = self.cluster_to_enity_graph[cluster1]
                        inbound_neighbors_of_c2 = self.cluster_to_enity_graph[cluster2]
                        double_sum = 0
                        for entity1 in inbound_neighbors_of_c1:
                            for entity2 in inbound_neighbors_of_c2:
                                double_sum += self.entity_simrank_buffer[
                                    frozenset({i, frozenset({entity1, entity2})})]
                        self.cluster_simrank_buffer[frozenset({i + 1, frozenset({cluster1, cluster2})})] = \
                            (constant_c2 * double_sum) / (len(inbound_neighbors_of_c1) * len(inbound_neighbors_of_c2))
                    else:
                        self.cluster_simrank_buffer[frozenset({i + 1, frozenset({cluster1, cluster2})})] = 1

            self.clean_simrank_iteration(i)

    def clean_simrank_iteration(self, i):
        for key in list(self.cluster_simrank_buffer):
            iteration = list(key)[0] if type(list(key)[0]) == int else list(key)[1]
            if iteration == i:
                del self.cluster_simrank_buffer[key]
        for key in list(self.entity_simrank_buffer):
            iteration = list(key)[0] if type(list(key)[0]) == int else list(key)[1]
            if iteration == i:
                del self.entity_simrank_buffer[key]

    def get_simrank_score_of_tokens(self, x: str, y: str):
        c1 = self.get_cluster_of_token(x)
        c2 = self.get_cluster_of_token(y)
        simrank_similarity = self.cluster_simrank_buffer[frozenset({self.num_iterations, frozenset({c1, c2})})]
        return simrank_similarity
