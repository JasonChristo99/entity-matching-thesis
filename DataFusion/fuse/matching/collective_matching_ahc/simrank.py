from typing import Union

from strsimpy.jaro_winkler import JaroWinkler
from strsimpy.cosine import Cosine

cosine = Cosine(2)


class SimRank:
    def __init__(self, dataset, attr, property):
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
        self.value_clusters = dict()
        self.value_pair_similarites = {}
        self.distinct_values = list()
        self.entity_to_cluster_graph = dict()
        self.cluster_to_enity_graph = dict()
        # key: iteration number, value: n^2 score table
        self.entity_simrank_buffer = dict()
        self.cluster_simrank_buffer = dict()

        # 1. Cluster string-similar property values
        self.cluster_similar_values()
        # 2. Get data to graph form of entity->value
        self.form_entity_cluster_graph()
        # 3. Compute simrank for graph
        self.init_simrank_scores()
        self.compute_simrank_scores()

    def cluster_similar_values(self, threshold=0.45):
        self.distinct_values = self.get_all_distinct_values_of_property()
        self.init_value_clusters()

        iteration = 1
        self.calculate_all_value_pair_similarities()
        max_pair_similarity = self.get_max_cluster_pair_similarity()
        while (max_pair_similarity["score"] > threshold):
            # merge the most similar cluster pair
            self.merge_clusters(max_pair_similarity["pair"])
            # calculate new maximum simlarity score
            max_pair_similarity = self.get_max_cluster_pair_similarity()

    def get_all_distinct_values_of_property(self):
        values_set = set()
        for fact_col in self.dataset.observed_fact_collections[self.attr]:
            for fact in fact_col.facts:
                value = fact.normalized[self.property] if fact.normalized[self.property] is not None else ""
                values_set.add(value)
        return list(values_set)

    def calculate_all_value_pair_similarities(self):
        for k, x in enumerate(self.distinct_values):
            for y in self.distinct_values[k + 1:]:
                # print(x, y)
                similarity = self.get_value_pair_similarity(x, y)
                self.value_pair_similarites[frozenset({x, y})] = similarity

    def get_value_pair_similarity(self, x: str, y: str):
        result = cosine.similarity(x, y)
        return result

    def init_value_clusters(self):
        for i, value in enumerate(self.distinct_values):
            self.value_clusters[frozenset({i})] = [value]

    def get_max_cluster_pair_similarity(self):
        clusters = list(self.value_clusters.keys())
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
        for k, v1 in enumerate(self.value_clusters[x]):
            for v2 in self.value_clusters[y]:
                score = self.value_pair_similarites[frozenset({v1, v2})]
                if score > max:
                    max = score
        return max

    def merge_clusters(self, cluster_pair: frozenset):
        clusters_list = list(cluster_pair)
        cluster_key_1: frozenset = clusters_list[0]
        cluster_key_2: frozenset = clusters_list[1]
        new_cluster_key = frozenset(cluster_key_1.union(cluster_key_2))
        # pop old keys, save values
        values_key_1 = self.value_clusters.pop(cluster_key_1)
        values_key_2 = self.value_clusters.pop(cluster_key_2)
        # add new key with values of old keys
        self.value_clusters[new_cluster_key] = values_key_1 + values_key_2

    def form_entity_cluster_graph(self):
        for fact_col in self.dataset.observed_fact_collections[self.attr]:
            for fact in fact_col.facts:
                entity_of_fact = fact.eid
                value = fact.normalized[self.property] if fact.normalized[self.property] is not None else ""
                cluster_of_value = self.get_cluster_of_value(value)
                if entity_of_fact not in self.entity_to_cluster_graph:
                    self.entity_to_cluster_graph[entity_of_fact] = []

                self.entity_to_cluster_graph[entity_of_fact].append(cluster_of_value)

        # also build the inverse dictionary, for easy lookups
        for k, v in self.entity_to_cluster_graph.items():
            for x in v:
                self.cluster_to_enity_graph.setdefault(x, set()).add(k)

    def get_cluster_of_value(self, value: str):
        value_index = self.distinct_values.index(value)
        cluster: frozenset
        for key in self.value_clusters:
            if value_index in key:
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

    def compute_simrank_scores(self, iterations=10, constant_c1=0.8, constant_c2=0.8):
        # first calulate entity scores
        for i in range(iterations):
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
