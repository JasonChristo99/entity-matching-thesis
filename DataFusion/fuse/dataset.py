import pandas as pd
from fact import ObservedFactCollection
from fact import CanonicalFact
from fusion import TruthDiscovery
from fusion import FuseObservations
from matching import MatchingStrategy
from gensim.models import KeyedVectors

filename = 'C:/Users/Iasonas/Downloads/GoogleNews-vectors-negative300.bin'


class Dataset:
    """
    This class defines a Dafu dataset.
    """

    def __init__(self, fuse_env, name, entity_attributes,
                 ent_attr_schema,
                 sources, observed_facts,
                 src_to_fact_map, matching_strategy: MatchingStrategy):
        """
        The constructor for Dataset
        :param fuse_env: A Fuse environment.
        :param name: Dataset name.
        :param entity_attributes: The set of entity attributes.
        :param sources: The set of data sources providing data.
        :param observed_facts: A dictionary with observed facts
                               for each entity_id and entity attribute.
        :param src_to_fact_map: A source to observed fact map
        """
        self.env = fuse_env
        self.name = name
        self.entity_attributes = list(entity_attributes)
        self.ent_attr_schema = ent_attr_schema
        self.sources = sources
        self.src_weights = self._initial_source_vertical_map()
        self.src_acc = self._initial_source_vertical_map()
        self.observed_facts = observed_facts
        self.src_to_fact_map = src_to_fact_map
        self.observed_fact_collections = {k: [] for k in self.entity_attributes}
        self.entity_attribute_matchers = {k: None for k in self.entity_attributes}
        self.cluster_map = {}
        # Preparation for fusion
        self._collect_attributes()
        self._normalize_ent_attr_schema()
        self.matching_strategy = matching_strategy
        self._init_entity_attr_matchers()
        self._create_fact_collections()
        # Data fusion results
        self.true_clusters = set([])
        self.canonical_facts = set([])
        self.true_facts = []
        self.true_facts_flat = []

    # Initialiation methods

    def _initial_source_vertical_map(self):
        return {a: {s: 1.0 for s in self.sources} for a in self.entity_attributes}

    def _collect_attributes(self):
        """
        Iterate over reported facts and collect all fact attributes
        per entity attribute.
        :return: None.
        """
        for eid in self.observed_facts:
            for attr in self.observed_facts[eid]:
                for fact in self.observed_facts[eid][attr]:
                    self.ent_attr_schema[attr] |= set(fact.get_fact_attributes())

    def _normalize_ent_attr_schema(self):
        """
        Iterate over reported facts and normalize
        the schema of reported tuples.
        :return: None.
        """
        for eid in self.observed_facts:
            for attr in self.observed_facts[eid]:
                for fact in self.observed_facts[eid][attr]:
                    fact.normalize(self.ent_attr_schema[attr])

    def _create_fact_collections(self):
        """
        Iterate over facts and assign them to collections
        per entity id and entity attribute.
        :return: None.
        """
        for eid in self.observed_facts:
            for attr in self.observed_facts[eid]:
                matcher = self.entity_attribute_matchers[attr]
                collection = ObservedFactCollection(eid, attr, matcher)
                for fact in self.observed_facts[eid][attr]:
                    if fact.is_valid():
                        collection.add_fact(fact)
                if len(collection.facts) > 0:
                    self.observed_fact_collections[attr].append(collection)

    def _init_entity_attr_matchers(self):
        """
        Instantiate a matcher object per entity attribute.
        A matcher is used to identify matching reported facts.
        :return: None
        """
        for attr in self.ent_attr_schema:
            matcher = self.matching_strategy(self, attr)
            self.entity_attribute_matchers[attr] = matcher

    # Fact matching methods
    def get_matcher(self, attr):
        return self.entity_attribute_matchers[attr]

    def train_matchers(self, matched_facts=None):
        """
        Iterate over matchers and train them.
        :param matched_facts: A dict containing pre-matched facts for each vertical (keys are vertical names)
                      as accepted by dedupe.Dedupe.mark_pairs
                      see https://docs.dedupe.io/en/latest/API-documentation.html#dedupe.Dedupe.mark_pairs
        :return: None
        """
        for attr in self.entity_attribute_matchers:
            prematched_facts = None if matched_facts is None else matched_facts.get(attr, None)
            self.entity_attribute_matchers[attr].train(prematched_facts=prematched_facts, samples=200)

    def match_observations(self):
        """
        Iterate over collections and retrieve clusters of matching observed facts.
        :return: None
        """
        for attr in self.observed_fact_collections:
            for col in self.observed_fact_collections[attr]:
                cmap = col.compute_fact_clusters()
                for cluster_id in cmap:
                    self.cluster_map[cluster_id] = cmap[cluster_id]

    # Truth finding methods
    def _gather_cluster_votes(self):
        """
        Iterate over collections of observed facts and obtain
        source to cluster votes
        :return: Global votes of sources.
        """
        global_votes = {}
        for attr in self.observed_fact_collections:
            for col in self.observed_fact_collections[attr]:
                # Get votes
                votes = col.get_votes()
                # Aggregate votes
                for cluster_id in votes:
                    global_votes[cluster_id] = {}
                    for src in votes[cluster_id]:
                        global_votes[cluster_id][src] = votes[cluster_id][src]
        return global_votes

    def find_true_clusters(self):
        """
        Perform truth discovery at the cluster level.
        :return: None
        """
        # Init true clusters
        self.true_clusters = set([])
        # Gather all src votes
        votes = self._gather_cluster_votes()
        # Call fusion with votes and src weights
        fuser = TruthDiscovery(self.env, votes, self.src_weights)
        cluster_truth = fuser.run()
        for cluster_id in cluster_truth:
            if cluster_truth[cluster_id]:
                self.true_clusters.add(self.cluster_map[cluster_id])
        return self.true_clusters

    def find_true_clusters_new(self):
        self.true_clusters = set([])
        for cid in self.cluster_map:
            self.true_clusters.add(self.cluster_map[cid])
        return self.true_clusters

    def find_true_facts(self, no_weights=False):
        """
        Iterate over true clusters and
        infer canonical facts.
        :param: no_weights: If set to `False` simple majority voting is used.
        :return: Canonical facts
        """
        # Init Canonical facts
        self.canonical_facts = set([])
        for cluster in self.true_clusters:
            cfact = CanonicalFact(self.env, cluster, self.src_acc[cluster.ent_attr])
            self.canonical_facts.add(cfact)
        # Call fusion with true clusters and src accuracy
        fuser = FuseObservations(self.env, self.canonical_facts, self.src_acc, no_weights)
        word_vectors = KeyedVectors.load_word2vec_format(filename, binary=True)
        fuser.run(word_vectors)
        self.true_facts = fuser.get_canonical_facts()
        self.true_facts_flat = fuser.flatten_facts()
        return self.true_facts

    # Methods to persist information
    def get_src_frame(self):
        """
        Return a Pandas dataframe with the source weights.
        :return: Pandas dataframe with schema ['source', 'weight'].
        """
        # flatten list (https://stackoverflow.com/a/952952)
        flat_weights = [[ver, src, wgt] for ver, src_wgt in self.src_weights.items() for src, wgt in src_wgt.items()]
        return pd.DataFrame(flat_weights, columns=['vertical', 'source', 'weight'])

    def get_entity_attrs_frame(self):
        """
        Return a Pandas dataframe with entity attributes.
        :return: Pandas dataframe with schema ['entity_attributes'].
        """
        return pd.DataFrame(self.entity_attributes, columns=['entity_attributes'])

    def get_src_to_fact_map_frame(self):
        """
        Return a Pandas dataframe with the source to fact map.
        :return: Pandas dataframe with schema ['source', 'fact_ids'].
                 Fact ids is a set.
        """
        return pd.DataFrame(self.src_to_fact_map, columns=['source', 'fact_ids'])

    def get_true_facts(self):
        """
        Return a Pandas dataframe with the true facts.
        :return: Pandas dataframe with schema ['entity id', 'entity attribute'
        """
        return pd.DataFrame(self.true_facts_flat, columns=['eid', 'entity_attr', 'attr', 'val'])
