import six
import math
import operator
from fuzzywuzzy import fuzz
from sklearn import model_selection
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from matching.ahc_matcher import AgglomerativeHierarchicalClustering
from matching.dedupe import DedupeMatcher

# filen1 = 'fuz_word.sav'
# loaded_model_1 = pickle.load(open(filen1, 'rb'))
# filen2 = 'fuzz_model.sav'
# loaded_model_2 = pickle.load(open(filen2, 'rb'))
# filen3   = 'finalized_model.sav'
from utils import WordVectors
from utils import RegressionModel


class ObservedFact:
    """
    This class defines an observed fact reported by a source.
    A fact is a relational tuple reporting an entry for an entity attribute.
    """

    def __init__(self, fact_id, entity_id, entity_attr, source, raw_dict):
        """
        Constructor for Observed fact.
        :param fact_id: A unique identifier for a fact.
        :param entity_id: The ID of the entity the fact refers to.
        :param entity_attr: The attribute associated with the reported fact.
        :param source: The source that reported the fact.
        :param rawDict: The raw entry corresponding to the fact.
        """
        self.fid = fact_id
        self.eid = entity_id
        self.ent_attr = entity_attr
        self.src = source
        self.rawValues = raw_dict
        self.normalized = {}

        # Associated Fact Collection
        self.fact_collection_id = None

    def get_fact_attributes(self):
        """
        Return a list with the attributes of the raw fact tuple.
        :return: A list with the attributes of the raw fact tuple.
        """
        if isinstance(self.rawValues, dict):
            return self.rawValues.keys()
        else:
            return ['_val_']

    def get_values(self):
        """
        Get raw values.
        :return: The raw entry corresponding to the fact.
        """
        return self.rawValues

    def normalize(self, attributes):
        """
        Normalize the schema of the tuple reported by the fact, given
        a set of attributes associated with a set of similar facts
        :param attributes: A list of attribute to be used as schema.
        :return: None.
        """
        self.normalized = {k: None for k in attributes}
        attrs = self.get_fact_attributes()
        if attrs == ['_val_']:
            self.normalized['_val_'] = self.rawValues
        else:
            for k in attrs:
                s = self.rawValues[k]
                if isinstance(s, list):
                    s = ','.join(s)
                if isinstance(s, six.string_types):
                    if s == '':
                        self.normalized[k] = None
                    else:
                        # self.normalized[k] = s.lower().strip()
                        self.normalized[k] = s.strip()
                else:
                    if s == '':
                        self.normalized[k] = None
                    else:
                        self.normalized[k] = str(s)

    def get_normalized_attrs(self):
        """
        Return a list with the normalized attributes of an observed fact.
        :return: A list with the attributes of the fact.
        """
        return self.normalized.keys()

    def is_valid(self):
        if not all(v is None or v == '' for v in self.normalized.values()):
            return True
        else:
            return False


class ObservedFactCollection:
    """
    This class defines a set of observed facts for one attribute of an entity.
    """

    def __init__(self, entity_id, entity_attr, matcher):
        """
        Constructor for ObservedFactCollection.
        :param entity_id: The ID of the entity the facts in the set refer to.
        :param entity_attr: The attribute associate with the reported facts.
        """
        self.eid = entity_id
        self.cid = 0
        self.ent_attr = entity_attr
        self.facts = []
        self.fact_records = {}
        self.fid = 0
        self.fact_ids = []
        self.sources = set([])
        self.attributes = None
        self.matcher = matcher
        self.clusters = {}

    def add_fact(self, new_fact):
        """
        Append a new fact to the collection.
        :return: None.
        """
        self.facts.append(new_fact)
        # Collect fact records to be passed to matcher
        self.fact_records[self.fid] = new_fact.normalized
        self.fid += 1
        self.fact_ids.append(new_fact.fid)
        # Update source set
        self.sources.add(new_fact.src)

    def compute_matches(self):
        """
        Use the associated matcher to find the pairwise
        similarity of facts in the collection.
        :return: None.
        """
        return self.matcher.get_matches(fact_records=self.fact_records, facts=self.facts)

    def _fids_to_facts(self, fid_set):
        """
        Convert a set of fact ids to set of fact objects.
        :param fid_set: A set of local (collection-based) fact ids.
        :return: A set of fact objects that corresponds to the input set of fids.
        """
        facts = set([])
        for fid in fid_set:
            facts.add(self.facts[fid])
        return facts

    def compute_fact_clusters(self):
        """
        Use the obtained pairwise similarities to find clusters of
        observed facts that agree with each other.
        :return: Return a map from cluster_id to an ObservedFactCluster object.
        """
        matches = self.compute_matches()
        for m in matches:
            cluster_id = str(self.eid) + '_' + self.ent_attr + '_' + str(self.cid)
            cluster_facts = self._fids_to_facts(set(m))
            self.clusters[cluster_id] = ObservedFactCluster(cluster_id, self.eid, self.ent_attr, cluster_facts)
            self.cid += 1
        return self.clusters

    def get_votes(self):
        """
        Iterate over clusters and construct a voting matrix.
        :return: A dictionary where we store the votes for each source.
        """
        # Iterate over all sources and find if they vote in favor or against a cluster.
        # If a source mentions a fact in a cluster then it votes in favor.
        # If a source does not mention a fact in a cluster then it votes against.
        votes = {}
        for cid in self.clusters:
            votes[cid] = {}
            for src in self.sources:
                votes[cid][src] = self.clusters[cid].in_favor(src)
        return votes


class ObservedFactCluster:
    """
    This class defines a cluster of matching observed facts reported for the same
    entity. All facts in a cluster refer to the same entity attribute and support
    each other.
    """

    def __init__(self, cluster_id, ent_id, ent_attr, facts):
        """
        Constructor for ObservedFactCollection.
        :param cluster_id: The id of the new cluster.
        :param ent_id: The id of the entity the new cluster contains info for.
        :param ent_attr: The entity attribute this cluster contains info for.
        :param facts: A set containing ObservedFact instances.
        """
        self.cid = cluster_id
        self.ent_id = ent_id
        self.ent_attr = ent_attr
        self.facts = facts
        self.sources = self._get_sources()

        # Fusion related flags
        self.isCorrect = True

    def _get_sources(self):
        """
        Iterate over all facts in cluster and extract their source.
        :return:
        """
        srcs = set([])
        for fact in self.facts:
            srcs.add(fact.src)
        return srcs

    def in_favor(self, src):
        """
        Find if a source votes in favor of the cluster.
        :param src:
        :return: 1.0 or -1.0
        """
        if src in self.sources:
            return 1.0
        else:
            return -1.0

    def get_normalized_attrs(self):
        """
        Get normalized attributes of corresponding facts.
        :return: A dictionary of attributes
        """
        f = next(iter(self.facts))
        return f.get_normalized_attrs()

    def get_ent_attr(self):
        """
        Get entity attribute of corresponding facts.
        :return: An entity attribute.
        """
        f = next(iter(self.facts))
        return f.ent_attr

    def get_ent_id(self):
        """
        Get entity id of corresponding facts.
        :return: An entity id.
        """
        f = next(iter(self.facts))
        return f.eid


class CanonicalFact:
    """
    This class defines a canonical fact.
    """

    def __init__(self, fuse_env, cluster, src_acc):
        """
        :param fuse_env: A Fuse environment.
        :param cluster: An ObservedFactCluster object.
        """
        self.env = fuse_env
        self.cluster = cluster
        self.ent_id = self.cluster.get_ent_id()
        self.ent_attr = self.cluster.get_ent_attr()
        self.canonicalTuple = dict.fromkeys(self.cluster.get_normalized_attrs())
        self.src_acc = src_acc

    def get_facts(self):
        """
        Return the facts associated with the cluster.
        :return: A set of ObservedFacts
        """
        return self.cluster.facts

    def infer_true_assignment(self):
        """
        Apply weighted majority voting to identify the canonical values.
        :return: None.
        """
        # Initialize value scores
        score = {key: {} for key in self.canonicalTuple.keys()}
        vote_count = {key: {} for key in self.canonicalTuple.keys()}
        # Iterate over observed facts

        for fact in self.cluster.facts:

            weight = self.src_acc[fact.src]
            for attr in fact.normalized:
                value = fact.normalized[attr]
                if value:
                    if value not in score[attr]:
                        score[attr][value] = weight
                        vote_count[attr][value] = 1
                    else:
                        score[attr][value] += weight  # pairnei to varos tis anistoixis pigis pou to exei psifisei
                        vote_count[attr][value] += 1  # kai an psifise ki auti tin idia timi

        # Find Max Score attribute value
        for attr in self.canonicalTuple.keys():
            # Iter over values and add normalizing score
            n = len(score[attr].keys())
            if n == 1:
                max_value = max(score[attr].items(), key=operator.itemgetter(1))[0]
                self.canonicalTuple[attr] = max_value
            else:
                for value in score[attr]:
                    score[attr][value] += vote_count[attr][value] * math.log(n - 1)
                if len(score[attr]) == 0:
                    max_value = None
                else:
                    max_value = max(score[attr].items(), key=operator.itemgetter(1))[0]
            self.canonicalTuple[attr] = max_value

    def evaluate_fact(self, fact):
        """
        Compare an observed fact with the inferred fact
        :return: A correctness score
        """
        word_vectors = WordVectors.getInstance()
        loaded_model_3 = RegressionModel.getInstance(
            getattr(self.env, 'home_dir', '') + '../dataset/' + 'finalized_model.sav')

        attrs = float(len(self.canonicalTuple.keys()))
        correct = 0.0
        for attr in self.canonicalTuple:

            fuz_sc = 0.0
            word_sc = 0.0
            tmp_sc = []
            scores = []

            fuz_sc = fuzz.token_sort_ratio(fact.normalized[attr], self.canonicalTuple[attr])
            if fact.normalized[attr] in word_vectors and self.canonicalTuple[attr] in word_vectors:
                word_sc = word_vectors.similarity(fact.normalized[attr], self.canonicalTuple[attr])
            else:
                word_sc = 0.0
            tmp_sc.append(fuz_sc)
            tmp_sc.append(word_sc)
            scores.append(tmp_sc)
            if loaded_model_3.predict(scores):
                correct += 1.0

            # if self.env.matcher_function(fact.normalized[attr], self.canonicalTuple[attr]):
            #    correct += 1.0
            # #1
            # if ( fact.normalized[attr] in word_vectors and self.canonicalTuple[attr] in word_vectors) and (fact.normalized[attr] and self.canonicalTuple[attr]):
            #     if self.env.matcher_function(fact.normalized[attr], self.canonicalTuple[attr]) and self.env.matcher_function2(fact.normalized[attr], self.canonicalTuple[attr]):
            #         correct += 1.0
            # else:
            #     if self.env.matcher_function(fact.normalized[attr], self.canonicalTuple[attr]):
            #         correct += 1.0
            # 2
            # if (attr == "degree" or  attr == "university") and ( fact.normalized[attr] and  self.canonicalTuple[attr]):
            #
            #       if (fuzz.token_sort_ratio(fact.normalized[attr],self.canonicalTuple[attr]) * 0.01 + word_vectors.similarity(fact.normalized[attr], self.canonicalTuple[attr])) > 0.6:
            #           correct += 1.0
            #
            # else :
            #     if fuzz.token_sort_ratio(fact.normalized[attr], self.canonicalTuple[attr]) > 60:
            #       correct += 1.0
        return correct / attrs

    def get_canonical_fact(self):
        """
        Return the inferred canonical fact.
        :return: A canonical tuple
        """
        return self.canonicalTuple
