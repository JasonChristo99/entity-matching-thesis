import math

import six
import pickle
import pandas as pd
from copy import deepcopy
import json
import pickle
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

from sklearn import model_selection
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier

# filen1 = 'fuz_word.sav'
# loaded_model_1 = pickle.load(open(filen1, 'rb'))
# filen2 = 'fuzz_model.sav'
# loaded_model_2 = pickle.load(open(filen2, 'rb'))
# filen3   = 'finalized_model.sav'
from utils import WordVectors

filen3 = 'C:/Users/Iasonas/Downloads/MSc_DataInt_Code/Dataset_Generator/olddata/finalized_model.sav'
loaded_model_3 = pickle.load(open(filen3, 'rb'))

import matplotlib.pyplot as plt


class Evaluation:
    """
    This class implements a collection of methods to evaluate
    the precision and recall of the inferred true facts.
    """

    def __init__(self, fuse_env, grd_path, canonical_facts, out_path=''):
        """
        Constructor for Evaluation
        :param fuse_env: A Fuse environment.
        :param grd_path: A filepath to a ground truth JSON file.
        :param canonical_facts_facts: A collection of CanonicalFact objects.
        """
        self.env = fuse_env
        self.grd_truth = self.load_grd_path(grd_path)
        self.canonical_facts = canonical_facts

        self.matched_facts_dict = []
        self.unmatched_facts_dict_true = []
        self.unmatched_facts_dict_infer = []
        self.all_facts = {}
        self.out_path = out_path

    def match_score(self, fact1, fact2):
        """
        Return the similarity between to facts associated with the
        entity attribute of the matcher.
        :param fact1: A dict with fact attributes and their respective values.
        :param fact2: A dict with fact attributes and their respective values.
        :return:
        """
        word_vectors = WordVectors.getInstance()

        score = 0.0
        f1attrs = set(fact1.keys())
        f2attrs = set(fact2.keys())
        common_attr = f1attrs & f2attrs

        if len(common_attr) == 0:
            return 0.0
        for attr in common_attr:

            # if (attr == 'degree') | (attr == 'university') | (attr== 'skill') | (attr == 'title'):
            #     fuz_sc = 0.0
            #     word_sc = 0.0
            #     tmp_sc = []
            #     scores = []
            #
            #     fuz_sc = fuzz.token_sort_ratio(fact1[attr], fact2[attr])
            #     if fact1[attr] in word_vectors and  fact2[attr] in word_vectors:
            #         word_sc = word_vectors.similarity(fact1[attr],fact2[attr])
            #     else:
            #         word_sc = 0.0
            #     tmp_sc.append(fuz_sc)
            #     tmp_sc.append(word_sc)
            #     scores.append(tmp_sc)
            #     if loaded_model_1.predict(scores):
            #         score += 1.0
            # elif  (attr == 'name') | (attr == 'company') :
            #     fuz_sc = 0.0
            #     tmp_sc = []
            #     scores = []
            #     fuz_sc = fuzz.token_sort_ratio(fact1[attr],fact2[attr])
            #     tmp_sc.append(fuz_sc)
            #     scores.append(tmp_sc)
            #     if loaded_model_2.predict(scores):
            #         score += 1.0
            # else:
            #     if fact1[attr] == fact2[attr]:
            #         score +=1.0
            # 3
            fuz_sc = 0.0
            word_sc = 0.0
            tmp_sc = []
            scores = []

            fuz_sc = fuzz.token_sort_ratio(fact1[attr], fact2[attr])
            if fact1[attr] in word_vectors and fact2[attr] in word_vectors:
                word_sc = word_vectors.similarity(fact1[attr], fact2[attr])
            else:
                word_sc = 0.0
            tmp_sc.append(fuz_sc)
            tmp_sc.append(word_sc)
            scores.append(tmp_sc)
            if loaded_model_3.predict(scores):
                score += 1.0

            # if (fact1[attr] in word_vectors and fact2[attr] in word_vectors) and (fact1[attr]  and fact2[attr] ) :
            #     if self.env.matcher_function(fact1[attr], fact2[attr]) and self.env.matcher_function2(fact1[attr], fact2[attr]):
            #         score += 1.0
            # else:
            #     if self.env.matcher_function(fact1[attr], fact2[attr]):
            #         score += 1.0
            # 2
            # if (attr == "degree" or attr == "university") and (fact1[attr]  and fact2[attr] ):
            #       if (fuzz.token_sort_ratio(fact1[attr],fact2[attr]) * 0.01 + word_vectors.similarity(fact1[attr], fact2[attr])) > 0.6:
            #           score += 1.0
            # else:
            #     if fuzz.token_sort_ratio(fact1[attr], fact2[attr]) > 60:
            #         score += 1.0
            # if self.env.matcher_function(fact1[attr], fact2[attr]):

        return score / len(common_attr)

    def get_fac_clusters(self):
        """
        Generate a dataframe containing all facts and true cluster ids
        :return: A Datafrane
        """
        return

    def get_facts_dict(self):
        """
        Generate a dictionary containing all facts and true cluster ids
        :return: A dictionary with facts
        """
        facts = {}
        for fact in self.canonical_facts:
            eid = fact.ent_id
            ent_attr = fact.ent_attr
            tuple = fact.canonicalTuple
            facts.setdefault(eid, {}).setdefault(ent_attr, []).append(tuple)

        return facts

    def evaluateTruthDiscovery(self):
        """
        Iterate over facts and evaluate the precision and recall of Truth Discovery.
        :return: None.
        """
        return

    def load_grd_path(self, grd_path):
        grd_truth = {}
        eid = 0
        rawFrame = pd.read_json(grd_path)
        for entry in rawFrame['body']:
            grd_truth[eid] = {}
            for category in entry:
                if len(category) != 1:
                    raise RuntimeError("Invalid ground truth file")
                attr = list(category.keys())[0]
                normalized_facts = {}
                fact = category[attr]
                normalized_fact = self.normalize_fact(fact)
                grd_truth[eid].setdefault(attr, []).append(normalized_fact)
                # grd_truth[eid][attr] = normalized_facts # depr
            eid += 1
        return grd_truth

    def get_fact_attributes(self, fact):
        """Based on ObservedFact.get_fact_attributes()."""
        if isinstance(fact, dict):
            return fact.keys()
        else:
            return ['_val_']

    def normalize_fact(self, fact):
        """Based on ObservedFact.normalize()."""
        normalized = {}
        attrs = self.get_fact_attributes(fact)
        if attrs == ['_val_']:
            normalized['_val_'] = fact
        else:
            for k in attrs:
                s = fact[k]
                if isinstance(s, list):
                    s = ','.join(s)
                if isinstance(s, six.string_types):
                    if s == '':
                        normalized[k] = None
                    else:
                        # normalized[k] = s.lower().strip()
                        normalized[k] = s.strip()
                else:
                    if s == '':
                        normalized[k] = None
                    else:
                        normalized[k] = str(s)
        return normalized

    def evaluateDataFusion(self):
        """
        Iterate over facts and evaluate the precision and recall of Fusion.
        :return: None.
        """

        inferred_facts = self.get_facts_dict()
        # Iterate over entities in ground truth
        matched = 0.0
        total = 0.0
        unmatched_true = 0.0
        unmatched_inferred = 0.0
        for eid in self.grd_truth:
            self.all_facts[eid] = {}

            for ent_attr in self.grd_truth[eid]:
                self.all_facts[eid][ent_attr] = None
                true_facts = self.grd_truth[eid][ent_attr]
                if eid in inferred_facts:
                    if ent_attr in inferred_facts[eid]:
                        inf_facts = deepcopy(inferred_facts[eid][ent_attr])

                        # self.all_facts[eid][ent_attr] = (list(true_facts.values()), deepcopy(inf_facts)) # depr
                        self.all_facts[eid][ent_attr] = (deepcopy(true_facts), deepcopy(inf_facts))
                    else:
                        # self.all_facts[eid][ent_attr] = (list(true_facts.values()), None) # depr
                        self.all_facts[eid][ent_attr] = (deepcopy(true_facts), None)
                        inf_facts = []
                else:
                    # self.all_facts[eid][ent_attr] = (list(true_facts.values()), None) # depr
                    self.all_facts[eid][ent_attr] = (deepcopy(true_facts), None)
                    inf_facts = []
                for f1id, fact1 in enumerate(true_facts):
                    max_score = 0.0
                    max_index = -1
                    for f2id, fact2 in enumerate(inf_facts):
                        new_score = self.match_score(fact1, fact2)
                        if new_score > max_score:
                            max_score = new_score
                            max_index = f2id
                    if max_index > -1:
                        del inf_facts[max_index]
                        # assert max_score <= 1
                        # matched += math.ceil(max_score)
                        # matched += math.ceil(max_score)
                        matched += 1
                        self.matched_facts_dict.append((eid, ent_attr, fact1, fact2))
                    else:
                        self.unmatched_facts_dict_true.append((eid, ent_attr, fact1))
                        unmatched_true += 1
                    total += 1.0
                unmatched_inferred += len(inf_facts)
                for f2 in inf_facts:
                    self.unmatched_facts_dict_infer.append((eid, ent_attr, f2))

        print(
            "Matched = %.2f, Unmatched true = %.2f, Unmatched inferred = %.2f, Total = %.2f" % (
                matched, unmatched_true, unmatched_inferred, total))
        # print("=========")
        # print("ALL FACTS:")
        with open('matched.json', 'w') as f:
            f.write(json.dumps(self.matched_facts_dict, indent=2))
        with open('unmatched_tr.json', 'w') as f:
            f.write(json.dumps(self.unmatched_facts_dict_true, indent=2))

        # print("=========")
        recall = matched / total
        precision = matched / (matched + unmatched_inferred)
        accuracy = matched / (total + unmatched_inferred)
        f1score = (2 * (precision * recall)) / (precision + recall)
        print("Recall= %.2f" % recall)
        print("Precision = %.2f" % precision)
        print("Accuracy = %.2f" % accuracy)
        print("f1score = %.2f" % f1score)
        # precision = matched / (matched + unmatched_inferred)
        # recall = matched / ( matched + unmatched_true)
        return {'recall': recall, 'precision': precision, 'accuracy': accuracy, 'f1score': f1score}

    def persist_dicts(self):
        pickle.dump(self.matched_facts_dict, open(self.out_path + "matched_facts_dict.pkl", "wb"))
        pickle.dump(self.unmatched_facts_dict_true, open(self.out_path + "unmatched_true_facts.pkl", "wb"))
        pickle.dump(self.unmatched_facts_dict_infer, open(self.out_path + "unmatched_inferred_facts.pkl", "wb"))
