import json
import os

import pandas as pd
import random
from DataGeneration import RANDOM_SEED
from matching.ahc_matcher.similarity_funcs import *

random.seed(RANDOM_SEED)


class EntityInference:
    def __init__(self, original_dataset_filepath: str, with_evaluation=True):
        self.original_dataset_filepath = original_dataset_filepath
        (name, extension) = os.path.splitext(original_dataset_filepath)
        self.dest_dataset_filepath = name + "_inferred" + extension
        self.with_evaluation = with_evaluation
        self.correct_groups_of_facts = []
        self.observed_data = {}
        self.result_clusters = {}
        self.relationship_similarity_cache = {}
        self.record_similarity_cache = {}
        self.relationship_R = {}
        self.record_to_cluster = {}
        self.cluster_to_record = {}
        self.output_dataset = {}
        self.relationship_similarity_threshold = 0.6
        self.algorithm_threshold = 0.28
        self.constant_a = 0.7
        self.record_similarity_name_weight = 0.1
        self.record_similarity_location_weight = 0.3
        self.record_similarity_education_weight = 0.3
        self.record_similarity_working_exp_weight = 0.3
        self.education_tuple_degree_weight = 0.3
        self.education_tuple_university_weight = 0.5
        self.education_tuple_year_weight = 0.2
        self.working_exp_tuple_title_weight = 0.3
        self.working_exp_tuple_company_weight = 0.5
        self.working_exp_tuple_years_weight = 0.2
        self.evaluation_analytical = None
        self.evaluation_summed = None

        # step 1
        self.ingest_observed_facts()
        # step 2
        self.infer_entities()
        # step 3
        self.write_output_dataset()

    def ingest_observed_facts(self):
        bag_of_facts = {}
        fact_id = 1
        try:
            raw_frame = pd.read_json(self.original_dataset_filepath)
            for group in raw_frame['body']:
                # print(group)
                # transform input to match AHC form
                observed_facts_of_source_for_entity = {}
                for observed_fact in group:
                    source = observed_fact['meta']['source']
                    vertical = list(observed_fact.keys())[0]
                    if source not in observed_facts_of_source_for_entity:
                        observed_facts_of_source_for_entity[source] = {}

                    if vertical not in observed_facts_of_source_for_entity[source]:
                        observed_facts_of_source_for_entity[source][vertical] = []

                    observed_facts_of_source_for_entity[source][vertical].append(observed_fact[vertical])

                self.correct_groups_of_facts.append([])
                most_recent_group = len(self.correct_groups_of_facts) - 1
                for source in observed_facts_of_source_for_entity:
                    observed_fact_normalized = observed_facts_of_source_for_entity[source]
                    fact_id_str = 'f' + str(fact_id)
                    observed_fact_normalized['id'] = fact_id_str
                    observed_fact_normalized['source'] = source
                    # construct correct groups for evaluation purposes
                    self.correct_groups_of_facts[most_recent_group].append(observed_fact_normalized)
                    # construct bag of facts (not grouped facts)
                    bag_of_facts[fact_id_str] = observed_fact_normalized

                    fact_id += 1

            # shuffle facts
            bag_of_facts_items = list(bag_of_facts.items())
            random.shuffle(bag_of_facts_items)
            bag_of_facts = dict(bag_of_facts_items)

            self.observed_data = bag_of_facts
            # return bag_of_facts
        except:
            print('Invalid file given as input.')
            raise

    def infer_entities(self):
        self._init_record_to_cluster()
        self._init_relationship_R()
        self._collective_clustering()
        self._construct_result_clusters()
        self._construct_output_dataset()
        if self.with_evaluation is True:
            self._evaluate_inference()

    def _init_record_to_cluster(self):
        record_to_cluster = {}
        for fact_id in self.observed_data:
            record_to_cluster[fact_id] = fact_id.replace('f', 'c')

        self.record_to_cluster = record_to_cluster
        self.cluster_to_record = self._reverse_cluster_to_record()

    def _reverse_cluster_to_record(self):
        reverse_map = dict()
        for fact_id in self.record_to_cluster:
            if self.record_to_cluster[fact_id] not in reverse_map:
                reverse_map[self.record_to_cluster[fact_id]] = []
            reverse_map[self.record_to_cluster[fact_id]].append(fact_id)

        return reverse_map

    def _init_relationship_R(self):

        relationship_R = {}
        for fact1_id in self.observed_data:
            for fact2_id in self.observed_data:
                if fact1_id == fact2_id:
                    continue
                if fact1_id not in relationship_R:
                    relationship_R[fact1_id] = []

                fact1 = self.observed_data[fact1_id]
                fact2 = self.observed_data[fact2_id]

                rel_sim: float
                # attempt finding the relationship similarity from cache or else
                if frozenset({fact1_id, fact2_id}) in self.relationship_similarity_cache:
                    rel_sim = self.relationship_similarity_cache[frozenset({fact1_id, fact2_id})]
                else:
                    # calculate similarity of records based on a dedicated sim. measure, then decide if they are related
                    rel_sim = round(self._relationship_similarity(fact1, fact2), 4)
                    self.relationship_similarity_cache[frozenset({fact1_id, fact2_id})] = rel_sim

                above_threshold = rel_sim >= self.relationship_similarity_threshold
                if above_threshold:
                    relationship_R[fact1_id].append(fact2_id)
        self.relationship_similarity_cache = {}
        self.relationship_R = relationship_R

    def _relationship_similarity(self, record1, record2):
        result = 0.5 * get_name_similarity(record1['Name'][0]['name'].lower(),
                                           record2['Name'][0]['name'].lower()) + 0.5 * get_location_similarity(
            record1['Location'][0]['location'].lower(), record2['Location'][0]['location'].lower())
        return result

    def _collective_clustering(self):
        iteration = 1
        while True:
            num_comparisons = 0
            clusters = list(set(self.record_to_cluster.values()))
            max_cluster_similarity = {'max_sim_value': 0, 'cluster1': '', 'cluster2': ''}

            for i in range(len(clusters)):
                for j in range(i + 1, len(clusters)):
                    if clusters[i] == clusters[j]: continue
                    if self._clusters_contain_facts_from_same_source(clusters[i], clusters[j]): continue

                    clusters_combined_similarity = self._cluster_similarity(clusters[i], clusters[j])
                    if clusters_combined_similarity > max_cluster_similarity['max_sim_value']:
                        max_cluster_similarity['max_sim_value'] = clusters_combined_similarity
                        max_cluster_similarity['cluster1'] = clusters[i]
                        max_cluster_similarity['cluster2'] = clusters[j]

                    num_comparisons += 1

            if max_cluster_similarity['max_sim_value'] < self.algorithm_threshold:
                return
            else:
                self._cluster_similarity(max_cluster_similarity['cluster1'], max_cluster_similarity['cluster2'])
                self.merge_clusters(max_cluster_similarity['cluster1'], max_cluster_similarity['cluster2'])
                iteration += 1

    def merge_clusters(self, cluster1, cluster2):
        records_of_cluster1 = self._get_records_of_cluster(cluster1)
        records_of_cluster2 = self._get_records_of_cluster(cluster2)

        new_cluster = cluster1 + '_' + cluster2

        for i in range(len(records_of_cluster1)):
            self.record_to_cluster[records_of_cluster1[i]] = new_cluster

        for i in range(len(records_of_cluster2)):
            self.record_to_cluster[records_of_cluster2[i]] = new_cluster

        self.cluster_to_record = self._reverse_cluster_to_record()

    def _clusters_contain_facts_from_same_source(self, cluster1, cluster2):
        r1 = self._get_records_of_cluster(cluster1)
        r2 = self._get_records_of_cluster(cluster2)
        sources_of_r1 = [self._get_record_by_id(r)['source'] for r in r1]
        sources_of_r2 = [self._get_record_by_id(r)['source'] for r in r2]
        common_sources = len(set(sources_of_r1).intersection(sources_of_r2))
        return common_sources > 0

    def _cluster_similarity(self, cluster1, cluster2):
        attribute_similarity = round(self._cluster_attribute_similarity(cluster1, cluster2), 4)
        neighborhood_similarity = round(self._cluster_neighborhood_similarity(cluster1, cluster2), 5)
        clusters_combined_similarity = round(
            self.constant_a * attribute_similarity + (1 - self.constant_a) * neighborhood_similarity, 4)

        return clusters_combined_similarity

    def _cluster_neighborhood_similarity(self, cluster1, cluster2, verbose=False):
        # sim(c1,c2) = ½ [simattributes(c1,c2) + simneighbours(c1,c2)] - with Complete Link
        N_A = self._get_neighborhood_of_cluster(cluster1)
        N_B = self._get_neighborhood_of_cluster(cluster2)
        intersection = N_A.intersection(N_B)
        union = N_A.union(N_B)
        if len(union) == 0:
            return 0
        result = len(intersection) / len(union)
        return result

    def _get_neighborhood_of_cluster(self, cluster):
        neighborhood = set()
        records_of_cluster = self._get_records_of_cluster(cluster)
        for record in records_of_cluster:
            for related_record in self.relationship_R[record]:
                neighborhood.add(self.record_to_cluster[related_record])
        return neighborhood

    def _cluster_attribute_similarity(self, cluster1, cluster2):
        # sim(c1,c2) = ½ [simattributes(c1,c2) + simneighbours(c1,c2)] - with Complete Link
        max_sim = 0
        records_cluster1 = self._get_records_of_cluster(cluster1)
        records_cluster2 = self._get_records_of_cluster(cluster2)
        for fact1_id in records_cluster1:
            for fact2_id in records_cluster2:
                if fact1_id == fact2_id:
                    continue

                fact1 = self.observed_data[fact1_id]
                fact2 = self.observed_data[fact2_id]
                record_sim = self._record_similarity(fact1_id, fact2_id)
                if record_sim > max_sim:
                    max_sim = record_sim

        return max_sim

    def _record_similarity(self, fact1_id, fact2_id):
        fact1 = self.observed_data[fact1_id]
        fact2 = self.observed_data[fact2_id]

        # if found in cache, just fetch the similarity and return it
        if frozenset([fact1_id, fact2_id]) in self.record_similarity_cache:
            result = self.record_similarity_cache[frozenset([fact1_id, fact2_id])]
            return result

        name_sim = round(self._name_vertical_similarity(fact1, fact2), 4)
        loc_sim = round(self._location_vertical_similarity(fact1, fact2), 4)
        edu_sim = round(self._education_vertical_similarity(fact1, fact2), 4)
        work_sim = round(self._working_experience_vertical_similarity(fact1, fact2), 4)

        result = self.record_similarity_location_weight * loc_sim + \
                 self.record_similarity_name_weight * name_sim + \
                 self.record_similarity_education_weight * edu_sim + \
                 self.record_similarity_working_exp_weight * work_sim

        self.record_similarity_cache[frozenset([fact1_id, fact2_id])] = round(result, 4)
        return result

    def _name_vertical_similarity(self, record1, record2):
        # 'Name' vertical always has cardinality of 1 tuple
        name1 = record1['Name'][0]['name'].lower()
        name2 = record2['Name'][0]['name'].lower()
        result = get_name_similarity(name1, name2)
        return result

    def _location_vertical_similarity(self, record1, record2):
        # 'Location' vertical always has cardinality of 1 tuple
        location1 = record1.get('Location')
        location2 = record2.get('Location')
        if location1 is None or location2 is None:
            return 0

        location1 = record1['Location'][0]['location'].lower()
        location2 = record2['Location'][0]['location'].lower()
        result = get_location_similarity(location1, location2)
        return result

    def _education_vertical_similarity(self, record1, record2):
        # 'Education' vertical may have cardinality of 1 or more tuple
        # we assume that the similarity between two multi-tuple education records
        # is the maximum similarity of all tuple pairs
        education1 = record1.get('Education')
        education2 = record2.get('Education')

        if education1 is None or education2 is None:
            return 0

        max_similarity = 0
        similarity_sum = 0
        count_pairs = 0
        for index1, tuple1 in enumerate(education1):
            for index2, tuple2 in enumerate(education2):
                tuple_similarity = self._education_tuple_similarity(tuple1, tuple2)
                # avg tuple similarity
                similarity_sum += tuple_similarity
                count_pairs += 1
                # or maximum tuple similarity
                if tuple_similarity > max_similarity:
                    max_similarity = tuple_similarity

        if count_pairs != 0:
            avg_similarity = similarity_sum / count_pairs
        else:
            avg_similarity = 0

        # return avg_similarity
        return max_similarity

    def _working_experience_vertical_similarity(self, record1, record2):
        # 'Working Experience' vertical may have cardinality of 1 or more tuple
        # we assume that the similarity between two multi-tuple education records
        # is the maximum similarity between all tuple pairs
        working_exp_1 = record1.get('Working Experience')
        working_exp_2 = record2.get('Working Experience')

        if working_exp_1 is None or working_exp_2 is None:
            return 0

        max_similarity = 0
        similarity_sum = 0
        count_pairs = 0
        for index1, tuple1 in enumerate(working_exp_1):
            for index2, tuple2 in enumerate(working_exp_2):
                # printer.log([global_vars.LOG], 'Comparing tuples', tuple1, 'VS', tuple2)
                tuple_similarity = self._working_experience_tuple_similarity(tuple1, tuple2)
                # avg tuple similarity
                similarity_sum += tuple_similarity
                count_pairs += 1
                # or maximum tuple similarity
                if tuple_similarity > max_similarity:
                    max_similarity = tuple_similarity

        if count_pairs != 0:
            avg_similarity = similarity_sum / count_pairs
        else:
            avg_similarity = 0

        # return avg_similarity
        return max_similarity

    def _education_tuple_similarity(self, tuple1, tuple2):
        # input tuple form: {'degree': 'Ph.D.', 'university': 'UC_Berkley', 'year': '2004'}
        degree_sim = round(get_education_degree_similarity(tuple1.get('degree', ''), tuple2.get('degree', '')), 4)
        university_sim = round(
            get_education_university_similarity(tuple1.get('university', ''), tuple2.get('university', '')),
            4)
        year_sim = round(get_education_year_similarity(tuple1.get('year', ''), tuple2.get('year', '')), 4)

        result = self.education_tuple_degree_weight * degree_sim + \
                 self.education_tuple_university_weight * university_sim + \
                 self.education_tuple_year_weight * year_sim

        return result

    def _working_experience_tuple_similarity(self, tuple1, tuple2):
        # input tuple form: {'title': 'developer', 'company': 'Lucas-Hernandez', 'years': '199-2004'}
        title_sim = round(get_working_experience_title_similarity(tuple1.get('title', ''), tuple2.get('title', '')), 4)
        company_sim = round(
            get_working_experience_company_similarity(tuple1.get('company', ''), tuple2.get('company', '')),
            4)
        years_sim = round(get_working_experience_years_similarity(tuple1.get('years', ''), tuple2.get('years', '')), 4)

        result = self.working_exp_tuple_title_weight * title_sim + \
                 self.working_exp_tuple_company_weight * company_sim + \
                 self.working_exp_tuple_years_weight * years_sim
        return result

    def _get_records_of_cluster(self, cluster):
        result = self.cluster_to_record[cluster]
        return result

    def _get_record_by_id(self, fact_id):
        return self.observed_data[fact_id]

    def _construct_result_clusters(self):
        parent_group = []
        for cluster in self.cluster_to_record:
            # if verbose: print(cluster)
            parent_group.append([])
            most_recent_group = len(parent_group) - 1
            for fact in self.cluster_to_record[cluster]:
                parent_group[most_recent_group].append(self._get_record_by_id(fact))
        self.result_clusters = parent_group

    def _construct_output_dataset(self):
        output_dataset = {'body': []}
        for entity_group in self.result_clusters:
            formatted_group = []
            for fact in entity_group:
                src_of_group = fact['source']
                for key in fact:
                    if type(fact[key]) is not list:
                        continue
                    for obs in fact[key]:
                        formatted_obs = {key: obs, 'meta': {'source': src_of_group}}
                        formatted_group.append(formatted_obs)
            output_dataset['body'].append(formatted_group)

        self.output_dataset = output_dataset

    def write_output_dataset(self):
        with open(self.dest_dataset_filepath, 'w') as fp:
            json.dump(self.output_dataset, fp)

    def _evaluate_inference(self):
        self.evaluate_result_clusters()
        self.sum_evaluation_for_all_facts()

    def _evaluate_fact(self, fact_id):
        # find elements in the same cluster as the given fact
        records_of_fact_cluster = self._get_records_of_cluster(self.record_to_cluster[fact_id]).copy()
        records_of_fact_cluster.remove(fact_id)
        neighboring_elements_of_fact_in_cluster = records_of_fact_cluster

        # find elements in the same cluster with the given as found in the correct groups
        correct_group = []
        for group in self.correct_groups_of_facts:
            for fact in group:
                if fact["id"] == fact_id:
                    correct_group = group
                    break
        correct_neighboring_elements_of_fact_in_cluster = []
        for fact in correct_group:
            if fact["id"] == fact_id: continue
            correct_neighboring_elements_of_fact_in_cluster.append(fact["id"])

        # compare predicted neighbors vs actual neighbors of the fact
        false_positives = sum(
            el not in correct_neighboring_elements_of_fact_in_cluster for el in neighboring_elements_of_fact_in_cluster)
        true_positives = sum(
            el in correct_neighboring_elements_of_fact_in_cluster for el in neighboring_elements_of_fact_in_cluster)
        false_negatives = sum(
            el not in neighboring_elements_of_fact_in_cluster for el in correct_neighboring_elements_of_fact_in_cluster)

        false_facts = list(self.observed_data.keys()).copy()
        for f in correct_neighboring_elements_of_fact_in_cluster:
            false_facts.remove(f)

        true_negatives = sum(el not in neighboring_elements_of_fact_in_cluster for el in false_facts)

        result = {
            "false_positives": false_positives,
            "true_positives": true_positives,
            "false_negatives": false_negatives,
            "true_negatives": true_negatives,
            "predicted_neighbors": neighboring_elements_of_fact_in_cluster,
            "correct_neighbors": correct_neighboring_elements_of_fact_in_cluster,
            "total_facts": len(self.observed_data.keys())
        }
        return result

    def evaluate_result_clusters(self):
        # for every fact in the result clusters count:
        # a) facts falsely in the same group (false positives) - ta stoixeia pou einai geitones enw de tha eprepe
        # b) facts correctly in the same group (true positives) - ta stoixeia pou einai geitones enw tha eprepe
        # c) facts falsely not in the same group (false negative) - ta stoixeia pou den einai geitones enw tha eprepe
        # d) facts correctly not in the same group (true negative) - ta stoixeia pou den einai geitones enw den tha eprepe
        result = dict()
        for result_cluster in self.result_clusters:
            for fact in result_cluster:
                fact_id = fact["id"]
                result[fact_id] = self._evaluate_fact(fact_id)
        self.evaluation_analytical = result

    def sum_evaluation_for_all_facts(self):
        total_FP = 0
        total_TP = 0
        total_FN = 0
        total_TN = 0
        for fact_id in self.evaluation_analytical:
            total_FP += self.evaluation_analytical[fact_id]["false_positives"]
            total_TP += self.evaluation_analytical[fact_id]["true_positives"]
            total_FN += self.evaluation_analytical[fact_id]["false_negatives"]
            total_TN += self.evaluation_analytical[fact_id]["true_negatives"]
        result = {
            "total_FP": total_FP,
            "total_TP": total_TP,
            "total_FN": total_FN,
            "total_TN": total_TN,
        }
        self.evaluation_summed = result
