import pandas as pd
import itertools
import random

random.seed(10)
from strsimpy.jaro_winkler import JaroWinkler
from strsimpy.cosine import Cosine
from strsimpy.sorensen_dice import SorensenDice
import io
import json

# import jellyfish
# from strsimpy.qgram import QGram
# from strsimpy.overlap_coefficient import OverlapCoefficient
# from strsimpy.metric_lcs import MetricLCS
# from strsimpy.jaccard import Jaccard

# constants


observed_facts_file_path = 'C:/Users/Iasonas/PycharmProjects/EntityMatching_Thesis/datasets/observed_facts.json'

verbose_file = True
verbose_console = False
experiment = True
experiment_with_combinations = True

# initialize similarity functions
cosine = Cosine(2)
jaro_winkler = JaroWinkler()
sor_dice = SorensenDice()

# global log
global_log = io.StringIO()

# global variables
observed_data = []
correct_groups_of_facts = []


def cosine_similarity(s1, s2):
    result = cosine.similarity(s1, s2)
    return result


def jaro_winkler_similarity(s1, s2):
    result = jaro_winkler.similarity(s1, s2)
    return result


def sorensen_dice_similarity(s1, s2):
    result = sor_dice.similarity(s1, s2)
    return result


def relationship_similarity_v1(record1, record2):
    result = name_vertical_similarity(record1, record2) + location_vertical_similarity(record1, record2)
    return result


def relationship_similarity_v2(record1, record2):
    result = name_vertical_similarity(record1, record2)
    return result


# default program parameters
program_parameters = {
    "name_sim_func": cosine_similarity,
    "location_sim_func": cosine_similarity,
    "education_degree_sim_func": cosine_similarity,
    "education_university_sim_func": cosine_similarity,
    "education_year_sim_func": cosine_similarity,
    "working_experience_title_sim_func": cosine_similarity,
    "working_experience_company_sim_func": cosine_similarity,
    "working_experience_years_sim_func": cosine_similarity,
    "relationship_R_sim_func": relationship_similarity_v1,
    "relationship_similarity_threshold": 0.6,
    "algorithm_threshold": 0.6,
    "constant_a": 0.7
}

string_similarity_functions = [cosine_similarity, jaro_winkler_similarity, sorensen_dice_similarity]

relationship_R_initialization_functions = [relationship_similarity_v1, relationship_similarity_v2]


def ingest_observed_data(file_path):
    bag_of_facts = []
    fact_id = 1
    try:
        raw_frame = pd.read_json(file_path)
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

            correct_groups_of_facts.append([])
            most_recent_group = len(correct_groups_of_facts) - 1
            for source in observed_facts_of_source_for_entity:
                observed_fact_normalized = observed_facts_of_source_for_entity[source]
                observed_fact_normalized['id'] = 'f' + str(fact_id)
                # construct correct groups for evaluation purposes
                correct_groups_of_facts[most_recent_group].append(observed_fact_normalized)
                # construct bag of facts (not grouped facts)
                bag_of_facts.append(observed_fact_normalized)

                fact_id += 1

        random.shuffle(bag_of_facts)
        global observed_data
        observed_data = bag_of_facts
        # return bag_of_facts
    except:
        if verbose_file: print('Invalid file given as input.')
        raise


def name_vertical_similarity(record1, record2):
    # 'Name' vertical always has cardinality of 1 tuple
    if verbose_file: print('Comparing Name vertical.', file=global_log)

    name1 = record1['Name'][0]['name'].lower()
    name2 = record2['Name'][0]['name'].lower()
    result = program_parameters["name_sim_func"](name1, name2)
    # result5 = jellyfish.jaro_winkler_similarity(name1, name2)
    # result = jellyfish.jaro_similarity(name1, name2)
    # jw = JaroWinkler()
    # result = jw.similarity(name1, name2)
    # qg = QGram()
    # result = qg.distance(name1, name2)
    # oc = OverlapCoefficient()
    # result3 = oc.similarity(name1, name2)
    # metric_lcs = MetricLCS()
    # result4 = metric_lcs.distance(name1, name2)
    # jac = Jaccard(2)
    # result6 = jac.similarity(name1, name2)
    # sd = SorensenDice()
    # result7 = sd.similarity(name1, name2)

    if verbose_file: print(name1, 'vs', name2, '=', result, file=global_log)

    if verbose_file: print('Name similarity=', result, file=global_log)

    return result


def location_vertical_similarity(record1, record2):
    # 'Location' vertical always has cardinality of 1 tuple
    if verbose_file: print('Comparing Location vertical.', file=global_log)

    location1 = record1.get('Location')
    location2 = record2.get('Location')

    if location1 is None or location2 is None:
        return 0

    location1 = record1['Location'][0]['location'].lower()
    location2 = record2['Location'][0]['location'].lower()
    # result = jellyfish.jaro_winkler_similarity(location1, location2)
    result = program_parameters["location_sim_func"](location1, location2)
    if verbose_file: print(location1, 'vs', location2, '=', result, file=global_log)

    if verbose_file: print('Location similarity=', result, file=global_log)

    return result


def education_degree_similarity(degree1, degree2):
    result = program_parameters["education_degree_sim_func"](degree1, degree2)
    return result


def education_university_similarity(university1, university2):
    result = program_parameters["education_university_sim_func"](university1, university2)
    return result


def education_year_similarity(year1, year2):
    result = program_parameters["education_year_sim_func"](year1, year2)
    return result


def education_tuple_similarity(tuple1, tuple2):
    # input tuple form: {'degree': 'Ph.D.', 'university': 'UC_Berkley', 'year': '2004'}

    degree_sim = education_degree_similarity(tuple1.get('degree', ''), tuple2.get('degree', ''))
    university_sim = education_university_similarity(tuple1.get('university', ''), tuple2.get('university', ''))
    year_sim = education_year_similarity(tuple1.get('year', ''), tuple2.get('year', ''))

    if verbose_file: print(tuple1.get('degree', ''), 'vs', tuple2.get('degree', ''), '=', degree_sim, file=global_log)
    if verbose_file: print(tuple1.get('university', ''), 'vs', tuple2.get('university', ''), '=', university_sim,
                           file=global_log)
    if verbose_file: print(tuple1.get('year', ''), 'vs', tuple2.get('year', ''), '=', year_sim, file=global_log)
    result = 0.3 * degree_sim + 0.5 * university_sim + 0.2 * year_sim

    if verbose_file: print('Tuples similarity=', 0.3, '*', degree_sim, '+', 0.5, '*', university_sim, '+', 0.2, '*',
                           year_sim, '=',
                           result, file=global_log)

    return result


def education_vertical_similarity(record1, record2):
    # 'Education' vertical may have cardinality of 1 or more tuple
    # we assume that the similarity between two multi-tuple education records
    # is the average similarity of all tuple pairs
    if verbose_file: print('Comparing Education vertical.', file=global_log)

    education1 = record1.get('Education')
    education2 = record2.get('Education')

    if education1 is None or education2 is None:
        return 0

    max_similarity = 0
    similarity_sum = 0
    count_pairs = 0
    for index1, tuple1 in enumerate(education1):
        for index2, tuple2 in enumerate(education2):
            if verbose_file: print('Comparing tuples', tuple1, 'vs', tuple2, file=global_log)
            tuple_similarity = education_tuple_similarity(tuple1, tuple2)
            # if verbose: print(tuple1, 'vs', tuple2, '=', tuple_similarity, file=global_log)
            # if verbose: print('Tuples similarity=', tuple_similarity, file=global_log)
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

    if verbose_file: print('Education similarity (max tuples similarity)=', max_similarity, file=global_log)
    return max_similarity


def working_experience_title_similarity(title1, title2):
    result = program_parameters["working_experience_title_sim_func"](title1, title2)
    return result


def working_experience_company_similarity(company1, company2):
    result = program_parameters["working_experience_company_sim_func"](company1, company2)
    return result


def working_experience_years_similarity(year_range1, year_range2):
    result = program_parameters["working_experience_years_sim_func"](year_range1, year_range2)
    return result


def working_experience_tuple_similarity(tuple1, tuple2):
    # input tuple form: {'title': 'developer', 'company': 'Lucas-Hernandez', 'years': '199-2004'}
    title_sim = working_experience_title_similarity(tuple1.get('title', ''), tuple2.get('title', ''))
    company_sim = working_experience_company_similarity(tuple1.get('company', ''), tuple2.get('company', ''))
    years_sim = working_experience_years_similarity(tuple1.get('years', ''), tuple2.get('years', ''))

    if verbose_file: print(tuple1.get('title', ''), 'vs', tuple2.get('title', ''), '=', title_sim, file=global_log)
    if verbose_file: print(tuple1.get('company', ''), 'vs', tuple2.get('company', ''), '=', company_sim,
                           file=global_log)
    if verbose_file: print(tuple1.get('years', ''), 'vs', tuple2.get('years', ''), '=', years_sim, file=global_log)
    result = 0.3 * title_sim + 0.5 * company_sim + 0.2 * years_sim

    if verbose_file: print('Tuples similarity=', 0.3, '*', title_sim, '+', 0.5, '*', company_sim, '+', 0.2, '*',
                           years_sim,
                           '=',
                           result, file=global_log)

    return result


def working_experience_vertical_similarity(record1, record2):
    # 'Working Experience' vertical may have cardinality of 1 or more tuple
    # we assume that the similarity between two multi-tuple education records
    # is the average similarity of all tuple pairs
    if verbose_file: print('Comparing Working Experience vertical.', file=global_log)

    working_exp_1 = record1.get('Working Experience')
    working_exp_2 = record2.get('Working Experience')

    if working_exp_1 is None or working_exp_2 is None:
        return 0

    max_similarity = 0
    similarity_sum = 0
    count_pairs = 0
    for index1, tuple1 in enumerate(working_exp_1):
        for index2, tuple2 in enumerate(working_exp_2):
            if verbose_file: print('Comparing tuples', tuple1, 'vs', tuple2, file=global_log)
            tuple_similarity = working_experience_tuple_similarity(tuple1, tuple2)
            # if verbose: print(tuple1, 'vs', tuple2, '=', tuple_similarity, file=global_log)
            # if verbose: print('Tuples similarity=', tuple_similarity, file=global_log)
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

    if verbose_file: print('Working Experience similarity (max tuples similarity)=', max_similarity, file=global_log)
    # return avg_similarity
    return max_similarity


def relationship_similarity(record1, record2):
    result = program_parameters["relationship_R_sim_func"](record1, record2)
    return result


def init_relationship_R():
    if verbose_file: print('Initializing relationship R...', file=global_log)

    relationship_R = {}
    for record1 in observed_data:
        for record2 in observed_data:
            if record1['id'] == record2['id']:
                continue
            if record1['id'] not in relationship_R:
                relationship_R[record1['id']] = []
            # calculate similarity of records based on a dedicated sim. measure, then decide if they are related
            rel_sim = relationship_similarity(record1, record2)
            if verbose_file: print('relationship sim.', record1, 'vs', record2, '=', rel_sim, file=global_log)
            if rel_sim >= program_parameters["relationship_similarity_threshold"]:
                relationship_R[record1['id']].append(record2['id'])

    return relationship_R


def init_record_to_cluster():
    record_to_cluster = {}
    for i in range(len(observed_data)):
        record_to_cluster['f' + str(i + 1)] = 'c' + str(i + 1)

    return record_to_cluster


def record_similarity(record1_id, record2_id):
    record1 = [rec for rec in observed_data if rec["id"] == record1_id][0]
    record2 = [rec for rec in observed_data if rec["id"] == record2_id][0]

    # if len(record1.keys()) != len(record2.keys()):
    common_verticals_count = len(set(record1.keys()).intersection(set(record2.keys())))
    # if common_verticals_count == 5:
    name_sim = name_vertical_similarity(record1, record2)
    loc_sim = location_vertical_similarity(record1, record2)
    edu_sim = education_vertical_similarity(record1, record2)
    work_sim = working_experience_vertical_similarity(record1, record2)
    # if verbose:
    # if verbose: print('Attribute sim. of ', record1_id, record2_id, file=global_log)
    # if verbose: print(record1.get('Name'), 'vs', record2.get('Name'), '=', name_sim, file=global_log)
    # if verbose: print(record1.get('Location'), 'vs', record2.get('Location'), '=', loc_sim, file=global_log)
    # if verbose: print(record1.get('Education'), 'vs', record2.get('Education'), '=', edu_sim, file=global_log)
    # if verbose: print(record1.get('Working Experience'), 'vs', record2.get('Working Experience'), '=', work_sim, file=global_log)

    result = 0.1 * loc_sim + 0.3 * name_sim + 0.3 * edu_sim + 0.3 * work_sim

    if verbose_file: print('Attribute sim. of records', record1_id, record2_id, '=', 0.3, '*', name_sim, '+', 0.1, '*',
                           loc_sim, '+',
                           0.3, '*', edu_sim, '+', 0.3, '*', work_sim, '=', result, file=global_log)

    # else:
    #     if common_verticals_count == 0:
    #         result = 0
    #     else:
    #         loc_sim = location_vertical_similarity(record1,
    #                                                record2) if 'Location' in record1 and 'Location' in record2 else 0
    #         name_sim = name_vertical_similarity(record1, record2) if 'Name' in record1 and 'Name' in record2 else 0
    #         edu_sim = education_vertical_similarity(record1,
    #                                                 record2) if 'Education' in record1 and 'Education' in record2 else 0
    #         work_sim = working_experience_vertical_similarity(record1,
    #                                                           record2) if 'Working Experience' in record1 and 'Working Experience' in record2 else 0
    #         result = (loc_sim + name_sim + edu_sim + work_sim) / common_verticals_count

    return result


def cluster_attribute_similarity(cluster1, cluster2, record_to_cluster, verbose=False):
    # sim(c1,c2) = ½ [simattributes(c1,c2) + simneighbours(c1,c2)] - with Complete Link

    max_sim = 0
    records_cluster1 = get_records_of_cluster(cluster1, record_to_cluster)
    records_cluster2 = get_records_of_cluster(cluster2, record_to_cluster)
    for record1 in records_cluster1:
        for record2 in records_cluster2:
            if record1 == record2:
                continue
            if verbose: print('Comparing records', record1, record2, 'by Attribute similarity:', file=global_log)
            record_sim = record_similarity(record1, record2)
            # if verbose:
            # if verbose: print('Attribute similarity of', record1, record2, '=', record_sim, file=global_log)
            if record_sim > max_sim:
                max_sim = record_sim

    return max_sim


def cluster_neighborhood_similarity(cluster1, cluster2, record_to_cluster, relationship_R, verbose=False):
    # sim(c1,c2) = ½ [simattributes(c1,c2) + simneighbours(c1,c2)] - with Complete Link
    if verbose: print('Calculating clusters', cluster1, 'vs', cluster2, 'Neighborhood similarity', file=global_log)
    N_A = set()
    records_of_cluster1 = get_records_of_cluster(cluster1, record_to_cluster)
    if verbose: print('Records of cluster', cluster1, ':', records_of_cluster1, file=global_log)
    for record in records_of_cluster1:
        rel_records_str = [(r + ' (in ' + record_to_cluster[r] + ')') for r in relationship_R[record]]
        if verbose: print('Records related to', record, ':', rel_records_str, file=global_log)
        for related_record in relationship_R[record]:
            N_A.add(record_to_cluster[related_record])
    if verbose: print('Neighborhood (clusters) of', cluster1, ':', N_A, file=global_log)

    N_B = set()
    records_of_cluster2 = get_records_of_cluster(cluster2, record_to_cluster)
    if verbose: print('Records of cluster', cluster2, ':', records_of_cluster2, file=global_log)
    for record in records_of_cluster2:
        rel_records_str = [(r + ' (in ' + record_to_cluster[r] + ')') for r in relationship_R[record]]
        if verbose: print('Records related to', record, ':', rel_records_str, file=global_log)
        for related_record in relationship_R[record]:
            N_B.add(record_to_cluster[related_record])
    if verbose: print('Neighborhood (clusters) of', cluster2, ':', N_B, file=global_log)

    intersection = N_A.intersection(N_B)
    union = N_A.union(N_B)

    if len(union) == 0:
        return 0

    result = len(intersection) / len(union)
    # if verbose:
    if verbose: print('Neighborhood of', cluster2, ':', N_B, file=global_log)
    if verbose: print('Intersection:', intersection, file=global_log)
    if verbose: print('Union:', union, file=global_log)
    if verbose: print('Neighborhood similarity=|intersection|/|union|=', len(intersection), '/', len(union), result,
                      file=global_log)
    return result


def get_records_of_cluster(cluster, record_to_cluster):
    result = []
    for rec in record_to_cluster:
        if record_to_cluster[rec] == cluster:
            result.append(rec)

    return result


def merge_clusters(cluster1, cluster2, record_to_cluster):
    records_of_cluster1 = get_records_of_cluster(cluster1, record_to_cluster)
    records_of_cluster2 = get_records_of_cluster(cluster2, record_to_cluster)

    new_cluster = cluster1 + '_' + cluster2

    for i in range(len(records_of_cluster1)):
        record_to_cluster[records_of_cluster1[i]] = new_cluster

    for i in range(len(records_of_cluster2)):
        record_to_cluster[records_of_cluster2[i]] = new_cluster

    if verbose_file: print('Result: records of', new_cluster, '=',
                           get_records_of_cluster(new_cluster, record_to_cluster),
                           file=global_log)

    return record_to_cluster


def reverse_cluster_to_record(record_to_cluster):
    reverse_map = dict()
    for record in record_to_cluster:
        if record_to_cluster[record] not in reverse_map:
            reverse_map[record_to_cluster[record]] = []
        reverse_map[record_to_cluster[record]].append(record)

    return reverse_map


def print_cluster(cluster, record_to_cluster):
    if verbose_file: print(cluster, '[', file=global_log)
    for record in record_to_cluster:
        if record_to_cluster[record] == cluster:
            if verbose_file: print('\t', record, get_record_by_id(record), file=global_log)
    if verbose_file: print(']', file=global_log)


def print_observed_data():
    if verbose_file: print('[', file=global_log)
    for record in observed_data:
        if verbose_file: print('\t', record['id'], record, file=global_log)
    if verbose_file: print(']', file=global_log)


def collective_clustering(relationship_R, record_to_cluster):
    iteration = 1

    while True:
        if verbose_console: print('Iteration', iteration, '...')
        if verbose_file: print('Iteration', iteration, '...', file=global_log)
        num_comparisons = 0
        clusters = list(set(record_to_cluster.values()))
        max_cluster_similarity = {'max_sim_value': 0, 'cluster1': '', 'cluster2': ''}

        for i in range(len(clusters)):
            for j in range(i + 1, len(clusters)):
                if clusters[i] == clusters[j]: continue
                # if verbose: print_cluster(clusters[i], record_to_cluster)
                # if verbose: print_cluster(clusters[j], record_to_cluster)
                if verbose_file: print('Comparing clusters:', clusters[i], 'vs', clusters[j], '...', file=global_log)
                print_cluster(clusters[i], record_to_cluster)
                print_cluster(clusters[j], record_to_cluster)
                clusters_combined_similarity = cluster_similarity(clusters[i], clusters[j],
                                                                  record_to_cluster,
                                                                  relationship_R,
                                                                  # verbose=True
                                                                  )
                # if verbose: print('Comparing:', clusters[i], clusters[j], '=', clusters_combined_similarity)

                if clusters_combined_similarity > max_cluster_similarity['max_sim_value']:
                    max_cluster_similarity['max_sim_value'] = clusters_combined_similarity
                    max_cluster_similarity['cluster1'] = clusters[i]
                    max_cluster_similarity['cluster2'] = clusters[j]

                # console.log(`sim(${clusters[i]},${clusters[j]}) = 1/2 [${attrSim} + ${neighSim}] = ${similarity}`)
                num_comparisons += 1

        if verbose_file: print('Maximum Similarity was between clusters', max_cluster_similarity['cluster1'], 'and',
                               max_cluster_similarity['cluster2'], 'with value',
                               max_cluster_similarity['max_sim_value'],
                               file=global_log)

        if max_cluster_similarity['max_sim_value'] < program_parameters["algorithm_threshold"]:
            if verbose_file: print('Threshold', program_parameters["algorithm_threshold"], 'was reached by',
                                   max_cluster_similarity['max_sim_value'],
                                   '. Terminating...', file=global_log)
            if verbose_file: print(reverse_cluster_to_record(record_to_cluster), file=global_log)
            print(iteration, 'total iterations.')
            return record_to_cluster
        else:
            if verbose_file: print('Merging clusters:', file=global_log)
            print_cluster(max_cluster_similarity['cluster1'], record_to_cluster)
            print_cluster(max_cluster_similarity['cluster2'], record_to_cluster)
            cluster_similarity(max_cluster_similarity['cluster1'], max_cluster_similarity['cluster2'],
                               record_to_cluster,
                               relationship_R,
                               # verbose=True
                               )
            if verbose_file: print('Merging', max_cluster_similarity['cluster1'], '(',
                                   get_records_of_cluster(max_cluster_similarity['cluster1'], record_to_cluster), ')',
                                   max_cluster_similarity['cluster2'], '(',
                                   get_records_of_cluster(max_cluster_similarity['cluster2'], record_to_cluster), ')',
                                   '...',
                                   file=global_log)
            record_to_cluster = merge_clusters(max_cluster_similarity['cluster1'], max_cluster_similarity['cluster2'],
                                               record_to_cluster)
            # print(reverse_cluster_to_record(record_to_cluster))
            if verbose_file: print('Result Clusters after iteration:', file=global_log)
            pretty_print_result_clusters(record_to_cluster)
            iteration += 1


def cluster_similarity(cluster1, cluster2, record_to_cluster, relationship_R, verbose=False):
    attribute_similarity = round(
        cluster_attribute_similarity(cluster1, cluster2, record_to_cluster, verbose), 2)
    # if verbose:
    # if verbose: print('Comparing:', cluster1, cluster2, file=global_log)
    if verbose: print('Attribute similarity of clusters (maximum sim. of cluster records)', cluster1, cluster2, '=',
                      attribute_similarity, file=global_log)

    neighborhood_similarity = round(
        cluster_neighborhood_similarity(cluster1, cluster2, record_to_cluster, relationship_R, verbose=True), 2)
    # if verbose:
    # if verbose: print('neighborhood_similarity=', '(', cluster1, cluster2, ')', neighborhood_similarity, file=global_log)

    clusters_combined_similarity = round(
        program_parameters["constant_a"] * attribute_similarity + (
                1 - program_parameters["constant_a"]) * neighborhood_similarity, 2)
    # if verbose:
    if verbose: print('Combined cluster similarity=', program_parameters["constant_a"], '* attribute_similarity + ',
                      (1 - program_parameters["constant_a"]),
                      '* neighborhood_similarity=', program_parameters["constant_a"], '*', attribute_similarity, '+',
                      (1 - program_parameters["constant_a"]), '*',
                      neighborhood_similarity, '=', clusters_combined_similarity, file=global_log)
    # if verbose:
    # if verbose: print('Comparing:', cluster1, cluster2, ': clusters_combined_similarity=', constant_a, '*',
    #       attribute_similarity, '+', (1 - constant_a), '*',
    #       neighborhood_similarity, '=', clusters_combined_similarity, file=global_log)
    return clusters_combined_similarity


def pretty_print_R(relationship_R):
    if verbose_file: print('Relationship R...', file=global_log)
    seen = set()
    for record_id in relationship_R:
        if record_id in seen:
            continue
        seen.add(record_id)
        seen.update(relationship_R[record_id])
        if verbose_file: print('[', file=global_log)
        if verbose_file: print('\t', record_id, get_record_by_id(record_id), file=global_log)
        for recd in relationship_R[record_id]:
            if verbose_file: print('\t', recd, get_record_by_id(recd), file=global_log)
        if verbose_file: print(']', file=global_log)


def get_record_by_id(record_id):
    return [rec for rec in observed_data if rec['id'] == record_id][0]


def pretty_print_result_clusters(record_to_cluster):
    # examine results
    reverse = reverse_cluster_to_record(record_to_cluster)
    for cluster in reverse:
        # if verbose: print(cluster)
        if verbose_console: print(cluster, '[')
        if verbose_file: print(cluster, '[', file=global_log)
        for fact in reverse[cluster]:
            if verbose_console: print('\t', get_record_by_id(fact))
            if verbose_file: print('\t', get_record_by_id(fact), file=global_log)
        if verbose_console: print(']')
        if verbose_file: print(']', file=global_log)
        if verbose_console: print()
        if verbose_file: print(file=global_log)


def construct_result_clusters(record_to_cluster):
    # examine results
    reverse = reverse_cluster_to_record(record_to_cluster)
    parent_group = []
    for cluster in reverse:
        # if verbose: print(cluster)
        parent_group.append([])
        most_recent_group = len(parent_group) - 1
        for fact in reverse[cluster]:
            parent_group[most_recent_group].append(get_record_by_id(fact))
    return parent_group


def count_false_positives_for_fact(result_clusters, fact_id, result_record_to_cluster):
    # find elements in the same cluster as the given fact
    records_of_fact_cluster = get_records_of_cluster(result_record_to_cluster[fact_id], result_record_to_cluster)
    records_of_fact_cluster.remove(fact_id)
    neigboring_elements_of_fact_in_cluster = records_of_fact_cluster

    # find elements in the same cluster with the given as found in the correct groups
    correct_group = []
    for group in correct_groups_of_facts:
        for fact in group:
            if fact["id"] == fact_id:
                correct_group = group
                break
    correct_neigboring_elements_of_fact_in_cluster = []
    for fact in correct_group:
        if fact["id"] == fact_id: continue
        correct_neigboring_elements_of_fact_in_cluster.append(fact["id"])

    # compare predicted neighbors vs actual neighbors of the fact
    false_positives = sum(
        el not in correct_neigboring_elements_of_fact_in_cluster for el in neigboring_elements_of_fact_in_cluster)
    true_positives = sum(
        el in correct_neigboring_elements_of_fact_in_cluster for el in neigboring_elements_of_fact_in_cluster)
    false_negatives = sum(
        el not in neigboring_elements_of_fact_in_cluster for el in correct_neigboring_elements_of_fact_in_cluster)

    false_facts = [f["id"] for f in observed_data]
    for f in correct_neigboring_elements_of_fact_in_cluster:
        false_facts.remove(f)

    true_negatives = sum(el not in neigboring_elements_of_fact_in_cluster for el in false_facts)

    result = {
        "false_positives": false_positives,
        "true_positives": true_positives,
        "false_negatives": false_negatives,
        "true_negatives": true_negatives,
        "predicted_neighbors": neigboring_elements_of_fact_in_cluster,
        "correct_neighbors": correct_neigboring_elements_of_fact_in_cluster,
        "total_facts": len(observed_data)
    }
    return result


def evaluate_result_clusters(result_clusters, result_record_to_cluster):
    # for every fact in the result clusters count:
    # a) facts falsely in the same group (false positives) - ta stoixeia pou einai geitones enw de tha eprepe
    # b) facts correctly in the same group (true positives) - ta stoixeia pou einai geitones enw tha eprepe
    # c) facts falsely not in the same group (false negative) - ta stoixeia pou den einai geitones enw tha eprepe
    # d) facts correctly not in the same group (true negative) - ta stoixeia pou den einai geitones enw den tha eprepe
    result = dict()
    for result_cluster in result_clusters:
        for fact in result_cluster:
            fact_id = fact["id"]
            result[fact_id] = count_false_positives_for_fact(result_clusters, fact_id, result_record_to_cluster)
    return result


def sum_evaluation_for_all_facts(evaluation):
    total_FP = 0
    total_TP = 0
    total_FN = 0
    total_TN = 0
    for fact_id in evaluation:
        total_FP += evaluation[fact_id]["false_positives"]
        total_TP += evaluation[fact_id]["true_positives"]
        total_FN += evaluation[fact_id]["false_negatives"]
        total_TN += evaluation[fact_id]["true_negatives"]
    result = {
        "total_FP": total_FP,
        "total_TP": total_TP,
        "total_FN": total_FN,
        "total_TN": total_TN,
    }
    return result


def run_experiment():
    experiment_configurations: []

    if experiment_with_combinations:
        parameter_combinations = itertools.product(
            [cosine_similarity],
            [relationship_similarity_v1],
            [0.5],
            [0.64, 0.645],
            [0.85, 0.95],
        )
        experiment_configurations = [
            {
                "name_sim_func": c[0],
                "location_sim_func": c[0],
                "education_degree_sim_func": c[0],
                "education_university_sim_func": c[0],
                "education_year_sim_func": c[0],
                "working_experience_title_sim_func": c[0],
                "working_experience_company_sim_func": c[0],
                "working_experience_years_sim_func": c[0],
                "relationship_R_sim_func": c[1],
                "relationship_similarity_threshold": c[2],
                "algorithm_threshold": c[3],
                "constant_a": c[4]
            } for c in parameter_combinations
        ]
    else:
        experiment_configurations = [
            {
                "name_sim_func": cosine_similarity,
                "location_sim_func": cosine_similarity,
                "education_degree_sim_func": cosine_similarity,
                "education_university_sim_func": cosine_similarity,
                "education_year_sim_func": cosine_similarity,
                "working_experience_title_sim_func": cosine_similarity,
                "working_experience_company_sim_func": cosine_similarity,
                "working_experience_years_sim_func": cosine_similarity,
                "relationship_R_sim_func": relationship_similarity_v1,
                "relationship_similarity_threshold": 0.6,
                "algorithm_threshold": 0.6,
                "constant_a": 0.7
            },
            {
                "name_sim_func": cosine_similarity,
                "location_sim_func": cosine_similarity,
                "education_degree_sim_func": cosine_similarity,
                "education_university_sim_func": cosine_similarity,
                "education_year_sim_func": cosine_similarity,
                "working_experience_title_sim_func": cosine_similarity,
                "working_experience_company_sim_func": cosine_similarity,
                "working_experience_years_sim_func": cosine_similarity,
                "relationship_R_sim_func": relationship_similarity_v1,
                "relationship_similarity_threshold": 0.7,
                "algorithm_threshold": 0.7,
                "constant_a": 0.6
            },
            {
                "name_sim_func": cosine_similarity,
                "location_sim_func": cosine_similarity,
                "education_degree_sim_func": cosine_similarity,
                "education_university_sim_func": cosine_similarity,
                "education_year_sim_func": cosine_similarity,
                "working_experience_title_sim_func": cosine_similarity,
                "working_experience_company_sim_func": cosine_similarity,
                "working_experience_years_sim_func": cosine_similarity,
                "relationship_R_sim_func": relationship_similarity_v1,
                "relationship_similarity_threshold": 0.5,
                "algorithm_threshold": 0.8,
                "constant_a": 0.4
            }
        ]

    for config in experiment_configurations:
        # print('Config:', json.dumps(config, sort_keys=True, indent=4), file=global_log)
        # print('Config:', json.dumps(config, sort_keys=True, indent=4))
        print('Config:', "name_sim_func=", config["name_sim_func"].__name__, "relationship_R_sim_func=",
              config["relationship_R_sim_func"].__name__, "relationship_similarity_threshold=",
              config["relationship_similarity_threshold"], "algorithm_threshold=",
              config["algorithm_threshold"], "constant_a=",
              config["constant_a"])
        global program_parameters
        program_parameters = config
        # reinit_global_vars()
        ingest_observed_data(observed_facts_file_path)
        relationship_R = init_relationship_R()
        record_to_cluster = init_record_to_cluster()
        result_record_to_cluster = collective_clustering(relationship_R, record_to_cluster)
        pretty_print_result_clusters(result_record_to_cluster)
        evaluation = evaluate_result_clusters(construct_result_clusters(record_to_cluster), result_record_to_cluster)
        summed_evaluation = sum_evaluation_for_all_facts(evaluation)
        # print('Evaluation:', json.dumps(summed_evaluation, sort_keys=True, indent=4), file=global_log)
        print('Evaluation:', json.dumps(summed_evaluation, sort_keys=True, indent=4))
        with open("log.txt", "w") as text_file:
            text_file.write(global_log.getvalue())
    print('End.')


def main():
    print('Started...')
    if experiment:
        global verbose_console
        verbose_console = False
        run_experiment()
        return

        # step 1: ingest observed data
    # observed_data = ingest_observed_data(observed_facts_file_path)
    ingest_observed_data(observed_facts_file_path)
    if verbose_file: print('Observed facts:', file=global_log)
    print_observed_data()
    # if verbose: print(observed_data, file=global_log)

    # step 2: build initial relationship R: groups similar records together as a preprocessing step
    relationship_R = init_relationship_R()
    pretty_print_R(relationship_R)

    # step 3: initialize the record to cluster relation: in which cluster does record X belong?
    record_to_cluster = init_record_to_cluster()

    # step 4: run the Collective Agglomerative Clustering algorithm to group the most similar records
    # together in the same clusters
    result_record_to_cluster = collective_clustering(relationship_R, record_to_cluster)
    pretty_print_result_clusters(result_record_to_cluster)

    # step 5: run evaluation metrics on the result
    evaluation = evaluate_result_clusters(construct_result_clusters(record_to_cluster), result_record_to_cluster)
    if verbose_file: print('Evaluation:', file=global_log)
    if verbose_file: print(json.dumps(evaluation, sort_keys=True, indent=4), file=global_log)
    # if verbose: print(evaluation, file=global_log)
    summed_evaluation = sum_evaluation_for_all_facts(evaluation)
    if verbose_file: print('Summed-up Evaluation:', file=global_log)

    # step 6: write global log to file
    # if verbose: print('GLOBAL LOG')
    # if verbose: print(global_log.getvalue())
    with open("log.txt", "w") as text_file:
        text_file.write(global_log.getvalue())

    print('Finished.')


main()
