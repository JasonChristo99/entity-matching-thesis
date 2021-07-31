import pandas as pd
import itertools
import json
import data_ingest
import writer
from EntityMatching.config import set_config
from util import *
from evaluation import *
from custom_similarity_funcs import *
from general_str_sim import *


# import jellyfish
# from strsimpy.qgram import QGram
# from strsimpy.overlap_coefficient import OverlapCoefficient
# from strsimpy.metric_lcs import MetricLCS
# from strsimpy.jaccard import Jaccard


# string_similarity_functions = [cosine_similarity, jaro_winkler_similarity, sorensen_dice_similarity]

# relationship_R_initialization_functions = [relationship_similarity_v1, relationship_similarity_v2]


def init_relationship_R():
    if global_vars.verbose_file: print('Initializing relationship R...', file=global_vars.log)

    relationship_R = {}
    for record1 in global_vars.observed_data:
        for record2 in global_vars.observed_data:
            if record1['id'] == record2['id']:
                continue
            if record1['id'] not in relationship_R:
                relationship_R[record1['id']] = []
            # calculate similarity of records based on a dedicated sim. measure, then decide if they are related
            rel_sim = relationship_similarity(record1, record2)
            if global_vars.verbose_file: print('relationship sim.', record1, 'vs', record2, '=', rel_sim,
                                               file=global_vars.log)
            if rel_sim >= program_parameters["relationship_similarity_threshold"]:
                relationship_R[record1['id']].append(record2['id'])

    return relationship_R


def init_record_to_cluster():
    record_to_cluster = {}
    for i in range(len(global_vars.observed_data)):
        record_to_cluster['f' + str(i + 1)] = 'c' + str(i + 1)

    return record_to_cluster


def merge_clusters(cluster1, cluster2, record_to_cluster):
    records_of_cluster1 = get_records_of_cluster(cluster1, record_to_cluster)
    records_of_cluster2 = get_records_of_cluster(cluster2, record_to_cluster)

    new_cluster = cluster1 + '_' + cluster2

    for i in range(len(records_of_cluster1)):
        record_to_cluster[records_of_cluster1[i]] = new_cluster

    for i in range(len(records_of_cluster2)):
        record_to_cluster[records_of_cluster2[i]] = new_cluster

    if global_vars.verbose_file: print('Result: records of', new_cluster, '=',
                                       get_records_of_cluster(new_cluster, record_to_cluster),
                                       file=global_vars.log)

    return record_to_cluster


def collective_clustering(relationship_R, record_to_cluster):
    iteration = 1

    while True:
        if global_vars.verbose_console: print('Iteration', iteration, '...')
        if global_vars.verbose_file: print('Iteration', iteration, '...', file=global_vars.log)
        num_comparisons = 0
        clusters = list(set(record_to_cluster.values()))
        max_cluster_similarity = {'max_sim_value': 0, 'cluster1': '', 'cluster2': ''}

        for i in range(len(clusters)):
            for j in range(i + 1, len(clusters)):
                if clusters[i] == clusters[j]: continue
                # if verbose: print_cluster(clusters[i], record_to_cluster)
                # if verbose: print_cluster(clusters[j], record_to_cluster)
                if global_vars.verbose_file: print('Comparing clusters:', clusters[i], 'vs', clusters[j], '...',
                                                   file=global_vars.log)
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

        if global_vars.verbose_file: print('Maximum Similarity was between clusters',
                                           max_cluster_similarity['cluster1'], 'and',
                                           max_cluster_similarity['cluster2'], 'with value',
                                           max_cluster_similarity['max_sim_value'],
                                           file=global_vars.log)

        if max_cluster_similarity['max_sim_value'] < program_parameters["algorithm_threshold"]:
            if global_vars.verbose_file: print('Threshold', program_parameters["algorithm_threshold"], 'was reached by',
                                               max_cluster_similarity['max_sim_value'],
                                               '. Terminating...', file=global_vars.log)
            if global_vars.verbose_file: print(reverse_cluster_to_record(record_to_cluster),
                                               file=global_vars.log)
            print(iteration, 'total iterations.')
            return record_to_cluster
        else:
            if global_vars.verbose_file: print('Merging clusters:', file=global_vars.log)
            print_cluster(max_cluster_similarity['cluster1'], record_to_cluster)
            print_cluster(max_cluster_similarity['cluster2'], record_to_cluster)
            cluster_similarity(max_cluster_similarity['cluster1'], max_cluster_similarity['cluster2'],
                               record_to_cluster,
                               relationship_R,
                               # verbose=True
                               )
            if global_vars.verbose_file: print('Merging', max_cluster_similarity['cluster1'], '(',
                                               get_records_of_cluster(max_cluster_similarity['cluster1'],
                                                                      record_to_cluster), ')',
                                               max_cluster_similarity['cluster2'], '(',
                                               get_records_of_cluster(max_cluster_similarity['cluster2'],
                                                                      record_to_cluster), ')',
                                               '...',
                                               file=global_vars.log)
            record_to_cluster = merge_clusters(max_cluster_similarity['cluster1'], max_cluster_similarity['cluster2'],
                                               record_to_cluster)
            # print(reverse_cluster_to_record(record_to_cluster))
            if global_vars.verbose_file: print('Result Clusters after iteration:', file=global_vars.log)
            pretty_print_result_clusters(record_to_cluster)
            iteration += 1


def run_experiment():
    experiment_configurations: []

    if global_vars.experiment_with_combinations:
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
        # print('Config:', json.dumps(config, sort_keys=True, indent=4), file=global_vars.global_log)
        # print('Config:', json.dumps(config, sort_keys=True, indent=4))
        print('Config:', "name_sim_func=", config["name_sim_func"].__name__, "relationship_R_sim_func=",
              config["relationship_R_sim_func"].__name__, "relationship_similarity_threshold=",
              config["relationship_similarity_threshold"], "algorithm_threshold=",
              config["algorithm_threshold"], "constant_a=",
              config["constant_a"])
        # config.program_parameters = config
        set_config(config)
        # reinit_global_vars()
        data_ingest.ingest_observed_data(global_vars.observed_facts_file_path)
        relationship_R = init_relationship_R()
        record_to_cluster = init_record_to_cluster()
        result_record_to_cluster = collective_clustering(relationship_R, record_to_cluster)
        pretty_print_result_clusters(result_record_to_cluster)
        evaluation = evaluate_result_clusters(construct_result_clusters(record_to_cluster), result_record_to_cluster)
        summed_evaluation = sum_evaluation_for_all_facts(evaluation)
        # print('Evaluation:', json.dumps(summed_evaluation, sort_keys=True, indent=4), file=global_vars.global_log)
        print('Evaluation:', json.dumps(summed_evaluation, sort_keys=True, indent=4))
    # writer.write_log()
    print('End.')


def main():
    print('Started...')
    if global_vars.experiment:
        # global verbose_console
        global_vars.verbose_console = False
        run_experiment()
        return

        # step 1: ingest observed data
    # observed_data = ingest_observed_data(global_vars.observed_facts_file_path)
    data_ingest.ingest_observed_data(global_vars.observed_facts_file_path)
    if global_vars.verbose_file: print('Observed facts:', file=global_vars.log)
    print_observed_data()
    # if verbose: print(observed_data, file=global_vars.global_log)

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
    if global_vars.verbose_file: print('Evaluation:', file=global_vars.log)
    if global_vars.verbose_file: print(json.dumps(evaluation, sort_keys=True, indent=4), file=global_vars.log)
    # if verbose: print(evaluation, file=global_vars.global_log)
    summed_evaluation = sum_evaluation_for_all_facts(evaluation)
    if global_vars.verbose_file: print('Summed-up Evaluation:', file=global_vars.log)

    # step 6: write global log to file
    # if verbose: print('GLOBAL LOG')
    # if verbose: print(global_vars.global_log.getvalue())
    writer.write_log()

    print('Finished.')


main()
