import itertools
import json
import data_reader
import global_vars
import util_funcs
import evaluation
import global_config
import general_str_sim
import custom_similarity_funcs
import printer


# import jellyfish
# from strsimpy.qgram import QGram
# from strsimpy.overlap_coefficient import OverlapCoefficient
# from strsimpy.metric_lcs import MetricLCS
# from strsimpy.jaccard import Jaccard


# string_similarity_functions = [cosine_similarity, jaro_winkler_similarity, sorensen_dice_similarity]

# relationship_R_initialization_functions = [relationship_similarity_v1, relationship_similarity_v2]


def init_relationship_R():
    if global_vars.verbose_file: printer.log([global_vars.LOG], 'Initializing relationship R...')

    relationship_R = {}
    for record1 in global_vars.observed_data:
        for record2 in global_vars.observed_data:
            if record1['id'] == record2['id']:
                continue
            if record1['id'] not in relationship_R:
                relationship_R[record1['id']] = []

            printer.log([global_vars.LOG], '\tComparing records', record1['id'], 'VS', record2['id'], '(', record1,
                        'VS', record2, ') ...')

            # calculate similarity of records based on a dedicated sim. measure, then decide if they are related
            rel_sim = round(custom_similarity_funcs.relationship_similarity(record1, record2), 4)
            above_threshold = rel_sim >= global_config.default_program_parameters["relationship_similarity_threshold"]
            if global_vars.verbose_file: printer.log([global_vars.LOG], '\tRelationship similarity of', record1['id'],
                                                     'VS', record2['id'], '=', rel_sim, '(above threshold ?',
                                                     above_threshold, ')')
            if above_threshold:
                relationship_R[record1['id']].append(record2['id'])

    return relationship_R


def init_record_to_cluster():
    record_to_cluster = {}
    for i in range(len(global_vars.observed_data)):
        record_to_cluster['f' + str(i + 1)] = 'c' + str(i + 1)

    return record_to_cluster


def merge_clusters(cluster1, cluster2):
    records_of_cluster1 = util_funcs.get_records_of_cluster(cluster1)
    records_of_cluster2 = util_funcs.get_records_of_cluster(cluster2)

    new_cluster = cluster1 + '_' + cluster2

    for i in range(len(records_of_cluster1)):
        global_vars.record_to_cluster[records_of_cluster1[i]] = new_cluster

    for i in range(len(records_of_cluster2)):
        global_vars.record_to_cluster[records_of_cluster2[i]] = new_cluster

    if global_vars.verbose_file: printer.log([global_vars.LOG], 'Result: records of', new_cluster, '=',
                                             util_funcs.get_records_of_cluster(new_cluster))


def collective_clustering():
    iteration = 1

    while True:
        printer.log([global_vars.LOG, global_vars.CONSOLE], '----- Iteration', iteration, '-----')
        num_comparisons = 0
        clusters = list(set(global_vars.record_to_cluster.values()))
        max_cluster_similarity = {'max_sim_value': 0, 'cluster1': '', 'cluster2': ''}

        for i in range(len(clusters)):
            for j in range(i + 1, len(clusters)):
                if clusters[i] == clusters[j]: continue
                # if verbose: print_cluster(clusters[i], record_to_cluster)
                # if verbose: print_cluster(clusters[j], record_to_cluster)
                if global_vars.verbose_file: printer.log([global_vars.LOG], 'Comparing clusters:', clusters[i],
                                                         'VS', clusters[j], '(',
                                                         util_funcs.construct_cluster_short(clusters[i]),
                                                         'VS', util_funcs.construct_cluster_short(clusters[j]), ') ...')
                # printer.log([global_vars.LOG], util_funcs.construct_cluster_short(clusters[i]))
                # printer.log([global_vars.LOG], util_funcs.construct_cluster_short(clusters[j]))
                clusters_combined_similarity = custom_similarity_funcs.cluster_similarity(clusters[i], clusters[j])

                if clusters_combined_similarity > max_cluster_similarity['max_sim_value']:
                    max_cluster_similarity['max_sim_value'] = clusters_combined_similarity
                    max_cluster_similarity['cluster1'] = clusters[i]
                    max_cluster_similarity['cluster2'] = clusters[j]

                num_comparisons += 1

        if global_vars.verbose_file: printer.log([global_vars.LOG], 'Maximum Similarity was between clusters',
                                                 max_cluster_similarity['cluster1'], 'and',
                                                 max_cluster_similarity['cluster2'], 'with value',
                                                 max_cluster_similarity['max_sim_value'])

        if max_cluster_similarity['max_sim_value'] < global_config.default_program_parameters["algorithm_threshold"]:
            if global_vars.verbose_file: printer.log([global_vars.LOG], 'Threshold',
                                                     global_config.default_program_parameters["algorithm_threshold"],
                                                     'was reached by',
                                                     max_cluster_similarity['max_sim_value'],
                                                     '. Terminating...')
            if global_vars.verbose_file: printer.log([global_vars.LOG],
                                                     util_funcs.reverse_cluster_to_record())
            printer.log(printer.ALL_OUTPUTS, iteration, 'total iterations.')
            # global_vars.record_to_cluster = record_to_cluster
            return
        else:
            printer.log([global_vars.LOG], 'Merging clusters:')
            printer.log([global_vars.LOG], util_funcs.construct_cluster(max_cluster_similarity['cluster1']))
            printer.log([global_vars.LOG], util_funcs.construct_cluster(max_cluster_similarity['cluster2']))
            custom_similarity_funcs.cluster_similarity(max_cluster_similarity['cluster1'],
                                                       max_cluster_similarity['cluster2'])
            if global_vars.verbose_file:  printer.log([global_vars.LOG], 'Merging', max_cluster_similarity['cluster1'],
                                                      '(',
                                                      util_funcs.get_records_of_cluster(
                                                          max_cluster_similarity['cluster1']
                                                      ), ')',
                                                      max_cluster_similarity['cluster2'], '(',
                                                      util_funcs.get_records_of_cluster(
                                                          max_cluster_similarity['cluster2'],
                                                      ), ')',
                                                      '...')
            merge_clusters(max_cluster_similarity['cluster1'], max_cluster_similarity['cluster2'])
            # print(reverse_cluster_to_record(record_to_cluster))
            printer.log([global_vars.LOG], 'Result Clusters after iteration:')
            printer.log([global_vars.LOG], util_funcs.construct_result_clusters())
            iteration += 1


def run_experiment():
    experiment_configurations: []

    # to run the experiment, either use automatic combination of parameters, or a hardcoded bunch of configurations
    if global_vars.experiment_with_combinations:
        parameter_combinations = itertools.product(
            [general_str_sim.cosine_similarity],
            [custom_similarity_funcs.relationship_similarity_v1],
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
                "name_sim_func": general_str_sim.cosine_similarity,
                "location_sim_func": general_str_sim.cosine_similarity,
                "education_degree_sim_func": general_str_sim.cosine_similarity,
                "education_university_sim_func": general_str_sim.cosine_similarity,
                "education_year_sim_func": general_str_sim.cosine_similarity,
                "working_experience_title_sim_func": general_str_sim.cosine_similarity,
                "working_experience_company_sim_func": general_str_sim.cosine_similarity,
                "working_experience_years_sim_func": general_str_sim.cosine_similarity,
                "relationship_R_sim_func": custom_similarity_funcs.relationship_similarity_v1,
                "relationship_similarity_threshold": 0.6,
                "algorithm_threshold": 0.6,
                "constant_a": 0.7
            },
            {
                "name_sim_func": general_str_sim.cosine_similarity,
                "location_sim_func": general_str_sim.cosine_similarity,
                "education_degree_sim_func": general_str_sim.cosine_similarity,
                "education_university_sim_func": general_str_sim.cosine_similarity,
                "education_year_sim_func": general_str_sim.cosine_similarity,
                "working_experience_title_sim_func": general_str_sim.cosine_similarity,
                "working_experience_company_sim_func": general_str_sim.cosine_similarity,
                "working_experience_years_sim_func": general_str_sim.cosine_similarity,
                "relationship_R_sim_func": custom_similarity_funcs.relationship_similarity_v1,
                "relationship_similarity_threshold": 0.7,
                "algorithm_threshold": 0.7,
                "constant_a": 0.6
            },
            {
                "name_sim_func": general_str_sim.cosine_similarity,
                "location_sim_func": general_str_sim.cosine_similarity,
                "education_degree_sim_func": general_str_sim.cosine_similarity,
                "education_university_sim_func": general_str_sim.cosine_similarity,
                "education_year_sim_func": general_str_sim.cosine_similarity,
                "working_experience_title_sim_func": general_str_sim.cosine_similarity,
                "working_experience_company_sim_func": general_str_sim.cosine_similarity,
                "working_experience_years_sim_func": general_str_sim.cosine_similarity,
                "relationship_R_sim_func": custom_similarity_funcs.relationship_similarity_v1,
                "relationship_similarity_threshold": 0.5,
                "algorithm_threshold": 0.8,
                "constant_a": 0.4
            }
        ]

    for experiment_config in experiment_configurations:
        printer.log([global_vars.EXP_LOG, global_vars.EXP_CONSOLE], 'Config:', "name_sim_func=",
                    experiment_config["name_sim_func"].__name__, "relationship_R_sim_func=",
                    experiment_config["relationship_R_sim_func"].__name__, "relationship_similarity_threshold=",
                    experiment_config["relationship_similarity_threshold"], "algorithm_threshold=",
                    experiment_config["algorithm_threshold"], "constant_a=",
                    experiment_config["constant_a"])

        global_config.set_config(experiment_config)

        global_vars.relationship_R = init_relationship_R()
        global_vars.record_to_cluster = init_record_to_cluster()

        collective_clustering()

        printer.log([global_vars.EXP_LOG, global_vars.EXP_CONSOLE], util_funcs.construct_result_clusters())

        evaluation_res = evaluation.evaluate_result_clusters(util_funcs.construct_result_clusters())
        summed_evaluation = evaluation.sum_evaluation_for_all_facts(evaluation_res)

        printer.log([global_vars.EXP_LOG, global_vars.EXP_CONSOLE], 'Evaluation:',
                    json.dumps(summed_evaluation, sort_keys=True, indent=4))
    printer.log([global_vars.EXP_LOG, global_vars.EXP_CONSOLE], '--- END ---')


def main():
    printer.log([global_vars.LOG, global_vars.CONSOLE, global_vars.EXP_LOG, global_vars.EXP_CONSOLE], '--- START ---')

    # step 1: ingest observed data
    data_reader.ingest_observed_facts(global_vars.observed_facts_file_path)
    if global_vars.verbose_file: printer.log([global_vars.LOG], 'Observed facts:')
    util_funcs.print_observed_data()

    # step 2: initialize the record to cluster relation: in which cluster does record X belong?
    global_vars.record_to_cluster = init_record_to_cluster()

    # step 2.1: if the global configuration suggests that we run an experiment, run it,
    # else continue with a run of the algorithm
    if global_vars.experiment:
        run_experiment()
        return

    # step 3: build initial relationship R: groups similar records together as a preprocessing step
    global_vars.relationship_R = init_relationship_R()
    printer.log([global_vars.LOG], 'Relationship R:')
    util_funcs.print_pretty_relationship_R()

    # step 4: run the Collective Agglomerative Clustering algorithm to group the most similar records
    # together in the same clusters
    collective_clustering()
    printer.log([global_vars.LOG], util_funcs.construct_result_clusters())

    # step 5: run evaluation metrics on the result
    evaluation_res = evaluation.evaluate_result_clusters(util_funcs.construct_result_clusters())
    printer.log([global_vars.LOG], 'Evaluation:')
    printer.log([global_vars.LOG], json.dumps(evaluation_res, sort_keys=True, indent=4))
    summed_evaluation = evaluation.sum_evaluation_for_all_facts(evaluation_res)
    printer.log([global_vars.LOG], 'Summed-up Evaluation:', summed_evaluation)

    printer.log(printer.ALL_OUTPUTS, '--- END ---')


main()
