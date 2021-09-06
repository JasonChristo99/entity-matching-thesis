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
    printer.log('Initializing relationship R...', destinations=[global_vars.LOG])

    relationship_R = {}
    for record1 in global_vars.observed_data:
        for record2 in global_vars.observed_data:
            if record1['id'] == record2['id']:
                continue
            if record1['id'] not in relationship_R:
                relationship_R[record1['id']] = []

            printer.log('\tComparing records', record1['id'], 'VS', record2['id'], '(', record1, 'VS', record2, ') ...',
                        destinations=[global_vars.LOG])

            rel_sim: float
            # attempt finding the relationship similarity from cache or else
            if frozenset({record1['id'], record2['id']}) in global_vars.relationship_similarity_cache:
                rel_sim = global_vars.relationship_similarity_cache[frozenset({record1['id'], record2['id']})]
            else:
                # calculate similarity of records based on a dedicated sim. measure, then decide if they are related
                rel_sim = round(custom_similarity_funcs.relationship_similarity(record1, record2), 4)
                global_vars.relationship_similarity_cache[frozenset({record1['id'], record2['id']})] = rel_sim

            above_threshold = rel_sim >= global_config.default_program_parameters["relationship_similarity_threshold"]
            if global_vars.verbose_file: printer.log('\tRelationship similarity of', record1['id'], 'VS', record2['id'],
                                                     '=', rel_sim, '(above threshold ?', above_threshold, ')',
                                                     destinations=[global_vars.LOG])
            if above_threshold:
                relationship_R[record1['id']].append(record2['id'])
    global_vars.relationship_similarity_cache = {}
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

    if global_vars.verbose_file: printer.log('Result: records of', new_cluster, '=',
                                             util_funcs.get_records_of_cluster(new_cluster),
                                             destinations=[global_vars.LOG])


def clusters_contain_facts_from_same_source(cluster1, cluster2):
    r1 = util_funcs.get_records_of_cluster(cluster1)
    r2 = util_funcs.get_records_of_cluster(cluster2)
    sources_of_r1 = [util_funcs.get_record_by_id(r)['source'] for r in r1]
    sources_of_r2 = [util_funcs.get_record_by_id(r)['source'] for r in r2]
    common_sources = len(set(sources_of_r1).intersection(sources_of_r2))
    return common_sources > 0


def collective_clustering():
    iteration = 1

    while True:
        printer.log('----- Iteration', iteration, '-----', destinations=[global_vars.LOG, global_vars.CONSOLE])
        num_comparisons = 0
        clusters = list(set(global_vars.record_to_cluster.values()))
        max_cluster_similarity = {'max_sim_value': 0, 'cluster1': '', 'cluster2': ''}

        for i in range(len(clusters)):
            for j in range(i + 1, len(clusters)):
                if clusters[i] == clusters[j]: continue
                if (clusters_contain_facts_from_same_source(clusters[i], clusters[j])): continue
                # if verbose: print_cluster(clusters[i], record_to_cluster)
                # if verbose: print_cluster(clusters[j], record_to_cluster)
                if global_vars.verbose_file: printer.log('Comparing clusters:', clusters[i], 'VS', clusters[j], '(',
                                                         util_funcs.construct_cluster_short(clusters[i]), 'VS',
                                                         util_funcs.construct_cluster_short(clusters[j]), ') ...',
                                                         destinations=[global_vars.LOG])
                # printer.log([global_vars.LOG], util_funcs.construct_cluster_short(clusters[i]))
                # printer.log([global_vars.LOG], util_funcs.construct_cluster_short(clusters[j]))
                clusters_combined_similarity = custom_similarity_funcs.cluster_similarity(clusters[i], clusters[j])

                if clusters_combined_similarity > max_cluster_similarity['max_sim_value']:
                    max_cluster_similarity['max_sim_value'] = clusters_combined_similarity
                    max_cluster_similarity['cluster1'] = clusters[i]
                    max_cluster_similarity['cluster2'] = clusters[j]

                num_comparisons += 1

        if global_vars.verbose_file: printer.log('Maximum Similarity was between clusters',
                                                 max_cluster_similarity['cluster1'], 'and',
                                                 max_cluster_similarity['cluster2'], 'with value',
                                                 max_cluster_similarity['max_sim_value'],
                                                 destinations=[global_vars.LOG])

        if max_cluster_similarity['max_sim_value'] < global_config.default_program_parameters["algorithm_threshold"]:
            if global_vars.verbose_file: printer.log('Threshold',
                                                     global_config.default_program_parameters["algorithm_threshold"],
                                                     'was reached by', max_cluster_similarity['max_sim_value'],
                                                     '. Terminating...', destinations=[global_vars.LOG])
            # if global_vars.verbose_file: printer.log([global_vars.LOG], 'Result clusters:', util_funcs.reverse_cluster_to_record())
            printer.log('Total iterations:', iteration, destinations=printer.ALL_OUTPUTS)
            return
        else:
            printer.log('Merging clusters:', destinations=[global_vars.LOG])
            printer.log(util_funcs.construct_cluster_short(max_cluster_similarity['cluster1']),
                        destinations=[global_vars.LOG])
            printer.log(util_funcs.construct_cluster_short(max_cluster_similarity['cluster2']),
                        destinations=[global_vars.LOG])
            custom_similarity_funcs.cluster_similarity(max_cluster_similarity['cluster1'],
                                                       max_cluster_similarity['cluster2'])
            if global_vars.verbose_file:  printer.log('Merging', max_cluster_similarity['cluster1'], '(',
                                                      util_funcs.get_records_of_cluster(
                                                          max_cluster_similarity['cluster1']
                                                      ), ')', max_cluster_similarity['cluster2'], '(',
                                                      util_funcs.get_records_of_cluster(
                                                          max_cluster_similarity['cluster2'],
                                                      ), ')', '...', destinations=[global_vars.LOG])
            merge_clusters(max_cluster_similarity['cluster1'], max_cluster_similarity['cluster2'])
            # print(reverse_cluster_to_record(record_to_cluster))
            # printer.log([global_vars.LOG], 'Result Clusters after iteration:')
            # util_funcs.pretty_print_result_clusters()
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
                # functions
                "name_sim_func": c[0],
                "location_sim_func": c[0],
                "education_degree_sim_func": c[0],
                "education_university_sim_func": c[0],
                "education_year_sim_func": c[0],
                "working_experience_title_sim_func": c[0],
                "working_experience_company_sim_func": c[0],
                "working_experience_years_sim_func": c[0],
                "relationship_R_sim_func": c[1],
                # constants
                "relationship_similarity_threshold": c[2],
                "algorithm_threshold": c[3],
                "constant_a": c[4],
                "record_similarity_name_weight": 0.1,
                "record_similarity_location_weight": 0.3,
                "record_similarity_education_weight": 0.3,
                "record_similarity_working_exp_weight": 0.3,
                "education_tuple_degree_weight": 0.3,
                "education_tuple_university_weight": 0.5,
                "education_tuple_year_weight": 0.2,
                "working_exp_tuple_title_weight": 0.3,
                "working_exp_tuple_company_weight": 0.5,
                "working_exp_tuple_years_weight": 0.2,
            } for c in parameter_combinations
        ]
    else:
        experiment_configurations = [
            # {
            #     # functions
            #     "name_sim_func": general_str_sim.cosine_similarity,
            #     "location_sim_func": general_str_sim.cosine_similarity,
            #     "education_degree_sim_func": general_str_sim.cosine_similarity,
            #     "education_university_sim_func": general_str_sim.cosine_similarity,
            #     "education_year_sim_func": general_str_sim.cosine_similarity,
            #     "working_experience_title_sim_func": general_str_sim.cosine_similarity,
            #     "working_experience_company_sim_func": general_str_sim.cosine_similarity,
            #     "working_experience_years_sim_func": general_str_sim.cosine_similarity,
            #     "relationship_R_sim_func": custom_similarity_funcs.relationship_similarity_v1,
            #     # constants
            #     "relationship_similarity_threshold": 0.6,
            #     "algorithm_threshold": 0.43,
            #     "constant_a": 0.7,
            #     "record_similarity_name_weight": 0.1,
            #     "record_similarity_location_weight": 0.3,
            #     "record_similarity_education_weight": 0.3,
            #     "record_similarity_working_exp_weight": 0.3,
            #     "education_tuple_degree_weight": 0.3,
            #     "education_tuple_university_weight": 0.5,
            #     "education_tuple_year_weight": 0.2,
            #     "working_exp_tuple_title_weight": 0.3,
            #     "working_exp_tuple_company_weight": 0.5,
            #     "working_exp_tuple_years_weight": 0.2,
            # },
            # {
            #     # functions
            #     "name_sim_func": general_str_sim.cosine_similarity,
            #     "location_sim_func": general_str_sim.cosine_similarity,
            #     "education_degree_sim_func": general_str_sim.cosine_similarity,
            #     "education_university_sim_func": general_str_sim.cosine_similarity,
            #     "education_year_sim_func": general_str_sim.cosine_similarity,
            #     "working_experience_title_sim_func": general_str_sim.cosine_similarity,
            #     "working_experience_company_sim_func": general_str_sim.cosine_similarity,
            #     "working_experience_years_sim_func": general_str_sim.cosine_similarity,
            #     "relationship_R_sim_func": custom_similarity_funcs.relationship_similarity_v2,
            #     # constants
            #     "relationship_similarity_threshold": 0.6,
            #     "algorithm_threshold": 0.58,
            #     "constant_a": 0.5,
            #     "record_similarity_name_weight": 0.1,
            #     "record_similarity_location_weight": 0.3,
            #     "record_similarity_education_weight": 0.3,
            #     "record_similarity_working_exp_weight": 0.3,
            #     "education_tuple_degree_weight": 0.3,
            #     "education_tuple_university_weight": 0.5,
            #     "education_tuple_year_weight": 0.2,
            #     "working_exp_tuple_title_weight": 0.3,
            #     "working_exp_tuple_company_weight": 0.5,
            #     "working_exp_tuple_years_weight": 0.2,
            # },
            {
                # functions
                "name_sim_func": general_str_sim.cosine_similarity,
                "location_sim_func": general_str_sim.cosine_similarity,
                "education_degree_sim_func": general_str_sim.cosine_similarity,
                "education_university_sim_func": general_str_sim.cosine_similarity,
                "education_year_sim_func": general_str_sim.cosine_similarity,
                "working_experience_title_sim_func": general_str_sim.cosine_similarity,
                "working_experience_company_sim_func": general_str_sim.cosine_similarity,
                "working_experience_years_sim_func": general_str_sim.cosine_similarity,
                "relationship_R_sim_func": custom_similarity_funcs.relationship_similarity_v1,
                # constants
                "relationship_similarity_threshold": 0.6,
                "algorithm_threshold": 0.35,
                "constant_a": 0.6,
                "record_similarity_name_weight": 0.4,
                "record_similarity_location_weight": 0.1,
                "record_similarity_education_weight": 0.2,
                "record_similarity_working_exp_weight": 0.3,
                "education_tuple_degree_weight": 0.3,
                "education_tuple_university_weight": 0.5,
                "education_tuple_year_weight": 0.2,
                "working_exp_tuple_title_weight": 0.4,
                "working_exp_tuple_company_weight": 0.5,
                "working_exp_tuple_years_weight": 0.1,
            }
        ]

    for experiment_config in experiment_configurations:
        printer.log('Config:', "name_sim_func=", experiment_config["name_sim_func"].__name__,
                    "relationship_R_sim_func=", experiment_config["relationship_R_sim_func"].__name__,
                    "relationship_similarity_threshold=", experiment_config["relationship_similarity_threshold"],
                    "algorithm_threshold=", experiment_config["algorithm_threshold"], "constant_a=",
                    experiment_config["constant_a"], destinations=[global_vars.EXP_LOG, global_vars.EXP_CONSOLE])

        global_config.set_config(experiment_config)

        global_vars.relationship_R = init_relationship_R()
        global_vars.record_to_cluster = init_record_to_cluster()

        collective_clustering()

        global_vars.record_similarity_cache = {}
        global_vars.relationship_similarity_cache = {}

        util_funcs.pretty_print_result_clusters(destinations=[global_vars.EXP_LOG, global_vars.EXP_CONSOLE])

        evaluation_res = evaluation.evaluate_result_clusters(util_funcs.construct_result_clusters())
        summed_evaluation = evaluation.sum_evaluation_for_all_facts(evaluation_res)

        printer.log('Evaluation:', json.dumps(summed_evaluation, sort_keys=True, indent=4),
                    destinations=[global_vars.EXP_LOG, global_vars.EXP_CONSOLE])
    printer.log('--- END ---', destinations=[global_vars.EXP_LOG, global_vars.EXP_CONSOLE])


def main():
    printer.log('--- START ---',
                destinations=[global_vars.LOG, global_vars.CONSOLE, global_vars.EXP_LOG, global_vars.EXP_CONSOLE])

    # step 1: ingest observed data
    data_reader.ingest_observed_facts(global_vars.observed_facts_file_path)
    if global_vars.verbose_file: printer.log('Observed facts:', destinations=[global_vars.LOG])
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
    printer.log('Relationship R:', destinations=[global_vars.LOG])
    util_funcs.print_pretty_relationship_R()

    # step 4: run the Collective Agglomerative Clustering algorithm to group the most similar records
    # together in the same clusters
    collective_clustering()
    printer.log('Result clusters:', destinations=[global_vars.LOG])
    util_funcs.pretty_print_result_clusters()

    # step 5: run evaluation metrics on the result
    evaluation_res = evaluation.evaluate_result_clusters(util_funcs.construct_result_clusters())
    if global_vars.analytical_evaluation:
        printer.log('Evaluation:', destinations=[global_vars.LOG])
        printer.log(json.dumps(evaluation_res, sort_keys=False, indent=4), destinations=[global_vars.LOG])
    summed_evaluation = evaluation.sum_evaluation_for_all_facts(evaluation_res)
    printer.log('Summed-up Evaluation:', summed_evaluation, destinations=[global_vars.LOG])

    printer.log('--- END ---', destinations=printer.ALL_OUTPUTS)


main()
