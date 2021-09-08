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
global_program_params = {}


def init_relationship_R():
    printer.log('Initializing relationship R...', destinations=[global_vars.LOG])

    relationship_R = {}
    for fact1_id in global_vars.observed_data:
        for fact2_id in global_vars.observed_data:
            if fact1_id == fact2_id:
                continue
            if fact1_id not in relationship_R:
                relationship_R[fact1_id] = []

            fact1 = global_vars.observed_data[fact1_id]
            fact2 = global_vars.observed_data[fact2_id]
            printer.log('\tComparing records', fact1_id, 'VS', fact2_id, '(', fact1, 'VS', fact2, ') ...',
                        destinations=[global_vars.LOG])

            rel_sim: float
            # attempt finding the relationship similarity from cache or else
            if frozenset({fact1_id, fact2_id}) in global_vars.relationship_similarity_cache:
                rel_sim = global_vars.relationship_similarity_cache[frozenset({fact1_id, fact2_id})]
            else:
                # calculate similarity of records based on a dedicated sim. measure, then decide if they are related
                rel_sim = round(custom_similarity_funcs.relationship_similarity(fact1, fact2), 4)
                global_vars.relationship_similarity_cache[frozenset({fact1_id, fact2_id})] = rel_sim

            above_threshold = rel_sim >= global_config.default_program_parameters["relationship_similarity_threshold"]
            if global_vars.verbose_file: printer.log('\tRelationship similarity of', fact1_id, 'VS', fact2_id,
                                                     '=', rel_sim, '(above threshold ?', above_threshold, ')',
                                                     destinations=[global_vars.LOG])
            if above_threshold:
                relationship_R[fact1_id].append(fact2_id)
    global_vars.relationship_similarity_cache = {}
    return relationship_R


def init_record_to_cluster():
    record_to_cluster = {}
    for fact_id in global_vars.observed_data:
        record_to_cluster[fact_id] = fact_id.replace('f', 'c')

    global_vars.record_to_cluster = record_to_cluster
    global_vars.cluster_to_record = util_funcs.reverse_cluster_to_record()


def merge_clusters(cluster1, cluster2):
    records_of_cluster1 = util_funcs.get_records_of_cluster(cluster1)
    records_of_cluster2 = util_funcs.get_records_of_cluster(cluster2)

    new_cluster = cluster1 + '_' + cluster2

    for i in range(len(records_of_cluster1)):
        global_vars.record_to_cluster[records_of_cluster1[i]] = new_cluster

    for i in range(len(records_of_cluster2)):
        global_vars.record_to_cluster[records_of_cluster2[i]] = new_cluster

    global_vars.cluster_to_record = util_funcs.reverse_cluster_to_record()

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
        printer.log('----- Iteration', iteration, '-----',
                    destinations=[global_vars.LOG, global_vars.CONSOLE, global_vars.EXP_CONSOLE],
                    important=True)
        num_comparisons = 0
        clusters = list(set(global_vars.record_to_cluster.values()))
        max_cluster_similarity = {'max_sim_value': 0, 'cluster1': '', 'cluster2': ''}

        for i in range(len(clusters)):
            for j in range(i + 1, len(clusters)):
                if clusters[i] == clusters[j]: continue
                if clusters_contain_facts_from_same_source(clusters[i], clusters[j]): continue
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

        printer.log('Maximum Similarity was between clusters',
                    max_cluster_similarity['cluster1'], 'and',
                    max_cluster_similarity['cluster2'], 'with value',
                    max_cluster_similarity['max_sim_value'],
                    destinations=[global_vars.LOG])

        if max_cluster_similarity['max_sim_value'] < global_config.default_program_parameters["algorithm_threshold"]:
            if global_vars.verbose_file: printer.log('Threshold',
                                                     global_config.default_program_parameters["algorithm_threshold"],
                                                     'was reached by', max_cluster_similarity['max_sim_value'],
                                                     '(clusters', max_cluster_similarity['cluster1'], 'and',
                                                     max_cluster_similarity['cluster2'], ')',
                                                     '. Terminating...',
                                                     destinations=[global_vars.LOG, global_vars.EXP_LOG,
                                                                   global_vars.EXP_CONSOLE], important=True)
            # if global_vars.verbose_file: printer.log([global_vars.LOG], 'Result clusters:', util_funcs.reverse_cluster_to_record())
            printer.log('Total iterations:', iteration, destinations=printer.ALL_OUTPUTS, important=True)
            return
        else:
            printer.log('Merging clusters:', destinations=[global_vars.LOG])
            printer.log(util_funcs.construct_cluster_short(max_cluster_similarity['cluster1']),
                        destinations=[global_vars.LOG])
            printer.log(util_funcs.construct_cluster_short(max_cluster_similarity['cluster2']),
                        destinations=[global_vars.LOG])
            custom_similarity_funcs.cluster_similarity(max_cluster_similarity['cluster1'],
                                                       max_cluster_similarity['cluster2'])
            printer.log('Merging', max_cluster_similarity['cluster1'], '(',
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


def main(program_params=None):
    printer.log('--- START ---',
                destinations=[global_vars.LOG, global_vars.CONSOLE, global_vars.EXP_LOG, global_vars.EXP_CONSOLE],
                important=True)

    if program_params is not None:
        global_config.default_program_parameters = program_params

    # step 1: ingest observed data
    data_reader.ingest_observed_facts(global_config.default_program_parameters["input_file_path"])
    printer.log('Observed facts:', destinations=[global_vars.LOG])
    util_funcs.print_observed_data()

    # step 2: initialize the record to cluster relation: in which cluster does record X belong?
    init_record_to_cluster()
    global_vars.record_similarity_cache = {}
    global_vars.relationship_similarity_cache = {}

    # step 3: build initial relationship R: groups similar records together as a preprocessing step
    global_vars.relationship_R = init_relationship_R()
    printer.log('Relationship R:', destinations=[global_vars.LOG])
    util_funcs.print_pretty_relationship_R()

    # step 4: run the Collective Agglomerative Clustering algorithm to group the most similar records
    # together in the same clusters
    collective_clustering()
    printer.log('Result clusters:', destinations=[global_vars.LOG])
    util_funcs.pretty_print_result_clusters([global_vars.LOG])

    # step 5: run evaluation metrics on the result
    evaluation_res = evaluation.evaluate_result_clusters(util_funcs.construct_result_clusters())
    if global_vars.analytical_evaluation:
        printer.log('Evaluation:', destinations=[global_vars.LOG])
        printer.log(json.dumps(evaluation_res, sort_keys=False, indent=4), destinations=[global_vars.LOG])
    summed_evaluation = evaluation.sum_evaluation_for_all_facts(evaluation_res)
    printer.log('Summed-up Evaluation:', summed_evaluation,
                destinations=[global_vars.LOG, global_vars.EXP_LOG, global_vars.EXP_CONSOLE], important=True)

    printer.log('--- END ---', destinations=printer.ALL_OUTPUTS, important=True)

