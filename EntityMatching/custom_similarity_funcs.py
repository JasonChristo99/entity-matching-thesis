import global_vars
import util_funcs
import global_config
import printer


def name_vertical_similarity(record1, record2):
    # 'Name' vertical always has cardinality of 1 tuple
    printer.log([global_vars.LOG], '\t\t\tComparing vertical "Name".')

    name1 = record1['Name'][0]['name'].lower()
    name2 = record2['Name'][0]['name'].lower()

    printer.log([global_vars.LOG], '\t\t\t\tComparing tuples', record1['Name'], 'VS', record2['Name'])

    result = global_config.default_program_parameters["name_sim_func"](name1, name2)
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

    printer.log([global_vars.LOG], '\t\t\t\t\tComparing attribute \'name\':', '"' + name1 + '"', 'VS',
                '"' + name2 + '"', '=', result)
    # printer.log([global_vars.LOG], name1, 'VS', name2, '=', result)

    printer.log([global_vars.LOG], '\t\t\tVertical "Name" similarity=', round(result, 4))

    return result


def location_vertical_similarity(record1, record2):
    # 'Location' vertical always has cardinality of 1 tuple
    printer.log([global_vars.LOG], '\t\t\tComparing vertical "Location".')

    location1 = record1.get('Location')
    location2 = record2.get('Location')

    if location1 is None or location2 is None:
        return 0

    location1 = record1['Location'][0]['location'].lower()
    location2 = record2['Location'][0]['location'].lower()
    printer.log([global_vars.LOG], '\t\t\t\tComparing tuples', record1['Location'], 'VS', record2['Location'])

    # result = jellyfish.jaro_winkler_similarity(location1, location2)
    result = global_config.default_program_parameters["location_sim_func"](location1, location2)

    printer.log([global_vars.LOG], '\t\t\t\t\tComparing attribute \'location\':', '"' + location1 + '"', 'VS',
                '"' + location2 + '"', '=', result)

    # printer.log([global_vars.LOG], location1, 'VS', location2, '=', result)

    printer.log([global_vars.LOG], '\t\t\tVertical "Location" similarity=', round(result, 4))

    return result


def education_degree_similarity(degree1, degree2):
    result = global_config.default_program_parameters["education_degree_sim_func"](degree1, degree2)
    return result


def education_university_similarity(university1, university2):
    result = global_config.default_program_parameters["education_university_sim_func"](university1, university2)
    return result


def education_year_similarity(year1, year2):
    result = global_config.default_program_parameters["education_year_sim_func"](year1, year2)
    return result


def education_tuple_similarity(tuple1, tuple2):
    # input tuple form: {'degree': 'Ph.D.', 'university': 'UC_Berkley', 'year': '2004'}

    degree_sim = round(education_degree_similarity(tuple1.get('degree', ''), tuple2.get('degree', '')), 4)
    university_sim = round(education_university_similarity(tuple1.get('university', ''), tuple2.get('university', '')),
                           4)
    year_sim = round(education_year_similarity(tuple1.get('year', ''), tuple2.get('year', '')), 4)

    printer.log([global_vars.LOG], '\t\t\t\t\tComparing attribute \'degree\':', '"' + tuple1.get('degree', '') + '"',
                'VS', '"' + tuple2.get('degree', '') + '"', '=', degree_sim)

    printer.log([global_vars.LOG], '\t\t\t\t\tComparing attribute \'university\':',
                '"' + tuple1.get('university', '') + '"', 'VS',
                '"' + tuple2.get('university', '') + '"', '=', university_sim)

    printer.log([global_vars.LOG], '\t\t\t\t\tComparing attribute \'year\':', '"' + tuple1.get('year', '') + '"', 'VS',
                '"' + tuple2.get('year', '') + '"', '=', year_sim)

    # printer.log([global_vars.LOG], tuple1.get('degree', ''), 'VS', tuple2.get('degree', ''), '=', degree_sim)
    # printer.log([global_vars.LOG], tuple1.get('university', ''), 'VS', tuple2.get('university', ''), '=', university_sim)
    # printer.log([global_vars.LOG], tuple1.get('year', ''), 'VS', tuple2.get('year', ''), '=', year_sim)
    result = 0.3 * degree_sim + 0.5 * university_sim + 0.2 * year_sim

    printer.log([global_vars.LOG], '\t\t\t\tTuple similarity =',
                0.3, '* (degree similarity) +', 0.5, '* (university similarity) +', 0.2, '* (year similarity) =',
                0.3, '*', degree_sim, '+', 0.5, '*', university_sim, '+', 0.2, '*', year_sim, '=', round(result, 4))

    return result


def education_vertical_similarity(record1, record2):
    # 'Education' vertical may have cardinality of 1 or more tuple
    # we assume that the similarity between two multi-tuple education records
    # is the average similarity of all tuple pairs

    printer.log([global_vars.LOG], '\t\t\tComparing vertical "Education".')
    # printer.log([global_vars.LOG], 'Comparing Education vertical.')

    education1 = record1.get('Education')
    education2 = record2.get('Education')

    if education1 is None or education2 is None:
        return 0

    max_similarity = 0
    similarity_sum = 0
    count_pairs = 0
    for index1, tuple1 in enumerate(education1):
        for index2, tuple2 in enumerate(education2):
            # printer.log([global_vars.LOG], 'Comparing tuples', tuple1, 'VS', tuple2)
            printer.log([global_vars.LOG], '\t\t\t\tComparing tuples', tuple1, 'VS', tuple2)

            tuple_similarity = education_tuple_similarity(tuple1, tuple2)
            # if verbose: print(tuple1, 'VS', tuple2, '=', tuple_similarity, file=global_vars.global_log)
            # if verbose: print('Tuples similarity=', tuple_similarity, file=global_vars.global_log)
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
    printer.log([global_vars.LOG], '\t\t\tVertical "Education" similarity (maximum tuple similarity)=',
                round(max_similarity, 4))

    # printer.log([global_vars.LOG], 'Education similarity (max tuples similarity)=', max_similarity)
    return max_similarity


def working_experience_title_similarity(title1, title2):
    result = global_config.default_program_parameters["working_experience_title_sim_func"](title1, title2)
    return result


def working_experience_company_similarity(company1, company2):
    result = global_config.default_program_parameters["working_experience_company_sim_func"](company1, company2)
    return result


def working_experience_years_similarity(year_range1, year_range2):
    result = global_config.default_program_parameters["working_experience_years_sim_func"](year_range1, year_range2)
    return result


def working_experience_tuple_similarity(tuple1, tuple2):
    # input tuple form: {'title': 'developer', 'company': 'Lucas-Hernandez', 'years': '199-2004'}
    title_sim = round(working_experience_title_similarity(tuple1.get('title', ''), tuple2.get('title', '')), 4)
    company_sim = round(working_experience_company_similarity(tuple1.get('company', ''), tuple2.get('company', '')), 4)
    years_sim = round(working_experience_years_similarity(tuple1.get('years', ''), tuple2.get('years', '')), 4)

    # printer.log([global_vars.LOG], tuple1.get('title', ''), 'VS', tuple2.get('title', ''), '=', title_sim)
    # printer.log([global_vars.LOG], tuple1.get('company', ''), 'VS', tuple2.get('company', ''), '=', company_sim)
    # printer.log([global_vars.LOG], tuple1.get('years', ''), 'VS', tuple2.get('years', ''), '=', years_sim)
    printer.log([global_vars.LOG], '\t\t\t\t\tComparing attribute \'title\':', '"' + tuple1.get('title', '') + '"',
                'VS', '"' + tuple2.get('title', '') + '"', '=', title_sim)

    printer.log([global_vars.LOG], '\t\t\t\t\tComparing attribute \'company\':',
                '"' + tuple1.get('company', '') + '"', 'VS',
                '"' + tuple2.get('company', '') + '"', '=', company_sim)

    printer.log([global_vars.LOG], '\t\t\t\t\tComparing attribute \'years\':', '"' + tuple1.get('years', '') + '"',
                'VS',
                '"' + tuple2.get('years', '') + '"', '=', years_sim)

    result = 0.3 * title_sim + 0.5 * company_sim + 0.2 * years_sim

    # printer.log([global_vars.LOG], 'Tuples similarity=', 0.3, '*', title_sim, '+', 0.5, '*', company_sim, '+', 0.2,
    #             '*', years_sim, '=', result)

    printer.log([global_vars.LOG], '\t\t\t\tTuple similarity =',
                0.3, '* (title similarity) +', 0.5, '* (company similarity) +', 0.2, '* (years similarity) =',
                0.3, '*', title_sim, '+', 0.5, '*', company_sim, '+', 0.2, '*', years_sim, '=', round(result, 4))

    return result


def working_experience_vertical_similarity(record1, record2):
    # 'Working Experience' vertical may have cardinality of 1 or more tuple
    # we assume that the similarity between two multi-tuple education records
    # is the average similarity of all tuple pairs
    # printer.log([global_vars.LOG], 'Comparing Working Experience vertical.')
    printer.log([global_vars.LOG], '\t\t\tComparing vertical "Working Experience".')

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
            printer.log([global_vars.LOG], '\t\t\t\tComparing tuples', tuple1, 'VS', tuple2)

            tuple_similarity = working_experience_tuple_similarity(tuple1, tuple2)
            # if verbose: print(tuple1, 'vs', tuple2, '=', tuple_similarity, file=global_vars.global_log)
            # if verbose: print('Tuples similarity=', tuple_similarity, file=global_vars.global_log)
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

    # printer.log([global_vars.LOG], 'Working Experience similarity (max tuples similarity)=', max_similarity)
    printer.log([global_vars.LOG], '\t\t\tVertical "Working Experience" similarity (maximum tuple similarity)=',
                round(max_similarity, 4))
    # return avg_similarity
    return max_similarity


def relationship_similarity_v1(record1, record2):
    result = name_vertical_similarity(record1, record2) + location_vertical_similarity(record1, record2)
    return result


def relationship_similarity_v2(record1, record2):
    result = name_vertical_similarity(record1, record2)
    return result


def relationship_similarity(record1, record2):
    result = global_config.default_program_parameters["relationship_R_sim_func"](record1, record2)
    return result


def record_similarity(record1_id, record2_id):
    record1 = [rec for rec in global_vars.observed_data if rec["id"] == record1_id][0]
    record2 = [rec for rec in global_vars.observed_data if rec["id"] == record2_id][0]

    # if len(record1.keys()) != len(record2.keys()):
    common_verticals_count = len(set(record1.keys()).intersection(set(record2.keys())))
    # if common_verticals_count == 5:
    name_sim = round(name_vertical_similarity(record1, record2), 4)
    loc_sim = round(location_vertical_similarity(record1, record2), 4)
    edu_sim = round(education_vertical_similarity(record1, record2), 4)
    work_sim = round(working_experience_vertical_similarity(record1, record2), 4)

    result = 0.1 * loc_sim + 0.3 * name_sim + 0.3 * edu_sim + 0.3 * work_sim

    printer.log([global_vars.LOG], '\t\tRecord similarity of', record1_id, 'VS', record2_id, '=',
                0.3, '* (name similarity) +', 0.1, '* (location similarity) +', 0.3, '* (education similarity) +', 0.3,
                '* working experience similarity) =',
                0.3, '*', name_sim, '+', 0.1, '*', loc_sim, '+', 0.3, '*', edu_sim, '+', 0.3, '*', work_sim, '=',
                round(result, 4))

    return result


def cluster_attribute_similarity(cluster1, cluster2):
    # sim(c1,c2) = ½ [simattributes(c1,c2) + simneighbours(c1,c2)] - with Complete Link

    printer.log([global_vars.LOG], '\tBy Attribute similarity:')
    max_sim = 0
    records_cluster1 = util_funcs.get_records_of_cluster(cluster1)
    records_cluster2 = util_funcs.get_records_of_cluster(cluster2)
    for record1_id in records_cluster1:
        for record2_id in records_cluster2:
            if record1_id == record2_id:
                continue

            record1 = [rec for rec in global_vars.observed_data if rec["id"] == record1_id][0]
            record2 = [rec for rec in global_vars.observed_data if rec["id"] == record2_id][0]

            printer.log([global_vars.LOG], '\t\tComparing records', record1_id, 'VS', record2_id, '(', record1, 'VS',
                        record2, ') ...')
            record_sim = record_similarity(record1_id, record2_id)
            if record_sim > max_sim:
                max_sim = record_sim

    return max_sim


def cluster_neighborhood_similarity(cluster1, cluster2, verbose=False):
    # sim(c1,c2) = ½ [simattributes(c1,c2) + simneighbours(c1,c2)] - with Complete Link
    printer.log([global_vars.LOG], '\tBy Neighborhood similarity:')
    # printer.log([global_vars.LOG], 'Calculating clusters', cluster1, 'VS', cluster2, 'Neighborhood similarity')
    N_A = set()
    records_of_cluster1 = util_funcs.get_records_of_cluster(cluster1)
    printer.log([global_vars.LOG], '\t\tCluster', cluster1, 'consists of records:', records_of_cluster1)
    for record in records_of_cluster1:
        rel_records_str = [(r + ' (in ' + global_vars.record_to_cluster[r] + ')') for r in
                           global_vars.relationship_R[record]]
        printer.log([global_vars.LOG], '\t\t\tRecords related to', record, ':', rel_records_str)
        for related_record in global_vars.relationship_R[record]:
            N_A.add(global_vars.record_to_cluster[related_record])
    printer.log([global_vars.LOG], '\t\t\tTherefore Neighborhood of', cluster1, 'consists of clusters:', sorted(N_A))

    N_B = set()
    records_of_cluster2 = util_funcs.get_records_of_cluster(cluster2)
    printer.log([global_vars.LOG], '\t\tCluster', cluster2, 'consists of records:', records_of_cluster2)
    for record in records_of_cluster2:
        rel_records_str = [(r + ' (in ' + global_vars.record_to_cluster[r] + ')') for r in
                           global_vars.relationship_R[record]]
        printer.log([global_vars.LOG], '\t\t\tRecords related to', record, ':', rel_records_str)
        for related_record in global_vars.relationship_R[record]:
            N_B.add(global_vars.record_to_cluster[related_record])
    printer.log([global_vars.LOG], '\t\t\tTherefore Neighborhood of', cluster2, 'consists of clusters:', sorted(N_B))

    intersection = N_A.intersection(N_B)
    union = N_A.union(N_B)

    if len(union) == 0:
        return 0

    result = len(intersection) / len(union)
    # if verbose:
    # printer.log([global_vars.LOG], '\t\tNeighborhood of', cluster2, ':', N_B)

    printer.log([global_vars.LOG], '\t\tIntersection of', cluster1, 'neighborhood and', cluster2, 'neighborhood:',
                sorted(intersection))
    printer.log([global_vars.LOG], '\t\tUnion of', cluster1, 'neighborhood and', cluster2, 'neighborhood:', sorted(union))
    printer.log([global_vars.LOG],
                '\tNeighborhood similarity of clusters', cluster1, 'VS', cluster2,
                '= |Intersection of neighborhoods| / |Union of neighborhoods| =',
                len(intersection), '/', len(union), '=', round(result, 4))
    return result


def cluster_similarity(cluster1, cluster2):
    attribute_similarity = round(cluster_attribute_similarity(cluster1, cluster2), 5)

    printer.log([global_vars.LOG], '\tAttribute similarity of clusters',
                cluster1, 'VS', cluster2,
                '(maximum record similarity)',
                '=', attribute_similarity)

    neighborhood_similarity = round(
        cluster_neighborhood_similarity(cluster1, cluster2), 5)

    clusters_combined_similarity = round(
        global_config.default_program_parameters["constant_a"] * attribute_similarity + (
                1 - global_config.default_program_parameters["constant_a"]) * neighborhood_similarity, 4)
    # if verbose:
    printer.log([global_vars.LOG], 'Combined cluster similarity=',
                global_config.default_program_parameters["constant_a"],
                '* (cluster similarity by attribute) +',
                round((1 - global_config.default_program_parameters["constant_a"]), 2),
                '* (cluster similarity by neighborhood) =', global_config.default_program_parameters["constant_a"], '*',
                attribute_similarity, '+',
                round((1 - global_config.default_program_parameters["constant_a"]), 2), '*',
                neighborhood_similarity, '=', clusters_combined_similarity)
    # if verbose:
    # if verbose: print('Comparing:', cluster1, cluster2, ': clusters_combined_similarity=', constant_a, '*',
    #       attribute_similarity, '+', (1 - constant_a), '*',
    #       neighborhood_similarity, '=', clusters_combined_similarity, file=global_vars.global_log)
    return clusters_combined_similarity
