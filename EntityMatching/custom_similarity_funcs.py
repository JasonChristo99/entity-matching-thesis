import global_vars
import util_funcs
import global_config
import printer


def name_vertical_similarity(record1, record2):
    # 'Name' vertical always has cardinality of 1 tuple
    printer.log('\t\t\tComparing vertical "Name".', destinations=[global_vars.LOG])

    name1 = record1['Name'][0]['name'].lower()
    name2 = record2['Name'][0]['name'].lower()

    printer.log('\t\t\t\tComparing tuples', record1['Name'], 'VS', record2['Name'], destinations=[global_vars.LOG])

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

    printer.log('\t\t\t\t\tComparing attribute \'name\':', '"' + name1 + '"', 'VS', '"' + name2 + '"', '=', result,
                destinations=[global_vars.LOG])

    printer.log('\t\t\tVertical "Name" similarity=', round(result, 4), destinations=[global_vars.LOG])

    return result


def location_vertical_similarity(record1, record2):
    # 'Location' vertical always has cardinality of 1 tuple
    printer.log('\t\t\tComparing vertical "Location".', destinations=[global_vars.LOG])

    location1 = record1.get('Location')
    location2 = record2.get('Location')

    if location1 is None or location2 is None:
        return 0

    location1 = record1['Location'][0]['location'].lower()
    location2 = record2['Location'][0]['location'].lower()
    printer.log('\t\t\t\tComparing tuples', record1['Location'], 'VS', record2['Location'],
                destinations=[global_vars.LOG])

    # result = jellyfish.jaro_winkler_similarity(location1, location2)
    result = global_config.default_program_parameters["location_sim_func"](location1, location2)

    printer.log('\t\t\t\t\tComparing attribute \'location\':', '"' + location1 + '"', 'VS', '"' + location2 + '"', '=',
                result, destinations=[global_vars.LOG])

    printer.log('\t\t\tVertical "Location" similarity=', round(result, 4), destinations=[global_vars.LOG])

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

    printer.log('\t\t\t\t\tComparing attribute \'degree\':', '"' + tuple1.get('degree', '') + '"', 'VS',
                '"' + tuple2.get('degree', '') + '"', '=', degree_sim, destinations=[global_vars.LOG])

    printer.log('\t\t\t\t\tComparing attribute \'university\':', '"' + tuple1.get('university', '') + '"', 'VS',
                '"' + tuple2.get('university', '') + '"', '=', university_sim, destinations=[global_vars.LOG])

    printer.log('\t\t\t\t\tComparing attribute \'year\':', '"' + tuple1.get('year', '') + '"', 'VS',
                '"' + tuple2.get('year', '') + '"', '=', year_sim, destinations=[global_vars.LOG])

    result = global_config.default_program_parameters["education_tuple_degree_weight"] * degree_sim + \
             global_config.default_program_parameters["education_tuple_university_weight"] * university_sim + \
             global_config.default_program_parameters["education_tuple_year_weight"] * year_sim

    printer.log('\t\t\t\tTuple similarity =', global_config.default_program_parameters["education_tuple_degree_weight"],
                '* (degree similarity) +',
                global_config.default_program_parameters["education_tuple_university_weight"],
                '* (university similarity) +', global_config.default_program_parameters["education_tuple_year_weight"],
                '* (year similarity) =', global_config.default_program_parameters["education_tuple_degree_weight"], '*',
                degree_sim, '+', global_config.default_program_parameters["education_tuple_university_weight"], '*',
                university_sim, '+', global_config.default_program_parameters["education_tuple_year_weight"], '*',
                year_sim, '=', round(result, 4), destinations=[global_vars.LOG])

    return result


def education_vertical_similarity(record1, record2):
    # 'Education' vertical may have cardinality of 1 or more tuple
    # we assume that the similarity between two multi-tuple education records
    # is the maximum similarity of all tuple pairs

    printer.log('\t\t\tComparing vertical "Education".', destinations=[global_vars.LOG])

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
            printer.log('\t\t\t\tComparing tuples', tuple1, 'VS', tuple2, destinations=[global_vars.LOG])

            tuple_similarity = education_tuple_similarity(tuple1, tuple2)
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
    printer.log('\t\t\tVertical "Education" similarity (maximum tuple similarity)=', round(max_similarity, 4),
                destinations=[global_vars.LOG])

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

    printer.log('\t\t\t\t\tComparing attribute \'title\':', '"' + tuple1.get('title', '') + '"', 'VS',
                '"' + tuple2.get('title', '') + '"', '=', title_sim, destinations=[global_vars.LOG])

    printer.log('\t\t\t\t\tComparing attribute \'company\':', '"' + tuple1.get('company', '') + '"', 'VS',
                '"' + tuple2.get('company', '') + '"', '=', company_sim, destinations=[global_vars.LOG])

    printer.log('\t\t\t\t\tComparing attribute \'years\':', '"' + tuple1.get('years', '') + '"', 'VS',
                '"' + tuple2.get('years', '') + '"', '=', years_sim, destinations=[global_vars.LOG])

    result = global_config.default_program_parameters["working_exp_tuple_title_weight"] * title_sim + \
             global_config.default_program_parameters["working_exp_tuple_company_weight"] * company_sim + \
             global_config.default_program_parameters["working_exp_tuple_years_weight"] * years_sim

    printer.log('\t\t\t\tTuple similarity =',
                global_config.default_program_parameters["working_exp_tuple_title_weight"], '* (title similarity) +',
                global_config.default_program_parameters["working_exp_tuple_company_weight"],
                '* (company similarity) +', global_config.default_program_parameters["working_exp_tuple_years_weight"],
                '* (years similarity) =', global_config.default_program_parameters["working_exp_tuple_title_weight"],
                '*', title_sim, '+', global_config.default_program_parameters["working_exp_tuple_company_weight"], '*',
                company_sim, '+', global_config.default_program_parameters["working_exp_tuple_years_weight"], '*',
                years_sim, '=', round(result, 4), destinations=[global_vars.LOG])

    return result


def working_experience_vertical_similarity(record1, record2):
    # 'Working Experience' vertical may have cardinality of 1 or more tuple
    # we assume that the similarity between two multi-tuple education records
    # is the maximum similarity between all tuple pairs
    printer.log('\t\t\tComparing vertical "Working Experience".', destinations=[global_vars.LOG])

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
            printer.log('\t\t\t\tComparing tuples', tuple1, 'VS', tuple2, destinations=[global_vars.LOG])

            tuple_similarity = working_experience_tuple_similarity(tuple1, tuple2)
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

    printer.log('\t\t\tVertical "Working Experience" similarity (maximum tuple similarity)=', round(max_similarity, 4),
                destinations=[global_vars.LOG])
    # return avg_similarity
    return max_similarity


def relationship_similarity_v1(record1, record2):
    result = 0.5 * name_vertical_similarity(record1, record2) + 0.5 * location_vertical_similarity(record1, record2)
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

    # if found in cache, just fetch the similarity and return it
    if frozenset([record1_id, record2_id]) in global_vars.record_similarity_cache:
        result = global_vars.record_similarity_cache[frozenset([record1_id, record2_id])]
        printer.log('\t\tRecord similarity of', record1_id, 'VS', record2_id, '(cached) =', result,
                    destinations=[global_vars.LOG])
        return result

    name_sim = round(name_vertical_similarity(record1, record2), 4)
    loc_sim = round(location_vertical_similarity(record1, record2), 4)
    edu_sim = round(education_vertical_similarity(record1, record2), 4)
    work_sim = round(working_experience_vertical_similarity(record1, record2), 4)

    result = global_config.default_program_parameters["record_similarity_location_weight"] * loc_sim + \
             global_config.default_program_parameters["record_similarity_name_weight"] * name_sim + \
             global_config.default_program_parameters["record_similarity_education_weight"] * edu_sim + \
             global_config.default_program_parameters["record_similarity_working_exp_weight"] * work_sim

    printer.log('\t\tRecord similarity of', record1_id, 'VS', record2_id, '=',
                global_config.default_program_parameters["record_similarity_name_weight"], '* (name similarity) +',
                global_config.default_program_parameters["record_similarity_location_weight"],
                '* (location similarity) +',
                global_config.default_program_parameters["record_similarity_education_weight"],
                '* (education similarity) +',
                global_config.default_program_parameters["record_similarity_working_exp_weight"],
                '* working experience similarity) =',
                global_config.default_program_parameters["record_similarity_name_weight"], '*', name_sim, '+',
                global_config.default_program_parameters["record_similarity_location_weight"], '*', loc_sim, '+',
                global_config.default_program_parameters["record_similarity_education_weight"], '*', edu_sim, '+',
                global_config.default_program_parameters["record_similarity_working_exp_weight"], '*', work_sim, '=',
                round(result, 4), destinations=[global_vars.LOG])

    global_vars.record_similarity_cache[frozenset([record1_id, record2_id])] = round(result, 4)
    return result


def cluster_attribute_similarity(cluster1, cluster2):
    # sim(c1,c2) = ½ [simattributes(c1,c2) + simneighbours(c1,c2)] - with Complete Link

    printer.log('\tBy Attribute similarity:', destinations=[global_vars.LOG])
    max_sim = 0
    records_cluster1 = util_funcs.get_records_of_cluster(cluster1)
    records_cluster2 = util_funcs.get_records_of_cluster(cluster2)
    for record1_id in records_cluster1:
        for record2_id in records_cluster2:
            if record1_id == record2_id:
                continue

            record1 = [rec for rec in global_vars.observed_data if rec["id"] == record1_id][0]
            record2 = [rec for rec in global_vars.observed_data if rec["id"] == record2_id][0]

            printer.log('\t\tComparing records', record1_id, 'VS', record2_id,
                        # '(', record1, 'VS', record2, ') ...',
                        destinations=[global_vars.LOG])
            record_sim = record_similarity(record1_id, record2_id)
            if record_sim > max_sim:
                max_sim = record_sim

    return max_sim


def cluster_neighborhood_similarity(cluster1, cluster2, verbose=False):
    # sim(c1,c2) = ½ [simattributes(c1,c2) + simneighbours(c1,c2)] - with Complete Link
    printer.log('\tBy Neighborhood similarity:', destinations=[global_vars.LOG])

    N_A = set()
    records_of_cluster1 = util_funcs.get_records_of_cluster(cluster1)
    printer.log('\t\tCluster', cluster1, 'consists of records:', records_of_cluster1, destinations=[global_vars.LOG])
    for record in records_of_cluster1:
        rel_records_str = [(r + ' (in ' + global_vars.record_to_cluster[r] + ')') for r in
                           global_vars.relationship_R[record]]
        printer.log('\t\t\tRecords related to', record, ':', rel_records_str, destinations=[global_vars.LOG])
        for related_record in global_vars.relationship_R[record]:
            N_A.add(global_vars.record_to_cluster[related_record])
    printer.log('\t\t\tTherefore Neighborhood of', cluster1, 'consists of clusters:', sorted(N_A),
                destinations=[global_vars.LOG])

    N_B = set()
    records_of_cluster2 = util_funcs.get_records_of_cluster(cluster2)
    printer.log('\t\tCluster', cluster2, 'consists of records:', records_of_cluster2, destinations=[global_vars.LOG])
    for record in records_of_cluster2:
        rel_records_str = [(r + ' (in ' + global_vars.record_to_cluster[r] + ')') for r in
                           global_vars.relationship_R[record]]
        printer.log('\t\t\tRecords related to', record, ':', rel_records_str, destinations=[global_vars.LOG])
        for related_record in global_vars.relationship_R[record]:
            N_B.add(global_vars.record_to_cluster[related_record])
    printer.log('\t\t\tTherefore Neighborhood of', cluster2, 'consists of clusters:', sorted(N_B),
                destinations=[global_vars.LOG])

    intersection = N_A.intersection(N_B)
    union = N_A.union(N_B)

    if len(union) == 0:
        return 0

    result = len(intersection) / len(union)

    printer.log('\t\tIntersection of', cluster1, 'neighborhood and', cluster2, 'neighborhood:', sorted(intersection),
                destinations=[global_vars.LOG])
    printer.log('\t\tUnion of', cluster1, 'neighborhood and', cluster2, 'neighborhood:', sorted(union),
                destinations=[global_vars.LOG])
    printer.log('\tNeighborhood similarity of clusters', cluster1, 'VS', cluster2,
                '= |Intersection of neighborhoods| / |Union of neighborhoods| =', len(intersection), '/', len(union),
                '=', round(result, 4), destinations=[global_vars.LOG])
    return result


def cluster_similarity(cluster1, cluster2):
    attribute_similarity = round(cluster_attribute_similarity(cluster1, cluster2), 4)

    printer.log('\tAttribute similarity of clusters', cluster1, 'VS', cluster2, '(maximum record similarity)', '=',
                attribute_similarity, destinations=[global_vars.LOG])

    neighborhood_similarity = round(
        cluster_neighborhood_similarity(cluster1, cluster2), 5)

    clusters_combined_similarity = round(
        global_config.default_program_parameters["constant_a"] * attribute_similarity + (
                1 - global_config.default_program_parameters["constant_a"]) * neighborhood_similarity, 4)

    printer.log('Combined cluster similarity=', global_config.default_program_parameters["constant_a"],
                '* (cluster similarity by attribute) +',
                round((1 - global_config.default_program_parameters["constant_a"]), 2),
                '* (cluster similarity by neighborhood) =', global_config.default_program_parameters["constant_a"], '*',
                attribute_similarity, '+', round((1 - global_config.default_program_parameters["constant_a"]), 2), '*',
                neighborhood_similarity, '=', clusters_combined_similarity, destinations=[global_vars.LOG])

    return clusters_combined_similarity
