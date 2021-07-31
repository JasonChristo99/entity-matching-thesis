import global_vars
from util import get_records_of_cluster
from config import program_parameters


def name_vertical_similarity(record1, record2):
    # 'Name' vertical always has cardinality of 1 tuple
    if global_vars.verbose_file: print('Comparing Name vertical.', file=global_vars.log)

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

    if global_vars.verbose_file: print(name1, 'vs', name2, '=', result, file=global_vars.log)

    if global_vars.verbose_file: print('Name similarity=', result, file=global_vars.log)

    return result


def location_vertical_similarity(record1, record2):
    # 'Location' vertical always has cardinality of 1 tuple
    if global_vars.verbose_file: print('Comparing Location vertical.', file=global_vars.log)

    location1 = record1.get('Location')
    location2 = record2.get('Location')

    if location1 is None or location2 is None:
        return 0

    location1 = record1['Location'][0]['location'].lower()
    location2 = record2['Location'][0]['location'].lower()
    # result = jellyfish.jaro_winkler_similarity(location1, location2)
    result = program_parameters["location_sim_func"](location1, location2)
    if global_vars.verbose_file: print(location1, 'vs', location2, '=', result, file=global_vars.log)

    if global_vars.verbose_file: print('Location similarity=', result, file=global_vars.log)

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

    if global_vars.verbose_file: print(tuple1.get('degree', ''), 'vs', tuple2.get('degree', ''), '=', degree_sim,
                                       file=global_vars.log)
    if global_vars.verbose_file: print(tuple1.get('university', ''), 'vs', tuple2.get('university', ''), '=',
                                       university_sim,
                                       file=global_vars.log)
    if global_vars.verbose_file: print(tuple1.get('year', ''), 'vs', tuple2.get('year', ''), '=', year_sim,
                                       file=global_vars.log)
    result = 0.3 * degree_sim + 0.5 * university_sim + 0.2 * year_sim

    if global_vars.verbose_file: print('Tuples similarity=', 0.3, '*', degree_sim, '+', 0.5, '*', university_sim, '+',
                                       0.2, '*',
                                       year_sim, '=',
                                       result, file=global_vars.log)

    return result


def education_vertical_similarity(record1, record2):
    # 'Education' vertical may have cardinality of 1 or more tuple
    # we assume that the similarity between two multi-tuple education records
    # is the average similarity of all tuple pairs
    if global_vars.verbose_file: print('Comparing Education vertical.', file=global_vars.log)

    education1 = record1.get('Education')
    education2 = record2.get('Education')

    if education1 is None or education2 is None:
        return 0

    max_similarity = 0
    similarity_sum = 0
    count_pairs = 0
    for index1, tuple1 in enumerate(education1):
        for index2, tuple2 in enumerate(education2):
            if global_vars.verbose_file: print('Comparing tuples', tuple1, 'vs', tuple2, file=global_vars.log)
            tuple_similarity = education_tuple_similarity(tuple1, tuple2)
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

    # return avg_similarity

    if global_vars.verbose_file: print('Education similarity (max tuples similarity)=', max_similarity,
                                       file=global_vars.log)
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

    if global_vars.verbose_file: print(tuple1.get('title', ''), 'vs', tuple2.get('title', ''), '=', title_sim,
                                       file=global_vars.log)
    if global_vars.verbose_file: print(tuple1.get('company', ''), 'vs', tuple2.get('company', ''), '=', company_sim,
                                       file=global_vars.log)
    if global_vars.verbose_file: print(tuple1.get('years', ''), 'vs', tuple2.get('years', ''), '=', years_sim,
                                       file=global_vars.log)
    result = 0.3 * title_sim + 0.5 * company_sim + 0.2 * years_sim

    if global_vars.verbose_file: print('Tuples similarity=', 0.3, '*', title_sim, '+', 0.5, '*', company_sim, '+', 0.2,
                                       '*',
                                       years_sim,
                                       '=',
                                       result, file=global_vars.log)

    return result


def working_experience_vertical_similarity(record1, record2):
    # 'Working Experience' vertical may have cardinality of 1 or more tuple
    # we assume that the similarity between two multi-tuple education records
    # is the average similarity of all tuple pairs
    if global_vars.verbose_file: print('Comparing Working Experience vertical.', file=global_vars.log)

    working_exp_1 = record1.get('Working Experience')
    working_exp_2 = record2.get('Working Experience')

    if working_exp_1 is None or working_exp_2 is None:
        return 0

    max_similarity = 0
    similarity_sum = 0
    count_pairs = 0
    for index1, tuple1 in enumerate(working_exp_1):
        for index2, tuple2 in enumerate(working_exp_2):
            if global_vars.verbose_file: print('Comparing tuples', tuple1, 'vs', tuple2, file=global_vars.log)
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

    if global_vars.verbose_file: print('Working Experience similarity (max tuples similarity)=', max_similarity,
                                       file=global_vars.log)
    # return avg_similarity
    return max_similarity


def relationship_similarity_v1(record1, record2):
    result = name_vertical_similarity(record1, record2) + location_vertical_similarity(record1, record2)
    return result


def relationship_similarity_v2(record1, record2):
    result = name_vertical_similarity(record1, record2)
    return result


def relationship_similarity(record1, record2):
    result = program_parameters["relationship_R_sim_func"](record1, record2)
    return result


def record_similarity(record1_id, record2_id):
    record1 = [rec for rec in global_vars.observed_data if rec["id"] == record1_id][0]
    record2 = [rec for rec in global_vars.observed_data if rec["id"] == record2_id][0]

    # if len(record1.keys()) != len(record2.keys()):
    common_verticals_count = len(set(record1.keys()).intersection(set(record2.keys())))
    # if common_verticals_count == 5:
    name_sim = name_vertical_similarity(record1, record2)
    loc_sim = location_vertical_similarity(record1, record2)
    edu_sim = education_vertical_similarity(record1, record2)
    work_sim = working_experience_vertical_similarity(record1, record2)
    # if verbose:
    # if verbose: print('Attribute sim. of ', record1_id, record2_id, file=global_vars.global_log)
    # if verbose: print(record1.get('Name'), 'vs', record2.get('Name'), '=', name_sim, file=global_vars.global_log)
    # if verbose: print(record1.get('Location'), 'vs', record2.get('Location'), '=', loc_sim, file=global_vars.global_log)
    # if verbose: print(record1.get('Education'), 'vs', record2.get('Education'), '=', edu_sim, file=global_vars.global_log)
    # if verbose: print(record1.get('Working Experience'), 'vs', record2.get('Working Experience'), '=', work_sim, file=global_vars.global_log)

    result = 0.1 * loc_sim + 0.3 * name_sim + 0.3 * edu_sim + 0.3 * work_sim

    if global_vars.verbose_file: print('Attribute sim. of records', record1_id, record2_id, '=', 0.3, '*', name_sim,
                                       '+', 0.1, '*',
                                       loc_sim, '+',
                                       0.3, '*', edu_sim, '+', 0.3, '*', work_sim, '=', result,
                                       file=global_vars.log)

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
            if verbose: print('Comparing records', record1, record2, 'by Attribute similarity:',
                              file=global_vars.log)
            record_sim = record_similarity(record1, record2)
            # if verbose:
            # if verbose: print('Attribute similarity of', record1, record2, '=', record_sim, file=global_vars.global_log)
            if record_sim > max_sim:
                max_sim = record_sim

    return max_sim


def cluster_neighborhood_similarity(cluster1, cluster2, record_to_cluster, relationship_R, verbose=False):
    # sim(c1,c2) = ½ [simattributes(c1,c2) + simneighbours(c1,c2)] - with Complete Link
    if verbose: print('Calculating clusters', cluster1, 'vs', cluster2, 'Neighborhood similarity',
                      file=global_vars.log)
    N_A = set()
    records_of_cluster1 = get_records_of_cluster(cluster1, record_to_cluster)
    if verbose: print('Records of cluster', cluster1, ':', records_of_cluster1, file=global_vars.log)
    for record in records_of_cluster1:
        rel_records_str = [(r + ' (in ' + record_to_cluster[r] + ')') for r in relationship_R[record]]
        if verbose: print('Records related to', record, ':', rel_records_str, file=global_vars.log)
        for related_record in relationship_R[record]:
            N_A.add(record_to_cluster[related_record])
    if verbose: print('Neighborhood (clusters) of', cluster1, ':', N_A, file=global_vars.log)

    N_B = set()
    records_of_cluster2 = get_records_of_cluster(cluster2, record_to_cluster)
    if verbose: print('Records of cluster', cluster2, ':', records_of_cluster2, file=global_vars.log)
    for record in records_of_cluster2:
        rel_records_str = [(r + ' (in ' + record_to_cluster[r] + ')') for r in relationship_R[record]]
        if verbose: print('Records related to', record, ':', rel_records_str, file=global_vars.log)
        for related_record in relationship_R[record]:
            N_B.add(record_to_cluster[related_record])
    if verbose: print('Neighborhood (clusters) of', cluster2, ':', N_B, file=global_vars.log)

    intersection = N_A.intersection(N_B)
    union = N_A.union(N_B)

    if len(union) == 0:
        return 0

    result = len(intersection) / len(union)
    # if verbose:
    if verbose: print('Neighborhood of', cluster2, ':', N_B, file=global_vars.log)
    if verbose: print('Intersection:', intersection, file=global_vars.log)
    if verbose: print('Union:', union, file=global_vars.log)
    if verbose: print('Neighborhood similarity=|intersection|/|union|=', len(intersection), '/', len(union), result,
                      file=global_vars.log)
    return result


def cluster_similarity(cluster1, cluster2, record_to_cluster, relationship_R, verbose=False):
    attribute_similarity = round(
        cluster_attribute_similarity(cluster1, cluster2, record_to_cluster, verbose), 2)
    # if verbose:
    # if verbose: print('Comparing:', cluster1, cluster2, file=global_vars.global_log)
    if verbose: print('Attribute similarity of clusters (maximum sim. of cluster records)', cluster1, cluster2, '=',
                      attribute_similarity, file=global_vars.log)

    neighborhood_similarity = round(
        cluster_neighborhood_similarity(cluster1, cluster2, record_to_cluster, relationship_R, verbose=True), 2)
    # if verbose:
    # if verbose: print('neighborhood_similarity=', '(', cluster1, cluster2, ')', neighborhood_similarity, file=global_vars.global_log)

    clusters_combined_similarity = round(
        program_parameters["constant_a"] * attribute_similarity + (
                1 - program_parameters["constant_a"]) * neighborhood_similarity, 2)
    # if verbose:
    if verbose: print('Combined cluster similarity=', program_parameters["constant_a"], '* attribute_similarity + ',
                      (1 - program_parameters["constant_a"]),
                      '* neighborhood_similarity=', program_parameters["constant_a"], '*', attribute_similarity, '+',
                      (1 - program_parameters["constant_a"]), '*',
                      neighborhood_similarity, '=', clusters_combined_similarity, file=global_vars.log)
    # if verbose:
    # if verbose: print('Comparing:', cluster1, cluster2, ': clusters_combined_similarity=', constant_a, '*',
    #       attribute_similarity, '+', (1 - constant_a), '*',
    #       neighborhood_similarity, '=', clusters_combined_similarity, file=global_vars.global_log)
    return clusters_combined_similarity
