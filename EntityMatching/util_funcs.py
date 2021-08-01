import global_vars


def get_records_of_cluster(cluster, record_to_cluster):
    result = []
    for rec in record_to_cluster:
        if record_to_cluster[rec] == cluster:
            result.append(rec)

    return result


def reverse_cluster_to_record(record_to_cluster):
    reverse_map = dict()
    for record in record_to_cluster:
        if record_to_cluster[record] not in reverse_map:
            reverse_map[record_to_cluster[record]] = []
        reverse_map[record_to_cluster[record]].append(record)

    return reverse_map


def print_cluster(cluster, record_to_cluster):
    if global_vars.verbose_file: print(cluster, '[', file=global_vars.log)
    for record in record_to_cluster:
        if record_to_cluster[record] == cluster:
            if global_vars.verbose_file: print('\t', record, get_record_by_id(record), file=global_vars.log)
    if global_vars.verbose_file: print(']', file=global_vars.log)


def print_observed_data():
    if global_vars.verbose_file: print('[', file=global_vars.log)
    for record in global_vars.observed_data:
        if global_vars.verbose_file: print('\t', record['id'], record, file=global_vars.log)
    if global_vars.verbose_file: print(']', file=global_vars.log)


def pretty_print_R(relationship_R):
    if global_vars.verbose_file: print('Relationship R...', file=global_vars.log)
    seen = set()
    for record_id in relationship_R:
        if record_id in seen:
            continue
        seen.add(record_id)
        seen.update(relationship_R[record_id])
        if global_vars.verbose_file: print('[', file=global_vars.log)
        if global_vars.verbose_file: print('\t', record_id, get_record_by_id(record_id), file=global_vars.log)
        for recd in relationship_R[record_id]:
            if global_vars.verbose_file: print('\t', recd, get_record_by_id(recd), file=global_vars.log)
        if global_vars.verbose_file: print(']', file=global_vars.log)


def get_record_by_id(record_id):
    return [rec for rec in global_vars.observed_data if rec['id'] == record_id][0]


def pretty_print_result_clusters(record_to_cluster):
    # examine results
    reverse = reverse_cluster_to_record(record_to_cluster)
    for cluster in reverse:
        # if verbose: print(cluster)
        if global_vars.verbose_console: print(cluster, '[')
        if global_vars.verbose_file: print(cluster, '[', file=global_vars.log)
        for fact in reverse[cluster]:
            if global_vars.verbose_console: print('\t', get_record_by_id(fact))
            if global_vars.verbose_file: print('\t', get_record_by_id(fact), file=global_vars.log)
        if global_vars.verbose_console: print(']')
        if global_vars.verbose_file: print(']', file=global_vars.log)
        if global_vars.verbose_console: print()
        if global_vars.verbose_file: print(file=global_vars.log)


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
