import global_vars
from EntityMatching import printer


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
    printer.log([global_vars.LOG], cluster, '[')
    for record in record_to_cluster:
        if record_to_cluster[record] == cluster:
            printer.log([global_vars.LOG], '\t', record, get_record_by_id(record))
    printer.log([global_vars.LOG], ']')


def print_observed_data():
    printer.log([global_vars.LOG], '[')
    for record in global_vars.observed_data:
        printer.log([global_vars.LOG], '\t', record['id'], record)
    printer.log([global_vars.LOG], ']')


def pretty_print_R(relationship_R):
    printer.log([global_vars.LOG], 'Relationship R...')
    seen = set()
    for record_id in relationship_R:
        if record_id in seen:
            continue
        seen.add(record_id)
        seen.update(relationship_R[record_id])
        printer.log([global_vars.LOG], '[')
        printer.log([global_vars.LOG], '\t', record_id, get_record_by_id(record_id))
        for recd in relationship_R[record_id]:
            printer.log([global_vars.LOG], '\t', recd, get_record_by_id(recd))
        printer.log([global_vars.LOG], ']')


def get_record_by_id(record_id):
    return [rec for rec in global_vars.observed_data if rec['id'] == record_id][0]


def pretty_print_result_clusters(record_to_cluster):
    # examine results
    reverse = reverse_cluster_to_record(record_to_cluster)
    for cluster in reverse:
        # if verbose: print(cluster)
        printer.log([global_vars.LOG], cluster, '[')
        printer.log([global_vars.LOG], cluster, '[')
        for fact in reverse[cluster]:
            printer.log([global_vars.LOG], '\t', get_record_by_id(fact))
            printer.log([global_vars.LOG], '\t', get_record_by_id(fact))
        printer.log([global_vars.LOG], ']')
        printer.log([global_vars.LOG], ']')
        printer.log([global_vars.LOG], )
        printer.log([global_vars.LOG])


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
