import json

import global_vars
from EntityMatching import printer


def get_records_of_cluster(cluster):
    result = []
    for rec in global_vars.record_to_cluster:
        if global_vars.record_to_cluster[rec] == cluster:
            result.append(rec)

    return result


def reverse_cluster_to_record():
    reverse_map = dict()
    for record in global_vars.record_to_cluster:
        if global_vars.record_to_cluster[record] not in reverse_map:
            reverse_map[global_vars.record_to_cluster[record]] = []
        reverse_map[global_vars.record_to_cluster[record]].append(record)

    return reverse_map


# def print_cluster(cluster):
#     printer.log([global_vars.LOG], cluster, '[')
#     for record in global_vars.record_to_cluster:
#         if global_vars.record_to_cluster[record] == cluster:
#             printer.log([global_vars.LOG], '\t', record, get_record_by_id(record))
#     printer.log([global_vars.LOG], ']')

def construct_cluster(cluster_id):
    arr = []
    for record in global_vars.record_to_cluster:
        if global_vars.record_to_cluster[record] == cluster_id:
            arr.append({record: get_record_by_id(record)})
    return arr


def construct_cluster_short(cluster_id):
    arr = []
    for record in global_vars.record_to_cluster:
        if global_vars.record_to_cluster[record] == cluster_id:
            arr.append(record)
    return arr


def print_observed_data():
    global_vars.observed_data = sorted(global_vars.observed_data, key=lambda k: int(k['id'][1:]))
    s = '\n['
    for record in global_vars.observed_data:
        s += '\n\t' + record['id'] + ' ' + str(record)
    s += '\n]'
    printer.log([global_vars.LOG], s)


# def pretty_print_R():
#     printer.log([global_vars.LOG], 'Relationship R...')
#     seen = set()
#     for record_id in global_vars.relationship_R:
#         if record_id in seen:
#             continue
#         seen.add(record_id)
#         seen.update(global_vars.relationship_R[record_id])
#         printer.log([global_vars.LOG], '[')
#         printer.log([global_vars.LOG], '\t', record_id, get_record_by_id(record_id))
#         for recd in global_vars.relationship_R[record_id]:
#             printer.log([global_vars.LOG], '\t', recd, get_record_by_id(recd))
#         printer.log([global_vars.LOG], ']')


def construct_pretty_relationship_R():
    groups = []
    seen = set()
    for record_id in global_vars.relationship_R:
        if record_id in seen:
            continue
        seen.add(record_id)
        seen.update(global_vars.relationship_R[record_id])
        arr = [{record_id: get_record_by_id(record_id)}]
        for recd in global_vars.relationship_R[record_id]:
            arr.append({recd: get_record_by_id(recd)})
        groups.append(arr)
    return groups


def print_pretty_relationship_R():
    s = '\n['
    for group in construct_pretty_relationship_R():
        s += '\n\t[ '
        for record in group:
            s += list(record.keys())[0] + ' '
        s += ']'
    s += '\n]'
    printer.log([global_vars.LOG], s)


def get_record_by_id(record_id):
    return [rec for rec in global_vars.observed_data if rec['id'] == record_id][0]


# def pretty_print_result_clusters(record_to_cluster):
#     # examine results
#     reverse = reverse_cluster_to_record(record_to_cluster)
#     for cluster in reverse:
#         # if verbose: print(cluster)
#         printer.log([global_vars.LOG], cluster, '[')
#         printer.log([global_vars.LOG], cluster, '[')
#         for fact in reverse[cluster]:
#             printer.log([global_vars.LOG], '\t', get_record_by_id(fact))
#             printer.log([global_vars.LOG], '\t', get_record_by_id(fact))
#         printer.log([global_vars.LOG], ']')
#         printer.log([global_vars.LOG], ']')
#         printer.log([global_vars.LOG], )
#         printer.log([global_vars.LOG])


def construct_result_clusters():
    # examine results
    reverse = reverse_cluster_to_record()
    parent_group = []
    for cluster in reverse:
        # if verbose: print(cluster)
        parent_group.append([])
        most_recent_group = len(parent_group) - 1
        for fact in reverse[cluster]:
            parent_group[most_recent_group].append(get_record_by_id(fact))
    return parent_group
