import json

import global_vars
from EntityMatching import printer


def get_records_of_cluster(cluster):
    result = global_vars.cluster_to_record[cluster]
    return result


def reverse_cluster_to_record():
    reverse_map = dict()
    for fact_id in global_vars.record_to_cluster:
        if global_vars.record_to_cluster[fact_id] not in reverse_map:
            reverse_map[global_vars.record_to_cluster[fact_id]] = []
        reverse_map[global_vars.record_to_cluster[fact_id]].append(fact_id)

    return reverse_map


# def print_cluster(cluster):
#     printer.log([global_vars.LOG], cluster, '[')
#     for record in global_vars.record_to_cluster:
#         if global_vars.record_to_cluster[record] == cluster:
#             printer.log([global_vars.LOG], '\t', record, get_record_by_id(record))
#     printer.log([global_vars.LOG], ']')

def construct_cluster(cluster_id):
    arr = []
    for fact_id in global_vars.cluster_to_record[cluster_id]:
        arr.append({fact_id: get_record_by_id(fact_id)})
    return arr


def construct_cluster_short(cluster_id):
    arr = []
    for fact_id in global_vars.cluster_to_record[cluster_id]:
        arr.append(fact_id)
    return arr


def print_observed_data():
    # global_vars.observed_data = sorted(global_vars.observed_data, key=lambda k: int(k['id'][1:]))
    data = sorted(global_vars.observed_data.keys(), key=lambda k: int(k[1:]))
    s = '\n['
    for fact_id in data:
        s += '\n\t' + fact_id + ' ' + str(global_vars.observed_data[fact_id])
    s += '\n]'
    printer.log(s, destinations=[global_vars.LOG])


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


# def construct_pretty_relationship_R():
#     groups = []
#     seen = set()
#     for record_id in global_vars.relationship_R:
#         if record_id in seen:
#             continue
#         seen.add(record_id)
#         seen.update(global_vars.relationship_R[record_id])
#         arr = [{record_id: get_record_by_id(record_id)}]
#         for recd in global_vars.relationship_R[record_id]:
#             arr.append({recd: get_record_by_id(recd)})
#         groups.append(arr)
#     return groups


# def print_pretty_relationship_R():
#     s = '\n['
#     for group in construct_pretty_relationship_R():
#         s += '\n\t[ '
#         for record in group:
#             s += list(record.keys())[0] + ' '
#         s += ']'
#     s += '\n]'
#     printer.log([global_vars.LOG], s)

def print_pretty_relationship_R():
    s = '\n{'
    for key in global_vars.relationship_R.keys():
        s += '\n\t' + key + ': [ '
        for value in global_vars.relationship_R[key]:
            s += value + ' '
        s += ']'
    s += '\n}'
    printer.log(s, destinations=[global_vars.LOG])


def get_record_by_id(fact_id):
    return global_vars.observed_data[fact_id]


def pretty_print_result_clusters(destinations=None):
    if destinations is None:
        destinations = [global_vars.LOG]
    reverse = reverse_cluster_to_record()
    s = '\n['
    for cluster in reverse:
        s += '\n\t' + cluster + ': [ '
        for fact in reverse[cluster]:
            s += '\n\t\t' + fact + ' ' + json.dumps(get_record_by_id(fact))
        s += '\n\t]'
    s += '\n]'
    printer.log(s, destinations=destinations)


def construct_result_clusters():
    # examine results
    # reverse = reverse_cluster_to_record()
    parent_group = []
    for cluster in global_vars.cluster_to_record:
        # if verbose: print(cluster)
        parent_group.append([])
        most_recent_group = len(parent_group) - 1
        for fact in global_vars.cluster_to_record[cluster]:
            parent_group[most_recent_group].append(get_record_by_id(fact))
    return parent_group
