import global_vars
import util_funcs


def evaluate_fact(fact_id, result_record_to_cluster):
    # find elements in the same cluster as the given fact
    records_of_fact_cluster = util_funcs.get_records_of_cluster(result_record_to_cluster[fact_id], result_record_to_cluster)
    records_of_fact_cluster.remove(fact_id)
    neighboring_elements_of_fact_in_cluster = records_of_fact_cluster

    # find elements in the same cluster with the given as found in the correct groups
    correct_group = []
    for group in global_vars.correct_groups_of_facts:
        for fact in group:
            if fact["id"] == fact_id:
                correct_group = group
                break
    correct_neighboring_elements_of_fact_in_cluster = []
    for fact in correct_group:
        if fact["id"] == fact_id: continue
        correct_neighboring_elements_of_fact_in_cluster.append(fact["id"])

    # compare predicted neighbors vs actual neighbors of the fact
    false_positives = sum(
        el not in correct_neighboring_elements_of_fact_in_cluster for el in neighboring_elements_of_fact_in_cluster)
    true_positives = sum(
        el in correct_neighboring_elements_of_fact_in_cluster for el in neighboring_elements_of_fact_in_cluster)
    false_negatives = sum(
        el not in neighboring_elements_of_fact_in_cluster for el in correct_neighboring_elements_of_fact_in_cluster)

    false_facts = [f["id"] for f in global_vars.observed_data]
    for f in correct_neighboring_elements_of_fact_in_cluster:
        false_facts.remove(f)

    true_negatives = sum(el not in neighboring_elements_of_fact_in_cluster for el in false_facts)

    result = {
        "false_positives": false_positives,
        "true_positives": true_positives,
        "false_negatives": false_negatives,
        "true_negatives": true_negatives,
        "predicted_neighbors": neighboring_elements_of_fact_in_cluster,
        "correct_neighbors": correct_neighboring_elements_of_fact_in_cluster,
        "total_facts": len(global_vars.observed_data)
    }
    return result


def evaluate_result_clusters(result_clusters, result_record_to_cluster):
    # for every fact in the result clusters count:
    # a) facts falsely in the same group (false positives) - ta stoixeia pou einai geitones enw de tha eprepe
    # b) facts correctly in the same group (true positives) - ta stoixeia pou einai geitones enw tha eprepe
    # c) facts falsely not in the same group (false negative) - ta stoixeia pou den einai geitones enw tha eprepe
    # d) facts correctly not in the same group (true negative) - ta stoixeia pou den einai geitones enw den tha eprepe
    result = dict()
    for result_cluster in result_clusters:
        for fact in result_cluster:
            fact_id = fact["id"]
            result[fact_id] = evaluate_fact(fact_id, result_record_to_cluster)
    return result


def sum_evaluation_for_all_facts(evaluation):
    total_FP = 0
    total_TP = 0
    total_FN = 0
    total_TN = 0
    for fact_id in evaluation:
        total_FP += evaluation[fact_id]["false_positives"]
        total_TP += evaluation[fact_id]["true_positives"]
        total_FN += evaluation[fact_id]["false_negatives"]
        total_TN += evaluation[fact_id]["true_negatives"]
    result = {
        "total_FP": total_FP,
        "total_TP": total_TP,
        "total_FN": total_FN,
        "total_TN": total_TN,
    }
    return result
