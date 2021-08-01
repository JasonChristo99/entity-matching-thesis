import random
import pandas as pd
import global_vars

random.seed(10)


def ingest_observed_data(file_path):
    bag_of_facts = []
    fact_id = 1
    try:
        raw_frame = pd.read_json(file_path)
        for group in raw_frame['body']:
            # print(group)
            # transform input to match AHC form
            observed_facts_of_source_for_entity = {}
            for observed_fact in group:
                source = observed_fact['meta']['source']
                vertical = list(observed_fact.keys())[0]
                if source not in observed_facts_of_source_for_entity:
                    observed_facts_of_source_for_entity[source] = {}

                if vertical not in observed_facts_of_source_for_entity[source]:
                    observed_facts_of_source_for_entity[source][vertical] = []

                observed_facts_of_source_for_entity[source][vertical].append(observed_fact[vertical])

            global_vars.correct_groups_of_facts.append([])
            most_recent_group = len(global_vars.correct_groups_of_facts) - 1
            for source in observed_facts_of_source_for_entity:
                observed_fact_normalized = observed_facts_of_source_for_entity[source]
                observed_fact_normalized['id'] = 'f' + str(fact_id)
                # construct correct groups for evaluation purposes
                global_vars.correct_groups_of_facts[most_recent_group].append(observed_fact_normalized)
                # construct bag of facts (not grouped facts)
                bag_of_facts.append(observed_fact_normalized)

                fact_id += 1

        random.shuffle(bag_of_facts)
        global_vars.observed_data = bag_of_facts
        # return bag_of_facts
    except:
        if global_vars.verbose_file: print('Invalid file given as input.')
        raise
