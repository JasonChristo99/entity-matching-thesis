from DataGeneration.DataGeneratorDriver import generate
from DataGeneration import RANDOM_SEED

import random

random.seed(RANDOM_SEED)


def getRandomSourceConfig():
    return {
        'Name': {
            'typos_binomial_param': random.uniform(0.03, 0.08),
            'synonym_chance': random.uniform(0.3, 0.6),
        },
        'Location': {
            'wrong_value_chance': random.uniform(0.1, 0.2),
            'synonym_chance': random.uniform(0.3, 0.6),
            'typos_binomial_param': random.uniform(0.005, 0.015),
        },
        'Education': {
            'incorrect_fact_geom_param': random.uniform(0.85, 0.95),
            # chance for the source to add incorrect facts to an entity
            'wrong_value_chance': random.uniform(0.05, 0.15),
            # chance for the source to give the wrong value for an attribute
            'synonym_chance': random.uniform(0.05, 0.1),
            'typos_binomial_param': random.uniform(0.01, 0.02)
        },
        'Working Experience': {
            'incorrect_fact_geom_param': random.uniform(0.85, 0.95),
            'wrong_value_chance': random.uniform(0.03, 0.18),
            'synonym_chance': random.uniform(0.03, 0.06),
            'typos_binomial_param': random.uniform(0.005, 0.015),
        }
    }


def run_experiment_dataset_versions():
    experiment_configurations = [
        # config 1
        {
            # constants
            "entities_count": 100,
            "out_file_name": "observed_facts_400_v1",
            # sources
            "sources": [
                {
                    'name': 'pro-profiles.com',
                    **getRandomSourceConfig()
                },
                {
                    'name': 'find.jobs',
                    **getRandomSourceConfig()
                },
                {
                    'name': 'internsteps.com',
                    **getRandomSourceConfig()
                },
                {
                    'name': 'pipl.com',
                    **getRandomSourceConfig()

                }
            ]
        },

        # config 2
        {
            # constants
            "entities_count": 100,
            "out_file_name": "observed_facts_400_v2",
            # sources
            "sources": [
                {
                    'name': 'pro-profiles.com',
                    **getRandomSourceConfig()
                },
                {
                    'name': 'find.jobs',
                    **getRandomSourceConfig()
                },
                {
                    'name': 'internsteps.com',
                    **getRandomSourceConfig()
                },
                {
                    'name': 'pipl.com',
                    **getRandomSourceConfig()

                }
            ]
        },

        # config 3
        {
            # constants
            "entities_count": 100,
            "out_file_name": "observed_facts_400_v3",
            # sources
            "sources": [
                {
                    'name': 'pro-profiles.com',
                    **getRandomSourceConfig()
                },
                {
                    'name': 'find.jobs',
                    **getRandomSourceConfig()
                },
                {
                    'name': 'internsteps.com',
                    **getRandomSourceConfig()
                },
                {
                    'name': 'pipl.com',
                    **getRandomSourceConfig()

                }
            ]
        },

        # config 4
        {
            # constants
            "entities_count": 100,
            "out_file_name": "observed_facts_400_v4",
            # sources
            "sources": [
                {
                    'name': 'pro-profiles.com',
                    **getRandomSourceConfig()
                },
                {
                    'name': 'find.jobs',
                    **getRandomSourceConfig()
                },
                {
                    'name': 'internsteps.com',
                    **getRandomSourceConfig()
                },
                {
                    'name': 'pipl.com',
                    **getRandomSourceConfig()

                }
            ]
        },

        # config 5
        {
            # constants
            "entities_count": 100,
            "out_file_name": "observed_facts_400_v5",
            # sources
            "sources": [
                {
                    'name': 'pro-profiles.com',
                    **getRandomSourceConfig()
                },
                {
                    'name': 'find.jobs',
                    **getRandomSourceConfig()
                },
                {
                    'name': 'internsteps.com',
                    **getRandomSourceConfig()
                },
                {
                    'name': 'pipl.com',
                    **getRandomSourceConfig()

                }
            ]
        }

    ]

    for experiment_config in experiment_configurations:
        print('Config:', "entities_count=", experiment_config["entities_count"], "out_file_name=",
              experiment_config["out_file_name"])
        generate(experiment_config)


if __name__ == "__main__":
    run_experiment_dataset_versions()
