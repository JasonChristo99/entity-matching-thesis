import json
import random
import time
from pathlib import Path

import fuse
from DataGeneration.DataGenerator import DataGenerator

from matching.ahc_matcher import AgglomerativeHierarchicalClustering
from matching.dedupe import DedupeMatcher

random.seed(10)


class FusionExperiment:
    def __init__(self):
        self.dataset_configurations = []
        self.timestamp = ''
        self.experiment_folder = ''
        self.current_dataset_path = ''
        self.prepare_dataset_configs()
        self.matching_strategy = None

    def init_experiment(self, dataset_config):
        self.timestamp = time.strftime("%Y%m%d_%H%M%S")
        self.prepare_experiment_folder(dataset_config)

    def prepare_experiment_folder(self, dataset_config):
        self.experiment_folder = 'experiment_' + self.timestamp + '/'
        Path(self.experiment_folder).mkdir(parents=True, exist_ok=True)
        with open(self.experiment_folder + 'dataset_config.json', 'w') as f:
            f.write(json.dumps(dataset_config, indent=2))

    def prepare_dataset_configs(self):
        entities_count = 200
        self.dataset_configurations = [
            # config 1
            {
                # constants
                "entities_count": entities_count,
                "out_file_name": "observed_facts_400_v1",
                # sources
                "sources": [
                    {
                        'name': 'pro-profiles.com',
                        **self.get_random_source_config()
                    },
                    {
                        'name': 'find.jobs',
                        **self.get_random_source_config()
                    },
                    {
                        'name': 'internsteps.com',
                        **self.get_random_source_config()
                    },
                    {
                        'name': 'pipl.com',
                        **self.get_random_source_config()

                    }
                ]
            },

            # config 2
            {
                # constants
                "entities_count": entities_count,
                "out_file_name": "observed_facts_400_v2",
                # sources
                "sources": [
                    {
                        'name': 'pro-profiles.com',
                        **self.get_random_source_config()
                    },
                    {
                        'name': 'find.jobs',
                        **self.get_random_source_config()
                    },
                    {
                        'name': 'internsteps.com',
                        **self.get_random_source_config()
                    },
                    {
                        'name': 'pipl.com',
                        **self.get_random_source_config()

                    }
                ]
            },

            # config 3
            {
                # constants
                "entities_count": entities_count,
                "out_file_name": "observed_facts_400_v3",
                # sources
                "sources": [
                    {
                        'name': 'pro-profiles.com',
                        **self.get_random_source_config()
                    },
                    {
                        'name': 'find.jobs',
                        **self.get_random_source_config()
                    },
                    {
                        'name': 'internsteps.com',
                        **self.get_random_source_config()
                    },
                    {
                        'name': 'pipl.com',
                        **self.get_random_source_config()

                    }
                ]
            },

            # config 4
            {
                # constants
                "entities_count": entities_count,
                "out_file_name": "observed_facts_400_v4",
                # sources
                "sources": [
                    {
                        'name': 'pro-profiles.com',
                        **self.get_random_source_config()
                    },
                    {
                        'name': 'find.jobs',
                        **self.get_random_source_config()
                    },
                    {
                        'name': 'internsteps.com',
                        **self.get_random_source_config()
                    },
                    {
                        'name': 'pipl.com',
                        **self.get_random_source_config()

                    }
                ]
            },

            # config 5
            {
                # constants
                "entities_count": entities_count,
                "out_file_name": "observed_facts_400_v5",
                # sources
                "sources": [
                    {
                        'name': 'pro-profiles.com',
                        **self.get_random_source_config()
                    },
                    {
                        'name': 'find.jobs',
                        **self.get_random_source_config()
                    },
                    {
                        'name': 'internsteps.com',
                        **self.get_random_source_config()
                    },
                    {
                        'name': 'pipl.com',
                        **self.get_random_source_config()

                    }
                ]
            }

        ]

    def get_random_source_config(self):
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

    def generate_dataset(self, dataset_config):
        print('Config:', "entities_count=", dataset_config["entities_count"], "out_file_name=",
              dataset_config["out_file_name"])
        generator = DataGenerator(dataset_config["sources"],
                                  entities_count=dataset_config["entities_count"],
                                  out_folder=self.experiment_folder + "dataset/")
        generator.generate(verbose=False)

    def run_fusion(self, matching_strategy):
        matcher_folder = "dedupe_result/" if matching_strategy is DedupeMatcher else "ahc_result/"
        current_folder = self.experiment_folder + matcher_folder
        Path(current_folder).mkdir(parents=True, exist_ok=True)
        fuse_env = fuse.Fuse(verbose=True, home_dir=current_folder)
        fuse_session = fuse_env.get_session('test')
        dataset = fuse_session.ingest(self.experiment_folder + 'dataset/observed.json', 'fusion_test',
                                      matching_strategy)
        fuse_session.train_matchers(self.experiment_folder + 'dataset/matched.json')
        fuse_session.match_observations()
        tr_clusters = fuse_session.find_true_clusters()
        true_facts = fuse_session.find_true_facts(persist=True, no_weights=False)
        eval = fuse_session.evaluate(inferred_facts=true_facts, grd_path=self.experiment_folder + 'dataset/eval.json')

    def run_experiment(self):
        # for each dataset version
        for dataset_config in self.dataset_configurations:
            self.init_experiment(dataset_config)

            # generate dataset
            self.generate_dataset(dataset_config)

            # run fusion using old matcher
            self.run_fusion(matching_strategy=DedupeMatcher)

            # run fusion using new matcher
            self.run_fusion(matching_strategy=AgglomerativeHierarchicalClustering)


if __name__ == "__main__":
    exp = FusionExperiment()
    exp.run_experiment()
