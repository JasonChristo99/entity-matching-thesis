import json
import random
import time
from pathlib import Path
from faker import Faker
import matplotlib.pyplot as plt

import fuse
from DataGeneration.DataGenerator import DataGenerator

from matching.ahc_matcher import AgglomerativeHierarchicalClusteringWithNaiveSimrank, \
    AgglomerativeHierarchicalClusteringWithDomainSimrank
from matching.dedupe import DedupeMatcher
from DataGeneration import RANDOM_SEED

Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)


class FusionExperiment:
    def __init__(self):
        self.dataset_configurations = []
        self.timestamp = ''
        self.experiment_folder = ''
        self.current_dataset_path = ''
        self.fake = Faker()
        self.prepare_dataset_configs()
        self.matching_strategy = None

    def init_experiment(self, dataset_config):
        self.timestamp = time.strftime("%Y%m%d_%H%M%S")
        self.prepare_experiment_folder(dataset_config)

    # def prepare_timestamp(self):
    #     self.timestamp = time.strftime("%Y%m%d_%H%M%S")

    def prepare_experiment_folder(self, dataset_config):
        self.experiment_folder = 'experiment_' + self.timestamp + '/'
        Path(self.experiment_folder).mkdir(parents=True, exist_ok=True)
        with open(self.experiment_folder + 'dataset_config.json', 'w') as f:
            f.write(json.dumps(dataset_config, indent=2))

    def prepare_dataset_configs(self, num_entities=20, num_sources=4, num_dataset_configs=1, version='generic'):
        TYPOS_BINOMIAL_PARAM = {'MIN': 0.01, 'MAX': 0.02}
        SYNONYM_CHANCE = {'MIN': 0.1, 'MAX': 0.4}
        INCORRECT_FACT_GEOM_PARAM = {'MIN': 0.85, 'MAX': 0.95}
        WRONG_VALUE_CHANCE = {'MIN': 0.1, 'MAX': 0.2}
        # if ds_type == 'generic' default
        if version == 'high_typos':
            TYPOS_BINOMIAL_PARAM = {'MIN': 0.03, 'MAX': 0.08}
        elif version == 'high_synonym_chance':
            SYNONYM_CHANCE = {'MIN': 0.3, 'MAX': 0.6}
        elif version == 'high_wrong_value_chance':
            WRONG_VALUE_CHANCE = {'MIN': 0.2, 'MAX': 0.5}
        elif version == 'many_sources':
            num_sources = 8

        self.dataset_configurations = [
            {
                # constants
                "entities_count": num_entities,
                # sources
                "sources": [
                    {
                        'name': self.fake.domain_name(),
                        'Name': {
                            'typos_binomial_param': random.uniform(TYPOS_BINOMIAL_PARAM['MIN'],
                                                                   TYPOS_BINOMIAL_PARAM['MAX']),
                            'synonym_chance': random.uniform(SYNONYM_CHANCE['MIN'], SYNONYM_CHANCE['MAX']),
                        },
                        'Location': {
                            'wrong_value_chance': random.uniform(WRONG_VALUE_CHANCE['MIN'], WRONG_VALUE_CHANCE['MAX']),
                            'synonym_chance': random.uniform(SYNONYM_CHANCE['MIN'], SYNONYM_CHANCE['MAX']),
                            'typos_binomial_param': random.uniform(TYPOS_BINOMIAL_PARAM['MIN'],
                                                                   TYPOS_BINOMIAL_PARAM['MAX']),
                        },
                        'Education': {
                            'incorrect_fact_geom_param': random.uniform(INCORRECT_FACT_GEOM_PARAM['MIN'],
                                                                        INCORRECT_FACT_GEOM_PARAM['MAX']),
                            # chance for the source to add incorrect facts to an entity
                            'wrong_value_chance': random.uniform(WRONG_VALUE_CHANCE['MIN'], WRONG_VALUE_CHANCE['MAX']),
                            # chance for the source to give the wrong value for an attribute
                            'synonym_chance': random.uniform(SYNONYM_CHANCE['MIN'], SYNONYM_CHANCE['MAX']),
                            'typos_binomial_param': random.uniform(TYPOS_BINOMIAL_PARAM['MIN'],
                                                                   TYPOS_BINOMIAL_PARAM['MAX'])
                        },
                        'Working Experience': {
                            'incorrect_fact_geom_param': random.uniform(INCORRECT_FACT_GEOM_PARAM['MIN'],
                                                                        INCORRECT_FACT_GEOM_PARAM['MAX']),
                            'wrong_value_chance': random.uniform(WRONG_VALUE_CHANCE['MIN'], WRONG_VALUE_CHANCE['MAX']),
                            'synonym_chance': random.uniform(SYNONYM_CHANCE['MIN'], SYNONYM_CHANCE['MAX']),
                            'typos_binomial_param': random.uniform(TYPOS_BINOMIAL_PARAM['MIN'],
                                                                   TYPOS_BINOMIAL_PARAM['MAX']),
                        }
                    } for _ in range(num_sources)

                ]
            } for _ in range(num_dataset_configs)

        ]

    def generate_dataset(self, dataset_config):
        print('Config:', "entities_count=", dataset_config["entities_count"])
        generator = DataGenerator(dataset_config["sources"],
                                  entities_count=dataset_config["entities_count"],
                                  out_folder=self.experiment_folder + "dataset/")
        generator.generate(verbose=False)

    def run_fusion(self, matching_strategy, infer_entities=False):
        # folder structure for experiments:
        # - experiment_X
        # -     dataset
        # -     ahc_naiveresult
        # -     ahc_domainresult
        # -     dedupe_result
        matcher_folder: str
        if matching_strategy is DedupeMatcher:
            matcher_folder = "dedupe_result/"
        elif matching_strategy is AgglomerativeHierarchicalClusteringWithNaiveSimrank:
            matcher_folder = "ahc_naive_simrank_result/"
        else:
            matcher_folder = "ahc_domain_simrank_result/"

        current_folder = self.experiment_folder + matcher_folder
        Path(current_folder).mkdir(parents=True, exist_ok=True)

        fuse_env = fuse.Fuse(verbose=True, home_dir=current_folder)
        fuse_session = fuse_env.get_session('test')
        dataset = fuse_session.ingest(self.experiment_folder + 'dataset/observed.json', 'fusion_test',
                                      matching_strategy, infer_entities=infer_entities)
        fuse_session.train_matchers(self.experiment_folder + 'dataset/matched.json')
        fuse_session.match_observations()
        tr_clusters = fuse_session.find_true_clusters()
        true_facts = fuse_session.find_true_facts(persist=True, no_weights=False)
        eval = fuse_session.evaluate(inferred_facts=true_facts, grd_path=self.experiment_folder + 'dataset/eval.json')
        return eval

    def run_experiment(self, strategies=None):
        if strategies is None:
            strategies = [
                AgglomerativeHierarchicalClusteringWithNaiveSimrank,
                AgglomerativeHierarchicalClusteringWithDomainSimrank,
                DedupeMatcher
            ]

        # for each dataset version
        for dataset_config in self.dataset_configurations:
            self.init_experiment(dataset_config)

            # generate dataset
            self.generate_dataset(dataset_config)

            eval = {}
            for strategy in strategies:
                # run fusion using strategy
                eval[strategy] = self.run_fusion(matching_strategy=strategy)

            print(eval)

    def run_experiment_with_entity_inference(self, strategies=None):
        if strategies is None:
            strategies = []

        # for each dataset version
        for dataset_config in self.dataset_configurations:
            self.init_experiment(dataset_config)

            # generate dataset
            self.generate_dataset(dataset_config)

            eval = {}
            for strategy in strategies:
                # run fusion using strategy
                eval[strategy] = self.run_fusion(matching_strategy=strategy, infer_entities=True)

            print(eval)

    def run_cumulative_experiment(self):
        # dataset_versions = ['generic', 'high_synonym_chance', 'high_wrong_value_chance']  # test
        # dataset_versions = ['generic', 'high_typos', 'high_synonym_chance', 'high_wrong_value_chance']  # final_exp_1
        dataset_versions = ['generic']  # final_exp_2
        # dataset_versions = ['generic', 'high_typos', 'high_synonym_chance', 'high_wrong_value_chance', 'many_sources']
        # dataset_sizes = [50, 100]  # test
        # dataset_sizes = [25, 50, 100, 250]  # test
        # dataset_sizes = [25, 50, 100, 250, 500] # final_exp_1
        dataset_sizes = [25, 50, 100, 250] # final_exp_2
        matching_strategies = [AgglomerativeHierarchicalClusteringWithNaiveSimrank,
                               AgglomerativeHierarchicalClusteringWithDomainSimrank, DedupeMatcher]
        # matching_strategies = [DedupeMatcher, AgglomerativeHierarchicalClustering]
        metrics = ['accuracy', 'recall', 'precision', 'f1score', 'time']

        # for each dataset version (typos, synonyms etc)
        data = {}

        fig, axes = plt.subplots(nrows=len(dataset_versions), ncols=len(metrics), figsize=(36, 24))
        cols = [metric.replace('_', ' ').capitalize() for metric in metrics]
        rows = [version.replace('_', ' ').capitalize() for version in dataset_versions]

        for dataset_version_index, dataset_version in enumerate(dataset_versions):
            # for each dataset size (100, ..., 500 etc)
            for dataset_size in dataset_sizes:

                # generate dataset
                self.prepare_dataset_configs(version=dataset_version, num_entities=dataset_size)
                self.init_experiment(self.dataset_configurations[0])
                self.generate_dataset(self.dataset_configurations[0])

                # for each matching strategy (ahc, dedupe)
                for matching_strategy in matching_strategies:
                    start = time.time()
                    # eval = self.run_fusion(matching_strategy=matching_strategy)  # final_exp_1
                    eval = self.run_fusion(matching_strategy=matching_strategy, infer_entities=True)  # final_exp_2
                    end = time.time()
                    data.setdefault(dataset_version, {}).setdefault(dataset_size, {}).setdefault(
                        matching_strategy.__name__, eval)
                    data[dataset_version][dataset_size][matching_strategy.__name__]['time'] = end - start

            print(data)
            # when we have the result metrics per strategy/per size, plot for each evaluation metric
            # fig1 = plt.figure()
            # ax1 = fig1.add_subplot(111)
            for evaluation_metric_index, evaluation_metric in enumerate(metrics):
                x = dataset_sizes
                y_ahc_naive = [
                    data[dataset_version][size][AgglomerativeHierarchicalClusteringWithNaiveSimrank.__name__][
                        evaluation_metric] for
                    size in
                    data[dataset_version]
                ]
                y_ahc_domain = [
                    data[dataset_version][size][AgglomerativeHierarchicalClusteringWithDomainSimrank.__name__][
                        evaluation_metric] for
                    size in
                    data[dataset_version]
                ]

                y_ded = [
                    data[dataset_version][size][DedupeMatcher.__name__][evaluation_metric] for size in
                    data[dataset_version]
                ]

                # plot a graph with two lines, one per strategy, x axis: dataset size, y axis: eval. metric
                axes[dataset_version_index, evaluation_metric_index].plot(x, y_ahc_naive)
                axes[dataset_version_index, evaluation_metric_index].plot(x, y_ahc_domain)
                axes[dataset_version_index, evaluation_metric_index].plot(x, y_ded)
                axes[dataset_version_index, evaluation_metric_index].legend(
                    ['AHC Matcher w/ Naive Simrank', 'AHC Matcher w/ Domain Simrank', 'Dedupe Matcher'],
                    loc='upper left')

        for ax, col in zip(axes[0], cols):
            ax.set_title(col)

        for ax, row in zip(axes[:, 0], rows):
            ax.set_ylabel(row, rotation=0, size='large', labelpad=60)

        fig.tight_layout()
        plt.show()

        fig.savefig('cumulative_experiment_graphs_' + self.timestamp + '.png')
        print(data)
        with open('cumulative_experiment_data_' + self.timestamp + '.json', 'w') as f:
            json.dump(data, f)

    def run_experiment_with_fed_data(self):
        # dataset_versions = ['generic', 'high_typos', 'high_synonym_chance', 'high_wrong_value_chance', 'many_sources']
        dataset_versions = ['generic']
        # dataset_sizes = [50, 100, 200, 300, 400, 500]
        dataset_sizes = [25, 50, 100, 250]  # test
        matching_strategies = [DedupeMatcher, AgglomerativeHierarchicalClusteringWithNaiveSimrank]
        # metrics = ['accuracy', 'recall', 'precision', 'f1score', 'time']
        metrics = ['accuracy', 'recall', 'precision', 'f1score']

        # for each dataset version (typos, synonyms etc)
        data = {'generic':
            {
                25:
                    {
                        'AgglomerativeHierarchicalClusteringWithNaiveSimrank': {

                            'recall': 0.8448,
                            'precision': 0.8673,
                            'accuracy': 0.7481,
                            'f1score': 0.8559
                        },
                        'AgglomerativeHierarchicalClusteringWithDomainSimrank': {
                            'recall': 0.8448,
                            'precision': 0.8596,
                            'accuracy': 0.7424,
                            'f1score': 0.8522
                        },

                        'DedupeMatcher': {
                            'recall': 0.8707,
                            'precision': 0.9352,
                            'accuracy': 0.8211,
                            'f1score': 0.9018
                        }
                    },
                50:
                    {
                        'AgglomerativeHierarchicalClusteringWithNaiveSimrank': {
                            'recall': 0.8475,
                            'precision': 0.9175,
                            'accuracy': 0.7875,
                            'f1score': 0.8811,
                        },
                        'AgglomerativeHierarchicalClusteringWithDomainSimrank': {
                            'recall': 0.8520,
                            'precision': 0.9135,
                            'accuracy': 0.7884,
                            'f1score': 0.8817,
                        },
                        'DedupeMatcher': {
                            'recall': 0.8969,
                            'precision': 1.0000,
                            'accuracy': 0.8969,
                            'f1score': 0.9456,
                        }

                    },

                100:
                    {
                        'AgglomerativeHierarchicalClusteringWithNaiveSimrank': {
                            'recall': 0.8717,
                            'precision': 0.9072,
                            'accuracy': 0.8004,
                            'f1score': 0.8891,
                        },
                        'AgglomerativeHierarchicalClusteringWithDomainSimrank': {
                            'recall': 0.8543,
                            'precision': 0.9014,
                            'accuracy': 0.7813,
                            'f1score': 0.8772
                        },
                        'DedupeMatcher': {
                            'recall': 0.8609,
                            'precision': 0.9900,
                            'accuracy': 0.8534,
                            'f1score': 0.9209,
                        }
                    },
                250:
                    {
                        'AgglomerativeHierarchicalClusteringWithNaiveSimrank': {
                            'recall': 0.8498,
                            'precision': 0.8792,
                            'accuracy': 0.7610,
                            'f1score': 0.8643,
                        },
                        'AgglomerativeHierarchicalClusteringWithDomainSimrank': {
                            'recall': 0.8644,
                            'precision': 0.8919,
                            'accuracy': 0.7824,
                            'f1score': 0.8779,
                        },
                        'DedupeMatcher': {
                            'recall': 0.8541,
                            'precision': 0.9950,
                            'accuracy': 0.8504,
                            'f1score': 0.9192,
                        }
                    }

            }

        }

        fig, axes = plt.subplots(nrows=len(dataset_versions), ncols=len(metrics), figsize=(36, 8))
        cols = [metric.replace('_', ' ').capitalize() for metric in metrics]
        rows = [version.replace('_', ' ').capitalize() for version in dataset_versions]

        for dataset_version_index, dataset_version in enumerate(dataset_versions):

            for evaluation_metric_index, evaluation_metric in enumerate(metrics):
                x = dataset_sizes
                y_ahc_naive = [
                    data[dataset_version][size][AgglomerativeHierarchicalClusteringWithNaiveSimrank.__name__][
                        evaluation_metric] for
                    size in
                    data[dataset_version]
                ]
                y_ahc_domain = [
                    data[dataset_version][size][AgglomerativeHierarchicalClusteringWithDomainSimrank.__name__][
                        evaluation_metric] for
                    size in
                    data[dataset_version]
                ]

                y_ded = [
                    data[dataset_version][size][DedupeMatcher.__name__][evaluation_metric] for size in
                    data[dataset_version]
                ]

                # plot a graph with two lines, one per strategy, x axis: dataset size, y axis: eval. metric
                axes[evaluation_metric_index].plot(x, y_ahc_naive)
                axes[evaluation_metric_index].plot(x, y_ahc_domain)
                axes[evaluation_metric_index].plot(x, y_ded)
                axes[evaluation_metric_index].legend(
                    ['AHC Matcher w/ Naive Simrank', 'AHC Matcher w/ Domain Simrank', 'Dedupe Matcher'],
                    loc='upper left')

        for ax, col in zip(axes, cols):
            ax.set_title(col)

        for ax, row in zip(axes, rows):
            ax.set_ylabel(row, rotation=0, size='large', labelpad=60)

        fig.tight_layout()
        plt.show()
        fig.savefig('foo.png')

        with open('cumulative_experiment_data_' + self.timestamp + '.json', 'w') as f:
            json.dump(data, f)


if __name__ == "__main__":
    exp = FusionExperiment()
    # exp.run_fusion(AgglomerativeHierarchicalClusteringWithNaiveSimrank)
    # exp.run_experiment()
    # exp.run_experiment_with_entity_inference()
    exp.run_cumulative_experiment()
    # exp.run_experiment_with_fed_data()
