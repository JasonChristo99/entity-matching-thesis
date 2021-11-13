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

    def run_fusion(self, matching_strategy):
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
                                      matching_strategy)
        fuse_session.train_matchers(self.experiment_folder + 'dataset/matched.json')
        fuse_session.match_observations()
        tr_clusters = fuse_session.find_true_clusters()
        true_facts = fuse_session.find_true_facts(persist=True, no_weights=False)
        eval = fuse_session.evaluate(inferred_facts=true_facts, grd_path=self.experiment_folder + 'dataset/eval.json')
        return eval

    def run_experiment(self):
        # for each dataset version
        for dataset_config in self.dataset_configurations:
            self.init_experiment(dataset_config)

            # generate dataset
            self.generate_dataset(dataset_config)

            # run fusion using new matcher
            eval2 = self.run_fusion(matching_strategy=AgglomerativeHierarchicalClusteringWithNaiveSimrank)

            # run fusion using old matcher
            eval1 = self.run_fusion(matching_strategy=DedupeMatcher)

            print({'ahc': eval2})
            print({'dedupe': eval1})

    def run_cumulative_experiment(self):
        # dataset_versions = ['generic', 'high_synonym_chance', 'high_wrong_value_chance']  # test
        dataset_versions = ['generic', 'high_typos', 'high_synonym_chance', 'high_wrong_value_chance']  # test
        # dataset_versions = ['generic', 'high_typos', 'high_synonym_chance', 'high_wrong_value_chance', 'many_sources']
        # dataset_sizes = [50, 100]  # test
        dataset_sizes = [25, 50, 100, 250]  # test
        # dataset_sizes = [25, 50, 100, 250, 500]
        matching_strategies = [AgglomerativeHierarchicalClusteringWithNaiveSimrank,
                               AgglomerativeHierarchicalClusteringWithDomainSimrank, DedupeMatcher]  # test
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
                    eval = self.run_fusion(matching_strategy=matching_strategy)
                    end = time.time()
                    data.setdefault(dataset_version, {}).setdefault(dataset_size, {}).setdefault(
                        matching_strategy.__name__, eval)
                    data[dataset_version][dataset_size][matching_strategy.__name__]['time'] = end - start

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
        with open('cumulative_experiment_data_' + self.timestamp + '.json', 'w') as f:
            json.dump(data, f)

    def run_experiment_2_test(self):
        dataset_versions = ['generic', 'high_typos', 'high_synonym_chance', 'high_wrong_value_chance', 'many_sources']
        # dataset_versions = ['generic', 'high_typos']
        # dataset_sizes = [50, 100, 200, 300, 400, 500]
        dataset_sizes = [20, 50, 100, 150]  # test
        matching_strategies = [DedupeMatcher, AgglomerativeHierarchicalClusteringWithNaiveSimrank]
        metrics = ['accuracy', 'recall', 'precision', 'f1score', 'time']

        # for each dataset version (typos, synonyms etc)
        data = {'generic': {20: {
            'DedupeMatcher': {'recall': 0.8791208791208791, 'precision': 1.0, 'accuracy': 0.8791208791208791,
                              'f1score': 0.935672514619883, 'time': 74.30176615715027},
            'AgglomerativeHierarchicalClustering': {'recall': 0.9340659340659341, 'precision': 0.9770114942528736,
                                                    'accuracy': 0.9139784946236559, 'f1score': 0.9550561797752809,
                                                    'time': 2.7278947830200195}}, 50: {
            'DedupeMatcher': {'recall': 0.8968609865470852, 'precision': 1.0, 'accuracy': 0.8968609865470852,
                              'f1score': 0.9456264775413712, 'time': 165.27150988578796},
            'AgglomerativeHierarchicalClustering': {'recall': 0.9147982062780269, 'precision': 0.9855072463768116,
                                                    'accuracy': 0.9026548672566371, 'f1score': 0.9488372093023256,
                                                    'time': 8.675016164779663}}, 100: {
            'DedupeMatcher': {'recall': 0.851063829787234, 'precision': 1.0, 'accuracy': 0.851063829787234,
                              'f1score': 0.9195402298850576, 'time': 370.6461546421051},
            'AgglomerativeHierarchicalClustering': {'recall': 0.9170212765957447, 'precision': 0.9930875576036866,
                                                    'accuracy': 0.9112050739957717, 'f1score': 0.9535398230088495,
                                                    'time': 27.244333028793335}}, 150: {
            'DedupeMatcher': {'recall': 0.8333333333333334, 'precision': 1.0, 'accuracy': 0.8333333333333334,
                              'f1score': 0.9090909090909091, 'time': 568.0700197219849},
            'AgglomerativeHierarchicalClustering': {'recall': 0.9111111111111111, 'precision': 0.9879518072289156,
                                                    'accuracy': 0.9010989010989011, 'f1score': 0.9479768786127168,
                                                    'time': 61.013402700424194}}}, 'high_typos': {20: {
            'DedupeMatcher': {'recall': 0.8947368421052632, 'precision': 1.0, 'accuracy': 0.8947368421052632,
                              'f1score': 0.9444444444444444, 'time': 73.49786019325256},
            'AgglomerativeHierarchicalClustering': {'recall': 0.9157894736842105, 'precision': 1.0,
                                                    'accuracy': 0.9157894736842105, 'f1score': 0.956043956043956,
                                                    'time': 3.403637409210205}}, 50: {
            'DedupeMatcher': {'recall': 0.8733624454148472, 'precision': 1.0, 'accuracy': 0.8733624454148472,
                              'f1score': 0.9324009324009325, 'time': 162.01553440093994},
            'AgglomerativeHierarchicalClustering': {'recall': 0.8820960698689956, 'precision': 0.9950738916256158,
                                                    'accuracy': 0.8782608695652174, 'f1score': 0.9351851851851851,
                                                    'time': 10.28048038482666}}, 100: {
            'DedupeMatcher': {'recall': 0.8849557522123894, 'precision': 1.0, 'accuracy': 0.8849557522123894,
                              'f1score': 0.9389671361502347, 'time': 314.65099477767944},
            'AgglomerativeHierarchicalClustering': {'recall': 0.8871681415929203, 'precision': 0.9950372208436724,
                                                    'accuracy': 0.8832599118942731, 'f1score': 0.9380116959064329,
                                                    'time': 30.488996744155884}}, 150: {
            'DedupeMatcher': {'recall': 0.8426966292134831, 'precision': 1.0, 'accuracy': 0.8426966292134831,
                              'f1score': 0.9146341463414633, 'time': 514.6359732151031},
            'AgglomerativeHierarchicalClustering': {'recall': 0.8960674157303371, 'precision': 0.9953198127925117,
                                                    'accuracy': 0.8923076923076924, 'f1score': 0.943089430894309,
                                                    'time': 74.16370224952698}}}, 'high_synonym_chance': {20: {
            'DedupeMatcher': {'recall': 0.851063829787234, 'precision': 1.0, 'accuracy': 0.851063829787234,
                              'f1score': 0.9195402298850576, 'time': 75.22728776931763},
            'AgglomerativeHierarchicalClustering': {'recall': 0.9148936170212766, 'precision': 0.9885057471264368,
                                                    'accuracy': 0.9052631578947369, 'f1score': 0.9502762430939227,
                                                    'time': 2.989353895187378}}, 50: {
            'DedupeMatcher': {'recall': 0.8403361344537815, 'precision': 1.0, 'accuracy': 0.8403361344537815,
                              'f1score': 0.91324200913242, 'time': 165.72637248039246},
            'AgglomerativeHierarchicalClustering': {'recall': 0.8697478991596639, 'precision': 0.9951923076923077,
                                                    'accuracy': 0.8661087866108786, 'f1score': 0.9282511210762332,
                                                    'time': 10.784807682037354}}, 100: {
            'DedupeMatcher': {'recall': 0.8368200836820083, 'precision': 1.0, 'accuracy': 0.8368200836820083,
                              'f1score': 0.9111617312072893, 'time': 335.55313777923584},
            'AgglomerativeHierarchicalClustering': {'recall': 0.893305439330544, 'precision': 0.9861431870669746,
                                                    'accuracy': 0.8822314049586777, 'f1score': 0.9374313940724479,
                                                    'time': 29.424325942993164}}, 150: {
            'DedupeMatcher': {'recall': 0.8620689655172413, 'precision': 1.0, 'accuracy': 0.8620689655172413,
                              'f1score': 0.9259259259259259, 'time': 496.50176215171814},
            'AgglomerativeHierarchicalClustering': {'recall': 0.8850574712643678, 'precision': 0.9919484702093397,
                                                    'accuracy': 0.8787446504992867, 'f1score': 0.935459377372817,
                                                    'time': 64.89614915847778}}}, 'high_wrong_value_chance': {20: {
            'DedupeMatcher': {'recall': 0.8222222222222222, 'precision': 1.0, 'accuracy': 0.8222222222222222,
                              'f1score': 0.9024390243902439, 'time': 61.880882024765015},
            'AgglomerativeHierarchicalClustering': {'recall': 0.8555555555555555, 'precision': 1.0,
                                                    'accuracy': 0.8555555555555555, 'f1score': 0.9221556886227544,
                                                    'time': 2.420032024383545}}, 50: {
            'DedupeMatcher': {'recall': 0.8583690987124464, 'precision': 1.0, 'accuracy': 0.8583690987124464,
                              'f1score': 0.9237875288683602, 'time': 132.58502864837646},
            'AgglomerativeHierarchicalClustering': {'recall': 0.7296137339055794, 'precision': 0.9826589595375722,
                                                    'accuracy': 0.7203389830508474, 'f1score': 0.8374384236453202,
                                                    'time': 8.913658380508423}}, 100: {
            'DedupeMatcher': {'recall': 0.8438818565400844, 'precision': 1.0, 'accuracy': 0.8438818565400844,
                              'f1score': 0.9153318077803204, 'time': 311.6573829650879},
            'AgglomerativeHierarchicalClustering': {'recall': 0.820675105485232, 'precision': 0.9823232323232324,
                                                    'accuracy': 0.8087318087318087, 'f1score': 0.8942528735632185,
                                                    'time': 28.122311115264893}}, 150: {
            'DedupeMatcher': {'recall': 0.8583690987124464, 'precision': 1.0, 'accuracy': 0.8583690987124464,
                              'f1score': 0.9237875288683602, 'time': 451.4672966003418},
            'AgglomerativeHierarchicalClustering': {'recall': 0.7410586552217453, 'precision': 0.9810606060606061,
                                                    'accuracy': 0.7306064880112835, 'f1score': 0.8443357783211084,
                                                    'time': 50.999128341674805}}}, 'many_sources': {20: {
            'DedupeMatcher': {'recall': 0.8791208791208791, 'precision': 1.0, 'accuracy': 0.8791208791208791,
                              'f1score': 0.935672514619883, 'time': 294.9292986392975},
            'AgglomerativeHierarchicalClustering': {'recall': 0.989010989010989, 'precision': 1.0,
                                                    'accuracy': 0.989010989010989, 'f1score': 0.994475138121547,
                                                    'time': 6.417567729949951}}, 50: {
            'DedupeMatcher': {'recall': 0.8368200836820083, 'precision': 1.0, 'accuracy': 0.8368200836820083,
                              'f1score': 0.9111617312072893, 'time': 819.5161745548248},
            'AgglomerativeHierarchicalClustering': {'recall': 0.9665271966527197, 'precision': 1.0,
                                                    'accuracy': 0.9665271966527197, 'f1score': 0.9829787234042554,
                                                    'time': 28.796324729919434}}, 100: {
            'DedupeMatcher': {'recall': 0.8565310492505354, 'precision': 1.0, 'accuracy': 0.8565310492505354,
                              'f1score': 0.922722029988466, 'time': 1687.778180360794},
            'AgglomerativeHierarchicalClustering': {'recall': 0.9721627408993576, 'precision': 0.9978021978021978,
                                                    'accuracy': 0.9700854700854701, 'f1score': 0.9848156182212582,
                                                    'time': 81.97229838371277}}, 150: {
            'DedupeMatcher': {'recall': 0.8438818565400844, 'precision': 1.0, 'accuracy': 0.8438818565400844,
                              'f1score': 0.9153318077803204, 'time': 3085.722060441971},
            'AgglomerativeHierarchicalClustering': {'recall': 0.9507735583684951, 'precision': 0.9926578560939795,
                                                    'accuracy': 0.9441340782122905, 'f1score': 0.971264367816092,
                                                    'time': 202.55817914009094}}}}

        fig, axes = plt.subplots(nrows=len(dataset_versions), ncols=len(metrics), figsize=(36, 24))
        cols = [metric.replace('_', ' ').capitalize() for metric in metrics]
        rows = [version.replace('_', ' ').capitalize() for version in dataset_versions]

        for dataset_version_index, dataset_version in enumerate(dataset_versions):

            for evaluation_metric_index, evaluation_metric in enumerate(metrics):
                x = dataset_sizes
                y_ahc = [
                    data[dataset_version][size][AgglomerativeHierarchicalClusteringWithNaiveSimrank.__name__][
                        evaluation_metric] for
                    size in
                    data[dataset_version]
                ]

                y_ded = [
                    data[dataset_version][size][DedupeMatcher.__name__][evaluation_metric] for size in
                    data[dataset_version]
                ]

                # plot a graph with two lines, one per strategy, x axis: dataset size, y axis: eval. metric
                axes[dataset_version_index, evaluation_metric_index].plot(x, y_ahc)
                axes[dataset_version_index, evaluation_metric_index].plot(x, y_ded)
                axes[dataset_version_index, evaluation_metric_index].legend(['AHC Matcher', 'Dedupe Matcher'],
                                                                            loc='upper left')

        for ax, col in zip(axes[0], cols):
            ax.set_title(col)

        for ax, row in zip(axes[:, 0], rows):
            ax.set_ylabel(row, rotation=0, size='large', labelpad=60)

        fig.tight_layout()
        plt.show()
        fig.savefig('foo.png')

        # with open('cumulative_experiment_data_' + self.timestamp + '.json', 'w') as f:
        #     json.dump(data, f)


if __name__ == "__main__":
    exp = FusionExperiment()
    # exp.run_experiment()
    exp.run_cumulative_experiment()
    # exp.run_experiment_2_test()
