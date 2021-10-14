import os

import dedupe
from dedupe.core import BlockingError
from matching import MatchingStrategy


class DedupeMatcher(MatchingStrategy):
    def __init__(self, dataset, attr, matcher_home='', num_cores=0, simple=False):
        """
        Constructor for a fact matcher.
        :param dataset: An instance of the Dataset class.
        :param attr: The entity attribute associated with the matcher.
        """
        self.dataset = dataset
        self.attr = attr
        self.attributes = dataset.ent_attr_schema[attr]
        self.variables = self._form_variables()
        self.home_dir = matcher_home
        self.static = False
        self.simple = simple
        self.num_cores = num_cores
        if self.simple:
            self.deduper = SimpleDedupe(self.dataset, self.attr)
        else:
            self.deduper = self._get_deduper()

    @property
    def model_file_path(self):
        model_name = 'fuse_model_' + self.attr
        model_dir = self.home_dir + 'dedupe_models'
        return model_dir + '/' + model_name

    @property
    def training_file_path(self):
        td_name = 'fuse_train_' + self.attr + '.json'
        td_dir = self.home_dir + 'dedupe_data'
        return td_dir + '/' + td_name

    def _form_variables(self):
        """
        Form the necessary schema to be given as input
        to dedupe.
        :return:
        """
        variables = []
        for attr in self.attributes:
            new_field = {'field': attr, 'type': 'String', 'has missing': True}
            variables.append(new_field)
        return variables

    def _get_deduper(self):
        """
        A function to check if a trained model already exists.
        :return: Returns StaticDeduper if the model exists;
                 A to-be-trained Deduper otherwise.
        """
        if os.path.isfile(self.model_file_path):
            return self._get_static_deduper()
        else:
            return dedupe.Dedupe(self.variables, num_cores=self.num_cores)

    def _get_static_deduper(self):
        self.static = True
        with open(self.model_file_path, 'rb') as f:
            return dedupe.StaticDedupe(f, num_cores=self.num_cores)

    def _form_dataset(self):
        """
        Form dataset based on associated collections
        :return:
        """
        records = {}
        fid = 1
        for col in self.dataset.observed_fact_collections[self.attr]:
            for fact in col.facts:
                records[fid] = fact.normalized
                fid += 1
        return records

    def _store_training_file(self):
        """
        A function to store training data.
        :return: None.
        """
        td_dir = os.path.dirname(self.training_file_path)
        if not os.path.exists(td_dir):
            os.makedirs(td_dir)
        with open(self.training_file_path, 'w') as f:
            self.deduper.write_training(f)

    def _load_training_file(self):
        """
        A function to load training data.
        """
        if os.path.exists(self.training_file_path):
            with open(self.training_file_path, 'r') as f:
                self.deduper._read_training(f)

    def _save_model(self):
        """
        A function to store the trained dedupe model.
        """
        model_dir = os.path.dirname(self.model_file_path)
        if not os.path.exists(model_dir):
            os.makedirs(model_dir)
        with open(self.model_file_path, 'wb') as f:
            self.deduper.write_settings(f)

    def train(self, **kwargs):
        """
        If deduper is an instance of Dedupe: train the dedupe classifier.
        We use the dedupe active learning console. If deduper is an instance
        of StaticDeDupe of SimpleDedupe skip training.
        :param matched_facts: A dict of pre-matched facts as accepted by dedupe.Dedupe.mark_pairs
                      see https://docs.dedupe.io/en/latest/API-documentation.html#dedupe.Dedupe.mark_pairs
        :param samples: The number of samples per collection.
        :return:
        """
        matched_facts = kwargs.get("prematched_facts", None)
        samples = kwargs.get("samples", 100)
        if self.static or self.simple:
            return
        else:
            # Check if training data already exists.
            self._load_training_file()
            if matched_facts is not None:
                self.deduper.mark_pairs(matched_facts)

            # Collect more training data and store it.
            records = self._form_dataset()
            self.deduper._sample(records, sample_size=samples)
            # self.deduper._sample(records)

            if matched_facts is None:
                dedupe.console_label(self.deduper)
            self._store_training_file()
            self.deduper.train()
            self._save_model()
            # XXX: switch to static deduper because the Dedupe() instance fails
            # to create good clusters for some reason with the following error:
            # ValueError: shapes (13,6) and (2,) not aligned: 6 (dim 1) != 2 (dim 0)
            # See get_matches().
            self.deduper = self._get_static_deduper() # TODO : test not switching

    def get_matches(self, **kwargs):
        facts = kwargs.get("fact_records")
        threshold = 0.5
        matches = []
        all_facts = list(facts.keys())
        if len(facts) == 1:
            fid = list(facts.keys())[0]
            matches.append([fid])
            return matches
        try:
            duplicates = self.deduper.partition(facts, threshold=threshold)
        except BlockingError as e:
            # TODO: if verbose log
            # self.dataset.env.logger.log(e)
            match = facts.keys()
            matches.append(match)
        except Exception as ex:
            # TODO: fix problem with empty strings.
            # What does the above mean? Is this what causes the following error?
            # ValueError: shapes (13,6) and (2,) not aligned: 6 (dim 1) != 2 (dim 0)
            # For now, this happens only after training, so we assume it is
            # a bug with Dedupe().
            # As a workaround, after training and saving the model, we switch
            # to StaticDedupe() (see train()). So we do not expect any
            # exceptions here, so raise.
            # raise
            match = facts.keys()
            matches.append(match)
        else:
            for cand in duplicates:
                match = list(cand[0])
                matches.append(match)
                for f in match:
                    all_facts.remove(f)
            for rem_f in all_facts:
                matches.append([rem_f])
        return matches

    def match(self, fact1, fact2, threshold=0.5):
        d = {}
        d[0] = fact1.normalized
        d[1] = fact2.normalized
        try:
            duplicates = self.deduper.match(d)
            score = duplicates[0][1][0]
            if score > threshold:
                return 1.0
        except BlockingError as e:
            # TODO: if verbose log
            # print("Trying to match:", d)
            # print("No block found. Returning non-match.")
            return 0.0


class SimpleDedupe:
    def __init__(self, dataset, attr):
        """
        Constructor for a simple deduper.
        :param dataset: An instance of the Dataset class.
        :param attr: The entity attribute associated with the matcher.
        """
        self.dataset = dataset
        self.attr = attr
        self.attributes = dataset.ent_attr_schema[attr]

    def train(self):
        pass

    def match(self, facts):
        """
        Return the similarity between to facts associated with the
        entity attribute of the matcher.
        :param facts: A dictionary with normalized attribute values
        :return: A matching score
        """
        f1 = facts[0]
        f2 = facts[1]
        f1attrs = f1.keys()
        f2attrs = f2.keys()
        if set(f1attrs) != set(f2attrs):
            return 0.0
        for attr in f1attrs:
            if f1.normalized[attr] != f2.normalized[attr]:
                return False
        return 1.0
