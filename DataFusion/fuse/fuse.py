import json
import os
import logging
import typing
from fuzzywuzzy import fuzz

from dataengine import DataEngine
import utils.logger as log
from evaluation import Evaluation
from utils import WordVectors
from matching.dedupe import DedupeMatcher
from matching import MatchingStrategy

word_vectors = WordVectors.getInstance()

# Define arguments and flags for Fuse
# Define arguments for HoloClean
arguments = [
    (('-u', '--db_user'),
     {'metavar': 'DB_USER',
      'dest': 'db_user',
      'default': 'fuseUser',
      'type': str,
      'help': 'User for DB used to persist state'}),
    (('-p', '--password', '--pass'),
     {'metavar': 'PASSWORD',
      'dest': 'db_pwd',
      'default': 'abcd1234',
      'type': str,
      'help': 'Password for DB used to persist state'}),
    (('-h', '--host'),
     {'metavar': 'HOST',
      'dest': 'db_host',
      'default': 'localhost',
      'type': str,
      'help': 'Host for DB used to persist state'}),
    (('-d', '--database'),
     {'metavar': 'DATABASE',
      'dest': 'db_name',
      'default': 'fuse',
      'type': str,
      'help': 'Name of DB used to persist state'}),
    (('--matcher_function'),
     {'dest': 'matcher_function',
      'default': lambda a, b: fuzz.token_sort_ratio(a, b) > 40,
      'type': typing.Callable,
      'help': 'Function for string matching.'}),
    (('--matcher_function2'),
     {'dest': 'matcher_function2',
      'default': lambda a, b: word_vectors.similarity(a, b) > 0.5,
      'type': typing.Callable,
      'help': 'Function for string matching.'})
]
flags = [
    (('-v', '--verbose'),
     {'default': False,
      'dest': 'verbose',
      'action': 'store_true',
      'help': 'Enables detailed logging when acivated.'}),
    (('--global_weights',),
     {'default': False,
      'dest': 'global_weights',
      'action': 'store_true',
      'help': 'Uses a single weight per source instead of per source/per vertical.'})
]


class Fuse:
    """
    Main Entry point for Fuse. Creates a Fuse Data Engine
    and initializes the dataengine.
    """

    def __init__(self, **kwargs):
        # Initialize default execution arguments
        arg_defaults = {}
        for arg, opts in arguments:
            if 'directory' in arg[0]:
                arg_defaults['directory'] = opts['default']
            else:
                arg_defaults[opts['dest']] = opts['default']

        # Initialize default execution flags
        for arg, opts in flags:
            arg_defaults[opts['dest']] = opts['default']

        for key in kwargs:
            arg_defaults[key] = kwargs[key]

        # Initialize additional arguments
        for (arg, default) in arg_defaults.items():
            setattr(self, arg, kwargs.get(arg, default))

        # Initialize global logger
        logname = kwargs.get('home_dir', '') + 'global_log.log'
        if self.verbose:
            self.logger = log.setup_logger(logname, logname, logging.INFO)
        else:
            self.logger = log.setup_logger(logname, logname, logging.ERROR)
        # Initialize data engine
        self.dataengine = DataEngine(self)
        # Initialize dataset object
        self.dataset = None

        # Initialize session dictionary
        self.active_sessions = {}
        self.session_id = 0

    def get_session(self, name):
        newSession = Session(name, self.session_id, self)
        self.active_sessions[name] = newSession
        self.session_id += 1
        return newSession


class Session:
    """
    A Session object controls the entire fusion task for an input dataset.
    Fuse allows users to start multiple sessions.
    """

    def __init__(self, name, id, fuse_env):
        self.name = name
        self.id = id
        self.env = fuse_env
        self.dataset = None
        self.eval = None
        # Initialize logger
        logname = getattr(fuse_env, 'home_dir', '') + 'session_' + self.name + '.log'
        if self.env.verbose:
            self.logger = log.setup_logger(self.name, logname, logging.INFO)
        else:
            self.logger = log.setup_logger(self.name, logname, logging.ERROR)

    def ingest(self, filepath, name, matching_strategy: MatchingStrategy = DedupeMatcher, persist=False):
        """
        Ingest data from a JSON file and generate a new Dataset object.
        :param filepath: String literal indicating the path to a JSON file.
        :param persist:
        :param name:
        :param matching_strategy:
        :return: A new Dataset object
        """
        # Check that the file is in JSON format
        ext = os.path.splitext(filepath)[-1].lower()
        if ext == '.json':
            self.dataset = self.env.dataengine.ingest_data(filepath, name, matching_strategy, persist)
        else:
            self.logger.error("Wrong file extension. Only JSON files supported.")
        return self.dataset

    def get_dataset(self):
        """
        Get the Dataset object associated with the Session
        :return: A Dataset object
        """
        if self.dataset:
            return self.dataset
        else:
            self.logger.error("Session " + self.name + " does not contain a dataset.")
            return None

    def train_matchers(self, matched_facts=None):
        """
        Train a matcher for each entity attribute in the associated dataset.
        :param matched_facts: A dict containing pre-matched facts for each vertical (keys are vertical names)
                              as accepted by dedupe.Dedupe.mark_pairs or a string filepath for such a json file
                              see https://docs.dedupe.io/en/latest/API-documentation.html#dedupe.Dedupe.mark_pairs
        :return: None
        """
        if isinstance(matched_facts, str):
            with open(matched_facts) as f:
                matched_facts = json.load(f)

        self.dataset.train_matchers(matched_facts)

    def match_observations(self):
        """
        Operate over the facts reported by different data sources
        and identify clusters of matching observations.
        :return: None
        """
        if self.dataset:
            self.dataset.match_observations()
        else:
            self.logger.error("Session " + self.name + " does not contain a dataset.")

    def find_true_clusters(self):
        """
        Perform truth discovery at the cluster level.
        :return: None
        """
        if self.dataset:
            return self.dataset.find_true_clusters()
        else:
            self.logger.error("Session " + self.name + " does not contain a dataset.")

    def find_true_facts(self, persist=False, no_weights=False):
        """
        Perform data fusion within each true cluster.
        :param: persist: If set to `True` all inferred facts will be persisted in the DB.
        :param: no_weights: If set to `False` simple majority voting is used.
        :return: None
        """
        if self.dataset:
            facts = self.dataset.find_true_facts(no_weights)
            if persist:
                tf_df = self.dataset.get_true_facts()
                self.env.dataengine.persist_frame(tf_df, "true_facts")
            return facts
        else:
            self.logger.error("Session " + self.name + " does not contain a dataset.")

    def evaluate(self, grd_path, inferred_facts, persist=False):
        """
        Evaluate inferred facts.
        :return: None.
        """
        self.eval = Evaluation(self.env, grd_path, inferred_facts)
        evaluation_scores = self.eval.evaluateDataFusion()
        if persist:
            self.eval.persist_dicts()
        return evaluation_scores
