import io
import logging
import os
import sys
import time

# constants
observed_facts_file_path = 'C:/Users/Iasonas/PycharmProjects/EntityMatching_Thesis/datasets/observed_facts.json'

verbose_file = True
verbose_console = False
experiment = True
experiment_with_combinations = True

# Logger names
LOG = 'log'
EXP_LOG = 'exp_log'
CONSOLE = 'console'
EXP_CONSOLE = 'exp_console'

# global log
# log = io.StringIO()
# exp_log = io.StringIO()
# console = sys.stdout
# exp_console = sys.stdout
# console = io.StringIO()
# exp_console = io.StringIO()


# global variables
observed_data = []
correct_groups_of_facts = []
