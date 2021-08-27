# constants
# observed_facts_file_path = 'C:/Users/i.christofilakis/Documents/entity-matching-thesis/datasets/observed_facts.json'
observed_facts_file_path = '../datasets/observed_facts_100.json'

verbose_file = True
analytical_output = False
verbose_console = True
experiment = False
experiment_with_combinations = False
analytical_evaluation = False
evaluation_only = False

# Logger names
LOG = 'log'
EXP_LOG = 'exp_log'
CONSOLE = 'console'
EXP_CONSOLE = 'exp_console'

# global variables
observed_data = []
correct_groups_of_facts = []
relationship_R = {}
record_to_cluster = {}
record_similarity_cache = {}
relationship_similarity_cache = {}
