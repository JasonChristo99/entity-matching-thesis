from general_str_sim import *
from custom_similarity_funcs import *

# default program parameters
program_parameters = {
    "name_sim_func": cosine_similarity,
    "location_sim_func": cosine_similarity,
    "education_degree_sim_func": cosine_similarity,
    "education_university_sim_func": cosine_similarity,
    "education_year_sim_func": cosine_similarity,
    "working_experience_title_sim_func": cosine_similarity,
    "working_experience_company_sim_func": cosine_similarity,
    "working_experience_years_sim_func": cosine_similarity,
    "relationship_R_sim_func": relationship_similarity_v1,
    "relationship_similarity_threshold": 0.6,
    "algorithm_threshold": 0.6,
    "constant_a": 0.7
}


def set_config(config):
    global program_parameters
    program_parameters = config
