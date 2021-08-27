import general_str_sim
import custom_similarity_funcs

# default program parameters
default_program_parameters = {
    # functions
    "name_sim_func": general_str_sim.cosine_similarity,
    "location_sim_func": general_str_sim.cosine_similarity,
    "education_degree_sim_func": general_str_sim.cosine_similarity,
    "education_university_sim_func": general_str_sim.cosine_similarity,
    "education_year_sim_func": general_str_sim.cosine_similarity,
    "working_experience_title_sim_func": general_str_sim.cosine_similarity,
    "working_experience_company_sim_func": general_str_sim.cosine_similarity,
    "working_experience_years_sim_func": general_str_sim.cosine_similarity,
    "relationship_R_sim_func": custom_similarity_funcs.relationship_similarity_v1,
    # constants
    "relationship_similarity_threshold": 0.6,
    "algorithm_threshold": 0.45,
    "constant_a": 0.7,
    "record_similarity_name_weight": 0.1,
    "record_similarity_location_weight": 0.3,
    "record_similarity_education_weight": 0.3,
    "record_similarity_working_exp_weight": 0.3,
    "education_tuple_degree_weight": 0.3,
    "education_tuple_university_weight": 0.5,
    "education_tuple_year_weight": 0.2,
    "working_exp_tuple_title_weight": 0.3,
    "working_exp_tuple_company_weight": 0.5,
    "working_exp_tuple_years_weight": 0.2,
}


def set_config(config):
    global default_program_parameters
    default_program_parameters = config
