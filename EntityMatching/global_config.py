import general_str_sim
import custom_similarity_funcs

# default program parameters
default_program_parameters = {
    "name_sim_func": general_str_sim.cosine_similarity,
    "location_sim_func": general_str_sim.cosine_similarity,
    "education_degree_sim_func": general_str_sim.cosine_similarity,
    "education_university_sim_func": general_str_sim.cosine_similarity,
    "education_year_sim_func": general_str_sim.cosine_similarity,
    "working_experience_title_sim_func": general_str_sim.cosine_similarity,
    "working_experience_company_sim_func": general_str_sim.cosine_similarity,
    "working_experience_years_sim_func": general_str_sim.cosine_similarity,
    "relationship_R_sim_func": custom_similarity_funcs.relationship_similarity_v1,
    "relationship_similarity_threshold": 0.6,
    "algorithm_threshold": 0.6,
    "constant_a": 0.7
}


def set_config(config):
    global default_program_parameters
    default_program_parameters = config
