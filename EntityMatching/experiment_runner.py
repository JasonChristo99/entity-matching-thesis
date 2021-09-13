import itertools

from EntityMatching.EntityMatcher import main
from EntityMatching import printer, global_vars, custom_similarity_funcs, general_str_sim


def run_experiment():
    experiment_configurations = [
        {
            # params
            "input_file_path": "../datasets/observed_facts_400.json",
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
            "algorithm_threshold": 0.28,
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
    ]

    for experiment_config in experiment_configurations:
        printer.log('Config:', "dataset", experiment_config["input_file_path"], "relationship_similarity_threshold=",
                    experiment_config["relationship_similarity_threshold"],
                    "algorithm_threshold=", experiment_config["algorithm_threshold"], "constant_a=",
                    experiment_config["constant_a"], destinations=[global_vars.EXP_LOG, global_vars.EXP_CONSOLE])

        main(experiment_config)


def run_experiment_auto_combinations_dataset_versions():
    experiment_configurations: []

    # to run the experiment, either use automatic combination of parameters, or a hardcoded bunch of configurations
    parameter_combinations = itertools.product(
        [
            # {"input_file_path": "../datasets/observed_facts_100.json"},
            {"input_file_path": "../datasets/observed_facts_400_v1.json"},
            {"input_file_path": "../datasets/observed_facts_400_v2.json"},
            {"input_file_path": "../datasets/observed_facts_400_v3.json"},
            {"input_file_path": "../datasets/observed_facts_400_v4.json"},
            {"input_file_path": "../datasets/observed_facts_400_v5.json"},
        ],
        [
            {
                "relationship_similarity_threshold": 0.6,
                "algorithm_threshold": 0.28,
                "constant_a": 0.7
            },
            {
                "relationship_similarity_threshold": 0.5,
                "algorithm_threshold": 0.26,
                "constant_a": 0.5
            },
            {
                "relationship_similarity_threshold": 0.4,
                "algorithm_threshold": 0.28,
                "constant_a": 0.4
            },
            {
                "relationship_similarity_threshold": 0.65,
                "algorithm_threshold": 0.27,
                "constant_a": 0.55
            },
            {
                "relationship_similarity_threshold": 0.55,
                "algorithm_threshold": 0.28,
                "constant_a": 0.65
            }
            # {
            #     "relationship_similarity_threshold": 0.55,
            #     "algorithm_threshold": 0.26,
            #     "constant_a": 0.55
            # },
            # {
            #     "relationship_similarity_threshold": 0.55,
            #     "algorithm_threshold": 0.28,
            #     "constant_a": 0.6
            # }
        ]
    )
    constant_part_of_the_config = {
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

    experiment_configurations = [
        {**c[0], **c[1], **constant_part_of_the_config} for c in parameter_combinations
    ]

    for experiment_config in experiment_configurations:
        printer.log('Config:', "dataset", experiment_config["input_file_path"], "relationship_similarity_threshold=",
                    experiment_config["relationship_similarity_threshold"],
                    "algorithm_threshold=", experiment_config["algorithm_threshold"], "constant_a=",
                    experiment_config["constant_a"], destinations=[global_vars.EXP_LOG, global_vars.EXP_CONSOLE])

        main(experiment_config)


def run_experiment_auto_combinations_different_datasets():
    experiment_configurations: []

    # to run the experiment, either use automatic combination of parameters, or a hardcoded bunch of configurations
    parameter_combinations = itertools.product(
        [
            {"input_file_path": "../datasets/observed_facts_40.json"},
            {"input_file_path": "../datasets/observed_facts_100.json"},
            {"input_file_path": "../datasets/observed_facts_200.json"},
            {"input_file_path": "../datasets/observed_facts_400.json"},
            {"input_file_path": "../datasets/observed_facts_1000.json"},
        ],
        [
            {
                "relationship_similarity_threshold": 0.6,
                "algorithm_threshold": 0.28,
                "constant_a": 0.7
            },
            # {
            #     "relationship_similarity_threshold": 0.55,
            #     "algorithm_threshold": 0.28,
            #     "constant_a": 0.65
            # }
        ]
    )
    constant_part_of_the_config = {
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

    experiment_configurations = [
        {**c[0], **c[1], **constant_part_of_the_config} for c in parameter_combinations
    ]

    for experiment_config in experiment_configurations:
        printer.log('Config:', "dataset", experiment_config["input_file_path"], "relationship_similarity_threshold=",
                    experiment_config["relationship_similarity_threshold"],
                    "algorithm_threshold=", experiment_config["algorithm_threshold"], "constant_a=",
                    experiment_config["constant_a"], destinations=[global_vars.EXP_LOG, global_vars.EXP_CONSOLE])

        main(experiment_config)


if global_vars.experiment is False:
    main()
elif global_vars.experiment is True and global_vars.experiment_with_combinations is False:
    run_experiment()
elif global_vars.experiment is True and global_vars.experiment_with_combinations is True:
    if global_vars.experiment_dataset_versions:
        run_experiment_auto_combinations_dataset_versions()
    elif global_vars.experiment_diff_datasets:
        run_experiment_auto_combinations_different_datasets()
