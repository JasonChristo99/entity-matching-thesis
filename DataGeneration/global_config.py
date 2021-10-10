# default program parameters
default_program_parameters = {
    # constants
    "entities_count": 500,
    "out_file_name": "observed_facts",
    # sources
    "sources": [
        {
            'name': 'pro-profiles.com',
            'Name': {
                'typos_binomial_param': 0.07,
                'synonym_chance': 0.6,
            },
            'Location': {
                # 'omitted_fact_chance': 0.03,
                # 'incorrect_fact_geom_param': 0.95,
                'wrong_value_chance': 0.1,
                # 'abstain_value_chance': 0.01,
                'synonym_chance': 0.3,
                'typos_binomial_param': 0.01,
            },
            'Education': {
                # 'omitted_fact_chance': 0.03,  # chance for the source to omit a fact
                'incorrect_fact_geom_param': 0.9,  # chance for the source to add incorrect facts to an entity
                'wrong_value_chance': 0.1,  # chance for the source to give the wrong value for an attribute
                # 'abstain_value_chance': 0.05,  # chance for the source to omit the value of an attribute
                'synonym_chance': 0.1,
                'typos_binomial_param': 0.01,
            },
            'Working Experience': {
                # 'omitted_fact_chance': 0.03,
                'incorrect_fact_geom_param': 0.9,
                'wrong_value_chance': 0.03,
                # 'abstain_value_chance': 0.05,
                'synonym_chance': 0.03,
                'typos_binomial_param': 0.01,
            }
        },
        {
            'name': 'find.jobs',
            'Name': {
                'typos_binomial_param': 0.06,
                'synonym_chance': 0.5,
            },
            'Location': {
                # 'omitted_fact_chance': 0.03,
                # 'incorrect_fact_geom_param': 0.95,
                'wrong_value_chance': 0.15,
                # 'abstain_value_chance': 0.01,
                'synonym_chance': 0.4,
                'typos_binomial_param': 0.01,
            },
            'Education': {
                # 'omitted_fact_chance': 0.03,
                'incorrect_fact_geom_param': 0.95,
                'wrong_value_chance': 0.08,
                # 'abstain_value_chance': 0.01,
                'synonym_chance': 0.1,
                'typos_binomial_param': 0.01,
            },
            'Working Experience': {
                # 'omitted_fact_chance': 0.03,
                'incorrect_fact_geom_param': 0.9,
                'wrong_value_chance': 0.18,
                # 'abstain_value_chance': 0.05,
                'synonym_chance': 0.06,
                'typos_binomial_param': 0.01,
            }
        },
        {
            'name': 'internsteps.com',
            'Name': {
                'typos_binomial_param': 0.05,
                'synonym_chance': 0.4,
            },
            'Location': {
                # 'omitted_fact_chance': 0.03,
                # 'incorrect_fact_geom_param': 0.95,
                'wrong_value_chance': 0.2,
                # 'abstain_value_chance': 0.01,
                'synonym_chance': 0.5,
                'typos_binomial_param': 0.01,
            },
            'Education': {
                # 'omitted_fact_chance': 0.03,
                'incorrect_fact_geom_param': 0.95,
                'wrong_value_chance': 0.15,
                # 'abstain_value_chance': 0.01,
                'synonym_chance': 0.1,
                'typos_binomial_param': 0.01,
            },
            'Working Experience': {
                # 'omitted_fact_chance': 0.01,
                'incorrect_fact_geom_param': 0.9,
                'wrong_value_chance': 0.05,
                # 'abstain_value_chance': 0.05,
                'synonym_chance': 0.05,
                'typos_binomial_param': 0.01,
            }
        },
        {
            'name': 'pipl.com',
            'Name': {
                'typos_binomial_param': 0.05,
                'synonym_chance': 0.3,
            },
            'Location': {
                # 'omitted_fact_chance': 0.03,
                # 'incorrect_fact_geom_param': 0.95,
                'wrong_value_chance': 0.15,
                # 'abstain_value_chance': 0.01,
                'synonym_chance': 0.4,
                'typos_binomial_param': 0.01,
            },
            'Education': {
                # 'omitted_fact_chance': 0.01,
                'incorrect_fact_geom_param': 0.85,
                'wrong_value_chance': 0.05,
                # 'abstain_value_chance': 0.05,
                'synonym_chance': 0.05,
                'typos_binomial_param': 0.01,
            },
            'Working Experience': {
                # 'omitted_fact_chance': 0.01,
                'incorrect_fact_geom_param': 0.9,
                'wrong_value_chance': 0.15,
                # 'abstain_value_chance': 0.05,
                'synonym_chance': 0.05,
                'typos_binomial_param': 0.01,
            }

        }
    ]
}
