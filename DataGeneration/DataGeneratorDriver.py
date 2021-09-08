from faker import Faker

from DataGeneration import global_config
from serializers import *

fake = Faker()

YEARS = range(1990, 2021)


# A function that generates a string name and its synonyms, returns a CanonicalValue instance
def name_generator():
    first = fake.first_name()
    last = fake.last_name()
    full = first + ' ' + last

    first_dot = first[0].upper() + '.'
    last_dot = last[0].upper() + '.'

    synonyms = [first_dot + ' ' + last, first + ' ' + last_dot, first_dot + ' ' + last_dot, full.upper()]
    synonyms += [first, last, first.upper(), last.upper()]
    # invert
    synonyms += [' '.join(reversed(s.split(' '))) for s in synonyms]

    return CanonicalValue(full, synonyms)


# A function that generates a year range in YYYY-YYYY format, return the generated range as string
def year_range_generator():
    sample = random.sample(YEARS, 2)
    from_year = min(sample[0], sample[1])
    to_year = max(sample[0], sample[1])
    # full = {from_year, to_year}
    full = str(from_year) + '-' + str(to_year)
    # full = {'from': from_year, 'to': to_year}
    # full = (from_year, to_year)

    return full


# A function that generates a string location and its synonyms, returns a CanonicalValue instance
def location_generator():
    city = fake.city()
    synonyms = [city.upper()]

    if city.count(' ') >= 2:
        city = city[0: city.index(' ', start=city.index(' ') + 1)]

    if city.count(' ') == 1:
        first = city[0: city.index(' ')]
        second = city[city.index(' ') + 1:]
        first_dot = first[0].upper() + '.'
        second_dot = second[0].upper() + '.'
        synonyms = [first_dot + ' ' + second, first + ' ' + second_dot]
        # invert
        synonyms += [' '.join(reversed(s.split(' '))) for s in synonyms]

    return CanonicalValue(city, synonyms)


name_vertical = {
    'name': 'Name',
    'attributes': {
        'name': name_generator
    }
}

location_vertical = {
    'name': 'Location',
    'attributes': {
        'location': location_generator
    }
}

education_vertical = {
    'name': 'Education',
    # 'presence_chance': 0.9,
    'repetitions': {
        'min': 1,
        'geometric_parameter': 0.7
    },
    'attributes': {
        'degree': [
            CanonicalValue('Bsc', synonyms=["undergraduate_degree", "B.Sc", "Bachelor", "bachelor_degree"]),
            CanonicalValue('Msc', synonyms=["master_degree", "master", "MSc", "M.Sc"]),
            CanonicalValue('Phd', synonyms=["doctorate", "doctoral_degree", "Ph.D."]),
        ],
        'university': [
            CanonicalValue('MIT', synonyms=['mit', 'Massachusetts']),
            CanonicalValue('UMass', synonyms=['UMass_Amherst', 'Massachusetts_Amherst']),
            CanonicalValue('UCSC', synonyms=['Santa_Cruz_UCSC', 'UC_Santa_Cruz']),
            CanonicalValue('UC_Berkley', synonyms=['UC_Berkely', 'Berkeley']),
            # CanonicalValue('RPI', synonyms=['Rensselaer_Polytechnic_Institute'])
        ],
        'year': YEARS
    }
}

working_experience_vertical = {
    'name': 'Working Experience',
    # 'presence_chance': 0.85,  # probability for the entity to have facts of this vertical
    'repetitions': {  # how many facts should the entity have
        'min': 1,
        'geometric_parameter': 0.8
        # see https://numpy.org/doc/stable/reference/random/generated/numpy.random.geometric.html
    },
    'attributes': {
        'title': [
            CanonicalValue('developer',
                           synonyms=['developers', 'programmer', 'coder', 'computer_programer', 'engineer']),
            CanonicalValue('manager', synonyms=['managing_director', 'adminstrator', 'boss', 'supervisor']),
            CanonicalValue('analyst', synonyms=['economist', 'strategist', 'Senior_Analyst', 'accountant']),
            CanonicalValue('doctor', synonyms=['physician', 'clinician']),
        ],
        'company': fake.company,
        'years': year_range_generator
    }
}

skills_vertical = {
    'name': 'Skills',
    'presence_chance': 0.8,
    'repetitions': {
        'min': 2,
        'geometric_parameter': 0.5
    },
    'attributes': {
        'skill': [
            CanonicalValue('Java', synonyms=['J2EE_Java', 'scripting_language']),
            CanonicalValue('Python', synonyms=['Python_Ruby', 'python']),
            'CSS',
            CanonicalValue('HTML', synonyms=['HTML_XML', 'HTML5', 'HTML_coding']),
            CanonicalValue('JavaScript', synonyms=['javascript', 'Java_Script']),
            'Prolog',
            'Web Design',
            CanonicalValue('MongoDB', synonyms=['NoSQL_database']),
            'Elastic Search',
            'D3',
            'Dedupe',
            'ExpressJS',
            'Customer Relations Management',
            'Fund Management',
            CanonicalValue('Digital Branding', synonyms=['Digital_Media', 'digital']),
            CanonicalValue('Guitar', synonyms=['guitar', 'Acoustic_Guitar', 'Vintage_Guitar', 'Electric_Guitar']),
            'Bass',
            'Drums',
            CanonicalValue('English', synonyms=['english', 'English_fluently']),
            'French',
            'Greek',
            'German',
            CanonicalValue('Driving', synonyms=['speeding', 'drive']),
            CanonicalValue('Team Spirit', synonyms=['Team Player']),
            'Cocktail Inventing',
        ]
    }
}


# source_config_perfect = {
#     'name': 'cnn.com',
#     'Name': {
#     },
#     'Location': {
#     },
#     # 'Skills': {
#     # },
#     'Education': {
#     },
#     'Working Experience': {
#     }
# }

def generate(config=None):
    if config is not None:
        global_config.default_program_parameters = config

    canonicals, observed = generate_multi(
        [
            name_vertical,
            location_vertical,
            education_vertical,
            working_experience_vertical,
            # skills_vertical,
        ],
        global_config.default_program_parameters["sources"],
        entities_count=global_config.default_program_parameters["entities_count"],
    )
    # print(canonicals)
    # print(observed)

    # Canonical facts: All the true facts for every real world entity, grouped by entity
    # - only for fusion
    # print('JSON Canonical')
    # with open('canonical_facts.json', 'w') as f:
    #     f.write(all_canonical_json(canonicals))
    # Observed: All the facts that the sources observed (vertical & value - source), grouped by entity
    print('JSON Observed')
    with open('../datasets/' + global_config.default_program_parameters["out_file_name"] + '.json', 'w') as f:
        f.write(all_observed_json(observed))

# generate()
