import time
from pathlib import Path

from faker import Faker

from DataGeneration import global_config
from serializers import *

import pandas as pd
from sklearn import linear_model
from fuzzywuzzy import fuzz
from gensim.models import KeyedVectors
import pickle

filename = 'C:/Users/Iasonas/Downloads/GoogleNews-vectors-negative300.bin'
word_vectors = KeyedVectors.load_word2vec_format(filename, binary=True)


class DataGenerator:
    def __init__(self, sources, entities_count):
        self.fake = Faker()
        self.YEARS = range(1990, 2021)
        self.canonicals = []
        self.observed = []
        self.sources = sources
        self.entities_count = entities_count

        # prepare output folder
        self.out_folder = '../datasets/' + time.strftime("%Y%m%d_%H%M%S") + '/'
        Path(self.out_folder).mkdir(parents=True, exist_ok=True)

        # verticals definitions
        self.name_vertical = {
            'name': 'Name',
            'attributes': {
                'name': self.name_generator
            }
        }

        self.location_vertical = {
            'name': 'Location',
            'attributes': {
                'location': self.location_generator
            }
        }

        self.education_vertical = {
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
                'year': self.YEARS
            }
        }

        self.working_experience_vertical = {
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
                'company': self.fake.company,
                'years': self.year_range_generator
            }
        }

        self.skills_vertical = {
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
                    CanonicalValue('Guitar',
                                   synonyms=['guitar', 'Acoustic_Guitar', 'Vintage_Guitar', 'Electric_Guitar']),
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

    # A function that generates a string name and its synonyms, returns a CanonicalValue instance
    def name_generator(self):
        first = self.fake.first_name()
        last = self.fake.last_name()
        full = first + ' ' + last

        first_dot = first[0].upper() + '.'
        last_dot = last[0].upper() + '.'

        synonyms = [first_dot + ' ' + last, first + ' ' + last_dot, first_dot + ' ' + last_dot, full.upper()]
        synonyms += [first, last, first.upper(), last.upper()]
        # invert
        synonyms += [' '.join(reversed(s.split(' '))) for s in synonyms]

        return CanonicalValue(full, synonyms)

    # A function that generates a year range in YYYY-YYYY format, return the generated range as string
    def year_range_generator(self):
        sample = random.sample(self.YEARS, 2)
        from_year = min(sample[0], sample[1])
        to_year = max(sample[0], sample[1])
        # full = {from_year, to_year}
        full = str(from_year) + '-' + str(to_year)
        # full = {'from': from_year, 'to': to_year}
        # full = (from_year, to_year)

        return full

    # A function that generates a string location and its synonyms, returns a CanonicalValue instance
    def location_generator(self):
        city = self.fake.city()
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

    def generate(self):
        self.canonicals, self.observed = generate_multi(
            [
                self.name_vertical,
                self.location_vertical,
                self.education_vertical,
                self.working_experience_vertical,
                # skills_vertical,
            ],
            self.sources,
            self.entities_count,
        )

        self.output_results()

    def output_results(self, observed_name='observed', canonical_name='eval', matched_name='matched'):
        print('JSON Observed')
        with open(self.out_folder + observed_name + '.json', 'w') as f:
            f.write(all_observed_json(self.observed))

        print('JSON Canonical')
        with open(self.out_folder + canonical_name + '.json', 'w') as f:
            f.write(all_canonical_json(self.canonicals))

        print('JSON Observed Matched')
        with open(self.out_folder + matched_name + '.json', 'w') as f:
            f.write(matched_observed_json(self.observed, 200))

    def train_regression_model_for_threshold(self):
        res = generate_value_matcher_dataset(
            [self.name_vertical, self.education_vertical, self.working_experience_vertical, self.location_vertical])
        fuzz_sc = []
        word_sc = []
        actual = []
        for i in range(len(res)):
            fuzz_sc.append(fuzz.token_sort_ratio(res[i].__getitem__(0), res[i].__getitem__(1)) * 0.01)
            if res[i].__getitem__(0) in word_vectors and res[i].__getitem__(1) in word_vectors:
                word_sc.append(word_vectors.similarity(res[i].__getitem__(0), res[i].__getitem__(1)))
            else:
                word_sc.append(0.0)
            actual.append(res[i].__getitem__(2))
        dict = {'fuzzy score': fuzz_sc, 'word2vec score': word_sc, 'true/false': actual}
        df = pd.DataFrame(dict, columns=['fuzzy score', 'word2vec score', 'true/false'])
        print(df)
        X = df[['fuzzy score', 'word2vec score']]
        y = df['true/false']
        regr = linear_model.LogisticRegression()
        regr.fit(X, y)
        pred = regr.predict(X)
        filename = self.out_folder + 'finalized_model.sav'
        pickle.dump(regr, open(filename, 'wb'))


if __name__ == '__main__':
    gen = DataGenerator(global_config.default_program_parameters["sources"],
                        entities_count=global_config.default_program_parameters["entities_count"])
    gen.generate()

    gen.train_regression_model_for_threshold()
