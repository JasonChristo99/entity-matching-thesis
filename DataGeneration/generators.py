from functools import reduce
import random
from typing import Sequence, Union, Callable, Mapping
import operator
import numpy as np
import pandas as pd

from DataGeneration.typo_generation import misspell


class Value:
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return repr(self.value)

    def __eq__(self, other):
        return isinstance(other, Value) and self.value == other.value or self.value == other

    def __hash__(self):
        # return hash(self.value)
        # if isinstance(self.value, dict):
        #     return hash(frozenset(self.value.items()))
        return hash(self.value)


class CanonicalValue(Value):
    def __init__(self, value, synonyms=None):
        if synonyms is None:
            synonyms = []
        self.synonyms = synonyms

        super(CanonicalValue, self).__init__(value)


# picks a value randomly from values, excluding exclude
def _random_value(values_provider: Union[Sequence, Callable], exclude=None):
    chooser = values_provider if callable(values_provider) else lambda: random.choice(values_provider)

    chosen = chooser()
    if exclude is not None and (callable(values_provider) or len(values_provider) > 1):
        while chosen == exclude:
            chosen = chooser()

    return chosen if isinstance(chosen, Value) else CanonicalValue(chosen)


class Fact:
    _FACT_ID = 0

    # schema is the schema of the vertical's attributes
    def __init__(self, schema, values: Sequence):
        self.schema = schema
        self.values = tuple(values)
        # self.id = str(uuid.uuid4())
        self.id = Fact._FACT_ID
        Fact._FACT_ID += 1

    def __repr__(self):
        return repr(tuple(self.schema['attributes'].keys())) + "=" + repr(self.values)

    @staticmethod
    def generate(schema):
        values = tuple(_random_value(v) for v in schema['attributes'].values())
        return Fact(schema, values)


class Entity:
    def __init__(self, vertical_schemas: Sequence = None, verticals: Mapping[str, Sequence[Fact]] = None):
        self.vertical_schemas = [] if vertical_schemas is None else vertical_schemas
        self.verticals = {} if verticals is None else verticals
        self._make_handy_dataframes()

    def _make_handy_dataframes(self):
        # make handy dataframes for the facts of each vertical
        self.verticals_dataframe = {}
        for schema in self.vertical_schemas:
            if schema['name'] not in self.verticals:
                continue
            v_name = schema['name']
            fact_values = (f.values for f in self.verticals[v_name])
            self.verticals_dataframe[v_name] = pd.DataFrame(columns=schema['attributes'].keys(), data=fact_values)

    def __repr__(self):
        return '\n'.join(
            (v + ':\n' + repr(df)) if len(df) > 0 else '' for v, df in self.verticals_dataframe.items()).strip()

    @staticmethod
    def generate(vertical_schemas: Sequence = None):
        if vertical_schemas is None:
            vertical_schemas = []
        verticals = {}

        for schema in vertical_schemas:
            # determine presence
            if np.random.random() > schema.get('presence_chance', 1):
                continue

            facts = []
            verticals[schema['name']] = facts

            # determine repetitions
            rep_conf = schema['repetitions'] if 'repetitions' in schema else {'min': 1, 'max': 1}
            min_reps = rep_conf.get('min', 0)
            if 'geometric_parameter' in rep_conf:
                repetition = np.random.geometric(rep_conf['geometric_parameter']) - 1 + min_reps
            else:
                repetition = min_reps
            if 'max' in rep_conf:
                repetition = min(repetition, rep_conf['max'])

            # determine the number of possible facts for the vertical
            # assume infinite if values are obtained by a function
            if any(callable(val_gen) for val_gen in schema['attributes'].values()):
                vertical_cardinality = np.Inf
            else:
                vertical_cardinality = reduce(operator.mul, (len(vals) for vals in schema['attributes'].values()))

            # now, generate the facts
            seen_values = set()  # do not repeat facts
            for i in range(min(repetition, vertical_cardinality)):
                fact = Fact.generate(schema)
                while fact.values in seen_values:
                    fact = Fact.generate(schema)
                facts.append(fact)
                seen_values.add(fact.values)

        return Entity(vertical_schemas, verticals)


class ObservedValue(Value):
    def __init__(self, value, canonical):
        self.canonical = canonical
        super(ObservedValue, self).__init__(value)

    @staticmethod
    def generate(canonical: CanonicalValue, source_vertical: dict):
        if len(canonical.synonyms) and np.random.random() < source_vertical.get('synonym_chance', 0):
            value = random.choice(canonical.synonyms)
        else:
            value = canonical.value

        if isinstance(value, str) and 'typos_binomial_param' in source_vertical:
            value = misspell(value, source_vertical['typos_binomial_param'])

        return ObservedValue(value, canonical)


class ObservedFact(Fact):
    # source is the config of the source the fact is observed in
    def __init__(self, source: dict, canonical: Fact = None, schema=None, values: Sequence = None):
        self.source = source
        self.canonical = canonical

        if schema is None and canonical is not None:
            schema = canonical.schema
        if values is None and canonical is not None:
            values = [ObservedValue(can_value.value, can_value) for can_value in canonical.values]

        super(ObservedFact, self).__init__(schema, values)

        # all observed facts from the same canonical have the same id for matching
        if canonical is not None:
            self.id = canonical.id

    @staticmethod
    def generate_from(source: dict, canonical: Fact):
        source_vertical = source[canonical.schema['name']]

        def pick_value(canonical_value, attribute_value_gen):
            if np.random.random() < source_vertical.get('abstain_value_chance', 0):
                return CanonicalValue(None)
            if np.random.random() < source_vertical.get('wrong_value_chance', 0):
                return _random_value(attribute_value_gen, canonical_value)
            return canonical_value

        attribute_values = canonical.schema['attributes'].values()
        values = tuple(ObservedValue.generate(pick_value(cv, av), source_vertical)
                       for cv, av in zip(canonical.values, attribute_values))

        return ObservedFact(source, canonical=canonical, values=values)

    @staticmethod
    def generate_incorrect(source, schema):
        values = Fact.generate(schema).values
        return ObservedFact(source, schema=schema, values=values)


class ObservedEntity(Entity):
    def __init__(self, source: dict, canonical: Entity, verticals: dict = None):
        if verticals is None and canonical is not None:
            verticals = canonical.verticals
        self.source = source
        super(ObservedEntity, self).__init__(canonical.vertical_schemas, verticals)

    def __repr__(self):
        return super(ObservedEntity, self).__repr__() + '\n-- as seen on ' + self.source['name']

    @staticmethod
    def generate_from(source: dict, canonical: Entity):
        verticals = {}

        for schema in canonical.vertical_schemas:
            if schema['name'] not in source:
                continue

            facts = []
            verticals[schema['name']] = facts

            source_vertical = source[schema['name']]
            canonical_facts = canonical.verticals.get(schema['name'], [])
            if 'omitted_fact_chance' in source_vertical:
                for canonical_fact in canonical_facts:
                    if np.random.random() > source_vertical['omitted_fact_chance']:
                        facts.append(ObservedFact.generate_from(source, canonical_fact))
            else:
                facts.extend(ObservedFact.generate_from(source, canonical_fact) for canonical_fact in canonical_facts)

            if 'incorrect_fact_geom_param' in source_vertical:
                incorrect_fact_count = np.random.geometric(source_vertical['incorrect_fact_geom_param']) - 1
                facts.extend(ObservedFact.generate_incorrect(source, schema) for _ in range(incorrect_fact_count))

        return ObservedEntity(source, canonical, verticals)


def generate_multi(vertical_schemas, sources, entities_count=100):
    canonicals = [
        Entity.generate(vertical_schemas) for _ in range(entities_count)
    ]

    observed = [
        [ObservedEntity.generate_from(source, canonical) for source in sources]
        for canonical in canonicals
    ]

    return canonicals, observed


class my_dictionary(dict):
    def __init__(self):
        self = dict()

    def add(self, key, value):
        self[key] = value


def generate_value_matcher_dataset(vertical_schemas,
                                   canon_values_to_pick=10,
                                   typos_per_canon=3,
                                   typos_per_synonym=1,
                                   typos_binomial_param=0.05,
                                   equiv_pairs_per_value=100,
                                   non_equiv_pairs_per_attr=150):
    pairs_dic = []
    pairs = []
    # dict = {}
    attr = []
    res_fuz = []
    res_b = []
    pairs_years_ed = []
    pairs_years_exp = []
    for schema in vertical_schemas:
        for attr_name, attr_values in schema['attributes'].items():
            if callable(attr_values):
                canons = set()
                for _ in range(canon_values_to_pick):
                    canon_value = attr_values()
                    while canon_value in canons:
                        canon_value = attr_values()
                    canons.add(canon_value)
            else:
                canons = random.sample(attr_values, min(len(attr_values), canon_values_to_pick))

            equiv_pairs = []

            for canon_value in canons:
                if isinstance(canon_value, CanonicalValue):
                    synonyms = canon_value.synonyms
                    canon_value = canon_value.value
                else:
                    synonyms = []

                if not isinstance(canon_value, str):
                    continue

                equiv_values = set([canon_value] + synonyms)
                equiv_values |= set(misspell(canon_value, typos_binomial_param)
                                    for _ in range(typos_per_canon))

                equiv_values |= set(misspell(syn, typos_binomial_param)
                                    for syn in synonyms
                                    for _ in range(typos_per_synonym))

                equiv_values = list(equiv_values)
                if len(equiv_values) < 2:
                    continue
                val_pairs = set(tuple(random.sample(equiv_values, 2)) for _ in range(equiv_pairs_per_value))

                equiv_pairs += [(pair[0], pair[1], True, canon_value) for pair in val_pairs]

            non_equivs = set()
            for _ in range(min(non_equiv_pairs_per_attr, len(equiv_pairs))):
                pair1 = random.choice(equiv_pairs)
                canon = pair1[-1]

                pair2 = random.choice(equiv_pairs)
                while pair2[-1] == canon:
                    pair2 = random.choice(equiv_pairs)

                non_equivs.add((
                    pair1[0],
                    pair2[1],
                    False
                ))

            print(f"{attr_name}: {len(equiv_pairs)} equivalent pairs, {len(non_equivs)} non-equivalent pairs")

            pairs += equiv_pairs + list(non_equivs)
            # p = equiv_pairs + list(non_equivs)
            # attr.append(attr_name)
            # print(dict)
            # pairs_dic.append(p)

            print(attr_name)

    dict = {'attr': attr, 'pairs': pairs_dic}
    return pairs
    # [pair[:3] for pair in pairs] , attr
