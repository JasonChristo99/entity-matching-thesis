import itertools
import json
import dedupe
from DataGeneration.generators import *


def _extract_values(fact: Fact):
    vals = ((value.value if isinstance(value, Value) else value) for value in fact.values)
    return [str(val) if val is not None else None for val in vals]


def _remove_nones(d: dict):
    return {k: v for k, v in d.items() if v is not None}


def vertical_as_json(schema, facts: Sequence[Fact], source: str = None, match_id=False):
    attributes = schema['attributes'].keys()

    if match_id:
        attributes = [*attributes, '_match_id']
        result = (dict(zip(attributes, _extract_values(fact) + [fact.id])) for fact in facts)
    else:
        result = (
            {schema['name']: _remove_nones(dict(zip(attributes, _extract_values(fact))))} for fact in facts
        )

    if source is not None:
        result = ({**vertical, 'meta': {'source': source}} for vertical in result)

    return list(result)


def entity_as_json(entity: Entity, source: str = None):
    result = (
        vertical_as_json(schema, entity.verticals[schema['name']], source)
        for schema in entity.vertical_schemas if len(entity.verticals.get(schema['name'], [])) > 0
    )

    return list(itertools.chain(*result))


def observed_entity_as_json(observed: Sequence[ObservedEntity]):
    result = itertools.chain(
        *(entity_as_json(ent, ent.source['name']) for ent in observed if len(ent.verticals) > 0)
    )
    # remove empty entities
    return [
        entity for entity in result if len(entity) > 0
    ]


def all_canonical_json(entities: Sequence[Entity]):
    return json.dumps({
        'body': [entity_as_json(entity) for entity in entities]
    })


def all_observed_json(observed: Sequence[Sequence[ObservedEntity]]):
    return json.dumps({
        'body': [observed_entity_as_json(entity) for entity in observed]
    })


def matched_observed_json(observed: Sequence[Sequence[ObservedEntity]], size=1000):
    # generated in the form required by dedupe.Dedupe.mark_pairs per vertical
    # https://docs.dedupe.io/en/latest/API-documentation.html#dedupe.Dedupe.mark_pairs
    result = {}
    for entity_group in observed:
        for ent in entity_group:
            for schema in ent.vertical_schemas:
                ver = schema['name']
                if ver not in ent.verticals: continue
                if ver not in result:
                    result[ver] = []
                result[ver].extend(vertical_as_json(schema, ent.verticals[ver], match_id=True))

    def process_vertical_records(records):
        random.shuffle(records)
        # convert to dict with arbitrary keys
        records = dict(enumerate(records))
        records = dedupe.training_data_dedupe(records, '_match_id', size)

        # remove _match_id
        def remove_match_id(pair):
            if '_match_id' in pair[0]: del pair[0]['_match_id']
            if '_match_id' in pair[1]: del pair[1]['_match_id']
            return pair

        records['match'] = list(map(remove_match_id, records['match']))
        records['distinct'] = list(map(remove_match_id, records['distinct']))
        return records

    result = {v: process_vertical_records(recs) for v, recs in result.items()}

    return json.dumps(result)
