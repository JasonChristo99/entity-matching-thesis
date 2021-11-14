from dataengine.entity_inference.inference import EntityInference
from fact import ObservedFact
from dataengine.readers.ingest import Ingest
from dataengine.readers.jsonreader import JsonReader


class IngestJson(Ingest):
    """
    This class provides the necessary methods to ingest a JSON file.
    """

    def __init__(self, filepath, infer_entities=False):
        """
        The constructor for IngestJson.
        :param filepath: The file path for the JSON file to be loaded.
        """
        # TODO test inference thoroughly
        if infer_entities is True:
            self.entity_inf = EntityInference(filepath)
            Ingest.__init__(self, self.entity_inf.dest_dataset_filepath)
        else:
            self.entity_inf = None
            Ingest.__init__(self, filepath)
        # Ingest input file
        self._init_reader()
        self._extract_entity_attrs()
        self._extract_observed_facts()

    def _init_reader(self):
        self.reader = JsonReader(self.filepath)

    def _init_entity_attrs_schema(self):
        self.entity_attribute_schema = {k: set([]) for k in self.entity_attributes}

    def _extract_entity_attrs(self):
        for entity in self.reader.get_entries():
            for source_entry in entity:
                for k in source_entry.keys():
                    if k == "meta":
                        continue
                    self.entity_attributes.add(k)
        self._init_entity_attrs_schema()

    def _extract_source(self, source_entry):
        return source_entry['meta']['source']

    def _extract_observed_facts(self):
        for entity in self.reader.get_entries():
            eid = self.entityID
            # Step 1: Initialize observed facts dictionary
            self.observed_facts[eid] = {k: [] for k in self.entity_attributes}

            # Step 2: Iterate over all source entries and
            #         get observed facts per entity attribute.
            for src_entry in entity:
                # Get source
                src = self._extract_source(src_entry)
                # Store source id
                self.sources.add(src)
                # Get facts reported by src for each entity attribute
                for attr, fact in self._extract_src_reported_facts(eid, src, src_entry):
                    self.observed_facts[eid][attr].append(fact)
                    self.src_to_observed_fact.setdefault(src, set([])).add(fact.fid)
            self.entityID += 1

    def _extract_src_reported_facts(self, eid, src, src_entry):
        for attr in self.entity_attributes:
            if attr in src_entry:
                entries = src_entry[attr]
                if not isinstance(entries, list):
                    entries = [entries]
                for entry in entries:
                    fid = self.factID
                    fact = ObservedFact(fid, eid, attr, src, entry)
                    # Update schema for entries of entity attribute attr
                    self.entity_attribute_schema[attr] \
                        .union(set(fact.get_fact_attributes()))
                    self.factID += 1
                    yield attr, fact
