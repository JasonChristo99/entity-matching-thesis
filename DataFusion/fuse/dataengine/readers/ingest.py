class Ingest:
    """
    This class provides the parent class for Ingest.
    """
    def __init__(self, filepath):
        self.filepath = filepath
        self.reader = None
        #Initialize identifiers
        self.entityID = 0
        self.factID = 0
        #Initialize data elements
        self.entity_attributes = set([])
        self.sources = set([])
        self.observed_facts = {}
        self.src_to_observed_fact = {}
        self.entity_attribute_schema = {}

    def get_entity_attrs(self):
        return self.entity_attributes

    def get_sources(self):
        return self.sources

    def get_observed_facts(self):
        return self.observed_facts

    def get_src_to_fact_map(self):
        return self.src_to_observed_fact

    def get_entity_attribute_schema(self):
        return self.entity_attribute_schema

    def _init_reader(self):
        print("Not implemented.")
        pass

    def _init_entity_attrs_schema(self):
        print("Not implemented.")
        pass

    def _extract_entity_attrs(self):
        print("Not implemented.")
        pass

    def _extract_source(self, src_entry):
        print("Not implemented")
        pass

    def _extract_observed_facts(self):
        print("Not implemented")
        pass

    def _extract_src_reported_facts(self, eid, src, src_entry):
        print("Not implemented")
        pass
