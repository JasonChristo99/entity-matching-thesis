import sqlalchemy as sqla
from dataengine.readers import IngestJson
from dataset import Dataset
from dataset_global_weights import DatasetGlobalWeights


class DataEngine:
    """
    This class provides a layer between Fuse and a persistent backend.
    """
    def __init__(self, fuse_env):
        """
        The constructor for DataEngine
        :param fuse_env: The fuse environment to use.
        """
        # Store Fuse env
        self.env = fuse_env

        # Validate DB metadata
        if not self.env.db_user:
            self.env.logger.error("No DB user specified.")
        if not self.env.db_pwd:
            self.env.logger.error("No DB password specified.")
        if not self.env.db_host:
            self.env.logger.error("No DB host specified.")
        if not self.env.db_name:
            self.env.logger.error("No DB name specified")

        # Store DB metadata
        self.db_user = self.env.db_user
        self.db_pwd = self.env.db_pwd
        self.db_host = self.env.db_host
        self.db_name = self.env.db_name

        # Initialize database backend
        self.db_conn = self._start_db()
        self.db_tables = set([])

        # Initialize dataset object
        self.dataset = None

    def _start_db(self):
        """
        Start and connect to the database backend
        :return: An Sqlalchemy Engine
        """
        # Start Sqlalchemy engine
        # TODO: change the connection to a proper DB backend
        connection = "sqlite:///fuse.db"
        db = sqla.create_engine(connection)
        # Connect to database
        try:
            conn = db.connect()
            self.env.logger.info("Connection established to database.")
            return conn
        except BaseException:
            self.env.logger.error("Cannot connect to database.")
            return None
            pass

    def persist_frame(self, df, name, dtype=None):
        """
        Persist a dataframe.
        :param df: The dataframe to persist.
        :param table_name: The table name to be used.
        :param dtype: dict of column name to SQL type, default None
        :return: None.
        """
        df.to_sql(name, self.db_conn, if_exists="replace",dtype=dtype)
        return name

    def ingest_data(self, filepath, dataset_name, persist=False):
        """
        Loads the data from an input JSON file and
        :param filepath: The path of the file to be loaded.
        :param dataset_name: The name of the dataset to be loaded.
        :return: A DataSet instance.
        """
        # Ingest data
        ingest = IngestJson(filepath)
        entity_attributes = ingest.get_entity_attrs()
        sources = ingest.get_sources()
        observed_facts = ingest.get_observed_facts()
        src_to_fact_map = ingest.get_src_to_fact_map()
        ent_attr_schema = ingest.get_entity_attribute_schema()

        # Generate new dataset object
        if self.env.global_weights:
            self.dataset = DatasetGlobalWeights(self.env, dataset_name,
                                                entity_attributes,
                                                ent_attr_schema,
                                                sources,
                                                observed_facts,
                                                src_to_fact_map)
        else:
            self.dataset = Dataset(self.env, dataset_name,
                                   entity_attributes,
                                   ent_attr_schema,
                                   sources,
                                   observed_facts,
                                   src_to_fact_map)

        # Persist dataset frames
        if persist:
            src_df = self.dataset.get_src_frame()
            self.persist_frame(src_df, "sources")

            attrs_df = self.dataset.get_entity_attrs_frame()
            self.persist_frame(attrs_df, "entity_attributes")

            map_df = self.dataset.get_src_to_fact_map_frame()
            self.persist_frame(map_df, "src_to_fact")

        # Return dataset
        return self.dataset


