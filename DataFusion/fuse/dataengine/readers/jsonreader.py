import pandas as pd
from dataengine.readers.reader import Reader


class JsonReader(Reader):
    """
    Read JSON file.
    """
    def __init__(self,input_path):
        Reader.__init__(self, input_path)

    def _load_input(self):
        try:
            self.rawFrame = pd.read_json(self.input_path)
        except:
            print('Invalid file given as input.')
            raise

    def get_entries(self):
        self._load_input()
        # Iterate over entries and extract facts
        for entry in self.rawFrame['body']:
            yield entry
