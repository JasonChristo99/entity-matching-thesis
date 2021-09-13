class Reader:
    """
       This is a parent class for a reader.
    """
    def __init__(self,input_path):
        self.input_path = input_path
        self.rawFrame = None

    def _load_input(self):
        print('Not implemented.')
        pass

    def get_entries(self):
        print('Not implemented.')
        pass