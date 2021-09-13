import numpy as np
from itertools import product


class Labeler:
    """
    A utility class to label matching observed facts.
    """
    def __init__(self, dataset, attr):
        """
        The constructor of a Labeler object.
        :param dataset: A dataset object.
        :param attr: The entity attribute
               associated with the facts considered.
        """
        self.ds = dataset
        self.attr = attr
        self.total_cols = len(self.ds.observed_fact_collections[self.attr])

    def label_samples(self, samples = 15):
        print("Label matches as 'y' and non-matches as 'n'. " \
              "To stop labeling type 's'.")
        examples = {'match' : [], 'distinct' : []}
        idxs = np.random.choice(self.total_cols, samples)
        for idx in idxs:
            print('New collection')
            col = self.ds.observed_fact_collections[self.attr][idx]
            data = col.fact_records
            if len(data.keys()) > 1:
                for idx1, idx2 in product(data.keys(), data.keys()):
                    if idx1 != idx2:
                        print("Record 1:", data[idx1])
                        print("Record 2:", data[idx2])
                        label = raw_input("Is this a match?")
                        if label == 's':
                            print("Done.")
                            break
                        elif label == 'y':
                            examples['match'].append((data[idx1], data[idx2]))
                        else:
                            examples['distinct'].append((data[idx1], data[idx2]))
        return examples


