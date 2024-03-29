import math


class TruthDiscoveryGlobalWeights:
    """
    This class defines a fuser to identify true facts given
    a collection of conflicting source votes on a fact being true or not.
    """

    def __init__(self, fuse_env, votes, src_weights):
        """
        :param fuse_env: A Fuse environment.
        :param votes: A dictionary with schema [fact][source] that captures
                      source votes. A vote of +1 represents a "Fact is True" vote and
                      -1 represents a "Fact is False" vote.
        :param src_weights: A dictionary with source weights that capture the accuracy
                      of each source.
        """
        self.env = fuse_env
        self.votes = votes
        self.src_weights = src_weights
        self.fact_assignments = {}
        self._find_fact_assignments()

    def _find_fact_assignments(self):
        """
        Iterate over facts and find their initial MAP assignment.
        :return: A dictionary of fact_assignments.
        """
        for fact in self.votes:
            fact_votes = self.votes[fact]
            self.fact_assignments[fact] = self._find_map_state(fact_votes)

    def _find_map_state(self, votes):
        """
        Iterate over source votes and find MAP assignment for a fact.
        :return: MAP assignment
        """
        score = 0.0
        for src in votes:
            score += votes[src] * self.src_weights[src]
        return score > 0.0

    def _update_src_weights(self):
        """
        Iterate over map fact assignments and update source weights.
        :return: None.
        """
        src_stats = {k: {'total': 0.0, 'correct': 0.0} for k in self.src_weights.keys()}
        for fact in self.votes:
            map_state = self.fact_assignments[fact]
            for src in self.votes[fact]:
                src_stats[src]['total'] += 1.0
                # Check if source agrees with map state
                if self.votes[fact][src] == 2 * int(map_state) - 1:
                    src_stats[src]['correct'] += 1.0
        # Compute src accuracy and then weight
        for src in self.src_weights:
            accu = 0.5
            if src_stats[src]['total'] != 0.0:
                accu = src_stats[src]['correct'] / src_stats[src]['total']
            if accu == 1.0:
                accu = 0.99
            elif accu == 0.0:
                accu = 0.01
            self.src_weights[src] = math.log(accu / (1 - accu))

    def run(self, iterations=10):
        """
        Run iterative truth discovery.
        :param iterations: The number of iterationrs to run for.
        :return: Fact map assignments.
        """
        self.env.logger.info("Running truth discovery for " + str(iterations) + " iterations.")
        for iter in range(iterations):
            self.env.logger.info("Iteration = " + str(iter))
            self.env.logger.info("Source weights = " + str(self.src_weights))
            self._find_fact_assignments()
            self._update_src_weights()
        return self.fact_assignments


class FuseObservationsGlobalWeights:
    """
    This class defines a fuser to merge observations in a true cluster.
    """

    def __init__(self, fuse_env, clusters, src_acc, no_weights):
        """
        :param fuse_env: A Fuse environment.
        :param clusters: A set of CanonicalFact clusters.
        :param: src_acc: A vector with the transformed source accuracies.
        :param: no_weights: If set to `False` simple majority voting is used.
        """
        self.env = fuse_env
        self.clusters = clusters
        self.src_acc = src_acc
        self.no_weights = no_weights

    def _update_src_acc(self):
        """
        Iterate over map fact assignments and update source weights.
        :return: None.
        """
        src_stats = {k: {'total': 0.0, 'correct': 0.0} for k in self.src_acc.keys()}
        for cluster in self.clusters:
            for fact in cluster.get_facts():
                src = fact.src
                score = cluster.evaluate_fact(fact)
                src_stats[src]['total'] += 1.0
                src_stats[src]['correct'] += score
        # Compute src accuracy and then weight
        for src in self.src_acc:
            accu = 0.99
            if src_stats[src]['total'] != 0.0:
                accu = src_stats[src]['correct'] / src_stats[src]['total']
            if accu == 1.0:
                accu = 0.99
            elif accu == 0.0:
                accu = 0.01
            print(src, " total")
            print(src_stats[src]['total'])
            print(src, " correct")
            print(src_stats[src]['correct'])
            print(src, " accuracy = ", accu)
            self.src_acc[src] = math.log(accu / (1 - accu))

    def _find_map_state(self):
        """
        Iterate over clusters and infer MAP state.
        :return: None.
        """
        for cluster in self.clusters:
            cluster.infer_true_assignment()

    def get_canonical_facts(self, flattened=False):
        """
        :return: CanonicalFact clusters.
        """
        return self.clusters

    def flatten_facts(self):
        """
        :return: Flattened facts
        """
        facts = []
        for cluster in self.clusters:
            for attr in cluster.canonicalTuple:
                f = (cluster.ent_id, cluster.ent_attr, attr, cluster.canonicalTuple[attr])
                facts.append(f)
        return facts

    def run(self, iterations=10):
        """
        Run iterative truth discovery.
        :param iterations: The number of iterationrs to run for.
        :return: Fact map assignments.
        """
        if self.no_weights:
            self.env.logger.info("Running fusion with majority voting.")
            self._find_map_state()
        else:
            self.env.logger.info("Running fusion for " + str(iterations) + " iterations.")
            for iter in range(iterations):
                self.env.logger.info("Iteration = " + str(iter))
                self.env.logger.info("Source weights = " + str(self.src_acc))
                self._find_map_state()
                self._update_src_acc()
