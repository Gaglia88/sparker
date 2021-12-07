import numpy as np
from queue import PriorityQueue
from .blocking_strategies import BlockingKeysStrategies
from .common_node_pruning import compute_statistics, calc_cbs, calc_weights, do_reset, WeightTypes


import numpy as np
from queue import PriorityQueue
from sparker.blocking_strategies import BlockingKeysStrategies
from sparker.common_node_pruning import compute_statistics, calc_cbs, calc_weights, do_reset, WeightTypes

class PPS(object):
    """
    Implements the PPS method proposed in Simonini et. all "Schema-agnostic Progressive Entity Resolution"
    https://arxiv.org/pdf/1905.06385.pdf
    """
    @staticmethod
    def calc_top_comparisons(profile_blocks_filtered, block_index, max_id, separator_id, weight_type,
                             profile_blocks_size_index, use_entropy, blocks_entropies, number_of_edges,
                             edges_per_profile):
        """
        For each profile computes the top comparison and the profile score
        :param profile_blocks_filtered: profile_blocks after block filtering
        :param block_index: a map that given a block ID returns the ID of the contained profiles
        :param max_id: maximum profile ID
        :param separator_id: maximum profile ID of each datasets (empty if its dirty)
        :param weight_type: type of weight to use
        :param profile_blocks_size_index: a map that contains for each profile the number of its blocks
        :param use_entropy: if true use the provided entropies to improve the edge weighting
        :param blocks_entropies: a map that contains for each block its entropy
        :param number_of_edges: global number of existings edges
        :param edges_per_profile: a map that contains for each profile the number of edges
        :return: an RDD which contains for each profile_id the threshold
        """

        def compute_partition(partition):
            top_comparisons = []
            profiles_list = []
            local_weights = [0] * (max_id + 1)
            neighbors = [0] * (max_id + 1)
            entropies = None
            if use_entropy:
                entropies = [0.0] * (max_id + 1)

            def inside_map(pb):
                neighbors_number = calc_cbs(pb, block_index, separator_id, use_entropy, blocks_entropies,
                                            local_weights, entropies, neighbors, False)
                calc_weights(pb, local_weights, neighbors, entropies, neighbors_number, block_index, separator_id,
                             weight_type, profile_blocks_size_index, use_entropy, number_of_edges, edges_per_profile)

                # For each profile computes the top-comparison and the duplication likelihood
                top_comparison = (-1, -1, -1)
                duplication_likelihood = 0
                for i in range(0, neighbors_number):
                    duplication_likelihood += local_weights[neighbors[i]]
                    if top_comparison[2] < local_weights[neighbors[i]]:
                        top_comparison = (pb.profile_id, neighbors[i], local_weights[neighbors[i]])

                if top_comparison[2] > 0:
                    top_comparisons.append(top_comparison)

                if neighbors_number > 0:
                    duplication_likelihood /= neighbors_number
                    profiles_list.append((pb.profile_id, duplication_likelihood))
                do_reset(local_weights, neighbors, entropies, use_entropy, neighbors_number)
                pass

            for p in partition:
                inside_map(p)

            res = []
            res.append((profiles_list, top_comparisons))
            return res

        return profile_blocks_filtered.mapPartitions(compute_partition)

    def __init__(self, profile_blocks_filtered, block_index, max_id, separator_ids=None, weight_type=WeightTypes.CBS,
                 profile_blocks_size_index=None, use_entropy=False, blocks_entropies=None, k=20, mini_batch=40):
        """
        Initialize the class
        :param profile_blocks_filtered: profile_blocks after block filtering
        :param block_index: a map that given a block ID returns the ID of the contained profiles
        :param max_id: maximum profile ID
        :param separator_id: maximum profile ID of each datasets (empty if its dirty)
        :param weight_type: type of weight to use
        :param profile_blocks_size_index: a map that contains for each profile the number of its blocks
        :param use_entropy: if true use the provided entropies to improve the edge weighting
        :param blocks_entropies: a map that contains for each block its entropy
        :param k: max number of comparisons to retain for each profile (default 20)
        :param mini_batch: number of profiles neighborhood to compute when the emission list is empty (default 40), if
               sets to -1 computes 1 profile at time
        :return: an RDD which contains for each profile_id the threshold
        """

        self.top_emitted = {}
        self.mini_batch = mini_batch
        self.profile_blocks = profile_blocks_filtered
        self.block_index = block_index
        self.max_id = max_id
        self.separator_ids = separator_ids
        self.weight_type = weight_type
        self.profile_blocks_size_index = profile_blocks_size_index
        self.use_entropy = use_entropy
        self.blocks_entropies = blocks_entropies
        self.profiles_list = PriorityQueue()
        self.comparison_list = PriorityQueue()
        self.k = k+1
        self.visited = set()

        self.local_weights = [0] * (max_id + 1)
        self.neighbors = [0] * (max_id + 1)
        self.entropies = None
        if self.use_entropy:
            self.entropies = [0.0] * (max_id + 1)

        if separator_ids is None:
            self.separator_ids = []

        valid_weights = [WeightTypes.CBS, WeightTypes.JS, WeightTypes.CHI_SQUARE, WeightTypes.ARCS,
                         WeightTypes.ECBS, WeightTypes.EJS]

        if weight_type not in valid_weights:
            raise ValueError("Please provide a valid Weight_type, " + str(weight_type) + " is not an acceptable value!")

        if use_entropy and blocks_entropies is None:
            raise ValueError('blocks_entropies must be defined')

        if (weight_type == WeightTypes.ECBS or weight_type == WeightTypes.EJS or weight_type == WeightTypes.JS or
            weight_type == WeightTypes.CHI_SQUARE) and profile_blocks_size_index is None:
            raise ValueError('profile_blocks_size_index must be defined')

        sc = profile_blocks_filtered.context
        self.number_of_edges = 0
        self.edges_per_profile = None

        if weight_type == WeightTypes.EJS:
            stats = compute_statistics(profile_blocks_filtered, block_index, max_id, separator_ids)
            self.number_of_edges = stats.map(lambda x: x[0]).sum()
            self.edges_per_profile = sc.broadcast(
                stats.map(lambda x: (x[1][0], x[1][1])).groupByKey().map(lambda x: (x[0], sum(x[1]))).collectAsMap())

    @staticmethod
    def get_top_comparisons(profile_blocks_filtered, block_index, max_id, separator_id, weight_type,
                            profile_blocks_size_index, use_entropy, blocks_entropies, number_of_edges,
                            edges_per_profile, k):
        """
        For each profile in profile_block_filtered computes its top comparisons
        :param profile_blocks_filtered: profile_blocks after block filtering
        :param block_index: a map that given a block ID returns the ID of the contained profiles
        :param max_id: maximum profile ID
        :param separator_id: maximum profile ID of each datasets (empty if its dirty)
        :param weight_type: type of weight to use
        :param profile_blocks_size_index: a map that contains for each profile the number of its blocks
        :param use_entropy: if true use the provided entropies to improve the edge weighting
        :param blocks_entropies: a map that contains for each block its entropy
        :param number_of_edges: global number of existings edges
        :param edges_per_profile: a map that contains for each profile the number of edges
        :param k number of top-k comparisons to retain
        :return: an RDD which contains for each profile_id the threshold
        """
        def compute_partition(partition):
            local_weights = [0] * (max_id + 1)
            neighbors = [0] * (max_id + 1)
            entropies = None
            if use_entropy:
                entropies = [0.0] * (max_id + 1)

            def inside_map(pb):
                comparisons = []
                neighbors_number = calc_cbs(pb, block_index, separator_id, use_entropy, blocks_entropies,
                                            local_weights, entropies, neighbors, True)
                calc_weights(pb, local_weights, neighbors, entropies, neighbors_number, block_index, separator_id,
                             weight_type, profile_blocks_size_index, use_entropy, number_of_edges, edges_per_profile)

                for i in range(0, neighbors_number):
                    comparisons.append((pb.profile_id, neighbors[i], local_weights[neighbors[i]]))

                do_reset(local_weights, neighbors, entropies, use_entropy, neighbors_number)
                if neighbors_number > k:
                    return (pb.profile_id,
                            [comparisons[x] for x in np.argpartition(comparisons, -k, axis=0)[:, 2][-k:]])
                else:
                    return pb.profile_id, comparisons

            return map(inside_map, partition)

        return profile_blocks_filtered.mapPartitions(compute_partition)

    def get_top_k(self, profile_block):
        """
        Computes top-k comparisons given a profile
        :param profile_block: list of blocks in which the profile is contained
        :return: top-k comparisons for the profile
        """
        neighbors_number = calc_cbs(profile_block, self.block_index, self.separator_ids, self.use_entropy,
                                    self.blocks_entropies, self.local_weights, self.entropies, self.neighbors, True)
        calc_weights(profile_block, self.local_weights, self.neighbors, self.entropies, neighbors_number,
                     self.block_index, self.separator_ids, self.weight_type, self.profile_blocks_size_index,
                     self.use_entropy, self.number_of_edges, self.edges_per_profile)

        comparisons = []

        for i in range(0, neighbors_number):
            if self.neighbors[i] not in self.visited:
                if profile_block.profile_id < self.neighbors[i]:
                    comparisons.append(
                        (profile_block.profile_id, self.neighbors[i], self.local_weights[self.neighbors[i]]))
                else:
                    comparisons.append(
                        (self.neighbors[i], profile_block.profile_id, self.local_weights[self.neighbors[i]]))

        do_reset(self.local_weights, self.neighbors, self.entropies, self.use_entropy, neighbors_number)

        if neighbors_number > self.k:
            return [comparisons[x] for x in np.argpartition(comparisons, -self.k, axis=0)[:, 2][-self.k:]]
        else:
            return comparisons

    def initialize(self):
        """
        Initialize the queues
        :return: None
        """
        res = PPS.calc_top_comparisons(self.profile_blocks, self.block_index, self.max_id, self.separator_ids,
                                       self.weight_type, self.profile_blocks_size_index, self.use_entropy,
                                       self.blocks_entropies, self.number_of_edges, self.edges_per_profile)

        self.profiles_list = PriorityQueue()
        list(map(self.profiles_list.put, res.flatMap(lambda x: x[0]).map(lambda x: (-x[1], x[0])).collect()))

        self.comparison_list = PriorityQueue()
        top_c = res.flatMap(lambda x: x[1]).map(lambda x: (-x[2], x[0], x[1])).collect()
        list(map(self.comparison_list.put, top_c))
        # The first emitted comparisons should not be emitted again
        for c in top_c:
            self.top_emitted[c[1]] = c[2]
        pass

    def get_next(self):
        """
        Returns the next top comparison
        :return: top comparison if there is any, otherwise (-1, -1, -1)
        """
        if self.comparison_list.empty() and self.profiles_list.not_empty:
            # Single mode
            if self.mini_batch < 0:
                pi = self.profiles_list.get()[1]
                self.visited.add(pi)
                pb = self.profile_blocks.filter(lambda x: x.profile_id == pi).collect()
                if len(pb) > 0:
                    pb = pb[0]
                    top_k = self.get_top_k(pb)
                    for c in top_k:
                        if c[1] != self.top_emitted[c[0]]:
                            self.comparison_list.put((-c[2], c[0], c[1]))
            else:
                # Mini batch mode
                # Gets the top profiles to explore
                pis = [self.profiles_list.get()[1] for _ in range(0, self.mini_batch) if self.profiles_list.not_empty]
                # For every profile gets the top-k comparisons
                pbf = self.profile_blocks.filter(lambda x: x.profile_id in pis)
                top_comp = PPS.get_top_comparisons(pbf, self.block_index, self.max_id, self.separator_ids,
                                                   self.weight_type, self.profile_blocks_size_index, self.use_entropy,
                                                   self.blocks_entropies, self.number_of_edges, self.edges_per_profile,
                                                   self.k).collectAsMap()
                # For every top profile
                for pi in pis:
                    # Set the profile as visited
                    self.visited.add(pi)

                    for c in top_comp[pi]:
                        # If the neighbor was not visited yet emit the comparison
                        if c[1] not in self.visited:
                            if c[0] < c[1]:
                                if c[1] != self.top_emitted[c[0]]:
                                    self.comparison_list.put((-c[2], c[0], c[1]))
                            else:
                                if c[0] != self.top_emitted[c[1]]:
                                    self.comparison_list.put((-c[2], c[1], c[0]))

        # Returns the top comparison
        if self.comparison_list.not_empty:
            return self.comparison_list.get()
        else:
            return -1, -1, -1


class GSPSN(object):
    """
    Implements the GSPSN method proposed in Simonini et. all "Schema-agnostic Progressive Entity Resolution"
    https://arxiv.org/pdf/1905.06385.pdf
    """
    @staticmethod
    def compute_profiles_list(profiles, max_profile_id, max_window_size, separator_id, blocking_strategy, **kwargs):
        """
        Computes the comparisons
        :param profiles: profiles list
        :param max_profile_id: max profiles id
        :param max_window_size: max window search size
        :param separator_id: separator id for clean-clean datasets
        :param blocking_strategy function to compute blocking keys
        :param **kwargs extra parameters for the blocking function
        :return: weighted comparisons
        """

        # Create a RDD of (token, profile ID)
        token_profile = profiles.map(lambda profile: (
            profile.profile_id, set(blocking_strategy(profile.attributes, **kwargs)))).flatMap(
            lambda x: [(t, x[0]) for t in x[1]])

        # Sort the rdd by the token then replace the token with an ID
        sorted_t = token_profile.sortBy(lambda x: x[0]).zipWithIndex().map(lambda x: (x[1], x[0][1]))

        sc = profiles.context
        # Index that given a profile returns its positions in the sorted list (i.e. the tokens it contains)
        position_index = sc.broadcast(sorted_t.map(lambda x: (x[1], x[0])).groupByKey().collectAsMap())
        # Index that for every position (token) provides the profile
        neighbor_list = sc.broadcast(sorted_t.collectAsMap())

        def compute_partition(part):
            neighbors = [0] * (max_profile_id + 1)
            cbs = [0] * (max_profile_id + 1)
            results = []

            def inside_map(profile):
                neighbors_num = 0
                # Position in which this profile appears
                positions = position_index.value[profile.profile_id]
                for pos in positions:
                    # For each windows
                    for window_size in range(1, max_window_size + 1):
                        # Explore the profiles at window distance before and after the current position
                        for i in [-1, 1]:
                            w = window_size * i

                            # If the position is valid (i.e. >0 and < max neighbor_list)
                            # if (w > 0 and (pos + w) < len(neighbor_list.value)) or (w < 0 and (pos + w) > 0):
                            if 0 <= (pos + w) < len(neighbor_list.value):
                                pi = neighbor_list.value[pos + w]
                                # If the neighbor is valid
                                if pi < profile.profile_id and (separator_id < 0 or (
                                        separator_id > 0 and pi <= separator_id and profile.profile_id > separator_id)):
                                    # Computes the CBS
                                    if cbs[pi] == 0:
                                        neighbors[neighbors_num] = pi
                                        neighbors_num += 1

                                    cbs[pi] += 1

                # Computes the comparisons
                for i in range(0, neighbors_num):
                    n_id = neighbors[i]
                    weight = cbs[n_id] / (len(positions) + len(position_index.value[n_id]) - cbs[n_id])
                    results.append((-weight, n_id, profile.profile_id))
                    cbs[n_id] = 0

                pass

            for p in part:
                inside_map(p)

            return results

        return profiles.mapPartitions(compute_partition).collect()

    def __init__(self, profiles, max_window_size, separator_id=-1):
        """
        Constructor
        :param profiles: profiles list
        :param max_window_size: max nearest neighbor distance
        :param separator_id: separator for clean-clean datasets
        """
        self.profiles = profiles
        self.max_window_size = max_window_size
        self.separator_id = separator_id
        self.max_profile_id = 0
        self.queue = None

    def initialize(self, blocking_strategy=BlockingKeysStrategies.token_blocking, **kwargs):
        """
        Initialize the queue
        :param blocking_strategy function to compute blocking keys
        :param **kwargs extra parameters for the blocking function
        :return: void method
        """
        self.max_profile_id = self.profiles.map(lambda p: p.profile_id).max()
        self.queue = PriorityQueue()

        list(map(self.queue.put, GSPSN.compute_profiles_list(self.profiles, self.max_profile_id, self.max_window_size,
                                                             self.separator_id, blocking_strategy, **kwargs)))

        pass

    def get_next(self):
        """
        Returns top comparison
        :return: top comparison if there is any, otherwise (-1, -1, -1)
        """
        if not self.queue.empty():
            return self.queue.get()
        else:
            return -1, -1, -1
