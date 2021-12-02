from .pruning_utils import WeightTypes
from .common_node_pruning import compute_statistics, calc_cbs, calc_weights, do_reset


class WEP(object):
    @staticmethod
    def do_pruning(profile_id, weights, neighbors, neighbors_number, groundtruth, threshold):
        """
        Performs the pruning
        :param profile_id: id of the profile
        :param weights: an array which contains the weight of each neighbor
        :param neighbors: an array which  contains the IDs of the neighbors
        :param neighbors_number: number of neighbors
        :param groundtruth: set of true matches (optional)
        :param threshold: global threshold used for pruning
        :return: a triplet that contains the number of retained edges, the edges that exists in the groundtruth, and
                 the retained edges
        """
        cont = 0
        gt_found = 0
        edges = []

        for i in range(0, neighbors_number):
            neighbor_id = neighbors[i]
            neighbor_weight = weights[neighbor_id]

            if neighbor_weight >= threshold:
                cont += 1
                if groundtruth is not None:
                    if (profile_id, neighbor_id) in groundtruth.value:
                        gt_found += 1

                edges.append((profile_id, neighbor_id, neighbor_weight))

        return cont, gt_found, edges

    @staticmethod
    def pruning(profile_blocks_filtered, block_index, max_id, separator_id, groundtruth, weight_type,
                profile_blocks_size_index, use_entropy, blocks_entropies, threshold,
                number_of_edges, edges_per_profile):
        """
        Performs the pruning
        :param profile_blocks_filtered: profile_blocks after block filtering
        :param block_index: a map that given a block ID returns the ID of the contained profiles
        :param max_id: maximum profile ID
        :param separator_id: maximum profile ID of each dataset (-1 if there is only one dataset)
        :param groundtruth: set of true matches
        :param weight_type: type of weight to use see pruning_utils.WeightTypes
        :param profile_blocks_size_index: a map that contains for each profile the number of its blocks
        :param use_entropy: if true use the provided entropies to improve the edge weighting
        :param blocks_entropies: a map that contains for each block its entropy
        :param threshold: global threshold used for pruning
        :param number_of_edges: global number of existing edges
        :param edges_per_profile: a map that contains for each profile the number of edges
        :return: an RDD that for each partition contains a triplet that contains the number of retained edges, the
                 edges that exists in the groundtruth, and the retained edges
        """

        def compute_partition(partition):
            local_weights = [0] * (max_id + 1)
            neighbors = [0] * (max_id + 1)
            entropies = None
            if use_entropy:
                entropies = [0.0] * (max_id + 1)

            def inside_map(pb):
                neighbors_number = calc_cbs(pb, block_index, separator_id, use_entropy, blocks_entropies, local_weights,
                                            entropies, neighbors, False)
                calc_weights(pb, local_weights, neighbors, entropies, neighbors_number, block_index, separator_id,
                             weight_type, profile_blocks_size_index, use_entropy, number_of_edges, edges_per_profile)
                result = WEP.do_pruning(pb.profile_id, local_weights, neighbors, neighbors_number, groundtruth,
                                        threshold)
                do_reset(local_weights, neighbors, entropies, use_entropy, neighbors_number)
                return result

            return map(inside_map, partition)

        return profile_blocks_filtered.mapPartitions(compute_partition)

    @staticmethod
    def sum_weights(weights, neighbors, neighbors_number):
        """
        Computes the threshold for a profile
        :param weights: an array which contains the weight of each neighbor
        :param neighbors: an array which contains the IDs of the neighbors
        :param neighbors_number: number of neighbors
        :return: the profile' threshold
        """
        acc = 0
        for i in range(0, neighbors_number):
            neighbor_id = neighbors[i]
            acc += weights[neighbor_id]

        return acc

    @staticmethod
    def calc_global_threshold(profile_blocks_filtered, block_index, max_id, separator_id, weight_type,
                              profile_blocks_size_index, use_entropy, blocks_entropies, number_of_edges,
                              edges_per_profile):
        """
        For each profile computes the threshold
        :param profile_blocks_filtered: profile_blocks after block filtering
        :param block_index: a map that given a block ID returns the ID of the contained profiles
        :param max_id: maximum profile ID
        :param separator_id: maximum profile ID of each datasets (empty if its dirty)
        :param weight_type: type of weight to use
        :param profile_blocks_size_index: a map that contains for each profile the number of its blocks
        :param use_entropy: if true use the provided entropies to improve the edge weighting
        :param blocks_entropies: a map that contains for each block its entropy
        :param number_of_edges: global number of existing edges
        :param edges_per_profile: a map that contains for each profile the number of edges
        :return: an RDD which contains for each profile_id the threshold
        """
        def compute_partition(partition):
            local_weights = [0] * (max_id + 1)
            neighbors = [0] * (max_id + 1)
            entropies = None
            if use_entropy:
                entropies = [0.0] * (max_id + 1)

            def inside_map(pb):
                neighbors_number = calc_cbs(pb, block_index, separator_id, use_entropy, blocks_entropies, local_weights,
                                            entropies, neighbors, True)
                calc_weights(pb, local_weights, neighbors, entropies, neighbors_number, block_index, separator_id,
                             weight_type, profile_blocks_size_index, use_entropy, number_of_edges, edges_per_profile)
                weight_sum = WEP.sum_weights(local_weights, neighbors, neighbors_number)
                do_reset(local_weights, neighbors, entropies, use_entropy, neighbors_number)
                return neighbors_number, weight_sum

            return map(inside_map, partition)

        sums = profile_blocks_filtered.mapPartitions(compute_partition).reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]))

        return sums[1] / sums[0]

    @staticmethod
    def wep(profile_blocks_filtered, block_index, max_id, separator_ids=None, groundtruth=None,
            weight_type=WeightTypes.CBS, profile_blocks_size_index=None, use_entropy=False, blocks_entropies=None):
        """
        Performs the Weight Edge Pruning. Returns an RDD that contains for each partition (number of edges after the
        pruning, number of true matches found in the groundtruth, list of edges).
        The average of all weights of the edges is used as pruning threshold.
        :param profile_blocks_filtered: profiles after filtering
        :param block_index: broadcasted blocking index
        :param max_id: highest profile ID
        :param separator_ids: id of the separators that identifies the different data sources
        :param groundtruth: groundtruth (optional)
        :param weight_type: method used to weight the edges
        :param profile_blocks_size_index: broadcast map that given a block ID returns its size,
               needed for ECBS, EJS, JS and chi_square weights. (optional)
        :param use_entropy: boolean, use entropy to weight the edges. Default false.
        :param blocks_entropies: broadcasted entropies, a map that given a block return its entropy. (optional)
        :return: Returns an RDD that contains for each partition (number of edges after the pruning, number of true
                 matches found in the groundtruth, list of edges)
        """
        if separator_ids is None:
            separator_ids = []

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
        number_of_edges = 0
        edges_per_profile = None

        if weight_type == WeightTypes.EJS:
            stats = compute_statistics(profile_blocks_filtered, block_index, max_id, separator_ids)
            number_of_edges = stats.map(lambda x: x[0]).sum()
            edges_per_profile = sc.broadcast(
                stats.map(lambda x: (x[1][0], x[1][1])).groupByKey().map(
                    lambda x: (x[0], sum(x[1]))).collectAsMap())

        threshold = WEP.calc_global_threshold(profile_blocks_filtered, block_index, max_id, separator_ids, weight_type,
                                              profile_blocks_size_index, use_entropy, blocks_entropies, number_of_edges,
                                              edges_per_profile)

        edges = WEP.pruning(profile_blocks_filtered, block_index, max_id, separator_ids, groundtruth,
                            weight_type, profile_blocks_size_index, use_entropy, blocks_entropies,
                            threshold, number_of_edges, edges_per_profile)

        if edges_per_profile is not None:
            edges_per_profile.unpersist()

        return edges
