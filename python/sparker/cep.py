from .pruning_utils import WeightTypes, ThresholdTypes, ComparisonTypes
from .common_node_pruning import compute_statistics, calc_cbs, calc_weights, do_reset
import math


class CEP(object):

    @staticmethod
    def do_pruning(profile_id, weights, neighbors, neighbors_number, groundtruth, threshold, edges_to_keep):
        """
        Performs the pruning
        :param profile_id: id of the profile
        :param weights: an array which contains the weight of each neighbor
        :param neighbors: an array which  contains the IDs of the neighbors
        :param neighbors_number: number of neighbors
        :param groundtruth: set of true matches (optional)
        :param threshold: global threshold
        :param edges_to_keep: number of edges to keep with a weight equal to global threshold for every profile
        :return: a triplet that contains the number of retained edges, the edges that exists in the groundtruth,
                 and the retained edges
        """
        num_edges = 0
        gt_found = 0
        edges = []
        neighbors_to_keep = 0
        if profile_id in edges_to_keep.value:
            neighbors_to_keep = edges_to_keep.value[profile_id]

        for i in range(0, neighbors_number):
            neighbor_id = neighbors[i]
            neighbor_weight = weights[neighbor_id]
            if (neighbor_weight > threshold) or (neighbor_weight == threshold and neighbors_to_keep > 0):
                num_edges += 1
                if groundtruth is not None:
                    if (profile_id, neighbor_id) in groundtruth.value:
                        gt_found += 1
                edges.append((profile_id, neighbor_id, neighbor_weight))

                if neighbor_weight == threshold:
                    neighbors_to_keep -= 1

        return num_edges, gt_found, edges

    @staticmethod
    def pruning(profile_blocks_filtered, block_index, max_id, separator_id, groundtruth, weight_type,
                profile_blocks_size_index, use_entropy, blocks_entropies, threshold, edges_to_keep, number_of_edges,
                edges_per_profile):
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
        :param threshold: global threshold
        :param edges_to_keep: number of edges to keep with a weight equal to global threshold for every profile
        :param number_of_edges: global number of existing edges
        :param edges_per_profile: a map that contains for each profile the number of edges
        :return: an RDD that for each partition contains a triplet that contains the number of retained edges,
        the edges that exists in the groundtruth, and the retained edges
        """

        def compute_partition(partition):
            local_weights = [0] * (max_id+1)
            neighbors = [0] * (max_id+1)
            entropies = None
            if use_entropy:
                entropies = [0.0] * (max_id+1)

            def inside_map(pb):
                neighbors_number = calc_cbs(pb, block_index, separator_id, use_entropy, blocks_entropies,
                                            local_weights, entropies, neighbors, False)
                calc_weights(pb, local_weights, neighbors, entropies, neighbors_number, block_index, separator_id,
                             weight_type, profile_blocks_size_index, use_entropy, number_of_edges, edges_per_profile)
                result = CEP.do_pruning(pb.profile_id, local_weights, neighbors, neighbors_number, groundtruth,
                                        threshold, edges_to_keep)
                do_reset(local_weights, neighbors, entropies, use_entropy, neighbors_number)
                return result

            return map(inside_map, partition)

        return profile_blocks_filtered.mapPartitions(compute_partition)

    @staticmethod
    def calc_threshold(weights, neighbors, neighbors_number, threshold_type):
        """
        Computes the threshold for a profile
        :param weights: an array which contains the weight of each neighbor
        :param neighbors: an array which contains the IDs of the neighbors
        :param neighbors_number: number of neighbors
        :param threshold_type: type of threshold to use
        :return: the profile' threshold
        """
        acc = 0
        for i in range(0, neighbors_number):
            neighbor_id = neighbors[i]
            if threshold_type == ThresholdTypes.AVG:
                acc += weights[neighbor_id]
            elif threshold_type == ThresholdTypes.MAX_FRACT_2 and weights[neighbor_id] > acc:
                acc = weights[neighbor_id]
            else:
                pass

        if threshold_type == ThresholdTypes.AVG:
            if neighbors_number > 0:
                acc /= float(neighbors_number)
        else:
            acc /= 2.0

        return acc

    @staticmethod
    def calc_weights_frequencies(weights, neighbors, neighbors_number):
        """
        Computes the number of neighbors for every weight.
        :param weights: list of weights for every neighbor
        :param neighbors: list of neighbors
        :param neighbors_number: number of neighbors
        :return: number of neighbors for every weight
        """
        out = {}
        for i in range(0, neighbors_number):
            weight = weights[neighbors[i]]
            if weight in out:
                out[weight] += 1
            else:
                out[weight] = 1
        return list(out.items())

    @staticmethod
    def calc_thresholds(profile_blocks_filtered,
                        block_index,
                        max_id,
                        separator_ids,
                        weight_type,
                        profile_blocks_size_index,
                        use_entropy,
                        blocks_entropies,
                        number_of_edges,
                        edges_per_profile,
                        num_of_edges_to_keep
                        ):
        """
         Given the number of edges to keep, for each profile calculate the weights of the neighbors.
         For each weight, its global frequency is calculated, i.e. for every weight how many edges are associated.
         Then, starting from the highest weight, it compute the weight that let to reach a sufficient number of edges,
         i.e. the weight that adding the edges of that weight would exceed the maximum number of edges to keep.
         For that threshold, for each profile it calculates how many edges must be kept.

         What it returns is:
           - The weight for which I have to hold all the edges> of that weight
           - A list showing for each profile how many edges it must keep with that weight

        :param profile_blocks_filtered:
        :param block_index:
        :param max_id:
        :param separator_ids:
        :param weight_type:
        :param profile_blocks_size_index:
        :param use_entropy:
        :param blocks_entropies:
        :param number_of_edges:
        :param edges_per_profile:
        :param num_of_edges_to_keep:
        :return:
        """

        def compute_partition(part):
            local_weights = [0.0] * (max_id + 1)
            neighbors = [0] * (max_id + 1)
            entropies = None
            if use_entropy:
                entropies = [0.0] * (max_id + 1)

            def inside_map(pb):
                neighbors_number = calc_cbs(pb, block_index, separator_ids, use_entropy, blocks_entropies,
                                            local_weights, entropies, neighbors, False)
                calc_weights(pb, local_weights, neighbors, entropies, neighbors_number, block_index, separator_ids,
                             weight_type, profile_blocks_size_index, use_entropy, number_of_edges, edges_per_profile)
                local_freq = CEP.calc_weights_frequencies(local_weights, neighbors, neighbors_number)
                do_reset(local_weights, neighbors, entropies, use_entropy, neighbors_number)
                return pb.profile_id, local_freq

            return map(inside_map, part)

        partial_freq = profile_blocks_filtered.mapPartitions(compute_partition)

        # Sorted list of tuples that contain for every edge weight the number of associated edges
        frequencies = sorted(
            partial_freq.flatMap(lambda x: x[1]).groupByKey().map(lambda x: (x[0], sum(x[1]))).collect(),
            key=lambda x: -x[0])

        sums = 0
        cont = 0
        # Continue while the sum of frequencies is lower than the number of edges to keep
        while sums < num_of_edges_to_keep and cont < len(frequencies):
            sums += frequencies[cont][1]
            cont += 1

        # All the edges with a weight greater than this threshold must be kept
        threshold = frequencies[cont - 1][0]
        # Number of edges to keep in the last level
        remaining_edges_to_keep = frequencies[cont - 1][1] - (sums - num_of_edges_to_keep)

        # For every profile, counts the number of edges with a weight equal to the threshold
        stats = partial_freq.map(lambda pr: (pr[0], list(map(lambda x: x[1],
                                                             filter(lambda x: x[0] == threshold, pr[1])))))\
            .filter(lambda x: len(x[1]) > 0).map(lambda x: (x[0], x[1][0])).collect()

        # For every profile, reports the number of edges to keep with a weight equal to the threshold
        num_edges_to_keep = []
        for p in stats:
            # Stop when there are no more edges to keep
            if remaining_edges_to_keep <= 0:
                break

            if p[1] > remaining_edges_to_keep:
                num_edges_to_keep.append((p[0], remaining_edges_to_keep))
            else:
                num_edges_to_keep.append(p)

            remaining_edges_to_keep -= p[1]

        return threshold, dict(num_edges_to_keep)

    @staticmethod
    def cep(profile_blocks_filtered, block_index, max_id, separator_ids=None, groundtruth=None,
            weight_type=WeightTypes.CBS, profile_blocks_size_index=None,
            use_entropy=False, blocks_entropies=None, comparison_type=ComparisonTypes.OR):
        """
        Performs the Weight Node Pruning. Returns an RDD that contains for each partition (number of edges after the
        pruning, number of true matches found in the groundtruth, list of edges)
        :param profile_blocks_filtered: profiles after filtering
        :param block_index: broadcasted blocking index
        :param max_id: highest profile ID
        :param separator_ids: id of the separators that identifies the different data sources
        :param groundtruth: groundtruth (optional)
        :param weight_type: weight method from WeightTypes, default CBS
        :param profile_blocks_size_index: broadcast map that given a block ID returns its size, needed for ECBS, EJS,
               JS and chi_square weights. (optional)
        :param use_entropy: boolean, use entropy to weight the edges. Default false.
        :param blocks_entropies: broadcasted entropies, a map that given a block return its entropy. (optional)
        :param comparison_type: pruning strategy from ComparisonTypes, default OR
        :return: Returns an RDD that contains for each partition (number of edges after the pruning, number of true
                 matches found in the groundtruth, list of edges)
        """
        if separator_ids is None:
            separator_ids = []
        valid_weights = [WeightTypes.CBS, WeightTypes.JS, WeightTypes.CHI_SQUARE, WeightTypes.ARCS,
                         WeightTypes.ECBS, WeightTypes.EJS]
        valid_comp_types = [ComparisonTypes.OR, ComparisonTypes.AND]

        if weight_type not in valid_weights:
            raise ValueError("Please provide a valid Weight_type, " + str(weight_type) + " is not an acceptable value!")

        if comparison_type not in valid_comp_types:
            raise ValueError(
                "Please provide a valid Comparison_type, " + str(comparison_type) + " is not an acceptable value!")

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
                stats.map(lambda x: (x[1][0], x[1][1])).groupByKey().map(lambda x: (x[0], sum(x[1])))
                .collectAsMap())

        num_of_edges_to_keep = math.floor(sum([sum(map(lambda y: len(y), block_index.value[x]))
                                          for x in block_index.value]) / 2.0)

        threshold, num_edges_to_keep = CEP.calc_thresholds(profile_blocks_filtered, block_index, max_id, separator_ids,
                                                           weight_type, profile_blocks_size_index, use_entropy,
                                                           blocks_entropies, number_of_edges, edges_per_profile,
                                                           num_of_edges_to_keep)

        edges_to_keep = sc.broadcast(num_edges_to_keep)

        edges = CEP.pruning(profile_blocks_filtered, block_index, max_id, separator_ids, groundtruth, weight_type,
                            profile_blocks_size_index, use_entropy, blocks_entropies, threshold, edges_to_keep,
                            number_of_edges, edges_per_profile)

        edges_to_keep.unpersist()
        if edges_per_profile is not None:
            edges_per_profile.unpersist()

        return edges
