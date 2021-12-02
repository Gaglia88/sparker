from .pruning_utils import WeightTypes, ThresholdTypes, ComparisonTypes
from .common_node_pruning import compute_statistics, calc_cbs, calc_weights, do_reset
import math


class WNP(object):

    @staticmethod
    def get_all_profile_edges(profile_id, weights, neighbors, neighbors_number):
        """
        Returns all the edges
        :param profile_id: id of the profile
        :param weights: an array which contains the weight of each neighbor
        :param neighbors: an array which  contains the IDs of the neighbors
        :param neighbors_number: number of neighbors
        :return: a triplet that represents the weighted edges
        """
        edges = []
        for i in range(0, neighbors_number):
            neighbor_id = neighbors[i]
            neighbor_weight = weights[neighbor_id]
            edges.append((profile_id, neighbor_id, neighbor_weight))

        return edges

    @staticmethod
    def do_pruning(profile_id, weights, neighbors, neighbors_number, groundtruth, weight_type, comparison_type,
                   thresholds, chi2divider):
        """
        Performs the pruning
        :param profile_id: id of the profile
        :param weights: an array which contains the weight of each neighbor
        :param neighbors: an array which  contains the IDs of the neighbors
        :param neighbors_number: number of neighbors
        :param groundtruth: set of true matches (optional)
        :param weight_type: type of weight to us
        :param comparison_type: type of comparison to perform @see ComparisonTypes
        :param thresholds: local profile threshold
        :param chi2divider: used only in the chi_square weight method to compute the local threshold
        :return: a triplet that contains the number of retained edges, the edges that exists in the groundtruth,
                 and the retained edges
        """
        cont = 0
        gt_found = 0
        edges = []
        profile_threshold = thresholds.value[profile_id]
        if weight_type == WeightTypes.CHI_SQUARE:
            for i in range(0, neighbors_number):
                neighbor_id = neighbors[i]
                neighbor_threshold = thresholds.value[neighbor_id]
                neighbor_weight = weights[neighbor_id]
                threshold = math.sqrt(math.pow(neighbor_threshold, 2) + math.pow(profile_threshold, 2)) / chi2divider

                if neighbor_weight >= threshold:
                    cont += 1
                    if groundtruth is not None:
                        if (profile_id, neighbor_id) in groundtruth.value:
                            gt_found += 1

                    edges.append((profile_id, neighbor_id, neighbor_weight))
        else:
            for i in range(0, neighbors_number):
                neighbor_id = neighbors[i]
                neighbor_threshold = thresholds.value[neighbor_id]
                neighbor_weight = weights[neighbor_id]

                if (comparison_type == ComparisonTypes.AND and neighbor_weight >= neighbor_threshold
                        and neighbor_weight >= profile_threshold) or (comparison_type == ComparisonTypes.OR and (
                        neighbor_weight >= neighbor_threshold or neighbor_weight >= profile_threshold)):
                    cont += 1
                    if groundtruth is not None:
                        if (profile_id, neighbor_id) in groundtruth.value:
                            gt_found += 1

                    edges.append((profile_id, neighbor_id, neighbor_weight))

        return cont, gt_found, edges

    @staticmethod
    def get_edges(profile_blocks_filtered, block_index, max_id, separator_id, weight_type, profile_blocks_size_index,
                  use_entropy, blocks_entropies, number_of_edges, edges_per_profile):
        """
        Returns all the weighted edges
        :param profile_blocks_filtered: profile_blocks after block filtering
        :param block_index: a map that given a block ID returns the ID of the contained profiles
        :param max_id: maximum profile ID
        :param separator_id: maximum profile ID of each dataset (-1 if there is only one dataset)
        :param weight_type: type of weight to use see pruning_utils.WeightTypes
        :param profile_blocks_size_index: a map that contains for each profile the number of its blocks
        :param use_entropy: if true use the provided entropies to improve the edge weighting
        :param blocks_entropies: a map that contains for each block its entropy
        :param number_of_edges: global number of existings edges
        :param edges_per_profile: a map that contains for each profile the number of edges
        :return: an RDD that for each partition contains a triplet that contains the number of retained edges, the edges
                 that exists in the groundtruth, and the retained edges
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

                result = WNP.get_all_profile_edges(pb.profile_id, local_weights, neighbors, neighbors_number)
                do_reset(local_weights, neighbors, entropies, use_entropy, neighbors_number)
                return result

            return map(inside_map, partition)

        return profile_blocks_filtered.mapPartitions(compute_partition)

    @staticmethod
    def pruning(profile_blocks_filtered, block_index, max_id, separator_id, groundtruth, weight_type,
                profile_blocks_size_index, use_entropy, blocks_entropies, chi2divider, comparison_type, thresholds,
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
        :param chi2divider: used only in the chi_square weight method to compute the threshold
        :param comparison_type: type of comparison to perform @see ComparisonTypes
        :param thresholds: thresholds for every profile
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
                result = WNP.do_pruning(pb.profile_id, local_weights, neighbors, neighbors_number, groundtruth,
                                        weight_type, comparison_type, thresholds, chi2divider)
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
    def calc_thresholds(profile_blocks_filtered, block_index, max_id, separator_id, threshold_type, weight_type,
                        profile_blocks_size_index, use_entropy, blocks_entropies, number_of_edges, edges_per_profile):
        """
        For each profile computes the threshold
        :param profile_blocks_filtered: profile_blocks after block filtering
        :param block_index: a map that given a block ID returns the ID of the contained profiles
        :param max_id: maximum profile ID
        :param separator_id: maximum profile ID of each datasets (empty if its dirty)
        :param threshold_type: type of threshold to use
        :param weight_type: type of weight to use
        :param profile_blocks_size_index: a map that contains for each profile the number of its blocks
        :param use_entropy: if true use the provided entropies to improve the edge weighting
        :param blocks_entropies: a map that contains for each block its entropy
        :param number_of_edges: global number of existings edges
        :param edges_per_profile: a map that contains for each profile the number of edges
        :return: an RDD which contains for each profile_id the threshold
        """

        def compute_partition(partition):
            local_weights = [0] * (max_id+1)
            neighbors = [0] * (max_id+1)
            entropies = None
            if use_entropy:
                entropies = [0.0] * (max_id+1)

            def inside_map(pb):
                neighbors_number = calc_cbs(pb, block_index, separator_id, use_entropy, blocks_entropies,
                                            local_weights, entropies, neighbors, True)
                calc_weights(pb, local_weights, neighbors, entropies, neighbors_number, block_index, separator_id,
                             weight_type, profile_blocks_size_index, use_entropy, number_of_edges, edges_per_profile)
                threshold = WNP.calc_threshold(local_weights, neighbors, neighbors_number, threshold_type)
                do_reset(local_weights, neighbors, entropies, use_entropy, neighbors_number)
                return pb.profile_id, threshold

            return map(inside_map, partition)

        return profile_blocks_filtered.mapPartitions(compute_partition)

    @staticmethod
    def wnp(profile_blocks_filtered, block_index, max_id, separator_ids=None, groundtruth=None,
            threshold_type=ThresholdTypes.AVG, weight_type=WeightTypes.CBS, profile_blocks_size_index=None,
            use_entropy=False, blocks_entropies=None, chi2divider=2.0, comparison_type=ComparisonTypes.OR):
        """
        Performs the Weight Node Pruning. Returns an RDD that contains for each partition (number of edges after the
        pruning, number of true matches found in the groundtruth, list of edges)
        :param profile_blocks_filtered: profiles after filtering
        :param block_index: broadcasted blocking index
        :param max_id: highest profile ID
        :param separator_ids: id of the separators that identifies the different data sources
        :param groundtruth: groundtruth (optional)
        :param threshold_type: type of threshold from ThresholdTypes, default AVG
        :param weight_type: weight method from WeightTypes, default CBS
        :param profile_blocks_size_index: broadcast map that given a block ID returns its size, needed for ECBS, EJS,
               JS and chi_square weights. (optional)
        :param use_entropy: boolean, use entropy to weight the edges. Default false.
        :param blocks_entropies: broadcasted entropies, a map that given a block return its entropy. (optional)
        :param chi2divider: parameter d of blast, see the paper for more info. Default 2.0
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

        thresholds = sc.broadcast(
            WNP.calc_thresholds(profile_blocks_filtered, block_index, max_id, separator_ids, threshold_type,
                                weight_type, profile_blocks_size_index, use_entropy, blocks_entropies, number_of_edges,
                                edges_per_profile).collectAsMap())

        edges = WNP.pruning(profile_blocks_filtered, block_index, max_id, separator_ids, groundtruth, weight_type,
                            profile_blocks_size_index, use_entropy, blocks_entropies, chi2divider, comparison_type,
                            thresholds, number_of_edges, edges_per_profile)

        thresholds.unpersist()
        if edges_per_profile is not None:
            edges_per_profile.unpersist()

        return edges

    @staticmethod
    def get_all_edges(profile_blocks_filtered, block_index, max_id, separator_ids=None,
                      weight_type=WeightTypes.CBS, profile_blocks_size_index=None, use_entropy=False,
                      blocks_entropies=None):
        """
        Returns all the weighted edges without performing the pruning
        :param profile_blocks_filtered: profiles after filtering
        :param block_index: broadcasted blocking index
        :param max_id: highest profile ID
        :param separator_ids: id of the separators that identifies the different data sources
        :param weight_type: weight method from WeightTypes, default CBS
        :param profile_blocks_size_index: broadcast map that given a block ID returns its size, needed for ECBS, EJS,
               JS and chi_square weights. (optional)
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
                stats.map(lambda x: (x[1][0], x[1][1])).groupByKey().map(lambda x: (x[0], sum(x[1])))
                .collectAsMap())

        edges = WNP.get_edges(profile_blocks_filtered, block_index, max_id, separator_ids, weight_type,
                              profile_blocks_size_index, use_entropy, blocks_entropies, number_of_edges,
                              edges_per_profile)

        if edges_per_profile is not None:
            edges_per_profile.unpersist()

        return edges
