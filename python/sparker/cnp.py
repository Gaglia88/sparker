from .pruning_utils import WeightTypes, ComparisonTypes, PruningUtils
from .common_node_pruning import compute_statistics, calc_cbs, calc_weights, do_reset
import math
import numpy as np


class CNP(object):
    
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
    def pruning(profile_blocks_filtered, block_index, max_id, separator_ids, groundtruth, comparison_type,
                retained_neighbors):
        """
        Performs the pruning
        :param profile_blocks_filtered: profile_blocks after block filtering
        :param block_index: a map that given a block ID returns the ID of the contained profiles
        :param max_id: maximum profile ID
        :param separator_ids: maximum profile ID of each dataset (-1 if there is only one dataset)
        :param groundtruth: set of true matches
        :param retained_neighbors: list of top-k neighbors for every profile
        :param comparison_type: type of comparison to perform @see ComparisonTypes
        :return: an RDD that for each partition contains a triplet that contains the number of retained edges,
        the edges that exists in the groundtruth, and the retained edges
        """

        def compute_partition(partition):
            visited = [False] * (max_id+1)
            neighbors = [0] * (max_id+1)

            def inside_map(pb):
                num_edges = 0
                gt_num = 0
                edges = []
                num_neighbors = 0

                profile_id = pb.profile_id
                profile_blocks = pb.blocks
                profile_retained_neighbors = retained_neighbors.value[profile_id]

                for block in profile_blocks:
                    block_id = block.block_id
                    if block_id in block_index.value:
                        block_profiles = block_index.value[block_id]
                        if len(separator_ids) == 0:
                            profiles_ids = block_profiles[0]
                        else:
                            profiles_ids = PruningUtils.get_all_neighbors(profile_id, block_profiles, separator_ids)
                        for neighbor_id in profiles_ids:
                            if profile_id < neighbor_id and not visited[neighbor_id]:
                                neighbor_retained_neighbors = retained_neighbors.value[neighbor_id]
                                visited[neighbor_id] = True
                                neighbors[num_neighbors] = neighbor_id
                                num_neighbors += 1

                                if comparison_type == ComparisonTypes.OR \
                                   and ((profile_id in neighbor_retained_neighbors)
                                        or (neighbor_id in profile_retained_neighbors)):
                                    num_edges += 1
                                    if groundtruth is not None and ((profile_id, neighbor_id) in groundtruth.value):
                                        gt_num += 1
                                    edges.append((profile_id, neighbor_id, 0))
                                elif comparison_type == ComparisonTypes.AND \
                                        and (profile_id in neighbor_retained_neighbors) \
                                        and (neighbor_id in profile_retained_neighbors):
                                    num_edges += 1
                                    if groundtruth is not None and ((profile_id, neighbor_id) in groundtruth.value):
                                        gt_num += 1
                                    edges.append((profile_id, neighbor_id, 0))

                for i in range(0, num_neighbors):
                    visited[neighbors[i]] = False

                return num_edges, gt_num, edges

            return map(inside_map, partition)

        return profile_blocks_filtered.mapPartitions(compute_partition)

    @staticmethod
    def calc_retained_neighbors(profile_blocks_filtered, block_index, max_id, separator_id, weight_type,
                                profile_blocks_size_index, use_entropy, blocks_entropies, number_of_edges,
                                edges_per_profile, cnp_threshold):
        """
        For every profiles compute the top-k neighbors
        :param profile_blocks_filtered:
        :param block_index:
        :param max_id:
        :param separator_id:
        :param weight_type:
        :param profile_blocks_size_index:
        :param use_entropy:
        :param blocks_entropies:
        :param number_of_edges:
        :param edges_per_profile:
        :param cnp_threshold:
        :return:
        """

        def compute_partition(partition):
            local_weights = [0] * (max_id + 1)
            neighbors = [0] * (max_id + 1)
            entropies = None
            if use_entropy:
                entropies = [0.0] * (max_id + 1)

            def inside_map(pb):
                neighbors_number = calc_cbs(pb, block_index, separator_id, use_entropy, blocks_entropies,
                                            local_weights, entropies, neighbors, True)
                calc_weights(pb, local_weights, neighbors, entropies, neighbors_number, block_index, separator_id,
                             weight_type, profile_blocks_size_index, use_entropy, number_of_edges, edges_per_profile)

                weighted_neighbors = [(local_weights[neighbors[i]], neighbors[i]) for i in range(0, neighbors_number)]

                if cnp_threshold >= len(weighted_neighbors):
                    sorted_neighbor = set([t[1] for t in weighted_neighbors])
                else:
                    top_k = np.argpartition(weighted_neighbors, -cnp_threshold, axis=0)[:, 0][-cnp_threshold:]
                    sorted_neighbor = set([weighted_neighbors[i][1] for i in top_k])

                do_reset(local_weights, neighbors, entropies, use_entropy, neighbors_number)
                return pb.profile_id, sorted_neighbor

            return map(inside_map, partition)

        return profile_blocks_filtered.mapPartitions(compute_partition)

    @staticmethod
    def cnp(blocks, number_of_profiles, profile_blocks_filtered, block_index, max_id, separator_ids=None,
            groundtruth=None, weight_type=WeightTypes.CBS, profile_blocks_size_index=None,
            use_entropy=False, blocks_entropies=None, comparison_type=ComparisonTypes.OR):
        """
        Performs the Cardinality Node Pruning. Returns an RDD that contains for each partition (number of edges after
        the pruning, number of true matches found in the groundtruth, list of edges)
        :param blocks: blocking collection
        :param number_of_profiles: profiles number
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

        cnp_threshold = CNP.compute_cnp_threshold(blocks, number_of_profiles)

        sc = profile_blocks_filtered.context
        number_of_edges = 0
        edges_per_profile = None

        if weight_type == WeightTypes.EJS:
            stats = compute_statistics(profile_blocks_filtered, block_index, max_id, separator_ids)
            number_of_edges = stats.map(lambda x: x[0]).sum()
            edges_per_profile = sc.broadcast(
                stats.map(lambda x: (x[1][0], x[1][1])).groupByKey().map(lambda x: (x[0], sum(x[1])))
                .collectAsMap())

        retained_neighbors = sc.broadcast(CNP.calc_retained_neighbors(profile_blocks_filtered, block_index, max_id,
                                                                      separator_ids, weight_type,
                                                                      profile_blocks_size_index, use_entropy,
                                                                      blocks_entropies, number_of_edges,
                                                                      edges_per_profile, cnp_threshold)
                                          .collectAsMap())

        edges = CNP.pruning(profile_blocks_filtered, block_index, max_id, separator_ids, groundtruth, comparison_type,
                            retained_neighbors)

        if edges_per_profile is not None:
            edges_per_profile.unpersist()

        return edges

    @staticmethod
    def compute_cnp_threshold(blocks, number_of_profiles):
        """
        Computes the CNP threshold.
        Number of neighbours to keep for each record.
        :param blocks: block collection
        :param number_of_profiles: number of profiles
        :return:
        """
        num_elements = blocks.map(lambda b: len(b.get_all_profiles())).sum()
        return int(math.floor((num_elements / number_of_profiles) - 1))
