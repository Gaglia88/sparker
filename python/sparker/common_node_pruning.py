import numpy as np
import math
from .pruning_utils import WeightTypes, PruningUtils


def calc_chi_square(cbs, neighbor_num_blocks, current_profile_num_blocks, total_number_of_blocks):
    """
    Computes the chi-square.
    :param cbs: Double - common blocks
    :param neighbor_num_blocks: Double - number of blocks that the neighbor owns
    :param current_profile_num_blocks: Double - number of blocks that the current profile owns
    :param total_number_of_blocks: Double - number of existing blocks
    :return: chi-square
    """

    c_mat = np.eye(3, 3)
    weight = 0

    c_mat[0][0] = cbs
    c_mat[0][1] = neighbor_num_blocks - cbs
    c_mat[0][2] = neighbor_num_blocks

    c_mat[1][0] = current_profile_num_blocks - cbs
    c_mat[1][1] = total_number_of_blocks - (neighbor_num_blocks + current_profile_num_blocks - cbs)
    c_mat[1][2] = total_number_of_blocks - neighbor_num_blocks

    c_mat[2][0] = current_profile_num_blocks
    c_mat[2][1] = total_number_of_blocks - current_profile_num_blocks

    for i in range(0, 2):
        for j in range(0, 2):
            expected_value = (c_mat[i][2] * c_mat[2][j]) / total_number_of_blocks
            weight += math.pow((c_mat[i][j] - expected_value), 2) / expected_value

    return weight


def do_reset(weights, neighbors, entropies, use_entropy, neighbors_number):
    """
    Resets the arrays
    :param weights: Array[Double]: an array which contains the weight of each neighbor
    :param neighbors: Array[Int] : an array which contains the IDs of the neighbors
    :param entropies: Array[Double] : an array which contains the entropy of each neighbor
    :param use_entropy: boolean: if true use the provided entropies to improve the edge weighting
    :param neighbors_number: int : number of neighbors
    :return:
    """
    for i in range(0, neighbors_number):
        neighbor_id = neighbors[i]
        weights[neighbor_id] = 0
        if use_entropy:
            entropies[neighbor_id] = 0
    pass


def compute_statistics(profile_blocks_filtered, block_index, max_id, separator_id):
    """
    Computes the statistics needed to perform the EJS
    :param profile_blocks_filtered: RDD[Profile_blocks] : profile_blocks after block filtering
    :param block_index: Broadcast[Map[Long, Array[Set[Long]]]] : a map that given a block ID returns the IDs of the
           contained profiles
    :param max_id: Long : maximum profile ID
    :param separator_id: Array[Long] : maximum profile ID of each dataset
    :return: an RDD which contains (number of distinct edges, (profile_id, number of neighbors))
    """

    def compute_partition(partition):
        local_weights = [0] * (max_id+1)
        neighbors = [0] * (max_id+1)

        def comp_inside(pb):
            neighbors_number = 0
            distinct_edges = 0
            profile_id = pb.profile_id
            profile_blocks = pb.blocks
            for block in profile_blocks:
                block_id = block.block_id
                if block_id in block_index.value:
                    block_profiles = block_index.value[block_id]
                    if len(separator_id) == 0:
                        profiles_ids = block_profiles[0]
                    else:
                        profiles_ids = PruningUtils.get_all_neighbors(profile_id, block_profiles, separator_id)

                    for neighbor_id in profiles_ids:
                        neighbor_weight = local_weights[neighbor_id]
                        if neighbor_weight == 0:
                            local_weights[neighbor_id] = 1
                            neighbors[neighbors_number] = neighbor_id
                            neighbors_number += 1
                            if profile_id < neighbor_id:
                                distinct_edges += 1

            for i in range(0, neighbors_number):
                local_weights[i] = 0

            return distinct_edges, (profile_id, neighbors_number)

        return map(comp_inside, partition)

    return profile_blocks_filtered.mapPartitions(compute_partition)


def calc_cbs(pb, block_index, separator_id, use_entropy, blocks_entropies, weights, entropies, neighbors, first_step):
    """
    Computes the CBS for a single profile
    :param pb: Profile_blocks: the profile
    :param block_index: Broadcast[scala.collection.Map[Long, Array[Set[Long]]]] : a map that given a block ID returns
           the ID of the contained profiles
    :param separator_id: Array[Long] : List of the ids that separate the profiles coming from the different datasets
    :param use_entropy: boolean : if true use the provided entropies to improve the edge weighting
    :param blocks_entropies: Broadcast[scala.collection.Map[Long, Double]]: a map that contains for each block its
           entropy
    :param weights: Array[Double] : an array which the CBS values will be stored in
    :param entropies: Array[Double] : an array which the entropy of each neighbor will be stored in
    :param neighbors: Array[Int] : an array which the ID of the neighbors will be stored in
    :param first_step: Boolean: if it is set to true all the edges will be computed, otherwise only the edges that have
           an ID greater than current profile ID.
    :return: number of neighbors
    """
    neighbors_number = 0
    profile_id = pb.profile_id
    profile_blocks = pb.blocks
    for block in profile_blocks:
        block_id = block.block_id
        if block_id in block_index.value:
            block_profiles = block_index.value[block_id]
            if len(separator_id) == 0:
                profiles_ids = block_profiles[0]
            else:
                profiles_ids = PruningUtils.get_all_neighbors(profile_id, block_profiles, separator_id)

            block_entropy = 0
            if use_entropy:
                if block_id in blocks_entropies.value:
                    block_entropy = blocks_entropies.value[block_id]

            for neighbor_id in profiles_ids:
                if (profile_id < neighbor_id) or first_step:
                    weights[neighbor_id] += 1
                    if use_entropy:
                        entropies[neighbor_id] += block_entropy
                    if weights[neighbor_id] == 1:
                        neighbors[neighbors_number] = neighbor_id
                        neighbors_number += 1
    return neighbors_number


def calc_weights(pb, weights, neighbors, entropies, neighbors_number, block_index, separator_id, weight_type,
                 profile_blocks_size_index, use_entropy, number_of_edges, edges_per_profile):
    """
    Computes the weights for each neighbor
    :param pb: Profile_blocks : the profile
    :param weights: Array[Double] : an array which contains the CBS values
    :param neighbors: Array[Int] : an array which contains the IDs of the neighbors
    :param entropies: Array[Double] : an array which contains the entropy of each neighbor
    :param neighbors_number: Int : number of neighbors
    :param block_index: Broadcast[scala.collection.Map[Long, Array[Set[Long]]]] : a map that given a block ID returns
           the ID of the contained profiles
    :param separator_id: Array[Long] : maximum profile ID of the first dataset (-1 if there is only one dataset)
    :param weight_type: String :  type of weight to use see PruningUtils.WeightTypes
    :param profile_blocks_size_index: Broadcast[scala.collection.Map[Long, Int]] : a map that contains for each profile
           the number of its blocks
    :param use_entropy: Boolean : if true use the provided entropies to improve the edge weighting
    :param number_of_edges: Double : global number of existing edges
    :param edges_per_profile: Broadcast[scala.collection.Map[Long, Double]] : a maps that contains for each profile the
           number of edges
    :return: number of neighbors
    """
    if weight_type == WeightTypes.CHI_SQUARE:
        number_of_profile_blocks = len(pb.blocks)
        total_number_of_blocks = float(len(block_index.value))
        for i in range(0, neighbors_number):
            neighbor_id = neighbors[i]
            if use_entropy:
                weights[neighbor_id] = calc_chi_square(weights[neighbor_id],
                                                       profile_blocks_size_index.value[neighbor_id],
                                                       number_of_profile_blocks,
                                                       total_number_of_blocks) * entropies[neighbor_id]
            else:
                weights[neighbor_id] = calc_chi_square(weights[neighbor_id],
                                                       profile_blocks_size_index.value[neighbor_id],
                                                       number_of_profile_blocks, total_number_of_blocks)

    elif weight_type == WeightTypes.ARCS:
        profile_blocks = pb.blocks
        for block in profile_blocks:
            block_id = block.block_id
            if block_id in block_index.value:
                block_profiles = block_index.value[block_id]
                if len(separator_id) == 0:
                    lb = len(block_profiles[0])
                    comparisons = float(lb * (lb - 1))
                else:
                    comparisons = float(np.prod(list(map(lambda x: len(x), block_profiles))))

                for i in range(0, neighbors_number):
                    neighbor_id = neighbors[i]
                    weights[neighbor_id] = weights[neighbor_id] / comparisons
                    if use_entropy:
                        weights[neighbor_id] = weights[neighbor_id] * entropies[neighbor_id]

    elif weight_type == WeightTypes.JS:
        number_of_profile_blocks = len(pb.blocks)
        for i in range(0, neighbors_number):
            neighbor_id = neighbors[i]
            common_blocks = weights[neighbor_id]
            if use_entropy:
                js = (common_blocks / (
                        number_of_profile_blocks + profile_blocks_size_index.value[neighbor_id] - common_blocks)) * \
                     entropies[neighbor_id]
            else:
                js = common_blocks / (number_of_profile_blocks + profile_blocks_size_index.value[neighbor_id] -
                                      common_blocks)
            weights[neighbor_id] = js

    elif weight_type == WeightTypes.EJS:
        profile_number_neighbors = 0.00000000001
        if pb.profile_id in edges_per_profile.value:
            profile_number_neighbors = edges_per_profile.value[pb.profile_id] + 0.00000000001

        number_of_profile_blocks = len(pb.blocks)

        for i in range(0, neighbors_number):
            neighbor_id = neighbors[i]
            common_blocks = weights[neighbor_id]
            ejs = 0
            en = 0.00000000001
            if neighbor_id in edges_per_profile.value:
                en = edges_per_profile.value[neighbor_id]

            if use_entropy:
                try:
                    ejs = ((common_blocks / (
                            number_of_profile_blocks + profile_blocks_size_index.value[neighbor_id] - common_blocks)) *
                           entropies[neighbor_id]) * math.log10(
                        number_of_edges / en * math.log10(number_of_edges / profile_number_neighbors))
                except Exception:
                    pass
            else:
                try:
                    ejs = (common_blocks / (number_of_profile_blocks + profile_blocks_size_index.value[
                        neighbor_id] - common_blocks)) * math.log10(
                        number_of_edges / (en * math.log10(number_of_edges / profile_number_neighbors)))
                except Exception:
                    pass

            weights[neighbor_id] = ejs
    elif weight_type == WeightTypes.ECBS:
        blocks_number = len(block_index.value)
        number_of_profile_blocks = len(pb.blocks)
        for i in range(0, neighbors_number):
            neighbor_id = neighbors[i]
            common_blocks = weights[neighbor_id]
            if use_entropy:
                ecbs = common_blocks * math.log10(blocks_number / number_of_profile_blocks) * math.log10(
                    blocks_number / profile_blocks_size_index.value[neighbor_id]) * entropies[neighbor_id]
            else:
                ecbs = common_blocks * math.log10(blocks_number / number_of_profile_blocks) * math.log10(
                    blocks_number / profile_blocks_size_index.value[neighbor_id])

            weights[neighbor_id] = ecbs
    pass
