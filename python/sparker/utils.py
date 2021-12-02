from .pruning_utils import PruningUtils
from .converters import Converters


class Utils(object):
    @staticmethod
    def get_ngrams(string, n):
        """
        Given a string returns its n-grams of size n.
        :param string: string to divide in n-grams
        :param n: ngrams size
        :return: iterator of n-grams
        """
        padding = "".join(["_"] * (n - 1))
        string = padding + string + padding
        for i in range(0, len(string) - (n - 1)):
            yield string[i:i + n]

    @staticmethod
    def get_statistics(blocks, max_id, gt, separator_ids=None):
        """
        Returns recall, precision and number of edges computed from the provided block collection
        :param blocks: blocks
        :param max_id: highest profile ID
        :param gt: list of pairs which represents the groundtruth
        :param separator_ids: id of the separators that identifies the different data sources
        :return: recall, precision and number of edges
        """
        if separator_ids is None:
            separator_ids = []
        sc = blocks.context

        profiles_blocks = Converters.blocks_to_profile_blocks(blocks)
        block_index_map = blocks.map(lambda b: (b.block_id, b.profiles)).collectAsMap()
        block_index = sc.broadcast(block_index_map)
        groundtruth = sc.broadcast(gt)
        
        num_matches = sc.accumulator(0)
        num_edges = sc.accumulator(0)

        def process_partition(part):
            explored = [False] * (max_id+1)
            neighbors = [0] * (max_id+1)
            
            def process_profile_block(pb):
                profile_id = pb.profile_id
                profile_blocks = pb.blocks
                neighbor_num = 0
                
                for block in profile_blocks:        
                    block_id = block.block_id
                    if block_id in block_index.value:
                        block_profiles = block_index.value[block_id]
                        if len(separator_ids) == 0:
                            profiles_ids = block_profiles[0]
                        else:
                            profiles_ids = PruningUtils.get_all_neighbors(profile_id, block_profiles, separator_ids)

                        for neighbor_id in profiles_ids:
                            if profile_id < neighbor_id:
                                if not explored[neighbor_id]:
                                    explored[neighbor_id] = True
                                    neighbors[neighbor_num] = neighbor_id
                                    neighbor_num += 1
                                    
                                    num_edges.add(1)
                                    if (profile_id, neighbor_id) in groundtruth.value:
                                        num_matches.add(1)
            
                for i in range(0, neighbor_num):
                    explored[neighbors[i]] = False

            list(map(process_profile_block, part))
        
        profiles_blocks.foreachPartition(process_partition)
        
        block_index.unpersist()
        groundtruth.unpersist()
        
        recall = num_matches.value / len(gt)
        precision = num_matches.value / num_edges.value
        
        return recall, precision, num_edges.value
