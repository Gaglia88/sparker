from .objects import BlockClean, BlockDirty, BlockWithComparisonSize, ProfileBlocks
from .blocking_utils import BlockingUtils


class Converters(object):
    """
        Contains converters to convert RDDs to different object types
    """

    @staticmethod
    def convert_groundtruth(groundtruth, profiles1, profiles2=None):
        """
        Convert the groundtruth by replacing the original ids with those
        given by Spark
        :param groundtruth: RDD that represents the groundtruth
        :param profiles1: first dataset profiles
        :param profiles2: second dataset profiles (leave it blank if the dataset is dirty)
        :return: a set of pairs that represent the new groundtruth
        """

        sc = groundtruth.context
        if profiles2 is not None:
            real_ids1 = sc.broadcast(profiles1.map(lambda p: (p.original_id, p.profile_id)).collectAsMap())
            real_ids2 = sc.broadcast(profiles2.map(lambda p: (p.original_id, p.profile_id)).collectAsMap())

            def convert_clean(gt_entry):
                if gt_entry.first_entity_id in real_ids1.value and gt_entry.second_entity_id in real_ids2.value:
                    first = real_ids1.value[gt_entry.first_entity_id]
                    second = real_ids2.value[gt_entry.second_entity_id]
                    if first < second:
                        return first, second
                    else:
                        return second, first
                else:
                    return -1, -1

            new_gt = set(groundtruth.map(convert_clean).filter(lambda x: x[0] >= 0).collect())
            real_ids1.unpersist()
            real_ids2.unpersist()
            return new_gt
        else:
            real_ids = sc.broadcast(profiles1.map(lambda p: (p.original_id, p.profile_id)).collectAsMap())

            def convert_dirty(gt_entry):
                if gt_entry.first_entity_id in real_ids.value and gt_entry.second_entity_id in real_ids.value:
                    first = real_ids.value[gt_entry.first_entity_id]
                    second = real_ids.value[gt_entry.second_entity_id]
                    if first < second:
                        return first, second
                    else:
                        return second, first
                else:
                    return -1, -1

            new_gt = set(groundtruth.map(convert_dirty).filter(lambda x: x[0] >= 0).collect())
            real_ids.unpersist()
            return new_gt

    @staticmethod
    def block_id_profile_id_from_block(block):
        """
        Given a block (clean or dirty), produces a list of (p_i, Block_with_comparison_size)
        where p_i is a profile indexed in the block.

        Parameters
        ----------
        block : Block_clean|Block_dirty
            A block (clean or dirty)
        """
        block_with_comparison_size = BlockWithComparisonSize(block.block_id, block.get_comparison_size())
        return map(lambda p: (p, block_with_comparison_size), block.get_all_profiles())

    @staticmethod
    def blocks_to_profile_blocks(blocks):
        """
        Given an RDD of blocks (clean or dirty), produces an RDD of Profile_blocks.

        Parameters
        ----------
        blocks : RDD[Block_clean|Block_dirty]
            An RDD of blocks (clean or dirty)
        """
        profiles_per_blocks = blocks.flatMap(Converters.block_id_profile_id_from_block).groupByKey()
        return profiles_per_blocks.map(lambda b: ProfileBlocks(b[0], set(b[1])))

    @staticmethod
    def profiles_block_to_blocks(profiles_blocks, separator_ids=None):
        """
        Given an RDD of Profile_blocks, produces an RDD of blocks (clean or dirty).

        Parameters
        ----------
        profiles_blocks : RDD[Profile_blocks]
            An RDD of profile blocks
        separator_ids : [int]
            Id of separators (if the ER is clean) that identifies how to split profiles across the different sources
        """

        if separator_ids is None:
            separator_ids = []

        def get_block(data):
            block_id = data[0]
            profiles_ids = set(data[1])
            if len(separator_ids) == 0:
                return BlockDirty(block_id, [profiles_ids])
            else:
                return BlockClean(block_id, BlockingUtils.separate_profiles(profiles_ids, separator_ids))

        block_id_profile_id = profiles_blocks.flatMap(lambda pb: [(b.block_id, pb.profile_id) for b in pb.blocks])
        blocks = block_id_profile_id.groupByKey().map(get_block)
        return blocks.filter(lambda b: b.get_comparison_size() > 0)
