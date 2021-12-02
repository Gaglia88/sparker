from .objects import ProfileBlocks
from .converters import Converters


class BlockPurging(object):
    @staticmethod
    def block_purging(blocks, smooth_factor):
        """
        Performs the block purging.
        Returns an RDD with the purged blocks.

        Parameters
        ----------
        blocks : RDD[Block]
            RDD of blocks (clean or dirty)
        smooth_factor : float
            smooth factor, minimum value 1. More the value is low, higher is the purging. A typical value is 1.005
        """
        blocks_comparisons_and_sizes = blocks.map(lambda block: (block.get_comparison_size(), block.get_size()))
        block_comparisons_and_sizes_per_comparison_level = blocks_comparisons_and_sizes.map(lambda x: (x[0], x))
        total_number_of_comparisons_and_size_per_comparison_level = block_comparisons_and_sizes_per_comparison_level\
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
        total_number_of_comparisons_and_size_per_comparison_level_sorted = \
            total_number_of_comparisons_and_size_per_comparison_level.sortBy(lambda x: x[0]).collect()

        def sum_precedent_levels(b):
            for i in range(0, len(b) - 1):
                b[i + 1] = (b[i + 1][0], (b[i][1][0] + b[i + 1][1][0], b[i][1][1] + b[i + 1][1][1]))
            return b

        total_number_of_comparisons_and_size_per_comparison_level_sorted_added = sum_precedent_levels(
            total_number_of_comparisons_and_size_per_comparison_level_sorted)

        def calc_max_comparison_number(input_d, smooth_factor_int):
            current_bc = 0
            current_cc = 0
            current_size = 0
            previous_size = 0

            for i in range(len(input_d) - 1, -1, -1):
                previous_size = current_size
                previous_bc = current_bc
                previous_cc = current_cc

                current_size = input_d[i][0]
                current_bc = input_d[i][1][1]
                current_cc = input_d[i][1][0]

                if current_bc * previous_cc < smooth_factor_int * current_cc * previous_bc:
                    return previous_size

            return previous_size

        maximum_number_of_comparison_allowed = calc_max_comparison_number(
            total_number_of_comparisons_and_size_per_comparison_level_sorted_added, smooth_factor)

        return blocks.filter(lambda block: block.get_comparison_size() <= maximum_number_of_comparison_allowed)


class BlockFiltering(object):
    @staticmethod
    def block_filtering(profiles_with_blocks, r):
        """
            Performs the block filtering.
            Returns an RDD of filtered Profile_blocks

            Parameters
            ----------
            profiles_with_blocks : RDD[Profile_blocks]
                RDD of Profile_blocks
            r : Float
                filtering factor. In range ]0, 1[, a typical value is 0.8
        """

        def apply_filtering(profile_with_block):
            sorted_blocks = sorted(profile_with_block.blocks, key=lambda x: x.comparisons)
            # blocks_to_keep = sorted_blocks[0:int(round(len(sorted_blocks)*r))]
            # return Profile_blocks(profile_with_block.profile_id, set(blocks_to_keep))
            index = int(round(len(sorted_blocks) * r))

            if index > 0:
                index -= 1

            if index >= len(sorted_blocks):
                index = len(sorted_blocks) - 1

            cnum = sorted_blocks[index].comparisons
            blocks_to_keep = filter(lambda x: x.comparisons <= cnum, sorted_blocks)
            return ProfileBlocks(profile_with_block.profile_id, set(blocks_to_keep))

        return profiles_with_blocks.map(apply_filtering)

    @staticmethod
    def block_filtering_quick(blocks, r, separator_ids=None):
        """
            Performs the block filtering.
            Returns a triple(profile_blocks, profile_blocks_filtered, blocks_after_filter_filtering)
            where profile_blocks is the RDD of Profile_blocks calculated from blocks;
            profile_blocks_filtered is the RDD of filtered Profile_blocks.
            blocks_after_filter_filtering is a the RDD of blocks generated from profile_blocks_filtered

            Parameters
            ----------
            blocks : RDD[Blocks]
                RDD of blocks, clear or dirty
            r
                filtering factor. In range ]0, 1[, a typical value is 0.8
            separator_ids : [int]
                Id of separators (if the ER is clean) that identifies how to split profiles across the different sources
        """
        if separator_ids is None:
            separator_ids = []

        profile_blocks = Converters.blocks_to_profile_blocks(blocks)
        profile_blocks_filtered = BlockFiltering.block_filtering(profile_blocks, r)
        blocks_after_filter_filtering = Converters.profiles_block_to_blocks(profile_blocks_filtered, separator_ids)
        return profile_blocks, profile_blocks_filtered, blocks_after_filter_filtering
