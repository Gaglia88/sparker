from py_sparker.objects import ProfileBlocks
from py_sparker.converters import Converters


class BlockPurging(object):
    @staticmethod
    def blockPurging(blocks, smoothFactor):
        """
        Performs the block purging.
        Returns an RDD with the purged blocks.

        Parameters
        ----------
        blocks : RDD[Block]
            RDD of blocks (clean or dirty)
        smoothFactor : float
            smooth factor, minimum value 1. More the value is low, higher is the purging. A typical value is 1.005
        """
        blocksComparisonsAndSizes = blocks.map(lambda block: (block.getComparisonSize(), block.getSize()))
        blockComparisonsAndSizesPerComparisonLevel = blocksComparisonsAndSizes.map(lambda x: (x[0], x))
        totalNumberOfComparisonsAndSizePerComparisonLevel = blockComparisonsAndSizesPerComparisonLevel.reduceByKey(
            lambda x, y: (x[0] + y[0], x[1] + y[1]))
        totalNumberOfComparisonsAndSizePerComparisonLevelSorted = totalNumberOfComparisonsAndSizePerComparisonLevel.sortBy(
            lambda x: x[0]).collect()

        def sumPrecedentLevels(b):
            for i in range(0, len(b) - 1):
                b[i + 1] = (b[i + 1][0], (b[i][1][0] + b[i + 1][1][0], b[i][1][1] + b[i + 1][1][1]))
            return b

        totalNumberOfComparisonsAndSizePerComparisonLevelSortedAdded = sumPrecedentLevels(
            totalNumberOfComparisonsAndSizePerComparisonLevelSorted)

        def calcMaxComparisonNumber(inputD, smoothFactor):
            currentBC = 0
            currentCC = 0
            currentSize = 0
            previousBC = 0
            previousCC = 0
            previousSize = 0

            for i in range(len(inputD) - 1, -1, -1):
                previousSize = currentSize
                previousBC = currentBC
                previousCC = currentCC

                currentSize = inputD[i][0]
                currentBC = inputD[i][1][1]
                currentCC = inputD[i][1][0]

                if currentBC * previousCC < smoothFactor * currentCC * previousBC:
                    return previousSize

            return previousSize

        maximumNumberOfComparisonAllowed = calcMaxComparisonNumber(
            totalNumberOfComparisonsAndSizePerComparisonLevelSortedAdded, smoothFactor)

        return blocks.filter(lambda block: block.getComparisonSize() <= maximumNumberOfComparisonAllowed)


class BlockFiltering(object):
    @staticmethod
    def blockFiltering(profilesWithBlocks, r):
        """
            Performs the block filtering.
            Returns an RDD of filtered ProfileBlocks

            Parameters
            ----------
            profilesWithBlocks : RDD[ProfileBlocks]
                RDD of ProfileBlocks
                filtering factor. In range ]0, 1[, a typical value is 0.8
        """

        def applyFiltering(profileWithBlock):
            sortedBlocks = sorted(profileWithBlock.blocks, key=lambda x: x.comparisons)
            # blocksToKeep = sortedBlocks[0:int(round(len(sortedBlocks)*r))]
            # return ProfileBlocks(profileWithBlock.profileID, set(blocksToKeep))
            index = int(round(len(sortedBlocks) * r))

            if index > 0:
                index -= 1

            if index >= len(sortedBlocks):
                index = len(sortedBlocks) - 1

            cnum = sortedBlocks[index].comparisons
            blocksToKeep = filter(lambda x: x.comparisons <= cnum, sortedBlocks)
            return ProfileBlocks(profileWithBlock.profileID, set(blocksToKeep))

        return profilesWithBlocks.map(applyFiltering)

    @staticmethod
    def blockFilteringQuick(blocks, r, separatorIDs=[]):
        """
            Performs the block filtering.
            Returns a triple(profileBlocks, profileBlocksFiltered, blocksAfterFilterFiltering)
            where profileBlocks is the RDD of ProfileBlocks calculated from blocks;
            profileBlocksFiltered is the RDD of filtered ProfileBlocks.
            blocksAfterFilterFiltering is a the RDD of blocks generated from profileBlocksFiltered

            Parameters
            ----------
            blocks : RDD[Blocks]
                RDD of blocks, clear or dirty
            r
                filtering factor. In range ]0, 1[, a typical value is 0.8
            separatorIDs : [int]
                Id of separators (if the ER is clean) that identifies how to split profiles across the different sources
        """
        profileBlocks = Converters.blocksToProfileBlocks(blocks)
        profileBlocksFiltered = BlockFiltering.blockFiltering(profileBlocks, r)
        blocksAfterFilterFiltering = Converters.profilesBlockToBlocks(profileBlocksFiltered, separatorIDs)
        return (profileBlocks, profileBlocksFiltered, blocksAfterFilterFiltering)
