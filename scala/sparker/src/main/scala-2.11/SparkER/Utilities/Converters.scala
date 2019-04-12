package SparkER.Utilities

import SparkER.BlockBuildingMethods.TokenBlocking
import org.apache.spark.rdd.RDD
import SparkER.DataStructures._
import org.apache.spark.partial.PartialResult

/**
  * Contains differents methods
  * to convert to differents DataStructures types.
  *
  * @author Luca Gagliardelli
  * @since 2016/12/08
  */
object Converters {

  /**
    * Given a RDD of blocks return a RDD of profiles block
    *
    * @param blocks RDD of blocks
    * @return RDD of profile blocks
    **/
  def blocksToProfileBlocks(blocks: RDD[BlockAbstract]): RDD[ProfileBlocks] = {
    val profilesPerBlocks = blocks.flatMap(blockIDProfileIDFromBlock).groupByKey()
    profilesPerBlocks map (x => ProfileBlocks(x._1, x._2.toSet))
  }

  /**
    * Given a block return a list that contains for each profile in the block (profileID, (blockID, number of comparison in the block))
    *
    * @param block block
    * @return a list that contains for each profile in the block (profileID, (blockID, number of comparison in the block))
    **/
  def blockIDProfileIDFromBlock(block: BlockAbstract): Iterable[(Long, BlockWithComparisonSize)] = {
    val blockWithComparisonSize = BlockWithComparisonSize(block.blockID, block.getComparisonSize())
    block.getAllProfiles.map((_, blockWithComparisonSize))
  }


  /**
    * Given a RDD of profiles block return a RDD of blocks
    *
    * @param profilesBlocks RDD of profileBlock
    * @param separatorIDs max ID of the first dataset (if it is clean-clean context), default -1
    * @return RDD of blocks
    **/
  def profilesBlockToBlocks(profilesBlocks: RDD[ProfileBlocks], separatorIDs: Array[Long] = Array.emptyLongArray): RDD[BlockAbstract] = {

    val blockIDProfileID = profilesBlocks flatMap {
      profileWithBlocks =>
        val profileID = profileWithBlocks.profileID
        profileWithBlocks.blocks map {
          BlockWithSize =>
            (BlockWithSize.blockID, profileID)
        }
    }

    val blocks = blockIDProfileID.groupByKey().map {
      block =>
        val blockID = block._1
        val profilesID = block._2.toSet

        if (separatorIDs.isEmpty) {
          BlockDirty(blockID, Array(profilesID))
        }
        else {
          BlockClean(blockID, TokenBlocking.separateProfiles(profilesID, separatorIDs))
        }
    }

    blocks.filter(_.getComparisonSize() > 0).map(x => x)

  }
}
