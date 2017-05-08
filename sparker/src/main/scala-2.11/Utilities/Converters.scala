package Utilities

import org.apache.spark.rdd.RDD
import DataStructures._
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
    * */
  def blocksToProfileBlocks(blocks : RDD[BlockAbstract]) : RDD[ProfileBlocks] = {
    //val createCombiner = (blockWithComparisonSize: BlockWithComparisonSize) => List(blockWithComparisonSize)
    //val combiner = (partialResult: List[BlockWithComparisonSize], newElement: BlockWithComparisonSize) => partialResult ++ List(newElement)
    //val merger = (partialResult1: List[BlockWithComparisonSize], partialResult2: List[BlockWithComparisonSize]) => partialResult1 ++ partialResult2

    def combiner(partialResult: List[BlockWithComparisonSize], newElement: BlockWithComparisonSize) : List[BlockWithComparisonSize] =
      newElement :: partialResult

    def merger(partialResult1: List[BlockWithComparisonSize], partialResult2: List[BlockWithComparisonSize]) : List[BlockWithComparisonSize] =
      partialResult1 ++ partialResult2

    val profilesPerBlocks = blocks.flatMap(blockIDProfileIDFromBlock).combineByKey(
      block => List(block),
      combiner,
      merger
    )

    profilesPerBlocks map(x => ProfileBlocks(x._1, x._2))
  }

  /**
    * Given a RDD of blocks, for each profile returns (profileID, [blocks in which appears])
    *
    * @param blocks RDD of blocks
    * @return for each profile in the blocks return (profileID, [blocks in which appears])
    * */
  def blocksToProfileRealBlocks(blocks : RDD[BlockAbstract]) : RDD[(Long, Array[BlockAbstract])] = {
    def combiner(partialResult: Array[BlockAbstract], newElement: BlockAbstract) : Array[BlockAbstract] =
      partialResult :+ newElement

    def merger(partialResult1: Array[BlockAbstract], partialResult2: Array[BlockAbstract]) : Array[BlockAbstract] =
      partialResult1 ++ partialResult2

    blocks.flatMap(ProfileIDFromBlock).combineByKey(
      block => Array(block),
      combiner,
      merger
    )
  }

  /**
    * Given a block return for each profile the tuple (profile ID, block ID)
    *
    * @param block generic block
    * @return a list that contains for each profile in the block (profileID, block)
    * */
  def ProfileIDFromBlock(block : BlockAbstract) : Iterable[(Long, BlockAbstract)] = {
    block.getAllProfiles.map((_, block))
    //block.getAllProfiles.map((_, BlockWithComparisonSize(block.blockID, block.getComparisonSize().toDouble/block.entropy)))
    //block.getAllProfiles.map((_, BlockWithComparisonSize(block.blockID, block.getComparisonSize().toDouble*block.entropy)))
    //block.getAllProfiles.map((_, BlockWithComparisonSize(block.blockID, block.entropy)))
  }

  /*
  def blocksToProfileBlocks(blocks : RDD[BlockAbstract]) : RDD[ProfileBlocks] = {
    val profilesPerBlocks = blocks.flatMap(blockIDProfileIDFromBlock).groupByKey()
    profilesPerBlocks map{
      p =>
        val profileID = p._1
        val blocks = p._2.toList
        ProfileBlocks(profileID, blocks)
    }
  }
  */

  /**
    * Given a RDD of profiles block return a RDD of blocks
 *
    * @param profilesBlocks RDD of profileBlock
    * @param separatorID max ID of the first dataset (if it is clean-clean context), default -1
    * @return RDD of blocks
    * */
  def profilesBlockToBlocks(profilesBlocks : RDD[ProfileBlocks], separatorID : Long = -1) : RDD[BlockAbstract] = {
    val blockIDProfileID = profilesBlocks flatMap {
      profileWithBlocks =>
        val profileID = profileWithBlocks.profileID
        profileWithBlocks.blocks map {
          BlockWithSize =>
            (BlockWithSize.blockID, profileID)
        }
    }

    val blocks = blockIDProfileID.groupByKey().filter(_._2.size > 1) map {
      block =>
        val blockID = block._1
        val profilesID = block._2.toList

        if (separatorID < 0){
          BlockDirty(blockID, (profilesID, Nil))
        }
        else{
          BlockClean(blockID, (profilesID.partition(_ <= separatorID)))
        }
    }

    blocks.filter(_.getComparisonSize() >=1).map(x => x)

  }

  /**
    * Given a block return a list that contains for each profile in the block (profileID, (blockID, number of comparison in the block))
    *
    * @param block block
    * @return a list that contains for each profile in the block (profileID, (blockID, number of comparison in the block))
    * */
  def blockIDProfileIDFromBlock(block : BlockAbstract) : Iterable[(Long, BlockWithComparisonSize)] = {
    val blockWithComparisonSize = BlockWithComparisonSize(block.blockID, block.getComparisonSize())
    //val blockWithComparisonSize = BlockWithComparisonSize(block.blockID, block.getComparisonSize()/Math.pow(10000000, block.entropy))
    block.getAllProfiles.map((_, blockWithComparisonSize))
  }
 }
