package BlockRefinementMethods

import DataStructures.{BlockWithComparisonSize, ProfileBlocks}
import Utilities.BoundedPriorityQueue
import org.apache.spark.rdd.RDD

/**
 * Created by Luca
 * on 08/12/16.
 * Implements the block filtering
 */
object BlockFiltering {

  /** *
    * Implements the block filtering
    * Keeps the N*r most relevant blocks for each profile. E.g. if a profiles appears in 10 blocks, and r=0.8, the
    * algorithm sorts the block by their number of comparisons, and then keep the 8 (10*0.8) blocks with the lowest
    * comparisons.
    *
    * @param profilesWithBlocks the RDD that contains each profile with the list of blocks in which appears
    * @param r blocking filter factor [0, 1], means the rate of blocks to keep for each profile
    * @param minCardinality minimum number of blocks to keep
    **/
  def blockFiltering(profilesWithBlocks: RDD[ProfileBlocks], r: Double, minCardinality: Int = 1): RDD[ProfileBlocks] = {
    profilesWithBlocks map {
      profileWithBlocks =>
        val profileID = profileWithBlocks.profileID
        /*val blocksSortedByComparisons = profileWithBlocks.blocks.sortWith(_.comparisons < _.comparisons)
        val blocksToKeep = Math.round(blocksSortedByComparisons.size * r).toInt
        ProfileBlocks(profileID, blocksSortedByComparisons.take(blocksToKeep))*/
        val blocksNumberToKeep = math.max(Math.round(profileWithBlocks.blocks.size * r).toInt, minCardinality)
        val blocksToKeep = new BoundedPriorityQueue[BlockWithComparisonSize](blocksNumberToKeep)
        blocksToKeep ++= profileWithBlocks.blocks
        ProfileBlocks(profileID, blocksToKeep.toList)
    }
  }
}
