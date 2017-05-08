package BlockRefinementMethods

import DataStructures.{BlockWithComparisonSize, ProfileBlocks}
import Utilities.{BoundedPriorityQueue, BoundedPriorityQueue2}
import org.apache.spark.rdd.RDD

/**
  * Implements the block filtering
  *
  * @author Luca Gagliardelli
  * @since 2016/12/08
 */
object BlockFiltering {

  /** *
    * Performs the block filtering
    * Keeps the N*r most relevant blocks for each profile. E.g. if a profiles appears in 10 blocks, and r=0.8, the
    * algorithm sorts the block by their number of comparisons, and then keep the 8 (10*0.8) blocks with the lowest
    * comparisons.
    *
    * @param profilesWithBlocks the RDD that contains each profile with the list of blocks in which appears
    * @param r blocking filter factor [0, 1], means the rate of blocks to keep for each profile
    * @param minCardinality minimum number of blocks to keep, the default is 1
    * @return profile block filtered
    **/
  def blockFiltering(profilesWithBlocks: RDD[ProfileBlocks], r: Double, minCardinality: Int = 1): RDD[ProfileBlocks] = {
    profilesWithBlocks map {
      profileWithBlocks =>
        /*val blocksSortedByComparisons = profileWithBlocks.blocks.sortWith(_.comparisons < _.comparisons)
        val blocksToKeep = Math.round(blocksSortedByComparisons.size * r).toInt
        ProfileBlocks(profileID, blocksSortedByComparisons.take(blocksToKeep))*/
        val blocksNumberToKeep = math.max(Math.round(profileWithBlocks.blocks.size * r).toInt, minCardinality)//Calculates the number of blocks to keep
        val blocksToKeep = new BoundedPriorityQueue[BlockWithComparisonSize](blocksNumberToKeep) //Creates a new priority queue of the size of the blocks to keeps
        blocksToKeep ++= profileWithBlocks.blocks //Adds all blocks to the queue, the bigger one will be automatically dropped
        ProfileBlocks(profileWithBlocks.profileID, blocksToKeep.toList) //Return the new blocks
    }
  }

  def blockFiltering2(profilesWithBlocks: RDD[ProfileBlocks], r: Double, minCardinality: Int = 1): RDD[ProfileBlocks] = {
    profilesWithBlocks mapPartitions {
      partition =>
        val blocksToKeep = new BoundedPriorityQueue2[BlockWithComparisonSize](1) //Creates a new priority queue
        partition.map{
          profileWithBlocks =>
            val blocksNumberToKeep = math.max(Math.round(profileWithBlocks.blocks.size * r).toInt, minCardinality)//Calculates the number of blocks to keep
            blocksToKeep.resize(blocksNumberToKeep) //Set the number of elements to keep to the priority queue
            blocksToKeep ++= profileWithBlocks.blocks //Adds all blocks to the queue, the bigger one will be automatically dropped
            ProfileBlocks(profileWithBlocks.profileID, blocksToKeep.toList) //Return the new blocks
        }
    }
  }
}
