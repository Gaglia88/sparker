package BlockRefinementMethods

import DataStructures.BlockAbstract
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Implements the block purging.
  * Created by Luca on 08/12/2016.
  */
object BlockPurging {

  /**
    * Performs the block purging
    * Removes the block with the highest number of comparisons
    *
    * @param blocks blocks to purge
    * @param smoothFactor smooth factor for tuning the purging level
    * */
  def blockPurging(blocks : RDD[BlockAbstract], smoothFactor : Double) : RDD[BlockAbstract] = {
    //For each block produces the tuple (number of comparison, block size)
    val blocksComparisonsAndSizes = blocks map {
      block => (block.getComparisonSize(), block.size)
    }

    //For each tuple (number of comparison, block size) produces (number of comparison, (number of comparison, block size))
    val blockComparisonsAndSizesPerComparisonLevel = blocksComparisonsAndSizes map {
      blockComparisonAndSize => (blockComparisonAndSize._1, (blockComparisonAndSize))
    }

    //Group the tuple for the number of comparison and sum the numbers of comparison and the blocks size
    val totalNumberOfComparisonsAndSizePerComparisonLevel = blockComparisonsAndSizesPerComparisonLevel.reduceByKey((x, y) => (x._1+y._1, x._2+y._2))

    //For each level of comparison contains the total number of comparisons and the the total number of profiles; it is sorted by comparison level
    val totalNumberOfComparisonsAndSizePerComparisonLevelSorted = totalNumberOfComparisonsAndSizePerComparisonLevel.sortBy(_._1).collect().toList

    //Sums to each level the values of it precedents level
    val totalNumberOfComparisonsAndSizePerComparisonLevelSortedAdded = sumPrecedentLevels(totalNumberOfComparisonsAndSizePerComparisonLevelSorted)

    //Calculate the maximum numbers of allowed comparisons
    val maximumNumberOfComparisonAllowed = calcMaxComparisonNumber(totalNumberOfComparisonsAndSizePerComparisonLevelSortedAdded, smoothFactor)

    //Retains only the blocks with a comparison level <= the calculated one
    blocks filter{
      block =>
        block.getComparisonSize() <= maximumNumberOfComparisonAllowed
    }
  }


  /**
    * Given as input the totalNumberOfComparisonsAndSizePerComparisonLevelSorted sum the number of comparison and the size
    * with all the precedent levels.
    * E.g. if we have [(1, (100, 10)), (2, (25, 5))] it returns [(1, (100, 10)), (2, (125, 15))]
    */
  def sumPrecedentLevels(input : Iterable[(Double, (Double, Double))]) : List[(Double, (Double, Double))] = {
    val out : ListBuffer[(Double, (Double, Double))] = new ListBuffer[(Double, (Double, Double))]()
    out ++= input

    for(i <- 0 to input.size-2){
      out(i+1) = (out(i+1)._1, (out(i+1)._2._1+out(i)._2._1, out(i+1)._2._2+out(i)._2._2))
    }

    out.toList
  }

  /**
    * Given as input the totalNumberOfComparisonsAndSizePerComparisonLevelSortedAdded and the smooth factor
    * compute the maximum number of comparisons to keep
    */
  def calcMaxComparisonNumber(input : List[(Double, (Double, Double))], smoothFactor : Double) : Double = {
    var currentBC : Double = 0
    var currentCC : Double = 0
    var currentSize : Double = 0
    var previousBC : Double = 0
    var previousCC : Double = 0
    var previousSize : Double = 0
    val arraySize = input.size

    for(i <- arraySize-1 to 0 by -1) {
      previousSize = currentSize
      previousBC = currentBC
      previousCC = currentCC

      currentSize = input(i)._1
      currentBC = input(i)._2._2
      currentCC = input(i)._2._1

      if (currentBC * previousCC < smoothFactor * currentCC * previousBC) {
        return previousSize
      }
    }

    return previousSize
  }
}
