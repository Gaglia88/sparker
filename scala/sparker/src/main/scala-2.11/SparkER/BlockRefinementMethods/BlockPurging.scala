package SparkER.BlockRefinementMethods

import SparkER.DataStructures.BlockAbstract
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Implements the block purging.
  * @author Luca Gagliardelli
  * @since 2016/12/08
  */
object BlockPurging {

  /**
    * Performs the block purging
    * Removes the block with the highest number of comparisons
    *
    * @param blocks blocks to purge
    * @param smoothFactor smooth factor for tuning the purging level
    *
    * @return blocks purged
    * */
  def blockPurging(blocks : RDD[BlockAbstract], smoothFactor : Double) : RDD[BlockAbstract] = {
    //For each block produces the tuple (number of comparison, block size)
    val blocksComparisonsAndSizes = blocks map {
      block => (block.getComparisonSize(), block.size)
    }

    //For each tuple (number of comparison, block size) produces (number of comparison, (number of comparison, block size))
    val blockComparisonsAndSizesPerComparisonLevel = blocksComparisonsAndSizes map {
      blockComparisonAndSize => (blockComparisonAndSize._1, blockComparisonAndSize)
    }

    //Group the tuple for the number of comparison and sum the numbers of comparison and the blocks size
    val totalNumberOfComparisonsAndSizePerComparisonLevel = blockComparisonsAndSizesPerComparisonLevel.reduceByKey((x, y) => (x._1+y._1, x._2+y._2))

    //For each level of comparison contains the total number of comparisons and the total number of profiles; it is sorted by comparison level
    val totalNumberOfComparisonsAndSizePerComparisonLevelSorted = totalNumberOfComparisonsAndSizePerComparisonLevel.sortBy(_._1).collect().toList

    //Sums to each level of comparisons the values of the all precedents levels
    val totalNumberOfComparisonsAndSizePerComparisonLevelSortedAdded = sumPrecedentLevels(totalNumberOfComparisonsAndSizePerComparisonLevelSorted)

    //Calculate the maximum numbers of allowed comparisons
    val maximumNumberOfComparisonAllowed = calcMaxComparisonNumber(totalNumberOfComparisonsAndSizePerComparisonLevelSortedAdded.toArray, smoothFactor)

    val log = org.apache.log4j.Logger.getRootLogger

    log.info("SPARKER - BLOCK PURGING COMPARISONS MAX "+maximumNumberOfComparisonAllowed)

    //Retains only the blocks with a comparison level less or equal the calculated one
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
  def sumPrecedentLevels(input : Iterable[(Double, (Double, Double))]) : Iterable[(Double, (Double, Double))] = {
    /*val out : ListBuffer[(Double, (Double, Double))] = new ListBuffer[(Double, (Double, Double))]()
    out ++= input

    for(i <- 0 to input.size-2){
      out(i+1) = (out(i+1)._1, (out(i+1)._2._1+out(i)._2._1, out(i+1)._2._2+out(i)._2._2))
    }

    out.toList*/
    if(input.isEmpty){
      input
    }
    else{
      input.tail.scanLeft(input.head)((acc, x) => (x._1, (x._2._1+acc._2._1, x._2._2+acc._2._2)))
    }
  }

  /**
    * Given as input the totalNumberOfComparisonsAndSizePerComparisonLevelSortedAdded and the smooth factor
    * compute the maximum number of comparisons to keep
    */
  def calcMaxComparisonNumber(input : Array[(Double, (Double, Double))], smoothFactor : Double) : Double = {
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

      currentSize = input(i)._1     //Current comparison level
      currentBC = input(i)._2._2    //Current comparison level number of elements
      currentCC = input(i)._2._1    //Current comparison level number of comparisons

      if (currentBC * previousCC < smoothFactor * currentCC * previousBC) {
        return previousSize
      }
    }

    previousSize
  }
}
