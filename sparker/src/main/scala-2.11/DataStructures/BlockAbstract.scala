package DataStructures

import WeightingMethods.WeightMethodTrait
import org.apache.spark.broadcast.Broadcast

/**
 * Represents a generic block.
 * @author Giovanni Simonini
 * @since 2016/12/07
 */
trait BlockAbstract  extends Ordered[BlockAbstract]{
  /** Id of the block */
  val blockID : Long
  /** Entropy of the block */
  var entropy : Double
  /** Id of the profiles contained in the block */
  val profiles: (List[Long], List[Long])

  /** Return the number of entities indexed in the block */
  def size = (profiles._1.size + profiles._2.size).toDouble

  /* Return the number of comparisons entailed by this block */
  def getComparisonSize(): Double

  /* Return the comparisons contained in the block */
  def getComparisons(): List[UnweightedEdge] {}

  //TODO: a cosa serve? mettere un commento
  def getComparison(p1: ProfileBlocks, p2: ProfileBlocks, weightMethod: WeightMethodTrait): WeightedEdge = {

    def getMinCommonBlock(b1: List[BlockWithComparisonSize], b2: List[BlockWithComparisonSize]): Long = {
      if(b1 == Nil || b2 == Nil) -1
      else {
        b1.head.blockID match {
          case x if x == b2.head.blockID => b1.head.blockID
          case x if x < b2.head.blockID => getMinCommonBlock(b1.tail, b2)
          case _ => getMinCommonBlock(b1, b2.tail)
        }
      }
    }

    if(getMinCommonBlock(p1.blocks, p2.blocks) == blockID) {
      weightMethod.getWeightEdge(p1, p2)
    }
    else null
  }

  /**
    * Returns the weighted comparison of the elements contained in this block
    * @param profileBlocks broadcast variable that contains the list of profileBlocks
    * @param weightMethod function to be used to weight the edges
    * */
  def getWeightedComparisons(profileBlocks: Broadcast[Map[Long,ProfileBlocks]], weightMethod: WeightMethodTrait): List[WeightedEdge]

  /* CleanClean return true */
  def isBilateral(): Boolean {}

  /* Returns all profiles */
  def getAllProfiles = profiles._1.union(profiles._2)

  /** Default comparator, blocks will be ordered by its comparison size */
  def compare(that : BlockAbstract) : Int = {
    this.getComparisonSize() compare that.getComparisonSize()
  }
}