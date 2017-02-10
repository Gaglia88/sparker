package DataStructures

import WeightingMethods.WeightMethodTrait
import org.apache.spark.broadcast.Broadcast

/**
 * Created by gio
 * on 07/12/16.
 */
trait BlockAbstract  extends Ordered[BlockAbstract]{

  val blockID : Long

  var entropy : Double

  val profiles: (List[Long], List[Long])
  /*val datasetLimit: Long*/

  // return the number of entities indexed in the block
  def size = (profiles._1.size + profiles._2.size).toDouble

  // return the number of comparisons entailed by this block
  def getComparisonSize(): Double

  // return an iterator of the comparisons
  def getComparisons(): List[UnweightedEdge] {}

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

  def getWeightedComparisons(profileBlocks: Broadcast[Map[Long,ProfileBlocks]], weightMethod: WeightMethodTrait): List[WeightedEdge]

  // CleanClean return true
  def isBilateral(): Boolean {}

  //Returns all profiles
  def getAllProfiles = profiles._1.union(profiles._2)

  def compare(that : BlockAbstract) : Int = {
    this.getComparisonSize() compare that.getComparisonSize()
  }
}