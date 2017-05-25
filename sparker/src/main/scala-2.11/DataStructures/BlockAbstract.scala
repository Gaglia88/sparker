package DataStructures

import WeightingMethods.WeightMethodTrait
import org.apache.spark.broadcast.Broadcast

import scala.collection.immutable.HashSet

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
  /** Cluster */
  var clusterID : Double
  /** Id of the profiles contained in the block */
  val profiles: (Set[Long], Set[Long])

  /** Return the number of entities indexed in the block */
  def size = (profiles._1.size + profiles._2.size).toDouble

  /* Return the number of comparisons entailed by this block */
  def getComparisonSize(): Double

  /* Return the comparisons contained in the block */
  def getComparisons(): List[UnweightedEdge] {}

  /* CleanClean return true */
  def isBilateral(): Boolean {}

  /* Returns all profiles */
  def getAllProfiles = profiles._1.union(profiles._2)

  /** Default comparator, blocks will be ordered by its comparison size */
  def compare(that : BlockAbstract) : Int = {
    this.getComparisonSize() compare that.getComparisonSize()
  }
}