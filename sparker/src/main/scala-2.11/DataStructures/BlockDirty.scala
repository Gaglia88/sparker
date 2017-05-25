package DataStructures

import WeightingMethods.WeightMethodTrait
import org.apache.spark.broadcast.Broadcast

import scala.collection.immutable.HashSet

/**
 * Dirty block: all the profiles comes from a single dataset
 * @author Giovanni Simononi
 * @since 2016/12/07
 */
case class BlockDirty(val blockID : Long, val profiles: (Set[Long], Set[Long]), var entropy : Double = -1, var clusterID : Double = -1) extends BlockAbstract with Serializable{
  override def getComparisonSize(): Double = profiles._1.size.toDouble * (profiles._1.size.toDouble - 1.0) / 2.0
  override def isBilateral(): Boolean = false

  def getComparisons(): List[UnweightedEdge] = {
    getAllProfiles.toList.combinations(2).map(x =>
      if(x(0) < x(1)) UnweightedEdge(x(0), x(1))
      else UnweightedEdge(x(1), x(0))
    ).toList
  }
}
