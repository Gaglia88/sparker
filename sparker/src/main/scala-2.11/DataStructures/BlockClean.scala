package DataStructures

import WeightingMethods.WeightMethodTrait
import org.apache.spark.broadcast.Broadcast

/**
  * Clean block: the profiles comes from two distinct datasets
  * @author Giovanni Simononi
  * @since 2016/12/07
  */
case class BlockClean(val blockID : Long, val profiles: (List[Long], List[Long]), var entropy : Double = -1, var clusterID : Double = -1) extends BlockAbstract with Serializable{
  override def getComparisonSize(): Double = profiles._1.size * profiles._2.size
  override def isBilateral(): Boolean = true

  def getComparisons(): List[UnweightedEdge] = {
    (for (e1<-profiles._1; e2<-profiles._2) yield UnweightedEdge(e1,e2))
  }

  def getWeightedComparisons(profileBlocks: Broadcast[Map[Long,ProfileBlocks]], weightMethod: WeightMethodTrait): List[WeightedEdge] = {
    for (
      e1<-profiles._1; e2<-profiles._2;
      edge = getComparison(profileBlocks.value(e1.toInt), profileBlocks.value(e2.toInt), weightMethod)
      if edge != null
    ) yield edge
  }
}
