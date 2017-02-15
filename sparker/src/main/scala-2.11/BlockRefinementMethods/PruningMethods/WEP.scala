package BlockRefinementMethods.PruningMethods

import DataStructures.WeightedEdge
import org.apache.spark.rdd.RDD

/**
  * Weighted Edge Pruning
  * Keeps only the edges that have the weight equal or above than the global average weight
  *
  * @author Luca Gagliardelli
  * @since 2016/12/09
  */
object WEP extends PruningTrait{
  def pruning(weightedEdges : RDD[WeightedEdge]) : RDD[WeightedEdge] = {
    val avgWeight = weightedEdges.map(_.weight).mean()
    weightedEdges.filter(_.weight >= avgWeight)
  }
}
