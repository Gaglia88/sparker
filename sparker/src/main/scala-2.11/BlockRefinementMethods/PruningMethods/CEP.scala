package BlockRefinementMethods.PruningMethods

import DataStructures.WeightedEdge
import org.apache.spark.rdd.RDD

/**
  * Cardinality Edge Pruning
  * Keeps the top K edges
  * K is calculated as floor(sum of the total number of profiles in all blocks / 2)
  * @author Song Zhu
  * @since 2016/12/09
  */
class CEP(totalMaxCardinality: Long) extends Serializable with PruningTrait{

  def pruning(weightedEdges : RDD[WeightedEdge]) : RDD[WeightedEdge] = {

    weightedEdges.sortBy(edge => - edge.weight).zipWithIndex().filter(x => x._2 <= totalMaxCardinality).map(x => x._1)
  }
}
