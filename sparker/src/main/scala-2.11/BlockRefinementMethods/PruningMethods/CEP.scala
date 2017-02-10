package BlockRefinementMethods.PruningMethods

import DataStructures.WeightedEdge
import org.apache.spark.rdd.RDD

/**
  * Created by Luca on 09/12/2016.
  * Cardinality Edge Pruning
  * Keeps the top K edges
  * K is calculated as floor(sum of the total number of profiles in all blocks / 2)
  */
class CEP(totalMaxCardinality: Long) extends Serializable with PruningTrait{

  def pruning(weightedEdges : RDD[WeightedEdge]) : RDD[WeightedEdge] = {

    weightedEdges.sortBy(edge => - edge.weight).zipWithIndex().filter(x => x._2 <= totalMaxCardinality).map(x => x._1)
  }
}
