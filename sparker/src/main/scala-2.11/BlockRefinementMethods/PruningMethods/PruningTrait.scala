package BlockRefinementMethods.PruningMethods

import DataStructures.WeightedEdge
import org.apache.spark.rdd.RDD

/**
  * Pruning trait
  * @author Luca Gagliardelli
  * @since 2016/09/12
  */
trait PruningTrait {
  /**
    * Applies the pruning on an RDD of undirected weighted edges
    * */
  def pruning(weightedEdges : RDD[WeightedEdge]) : RDD[WeightedEdge]
}
