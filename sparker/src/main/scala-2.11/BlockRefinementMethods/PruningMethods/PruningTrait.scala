package BlockRefinementMethods.PruningMethods

import DataStructures.WeightedEdge
import org.apache.spark.rdd.RDD

/**
  * Created by Luca on 09/12/2016.
  */
trait PruningTrait {
  /**
    * Applies pruning on an RDD of undirectional unweighted edges
    * */
  def pruning(weightedEdges : RDD[WeightedEdge]) : RDD[WeightedEdge]
}
