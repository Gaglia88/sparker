package WeightingMethods

import DataStructures.{UnweightedEdge, WeightedEdge}
import org.apache.spark.rdd.RDD

/**
  * Created by song on 2016-12-13.
  *
  * Compute CBS with sum of non distinct and unweighted Edges
  */
object groupByCBS {

  def computeWeight(edges: RDD[UnweightedEdge]) : RDD[WeightedEdge] =
    //edges.map(edge => (edge,1)).reduceByKey((x,y) => x+y).map(x => WeightedEdge(x._1.firstProfileID, x._1.secondProfileID, x._2))
    edges.map(edge => (edge,1)).reduceByKey(_+_).map(x => WeightedEdge(x._1.firstProfileID, x._1.secondProfileID, x._2))

}