package BlockRefinementMethods.PruningMethods

import DataStructures.WeightedEdge
import org.apache.spark.rdd.RDD
import Utilities.BoundedPriorityQueue

import scala.collection.immutable.TreeSet


/**
  * Created by Luca on 09/12/2016.
  *
  * Cardinality Node Pruning
  */
object CNP extends PruningTrait{

  override def pruning(weightedEdges : RDD[WeightedEdge]) : RDD[WeightedEdge] = pruning(weightedEdges, 0.5)

  def pruning(weightedEdges : RDD[WeightedEdge], r: Double, minCardinality: Int = 1) : RDD[WeightedEdge] = {
    // Get nodes from first edges
    val nodes1 = weightedEdges.map(edge => (edge.firstProfileID, edge))
    // Get nodes from second edges
    val nodes2 = weightedEdges.map(edge => (edge.secondProfileID, edge))

    val nodes = nodes1.union(nodes2).groupByKey()

    nodes.flatMap(cardinalityFilter(r, minCardinality)).distinct()
  }

  def cardinalityFilter(r: Double, minCardinality: Int)(input: (Long, Iterable[WeightedEdge])): Iterable[(WeightedEdge)] = {
    val maxCardinality = math.max(fractionNodeCardinality(input, r), minCardinality)
    //input.Edges.toList.sortWith(_.weight > _.weight).take(maxCardinality)

    val queue = new BoundedPriorityQueue[WeightedEdge](maxCardinality)(Ordering[Double].on[WeightedEdge](edge => edge.weight))
    queue ++= input._2
    queue
  }

  def fractionNodeCardinality(input: (Long, Iterable[WeightedEdge]), fraction: Double) : Int = {
    (input._2.size * fraction).toInt
  }

}
