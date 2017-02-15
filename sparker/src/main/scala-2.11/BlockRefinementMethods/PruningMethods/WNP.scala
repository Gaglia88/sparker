package BlockRefinementMethods.PruningMethods

import DataStructures.WeightedEdge
import org.apache.spark.rdd.RDD

/**
  *
  * Weighted Node Pruning
  *
  * For each Node (Profile) keeps only the edges that have a weight equal or above than the average weight of the
  * node edges
  *
  * @author Luca Gagliardelli
  * @since 2016/12/09
  */
object WNP extends PruningTrait{

  /**
    * Supported WNP Types
    * */
  object WNPTypes {
    /** Basic WNP: keeps all the edges obtained after pruning process (duplicates are possible) */
    val BASIC : String = "basic"
    /** AND WNP: keeps only the edges that are both retained in two adjacent nodes */
    val AND : String = "and"
    /** OR WNP: keeps all the edges that are retained for almost one of two adjacent nodes (no duplicates are possible) */
    val OR : String = "or"
  }

  /**
    * Supported retain edges types
    * */
  object retainEdgesTypes {
    /** Retain only local edges that have a weight equal or above than the local maximum edge weight / 2  */
    val MAX_FRACT_2 = "max"
    /** Default, Retain only local edges that have a weight equal or above than the local average edge weight / 2  */
    val AVG = "avg"
  }

  override def pruning(weightedEdges : RDD[WeightedEdge]) : RDD[WeightedEdge] = {
    pruning(weightedEdges, WNPTypes.BASIC)
  }

  /**
    * Pruning the edges
    *
    * @param weightedEdges undirected weighted edges
    * @param WNPType type of WNP as listed in the WNPTypes object
    * @param retainType metric to use for calculate the threshold as listed in the retainEdgesTypes object
    * */
  def pruning(weightedEdges : RDD[WeightedEdge], WNPType : String, retainType : String = retainEdgesTypes.AVG) : RDD[WeightedEdge] = {
    //Creates the directed edges
    val directedWeightedEdges = weightedEdges.map(e => (e.firstProfileID, e)).union(weightedEdges.map(e => (e.secondProfileID, e)))
    //For each node (Profile) I obtain the list of its edges
    val edgesPerNode = directedWeightedEdges.groupByKey()

    //For each node keeps only the edges that have a weight equal or above than the average weight
    val retainedEdges = edgesPerNode flatMap {
      nodeWithEdges =>
        val nodeEdges = nodeWithEdges._2

        val avgWeight = retainType match {
          case retainEdgesTypes.MAX_FRACT_2 =>
            nodeEdges.map(_.weight).max / 2
          case _ =>
            nodeEdges.map(_.weight).sum / nodeEdges.size
        }

        nodeEdges.filter(_.weight >= avgWeight)
    }

    WNPType match {
      case WNPTypes.AND =>
        //Returns the edges that are in both nodes
        retainedEdges.map(x => (x.firstProfileID+"_"+x.secondProfileID, x)).groupByKey().filter(_._2.size == 2).flatMap(_._2.take(1))
      case WNPTypes.OR =>
        //Returns the retained edges without duplicates
        retainedEdges.distinct()
      case _ =>
        //Returns all the retained edges
        retainedEdges
    }
  }
}
