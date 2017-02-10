package WeightingMethods

import DataStructures.{BlockWithComparisonSize, ProfileBlocks, UnweightedEdge, WeightedEdge}
import org.apache.spark.broadcast.Broadcast

/**
  * Created by Luca on 09/12/2016.
  */
object JaccardSimilarity extends WeightMethodTrait{

  def computeWeight(edge : UnweightedEdge, profileBlocks: Broadcast[Map[Long,ProfileBlocks]]) : WeightedEdge = {
    // todo quando e.firstProfileID < e.secondProfileID non dovrebbe essere calcolato il peso
    val firstProfileBlocks = profileBlocks.value(edge.firstProfileID.toInt)
    val secondProfileBlocks = profileBlocks.value(edge.secondProfileID.toInt)

    WeightedEdge(edge.firstProfileID, edge.secondProfileID, weightMethod(firstProfileBlocks, secondProfileBlocks))
  }

  def weightMethod(p1: ProfileBlocks, p2: ProfileBlocks): Double = {
    val commonBlocksNumber = (p1.blocks.intersect(p2.blocks).size).toDouble
    commonBlocksNumber / (p1.blocks.size + p2.blocks.size - commonBlocksNumber)
  }
}
