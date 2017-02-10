package WeightingMethods

import DataStructures.{ProfileBlocks, UnweightedEdge, WeightedEdge}
import org.apache.spark.broadcast.Broadcast

/**
  * Created by Luca on 09/12/2016.
  */
trait WeightMethodTrait {
  def computeWeight(edge : UnweightedEdge, profileBlocks: Broadcast[Map[Long,ProfileBlocks]]) : WeightedEdge

  def getWeightEdge(p1: ProfileBlocks, p2: ProfileBlocks): WeightedEdge = {
    WeightedEdge(p1.profileID, p2.profileID, weightMethod(p1,p2))
  }

  def weightMethod(p1: ProfileBlocks, p2: ProfileBlocks): Double
}
