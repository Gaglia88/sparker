package DataStructures

/**
  * Created by Luca on 09/12/2016.
  *
  * Represents a weighted edge
  */
case class WeightedEdge(firstProfileID : Long, secondProfileID : Long, weight : Double)  extends EdgeTrait with Serializable{

}
