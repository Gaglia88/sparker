package DataStructures

/**
  * Created by song on 2016-12-14.
  *
  * Edge trait
  */
trait EdgeTrait {
  val firstProfileID : Long
  val secondProfileID : Long

  def getEntityMatch(map: Map[Long, String]): MatchingEntities = MatchingEntities(map(firstProfileID),map(secondProfileID))

}
