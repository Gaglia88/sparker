package SparkER.DataStructures

/**
  *
  * Edge trait
  *
  * @author Song Zhu
  * @since 2016-12-14
  */
trait EdgeTrait {
  /* First profile ID */
  val firstProfileID : Long
  /** Second profile ID */
  val secondProfileID : Long
  /** Return the equivalent entity match of this edge
    * @param map a map that maps the interlan id to the groundtruth id
    * */
  def getEntityMatch(map: Map[Long, String]): MatchingEntities = MatchingEntities(map(firstProfileID),map(secondProfileID))

}
