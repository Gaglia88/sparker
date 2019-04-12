package SparkER.Wrappers

import SparkER.DataStructures.{KeyValue, MatchingEntities, Profile}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by Luca on 16/12/2016.
  */
object SerializedObjectLoader extends WrapperTrait {

  def loadProfiles(filePath: String, startIDFrom: Long = 0, realFieldID: String = "", sourceId: Int = 0): RDD[Profile] = {
    @transient lazy val log = org.apache.log4j.LogManager.getRootLogger

    log.info("SPARKER - Start to loading entities")
    val entities = DataLoaders.SerializedLoader.loadSerializedDataset(filePath)
    log.info("SPARKER - Loading ended")

    log.info("SPARKER - Start to generate profiles")
    val profiles: Array[Profile] = new Array(entities.size())

    for (i <- 0 until entities.size()) {
      val profile = Profile(id = i + startIDFrom, originalID = i + "", sourceId = sourceId)

      val entity = entities.get(i)
      val it = entity.getAttributes.iterator()
      while (it.hasNext) {
        val attribute = it.next()
        profile.addAttribute(KeyValue(attribute.getName, attribute.getValue))
      }

      profiles.update(i, profile)
    }
    log.info("SPARKER - Ended to loading profiles")

    log.info("SPARKER - Start to parallelize profiles")
    val sc = SparkContext.getOrCreate()

    sc.union(profiles.grouped(10000).map(sc.parallelize(_)).toArray)
  }

  def loadGroundtruth(filePath: String): RDD[MatchingEntities] = {

    val groundtruth = DataLoaders.SerializedLoader.loadSerializedGroundtruth(filePath)

    val matchingEntitites: Array[MatchingEntities] = new Array(groundtruth.size())

    var i = 0

    val it = groundtruth.iterator
    while (it.hasNext) {
      val matching = it.next()
      matchingEntitites.update(i, MatchingEntities(matching.getEntityId1.toString, matching.getEntityId2.toString))
      i += 1
    }

    val sc = SparkContext.getOrCreate()
    sc.union(matchingEntitites.grouped(10000).map(sc.parallelize(_)).toArray)
  }
}