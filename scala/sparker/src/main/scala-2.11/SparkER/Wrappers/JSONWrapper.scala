package SparkER.Wrappers

import SparkER.DataStructures.{KeyValue, MatchingEntities, Profile}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.json.{JSONArray, JSONObject}

import scala.collection.mutable.MutableList

/**
  * Created by Luca on 08/12/2016.
  *
  * JSON Wrapper
  */
object JSONWrapper {

  /**
    * Load the profiles from a JSON file.
    * The JSON must contains a JSONObject for each row, and the JSONObject must be in the form key: value
    * the value can be a single value or an array of values
    **/
  def loadProfiles(filePath: String, startIDFrom: Long = 0, realIDField: String = "", sourceId: Int = 0, fieldsToKeep: List[String] = Nil): RDD[Profile] = {
    val sc = SparkContext.getOrCreate()
    val raw = sc.textFile(filePath, sc.defaultParallelism)

    raw.zipWithIndex().map { case (row, id) =>
      val obj = new JSONObject(row)
      val realID = {
        if (realIDField.isEmpty) {
          ""
        }
        else {
          obj.get(realIDField).toString
        }
      }
      val p = Profile(id + startIDFrom, originalID = realID, sourceId = sourceId)

      val keys = obj.keys()
      while (keys.hasNext) {
        val key = keys.next()
        if (key != realIDField && (fieldsToKeep.isEmpty || fieldsToKeep.contains(key))) {
          val data = obj.get(key)
          data match {
            case jsonArray: JSONArray =>
              val it = data.asInstanceOf[JSONArray].iterator()
              while (it.hasNext) {
                p.addAttribute(KeyValue(key, it.next().toString))
              }
            case _ => p.addAttribute(KeyValue(key, data.toString))
          }
        }
      }
      p
    }
  }

  /**
    * Load the groundtruth from a JSON file.
    * The JSON must contains a JSONObject for each row, and the JSONObject must contains two key : value
    * that represents the ids of the matching entities
    **/
  def loadGroundtruth(filePath: String, firstDatasetAttribute: String, secondDatasetAttribute: String): RDD[MatchingEntities] = {
    val sc = SparkContext.getOrCreate()
    val raw = sc.textFile(filePath)
    raw.map { row =>
      val obj = new JSONObject(row)
      MatchingEntities(obj.get(firstDatasetAttribute).toString, obj.get(secondDatasetAttribute).toString)
    }
  }

}
