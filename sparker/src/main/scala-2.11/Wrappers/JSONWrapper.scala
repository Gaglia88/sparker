package Wrappers

import DataStructures.{KeyValue, MatchingEntities, Profile}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.MutableList

/**
  * Created by Luca on 08/12/2016.
  *
  * JSON Wrapper
  */
object JSONWrapper extends WrapperTrait{

  /**
    * Load the profiles from a JSON file.
    * The JSON must contains a JSONObject for each row, and the JSONObject must be in the form key: value
    * the value can be a single value or an array of values
    * */
  def loadProfiles(filePath : String, startIDFrom : Long = 0) : RDD[Profile] = {
    val sparkSession = SparkSession.builder.getOrCreate()
    val df = sparkSession.read.json(filePath)
    val columnNames = df.columns

    df.rdd.map(row => rowToAttributes(columnNames, row)).zipWithIndex().map {
      profile =>
        val profileID = profile._2 + startIDFrom
        val attributes = profile._1
        Profile(profileID, attributes)
    }
  }

  /**
    * Load the groundtruth from a JSON file.
    * The JSON must contains a JSONObject for each row, and the JSONObject must contains two key : value
    * that represents the ids of the matching entities
    * */
  def loadGroundtruth(filePath : String) : RDD[MatchingEntities] = {
    val sparkSession = SparkSession.builder.getOrCreate()
    val df = sparkSession.read.json(filePath)
    df.rdd map {
      row =>
        MatchingEntities(row.get(0).toString, row.get(1).toString)
    }
  }

}
