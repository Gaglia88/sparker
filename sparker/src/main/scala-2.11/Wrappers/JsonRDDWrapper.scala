package Wrappers

import DataStructures.{KeyValue, MatchingEntities, Profile}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.json4s.JsonAST.JArray
import org.json4s.{DefaultFormats, JString, jackson}

import scala.collection.mutable.MutableList

/**
  * Created by song on 2016-12-26.
  */
object JsonRDDWrapper extends WrapperTrait{

  /**
    * Load the profiles from a JSON file.
    * The JSON must contains a JSONObject for each row, and the JSONObject must be in the form key: value
    * the value can be a single value or an array of values
    * */
  def loadProfiles(filePath : String, startIDFrom : Long = 0) : RDD[Profile] = {
    val sc = SparkContext.getOrCreate()
    val rdd = sc.textFile(filePath)

    rdd.map(parseJsonAttributes(_)).zipWithIndex().map {
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
    JSONWrapper.loadGroundtruth(filePath)
  }

  def parseJsonAttributes(input: String, explodeInnerFields:Boolean = false, innerSeparator : String = ","): MutableList[KeyValue] = {
    val attributes: MutableList[KeyValue] = new MutableList()
    val rawAttributes = jackson.parseJson(input).filterField(x => true)

    implicit val formats = DefaultFormats

    rawAttributes.foreach(x => {
      x._2 match {
        case listOfAttributes : JArray =>
          listOfAttributes.children map {
            attributeValue =>
              attributes += KeyValue(x._1, attributeValue.extract[String]/*jackson.compactJson(attributeValue)*/)
          }
        case stringAttribute : JString =>
          if(explodeInnerFields){
            stringAttribute.extract[String].split(innerSeparator) map {
              attributeValue =>
                attributes += KeyValue(x._1, attributeValue)
            }
          }
          else {
            attributes += KeyValue(x._1, jackson.compactJson(stringAttribute))
          }
        case singleAttribute =>
          attributes += KeyValue(x._1, jackson.compactJson(singleAttribute))
      }
    })

    attributes
  }

}
