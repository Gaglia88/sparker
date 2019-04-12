package SparkER.Wrappers

import SparkER.DataStructures.{KeyValue, MatchingEntities, Profile}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import scala.collection.mutable.MutableList

/**
  * Created by Luca on 08/12/2016.
  *
  * Generic wrapper
  */
trait WrapperTrait {
  /**
    * Given a file path return an RDD of Profiles
    **/
  def loadProfiles(filePath: String, startIDFrom: Long, realIDField: String, sourceId: Int): RDD[Profile]

  /**
    * Given a file path return an RDD of EqualEntities
    **/
  def loadGroundtruth(filePath: String): RDD[MatchingEntities]

  /**
    * Given a row return the list of attributes
    *
    * @param columnNames names of the dataframe columns
    * @param row         single dataframe row
    **/
  def rowToAttributes(columnNames: Array[String], row: Row, explodeInnerFields: Boolean = false, innerSeparator: String = ","): MutableList[KeyValue] = {
    val attributes: MutableList[KeyValue] = new MutableList()
    for (i <- 0 to row.size - 1) {
      try {
        val value = row(i)
        val attributeKey = columnNames(i)

        if (value != null) {
          value match {
            case listOfAttributes: Iterable[Any] =>
              listOfAttributes map {
                attributeValue =>
                  attributes += KeyValue(attributeKey, attributeValue.toString)
              }
            case stringAttribute: String =>
              if (explodeInnerFields) {
                stringAttribute.split(innerSeparator) map {
                  attributeValue =>
                    attributes += KeyValue(attributeKey, attributeValue)
                }
              }
              else {
                attributes += KeyValue(attributeKey, stringAttribute)
              }
            case singleAttribute =>
              attributes += KeyValue(attributeKey, singleAttribute.toString)
          }
        }
      }
      catch {
        case e: Throwable => println(e)
      }
    }
    attributes
  }
}
