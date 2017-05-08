package Utilities

import DataStructures.{MatchingEntities, Profile}
import Wrappers._
import org.apache.spark.rdd.RDD

/**
  * Created by song on 2016-12-20.
  */
object FileLoaders {

  final val csvWrapper = "CSV"
  final val jsonWrapper = "JSON"
  final val jsonRDDWrapper = "JSONR"
  final val serializedWrapper = "SER"

  def loadProfiles(filename : String, startIDFrom : Long = 0, wrapper: String = serializedWrapper, realIDField : String = "") : RDD[Profile] =
    getWrapper(wrapper).loadProfiles(filename, startIDFrom, realIDField)

  def loadGroundTruth(filename : String, wrapper: String = serializedWrapper) : RDD[MatchingEntities] =
    getWrapper(wrapper).loadGroundtruth(filename)

  def getWrapper(wrapper: String) : WrapperTrait =
    wrapper match {
      case FileLoaders.csvWrapper => CSVWrapper
      case FileLoaders.jsonWrapper => JSONWrapper
      case FileLoaders.jsonRDDWrapper => JsonRDDWrapper
      case _ => SerializedObjectLoader
    }
}
