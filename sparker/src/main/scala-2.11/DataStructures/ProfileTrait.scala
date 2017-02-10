package DataStructures
import scala.collection.mutable.MutableList;

/**
 * Created by gio
 * on 07/12/16.
 */
trait ProfileTrait {
  val id: Long
  val attributes: MutableList[KeyValue]
  val originalID : String

  /**
    * Given a key return the value concatenated by the space of all attributes with that key
    * */
  def getAttributeValues(key : String, separator : String = " ") : String = {
    attributes.filter(_.key.equals(key)).map(_.value).mkString(separator)
  }
}
