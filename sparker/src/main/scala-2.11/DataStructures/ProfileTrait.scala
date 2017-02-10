package DataStructures
import scala.collection.mutable.MutableList;

/**
 * Created by gio
 * on 07/12/16.
 */
trait ProfileTrait {
  val id: Long
  val attributes: MutableList[KeyValue] // todo define an object attribute (questa sarà una lista di attirbuti)

  /**
    * Given a key return the value concatenated by the space of all attributes with that key
    * */
  def getAttributeValues(key : String, separator : String = " ") : String = {
    attributes.filter(_.key.equals(key)).map(_.value).mkString(separator)
  }
}
