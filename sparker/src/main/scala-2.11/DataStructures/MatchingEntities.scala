package DataStructures

/**
  * Created by Luca on 09/12/2016.
  *
  * Represents two matching entities
  */
case class MatchingEntities(val firstEntityID : String, val secondEntityID : String) {

  /**
    * Override the equals conditions
    * */
  override def equals(that: Any): Boolean = {
    that match {
      case that: MatchingEntities =>
        (that.firstEntityID == this.firstEntityID && that.secondEntityID == this.secondEntityID)
      case _ => false
    }
  }

  /**
    * Custom hashcode, used to check if two objects are equals
    * */
  override def hashCode:Int = {
    val firstEntityHashCode = (if(firstEntityID == null) 0 else firstEntityID.hashCode)
    val secondEntityHashCode = (if(secondEntityID == null) 0 else secondEntityID.hashCode)
    (firstEntityHashCode+"_"+secondEntityHashCode).hashCode
  }
}
