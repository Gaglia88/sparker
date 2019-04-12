package SparkER.DataStructures

/**
  * Represents two matching entities
  *
  * @author Luca Gagliardelli
  * @since 2016/12/09
  */
case class MatchingEntities(firstEntityID: String, secondEntityID: String) {

  /**
    * Override the equals conditions
    **/
  override def equals(that: Any): Boolean = {
    that match {
      case that: MatchingEntities =>
        that.firstEntityID == this.firstEntityID && that.secondEntityID == this.secondEntityID
      case _ => false
    }
  }

  /**
    * Custom hashcode, used to check if two objects are equals
    **/
  override def hashCode: Int = {
    val firstEntityHashCode = if (firstEntityID == null) 0 else firstEntityID.hashCode
    val secondEntityHashCode = if (secondEntityID == null) 0 else secondEntityID.hashCode
    (firstEntityHashCode + "_" + secondEntityHashCode).hashCode
  }
}
