package DataStructures

/**
  * Clean block: the profiles comes from two distinct datasets
  *
  * @author Giovanni Simononi
  * @since 2016/12/07
  */
case class BlockClean(blockID: Long, profiles: Array[Set[Long]], var entropy: Double = -1, var clusterID: Double = -1, blockingKey: String = "") extends BlockAbstract with Serializable {
  override def getComparisonSize(): Double = {
    val a = profiles.filter(_.nonEmpty)
    if (a.length > 1) {
      //a.map(_.size.toDouble).product
      var comparisons: Double = 0
      var i = 0
      while (i < profiles.length) {
        var j = i + 1
        while (j < profiles.length) {
          comparisons += a(i).size * a(j).size
          j += 1
        }
        i += 1
      }
      comparisons
    }
    else {
      0
    }
  }

  override def getComparisons(): Set[(Long, Long)] = {
    var out: List[(Long, Long)] = Nil

    for (i <- profiles.indices) {
      for (j <- (i + 1) until profiles.length) {
        val a = profiles(i)
        val b = profiles(j)
        for (e1 <- a; e2 <- b) {
          if (e1 < e2) {
            out = (e1, e2) :: out
          }
          else {
            out = (e2, e1) :: out
          }
        }
      }
    }

    out.toSet
  }
}
