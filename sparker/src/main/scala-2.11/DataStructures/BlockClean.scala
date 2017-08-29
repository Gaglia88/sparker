package DataStructures

/**
  * Clean block: the profiles comes from two distinct datasets
  * @author Giovanni Simononi
  * @since 2016/12/07
  */
case class BlockClean(val blockID : Long, val profiles: (Set[Long], Set[Long]), var entropy : Double = -1, var clusterID : Double = -1) extends BlockAbstract with Serializable{
  override def getComparisonSize(): Double = profiles._1.size.toDouble * profiles._2.size.toDouble
  override def isBilateral(): Boolean = true
}
