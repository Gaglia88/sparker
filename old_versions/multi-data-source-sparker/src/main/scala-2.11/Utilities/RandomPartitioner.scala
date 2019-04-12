package Utilities

import org.apache.spark.Partitioner

/**
  * Created by Luca on 02/08/2017.
  */
class RandomPartitioner(override val numPartitions : Int) extends MyPartitioner {
  val rnd = new scala.util.Random

  override def getPartition(key: Any): Int = {
    return rnd.nextInt(numPartitions)
  }

  def getName(): String ={
    return "Random partitioner"
  }
}
