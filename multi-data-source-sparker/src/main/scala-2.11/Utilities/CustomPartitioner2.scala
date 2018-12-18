package Utilities

import org.apache.log4j.LogManager

/**
  * Created by Luca on 01/08/2017.
  */
class CustomPartitioner2(override val numPartitions : Int) extends MyPartitioner {
  val partitions = Array.ofDim[Double](numPartitions)

  override def getPartition(key: Any): Int = {
    val num = key.asInstanceOf[Double]
    val partition = partitions.indexOf(partitions.min)
    partitions.update(partition, partitions(partition)+num)
    //val log = LogManager.getRootLogger
    //log.info("SPARKER - situazione carico partizioni "+partitions.toList)
    return partition
  }

  def getName(): String ={
    return "Custom partitioner 2"
  }

}
