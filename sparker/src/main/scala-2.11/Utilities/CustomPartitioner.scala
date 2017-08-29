package Utilities

import org.apache.log4j.LogManager
import org.apache.spark.Partitioner

/**
  * Created by Luca on 01/08/2017.
  */
class CustomPartitioner(override val numPartitions : Int/*, partitionCapacity : Array[Double]*/) extends MyPartitioner {

  override def getPartition(key: Any): Int = {

    val num = key.asInstanceOf[Long]

    val partition = {
      if(Math.ceil(num/numPartitions.toDouble)%2 == 0){
        num%numPartitions
      }
      else{
        numPartitions - 1 - (num%numPartitions)
      }
    }
    return partition.toInt


    /*val cost = key.asInstanceOf[Double]
    val r = new scala.util.Random

    val log = LogManager.getRootLogger

    var position = -1
    try{
      for(i<-0 to numPartitions-1){
        if(partitionCapacity(i) > cost){
          position = i
          throw new Exception("ciao")
        }
      }
    }
    catch{ case _ =>}

    if(position < 0){
      position = r.nextInt(numPartitions)
    }
    else{
      partitionCapacity.update(position, partitionCapacity(position)-cost)
    }
    log.info(partitionCapacity.mkString(","))



    return  position*/
  }

  def getName(): String ={
    return "Custom partitioner"
  }
}
