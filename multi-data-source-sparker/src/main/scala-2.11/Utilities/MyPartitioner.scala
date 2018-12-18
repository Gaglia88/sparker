package Utilities

import org.apache.spark.Partitioner

/**
  * Created by Luca on 03/08/2017.
  */
trait MyPartitioner extends Partitioner {
  def getName() : String
}
