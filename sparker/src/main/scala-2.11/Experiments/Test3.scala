package Experiments

import java.util.Calendar

import BlockBuildingMethods.LSHTwitter3
import BlockRefinementMethods.PruningMethods.WNPFor
import Wrappers.SerializedObjectLoader
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Luca on 28/02/2017.
  */
object Test3 {
  def main(args: Array[String]) {
    val memoryHeap = 15
    val memoryStack = 5
    val pathDataset1 = "C:/Users/Luca/Desktop/UNI/BlockingFramework/datasets/movies/profiles/dataset1"
    val pathDataset2 = "C:/Users/Luca/Desktop/UNI/BlockingFramework/datasets/movies/profiles/dataset2"
    val pathGt = "C:/Users/Luca/Desktop/UNI/BlockingFramework/datasets/movies/groundtruth"


    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.executor.memory", memoryHeap + "g")
      .set("spark.network.timeout", "10000s")
      .set("spark.executor.heartbeatInterval", "40s")
      .set("spark.default.parallelism", "32")
      .set("spark.executor.extraJavaOptions", "-Xss" + memoryStack + "g")
      .set("spark.local.dir", "/data2/sparkTmp/")
      .set("spark.driver.maxResultSize", "10g")

    val sc = new SparkContext(conf)

    println("Start to loading profiles")
    val startTime = Calendar.getInstance();
    val dataset1 = SerializedObjectLoader.loadProfiles(pathDataset1)
    val separatorID = dataset1.map(_.id).max()
    val dataset2 = SerializedObjectLoader.loadProfiles(pathDataset2, separatorID + 1)
    val maxProfileID = dataset2.map(_.id).max()

    val profiles = dataset1.union(dataset2)
    profiles.cache()
    val numProfiles = profiles.count()

    LSHTwitter3.clusterSimilarAttributes(profiles, 16, 0.1, -1, separatorID)
  }
}
