package Experiments

import java.util.Calendar

import BlockBuildingMethods.LSHTwitter3.Settings
import BlockBuildingMethods.{BlockingUtils, LSHTwitter3}
import BlockRefinementMethods.PruningMethods.WNPFor
import Wrappers.SerializedObjectLoader
import com.twitter.algebird.{MinHasher, MinHasher32}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Luca on 28/02/2017.
  */
object Test4 {
  def main(args: Array[String]) {
    val purgingRatio = 1.0
    val filteringRatio = 0.8
    val thresholdType = WNPFor.ThresholdTypes.MAX_FRACT_2
    val hashNum = 32
    val clusterThreshold = 0.3

    /*
    val memoryHeap = args(0)
    val memoryStack = args(1)
    val pathDataset1 = args(2)
    val pathDataset2 = args(3)
    val pathGt = args(4)
    */

    val memoryHeap = 15
    val memoryStack = 5
    val pathDataset1 = "C:/Users/Luca/Desktop/UNI/BlockingFramework/datasets/movies/profiles/dataset1"
    val pathDataset2 = "C:/Users/Luca/Desktop/UNI/BlockingFramework/datasets/movies/profiles/dataset2"
    val pathGt = "C:/Users/Luca/Desktop/UNI/BlockingFramework/datasets/movies/groundtruth"

    println("Heap " + memoryHeap + "g")
    println("Stack " + memoryStack + "g")
    println("First dataset path " + pathDataset1)
    println("Second dataset path " + pathDataset2)
    println("Groundtruth path " + pathGt)
    println("Threshold type " + thresholdType)
    println()


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
    val dataset1 = SerializedObjectLoader.loadProfiles(pathDataset1) //CSVWrapper.loadProfiles(filePath = pathDataset1, header = true, realIDField = "id")
    val separatorID = dataset1.map(_.id).max()
    val dataset2 = SerializedObjectLoader.loadProfiles(pathDataset2, separatorID + 1) //CSVWrapper.loadProfiles(filePath = pathDataset2, startIDFrom = separatorID+1, header = true, realIDField = "id")
    val maxProfileID = dataset2.map(_.id).max()

    val profiles = dataset1.union(dataset2)
    profiles.cache()
    val numProfiles = profiles.count()


    val numBands = -1
    val targetThreshold = 0.9
    val numHashes = 10

    val test = profiles.filter(x => x.id == 7669 || x.id == 30036)
    val keysPerToken = test.flatMap{profile =>
      val dataset = if (profile.id > separatorID) Settings.FIRST_DATASET_PREFIX else Settings.SECOND_DATASET_PREFIX
      profile.attributes.flatMap{ak =>
        val tokens = ak.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)
        val tokens1 = tokens.map(_.trim).filter(_.size > 0)
        tokens1.map((_, dataset+ak.key))
      }
    }

    val mantained = keysPerToken.groupByKey().filter(_._2.size > 1).map(_._2).distinct()
    mantained.collect().foreach(println)

    ???


    val a = profiles.filter(x => x.id == 7669).collect()(0)
    val b = profiles.filter(x => x.id == 30036).collect()(0)


    val t1 = a.attributes.flatMap{ak =>
      val tokens = ak.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)
      tokens.map((_, "d1_"+ak.key))
    }
    val t2 = b.attributes.flatMap{ak =>
      val tokens = ak.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)
      tokens.map((_, "d2_"+ak.key))
    }

    val t = t1.union(t2)

    t.groupBy(x => x._1).filter(_._2.size > 1).map(x => x._2.map(_._2)).foreach(println)



    /*val c = profiles.filter(x => x.id == 14759).collect()(0)
    val d = profiles.filter(x => x.id == 37945).collect()(0)*/

    /*
    val res = LSHTwitter3.createBlocks(profiles, numHashes, targetThreshold, numBands, separatorID)
    res.collect().foreach(println)
    */
  }
}
