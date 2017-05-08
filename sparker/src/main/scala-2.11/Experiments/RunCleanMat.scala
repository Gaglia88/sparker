package Experiments

import java.util.Calendar

import BlockBuildingMethods.{LSHTwitter, TokenBlocking}
import BlockRefinementMethods.PruningMethods.{CNPFor, PruningUtils, WNPFor, WNPMat}
import BlockRefinementMethods.{BlockFiltering, BlockPurging}
import DataStructures.KeysCluster
import Utilities.Converters
import Wrappers.{CSVWrapper, JsonRDDWrapper, SerializedObjectLoader}
import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast

/**
  * Created by Luca on 13/03/2017.
  */
object RunCleanMat {
  object WRAPPER_TYPES {
    val serialized = "serialized"
    val csv = "csv"
    val json = "json"
  }

  object PRUNING_TYPES {
    val CNP = "CNP"
    val WNP = "WNP"
  }

  object BLOCKING_TYPES {
    val inferSchema = "inferSchema"
    val schemaAware = "schemaAware"
  }

  def main(args: Array[String]) {
    val pathDataset1 = "C:/Users/Luca/Desktop/UNI/BlockingFramework/datasets/movies/profiles/dataset1"
    val pathDataset2 = "C:/Users/Luca/Desktop/UNI/BlockingFramework/datasets/movies/profiles/dataset2"
    val pathGt = "C:/Users/Luca/Desktop/UNI/BlockingFramework/datasets/movies/groundtruth"
    val defaultLogPath = "C:/Users/Luca/Desktop/"
    val wrapperType = RunClean.WRAPPER_TYPES.serialized
    val GTWrapperType = RunClean.WRAPPER_TYPES.serialized
    val purgingRatio = 1.000
    val filteringRatio = 0.8
    val hashNum = 16
    val clusterThreshold = 0.3
    val memoryHeap = 118
    val memoryStack = 5

    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.executor.memory", memoryHeap+"g")
      .set("spark.network.timeout", "10000s")
      .set("spark.executor.heartbeatInterval", "40s")
      .set("spark.default.parallelism", "32")
      .set("spark.executor.extraJavaOptions", "-Xss"+memoryStack+"g")
      .set("spark.local.dir", "/data2/sparkTmp/")
      .set("spark.driver.maxResultSize", "10g")

    val sc = new SparkContext(conf)

    val saveResPath = "C:/Users/Luca/Desktop/results"
    RunCleanMat(sc, defaultLogPath+"WNP_CBS_AVG_NO-SCHEMA.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.schemaLess, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.AVG, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = false, memoryHeap, memoryStack, pathDataset1, pathDataset2, saveResPath)
  }

  def RunCleanMat(sc : SparkContext, logPath : String, wrapperType : String, purgingRatio : Double, filteringRatio : Double, blockingType : String,
               pruningType : String, thresholdType : String, weightType : String, hashNum : Int, clusterThreshold : Double,
               useEntropy : Boolean, memoryHeap : Int, memoryStack : Int, pathDataset1 : String, pathDataset2 : String,
                 pathSaveEdges : String): Unit = {

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val layout = new SimpleLayout();
    val appender = new FileAppender(layout,logPath,false);
    log.addAppender(appender);

    log.info("SPARKER - Heap "+memoryHeap+"g")
    log.info("SPARKER - Stack "+memoryStack+"g")
    log.info("SPARKER - First dataset path "+pathDataset1)
    log.info("SPARKER - Second dataset path "+pathDataset2)
    log.info("SPARKER - Threshold type "+thresholdType)
    log.info("SPARKER - Blocking type "+blockingType)
    log.info("SPARKER - Pruning type "+pruningType)
    log.info()

    log.info("SPARKER - Start to loading the profiles")
    val startTime = Calendar.getInstance();
    val dataset1 = {
      if(wrapperType == WRAPPER_TYPES.serialized){
        SerializedObjectLoader.loadProfiles(pathDataset1)
      }
      else if(wrapperType == WRAPPER_TYPES.json){
        JsonRDDWrapper.loadProfiles(pathDataset1, 0, "realId")
      }
      else{
        CSVWrapper.loadProfiles(filePath = pathDataset1, header = true, realIDField = "id")
      }
    }
    val separatorID = dataset1.map(_.id).max()
    val profilesDataset1 = dataset1.count()
    val dataset2 = {
      if (wrapperType == WRAPPER_TYPES.serialized) {
        SerializedObjectLoader.loadProfiles(pathDataset2, separatorID + 1)
      }
      else if(wrapperType == WRAPPER_TYPES.json){
        JsonRDDWrapper.loadProfiles(pathDataset2, separatorID + 1, "realId")
      }
      else {
        CSVWrapper.loadProfiles(filePath = pathDataset2, startIDFrom = separatorID + 1, header = true, realIDField = "id")
      }
    }
    val maxProfileID = dataset2.map(_.id).max()
    val profilesDataset2 = dataset2.count()

    val profiles = dataset1.union(dataset2)
    profiles.cache()
    val numProfiles = profiles.count()

    log.info("SPARKER - First dataset max ID "+separatorID)
    log.info("SPARKER - Max profiles id "+maxProfileID)
    val profilesTime = Calendar.getInstance()
    log.info("SPARKER - Number of profiles in the 1st dataset "+profilesDataset1)
    log.info("SPARKER - Number of profiles in the 2nd dataset "+profilesDataset2)
    log.info("SPARKER - Total number of profiles "+numProfiles)
    log.info("SPARKER - Time to load profiles "+(profilesTime.getTimeInMillis-startTime.getTimeInMillis)+" ms")
    log.info()

    var clusters : List[KeysCluster] = null
    var clusterTime = profilesTime

    if(blockingType == BLOCKING_TYPES.inferSchema){
      log.info("SPARKER - Start to generating clusters")
      log.info("SPARKER - Number of hashes "+hashNum)
      log.info("SPARKER - Target threshold "+clusterThreshold)
      clusters = LSHTwitter.clusterSimilarAttributes(profiles, hashNum, clusterThreshold, separatorID = separatorID)
      clusterTime = Calendar.getInstance()
      log.info("SPARKER - Time to generate clusters "+(clusterTime.getTimeInMillis-profilesTime.getTimeInMillis)+" ms")
      clusters.foreach(log.info)
      log.info()
    }

    log.info("SPARKER - Start to generating blocks")
    val blocks = {
      if(blockingType == BLOCKING_TYPES.inferSchema){
        TokenBlocking.createBlocksCluster(profiles, separatorID, clusters)
      }
      else{
        TokenBlocking.createBlocks(profiles, separatorID)
      }
    }
    blocks.cache()
    val numBlocks = blocks.count()
    profiles.unpersist()
    dataset1.unpersist()
    dataset2.unpersist()
    val blocksTime = Calendar.getInstance()
    log.info("SPARKER - Number of blocks "+numBlocks)
    log.info("SPARKER - Time to generate blocks "+(blocksTime.getTimeInMillis-clusterTime.getTimeInMillis)+" ms")
    log.info()


    log.info("SPARKER - Start to block purging, smooth factor "+purgingRatio)
    val blocksPurged = BlockPurging.blockPurging(blocks, purgingRatio)
    val numPurgedBlocks = blocksPurged.count()
    blocks.unpersist()
    val blocksPurgingTime = Calendar.getInstance()
    log.info("SPARKER - Number of blocks after purging "+numPurgedBlocks)
    log.info("SPARKER - Time to purging blocks "+(blocksPurgingTime.getTimeInMillis-blocksTime.getTimeInMillis)+" ms")
    log.info()


    log.info("SPARKER - Start to block filtering, factor "+filteringRatio)
    val profileBlocks = Converters.blocksToProfileBlocks(blocksPurged)
    val profileBlocksFiltered = BlockFiltering.blockFiltering(profileBlocks, filteringRatio)
    profileBlocksFiltered.cache()
    val blocksAfterFiltering = Converters.profilesBlockToBlocks(profileBlocksFiltered, separatorID)
    blocksAfterFiltering.cache()
    val numFilteredBlocks = blocksAfterFiltering.count()
    blocksPurged.unpersist()
    val blocksFilteringTime = Calendar.getInstance()
    log.info("SPARKER - Number of blocks after filtering "+numFilteredBlocks)
    log.info("SPARKER - Time to filtering blocks "+(blocksFilteringTime.getTimeInMillis-blocksPurgingTime.getTimeInMillis)+" ms")
    log.info()


    log.info("SPARKER - Start to pruning edges")
    val blockIndex = sc.broadcast(blocksAfterFiltering.map(b => (b.blockID, b.profiles)).collectAsMap())
    log.info("SPARKER - BlockIndex broadcast done")
    log.info("SPARKER - Groundtruth broadcast done")
    val profileBlocksSizeIndex : Broadcast[scala.collection.Map[Long, Int]] = {
      if(weightType == PruningUtils.WeightTypes.JS){
        sc.broadcast(profileBlocksFiltered.map(pb => (pb.profileID, pb.blocks.size)).collectAsMap())
      }
      else{
        null
      }
    }
    log.info("SPARKER - profileBlocksSizeIndex broadcast done (if JS)")
    val edges = WNPMat.WNP5(profileBlocksFiltered, blockIndex, maxProfileID.toInt, separatorID)
    edges.cache()
    //edges.repartition(1).write.csv(pathSaveEdges)
    val numEdges = edges.count()
    val pruningTime = Calendar.getInstance()
    blocksAfterFiltering.unpersist()
    blockIndex.unpersist()
    profileBlocksFiltered.unpersist()
    log.info("SPARKER - Number of retained edges "+numEdges)
    log.info()
    log.info("SPARKER - Time to pruning edges "+(pruningTime.getTimeInMillis-blocksFilteringTime.getTimeInMillis)+" ms")
    log.info()


    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "3000")
    import sqlContext.implicits._
    val sortedEdges = edges.sort($"_3".desc)
    sortedEdges.count()
    val sortingTime = Calendar.getInstance()
    log.info("SPARKER - Time to sort edges "+(sortingTime.getTimeInMillis-pruningTime.getTimeInMillis)+" ms")
    log.info()

    log.info("SPARKER - Total execution time "+(pruningTime.getTimeInMillis-startTime.getTimeInMillis))
  }
}
