package Experiments

import java.util.Calendar

import BlockBuildingMethods.{LSHTwitter, TokenBlocking}
import BlockBuildingMethods.LSHTwitter.Settings
import BlockRefinementMethods.PruningMethods.{CNPFor, PruningUtils, WNPFor}
import BlockRefinementMethods.{BlockFiltering, BlockPurging}
import DataStructures.KeysCluster
import Utilities.Converters
import Wrappers.{CSVWrapper, JsonRDDWrapper, SerializedObjectLoader}
import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast

/**
  * Created by Luca on 27/03/2017.
  */
object RunDirty {
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
    val manualMapping = "manualMapping"
  }

  def main(args: Array[String]) {
    val pathDataset1 = "C:\\Users\\Luca\\Downloads\\songs\\msd.csv"
    val pathGt = "C:\\Users\\Luca\\Downloads\\songs\\matches_msd_msd.csv"
    val defaultLogPath = "C:/Users/Luca/Desktop/"
    val wrapperType = RunClean.WRAPPER_TYPES.csv
    val GTWrapperType = RunClean.WRAPPER_TYPES.csv
    val purgingRatio = 1.015
    val filteringRatio = 0.8
    val hashNum = 16
    val clusterThreshold = 0.3
    val memoryHeap = 14
    val memoryStack = 1
    val realId = "id"

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

    /*val clusters = "id,title,release,artist_name,duration,artist_familiarity,artist_hotttnesss,year".split(",").toList
    runDirty(sc, defaultLogPath+"WNP_CBS_AVG_MANUAL_SCHEMA_NO_ENTROPY.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.manualMapping, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.AVG, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = false, memoryHeap, memoryStack, pathDataset1, pathGt, GTWrapperType, realId, clusters)*/

    RunDirty.runDirty(sc, defaultLogPath+"WNP_CBS_AVG_NO_SCHEMA_NO_ENTROPY.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.schemaLess, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.AVG, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = false, memoryHeap, memoryStack, pathDataset1, pathGt, GTWrapperType, realId)
  }

  def runDirty(sc : SparkContext, logPath : String, wrapperType : String, purgingRatio : Double, filteringRatio : Double, blockingType : String,
               pruningType : String, thresholdType : String, weightType : String, hashNum : Int, clusterThreshold : Double,
               useEntropy : Boolean, memoryHeap : Int, memoryStack : Int, pathDataset1 : String,
               pathGt : String, wrapperGtType : String, realID : String = "", attributesToUse : List[String] = Nil): Unit = {

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val layout = new SimpleLayout();
    val appender = new FileAppender(layout,logPath,false);
    log.addAppender(appender);

    log.info("SPARKER - Heap "+memoryHeap+"g")
    log.info("SPARKER - Stack "+memoryStack+"g")
    log.info("SPARKER - First dataset path "+pathDataset1)
    log.info("SPARKER - Groundtruth path "+pathGt)
    log.info("SPARKER - Threshold type "+thresholdType)
    log.info("SPARKER - Blocking type "+blockingType)
    log.info("SPARKER - Pruning type "+pruningType)
    log.info()

    log.info("SPARKER - Start to loading the profiles")
    val startTime = Calendar.getInstance();
    val profiles = {
      if(wrapperType == WRAPPER_TYPES.serialized){
        SerializedObjectLoader.loadProfiles(pathDataset1)
      }
      else if(wrapperType == WRAPPER_TYPES.json){
        JsonRDDWrapper.loadProfiles(pathDataset1, 0, realID)
      }
      else{
        CSVWrapper.loadProfiles(filePath = pathDataset1, header = true, realIDField = realID)
      }
    }
    val separatorID = -1
    val maxProfileID = profiles.map(_.id).max()
    profiles.cache()
    val numProfiles = profiles.count()


    log.info("SPARKER - Max profiles id "+maxProfileID)
    val profilesTime = Calendar.getInstance()
    log.info("SPARKER - Total number of profiles "+numProfiles)
    log.info("SPARKER - Time to load profiles "+(profilesTime.getTimeInMillis-startTime.getTimeInMillis)+" ms")
    log.info()
    log.info()

    log.info("SPARKER - Start to loading the groundtruth")
    val groundtruth = {
      if(wrapperGtType == WRAPPER_TYPES.serialized){
        SerializedObjectLoader.loadGroundtruth(pathGt)
      }
      else{
        CSVWrapper.loadGroundtruth(filePath = pathGt, header = true)
      }
    }
    val gtNum = groundtruth.count()
    val realIds = sc.broadcast(profiles.map(p => (p.originalID, p.id)).collectAsMap())

    log.info("SPARKER - Start to generate the new groundtruth")
    val newGT = groundtruth.map{
      g =>
        val first = realIds.value(g.firstEntityID)
        val second = realIds.value(g.secondEntityID)
        if(first < second){
          (first, second)
        }
        else{
          (second, first)
        }
    }.distinct().collect().toSet
    realIds.unpersist()
    groundtruth.cache()
    val newGTSize = newGT.size
    log.info("SPARKER - Generation completed")
    log.info("SPARKER - Number of elements in the new groundtruth "+newGTSize)
    groundtruth.unpersist()
    val gtTime = Calendar.getInstance()
    log.info("SPARKER - Time to generate the new groundtruth "+(gtTime.getTimeInMillis-profilesTime.getTimeInMillis)+" ms")
    log.info()

    var clusters : List[KeysCluster] = null
    var clusterTime = gtTime

    if(blockingType == BLOCKING_TYPES.inferSchema){
      log.info("SPARKER - Start to generating clusters")
      log.info("SPARKER - Number of hashes "+hashNum)
      log.info("SPARKER - Target threshold "+clusterThreshold)
      clusters = LSHTwitter.clusterSimilarAttributes(profiles, hashNum, clusterThreshold, separatorID = separatorID)
      clusterTime = Calendar.getInstance()
      log.info("SPARKER - Time to generate clusters "+(clusterTime.getTimeInMillis-gtTime.getTimeInMillis)+" ms")
      clusters.foreach(log.info)
      log.info()
    }

    log.info("SPARKER - Start to generating blocks")
    val blocks = {
      if(blockingType == BLOCKING_TYPES.inferSchema){
        TokenBlocking.createBlocksCluster(profiles, separatorID, clusters)
      }
      else if(blockingType == BLOCKING_TYPES.manualMapping){
        if(useEntropy){
          TokenBlocking.createBlocksUsingSchema(profiles, separatorID, attributesToUse)
        }
        else{
          TokenBlocking.createBlocksUsingSchemaEntropy(profiles, separatorID, attributesToUse)
        }
      }
      else{
        TokenBlocking.createBlocks(profiles, separatorID)
      }
    }
    blocks.cache()
    val numBlocks = blocks.count()
    profiles.unpersist()
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
    val gt = sc.broadcast(newGT)
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
    val edgesAndCount = {
      if(pruningType == PRUNING_TYPES.CNP){
        val CNPThreshold = CNPFor.computeThreshold(blocksAfterFiltering, numProfiles)
        log.info("SPARKER - CNP Threshold "+CNPThreshold)
        CNPFor.CNP(profileBlocksFiltered, blockIndex, maxProfileID.toInt, separatorID, gt, CNPThreshold, weightType, profileBlocksSizeIndex)
      }
      else{
        if(useEntropy){
          val blocksEntropies = blocks.map(b => (b.blockID, b.entropy))
          val blocksEntropiesMap = sc.broadcast(blocksEntropies.collectAsMap())
          WNPFor.WNPEntropy(profileBlocksFiltered, blockIndex, maxProfileID.toInt, separatorID, gt, blocksEntropiesMap, thresholdType, weightType, profileBlocksSizeIndex)
        }
        else{
          WNPFor.WNP(profileBlocksFiltered, blockIndex, maxProfileID.toInt, separatorID, gt, thresholdType, weightType, profileBlocksSizeIndex)
        }
      }
    }
    edgesAndCount.cache()
    val numEdges = edgesAndCount.map(_._1).sum()
    val edges = edgesAndCount.flatMap(_._2).distinct()
    edges.cache()
    val perfectMatch = edges.count()
    val pruningTime = Calendar.getInstance()
    blocksAfterFiltering.unpersist()
    blockIndex.unpersist()
    profileBlocksFiltered.unpersist()
    log.info("SPARKER - Number of retained edges "+numEdges)
    log.info("SPARKER - Number of perfect match found "+perfectMatch)
    log.info("SPARKER - Number of elements in the gt "+newGTSize)
    log.info("SPARKER - PC = "+(perfectMatch.toFloat/newGTSize.toFloat))
    log.info("SPARKER - PQ = "+(perfectMatch.toFloat/numEdges.toFloat))
    log.info()
    log.info("SPARKER - Time to pruning edges "+(pruningTime.getTimeInMillis-gtTime.getTimeInMillis)+" ms")
    log.info()
    log.info("SPARKER - Total execution time "+(pruningTime.getTimeInMillis-startTime.getTimeInMillis))
  }
}
