package Experiments

import java.util.Calendar

import BlockBuildingMethods.{LSHTwitter, TokenBlocking}
import BlockRefinementMethods.{BlockFiltering, BlockPurging}
import BlockRefinementMethods.PruningMethods.{CNPFor, PruningUtils, WNPFor}
import DataStructures.KeysCluster
import Utilities.Converters
import Wrappers.{CSVWrapper, SerializedObjectLoader}
import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Luca on 07/03/2017.
  */
object MainGenerico {
  def main(args: Array[String]) {

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


    /*val memoryHeap = args(0)
    val memoryStack = args(1)
    val pathDataset1 = args(2)
    val pathDataset2 = args(3)
    val pathGt = args(4)*/

    val logPath = "C:/Users/Luca/Desktop/log.txt"
    val wrapperType = WRAPPER_TYPES.serialized
    val purgingRatio = 1.005
    val filteringRatio = 0.8
    val blockingType = BLOCKING_TYPES.inferSchema
    val pruningType = PRUNING_TYPES.WNP
    val thresholdType = WNPFor.ThresholdTypes.AVG
    val weightType = PruningUtils.WeightTypes.CBS
    val hashNum = 16
    val clusterThreshold = 0.3
    val useEntropy = true
    val memoryHeap = 15
    val memoryStack = 5
    val pathDataset1 = "C:/Users/Luca/Desktop/UNI/BlockingFramework/datasets/movies/profiles/dataset1"
    val pathDataset2 = "C:/Users/Luca/Desktop/UNI/BlockingFramework/datasets/movies/profiles/dataset2"
    val pathGt = "C:/Users/Luca/Desktop/UNI/BlockingFramework/datasets/movies/groundtruth"

    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val layout = new SimpleLayout();
    val appender = new FileAppender(layout,logPath,false);
    log.addAppender(appender);

    log.info("Heap "+memoryHeap+"g")
    log.info("Stack "+memoryStack+"g")
    log.info("First dataset path "+pathDataset1)
    log.info("Second dataset path "+pathDataset2)
    log.info("Groundtruth path "+pathGt)
    log.info("Threshold type "+thresholdType)
    log.info("Blocking type "+blockingType)
    log.info("Pruning type "+pruningType)
    log.info()


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

    log.info("Start to loading profiles")
    val startTime = Calendar.getInstance();
    val dataset1 = {
      if(wrapperType == WRAPPER_TYPES.serialized){
        SerializedObjectLoader.loadProfiles(pathDataset1)
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
      else {
        CSVWrapper.loadProfiles(filePath = pathDataset2, startIDFrom = separatorID + 1, header = true, realIDField = "id")
      }
    }
    val maxProfileID = dataset2.map(_.id).max()
    val profilesDataset2 = dataset2.count()

    val profiles = dataset1.union(dataset2)
    profiles.cache()
    val numProfiles = profiles.count()

    log.info("First dataset max ID "+separatorID)
    log.info("Max profiles id "+maxProfileID)
    val profilesTime = Calendar.getInstance()
    log.info("Number of profiles in the 1st dataset "+profilesDataset1)
    log.info("Number of profiles in the 2nd dataset "+profilesDataset2)
    log.info("Total number of profiles "+numProfiles)
    log.info("Time to load profiles "+(profilesTime.getTimeInMillis-startTime.getTimeInMillis)+" ms")
    log.info()

    log.info("Start to loading the groundtruth")
    val groundtruth = SerializedObjectLoader.loadGroundtruth(pathGt)//CSVWrapper.loadGroundtruth(filePath = pathGt, header = true)
    val gtNum = groundtruth.count()
    val realIdId1 = sc.broadcast(dataset1.map(p => (p.originalID, p.id)).collectAsMap())
    val realIdId2 = sc.broadcast(dataset2.map(p => (p.originalID, p.id)).collectAsMap())
    log.info("Start to generate the new groundtruth")
    val newGT = groundtruth.map(g => (realIdId1.value(g.firstEntityID), realIdId2.value(g.secondEntityID))).collect().toSet
    realIdId1.unpersist()
    realIdId2.unpersist()
    groundtruth.cache()
    log.info("Generation completed")
    groundtruth.unpersist()
    val gtTime = Calendar.getInstance()
    log.info("Time to generate the new groundtruth "+(gtTime.getTimeInMillis-profilesTime.getTimeInMillis)+" ms")
    log.info()

    var clusters : List[KeysCluster] = null
    var clusterTime = gtTime

    if(blockingType == BLOCKING_TYPES.inferSchema){
      log.info("Start to generating clusters")
      log.info("Number of hashes "+hashNum)
      log.info("Target threshold "+clusterThreshold)
      clusters = LSHTwitter.clusterSimilarAttributes2(profiles, hashNum, clusterThreshold, separatorID = separatorID)
      clusterTime = Calendar.getInstance()
      log.info("Time to generate clusters "+(clusterTime.getTimeInMillis-gtTime.getTimeInMillis)+" ms")
      clusters.foreach(log.info)
      log.info()
    }

    log.info("Start to generating blocks")
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
    log.info("Number of blocks "+numBlocks)
    log.info("Time to generate blocks "+(blocksTime.getTimeInMillis-clusterTime.getTimeInMillis)+" ms")
    log.info()


    log.info("Start to block purging, smooth factor "+purgingRatio)
    val blocksPurged = BlockPurging.blockPurging(blocks, purgingRatio)
    val numPurgedBlocks = blocksPurged.count()
    blocks.unpersist()
    val blocksPurgingTime = Calendar.getInstance()
    log.info("Number of blocks after purging "+numPurgedBlocks)
    log.info("Time to purging blocks "+(blocksPurgingTime.getTimeInMillis-blocksTime.getTimeInMillis)+" ms")
    log.info()


    log.info("Start to block filtering, factor "+filteringRatio)
    val profileBlocks = Converters.blocksToProfileBlocks(blocksPurged)
    val profileBlocksFiltered = BlockFiltering.blockFiltering(profileBlocks, filteringRatio)
    profileBlocksFiltered.cache()
    val blocksAfterFiltering = Converters.profilesBlockToBlocks(profileBlocksFiltered, separatorID)
    blocksAfterFiltering.cache()
    val numFilteredBlocks = blocksAfterFiltering.count()
    blocksPurged.unpersist()
    val blocksFilteringTime = Calendar.getInstance()
    log.info("Number of blocks after filtering "+numFilteredBlocks)
    log.info("Time to filtering blocks "+(blocksFilteringTime.getTimeInMillis-blocksPurgingTime.getTimeInMillis)+" ms")
    log.info()


    log.info("Start to pruning edges")
    val blockIndex = sc.broadcast(blocksAfterFiltering.map(b => (b.blockID, b.profiles)).collectAsMap())
    val gt = sc.broadcast(newGT)
    val profileBlocksSizeIndex : Broadcast[scala.collection.Map[Long, Int]] = {
      if(weightType == PruningUtils.WeightTypes.JS){
        sc.broadcast(profileBlocksFiltered.map(pb => (pb.profileID, pb.blocks.size)).collectAsMap())
      }
      else{
        null
      }
    }
    val edgesAndCount = {
      if(pruningType == PRUNING_TYPES.CNP){
        val CNPThreshold = CNPFor.computeThreshold(blocksAfterFiltering, numProfiles)
        log.info("CNP Threshold "+CNPThreshold)
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
    log.info("Number of retained edges "+numEdges)
    log.info("Number of perfect match found "+perfectMatch)
    log.info("Number of elements in the gt "+gtNum)
    log.info("PC = "+(perfectMatch.toFloat/gtNum.toFloat))
    log.info("PQ = "+(perfectMatch.toFloat/numEdges.toFloat))
    log.info()
    log.info("Time to pruning edges "+(pruningTime.getTimeInMillis-gtTime.getTimeInMillis)+" ms")
    log.info()
    log.info("Total execution time "+(pruningTime.getTimeInMillis-startTime.getTimeInMillis))
    sc.stop()
  }
}
