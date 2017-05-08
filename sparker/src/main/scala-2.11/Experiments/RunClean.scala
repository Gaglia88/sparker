package Experiments

import java.util.Calendar

import BlockBuildingMethods.LSHTwitter.Settings
import BlockBuildingMethods.{LSHTwitter, TokenBlocking}
import BlockRefinementMethods.{BlockFiltering, BlockPurging}
import BlockRefinementMethods.PruningMethods.{CNPFor, PruningUtils, WNPFor, WNPFor2}
import DataStructures.{KeysCluster, UnweightedEdge}
import Utilities.Converters
import Wrappers.{CSVWrapper, JsonRDDWrapper, SerializedObjectLoader}
import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

/**
  * Created by Luca on 13/03/2017.
  */
object RunClean {
  object WRAPPER_TYPES {
    val serialized = "serialized"
    val csv = "csv"
    val json = "json"
  }

  object PRUNING_TYPES {
    val CNP = "CNP"
    val WNP = "WNP"
    val DATASET = "DATASET"
  }

  object BLOCKING_TYPES {
    val inferSchema = "inferSchema"
    val schemaLess = "schemaLess"
    val manualMapping = "manualMapping"
  }

  object DATASETS {
    val abtBuy = "abtBuy"
    val googleAmazon = "googleAmazon"
    val scholarDblp = "scholarDblp"
    val movies = "movies"
  }

  def main(args: Array[String]) {
    val baseBath = "C:/Users/Luca/Desktop/datasets/"+DATASETS.scholarDblp
    val pathDataset1 = baseBath+"/dataset1"
    val pathDataset2 = baseBath+"/dataset2"
    val pathGt = baseBath+"/groundtruth"
    val defaultLogPath = "C:/Users/Luca/Desktop/"
    val wrapperType = RunClean.WRAPPER_TYPES.serialized
    val GTWrapperType = RunClean.WRAPPER_TYPES.serialized
    val purgingRatio = 1.005
    val filteringRatio = 0.8
    val hashNum = 16
    val clusterThreshold = 0.3
    val memoryHeap = 118
    val memoryStack = 5
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


    val a = PruningUtils.WeightTypes.JS :: PruningUtils.WeightTypes.CBS :: PruningUtils.WeightTypes.chiSquare :: PruningUtils.WeightTypes.ARCS :: Nil

    val b = for(i <- 0 to a.size-1) yield {
      RunClean.runClean(
        sc,
        defaultLogPath + "WNP_CBS_AVG_SCHEMA_ENTROPY.txt",
        wrapperType,
        purgingRatio,
        filteringRatio,
        RunClean.BLOCKING_TYPES.schemaLess,
        RunClean.PRUNING_TYPES.WNP,
        WNPFor.ThresholdTypes.AVG,
        a(i),
        hashNum,
        clusterThreshold,
        useEntropy = false,
        memoryHeap,
        memoryStack,
        pathDataset1,
        pathDataset2,
        pathGt,
        GTWrapperType,
        realId
      )
    }

    b.foreach(println)


    //RunClean.runClean(sc, defaultLogPath+"WNP_CBS_AVG_SCHEMA_ENTROPY.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.schemaLess, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.AVG, PruningUtils.WeightTypes.JS, hashNum, clusterThreshold, useEntropy = true, memoryHeap, memoryStack, pathDataset1, pathDataset2, pathGt, GTWrapperType, realId)
    //RunClean.runClean(sc, defaultLogPath+"WNP_CBS_AVG_NO_SCHEMA_NO_ENTROPY.txt", wrapperType, purgingRatio, filteringRatio, RunClean.BLOCKING_TYPES.schemaLess, RunClean.PRUNING_TYPES.WNP, WNPFor.ThresholdTypes.AVG, PruningUtils.WeightTypes.CBS, hashNum, clusterThreshold, useEntropy = false, memoryHeap, memoryStack, pathDataset1, pathDataset2, pathGt, GTWrapperType, realId)
  }

  def runClean(sc : SparkContext, logPath : String, wrapperType : String, purgingRatio : Double, filteringRatio : Double, blockingType : String,
               pruningType : String, thresholdType : String, weightType : String, hashNum : Int, clusterThreshold : Double,
               useEntropy : Boolean, memoryHeap : Int, memoryStack : Int, pathDataset1 : String, pathDataset2 : String,
               pathGt : String, wrapperGtType : String, realID : String = "", manualClusters : List[KeysCluster] = Nil, storageLevel : StorageLevel = StorageLevel.MEMORY_AND_DISK): String = {
    
    
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val layout = new SimpleLayout();
    val appender = new FileAppender(layout,logPath,false);
    log.addAppender(appender);

    log.info("SPARKER - Heap "+memoryHeap+"g")
    log.info("SPARKER - Stack "+memoryStack+"g")
    log.info("SPARKER - First dataset path "+pathDataset1)
    log.info("SPARKER - Second dataset path "+pathDataset2)
    log.info("SPARKER - Groundtruth path "+pathGt)
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
        JsonRDDWrapper.loadProfiles(pathDataset1, 0, realID)
      }
      else{
        CSVWrapper.loadProfiles(filePath = pathDataset1, header = true, realIDField = realID)
      }
    }
    val separatorID = dataset1.map(_.id).max()
    val profilesDataset1 = dataset1.count()
    val dataset2 = {
      if (wrapperType == WRAPPER_TYPES.serialized) {
        SerializedObjectLoader.loadProfiles(pathDataset2, separatorID + 1)
      }
      else if(wrapperType == WRAPPER_TYPES.json){
        JsonRDDWrapper.loadProfiles(pathDataset2, separatorID + 1, realID)
      }
      else {
        CSVWrapper.loadProfiles(filePath = pathDataset2, startIDFrom = separatorID + 1, header = true, realIDField = realID)
      }
    }
    val maxProfileID = dataset2.map(_.id).max()
    val profilesDataset2 = dataset2.count()

    val profiles = dataset1.union(dataset2)
    profiles.persist(storageLevel)
    val numProfiles = profiles.count()

    log.info("SPARKER - First dataset max ID "+separatorID)
    log.info("SPARKER - Max profiles id "+maxProfileID)
    val profilesTime = Calendar.getInstance()
    log.info("SPARKER - Number of profiles in the 1st dataset "+profilesDataset1)
    log.info("SPARKER - Number of profiles in the 2nd dataset "+profilesDataset2)
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
    val realIdId1 = sc.broadcast(dataset1.map{p =>
      (p.originalID, p.id)
    }.collectAsMap())
    val realIdId2 = sc.broadcast(dataset2.map{p =>
      (p.originalID, p.id)
    }.collectAsMap())
    log.info("SPARKER - Start to generate the new groundtruth")
    val newGT = groundtruth.map{g =>
      val first = realIdId1.value.get(g.firstEntityID)
      val second = realIdId2.value.get(g.secondEntityID)
      if(first.isDefined && second.isDefined){
        (first.get, second.get)
      }
      else{
        (-1L, -1L)
      }
    }.filter(_._1 >= 0).collect().toSet
    realIdId1.unpersist()
    realIdId2.unpersist()
    groundtruth.persist(storageLevel)
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
      log.info("SPARKER - Generated clusters")
      clusters.foreach(log.info)
      clusterTime = Calendar.getInstance()
      log.info("SPARKER - Time to generate clusters "+(clusterTime.getTimeInMillis-gtTime.getTimeInMillis)+" ms")
      clusters.foreach(log.info)
      log.info()
    }
    else if(blockingType == BLOCKING_TYPES.manualMapping){
      clusters = KeysCluster(manualClusters.map(_.id).max+1, (Settings.DEFAULT_CLUSTER_NAME :: Nil)) :: manualClusters
    }

    log.info("SPARKER - Start to generating blocks")
    val blocks = {
      if(blockingType == BLOCKING_TYPES.inferSchema || blockingType == BLOCKING_TYPES.manualMapping){
        TokenBlocking.createBlocksCluster(profiles, separatorID, clusters)
      }
      else{
        TokenBlocking.createBlocks(profiles, separatorID)
      }
    }

    blocks.persist(storageLevel)
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
    profileBlocksFiltered.persist(storageLevel)
    val blocksAfterFiltering = Converters.profilesBlockToBlocks(profileBlocksFiltered, separatorID)
    blocksAfterFiltering.persist(storageLevel)
    val numFilteredBlocks = blocksAfterFiltering.count()
    blocksPurged.unpersist()
    val blocksFilteringTime = Calendar.getInstance()
    log.info("SPARKER - Number of blocks after filtering "+numFilteredBlocks)
    log.info("SPARKER - Time to filtering blocks "+(blocksFilteringTime.getTimeInMillis-blocksPurgingTime.getTimeInMillis)+" ms")
    log.info()

    log.info("SPARKER - Start to pruning edges")
    val blockIndexMap = blocksAfterFiltering.map(b => (b.blockID, b.profiles)).collectAsMap()
    val blockIndex = sc.broadcast(blockIndexMap)
    log.info("SPARKER - Size of the broadcast blockIndex "+SizeEstimator.estimate(blockIndexMap)+" byte")
    log.info("SPARKER - BlockIndex broadcast done")
    val gt = sc.broadcast(newGT)
    log.info("SPARKER - Size of the broadcast groundtruth "+SizeEstimator.estimate(newGT)+" byte")
    log.info("SPARKER - Groundtruth broadcast done")
    val profileBlocksSizeIndex : Broadcast[scala.collection.Map[Long, Int]] = {
      if(weightType == PruningUtils.WeightTypes.JS || weightType == PruningUtils.WeightTypes.chiSquare){
        val profileBlocksMap = profileBlocksFiltered.map(pb => (pb.profileID, pb.blocks.size)).collectAsMap()
        log.info("SPARKER - Size of the broadcast profileBlocksMap "+SizeEstimator.estimate(profileBlocksMap)+" byte")
        sc.broadcast(profileBlocksMap)
      }
      else{
        null
      }
    }
    log.info("SPARKER - profileBlocksSizeIndex broadcast done (if JS)")

    val blocksEntropiesMap : Broadcast[scala.collection.Map[Long, Double]] = {
      if(useEntropy) {
        val blocksEntropies = blocks.map(b => (b.blockID, b.entropy)).collectAsMap()
        log.info("SPARKER - Size of the broadcast blocksEntropiesMap " + SizeEstimator.estimate(blocksEntropies) + " byte")
        sc.broadcast(blocksEntropies)
      }
      else{
        null
      }
    }


    if(pruningType == PRUNING_TYPES.DATASET){
      val edgesAndCount = WNPFor.WNP_dataset(profileBlocksFiltered, blockIndex, maxProfileID.toInt, separatorID, gt, thresholdType, weightType, profileBlocksSizeIndex)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      sqlContext.setConf("spark.sql.shuffle.partitions", "3000")
      import sqlContext.implicits._
      val numEdges = edgesAndCount.agg(sum($"_1")).first().getAs[Double](0)

      val edges = edgesAndCount.select($"_2").where("_2 is not null").rdd.flatMap{row =>
        val edges = row.getSeq[Row](0)
        edges.map{
          case Row(e1 : Long, e2 : Long, w12 : Float) =>
            (e1, e2, w12)
        }
      }.distinct()

      edges.persist(storageLevel)
      val perfectMatch = edges.count()
      val pruningTime = Calendar.getInstance()
      blocksAfterFiltering.unpersist()
      blockIndex.unpersist()
      profileBlocksFiltered.unpersist()
      log.info("SPARKER - Number of retained edges "+numEdges)
      log.info("SPARKER - Number of perfect match found "+perfectMatch)
      log.info("SPARKER - Number of elements in the gt "+newGTSize)
      log.info("SPARKER - PC = "+(perfectMatch.toFloat/newGTSize.toFloat))
      log.info("SPARKER - PQ = "+(perfectMatch.toFloat/numEdges))
      log.info()
      log.info("SPARKER - Time to pruning edges "+(pruningTime.getTimeInMillis-gtTime.getTimeInMillis)+" ms")
      log.info()
      log.info("SPARKER - Total execution time "+(pruningTime.getTimeInMillis-startTime.getTimeInMillis))
      ""
    }
    else{
      val edgesAndCount = {
        if(pruningType == PRUNING_TYPES.CNP){
          val CNPThreshold = CNPFor.computeThreshold(blocksAfterFiltering, numProfiles)
          log.info("SPARKER - CNP Threshold "+CNPThreshold)
          CNPFor.CNP(profileBlocksFiltered, blockIndex, maxProfileID.toInt, separatorID, gt, CNPThreshold, weightType, profileBlocksSizeIndex)
        }
        else{
          WNPFor2.WNP(
            profileBlocksFiltered,
            blockIndex,
            maxProfileID.toInt,
            separatorID,
            gt,
            thresholdType,
            weightType,
            profileBlocksSizeIndex,
            useEntropy,
            blocksEntropiesMap
          )
          /*if(useEntropy){
            val blocksEntropies = blocks.map(b => (b.blockID, b.entropy)).collectAsMap()
            log.info("SPARKER - Size of the broadcast blocksEntropiesMap "+SizeEstimator.estimate(blocksEntropies)+" byte")
            val blocksEntropiesMap = sc.broadcast(blocksEntropies)
            //WNPFor.WNPEntropy(profileBlocksFiltered, blockIndex, maxProfileID.toInt, separatorID, gt, blocksEntropiesMap, thresholdType, weightType, profileBlocksSizeIndex)
            //WNPFor.WNPEntropyChiSquare(profileBlocksFiltered, blockIndex, maxProfileID.toInt, separatorID, gt, blocksEntropiesMap, thresholdType, profileBlocksSizeIndex)
            WNPFor.WNPArcs(profileBlocksFiltered, blockIndex, maxProfileID.toInt, separatorID, gt, blocksEntropiesMap, thresholdType, profileBlocksSizeIndex)
          }
          else{
            WNPFor.WNP(profileBlocksFiltered, blockIndex, maxProfileID.toInt, separatorID, gt, thresholdType, weightType, profileBlocksSizeIndex)
          }*/
        }
      }
      edgesAndCount.persist(storageLevel)
      val numEdges = edgesAndCount.map(_._1).sum()
      val edges = edgesAndCount.flatMap(_._2).distinct()
      edges.persist(storageLevel)
      val perfectMatch = edges.count()
      val pruningTime = Calendar.getInstance()

      log.info("SPARKER - Number of retained edges "+numEdges)
      log.info("SPARKER - Number of perfect match found "+perfectMatch)
      log.info("SPARKER - Number of elements in the gt "+newGTSize)
      log.info("SPARKER - PC = "+(perfectMatch.toFloat/newGTSize.toFloat))
      log.info("SPARKER - PQ = "+(perfectMatch.toFloat/numEdges.toFloat))
      log.info()
      log.info("SPARKER - Time to pruning edges "+(pruningTime.getTimeInMillis-gtTime.getTimeInMillis)+" ms")
      log.info()
      log.info("SPARKER - Total execution time "+(pruningTime.getTimeInMillis-startTime.getTimeInMillis))

      "blockingType = "+blockingType+", thresholdType = "+thresholdType+", weightType = "+weightType+", useEntropy = "+useEntropy+", PC = "+(perfectMatch.toFloat/newGTSize.toFloat)+", PQ = "+(perfectMatch.toFloat/numEdges.toFloat)+", Total execution time "+(pruningTime.getTimeInMillis-startTime.getTimeInMillis)
    }
  }
}
