package Experiments


import java.util.{Calendar}

import BlockBuildingMethods.{LSHLuca, TokenBlocking}
import BlockRefinementMethods.{BlockFiltering, BlockPurging}
import BlockRefinementMethods.PruningMethods.{CNPFor, PruningUtils, WNPFor}
import Utilities.Converters
import Wrappers.{CSVWrapper, JSONWrapper, SerializedObjectLoader}
import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Allow to run all experiments on the datasets
  *
  * @author Luca Gagliardelli
  * @since 07/09/2017
  */
object AllTest {

  /**
    * Represents a dataset
    * @param name name of the dataset
    * @param wrapperType type of wrapper
    * @param purgingRatio purging ratio
    * @param filteringRatio filtering ratio
    * @param hashNum number of hash for LSH
    * @param clusterThreshold cluster threshold for LSH
    * @param blastDiv divider for Blast threshold
    * */
  case class Dataset(name : String, wrapperType : String, purgingRatio : Double, filteringRatio : Double, hashNum : Int = 128, clusterThreshold : Double = 0.3, blastDiv : Double = 2.0){}

  /** Types of wrappers */
  object WRAPPER_TYPES {
    val serialized = ""
    val csv = ".csv"
    val json = ".json"
  }

  /** Types of blocking */
  object BLOCKING_TYPES {
    val inferSchema = "inferSchema"
    val schemaLess = "schemaLess"
    val manualMapping = "manualMapping"
  }

  /**
    * Computes the log name
    * @param numberOfNodes number of nodes
    * @param defaultLogPath absolute log path
    * @param baseName base name of the log
    *
    * @return the full log path as: defaultLogPath+baseName+"_"+numberOfNodes+"nodes_"+intNum+".txt"
    * */
  def getLogName(numberOfNodes: String, defaultLogPath: String, baseName: String): String = {
    var i = 0
    while (scala.reflect.io.File(defaultLogPath + baseName + "_" + numberOfNodes + "nodes" + "_" + i + ".txt").exists) {
      i = i + 1
    }
    defaultLogPath + baseName + "_" + numberOfNodes + "nodes" + "_" + i + ".txt"
  }

  /**
    * Converts ms to minutes
    *
    * @param ms time in milliseconds
    * */
  def msToMin(ms: Long): Double = (ms / 1000.0) / 60.0


  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.network.timeout", "10000s")
      .set("spark.executor.memory", "10g")
      .set("spark.executor.heartbeatInterval", "40s")
      .set("spark.default.parallelism", "4")
      .set("spark.local.dir", "/data2/sparkTmp/")
      .set("spark.driver.maxResultSize", "0")

    val sc = new SparkContext(conf)


    /*
    * List of datasets on wich the tests will be performed
    * */
    val datasets = List(
      Dataset("movies", WRAPPER_TYPES.serialized, 1.005, 0.8, 128, 0.3, 4.0),
      Dataset("citations", WRAPPER_TYPES.csv, 1.00, 0.8, 128, 0.2),
      Dataset("dbpedia", WRAPPER_TYPES.json, 1.00, 0.8),
      Dataset("freebase", WRAPPER_TYPES.json, 1.0, 0.7, 128, 0.3, 4.0)
    )

    /* List of blocking types to test */
    val blockingTypes = List(BLOCKING_TYPES.inferSchema, BLOCKING_TYPES.schemaLess, "BLAST")

    /* Execute the test on each dataset */
    datasets.foreach { d =>

      val wrapperType = d.wrapperType
      val GTWrapperType = wrapperType

      val dataset = d.name

      /* Base datasets path */
      val basePath = "/data2/ER/"
      val pathDataset1 = basePath + dataset + "/dataset1" + wrapperType
      val pathDataset2 = basePath + dataset + "/dataset2" + wrapperType
      val pathGt = basePath + dataset + "/groundtruth" + wrapperType

      /* Base log path */
      val defaultLogPath = "/home/luca/"

      val purgingRatio = d.purgingRatio
      val filteringRatio = d.filteringRatio
      val hashNum = d.hashNum
      val clusterThreshold = d.clusterThreshold

      val realId = {
        if (wrapperType == WRAPPER_TYPES.json) {
          "realProfileID"
        }
        else {
          "id"
        }
      }

      val clusterSeparateAttributes = true
      val clusterMaxFactor = 1


      /* Foreach blocking type performs the tests */
      blockingTypes.foreach { blockingType =>
        val blockingType1 = {
          if(blockingType == "BLAST"){
            BLOCKING_TYPES.inferSchema
          }
          else{
            blockingType
          }
        }

        val useEntropy = {
          if(blockingType == "BLAST"){
            true
          }
          else{
            false
          }
        }

        val thresholdType = {
          if(blockingType == "BLAST"){
            PruningUtils.ThresholdTypes.MAX_FRACT_2
          }
          else{
            PruningUtils.ThresholdTypes.AVG
          }
        }

        val logName = getLogName("1", defaultLogPath, dataset + "_allTest" + blockingType)

        val weightTypes = {
          if(blockingType == "BLAST"){
            List(PruningUtils.WeightTypes.chiSquare)
          }
          else{
            List(PruningUtils.WeightTypes.JS, PruningUtils.WeightTypes.CBS, PruningUtils.WeightTypes.ARCS, PruningUtils.WeightTypes.EJS, PruningUtils.WeightTypes.ECBS)
          }
        }

        val blast = {
          if(blockingType == "BLAST"){
            true
          }
          else{
            false
          }
        }

        runClean(
          sc,
          logName,
          wrapperType,
          purgingRatio,
          filteringRatio,
          blockingType1,
          thresholdType,
          pathDataset1,
          pathDataset2,
          pathGt,
          GTWrapperType,
          weightTypes,
          realId,
          useEntropy,
          hashNum,
          clusterSeparateAttributes,
          clusterMaxFactor,
          clusterThreshold,
          d.blastDiv,
          blast
        )
      }
    }
    sc.stop()
  }

  /**
    * Execute the meta-blocking on clean datasets
    *
    * @param sc Spark context
    * @param logPath absolute log path
    * @param wrapperType type of wrapper for the datasets
    * @param purgingRatio purging factor (>= 1.0)
    * @param filteringRatio filtering ration ]0,1]
    * @param blockingType blocking type
    * @param thresholdType threshold type (avg, max/2)
    * @param pathDataset1 absolute path dataset1
    * @param pathDataset2 absolute path dataset2
    * @param pathGt absolute groundtruth path
    * @param wrapperGtType wrapper type for the groundtruth
    * @param weightTypes list of weight types to apply
    * @param realID name of the field in a record that corresponds to the groundtruth fields
    * @param useEntropy use the entropy to improve the process
    * @param hashNum number of hashes for LSH
    * @param clusterSeparateAttributes if true keeps only clusters that contains attribute from both datasets
    * @param clusterMaxFactor ...
    * @param clusterThreshold threshold for LSH
    * @param blastDiv blast divider, used when computes the threshold
    * @param blast true if its a Blast test
    * @param storageLevel Spark storage level for cached items
    * */
  def runClean(sc: SparkContext, logPath: String, wrapperType: String, purgingRatio: Double, filteringRatio: Double, blockingType: String,
               thresholdType: String, pathDataset1: String, pathDataset2: String,
               pathGt: String, wrapperGtType: String, weightTypes: List[String], realID: String = "", useEntropy: Boolean = false,
               hashNum : Int, clusterSeparateAttributes : Boolean, clusterMaxFactor : Double, clusterThreshold : Double,
               blastDiv : Double, blast : Boolean,
               storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY): Unit = {


    /* Inizialize the log */
    val log = LogManager.getRootLogger
    log.setLevel(Level.INFO)
    val layout = new SimpleLayout();
    val appender = new FileAppender(layout, logPath, false);
    log.addAppender(appender);

    log.info("SPARKER - logPath " + logPath)
    log.info("SPARKER - purgingRatio " + purgingRatio)
    log.info("SPARKER - filteringRatio " + filteringRatio)
    log.info("SPARKER - hashNum " + hashNum)
    log.info("SPARKER - pathDataset1 " + pathDataset1)
    log.info("SPARKER - pathDataset2 " + pathDataset2)
    log.info("SPARKER - pathGt " + pathGt)
    log.info("SPARKER - realID " + realID)
    log.info("SPARKER - clusterSeparateAttributes " + clusterSeparateAttributes)
    log.info("SPARKER - clusterMaxFactor " + clusterMaxFactor)
    log.info("SPARKER - storageLevel " + storageLevel)
    log.info("SPARKER - clusterThreshold " + clusterThreshold)
    log.info("SPARKER - use entropy "+useEntropy)

    log.info("SPARKER - First dataset path " + pathDataset1)
    log.info("SPARKER - Second dataset path " + pathDataset2)
    log.info("SPARKER - Groundtruth path " + pathGt)
    log.info("SPARKER - Threshold type " + thresholdType)
    log.info("SPARKER - Blocking type " + blockingType)
    log.info()

    log.info("SPARKER - Start to loading the profiles")
    val startTime = Calendar.getInstance();
    val dataset1 = {
      if (wrapperType == WRAPPER_TYPES.serialized) {
        SerializedObjectLoader.loadProfiles(pathDataset1)
      }
      else if (wrapperType == WRAPPER_TYPES.json) {
        JSONWrapper.loadProfiles(pathDataset1, 0, realID)
      }
      else {
        CSVWrapper.loadProfiles(filePath = pathDataset1, header = true, realIDField = realID)
      }
    }
    val profilesDataset1 = dataset1.count()
    val separatorID = dataset1.map(_.id).max()

    val dataset2 = {
      if (wrapperType == WRAPPER_TYPES.serialized) {
        SerializedObjectLoader.loadProfiles(pathDataset2, separatorID + 1)
      }
      else if (wrapperType == WRAPPER_TYPES.json) {
        JSONWrapper.loadProfiles(pathDataset2, separatorID + 1, realID)
      }
      else {
        CSVWrapper.loadProfiles(filePath = pathDataset2, startIDFrom = separatorID + 1, header = true, realIDField = realID)
      }
    }
    val profilesDataset2 = dataset2.count()
    val maxProfileID = dataset2.map(_.id).max()
    val profiles = dataset1.union(dataset2)

    profiles.persist(storageLevel)
    val numProfiles = profiles.count()

    log.info("SPARKER - First dataset max ID " + separatorID)
    log.info("SPARKER - Max profiles id " + maxProfileID)
    val profilesTime = Calendar.getInstance()
    log.info("SPARKER - Number of profiles in the 1st dataset " + profilesDataset1)
    log.info("SPARKER - Number of profiles in the 2nd dataset " + profilesDataset2)
    log.info("SPARKER - Total number of profiles " + numProfiles)
    log.info("SPARKER - Time to load profiles " + msToMin(profilesTime.getTimeInMillis - startTime.getTimeInMillis) + " ms")
    log.info()
    log.info()

    log.info("SPARKER - Start to loading the groundtruth")
    val groundtruth = {
      if (wrapperGtType == WRAPPER_TYPES.serialized) {
        SerializedObjectLoader.loadGroundtruth(pathGt)
      }
      else if (wrapperGtType == WRAPPER_TYPES.json) {
        JSONWrapper.loadGroundtruth(pathGt, "id1", "id2")
      }
      else {
        CSVWrapper.loadGroundtruth(filePath = pathGt, header = true)
      }
    }
    val gtNum = groundtruth.count()


    var newGT: Set[(Long, Long)] = null

    val realIdId1 = sc.broadcast(dataset1.map { p =>
      (p.originalID, p.id)
    }.collectAsMap())
    val realIdId2 = sc.broadcast(dataset2.map { p =>
      (p.originalID, p.id)
    }.collectAsMap())
    log.info("SPARKER - Start to generate the new groundtruth")
    newGT = groundtruth.map { g =>
      val first = realIdId1.value.get(g.firstEntityID)
      val second = realIdId2.value.get(g.secondEntityID)
      if (first.isDefined && second.isDefined) {
        (first.get, second.get)
      }
      else {
        (-1L, -1L)
      }
    }.filter(_._1 >= 0).collect().toSet
    realIdId1.unpersist()
    realIdId2.unpersist()

    groundtruth.persist(storageLevel)
    val newGTSize = newGT.size
    log.info("SPARKER - Generation completed")
    log.info("SPARKER - Number of elements in the new groundtruth " + newGTSize)
    groundtruth.unpersist()
    val gtTime = Calendar.getInstance()
    log.info("SPARKER - Time to generate the new groundtruth " + msToMin(gtTime.getTimeInMillis - profilesTime.getTimeInMillis) + " ms")
    log.info()

    log.info("SPARKER - Start to generating blocks")
    val blocks = {
      if (blockingType == BLOCKING_TYPES.inferSchema) {
        log.info("SPARKER - Start to generating clusters")
        log.info("SPARKER - Number of hashes " + hashNum)
        log.info("SPARKER - Target threshold " + clusterThreshold)

        val clusters ={
            LSHLuca.clusterSimilarAttributes2(
              profiles = profiles,
              numHashes = hashNum,
              targetThreshold = clusterThreshold,
              maxFactor = clusterMaxFactor,
              separatorID = separatorID,
              separateAttributes = clusterSeparateAttributes
            )
        }

        log.info("SPARKER - Number of clusters " + clusters.size)
        log.info("SPARKER - Generated clusters")
        clusters.foreach(log.info)
        val clusterTime = Calendar.getInstance()
        log.info("SPARKER - Time to generate clusters " + msToMin(clusterTime.getTimeInMillis - gtTime.getTimeInMillis) + " min")
        log.info()


        log.info("SPARKER - Start to generating blocks")

        TokenBlocking.createBlocksCluster(profiles, separatorID, clusters)

      }
      else {
        TokenBlocking.createBlocks(profiles, separatorID)
      }
    }

    blocks.persist(storageLevel)
    val numBlocks = blocks.count()
    profiles.unpersist()
    dataset1.unpersist()
    if (dataset2 != null) {
      dataset2.unpersist()
    }
    val blocksTime = Calendar.getInstance()
    log.info("SPARKER - Number of blocks " + numBlocks)
    log.info("SPARKER - Time to generate blocks " + msToMin(blocksTime.getTimeInMillis - gtTime.getTimeInMillis) + " ms")
    log.info()


    log.info("SPARKER - Start to block purging, smooth factor " + purgingRatio)
    val blocksPurged = BlockPurging.blockPurging(blocks, purgingRatio)
    val numPurgedBlocks = blocksPurged.count()
    blocks.unpersist()
    val blocksPurgingTime = Calendar.getInstance()
    log.info("SPARKER - Number of blocks after purging " + numPurgedBlocks)
    log.info("SPARKER - Time to purging blocks " + msToMin(blocksPurgingTime.getTimeInMillis - blocksTime.getTimeInMillis) + " ms")
    log.info()


    log.info("SPARKER - Start to block filtering, factor " + filteringRatio)

    val profileBlocks = Converters.blocksToProfileBlocks(blocksPurged)
    val profileBlocksFiltered = BlockFiltering.blockFiltering(profileBlocks, filteringRatio)
    profileBlocksFiltered.persist(storageLevel)
    val blocksAfterFiltering = Converters.profilesBlockToBlocks(profileBlocksFiltered, separatorID)
    blocksAfterFiltering.persist(storageLevel)
    val numFilteredBlocks = blocksAfterFiltering.count()
    blocksPurged.unpersist()
    val blocksFilteringTime = Calendar.getInstance()
    log.info("SPARKER - Number of blocks after filtering " + numFilteredBlocks)
    log.info("SPARKER - Time to filtering blocks " + msToMin(blocksFilteringTime.getTimeInMillis - blocksPurgingTime.getTimeInMillis) + " ms")
    log.info()


    log.info("SPARKER - Start to pruning edges")
    val blockIndexMap = blocksAfterFiltering.map(b => (b.blockID, b.profiles)).collectAsMap()
    val blockIndex = sc.broadcast(blockIndexMap)
    log.info("SPARKER - Size of the broadcast blockIndex " + SizeEstimator.estimate(blockIndexMap) + " byte")
    val gt = sc.broadcast(newGT)
    log.info("SPARKER - Size of the broadcast groundtruth " + SizeEstimator.estimate(newGT) + " byte")
    val profileBlocksSizeIndex: Broadcast[scala.collection.Map[Long, Int]] = sc.broadcast(profileBlocksFiltered.map(pb => (pb.profileID, pb.blocks.size)).collectAsMap())


    val blocksEntropiesMap: Broadcast[scala.collection.Map[Long, Double]] = {
      if (useEntropy) {
        val blocksEntropies = blocks.map(b => (b.blockID, b.entropy)).collectAsMap()
        log.info("SPARKER - Size of the broadcast blocksEntropiesMap " + SizeEstimator.estimate(blocksEntropies) + " byte")
        sc.broadcast(blocksEntropies)
      }
      else {
        null
      }
    }

    var startPruningTime: Calendar = null
    var endPruningTime: Calendar = null

    val methods = {
      if(blast){
        List(PruningUtils.ComparisonTypes.OR)
      }
      else{
        List(PruningUtils.ComparisonTypes.AND, PruningUtils.ComparisonTypes.OR)
      }
    }

    val endPreprocessing = Calendar.getInstance()

    log.info("SPARKER - Total preprocessing time "+msToMin(endPreprocessing.getTimeInMillis-startTime.getTimeInMillis)+" min")

    methods.foreach { method =>
      val resultsWNP = for (i <- 0 to weightTypes.size - 1) yield {
        startPruningTime = Calendar.getInstance()

        val edgesAndCount = WNPFor.WNP(
          profileBlocksFiltered,
          blockIndex,
          maxProfileID.toInt,
          separatorID,
          gt,
          thresholdType,
          weightTypes(i),
          profileBlocksSizeIndex,
          useEntropy,
          blocksEntropiesMap,
          blastDiv,
          method
        )

        edgesAndCount.persist(storageLevel)
        val numEdges = edgesAndCount.map(_._1).sum()
        val edges = edgesAndCount.flatMap(_._2).distinct()
        edges.persist(storageLevel)
        val perfectMatch = edges.count()
        endPruningTime = Calendar.getInstance()

        log.info("SPARKER - WNP " + method + " "+weightTypes(i))
        log.info("SPARKER - Number of retained edges " + numEdges)
        log.info("SPARKER - Number of perfect match found " + perfectMatch)
        log.info("SPARKER - Number of elements in the gt " + newGTSize)
        log.info("SPARKER - PC = " + (perfectMatch.toFloat / newGTSize.toFloat))
        log.info("SPARKER - PQ = " + (perfectMatch.toFloat / numEdges.toFloat))
        log.info()
        log.info("SPARKER - Time to pruning edges " + msToMin(endPruningTime.getTimeInMillis - startPruningTime.getTimeInMillis) + " ms")
        log.info()

        edgesAndCount.unpersist()

        "SPARKER - WNP, type = " + method + ", blockingType = " + blockingType + ", thresholdType = " + thresholdType + ", weightType = " + weightTypes(i) + ", PC = " + (perfectMatch.toFloat / newGTSize.toFloat) + ", PQ = " + (perfectMatch.toFloat / numEdges.toFloat) + ", Retained edges " + numEdges + ", Pruning execution time " + msToMin(endPruningTime.getTimeInMillis - startPruningTime.getTimeInMillis)
      }


      val resultsCNP = if(!blast) {
        for (i <- 0 to weightTypes.size - 1) yield {
          startPruningTime = Calendar.getInstance()

          val edgesAndCount = CNPFor.CNP(
            blocks,
            numProfiles,
            profileBlocksFiltered,
            blockIndex,
            maxProfileID.toInt,
            separatorID,
            gt,
            thresholdType,
            weightTypes(i),
            profileBlocksSizeIndex,
            useEntropy,
            blocksEntropiesMap,
            method
          )

          edgesAndCount.persist(storageLevel)
          val numEdges = edgesAndCount.map(_._1).sum()
          val edges = edgesAndCount.flatMap(_._2).distinct()
          edges.persist(storageLevel)
          val perfectMatch = edges.count()
          endPruningTime = Calendar.getInstance()

          log.info("SPARKER - CNP " + method + " "+weightTypes(i))
          log.info("SPARKER - Number of retained edges " + numEdges)
          log.info("SPARKER - Number of perfect match found " + perfectMatch)
          log.info("SPARKER - Number of elements in the gt " + newGTSize)
          log.info("SPARKER - PC = " + (perfectMatch.toFloat / newGTSize.toFloat))
          log.info("SPARKER - PQ = " + (perfectMatch.toFloat / numEdges.toFloat))
          log.info()
          log.info("SPARKER - Time to pruning edges " + msToMin(endPruningTime.getTimeInMillis - startPruningTime.getTimeInMillis) + " ms")
          log.info()

          edgesAndCount.unpersist()

          "SPARKER - CNP, type = " + method + ",  blockingType = " + blockingType + ", thresholdType = " + thresholdType + ", weightType = " + weightTypes(i) + ", PC = " + (perfectMatch.toFloat / newGTSize.toFloat) + ", PQ = " + (perfectMatch.toFloat / numEdges.toFloat) + ", Retained edges " + numEdges + ", Pruning execution time " + msToMin(endPruningTime.getTimeInMillis - startPruningTime.getTimeInMillis)
        }
      }
      else{
        List()
      }

      resultsWNP.foreach(log.info)
      resultsCNP.foreach(log.info)
    }

    log.info("SPARKER - Total execution time " + msToMin(endPruningTime.getTimeInMillis - startTime.getTimeInMillis))

    log.removeAppender(appender)
    appender.close()
  }
}