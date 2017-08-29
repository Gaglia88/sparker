package Experiments

import java.util.Calendar

import BlockBuildingMethods.LSHTwitter.Settings
import BlockBuildingMethods._
import BlockRefinementMethods.PruningMethods._
import BlockRefinementMethods.{BlockFiltering, BlockPurging}
import DataStructures._
import Utilities.Converters
import Wrappers.{CSVWrapper, JSONWrapper, SerializedObjectLoader}
import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Luca on 13/03/2017.
  */
object Main {
  object WRAPPER_TYPES {
    val serialized = "serialized"
    val csv = "csv"
    val json = "json"
  }

  object BLOCKING_TYPES {
    val inferSchema = "inferSchema"
    val schemaLess = "schemaLess"
    val manualMapping = "manualMapping"
  }

  object DATASETS_CLEAN {
    val abtBuy = "clean/abtBuy"
    val googleAmazon = "clean/googleAmazon"
    val scholarDblp = "clean/scholarDblp"
    val movies = "clean/movies"
    val dblpAcm = "clean/DblpAcm"
  }

  object DATASETS_DIRTY {
    val dblpAcm = "dirty/DblpAcm"
    val restaurant = "dirty/restaurant"
    val songs = "dirty/songs"
  }

  def main(args: Array[String]) {

    val baseBath = "C:/Users/Luca/Desktop/datasets/"+DATASETS_CLEAN.movies
    val clean = true


    val pathDataset1 = baseBath+"/dataset1"
    val pathDataset2 : String = {
      if (clean) {
        baseBath + "/dataset2"
      }
      else{
        null
      }
    }
    val pathGt = baseBath+"/groundtruth"
    val defaultLogPath = "C:/Users/Luca/Desktop/"
    val wrapperType = WRAPPER_TYPES.serialized
    val GTWrapperType = WRAPPER_TYPES.serialized
    val purgingRatio = 1.005
    val filteringRatio = 0.8
    val hashNum = 16
    val clusterThreshold = 0.3
    val memoryHeap = 10
    val memoryStack = 5
    val realId = "id"
    val clusterSeparateAttributes = true
    val clusterMaxFactor = 1
    val useEntropy = true
    val blockingType = BLOCKING_TYPES.schemaLess
    val thresholdType = PruningUtils.ThresholdTypes.AVG
    val logName = "WNP_CHI_SQUARE_MAXDIV2_SCHEMA_ENTROPY_2.txt"

    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.executor.memory", memoryHeap+"g")
      .set("spark.network.timeout", "10000s")
      .set("spark.executor.heartbeatInterval", "40s")
      //.set("spark.default.parallelism", "32")
      .set("spark.executor.extraJavaOptions", "-Xss"+memoryStack+"g -XX:+UseConcMarkSweepGC")
      .set("spark.local.dir", "/data2/sparkTmp/")
      .set("spark.driver.maxResultSize", "10g")

    val sc = new SparkContext(conf)
    val weightTypes = List(PruningUtils.WeightTypes.JS, PruningUtils.WeightTypes.CBS, PruningUtils.WeightTypes.ARCS, PruningUtils.WeightTypes.chiSquare, PruningUtils.WeightTypes.EJS, PruningUtils.WeightTypes.ECBS)
    //val weightTypes = List(PruningUtils.WeightTypes.chiSquare)

    Main.runClean(
      sc,
      defaultLogPath + logName,
      wrapperType,
      purgingRatio,
      filteringRatio,
      blockingType,
      thresholdType,
      hashNum,
      clusterThreshold,
      useEntropy = useEntropy,
      memoryHeap,
      memoryStack,
      pathDataset1,
      pathDataset2,
      pathGt,
      GTWrapperType,
      weightTypes,
      realId,
      clusterSeparateAttributes,
      clusterMaxFactor
    )

    sc.stop()
  }

  def runClean(sc : SparkContext, logPath : String, wrapperType : String, purgingRatio : Double, filteringRatio : Double, blockingType : String,
               thresholdType : String, hashNum : Int, clusterThreshold : Double,
               useEntropy : Boolean, memoryHeap : Int, memoryStack : Int, pathDataset1 : String, pathDataset2 : String,
               pathGt : String, wrapperGtType : String, weightTypes : List[String], realID : String = "",
               clusterSeparateAttributes : Boolean = true, clusterMaxFactor : Double = 0.9,
               manualClusters : List[KeysCluster] = Nil, storageLevel : StorageLevel = StorageLevel.MEMORY_AND_DISK): Unit = {


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
    log.info()

    log.info("SPARKER - Start to loading the profiles")
    val startTime = Calendar.getInstance();
    val dataset1 = {
      if(wrapperType == WRAPPER_TYPES.serialized){
        SerializedObjectLoader.loadProfiles(pathDataset1)
      }
      else if(wrapperType == WRAPPER_TYPES.json){
        JSONWrapper.loadProfiles(pathDataset1, 0, realID)
      }
      else{
        CSVWrapper.loadProfiles(filePath = pathDataset1, header = true, realIDField = realID)
      }
    }
    val profilesDataset1 = dataset1.count()

    var separatorID : Long = -1
    var profiles : RDD[Profile] = null
    var maxProfileID : Long = 0
    var profilesDataset2 : Long = 0
    var dataset2 : RDD[Profile] = null

    if(pathDataset2 != null){
      separatorID = dataset1.map(_.id).max()
      dataset2 = {
        if (wrapperType == WRAPPER_TYPES.serialized) {
          SerializedObjectLoader.loadProfiles(pathDataset2, separatorID + 1)
        }
        else if(wrapperType == WRAPPER_TYPES.json){
          JSONWrapper.loadProfiles(pathDataset2, separatorID + 1, realID)
        }
        else {
          CSVWrapper.loadProfiles(filePath = pathDataset2, startIDFrom = separatorID + 1, header = true, realIDField = realID)
        }
      }
      profilesDataset2 = dataset2.count()

      maxProfileID = dataset2.map(_.id).max()
      profiles = dataset1.union(dataset2)
    }
    else{
      profiles = dataset1
      maxProfileID = dataset1.map(_.id).max()
    }
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


    /*val na1 = dataset1.flatMap{profile =>
      profile.attributes.map(kv => kv.key)
    }.distinct().count()

    val na2 = dataset2.flatMap{profile =>
      profile.attributes.map(kv => kv.key)
    }.distinct().count()

    log.info("SPARKER - Attributes d1 "+na1)
    log.info("SPARKER - Attributes d2 "+na2)*/


    log.info("SPARKER - Start to loading the groundtruth")
    val groundtruth = {
      if(wrapperGtType == WRAPPER_TYPES.serialized){
        SerializedObjectLoader.loadGroundtruth(pathGt)
      }
      else if(wrapperGtType == WRAPPER_TYPES.json){
        JSONWrapper.loadGroundtruth(pathGt, "id1", "id2")
      }
      else{
        CSVWrapper.loadGroundtruth(filePath = pathGt, header = true)
      }
    }
    val gtNum = groundtruth.count()


    var newGT : Set[(Long, Long)] = null

    if(dataset2 == null){
      val realIds = sc.broadcast(profiles.map(p => (p.originalID, p.id)).collectAsMap())

      log.info("SPARKER - Start to generate the new groundtruth")
      newGT = groundtruth.map{
        g =>
          val first = realIds.value(g.firstEntityID)
          val second = realIds.value(g.secondEntityID)
          if(first < second){
            (first, second)
          }
          else{
            (second, first)
          }
      }.filter(x => x._1 != x._2).collect().toSet
      realIds.unpersist()
    }
    else{
      val realIdId1 = sc.broadcast(dataset1.map{p =>
        (p.originalID, p.id)
      }.collectAsMap())
      val realIdId2 = sc.broadcast(dataset2.map{p =>
        (p.originalID, p.id)
      }.collectAsMap())
      log.info("SPARKER - Start to generate the new groundtruth")
      newGT = groundtruth.map{g =>
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
    }

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


      /*val similarProfilesBlocks = LSHTwitter.createBlocks(profiles, 128, 0.8, -1, separatorID)
      val similarProfilesNum = similarProfilesBlocks.count()
      log.info("Numero blocchi "+similarProfilesNum)

      val similarIds = sc.broadcast(similarProfilesBlocks.flatMap(_.getAllProfiles).collect().toSet)
      val similarProfiles = profiles.filter(p => similarIds.value.contains(p.id))
      similarIds.unpersist()*/

      /*
      val a = LSHTwitter2.createBlocks(profiles, 16, 0.9, -1, separatorID)
      val b = a.count()
      log.info("Numero blocchi "+b)

      val c = sc.broadcast(a.flatMap(_.getAllProfiles).collect().toSet)
      val profilesMap = profiles.filter(p => c.value.contains(p.id)).map(p => (p.id, p.attributes)).collectAsMap()
      c.unpersist()
      val d = a.collect()
      val e = d.flatMap{block =>
        block.getComparisons().flatMap{edge =>
          val a1 = profilesMap(edge.firstProfileID)
          val a2 = profilesMap(edge.secondProfileID)

          val t1 = a1.flatMap{kq =>
              val keys = kq.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).filter(_.trim.size > 0).distinct
              keys.map((_, LSHTwitter2.Settings.FIRST_DATASET_PREFIX+kq.key))
          }
          val t2 = a2.flatMap{kq =>
              val keys = kq.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).filter(_.trim.size > 0).distinct
              keys.map((_, LSHTwitter2.Settings.SECOND_DATASET_PREFIX+kq.key))
          }
          val t3 = t1.union(t2)
          t3.groupBy(_._1).filter(_._2.size > 1).map(x => x._2.map(_._2).toSet).filter(_.size > 1).toSet
        }
      }.toSet

      e.foreach(println)*/

      clusters = //LSHSpark.clusterSimilarAttributes(profiles, hashNum, clusterThreshold, separatorID = separatorID)
        //LSHTwitter2.clusterSimilarAttributes(profiles, hashNum, clusterThreshold, separatorID = separatorID)
      LSHTwitter.clusterSimilarAttributes(
        profiles = profiles,//similarProfiles,
        numHashes = hashNum,
        targetThreshold = clusterThreshold,
        maxFactor = clusterMaxFactor,
        separatorID = separatorID,
        separateAttributes = clusterSeparateAttributes
      )
      log.info("SPARKER - Number of clusters "+clusters.size)
      log.info("SPARKER - Generated clusters")
      clusters.foreach(log.info)
      clusterTime = Calendar.getInstance()
      log.info("SPARKER - Time to generate clusters "+(clusterTime.getTimeInMillis-gtTime.getTimeInMillis)+" ms")
      log.info()
    }
    else if(blockingType == BLOCKING_TYPES.manualMapping){

      if(manualClusters.filter(x => x.keys.contains(Settings.DEFAULT_CLUSTER_NAME)).isEmpty){
        clusters = KeysCluster(manualClusters.map(_.id).max+1, List(Settings.DEFAULT_CLUSTER_NAME)) :: manualClusters
      }
      else{
        clusters = manualClusters
      }
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
    if(dataset2 != null){
      dataset2.unpersist()
    }
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

    /*val profileBlocks1 = profileBlocks.filter(_.profileID <= separatorID)
    val profileBlocks2 = profileBlocks.filter(_.profileID > separatorID)

    val profileBlocksFiltered1 = BlockFiltering.blockFiltering(profileBlocks1, 0.5)
    val profileBlocksFiltered2 = BlockFiltering.blockFiltering(profileBlocks2, 0.7)
    val profileBlocksFiltered = profileBlocksFiltered1.union(profileBlocksFiltered2)*/
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

    /*val edgesAndCount = WNPFor.CalcPCPQ(profileBlocksFiltered, blockIndex, maxProfileID.toInt, separatorID, gt)

    val numEdges = edgesAndCount.map(_._1).sum()
    val edges = edgesAndCount.flatMap(_._2).distinct()
    edges.persist(storageLevel)
    val perfectMatch = edges.count()

    log.info("SPARKER - Number of retained edges " + numEdges)
    log.info("SPARKER - Number of perfect match found " + perfectMatch)
    log.info("SPARKER - Number of elements in the gt " + newGTSize)
    log.info("SPARKER - PC = " + (perfectMatch.toFloat / newGTSize.toFloat))
    log.info("SPARKER - PQ = " + (perfectMatch.toFloat / numEdges.toFloat))
    log.info()
    log.info()*/


    val profileBlocksSizeIndex : Broadcast[scala.collection.Map[Long, Int]] = {
      if(weightTypes.contains(PruningUtils.WeightTypes.chiSquare) || weightTypes.contains(PruningUtils.WeightTypes.JS) || weightTypes.contains(PruningUtils.WeightTypes.EJS)){
        val profileBlocksMap = profileBlocksFiltered.map(pb => (pb.profileID, pb.blocks.size)).collectAsMap()
        log.info("SPARKER - Size of the broadcast profileBlocksMap "+SizeEstimator.estimate(profileBlocksMap)+" byte")
        sc.broadcast(profileBlocksMap)
      }
      else{
        null
      }
    }
    log.info("SPARKER - profileBlocksSizeIndex broadcast done (if needed)")

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

    var startPruningTime : Calendar = null
    var endPruningTime : Calendar = null

    val results = for(i <- 0 to weightTypes.size-1) yield {
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
        2.0,
        PruningUtils.ComparisonTypes.AND
      )

      edgesAndCount.persist(storageLevel)
      val numEdges = edgesAndCount.map(_._1).sum()
      val edges = edgesAndCount.flatMap(_._2).distinct()
      edges.persist(storageLevel)
      val perfectMatch = edges.count()
      endPruningTime = Calendar.getInstance()

      log.info("SPARKER - Number of retained edges " + numEdges)
      log.info("SPARKER - Number of perfect match found " + perfectMatch)
      log.info("SPARKER - Number of elements in the gt " + newGTSize)
      log.info("SPARKER - PC = " + (perfectMatch.toFloat / newGTSize.toFloat))
      log.info("SPARKER - PQ = " + (perfectMatch.toFloat / numEdges.toFloat))
      log.info()
      log.info("SPARKER - Time to pruning edges " + (endPruningTime.getTimeInMillis - startPruningTime.getTimeInMillis) + " ms")
      log.info()

      "SPARKER - blockingType = "+blockingType+", thresholdType = "+thresholdType+", weightType = "+weightTypes(i)+", useEntropy = "+useEntropy+", PC = "+(perfectMatch.toFloat/newGTSize.toFloat)+", PQ = "+(perfectMatch.toFloat/numEdges.toFloat)+", Pruning execution time "+(endPruningTime.getTimeInMillis-startPruningTime.getTimeInMillis)
    }
    log.info("SPARKER - Total execution time "+(endPruningTime.getTimeInMillis-startTime.getTimeInMillis))

    results.foreach(log.info)
  }
}
