package Experiments

import java.util.Calendar

import BlockBuildingMethods.{LSHTwitterOld, TokenBlocking}
import BlockRefinementMethods.{BlockFiltering, BlockPurging}
import BlockRefinementMethods.PruningMethods.WNPFor
import DataStructures.{KeysCluster, MatchingEntities}
import Utilities.Converters
import Wrappers.CSVWrapper
import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by Luca on 25/02/2017.
  */
object MainDirty {
  def main(args: Array[String]) {
    val purgingRatio = 1.015
    val filteringRatio = 0.8
    val thresholdType = WNPFor.ThresholdTypes.AVG


    /*val memoryHeap = args(0)
    val memoryStack = args(1)
    val pathDataset = args(2)
    val pathGt = args(3)
*/

    val memoryHeap = 15
    val memoryStack = 5
    val pathDataset = "C:/Users/Luca//Downloads/songs/msd.csv"
    val pathGt = "C:/Users/Luca//Downloads/songs/matches_msd_msd.csv"


    println("Heap "+memoryHeap+"g")
    println("Stack "+memoryStack+"g")
    println("Dataset path "+pathDataset)
    println("Groundtruth path "+pathGt)
    println("Threshold type "+thresholdType)
    println()


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


    val startTime = Calendar.getInstance();

    println("Start to loading profiles")
    val profiles = CSVWrapper.loadProfiles(filePath = pathDataset, header = true, realIDField = "id")
    profiles.cache()
    val separatorID = -1
    val maxProfileID = profiles.map(_.id).max()
    val numProfiles = profiles.count()
    val profilesTime = Calendar.getInstance()

    println("Max profiles id "+maxProfileID)
    println("Number of profiles "+numProfiles)
    println("Time to load profiles "+(profilesTime.getTimeInMillis-startTime.getTimeInMillis)+" ms")
    println()


    println("Start to loading groundtruth")
    val groundtruth = CSVWrapper.loadGroundtruth(filePath = pathGt, header = true)
    val realIdIds = sc.broadcast(profiles.map(p => (p.originalID, p.id)).collectAsMap())
    println("Start to generate the new groundtruth")
    val newGT = groundtruth.map{
      g =>
        val first = realIdIds.value(g.firstEntityID)
        val second = realIdIds.value(g.secondEntityID)
        if(first < second){
          (first, second)
        }
        else{
          (second, first)
        }
    }.distinct().collect().toSet

    val gtNum = newGT.size

    realIdIds.unpersist()
    groundtruth.unpersist()
    println("Generation completed")
    val gtTime = Calendar.getInstance()
    println("Time to generate the new groundtruth "+(gtTime.getTimeInMillis-profilesTime.getTimeInMillis)+" ms")
    println()

    println("Start to generating blocks")
    //val blocks = TokenBlocking.createBlocksUsingSchema(profiles, separatorID, "title,release,artist_name,duration,artist_familiarity,artist_hotttnesss,year".split(",").toList)
    val blocks = TokenBlocking.createBlocks(profiles, separatorID)
    blocks.cache()
    val numBlocks = blocks.count()
    profiles.unpersist()
    val blocksTime = Calendar.getInstance()
    println("Number of blocks "+numBlocks)
    println("Time to generate blocks "+(blocksTime.getTimeInMillis-gtTime.getTimeInMillis)+" ms")
    println()

    println("Start to block purging, smooth factor "+purgingRatio)
    val blocksPurged = BlockPurging.blockPurging(blocks, purgingRatio)
    val numPurgedBlocks = blocksPurged.count()
    blocks.unpersist()
    val blocksPurgingTime = Calendar.getInstance()
    println("Number of blocks after purging "+numPurgedBlocks)
    println("Time to purging blocks "+(blocksPurgingTime.getTimeInMillis-blocksTime.getTimeInMillis)+" ms")
    println()


    println("Start to block filtering, factor "+filteringRatio)
    val profileBlocks = Converters.blocksToProfileBlocks(blocksPurged)
    val profileBlocksFiltered = BlockFiltering.blockFiltering(profileBlocks, filteringRatio)
    profileBlocksFiltered.cache()
    val blocksAfterFiltering = Converters.profilesBlockToBlocks(profileBlocksFiltered, separatorID)
    blocksAfterFiltering.cache()
    val numFilteredBlocks = blocksAfterFiltering.count()
    blocksPurged.unpersist()
    val blocksFilteringTime = Calendar.getInstance()
    println("Number of blocks after filtering "+numFilteredBlocks)
    println("Time to filtering blocks "+(blocksFilteringTime.getTimeInMillis-blocksPurgingTime.getTimeInMillis)+" ms")
    println()


    println("Start to pruning edges")
    val blockIndexMap = blocksAfterFiltering.map(b => (b.blockID, b.profiles)).collectAsMap()
    println("Size of blockIndex "+SizeEstimator.estimate(blockIndexMap)+" byte")
    val blockIndex = sc.broadcast(blockIndexMap)
    val gt = sc.broadcast(newGT)

    val profileBlocksIndex = sc.broadcast(
      profileBlocksFiltered.map(pb => (pb.profileID, pb.blocks.size)).collectAsMap()
    )

    //val edgesAndCount = WNPFor.WNPJS(profileBlocksFiltered, blockIndex, profileBlocksIndex, maxProfileID.toInt, separatorID, gt, thresholdType)
    val edgesAndCount = WNPFor.WNPCBS(profileBlocksFiltered, blockIndex, maxProfileID.toInt, separatorID, gt, thresholdType)
    //val edgesAndCount = WNPFor.CalcPCPQ(profileBlocksFiltered, blockIndex, maxProfileID.toInt, separatorID, gt)
    edgesAndCount.cache()
    val numEdges = edgesAndCount.map(_._1).sum()
    val edges = edgesAndCount.flatMap(_._2).distinct()
    edges.cache()
    val perfectMatch = edges.count()
    val pruningTime = Calendar.getInstance()
    blocksAfterFiltering.unpersist()
    blockIndex.unpersist()
    profileBlocksFiltered.unpersist()

    println("Number of retained edges "+numEdges)
    println("Number of perfect match found "+perfectMatch)
    println("Number of elements in the gt "+gtNum)
    println("PC = "+(perfectMatch.toFloat/gtNum.toFloat))
    println("PQ = "+(perfectMatch.toFloat/numEdges.toFloat))
    println()
    println("Time to pruning edges "+(pruningTime.getTimeInMillis-gtTime.getTimeInMillis)+" ms")
    println()
    println("Total execution time "+(pruningTime.getTimeInMillis-startTime.getTimeInMillis))
    sc.stop()
  }
}
