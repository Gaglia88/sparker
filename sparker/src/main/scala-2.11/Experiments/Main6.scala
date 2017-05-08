package Experiments

import java.util.Calendar

import BlockBuildingMethods.{NGrams, TokenBlocking}
import BlockRefinementMethods.{BlockFiltering, BlockPurging}
import BlockRefinementMethods.PruningMethods.WNPFor
import DataStructures.UnweightedEdge
import Utilities.Converters
import Wrappers.{CSVWrapper, SerializedObjectLoader}
import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Luca on 06/02/2017.
  */
object Main6 {
  def main(args: Array[String]) {
    val purgingRatio = 1.000
    val filteringRatio = 0.6
    val ngramSize = 4

    val memoryHeap = args(0)
    val memoryStack = args(1)
    val pathDataset1 = args(2)
    val pathDataset2 = args(3)
    val pathGt = args(4)



    /*val memoryHeap = 15
    val memoryStack = 5
    val pathDataset1 = "C:/Users/Luca/Desktop/UNI/BlockingFramework/datasets/movies/profiles/dataset1"
    val pathDataset2 = "C:/Users/Luca/Desktop/UNI/BlockingFramework/datasets/movies/profiles/dataset2"
    val pathGt = "C:/Users/Luca/Desktop/UNI/BlockingFramework/datasets/movies/groundtruth"*/


    println("Heap "+memoryHeap+"g")
    println("Stack "+memoryStack+"g")
    println("First dataset path "+pathDataset1)
    println("Second dataset path "+pathDataset2)
    println("Groundtruth path "+pathGt)
    println()


    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.executor.memory", memoryHeap+"g")
      .set("spark.network.timeout", "10000s")
      .set("spark.executor.heartbeatInterval", "40s")
	    .set("spark.default.parallelism", "32")
      .set("spark.driver.maxResultSize", "10g")
      .set("spark.executor.extraJavaOptions", "-Xss"+memoryStack+"g")
      .set("spark.local.dir", "/data2/sparkTmp/")

    val sc = new SparkContext(conf)

    println("Start to loading profiles")
    val startTime = Calendar.getInstance();
    val dataset1 = CSVWrapper.loadProfiles(filePath = pathDataset1, header = true, realIDField = "id")//SerializedObjectLoader.loadProfiles(filePath = pathDataset1, realFieldID = "id")
    val separatorID = dataset1.map(_.id).max()
    val dataset2 = CSVWrapper.loadProfiles(filePath = pathDataset2, startIDFrom = separatorID+1, header = true, realIDField = "id")//SerializedObjectLoader.loadProfiles(filePath = pathDataset2, realFieldID = "id", startIDFrom = separatorID+1)
    val maxProfileID = dataset2.map(_.id).max()

    val profiles = dataset1.union(dataset2)
    profiles.cache()
    val numProfiles = profiles.count()

    println("First dataset max ID "+separatorID)
    println("Max profiles id "+maxProfileID)
    val profilesTime = Calendar.getInstance()
    println("Number of profiles "+numProfiles)
    println("Time to load profiles "+(profilesTime.getTimeInMillis-startTime.getTimeInMillis)+" ms")
    println()

    println("Start to loading groundtruth")
    val groundtruth = CSVWrapper.loadGroundtruth(filePath = pathGt, header = true)//SerializedObjectLoader.loadGroundtruth(pathGt)
    val gtNum = groundtruth.count()
    val realIdId1 = sc.broadcast(dataset1.map(p => (p.originalID, p.id)).collectAsMap())
    val realIdId2 = sc.broadcast(dataset2.map(p => (p.originalID, p.id)).collectAsMap())
    println("Start to generate the new groundtruth")
    val newGT = groundtruth.map(g => (realIdId1.value(g.firstEntityID), realIdId2.value(g.secondEntityID))).collectAsMap()
    realIdId1.unpersist()
    realIdId2.unpersist()
    groundtruth.cache()
    println("Generation completed")
    groundtruth.unpersist()
    val gtTime = Calendar.getInstance()
    println("Time to generate the new groundtruth "+(gtTime.getTimeInMillis-profilesTime.getTimeInMillis)+" ms")
    println()

    println("Start to generating blocks")
    val blocks = NGrams.createBlocks(profiles, ngramSize, separatorID)//TokenBlocking.createBlocks(profiles, separatorID)
    blocks.cache()
    val numBlocks = blocks.count()
    profiles.unpersist()
    dataset1.unpersist()
    dataset2.unpersist()
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
    val profileBlocksFiltered = BlockFiltering.blockFiltering2(profileBlocks, filteringRatio)
    profileBlocksFiltered.cache()
    val blocksAfterFiltering = Converters.profilesBlockToBlocks(profileBlocksFiltered, separatorID)
    blocksAfterFiltering.cache()
    val numFilteredBlocks = blocksAfterFiltering.count()
    blocksPurged.unpersist()
    val blocksFilteringTime = Calendar.getInstance()
    println("Number of blocks after filtering "+numFilteredBlocks)
    println("Time to filtering blocks "+(blocksFilteringTime.getTimeInMillis-blocksPurgingTime.getTimeInMillis)+" ms")
    println()

    val gt = sc.broadcast(newGT)

    val edges = blocksAfterFiltering.flatMap(_.getComparisons())
    edges.cache()
    val numEdges = edges.count()

    val found = edges.mapPartitions{
      partition =>
        val arrayFound = Array.fill[Boolean](maxProfileID.toInt+1){false}
        partition.map{
          edge =>
            if(!arrayFound(edge.firstProfileID.toInt)){
              val m = gt.value.get(edge.firstProfileID)
              if(m.isDefined && m.get == edge.secondProfileID){
                arrayFound.update(edge.firstProfileID.toInt, true)
                arrayFound.update(edge.secondProfileID.toInt, true)
                edge
              }
              else{
                null
              }
            }
            else{
              null
            }
        }
    }.filter(_ != null).distinct().count()

    println("FOUND "+found)

    println("PC = "+(found.toFloat/gtNum.toFloat)+", PQ = "+(found.toFloat/numEdges.toFloat))

    val endTime = Calendar.getInstance()
    println("Total time "+(endTime.getTimeInMillis-startTime.getTimeInMillis)+" ms")

    /*
    println("Start to pruning edges")
    val blockIndexMap = blocks.map(b => (b.blockID, b.profiles)).collectAsMap()
    println("Size of blockIndex "+SizeEstimator.estimate(blockIndexMap)+" byte")
    val blockIndex = sc.broadcast(blockIndexMap)
    val gt = sc.broadcast(newGT)
    val edgesAndCount = WNPCBSFor.CalcPCPQ(profileBlocksFiltered, blockIndex, maxProfileID.toInt, separatorID, gt)
    edgesAndCount.cache()
    val numEdges = edgesAndCount.map(_._1).sum()
    val perfectMatch = edgesAndCount.map(_._2).filter(_ != null).distinct().count()
    val pruningTime = Calendar.getInstance()
    blocks.unpersist()
    blockIndex.unpersist()
    profileBlocksFiltered.unpersist()
    println("Number of retained edges "+numEdges)
    println("Number of perfect match found "+perfectMatch)
    println("Number of elements in the gt "+gtNum)
    println("PC = "+perfectMatch.toFloat/gtNum.toFloat)
    println("PQ = "+(perfectMatch.toFloat/numEdges.toFloat))
    println()
    println("Time to pruning edges "+(pruningTime.getTimeInMillis-gtTime.getTimeInMillis)+" ms")
    println()
    println("Total execution time "+(pruningTime.getTimeInMillis-startTime.getTimeInMillis))*/

    sc.stop()
  }
}
