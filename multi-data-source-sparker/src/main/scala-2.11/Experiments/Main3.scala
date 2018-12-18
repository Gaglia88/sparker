package Experiments

import BlockBuildingMethods.{LSHMio, TokenBlocking}
import BlockBuildingMethods.LSHMio.Settings
import BlockRefinementMethods.PruningMethods.{PCPQBlockCalc, PruningUtils, WNPFor}
import BlockRefinementMethods.{BlockFiltering, BlockPurging}
import DataStructures.Profile
import Utilities.Converters
import Wrappers.{CSVWrapper, JSONWrapper}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Test BLAST meta-blocking with multiple datasources
  *
  * @author Luca Gagliardelli
  * @since 18/12/2018
  **/
object Main3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "4")

    val sc = new SparkContext(conf)

    val path = "multidataset2/"


    val groundtruth = JSONWrapper.loadGroundtruth(path + "groundtruth.json", "id1", "id2")

    val fields = List("Name", "Cast", "Director")

    val dataset1 = JSONWrapper.loadProfiles(path + "amazon.json", sourceId = 1, realIDField = "realProfileID", fieldsToKeep = fields)
    val maxIdDataset1 = dataset1.map(_.id).max()

    val dataset2 = JSONWrapper.loadProfiles(path + "imdb.json", startIDFrom = maxIdDataset1 + 1, sourceId = 2, realIDField = "realProfileID", fieldsToKeep = fields)
    val maxIdDataset2 = dataset2.map(_.id).max()

    val dataset3 = JSONWrapper.loadProfiles(path + "rotten.json", startIDFrom = maxIdDataset2 + 1, sourceId = 3, realIDField = "realProfileID", fieldsToKeep = fields)
    val maxIdDataset3 = dataset3.map(_.id).max()

    val dataset4 = JSONWrapper.loadProfiles(path + "tmd.json", startIDFrom = maxIdDataset3 + 1, sourceId = 4, realIDField = "realProfileID", fieldsToKeep = fields)

    val maxProfileID = dataset4.map(_.id).max()

    val useEntropy = true

    val separators = Array(maxIdDataset1, maxIdDataset2, maxIdDataset3)

    val profiles = dataset1.union(dataset2).union(dataset3).union(dataset4)

    val clusters = LSHMio.clusterSimilarAttributes(
      profiles = profiles,
      numHashes = 128,
      targetThreshold = 0.3,
      maxFactor = 1.0,
      numBands = -1,
      keysToExclude = Nil,
      computeEntropy = useEntropy,
      separator = Settings.SOURCE_NAME_SEPARATOR
    )
    clusters.foreach(println)

    val realIdIds = sc.broadcast(profiles.map { p =>
      (p.originalID, p.id)
    }.collectAsMap())

    var newGT: Set[(Long, Long)] = null
    newGT = groundtruth.map { g =>
      val first = realIdIds.value.get(g.firstEntityID)
      val second = realIdIds.value.get(g.secondEntityID)
      if (first.isDefined && second.isDefined) {
        val f = first.get
        val s = second.get
        if (f < s) {
          (f, s)
        }
        else {
          (s, f)
        }
      }
      else {
        (-1L, -1L)
      }
    }.filter(_._1 >= 0).collect().toSet


    val newGTSize = newGT.size
    val gt = sc.broadcast(newGT)

    val blocks = TokenBlocking.createBlocksCluster(profiles, separators, clusters)

    println("Numero blocchi " + blocks.count())

    //println("SPARKER - PC/PQ after blocking " + PCPQBlockCalc.getPcPq(blocks, newGT, maxProfileID.toInt, separators))

    val blocksPurged = BlockPurging.blockPurging(blocks, 1.005)

    val profileBlocks = Converters.blocksToProfileBlocks(blocksPurged)
    val profileBlocksFiltered = BlockFiltering.blockFiltering(profileBlocks, 0.8)
    val blocksAfterFiltering = Converters.profilesBlockToBlocks(profileBlocksFiltered, separators)

    //println("SPARKER - PC/PQ after purging/filtering " + PCPQBlockCalc.getPcPq(blocksAfterFiltering, newGT, maxProfileID.toInt, separators))

    val blockIndexMap = blocksAfterFiltering.map(b => (b.blockID, b.profiles)).collectAsMap()
    val blockIndex = sc.broadcast(blockIndexMap)
    val profileBlocksSizeIndex: Broadcast[scala.collection.Map[Long, Int]] = sc.broadcast(profileBlocksFiltered.map(pb => (pb.profileID, pb.blocks.size)).collectAsMap())

    val blocksEntropiesMap: Broadcast[scala.collection.Map[Long, Double]] = {
      if (useEntropy) {
        val blocksEntropies = blocks.map(b => (b.blockID, b.entropy)).collectAsMap()
        sc.broadcast(blocksEntropies)
      }
      else {
        null
      }
    }


    val div = List(2, 4, 6, 8)

    val res = div.map { method =>
      val edgesAndCount = WNPFor.WNP(
        profileBlocksFiltered,
        blockIndex,
        maxProfileID.toInt,
        separators,
        gt,
        PruningUtils.ThresholdTypes.MAX_FRACT_2,
        PruningUtils.WeightTypes.chiSquare,
        profileBlocksSizeIndex,
        useEntropy,
        blocksEntropiesMap,
        method,
        PruningUtils.ComparisonTypes.OR
      )

      val numEdges = edgesAndCount.map(_._1).sum()
      val edges = edgesAndCount.flatMap(_._2).distinct()
      val perfectMatch = edges.count()

      val pc = perfectMatch.toFloat / newGTSize.toFloat
      val pq = perfectMatch.toFloat / numEdges.toFloat

      "SPARKER - BLAST, div = " + method + ", PC = " + pc + ", PQ = " + pq
    }

    res.foreach(println)

  }
}
