package Experiments

import BlockBuildingMethods.{BlockingUtils, LSHMio, TokenBlocking}
import BlockBuildingMethods.LSHMio.Settings
import BlockRefinementMethods.PruningMethods.{CNPFor, PCPQBlockCalc, PruningUtils, WNPFor}
import BlockRefinementMethods.{BlockFiltering, BlockPurging}
import Utilities.Converters
import Wrappers.{CSVWrapper, JSONWrapper}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Test standard meta-blocking with multiple datasources
  *
  * @author Luca Gagliardelli
  * @since 18/12/2018
  **/
object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.default.parallelism", "4")

    val sc = new SparkContext(conf)

    val path = "multidataset2/"

    val fields = List("Name", "Cast", "Director")

    val dataset1 = JSONWrapper.loadProfiles(path + "amazon.json", sourceId = 1, realIDField = "realProfileID", fieldsToKeep = fields)
    val maxIdDataset1 = dataset1.map(_.id).max()

    val dataset2 = JSONWrapper.loadProfiles(path + "imdb.json", startIDFrom = maxIdDataset1 + 1, sourceId = 2, realIDField = "realProfileID", fieldsToKeep = fields)
    val maxIdDataset2 = dataset2.map(_.id).max()

    val dataset3 = JSONWrapper.loadProfiles(path + "rotten.json", startIDFrom = maxIdDataset2 + 1, sourceId = 3, realIDField = "realProfileID", fieldsToKeep = fields)
    val maxIdDataset3 = dataset3.map(_.id).max()

    val dataset4 = JSONWrapper.loadProfiles(path + "tmd.json", startIDFrom = maxIdDataset3 + 1, sourceId = 4, realIDField = "realProfileID", fieldsToKeep = fields)

    val maxProfileID = dataset4.map(_.id).max()

    val useEntropy = false

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

    val groundtruth = JSONWrapper.loadGroundtruth(path + "groundtruth.json", "id1", "id2")

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

    val a: Set[Long] = newGT.flatMap(x => List(x._1, x._2))
    val gtOnly = sc.broadcast(a)

    val blocks = TokenBlocking.createBlocks(profiles, separators)
    //val blocks = TokenBlocking.createBlocksCluster(profiles, separators, clusters)

    println("Blocks " + blocks.count())

    //println("SPARKER - PC/PQ after blocking " + PCPQBlockCalc.getPcPq(blocks, newGT, maxProfileID.toInt, separators))

    val blocksPurged = BlockPurging.blockPurging(blocks, 1.005)

    val profileBlocks = Converters.blocksToProfileBlocks(blocksPurged)
    val profileBlocksFiltered = BlockFiltering.blockFiltering(profileBlocks, 0.8)
    val blocksAfterFiltering = Converters.profilesBlockToBlocks(profileBlocksFiltered, separators)


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


    val methods = List(PruningUtils.WeightTypes.JS, PruningUtils.WeightTypes.CBS, PruningUtils.WeightTypes.ARCS, PruningUtils.WeightTypes.ECBS, PruningUtils.WeightTypes.EJS)
    val tt = List(PruningUtils.ComparisonTypes.OR, PruningUtils.ComparisonTypes.AND)

    val res = tt.flatMap { t =>
      methods.map { method =>
        val edgesAndCount = WNPFor.WNP(
          profileBlocksFiltered,
          blockIndex,
          maxProfileID.toInt,
          separators,
          gt,
          PruningUtils.ThresholdTypes.AVG,
          method,
          profileBlocksSizeIndex,
          useEntropy,
          blocksEntropiesMap,
          10.0,
          t
        )

        val numEdges = edgesAndCount.map(_._1).sum()
        val edges = edgesAndCount.flatMap(_._2).distinct()
        val perfectMatch = edges.count()

        val pc = perfectMatch.toFloat / newGTSize.toFloat
        val pq = perfectMatch.toFloat / numEdges.toFloat

        "SPARKER - WNP " + t + ", method = " + method + ", PC = " + pc + ", PQ = " + pq
      }
    }

    val numberOfProfiles = profiles.count()
    val res1 = tt.flatMap { t =>
      methods.map { method =>
        val edgesAndCount = CNPFor.CNP(blocksAfterFiltering,
          numberOfProfiles,
          profileBlocksFiltered,
          blockIndex,
          maxProfileID.toInt,
          separators,
          gt,
          PruningUtils.ThresholdTypes.AVG,
          method,
          profileBlocksSizeIndex,
          useEntropy,
          blocksEntropiesMap,
          t
        )

        val numEdges = edgesAndCount.map(_._1).sum()
        val edges = edgesAndCount.flatMap(_._2).distinct()
        val perfectMatch = edges.count()

        val pc = perfectMatch.toFloat / newGTSize.toFloat
        val pq = perfectMatch.toFloat / numEdges.toFloat

        "SPARKER - CNP " + t + ", method = " + method + ", PC = " + pc + ", PQ = " + pq
      }
    }

    res.foreach(println)
    res1.foreach(println)

  }
}
