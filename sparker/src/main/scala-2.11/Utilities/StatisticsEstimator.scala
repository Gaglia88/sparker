package Utilities

import DataStructures._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.tools.nsc.interpreter.Completion.Candidates

/**
  * Created by Luca on 11/12/2016.
  */
object StatisticsEstimator {
  /**
    * Estimate PC and PQ
    * @param groundtruth the groundtruth
    * @param profiles original RDD of profiles
    * @param edges RDD of the final edges after pruning
    * @param realIDKey key of the parameter contained in the attibutes of the profiles that represents the ID of a profile (the same ID contained in the groundtruth)
    *
    * @return PC and PQ of the edges set
    * */
  def estimatePCandPQ(groundtruth : RDD[MatchingEntities], profiles : RDD[Profile], edges: RDD[WeightedEdge], realIDKey : String, firstDatasetMaxID : Long = -1) : (Double, Double) = {
    val profilesWithRealID = profiles.map(p => (p.id, p.getAttributeValues(realIDKey)))

    val candidatesCouplesID = edges.map(
      e =>
        if(firstDatasetMaxID >= 0 && e.firstProfileID > firstDatasetMaxID){
          (e.secondProfileID, e.firstProfileID)
        }
        else{
          (e.firstProfileID, e.secondProfileID)
        }
    )

    val candidatesCouples = candidatesCouplesID.join(profilesWithRealID).map(_._2).join(profilesWithRealID).map(x => MatchingEntities(x._2._1, x._2._2))

    val common = groundtruth.intersection(candidatesCouples)

    val foundMatch = common.count()
    val numCandidates = candidatesCouples.count()
    val totalPerfectMatch = groundtruth.count()

    ((foundMatch.toDouble/totalPerfectMatch), (foundMatch.toDouble/numCandidates))
  }

  /**
    * Estimate PC and PQ
    * @param groundtruth the groundtruth
    * @param profiles original RDD of profiles
    * @param edges RDD of the final edges after pruning
    * @param realIDKey key of the parameter contained in the attibutes of the profiles that represents the ID of a profile (the same ID contained in the groundtruth)
    *
    * @return PC and PQ of the edges set
    * */
  def estimatePCandPQ2(groundtruth : RDD[MatchingEntities], profiles : RDD[Profile], edges: RDD[UnweightedEdge], realIDKey : String, firstDatasetMaxID : Long = -1) : (Double, Double) = {
    val profilesWithRealID = profiles.map(p => (p.id, p.getAttributeValues(realIDKey)))

    val candidatesCouplesID = edges.map(
      e =>
        if(firstDatasetMaxID >= 0 && e.firstProfileID > firstDatasetMaxID){
          (e.secondProfileID, e.firstProfileID)
        }
        else{
          (e.firstProfileID, e.secondProfileID)
        }
    )

    val candidatesCouples = candidatesCouplesID.join(profilesWithRealID).map(_._2).join(profilesWithRealID).map(x => MatchingEntities(x._2._1, x._2._2))

    val common = groundtruth.intersection(candidatesCouples)

    val foundMatch = common.count()
    val numCandidates = candidatesCouples.count()
    val totalPerfectMatch = groundtruth.count()

    ((foundMatch.toDouble/totalPerfectMatch), (foundMatch.toDouble/numCandidates))
  }

  /**
    * Estimate PC and PQ profile real ID must be unique
    * @param groundTruth the ground truth
    * @param profiles original RDD of profiles
    * @param candidates RDD of the final edges after pruning
    * @param realIDKey key of the parameter contained in the attibutes of the profiles that represents the ID of a profile (the same ID contained in the groundtruth)
    *
    * @return PC and PQ of the edges set
    * */
  def estimatePCPQ_uniqueRealID(groundTruth : RDD[MatchingEntities], profiles : RDD[Profile], candidates: RDD[(Long, Long)], realIDKey : String) : (Double, Double) = {

    val sc = SparkContext.getOrCreate()
    println(profiles.map(_.getAttributeValues(realIDKey)).distinct.count)
    println(profiles.map(_.id).distinct.count)
    val profilesWithRealID = sc.broadcast(profiles.map(p => (p.getAttributeValues(realIDKey), p.id)).collectAsMap)

    def combinerAdd(xs: Set[Long], x: Long): Set[Long] = xs + x
    def combinerMerge(xs: Set[Long], ys: Set[Long]): Set[Long] = xs ++ ys
    val gt =
      sc.broadcast(
        groundTruth
          .map(me => {
            val pid = (profilesWithRealID.value(me.firstEntityID), profilesWithRealID.value(me.secondEntityID))
            pid match {
              case (pid1, pid2) if pid1 < pid2 => (pid1, pid2)
              case _ => (pid._2, pid._1)
            }
          })
          .combineByKey(x => Set(x),combinerAdd, combinerMerge)
          .collectAsMap
      )

    profilesWithRealID.destroy

    val foundMatch = candidates.filter(e => {
      gt.value.getOrElse(e._1, Set[Long]()) contains e._2
    }).distinct.count

    gt.destroy

    (foundMatch.toDouble/groundTruth.count, foundMatch.toDouble/candidates.count)
  }

  /**
    * Estimate PC and PQ
    * @param groundTruth the ground truth
    * @param profiles original RDD of profiles
    * @param candidates RDD of the final edges after pruning
    * @param realIDKey key of the parameter contained in the attibutes of the profiles that represents the ID of a profile (the same ID contained in the groundtruth)
    *
    * @return PC and PQ of the edges set
    * */
  def estimatePCPQ(groundTruth : RDD[MatchingEntities], profiles : RDD[Profile], candidates: RDD[(Long, Long)], realIDKey : String) : (Double, Double) = {

    val sc = SparkContext.getOrCreate()
    val profilesWithRealID = sc.broadcast(profiles.map(p => (p.id, p.getAttributeValues(realIDKey))).collectAsMap)

    def combinerAdd(xs: Set[String], x: String): Set[String] = xs + x
    def combinerMerge(xs: Set[String], ys: Set[String]): Set[String] = xs ++ ys

    val gt =
      sc.broadcast(
        groundTruth.map(x => (x.firstEntityID, x.secondEntityID))
          .combineByKey(x => Set(x),combinerAdd, combinerMerge)
          .collectAsMap
      )

    val foundMatch = candidates.filter(e => {
      gt.value.getOrElse(profilesWithRealID.value(e._1), Set[String]()) contains profilesWithRealID.value(e._2)
    }).distinct.count

    gt.destroy
    profilesWithRealID.destroy

    (foundMatch.toDouble/groundTruth.count, foundMatch.toDouble/candidates.count)
  }

  def test(groundTruth : RDD[MatchingEntities], profiles : RDD[Profile], candidates: RDD[(Long, Long)], realIDKey : String) = {
    val sc = SparkContext.getOrCreate()
    val profilesWithRealID = sc.broadcast(profiles.map(p => (p.getAttributeValues(realIDKey), p.id)).collectAsMap)

    def combinerAdd(xs: Set[Long], x: Long): Set[Long] = xs + x
    def combinerMerge(xs: Set[Long], ys: Set[Long]): Set[Long] = xs ++ ys

    val a = groundTruth
          .map(me => {
            val pid = (profilesWithRealID.value(me.firstEntityID), profilesWithRealID.value(me.secondEntityID))
            pid match {
              case (pid1, pid2) if pid1 < pid2 => (pid1, pid2)
              case _ => (pid._2, pid._1)
            }
          })
          .combineByKey(x => Set(x),combinerAdd, combinerMerge)

    val gt =
      sc.broadcast(a.collectAsMap)

    val foundMatch = candidates.filter(e => {
      gt.value.getOrElse(e._1, Set[Long]()) contains e._2
    })

    val profilesWithRealID1 = sc.broadcast(profiles.map(p => (p.id, p.getAttributeValues(realIDKey))).collect.toMap)
    val candidatesCouples = candidates.map(c => UnweightedEdge(c._1, c._2).getEntityMatch(profilesWithRealID1.value))

    val mapGroundTruth = SparkContext.getOrCreate().broadcast(groundTruth.map(gt => (gt.firstEntityID, gt.secondEntityID)).collect.toMap)
    val common =
      candidatesCouples
        .map(c => (c.firstEntityID, c.secondEntityID))
        .groupByKey().filter(containsKey(mapGroundTruth.value))
        .map(x => (profilesWithRealID.value(x._1), profilesWithRealID.value(mapGroundTruth.value(x._1))))

    val test =common.subtract(foundMatch).distinct()

    println(test.count)
    println(candidates.count)
    println(test.intersection(candidates).count)
    println(test.intersection(a.flatMap(x =>for(y <- x._2) yield (x._1, y))).count)
  }

    /**
    * Estimate PC and PQ
    * @param groundtruth the groundtruth
    * @param profiles original RDD of profiles
    * @param edges RDD of the final edges after pruning
    * @param realIDKey key of the parameter contained in the attibutes of the profiles that represents the ID of a profile (the same ID contained in the groundtruth)
    *
    * @return PC and PQ of the edges set
    * */
  def estimatePCandPQbroadcast(groundtruth : RDD[MatchingEntities], profiles : RDD[Profile], edges: RDD[WeightedEdge], realIDKey : String, isCleanClean: Boolean = false) : (Double, Double) = {

    val profilesWithRealID = SparkContext.getOrCreate().broadcast(profiles.map(p => (p.id, p.getAttributeValues(realIDKey))).collect.toMap)
    val candidatesCouples = edges.map(edge => edge.getEntityMatch(profilesWithRealID.value))

    val foundMatch = comparisonCandidatesGroundtruth(groundtruth, candidatesCouples, isCleanClean)

    profilesWithRealID.destroy()

    val numCandidates = edges.count()
    val totalPerfectMatch = groundtruth.count()

    (foundMatch.toDouble/totalPerfectMatch, foundMatch.toDouble/numCandidates)
  }

  /**
    * Estimate PC and PQ
    * @param groundtruth the groundtruth
    * @param profiles original RDD of profiles
    * @param edges RDD of the final edges after pruning
    * @param realIDKey key of the parameter contained in the attibutes of the profiles that represents the ID of a profile (the same ID contained in the groundtruth)
    *
    * @return PC and PQ of the edges set
    * */
  def estimatePCandPQbroadcast2(groundtruth : RDD[MatchingEntities], profiles : RDD[Profile], edges: RDD[UnweightedEdge], realIDKey : String, isCleanClean: Boolean = false) : (Double, Double) = {

    val profilesWithRealID = SparkContext.getOrCreate().broadcast(profiles.map(p => (p.id, p.getAttributeValues(realIDKey))).collect.toMap)
    val candidatesCouples = edges.map(edge => edge.getEntityMatch(profilesWithRealID.value))

    val foundMatch = comparisonCandidatesGroundtruth(groundtruth, candidatesCouples, isCleanClean)

    profilesWithRealID.destroy()

    val numCandidates = edges.count()
    val totalPerfectMatch = groundtruth.count()

    println("Matches2: " + foundMatch)

    (foundMatch.toDouble/totalPerfectMatch, foundMatch.toDouble/numCandidates)
  }

  def comparisonCandidatesGroundtruth(groundtruth: RDD[MatchingEntities], candidates: RDD[MatchingEntities], isCleanClean: Boolean): Long = {
    if(isCleanClean) {
      val mapGroundTruth = SparkContext.getOrCreate().broadcast(groundtruth.map(gt => (gt.firstEntityID, gt.secondEntityID)).collect.toMap)
      /*
      val commonCounter =
        candidates.map(c => (c.firstEntityID, c.secondEntityID)).groupByKey().filter(x => mapGroundTruth.value.contains(x._1) && x._2.toSet.contains(mapGroundTruth.value(x._1))).count()
      */
      val commonCounter =
        candidates.map(c => (c.firstEntityID, c.secondEntityID)).groupByKey().filter(containsKey(mapGroundTruth.value)).count()
      mapGroundTruth.destroy()
      commonCounter
    }
    else
      groundtruth.intersection(candidates).count()
  }

  // Check match elements
  def containsKey(map: Map[String, String])(input: (String, Iterable[String])): Boolean = {
    // If not exists the fist key in map, exit immediately
    if(! map.contains(input._1))
      false
    else {
      // Loop exit at first match found
      val matchValue = map(input._1)
      def loop(list: List[String]) : Boolean = {
        if(list == Nil) false
        else if(list.head == matchValue) true
        else loop(list.tail)
      }
      loop(input._2.toList)
    }
  }


  /***
    * Estimates the blocks entropy
    * */
  def estimateNormalizedEntropy(blocks : RDD[BlockAbstract], profileBlocks: Broadcast[Map[Long,ProfileBlocks]]) : RDD[(Long, Double)] = {
    val keysPerBlock = blocks.map{
      block =>
        val allKeys = block.getAllProfiles.flatMap {
          profileID =>
            profileBlocks.value(profileID).blocks.map(_.blockID)
        }
        (block.blockID, allKeys, block.size)
    }

    keysPerBlock map {
      blockWithKeys =>
        val blockID = blockWithKeys._1
        val keys = blockWithKeys._2
        val blockSize = blockWithKeys._3.toDouble

        val numberOfKeys = keys.size.toDouble
        val entropy = -keys.groupBy(x => x).map(x => (x._2.size)).map(s => (s / numberOfKeys) * Math.log(s.toDouble / numberOfKeys)).sum / numberOfKeys

        (blockID, entropy/blockSize)
    }
  }

  /***
    * Estimates the blocks entropy
    * */
  def estimateEntropy(blocks : RDD[BlockAbstract], profileBlocks: Broadcast[Map[Long,ProfileBlocks]]) : RDD[(Long, Double)] = {
    val keysPerBlock = blocks.map{
      block =>
        val allKeys = block.getAllProfiles.flatMap {
          profileID =>
            profileBlocks.value(profileID).blocks.map(_.blockID)
        }
        (block.blockID, allKeys)
    }

    keysPerBlock map {
      blockWithKeys =>
        val blockID = blockWithKeys._1
        val keys = blockWithKeys._2

        val numberOfKeys = keys.size.toDouble
        val entropy = -keys.groupBy(x => x).map(x => (x._2.size)).map(s => (s / numberOfKeys) * Math.log(s.toDouble / numberOfKeys)).sum / numberOfKeys

        (blockID, entropy)
    }
  }
}
