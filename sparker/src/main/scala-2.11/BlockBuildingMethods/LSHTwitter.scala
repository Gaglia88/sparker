package BlockBuildingMethods

import DataStructures._
import com.twitter.algebird.{MinHasher, MinHasher32}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by Luca on 23/12/2016.
  */
object LSHTwitter {

  def createBlocks(profiles: RDD[Profile], numHashes : Int, targetThreshold : Double, numBands : Int = -1, separatorID: Long = -1): RDD[BlockAbstract] = {
    val b = numBands match {
      case -1 => MinHasher.pickBands(targetThreshold, numHashes)
      case _ => numBands
    }

    implicit lazy val minHasher = new MinHasher32(numHashes, b)

    println("Numero bande "+minHasher.numBands)

    val hashesPerProfile = profiles.flatMap{
      profile =>
        val keys = profile.attributes.flatMap(_.value.split("\\W+")).distinct.map(minHasher.init(_))
        keys.map((profile.id, _))
    }.groupByKey()

    val profilesWithSignature = hashesPerProfile.map {
      profileWithHashes =>
        (profileWithHashes._1, profileWithHashes._2.reduce((hash1, hash2) => minHasher.plus(hash1, hash2)))
    }

    val profilesPerBucket = profilesWithSignature.flatMap {
      profileWithSignature =>
        minHasher.buckets(profileWithSignature._2).map((_, profileWithSignature._1))
    }.groupByKey()


    profilesPerBucket.map{
      bucketWithProfiles =>
        if (separatorID < 0) BlockDirty(bucketWithProfiles._1, (bucketWithProfiles._2.toList, Nil))
        else BlockClean(bucketWithProfiles._1, bucketWithProfiles._2.toList.partition(_ <= separatorID))
    }
  }

  def clusterSimilarAttributes(profiles: RDD[Profile], numHashes : Int, targetThreshold : Double, numBands : Int = -1, separatorID: Long = -1): List[(Int, List[String], Double)] = {
    val b = numBands match {
      case -1 => MinHasher.pickBands(targetThreshold, numHashes)
      case _ => numBands
    }

    implicit lazy val minHasher = new MinHasher32(numHashes, b)

    val attributesToken = profiles.flatMap {
      profile =>
        val dataset = if(profile.id > separatorID) "d_"+1+"_" else "d_"+2+"_"

        profile.attributes.filter(_.key != "id").flatMap{
          av =>
            av.value.split("\\W+").map((dataset+av.key, _))
        }
    }

    val attributeWithHashes = attributesToken.map(at => (at._1, minHasher.init(at._2))).groupByKey()

    val attributeWithSignature = attributeWithHashes.map {
      ah => (ah._1, ah._2.reduce((x, y) => minHasher.plus(x, y)))
    }

    val attributeWithBuckets = attributeWithSignature.map(as => (as._1, minHasher.buckets(as._2)))
    val attributesPerBucket = attributeWithBuckets.flatMap(ab => ab._2.map((_, ab._1))).groupByKey().filter(_._2.size > 1)

    val cluster = attributesPerBucket.map(_._2).distinct()

    val clusters = transitiveClosure(cluster)

    val a = clusters.zipWithIndex
    val maxE = a.map(_._2).max + 1

    val c = a.flatMap(x => x._1.map((_, x._2))).toMap

    val entropyPerCluster = attributesToken.map{
      x =>
        val clusterID = c.get(x._1)
        if(clusterID.isDefined){
          (clusterID.get, x._2)
        }
        else{
          (maxE, x._2)
        }
    }.groupByKey() map {
      tc =>
        val tokens = tc._2
        val numberOfTokens = tokens.size.toDouble
        val entropy = -tokens.groupBy(x => x).map(x => (x._2.size)).map(s => (s / numberOfTokens) * Math.log(s.toDouble / numberOfTokens)).sum / numberOfTokens
        (tc._1, entropy)
    }

    val d = entropyPerCluster.collectAsMap()

    a.map {
      c =>
        (c._2, c._1, d(c._2))
    } ::: (maxE, ("tuttiTokenNonNeiCluster" :: Nil), d(maxE)) :: Nil
  }

  def transitiveClosure(input : RDD[Iterable[String]]) : List[List[String]] = {
    val elements = input.map(e => mutable.MutableList(e.toList: _*)).collect().toList

    val result : mutable.MutableList[mutable.MutableList[String]] = new mutable.MutableList

    var inserted : Boolean = false

    for(i <- 0 to elements.size-1){
      inserted = false
      for(j <- 0 to result.size-1){
        if(!result(j).intersect(elements(i)).isEmpty){
          result(j) ++= elements(i)
          result(j) = result(j).distinct
          inserted = true
        }
      }
      if(!inserted){
        result += elements(i)
      }
    }

    result.map(_.toList).toList
  }

  def clusterSimilarAttributesTest(profiles: RDD[Profile], numHashes : Int, targetThreshold : Double, numBands : Int = -1, separatorID: Long = -1): RDD[Iterable[String]] = {
    val b = numBands match {
      case -1 => MinHasher.pickBands(targetThreshold, numHashes)
      case _ => numBands
    }

    implicit lazy val minHasher = new MinHasher32(numHashes, b)

    val distinctAttributes = profiles.flatMap(p => p.attributes.map(_.key).distinct).distinct()
    val attributeWithHashes = distinctAttributes.map(a => (a, a.toList.map(minHasher.init(_))))
    val attributeWithSignature = attributeWithHashes.map {
      ah => (ah._1, ah._2.reduce((x, y) => minHasher.plus(x, y)))
    }
    val attributeWithBuckets = attributeWithSignature.map(as => (as._1, minHasher.buckets(as._2)))
    val attributesPerBucket = attributeWithBuckets.flatMap(ab => ab._2.map((_, ab._1))).groupByKey().filter(_._2.size > 1)

    attributesPerBucket.map(_._2).distinct()
  }

}
