package BlockBuildingMethods

import DataStructures._
import com.twitter.algebird.{MinHashSignature, MinHasher, MinHasher32}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Implements methods based on LSH.
  * To performs LSH it use the library Twitter Algebird
  *
  * @author Luca Gagliardelli
  * @since 2016/12/23
  */
object LSHTwitterOld {
  /** Settings */
  object Settings {
    /** Name for the default cluster */
    val DEFAULT_CLUSTER_NAME = "tuttiTokenNonNeiCluster"
    val FIRST_DATASET_PREFIX = "d_1_"
    val SECOND_DATASET_PREFIX = "d_2_"
  }


  /**
    * Performs blocking using LSH.
    *
    * @param profiles RDD of entity profiles
    * @param numHashes number of hashes to generate
    * @param targetThreshold similarity threshold between profiles that have to finish in the same bucket
    * @param numBands number of bands for the LSH, if it is not set (or set to -1) the number of bands is calculated
    *                 automatically using the number of hashes and the target threshold
    * @param separatorID id to separate profiles from different dataset (Clean-Clean context), if it is Dirty put -1
    * @param keysToExclude keys to exclude from the blocking process
    * @return blocks
    * */
  def createBlocks(profiles: RDD[Profile], numHashes : Int, targetThreshold : Double, numBands : Int = -1, separatorID: Long = -1, keysToExclude : Iterable[String] = Nil): RDD[BlockAbstract] = {
    /* Number of bands */
    val b = numBands match {
      case -1 => MinHasher.pickBands(targetThreshold, numHashes)
      case _ => numBands
    }

    implicit lazy val minHasher = new MinHasher32(numHashes, b)

    /* For each profiles do the tokenization and then hashes the tokens, returns a list of (profileID, [hashes]) */
    val hashesPerProfile = profiles.flatMap{
      profile =>
        val attributes = profile.attributes.filter(kv => !keysToExclude.exists(_.equals(kv.key)))
        val keys = attributes.flatMap(_.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)).filter(_.trim.size > 0).distinct
        val hashes = keys.map(minHasher.init(_))
        hashes.map((profile.id, _))
    }.groupByKey()

    /* Merge together the hashes of each profiles, obtaining a signature for each profile */
    val profilesWithSignature = hashesPerProfile.map {
      case(profileID, hashes) =>
        (profileID, hashes.reduce((hash1, hash2) => minHasher.plus(hash1, hash2)))
    }

    /* Map the profiles in the buckets using the profile signature */
    val profilesPerBucket = profilesWithSignature.flatMap {
      case(profileID, signature) =>
        minHasher.buckets(signature).map((_, profileID))
    }.groupByKey()

    /* Transform each bucket in blocks */
    profilesPerBucket.map{
      case(bucketID, profileIDs) =>
        if (separatorID < 0) BlockDirty(bucketID, (profileIDs.toList, Nil))
        else BlockClean(bucketID, profileIDs.toList.partition(_ <= separatorID))
    }
  }

  /**
    * Given a list of profiles return a list of clusters of similar attributes based on the attributes values.
    * Thi cluster can be used to perform the clusted token blocking.
    *
    * @param profiles RDD of entity profiles
    * @param numHashes number of hashes to generate
    * @param targetThreshold similarity threshold between profiles that have to finish in the same bucket
    * @param numBands number of bands for the LSH, if it is not set (or set to -1) the number of bands is calculated
    *                 automatically using the number of hashes and the target threshold
    * @param separatorID id to separate profiles from different dataset (Clean-Clean context), if it is Dirty put -1
    * @param keysToExclude keys to exclude from the blocking process
    * @return clusters of similar attributes
    * */
  def clusterSimilarAttributes(profiles: RDD[Profile], numHashes : Int, targetThreshold : Double, numBands : Int = -1, separatorID: Long = -1, keysToExclude : Iterable[String] = Nil): List[KeysCluster] = {
    /* Number of bands */
    val b = numBands match {
      case -1 => MinHasher.pickBands(targetThreshold, numHashes)
      case _ => numBands
    }

    implicit lazy val minHasher = new MinHasher32(numHashes, b)

    /* Generate the tokens */
    val attributesToken : RDD[(String, String)] = profiles.flatMap {
      profile =>
        val dataset = if(profile.id > separatorID) Settings.FIRST_DATASET_PREFIX else Settings.SECOND_DATASET_PREFIX //Calculate the datasetID of the profile
        val attributes = profile.attributes.filter(kv => !keysToExclude.exists(_.equals(kv.key))) //Attributes to keep
        /* Tokenize the values of the keeped attributes, then for each token emits (dataset + key, token) */
        attributes.flatMap{
          kv =>
            kv.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).map((dataset+kv.key, _))
        }
    }

    /* Hashes the tokens for each attribute and the groups them */
    val attributeWithHashes : RDD[(String, Iterable[MinHashSignature])] = attributesToken.map{
      case(key, tokens) =>
        (key, minHasher.init(tokens))
    }.groupByKey()

    /** Calculates the signatures */
    val attributeWithSignature = attributeWithHashes.map {
      case(key, hashes) => (key, hashes.reduce((x, y) => minHasher.plus(x, y)))
    }

    /** Map the signature to the buckets, this will produce a list of (key, [buckets]) */
    val attributeWithBuckets = attributeWithSignature.map{case((key, signature)) => (key, minHasher.buckets(signature))}

    /** For each bucket emits (bucket, key) then groups the keys by buckets, and removes the buckets that contains
      * only one element. */
    val attributesPerBucket = attributeWithBuckets.flatMap{
      case(key, buckets) =>
        buckets.map((_, key))
    }.groupByKey().filter(_._2.size > 1)

    /** Generates the clusters of attributes (attributes that are finished in the same bucket) */
    val partialClusters = attributesPerBucket.map(_._2).distinct()

    /** Performs the transitive closure on the clusters, and add an unique id to the clusters */
    val clusters = transitiveClosure(partialClusters).zipWithIndex

    /** Calculates the default cluster ID */
    val defaultClusterID = {
      if(clusters.isEmpty){
       0
      }
      else{
        clusters.map(_._2).max + 1
      }
    }


    /** Generate a map to obain the cluster ID given the key */
    val keyClusterMap = clusters.flatMap {
      case(keys, clusterID) =>
        keys.map((_, clusterID))
    }.toMap

    /** Assign the tokens to each cluster */
    val keysPerCluster = attributesToken.map{
      case (key, token) =>
        val clusterID = keyClusterMap.get(key)  //Obain the cluster ID
        if(clusterID.isDefined){                //If is defined assigns the token to this cluster
          (clusterID.get, token)
        }
        else{                                   //Otherwise the token will be assigned to the default cluster
          (defaultClusterID, token)
        }
    }

    /** Calculates the entropy for each cluster */
    val entropyPerCluster = keysPerCluster.groupByKey() map {
      case (clusterID, tokens) =>
        val numberOfTokens = tokens.size.toDouble
        val entropy = -tokens.groupBy(x => x).map(x => (x._2.size)).map(s => (s / numberOfTokens) * Math.log(s.toDouble / numberOfTokens)).sum / numberOfTokens
        (clusterID, entropy)
    }

    /** A map that contains the cluster entropy for each cluster id */
    val entropyMap = entropyPerCluster.collectAsMap()

    /* Compose everything together */
    clusters.map {
      case (keys, clusterID) =>
        KeysCluster(clusterID, keys, entropyMap(clusterID))
    } ::: KeysCluster(defaultClusterID, (LSHTwitterOld.Settings.DEFAULT_CLUSTER_NAME :: Nil), entropyMap(defaultClusterID)) :: Nil
  }

  /**
    * Performs the transitive closure of the clusters.
    * If the same elements compares in more than one cluster, the two clusters
    * will be merged.
    * */
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

}
