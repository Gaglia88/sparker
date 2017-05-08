package BlockBuildingMethods

import DataStructures._
import com.twitter.algebird.{MinHashSignature, MinHasher, MinHasher32}
import org.apache.spark.rdd.RDD
import scala.collection.mutable

/**
  * @since 2017/02/27
  * @author Luca Gagliardelli
  */
object LSHTwitter3 {
  /** Settings */
  object Settings {
    /** Name for the default cluster */
    val DEFAULT_CLUSTER_NAME = "tuttiTokenNonNeiCluster"
    /** Prefix for the attributes of the first dataset */
    val FIRST_DATASET_PREFIX = "d_1_"
    /** Prefix for the attributes of the second dataset */
    val SECOND_DATASET_PREFIX = "d_2_"
  }

  def createBlocks(profiles: RDD[Profile], numHashes: Int, targetThreshold: Double, numBands: Int = -1, separatorID: Long = -1, keysToExclude: Iterable[String] = Nil): RDD[BlockAbstract] = {
    /* Number of bands */
    val b = numBands match {
      case -1 => MinHasher.pickBands(targetThreshold, numHashes)
      case _ => numBands
    }

    implicit lazy val minHasher = new MinHasher32(numHashes, b)

    /* For each profiles do the tokenization and then hashes the tokens, returns a list of (profileID, [hashes]) */
    val hashesPerProfile = profiles.flatMap {
      profile =>
        val attributes = profile.attributes.filter(kv => !keysToExclude.exists(_.equals(kv.key)))
        val keys = attributes.flatMap(_.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)).filter(_.trim.size > 0).distinct
        val hashes = keys.map(minHasher.init(_))
        hashes.map((profile.id, _))
    }.groupByKey()

    /* Merge together the hashes of each profiles, obtaining a signature for each profile */
    val profilesWithSignature = hashesPerProfile.map {
      case (profileID, hashes) =>
        (profileID, hashes.reduce((hash1, hash2) => minHasher.plus(hash1, hash2)))
    }

    /* Map the profiles in the buckets using the profile signature */
    val profilesPerBucket = profilesWithSignature.flatMap {
      case (profileID, signature) =>
        minHasher.buckets(signature).map((_, profileID))
    }.groupByKey()

    /* Transform each bucket in blocks */
    profilesPerBucket.map {
      case (bucketID, profileIDs) =>
        if (separatorID < 0) BlockDirty(bucketID, (profileIDs.toList, Nil))
        else BlockClean(bucketID, profileIDs.toList.partition(_ <= separatorID))
    }.filter(_.getComparisonSize() > 0).map(x => x)
  }

  /**
    * Given a list of profiles return a list of clusters of similar attributes based on the attributes values.
    * Thi cluster can be used to perform the clusted token blocking.
    *
    * @param profiles        RDD of entity profiles
    * @param numHashes       number of hashes to generate
    * @param targetThreshold similarity threshold between profiles that have to finish in the same bucket
    * @param numBands        number of bands for the LSH, if it is not set (or set to -1) the number of bands is calculated
    *                        automatically using the number of hashes and the target threshold
    * @param separatorID     id to separate profiles from different dataset (Clean-Clean context), if it is Dirty put -1
    * @param keysToExclude   keys to exclude from the blocking process
    * @return clusters of similar attributes
    **/
  def clusterSimilarAttributes(profiles: RDD[Profile], numHashes: Int, targetThreshold: Double, numBands: Int = -1, separatorID: Long = -1, keysToExclude: Iterable[String] = Nil): List[KeysCluster] = {
    /* Number of bands */
    val b = numBands match {
      case -1 => MinHasher.pickBands(targetThreshold, numHashes)
      case _ => numBands
    }

    implicit lazy val minHasher = new MinHasher32(numHashes, b)

    /* Generate the tokens */
    val attributesToken: RDD[(String, String)] = profiles.flatMap {
      profile =>
        val dataset = if (profile.id > separatorID) Settings.FIRST_DATASET_PREFIX else Settings.SECOND_DATASET_PREFIX //Calculate the datasetID of the profile
        val attributes = profile.attributes.filter(kv => !keysToExclude.exists(_.equals(kv.key))) //Attributes to keep
        /* Tokenize the values of the keeped attributes, then for each token emits (dataset + key, token) */
        attributes.flatMap {
          kv =>
            kv.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).filter(_.trim.size > 0).map((dataset + kv.key, _))
        }
    }

    /* Hashes the tokens for each attribute and the groups them */
    val attributeWithHashes: RDD[(String, Iterable[MinHashSignature])] = attributesToken.map {
      case (attribute, tokens) =>
        (attribute, minHasher.init(tokens))
    }.groupByKey()

    /** Calculates the signatures */
    val attributeWithSignature = attributeWithHashes.map {
      case (attribute, hashes) => (attribute, hashes.reduce((x, y) => minHasher.plus(x, y)))
    }

    /** Map the (attribute, signature) to the buckets, this will produce a list of (attribute, signature, [buckets]) */
    val attributeWithBuckets = attributeWithSignature.map { case ((attribute, signature)) => (attribute, signature, minHasher.buckets(signature)) }

    /** For each bucket emits (bucket, (attribute, signature)) then groups the keys by the buckets,
      * and removes the buckets that contains only one element. */
    val attributesPerBucket = attributeWithBuckets.flatMap {
      case (attribute, signature, buckets) =>
        buckets.map(bucketID => (bucketID, (attribute, signature)))
    }.groupByKey().filter(_._2.size > 1)

    /** Generates the clusters of attributes (attributes that are finished in the same bucket) */
    val partialClusters = attributesPerBucket.map(_._2).distinct()

    /**
      * Generates edges between the different attributes in each cluster
      * Produces a list of (attr1, (attr2, JaccardSimilarity(attr1, attr2))
      * */
    val edges = partialClusters.flatMap{clusterElements =>
      /**
        * For each cluster divide the attributes by first/second datasets
        * This will produces two list, one with the attributes that belongs from the first dataset, and one with the
        * attributes that belongs from the second.
        * */
      val attrPartitioned = clusterElements.partition(_._1.startsWith(Settings.FIRST_DATASET_PREFIX))
      /** Generates the edges */
      if(separatorID >= 0){//Clean context
        (for (e1<-attrPartitioned._1; e2<-attrPartitioned._2) yield (e1._1,(e2._1,minHasher.similarity(e1._2, e2._2)))).toList
      }
      else{//Dirty context
        attrPartitioned._1.toList.combinations(2).map(x =>
          (x(0)._1, (x(1)._1, minHasher.similarity(x(0)._2, x(1)._2)))
        ).toList
      }
    }.distinct()

    /** Produces all the edges,
      * e.g if we have (attr1, (attr2, sim(a1,a2)) will also generates
      * (attr2, (attr1, sim(a1, a2))
      * Then groups the edges for the first attribute, this will produce
      * for each attribute a list of similar attributes
      * */
    val edgesPerKey =
      edges.union(
        edges.map{case(attr1, (attr2, sim)) =>
          (attr2, (attr1, sim))
        }
      ).groupByKey()

    /** For each attribute keeps the attributes with the highest JS, and produce a cluster of elements (k1, [top k2]) */
    val topEdges = edgesPerKey.map{case(key1, keys2) =>
      val max = keys2.map(_._2).max
      (key1, keys2.filter(_._2 == max).map(_._1))
    }

    /** Performs the transitive closure on the clusters, and add an unique id to each cluster */
    val clusters : Iterable[(Iterable[String], Int)] = clustersTransitiveClosure(topEdges).zipWithIndex

    /** Calculates the default cluster ID */
    val defaultClusterID = {
      if (clusters.isEmpty) {
        0
      }
      else {
        clusters.map(_._2).max + 1
      }
    }

    /** Generates a map to obain the cluster ID given an attribute */
    val keyClusterMap = clusters.flatMap {
      case (attributes, clusterID) =>
        attributes.map(attribute => (attribute, clusterID))
    }.toMap

    /** Assign the tokens to each cluster */
    val keysPerCluster = attributesToken.map {
      case (attribute, tokens) =>
        val clusterID = keyClusterMap.get(attribute) //Obain the cluster ID
        if (clusterID.isDefined) {//If is defined assigns the tokens to this cluster
          (clusterID.get, tokens)
        }
        else {//Otherwise the tokens will be assigned to the default cluster
          (defaultClusterID, tokens)
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

    /** Entropy of the default cluster */
    val defaultEntropy = {
      val e = entropyMap.get(defaultClusterID)
      if(e.isDefined){
        e.get
      }
      else{
        0.0
      }
    }

    /* Compose everything together */
    clusters.map {
      case (keys, clusterID) =>
        val entropy = {
          val e = entropyMap.get(clusterID)
          if(e.isDefined){
            e.get
          }
          else{
            1
          }
        }
        KeysCluster(clusterID, keys.toList, entropy)
    }.toList ::: KeysCluster(defaultClusterID, (Settings.DEFAULT_CLUSTER_NAME :: Nil), defaultEntropy) :: Nil
  }

  /**
    * Performs the transitive closure
    *
    * @param edges the edges generated by the clustering process
    * @return clusters
    * */
  def clustersTransitiveClosure(edges : RDD[(String, Iterable[String])]) : Iterable[Iterable[String]] = {
    val edgesMap = edges.collectAsMap()//A map that allow to access to the edges of each attribute
    val visited : mutable.HashSet[String] = new mutable.HashSet[String]()//Attributes that was already assigned to a cluster
    val clusters : mutable.ListBuffer[List[String]] = new mutable.ListBuffer[List[String]]()//Generated clusters

    val attributes = edges.map(_._1).collect()//All attributes that are in the edges, they are the vertex of the graph

    attributes.foreach{vertex => //foreach attribute (vertex of the graph)
      if(!visited.contains(vertex)){//If this vertex was not visited yet
        val cluster = getCluster(vertex, edgesMap)//Generate the cluster of this vertex
        clusters += cluster.toList//Add the cluster to the results
        visited ++= cluster//Mark as visited all the vertex that compose the cluster
      }
    }

    clusters.toList
  }

  /**
    * Given a vertex (attribute) of the graph and the edges, returns the cluster (all elements that are connected
    * to the given vertex).
    * @param vertexToExplore the starting vertex
    * @param edges a map that contains for each vertex the edges
    * */
  def getCluster(vertexToExplore : String, edges : scala.collection.Map[String, Iterable[String]]) : Iterable[String] = {
    val toExplore : mutable.Queue[String] = new mutable.Queue[String]()//Contains the vertices to explore
    val cluster : mutable.HashSet[String] = new mutable.HashSet[String]()//Contains the elements of the cluster
    toExplore += vertexToExplore//Add the vertex to the vertices to explore

    while(!toExplore.isEmpty){//Continues until it has explored all vertices
      val currentNode = toExplore.dequeue()//Current vertex to explore
      cluster += currentNode//Add the vertex to the cluster
      val neighbours = edges.get(currentNode)//Get his neighbours
      if(neighbours.isDefined){//If there are neighbours
        neighbours.get.foreach{neighbour => //For each neighbour
          if(!cluster.contains(neighbour)){//If it has not explored this neighbour yet
            toExplore += neighbour//Adds the neighbour to the vertices to explore
          }
        }
      }

    }
    cluster
  }
}
