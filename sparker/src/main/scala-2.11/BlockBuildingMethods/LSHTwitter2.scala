package BlockBuildingMethods

import java.util
import java.util.Calendar

import DataStructures._
import com.twitter.algebird.{MinHashSignature, MinHasher, MinHasher32}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.jgrapht.alg.ConnectivityInspector
import org.jgrapht.graph.{DefaultEdge, SimpleGraph}

/**
  * @since 2017/02/27
  * @author Luca Gagliardelli
  */
object LSHTwitter2 {
  /** Settings */
  object Settings {
    /** Name for the default cluster */
    val DEFAULT_CLUSTER_NAME = "tuttiTokenNonNeiCluster"
    /** Prefix for the attributes of the first dataset */
    val FIRST_DATASET_PREFIX = "d_1_"
    /** Prefix for the attributes of the second dataset */
    val SECOND_DATASET_PREFIX = "d_2_"
    /** First pregel msg */
    val INITIAL_MSG = -1.0
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
        if (separatorID < 0) BlockDirty(bucketID, (profileIDs.toSet, Set.empty))
        else BlockClean(bucketID, profileIDs.toSet.partition(_ <= separatorID))
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
    @transient lazy val log = org.apache.log4j.LogManager.getRootLogger

    val t0 = Calendar.getInstance()

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

    val firstDataset = attributeWithHashes.filter(_._1.startsWith(Settings.FIRST_DATASET_PREFIX)).zipWithUniqueId()
    val dasetSeparator = firstDataset.map(_._2).max()
    val secondDataset = attributeWithHashes.filter(_._1.startsWith(Settings.SECOND_DATASET_PREFIX)).zipWithUniqueId().map(x => (x._1, x._2+dasetSeparator+1))
    val attributesIdHashes = firstDataset.union(secondDataset)

    /** Calculates the signatures */
    val attributeWithSignature = attributesIdHashes.map {
      case ((attribute, hashes), attributeID) => (attributeID, hashes.reduce((x, y) => minHasher.plus(x, y)))
    }

    val attributeSignatures = attributeWithSignature.collectAsMap()

    /** Map the (attribute, signature) to the buckets, this will produce a list of (attribute, signature, [buckets]) */
    val attributeWithBuckets = attributeWithSignature.map { case ((attributeID, signature)) => (attributeID, minHasher.buckets(signature)) }

    attributeWithBuckets.count()
    val t1 = Calendar.getInstance()
    log.info("SPARKER - Time to perform LSH "+(t1.getTimeInMillis-t0.getTimeInMillis)+" ms")


    /** For each bucket emits (bucket, (attribute, signature)) then groups the keys by the buckets,
      * and removes the buckets that contains only one element. */
    val attributesPerBucket = attributeWithBuckets.flatMap {
      case (attributeID, buckets) =>
        buckets.map(bucketID => (bucketID, attributeID))
    }.groupByKey().filter(x => x._2.size > 1 && x._2.size < 101)

    attributesPerBucket.count()
    val t2 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate attributesPerBucket "+(t2.getTimeInMillis-t1.getTimeInMillis)+" ms")

    /** Generates the clusters of attributes (attributes that are finished in the same bucket) */
    val partialClusters = attributesPerBucket.map(_._2)


    val sc = SparkContext.getOrCreate()
    val attributeSignaturesBroadcast = sc.broadcast(attributeSignatures)

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
      val attrPartitioned = clusterElements.partition(_ <= dasetSeparator)
      /** Generates the edges */
      if(separatorID >= 0){//Clean context
        (for (e1<-attrPartitioned._1; e2<-attrPartitioned._2) yield (e1,(e2,minHasher.similarity(attributeSignaturesBroadcast.value(e1), attributeSignaturesBroadcast.value(e2)))))
      }
      else{//Dirty context
        attrPartitioned._1.toList.combinations(2).map(x =>
          (x(0), (x(1), minHasher.similarity(attributeSignaturesBroadcast.value(x(0)), attributeSignaturesBroadcast.value(x(1)))))
        )
      }
    }//.distinct()


    edges.count()
    val t3 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate edges "+(t3.getTimeInMillis-t2.getTimeInMillis)+" ms")


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

    /** For each attribute keeps the attribute with the highest JS, and produce a cluster of elements (k1, k2) */
    val topEdges = edgesPerKey.map{case(key1, keys2) =>
      val max = keys2.maxBy(_._2)
      (key1, max._1)
    }

    topEdges.count()
    val t4 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate top edges "+(t4.getTimeInMillis-t3.getTimeInMillis)+" ms")


    val graph = new SimpleGraph[Long, DefaultEdge](classOf[DefaultEdge]);

    val vertices = topEdges.map(_._1).union(topEdges.map(_._2)).distinct().collect()

    vertices.foreach{ v =>
      graph.addVertex(v)
    }
    topEdges.collect().foreach{ case(from, to) =>
      graph.addEdge(from, to)
    }

    val ci =  new ConnectivityInspector(graph)

    val connectedComponents = ci.connectedSets()

    val attributeMap = attributesIdHashes.map(x => (x._2, x._1._1)).collectAsMap()

    val clusters : Iterable[(Iterable[String], Int)] = (for(i <- 0 to connectedComponents.size()-1) yield{
      val a = connectedComponents.get(i).asInstanceOf[util.HashSet[Long]].iterator()
      var l : List[String] = Nil
      while(a.hasNext){
        l = attributeMap(a.next()) :: l
      }
      (l, i)
    }).filter(_._1.size > 0)

    val t5 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate clusters "+(t5.getTimeInMillis-t4.getTimeInMillis)+" ms")
    attributeSignaturesBroadcast.destroy()

    /** Performs the transitive closure on the clusters, and add an unique id to each cluster */
    //val clusters : Iterable[(Iterable[String], Int)] = clustersTransitiveClosure(topEdges).zipWithIndex

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

    val normalizeEntropy = false

    /** Calculates the entropy for each cluster */
    val entropyPerAttribute = attributesToken.groupByKey().map {
      case (attribute, tokens) =>
        val numberOfTokens = tokens.size.toDouble
        val tokensCount = tokens.groupBy(x => x).map(x => (x._2.size))
        val tokensP = tokensCount.map{
          tokenCount =>
            val p_i : Double = tokenCount/numberOfTokens
            (p_i * (Math.log10(p_i) / Math.log10(2.0d)))
        }

        val entropy = {
          if(normalizeEntropy){
            -tokensP.sum / (Math.log10(numberOfTokens) / Math.log10(2.0d))
          }
          else{
            -tokensP.sum
          }
        }
        (attribute, entropy)
    }

    attributesToken.unpersist()


    /** Assign the tokens to each cluster */
    val entropyPerCluster = entropyPerAttribute.map {
      case (attribute, entropy) =>
        val clusterID = keyClusterMap.get(attribute) //Obain the cluster ID
        if (clusterID.isDefined) {//If is defined assigns the tokens to this cluster
          (clusterID.get, entropy)
        }
        else {//Otherwise the tokens will be assigned to the default cluster
          (defaultClusterID, entropy)
        }
    }.groupByKey().map(x => (x._1, (x._2.sum/x._2.size)))

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
  def clusterSimilarAttributes2(profiles: RDD[Profile], numHashes: Int, targetThreshold: Double, maxFactor : Double, separateAttributes : Boolean = true, numBands: Int = -1, separatorID: Long = -1, keysToExclude: Iterable[String] = Nil): List[KeysCluster] = {
    @transient lazy val log = org.apache.log4j.LogManager.getRootLogger

    val t0 = Calendar.getInstance()

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

    val firstDataset = attributeWithHashes.filter(_._1.startsWith(Settings.FIRST_DATASET_PREFIX)).zipWithUniqueId()
    val dasetSeparator = firstDataset.map(_._2).max()
    val secondDataset = attributeWithHashes.filter(_._1.startsWith(Settings.SECOND_DATASET_PREFIX)).zipWithUniqueId().map(x => (x._1, x._2+dasetSeparator+1))
    val attributesIdHashes = firstDataset.union(secondDataset)

    /** Calculates the signatures */
    val attributeWithSignature = attributesIdHashes.map {
      case ((attribute, hashes), attributeID) => (attributeID, hashes.reduce((x, y) => minHasher.plus(x, y)))
    }

    val attributeSignatures = attributeWithSignature.collectAsMap()

    /** Map the (attribute, signature) to the buckets, this will produce a list of (attribute, signature, [buckets]) */
    val attributeWithBuckets = attributeWithSignature.map { case ((attributeID, signature)) => (attributeID, minHasher.buckets(signature)) }

    attributeWithBuckets.count()
    val t1 = Calendar.getInstance()
    log.info("SPARKER - Time to perform LSH "+(t1.getTimeInMillis-t0.getTimeInMillis)+" ms")


    /** For each bucket emits (bucket, (attribute, signature)) then groups the keys by the buckets,
      * and removes the buckets that contains only one element. */
    val attributesPerBucket = attributeWithBuckets.flatMap {
      case (attributeID, buckets) =>
        buckets.map(bucketID => (bucketID, attributeID))
    }.groupByKey().filter(x => x._2.size > 1 && x._2.size < 101)

    attributesPerBucket.count()
    val t2 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate attributesPerBucket "+(t2.getTimeInMillis-t1.getTimeInMillis)+" ms")

    /** Generates the clusters of attributes (attributes that are finished in the same bucket) */
    val partialClusters = attributesPerBucket.map(_._2)


    val sc = SparkContext.getOrCreate()
    val attributeSignaturesBroadcast = sc.broadcast(attributeSignatures)

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
      if(separateAttributes){
        val attrPartitioned = clusterElements.partition(_ <= dasetSeparator)
        /** Generates the edges */
        if(separatorID >= 0){//Clean context
          (for (e1<-attrPartitioned._1; e2<-attrPartitioned._2) yield (e1,(e2,minHasher.similarity(attributeSignaturesBroadcast.value(e1), attributeSignaturesBroadcast.value(e2)))))
        }
        else{//Dirty context
          attrPartitioned._1.toList.combinations(2).map(x =>
            (x(0), (x(1), minHasher.similarity(attributeSignaturesBroadcast.value(x(0)), attributeSignaturesBroadcast.value(x(1)))))
          )
        }
      }
      else{
        clusterElements.toList.combinations(2).map(x =>
          (x(0), (x(1), minHasher.similarity(attributeSignaturesBroadcast.value(x(0)), attributeSignaturesBroadcast.value(x(1)))))
        )
      }
    }


    edges.count()
    val t3 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate edges "+(t3.getTimeInMillis-t2.getTimeInMillis)+" ms")


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

    /** For each attribute keeps the attribute with the highest JS, and produce a cluster of elements (k1, k2) */
    val topEdges = edgesPerKey.map{case(key1, keys2) =>
      val max = keys2.map(_._2).max*maxFactor
      (key1, keys2.filter(_._2 >= max).map(_._1))
    }

    topEdges.count()
    val t4 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate top edges "+(t4.getTimeInMillis-t3.getTimeInMillis)+" ms")


    val graph = new SimpleGraph[Long, DefaultEdge](classOf[DefaultEdge]);

    val vertices = topEdges.map(_._1).union(topEdges.flatMap(_._2)).distinct().collect()

    vertices.foreach{ v =>
      graph.addVertex(v)
    }
    topEdges.collect().foreach{ case(from, to) =>
      to.foreach{n =>
        graph.addEdge(from, n)
      }
    }

    val ci =  new ConnectivityInspector(graph)

    val connectedComponents = ci.connectedSets()

    val attributeMap = attributesIdHashes.map(x => (x._2, x._1._1)).collectAsMap()

    val clusters : Iterable[(Iterable[String], Int)] = (for(i <- 0 to connectedComponents.size()-1) yield{
      val a = connectedComponents.get(i).asInstanceOf[util.HashSet[Long]].iterator()
      var l : List[String] = Nil
      while(a.hasNext){
        l = attributeMap(a.next()) :: l
      }
      (l, i)
    }).filter(_._1.size > 0)

    val t5 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate clusters "+(t5.getTimeInMillis-t4.getTimeInMillis)+" ms")
    attributeSignaturesBroadcast.destroy()

    /** Performs the transitive closure on the clusters, and add an unique id to each cluster */
    //val clusters : Iterable[(Iterable[String], Int)] = clustersTransitiveClosure(topEdges).zipWithIndex

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

    val normalizeEntropy = false

    /** Calculates the entropy for each cluster */
    val entropyPerAttribute = attributesToken.groupByKey().map {
      case (attribute, tokens) =>
        val numberOfTokens = tokens.size.toDouble
        val tokensCount = tokens.groupBy(x => x).map(x => (x._2.size))
        val tokensP = tokensCount.map{
          tokenCount =>
            val p_i : Double = tokenCount/numberOfTokens
            (p_i * (Math.log10(p_i) / Math.log10(2.0d)))
        }

        val entropy = {
          if(normalizeEntropy){
            -tokensP.sum / (Math.log10(numberOfTokens) / Math.log10(2.0d))
          }
          else{
            -tokensP.sum
          }
        }
        (attribute, entropy)
    }

    attributesToken.unpersist()


    /** Assign the tokens to each cluster */
    val entropyPerCluster = entropyPerAttribute.map {
      case (attribute, entropy) =>
        val clusterID = keyClusterMap.get(attribute) //Obain the cluster ID
        if (clusterID.isDefined) {//If is defined assigns the tokens to this cluster
          (clusterID.get, entropy)
        }
        else {//Otherwise the tokens will be assigned to the default cluster
          (defaultClusterID, entropy)
        }
    }.groupByKey().map(x => (x._1, (x._2.sum/x._2.size)))

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

}
