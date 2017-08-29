package BlockBuildingMethods

import java.util
import java.util.Calendar

import BlockBuildingMethods.LSHTwitter.Settings
import DataStructures._
import com.twitter.algebird.{MinHashSignature, MinHasher, MinHasher32}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.jgrapht.alg.ConnectivityInspector
import org.jgrapht.graph.{DefaultEdge, SimpleGraph}

import scala.util.Random

/**
  * Created by Luca on 23/08/2017.
  */
object LSHMio {

  def getHashes(startingHash : Int, count : Int, seed : Int = 1234) : Array[Int] = {
    val hashCodes = Array.ofDim[Int](count);
    val machineWordSize = 32;
    val hashCodeSize = machineWordSize / 2;
    val hashCodeSizeDiff = machineWordSize - hashCodeSize;
    val hstart = startingHash;
    val bmax = 1 << hashCodeSizeDiff;
    val rnd = new Random(seed);

    for(i <- 0 to count-1) {
      hashCodes.update(i, ((hstart * (i*2 + 1)) + rnd.nextInt(bmax)) >>  hashCodeSizeDiff)
    }
    hashCodes
  }

  def getHashes2(strHash : Int, numHashes : Int, seed : Int = 1234) : Array[Int] = {
    val rnd = new Random(seed)
    val hashes = for(i <- 0 to numHashes-1) yield {
      val a : Int = 1+rnd.nextInt()
      val b : Int = rnd.nextInt()
      (((a.toLong*strHash.toLong+b.toLong)%2147495899L)%Integer.MAX_VALUE).toInt
    }
    hashes.toArray
  }


  def getHashCode(str : String) : Int = {
    str.hashCode() & Integer.MAX_VALUE
  }

  def getNumBands(targetThreshold : Double, sigNum : Int) : Int = {
    var b = sigNum
    def r = (sigNum.toDouble / b)
    def t = Math.pow(1.0/b, 1.0/r)

    while (t < targetThreshold && b > 1){
      b -= 1
    }
    b+1
  }

  def getNumRows(targetThreshold : Double, sigNum : Int) : Int = {
    val bands = getNumBands(targetThreshold, sigNum)
    sigNum/bands
  }

  def calcSimilarity(sig1: Array[Int], sig2: Array[Int]) : Double = {
    //val common = sig1.intersect(sig2).length
    //common.toDouble/(sig1.length+sig2.length-common).toDouble
    var common : Double = 0
    for(i <- 0 to sig1.length-1 if (sig1(i) == sig2(i))){common += 1}
    common/sig1.length.toDouble
  }


  def createBlocks(profiles: RDD[Profile], numHashes: Int, targetThreshold: Double, numBands: Int = -1, separatorID: Long = -1, keysToExclude: Iterable[String] = Nil): RDD[BlockAbstract] = {
    @transient lazy val log = org.apache.log4j.LogManager.getRootLogger
    /* For each profiles do the tokenization and then hashes the tokens, returns a list of (profileID, [hashes]) */
    val hashesPerProfile = profiles.flatMap {
      profile =>
        val attributes = profile.attributes.filter(kv => !keysToExclude.exists(_.equals(kv.key)))
        val keys = attributes.flatMap(_.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)).filter(_.trim.size > 0).distinct
        val hashes = keys.map(k => getHashes2(getHashCode(k), numHashes))
        hashes.map((profile.id, _))
    }.groupByKey()

    /* Merge together the hashes of each profiles, obtaining a signature for each profile */
    val profilesWithSignature = hashesPerProfile.map{case (attribute, hashes) =>
      val signature = hashes.reduce{
        (a1, a2) =>
          for(i <- 0 to a1.length-1){
            if(a2(i) < a1(i)){
              a1.update(i, a2(i))
            }
          }
          a1
      }
      (attribute, signature)
    }

    log.info("SPARKER - Num bands "+getNumBands(targetThreshold, numHashes))
    val numRows = getNumRows(targetThreshold, numHashes)
    val buckets = profilesWithSignature.map{case (attribute, signature) =>
      val buckets = signature.sliding(numRows, numRows).map(_.toList.hashCode()).toIterable
      (attribute, buckets)
    }

    val profilesPerBucket = buckets.flatMap(x => x._2.map((_, x._1))).groupByKey().map(x => (x._1, x._2.toSet)).filter(x => x._2.size > 1).distinct()

    /* Transform each bucket in blocks */
    profilesPerBucket.map {
      case (bucketID, profileIDs) =>
        if (separatorID < 0) BlockDirty(bucketID, (profileIDs, Set.empty))
        else BlockClean(bucketID, profileIDs.partition(_ <= separatorID))
    }.filter(_.getComparisonSize() > 0).map(x => x)
  }


  def clusterSimilarAttributes(profiles: RDD[Profile], numHashes: Int, targetThreshold: Double, maxFactor : Double, separateAttributes : Boolean = true, numBands: Int = -1, separatorID: Long = -1, keysToExclude: Iterable[String] = Nil): List[KeysCluster] = {
    @transient lazy val log = org.apache.log4j.LogManager.getRootLogger

    log.info("SPARKER - MY LSH VERSION ;-D")

    val t0 = Calendar.getInstance()

    /* Generate the tokens */
    val attributesToken: RDD[(String, Int)] = profiles.flatMap {
      profile =>
        val dataset = if (profile.id > separatorID) Settings.FIRST_DATASET_PREFIX else Settings.SECOND_DATASET_PREFIX
        val attributes = profile.attributes.filter(kv => !keysToExclude.exists(_.equals(kv.key)))
        attributes.flatMap {
          kv =>
            kv.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).filter(_.trim.size > 0).map(_.toLowerCase).map(x => (dataset + kv.key, getHashCode(x)))
        }
    }

    val tokensPerAttribute = attributesToken.groupByKey().map(x => (x._1, x._2.toSet))

    val hashes = tokensPerAttribute.map{case(attribute, tokens) =>
      //(attribute, tokens.map(t => getHashes(t, numHashes)))
      (attribute, tokens.map(t => getHashes2(t, numHashes)))
    }


    //hashes.map(x => (x._1, x._2.map(_.toList))).collect().foreach(log.info)

    val attributeWithSignature = hashes.map{case (attribute, hashes) =>
      val signature = hashes.reduce{
        (a1, a2) =>
          for(i <- 0 to a1.length-1){
            if(a2(i) < a1(i)){
              a1.update(i, a2(i))
            }
          }
          a1
      }
      (attribute, signature)
    }

    log.info("SPARKER - Num bands "+getNumBands(targetThreshold, numHashes))

    val numRows = getNumRows(targetThreshold, numHashes)

    val buckets = attributeWithSignature.map{case (attribute, signature) =>
      val buckets = signature.sliding(numRows, numRows).map(_.toList.hashCode()).toIterable
      (attribute, buckets)
    }

    val attributesPerBucket = buckets.flatMap(x => x._2.map((_, x._1))).groupByKey().map(x => (x._1, x._2.toSet)).filter(x => x._2.size > 1 && x._2.size < 101).map(_._2).distinct()

    attributesPerBucket.count()

    val attributeSignatures = attributeWithSignature.collectAsMap()


    val t1 = Calendar.getInstance()
    log.info("SPARKER - Time to perform LSH "+(t1.getTimeInMillis-t0.getTimeInMillis)+" ms")

    val numbuckets = attributesPerBucket.count()
    val t2 = Calendar.getInstance()
    log.info("SPARKER - Number of buckets "+numbuckets)
    log.info("SPARKER - Time to calculate attributesPerBucket "+(t2.getTimeInMillis-t1.getTimeInMillis)+" ms")

    /** Generates the clusters of attributes (attributes that are finished in the same bucket) */
    val partialClusters = attributesPerBucket


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
      if(separateAttributes && separatorID >= 0){
        val attrPartitioned = clusterElements.partition(_.startsWith(Settings.FIRST_DATASET_PREFIX))
        (for (e1<-attrPartitioned._1; e2<-attrPartitioned._2) yield (e1,(e2, calcSimilarity(attributeSignaturesBroadcast.value(e1), attributeSignaturesBroadcast.value(e2)))))
      }
      else{
        clusterElements.toList.combinations(2).map(x =>
          (x(0), (x(1), calcSimilarity(attributeSignaturesBroadcast.value(x(0)), attributeSignaturesBroadcast.value(x(1)))))
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
      ).groupByKey().map(x => (x._1, x._2.toSet))

    /** For each attribute keeps the attribute with the highest JS, and produce a cluster of elements (k1, k2) */
    val topEdges = edgesPerKey.map{case(key1, keys2) =>
      val max = keys2.map(_._2).max*maxFactor
      (key1, keys2.filter(_._2 >= max).map(_._1))
    }

    topEdges.count()
    val t4 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate top edges "+(t4.getTimeInMillis-t3.getTimeInMillis)+" ms")


    val graph = new SimpleGraph[String, DefaultEdge](classOf[DefaultEdge]);

    val vertices = topEdges.map(_._1).union(topEdges.flatMap(_._2)).distinct().collect()

    vertices.foreach{ v =>
      graph.addVertex(v)
    }
    topEdges.collect().foreach{ case(from, to) =>
      to.foreach{n =>
        graph.addEdge(from, n)
      }
    }

    attributeSignaturesBroadcast.unpersist()

    val ci =  new ConnectivityInspector(graph)

    val connectedComponents = ci.connectedSets()

    val clusters : Iterable[(Iterable[String], Int)] = (for(i <- 0 to connectedComponents.size()-1) yield{
      val a = connectedComponents.get(i).asInstanceOf[util.HashSet[String]].iterator()
      var l : List[String] = Nil
      while(a.hasNext){
        l = a.next() :: l
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
