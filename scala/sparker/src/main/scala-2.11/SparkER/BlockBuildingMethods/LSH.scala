package SparkER.BlockBuildingMethods

import java.util
import java.util.Calendar

import SparkER.DataStructures._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.jgrapht.alg.ConnectivityInspector
import org.jgrapht.graph.{DefaultEdge, SimpleGraph}

import scala.util.Random

/**
  * Created by Luca on 23/08/2017.
  */
object LSH {

  /** Settings */
  object Settings {
    /** Name for the default cluster */
    val DEFAULT_CLUSTER_NAME = "tuttiTokenNonNeiCluster"
    /** First pregel msg */
    val INITIAL_MSG = -1.0
    /** Name of the separator */
    val SOURCE_NAME_SEPARATOR = "_"
  }

  def getHashes(startingHash: Int, count: Int, seed: Int = 1234): Array[Int] = {
    val hashCodes = Array.ofDim[Int](count)
    val machineWordSize = 32
    val hashCodeSize = machineWordSize / 2
    val hashCodeSizeDiff = machineWordSize - hashCodeSize
    val hstart = startingHash
    val bmax = 1 << hashCodeSizeDiff
    val rnd = new Random(seed)

    for (i <- 0 until count) {
      hashCodes.update(i, ((hstart * (i * 2 + 1)) + rnd.nextInt(bmax)) >> hashCodeSizeDiff)
    }
    hashCodes
  }

  def getHashes2(strHash: Int, numHashes: Int, seed: Int = 1234): Array[Int] = {
    val rnd = new Random(seed)
    val hashes = for (i <- 0 until numHashes) yield {
      val a: Int = 1 + rnd.nextInt()
      val b: Int = rnd.nextInt()
      (((a.toLong * strHash.toLong + b.toLong) % 2147495899L) % Integer.MAX_VALUE).toInt
    }
    hashes.toArray
  }


  def getHashCode(str: String): Int = {
    str.hashCode() & Integer.MAX_VALUE
  }

  def getNumBands(targetThreshold: Double, sigNum: Int): Int = {
    var b = sigNum

    def r = sigNum.toDouble / b

    def t = Math.pow(1.0 / b, 1.0 / r)

    while (t < targetThreshold && b > 1) {
      b -= 1
    }
    b + 1
  }

  def getNumRows(targetThreshold: Double, sigNum: Int): Int = {
    val bands = getNumBands(targetThreshold, sigNum)
    val nrows = sigNum / bands
    if(nrows < 1){
      1
    }
    else{
      nrows
    }
  }

  def calcSimilarity(sig1: Array[Int], sig2: Array[Int]): Double = {
    //val common = sig1.intersect(sig2).length
    //common.toDouble/(sig1.length+sig2.length-common).toDouble
    var common: Double = 0
    for (i <- sig1.indices if sig1(i) == sig2(i)) {
      common += 1
    }
    common / sig1.length.toDouble
  }

  case class Attr(sourceName: Int, attribute: String)

  def clusterSimilarAttributes(profiles: RDD[Profile], numHashes: Int, targetThreshold: Double, maxFactor: Double, numBands: Int = -1, keysToExclude: Iterable[String] = Nil, computeEntropy: Boolean = false, separator: String = Settings.SOURCE_NAME_SEPARATOR): List[KeysCluster] = {
    @transient lazy val log = org.apache.log4j.LogManager.getRootLogger

    val t0 = Calendar.getInstance()

    /**
      * Per ogni attributo splitta i valori secondo lo splitter di default.
      * Per ogni token ottenuto dallo splitting ritorna ((nome sorgente, attributo), token)
      **/
    val attributesToken: RDD[(Attr, String)] = profiles.flatMap {
      profile =>
        val attributes = profile.attributes.filter(kv => !keysToExclude.exists(_.equals(kv.key)))
        attributes.flatMap {
          kv =>
            kv.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).filter(_.trim.nonEmpty).map(_.toLowerCase).map(token => (Attr(profile.sourceId, kv.key), token))
        }
    }

    /**
      * Inverte la coppia (attributo, token).
      * Raggruppa per token ottenendo (token, [elenco attributi])
      * Dà un id univoco ad ogni token
      * Restituisce (id univoco, [elenco attributi])
      **/
    val attributesPerToken: RDD[(Int, Iterable[Attr])] = attributesToken.map(_.swap).groupByKey().zipWithIndex().map(x => (x._2.toInt, x._1._2))

    val sc = SparkContext.getOrCreate()

    /**
      * Genera gli hash per ogni token e li invia in broadcast
      **/
    val hashes2 = sc.broadcast(attributesPerToken.map { case (tokenID, dataprofiles) =>
      val hashes = getHashes2(tokenID, numHashes)
      (tokenID, hashes)
    }.collectAsMap())

    /**
      * Raccoglie per ogni attributo gli id dei token che contiene, si ottiene quindi (attributo, [elenco token])
      **/
    val tokensPerAttribute: RDD[(Attr, Set[Int])] = attributesPerToken.flatMap { case (tokenID, attributes) =>
      attributes.map(p => (p, tokenID))
    }.groupByKey().map(x => (x._1, x._2.toSet))

    /**
      * Estrae tutti gli attributi esistenti (mi serve per dopo)
      **/
    val allAttributes = tokensPerAttribute.map(_._1)

    /**
      * Genera la signature per ogni attributo
      **/
    val attributeWithSignature: RDD[(Attr, Array[Int])] = tokensPerAttribute.map { case (attribute, tokens) =>

      val signature = Array.fill[Int](numHashes) {
        Int.MaxValue
      }

      tokens.foreach { t =>
        val h = hashes2.value(t)
        for (i <- h.indices) {
          if (h(i) < signature(i)) {
            signature.update(i, h(i))
          }
        }
      }

      (attribute, signature)
    }

    log.info("SPARKER - Num bands " + getNumBands(targetThreshold, numHashes))

    val numRows = getNumRows(targetThreshold, numHashes)

    println("NUMERO DI RIGHE "+numRows)

    val buckets = attributeWithSignature.map { case (attribute, signature) =>
      val buckets = signature.sliding(numRows, numRows).map(_.toList.hashCode()).toIterable
      (attribute, buckets)
    }

    val attributesPerBucket = buckets.flatMap(x => x._2.map((_, x._1))).groupByKey().map(x => (x._1, x._2.toSet)).filter(x => x._2.size > 1 && x._2.size < 101).map(_._2).distinct()

    attributesPerBucket.count()

    val attributeSignatures = attributeWithSignature.collectAsMap()

    hashes2.unpersist()

    val t1 = Calendar.getInstance()
    log.info("SPARKER - Time to perform LSH " + (t1.getTimeInMillis - t0.getTimeInMillis) + " ms")

    val numbuckets = attributesPerBucket.count()
    val t2 = Calendar.getInstance()
    log.info("SPARKER - Number of buckets " + numbuckets)
    log.info("SPARKER - Time to calculate attributesPerBucket " + (t2.getTimeInMillis - t1.getTimeInMillis) + " ms")

    /** Generates the clusters of attributes (attributes that are finished in the same bucket) */
    val partialClusters = attributesPerBucket


    val attributeSignaturesBroadcast = sc.broadcast(attributeSignatures)

    /**
      * Generates edges between the different attributes in each cluster
      * Produces a list of (attr1, (attr2, JaccardSimilarity(attr1, attr2))
      **/
    val edges = partialClusters.flatMap { clusterElements =>

      /**
        * For each cluster divide the attributes by first/second datasets
        * This will produces two list, one with the attributes that belongs from the first dataset, and one with the
        * attributes that belongs from the second.
        **/
      clusterElements.toList.combinations(2).filter { x =>
        x.head.sourceName != x.last.sourceName
      }.map(x => (x.head, (x.last, calcSimilarity(attributeSignaturesBroadcast.value(x.head), attributeSignaturesBroadcast.value(x.last)))))
    }


    edges.count()
    val t3 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate edges " + (t3.getTimeInMillis - t2.getTimeInMillis) + " ms")


    /** Produces all the edges,
      * e.g if we have (attr1, (attr2, sim(a1,a2)) will also generates
      * (attr2, (attr1, sim(a1, a2))
      * Then groups the edges for the first attribute, this will produce
      * for each attribute a list of similar attributes
      * */
    val edgesPerKey =
      edges.union(
        edges.map { case (attr1, (attr2, sim)) =>
          (attr2, (attr1, sim))
        }
      ).groupByKey().map(x => (x._1, x._2.toSet))

    /** For each attribute keeps the attribute with the highest JS, and produce a cluster of elements (k1, k2) */
    val topEdges = edgesPerKey.map { case (key1, keys2) =>
      val max = keys2.map(_._2).max * maxFactor
      (key1, keys2.filter(_._2 >= max).map(_._1))
    }

    topEdges.count()
    val t4 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate top edges " + (t4.getTimeInMillis - t3.getTimeInMillis) + " ms")


    val graph = new SimpleGraph[Attr, DefaultEdge](classOf[DefaultEdge])

    val vertices = topEdges.map(_._1).union(topEdges.flatMap(_._2)).distinct().collect()

    vertices.foreach { v =>
      graph.addVertex(v)
    }
    topEdges.collect().foreach { case (from, to) =>
      to.foreach { n =>
        graph.addEdge(from, n)
      }
    }

    attributeSignaturesBroadcast.unpersist()

    val ci = new ConnectivityInspector(graph)

    val connectedComponents = ci.connectedSets()

    val clusters: Iterable[(Iterable[Attr], Int)] = (for (i <- 0 until connectedComponents.size()) yield {
      val a = connectedComponents.get(i).asInstanceOf[util.HashSet[Attr]].iterator()
      var l: List[Attr] = Nil
      while (a.hasNext) {
        l = a.next() :: l
      }
      (l, i)
    }).filter(_._1.nonEmpty)

    val t5 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate clusters " + (t5.getTimeInMillis - t4.getTimeInMillis) + " ms")
    attributeSignaturesBroadcast.destroy()


    /** Calculates the default cluster ID */
    val defaultClusterID = {
      if (clusters.isEmpty) {
        0
      }
      else {
        clusters.map(_._2).max + 1
      }
    }


    val clusteredAttributes = clusters.flatMap(_._1).toSet
    val nonClusteredAttributes = allAttributes.collect().filter(!clusteredAttributes.contains(_))


    if (computeEntropy) {
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
          val tokensCount = tokens.groupBy(x => x).map(x => x._2.size)
          val tokensP = tokensCount.map {
            tokenCount =>
              val p_i: Double = tokenCount / numberOfTokens
              p_i * (Math.log10(p_i) / Math.log10(2.0d))
          }

          val entropy = {
            if (normalizeEntropy) {
              -tokensP.sum / (Math.log10(numberOfTokens) / Math.log10(2.0d))
            }
            else {
              -tokensP.sum
            }
          }
          (attribute, entropy)
      }

      attributesToken.unpersist()

      log.info(entropyPerAttribute.collect().toList)


      /** Assign the tokens to each cluster */
      val entropyPerCluster = entropyPerAttribute.map {
        case (attribute, entropy) =>
          val clusterID = keyClusterMap.get(attribute) //Obain the cluster ID
          if (clusterID.isDefined) {
            //If is defined assigns the tokens to this cluster
            (clusterID.get, entropy)
          }
          else {
            //Otherwise the tokens will be assigned to the default cluster
            (defaultClusterID, entropy)
          }
      }.groupByKey().map(x => (x._1, x._2.sum / x._2.size))

      /** A map that contains the cluster entropy for each cluster id */
      val entropyMap = entropyPerCluster.collectAsMap()

      /** Entropy of the default cluster */
      val defaultEntropy = {
        val e = entropyMap.get(defaultClusterID)
        if (e.isDefined) {
          e.get
        }
        else {
          0.0
        }
      }

      /* Compose everything together */
      clusters.map {
        case (keys, clusterID) =>
          val entropy = {
            val e = entropyMap.get(clusterID)
            if (e.isDefined) {
              e.get
            }
            else {
              1
            }
          }
          KeysCluster(clusterID, keys.map(k => k.sourceName + separator + k.attribute).toList, entropy)
      }.toList ::: KeysCluster(defaultClusterID, nonClusteredAttributes.map(k => k.sourceName + separator + k.attribute).toList, defaultEntropy) :: Nil
    }
    else {
      clusters.map {
        case (keys, clusterID) =>
          KeysCluster(clusterID, keys.map(k => k.sourceName + separator + k.attribute).toList)
      }.toList ::: KeysCluster(defaultClusterID, nonClusteredAttributes.map(k => k.sourceName + separator + k.attribute).toList) :: Nil
    }
  }

  def clusterSimilarAttributesLivy(profiles: RDD[Profile], numHashes: Int, targetThreshold: Double, maxFactor: Double, numBands: Int = -1, keysToExclude: Iterable[String] = Nil, computeEntropy: Boolean = false, separator: String = Settings.SOURCE_NAME_SEPARATOR): (List[KeysCluster], Array[(String, Double)]) = {
    @transient lazy val log = org.apache.log4j.LogManager.getRootLogger

    val t0 = Calendar.getInstance()

    /**
      * Per ogni attributo splitta i valori secondo lo splitter di default.
      * Per ogni token ottenuto dallo splitting ritorna ((nome sorgente, attributo), token)
      **/
    val attributesToken: RDD[(Attr, String)] = profiles.flatMap {
      profile =>
        val attributes = profile.attributes.filter(kv => !keysToExclude.exists(_.equals(kv.key)))
        attributes.flatMap {
          kv =>
            kv.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).filter(_.trim.nonEmpty).map(_.toLowerCase).map(token => (Attr(profile.sourceId, kv.key), token))
        }
    }

    /**
      * Inverte la coppia (attributo, token).
      * Raggruppa per token ottenendo (token, [elenco attributi])
      * Dà un id univoco ad ogni token
      * Restituisce (id univoco, [elenco attributi])
      **/
    val attributesPerToken: RDD[(Int, Iterable[Attr])] = attributesToken.map(_.swap).groupByKey().zipWithIndex().map(x => (x._2.toInt, x._1._2))

    val sc = SparkContext.getOrCreate()

    /**
      * Genera gli hash per ogni token e li invia in broadcast
      **/
    val hashes2 = sc.broadcast(attributesPerToken.map { case (tokenID, dataprofiles) =>
      val hashes = getHashes2(tokenID, numHashes)
      (tokenID, hashes)
    }.collectAsMap())

    /**
      * Raccoglie per ogni attributo gli id dei token che contiene, si ottiene quindi (attributo, [elenco token])
      **/
    val tokensPerAttribute: RDD[(Attr, Set[Int])] = attributesPerToken.flatMap { case (tokenID, attributes) =>
      attributes.map(p => (p, tokenID))
    }.groupByKey().map(x => (x._1, x._2.toSet))

    /**
      * Estrae tutti gli attributi esistenti (mi serve per dopo)
      **/
    val allAttributes = tokensPerAttribute.map(_._1)

    /**
      * Genera la signature per ogni attributo
      **/
    val attributeWithSignature: RDD[(Attr, Array[Int])] = tokensPerAttribute.map { case (attribute, tokens) =>

      val signature = Array.fill[Int](numHashes) {
        Int.MaxValue
      }

      tokens.foreach { t =>
        val h = hashes2.value(t)
        for (i <- h.indices) {
          if (h(i) < signature(i)) {
            signature.update(i, h(i))
          }
        }
      }

      (attribute, signature)
    }

    log.info("SPARKER - Num bands " + getNumBands(targetThreshold, numHashes))

    val numRows = getNumRows(targetThreshold, numHashes)

    println("NUMERO DI RIGHE "+numRows)

    val buckets = attributeWithSignature.map { case (attribute, signature) =>
      val buckets = signature.sliding(numRows, numRows).map(_.toList.hashCode()).toIterable
      (attribute, buckets)
    }

    val attributesPerBucket = buckets.flatMap(x => x._2.map((_, x._1))).groupByKey().map(x => (x._1, x._2.toSet)).filter(x => x._2.size > 1 && x._2.size < 101).map(_._2).distinct()

    attributesPerBucket.count()

    val attributeSignatures = attributeWithSignature.collectAsMap()

    hashes2.unpersist()

    val t1 = Calendar.getInstance()
    log.info("SPARKER - Time to perform LSH " + (t1.getTimeInMillis - t0.getTimeInMillis) + " ms")

    val numbuckets = attributesPerBucket.count()
    val t2 = Calendar.getInstance()
    log.info("SPARKER - Number of buckets " + numbuckets)
    log.info("SPARKER - Time to calculate attributesPerBucket " + (t2.getTimeInMillis - t1.getTimeInMillis) + " ms")

    /** Generates the clusters of attributes (attributes that are finished in the same bucket) */
    val partialClusters = attributesPerBucket


    val attributeSignaturesBroadcast = sc.broadcast(attributeSignatures)

    /**
      * Generates edges between the different attributes in each cluster
      * Produces a list of (attr1, (attr2, JaccardSimilarity(attr1, attr2))
      **/
    val edges = partialClusters.flatMap { clusterElements =>

      /**
        * For each cluster divide the attributes by first/second datasets
        * This will produces two list, one with the attributes that belongs from the first dataset, and one with the
        * attributes that belongs from the second.
        **/
      clusterElements.toList.combinations(2).filter { x =>
        x.head.sourceName != x.last.sourceName
      }.map(x => (x.head, (x.last, calcSimilarity(attributeSignaturesBroadcast.value(x.head), attributeSignaturesBroadcast.value(x.last)))))
    }


    edges.count()
    val t3 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate edges " + (t3.getTimeInMillis - t2.getTimeInMillis) + " ms")


    /** Produces all the edges,
      * e.g if we have (attr1, (attr2, sim(a1,a2)) will also generates
      * (attr2, (attr1, sim(a1, a2))
      * Then groups the edges for the first attribute, this will produce
      * for each attribute a list of similar attributes
      * */
    val edgesPerKey =
      edges.union(
        edges.map { case (attr1, (attr2, sim)) =>
          (attr2, (attr1, sim))
        }
      ).groupByKey().map(x => (x._1, x._2.toSet))

    /** For each attribute keeps the attribute with the highest JS, and produce a cluster of elements (k1, k2) */
    val topEdges = edgesPerKey.map { case (key1, keys2) =>
      val max = keys2.map(_._2).max * maxFactor
      (key1, keys2.filter(_._2 >= max).map(_._1))
    }

    topEdges.count()
    val t4 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate top edges " + (t4.getTimeInMillis - t3.getTimeInMillis) + " ms")


    val graph = new SimpleGraph[Attr, DefaultEdge](classOf[DefaultEdge])

    val vertices = topEdges.map(_._1).union(topEdges.flatMap(_._2)).distinct().collect()

    vertices.foreach { v =>
      graph.addVertex(v)
    }
    topEdges.collect().foreach { case (from, to) =>
      to.foreach { n =>
        graph.addEdge(from, n)
      }
    }

    attributeSignaturesBroadcast.unpersist()

    val ci = new ConnectivityInspector(graph)

    val connectedComponents = ci.connectedSets()

    val clusters: Iterable[(Iterable[Attr], Int)] = (for (i <- 0 until connectedComponents.size()) yield {
      val a = connectedComponents.get(i).asInstanceOf[util.HashSet[Attr]].iterator()
      var l: List[Attr] = Nil
      while (a.hasNext) {
        l = a.next() :: l
      }
      (l, i)
    }).filter(_._1.nonEmpty)

    val t5 = Calendar.getInstance()
    log.info("SPARKER - Time to calculate clusters " + (t5.getTimeInMillis - t4.getTimeInMillis) + " ms")
    attributeSignaturesBroadcast.destroy()


    /** Calculates the default cluster ID */
    val defaultClusterID = {
      if (clusters.isEmpty) {
        0
      }
      else {
        clusters.map(_._2).max + 1
      }
    }


    val clusteredAttributes = clusters.flatMap(_._1).toSet
    val nonClusteredAttributes = allAttributes.collect().filter(!clusteredAttributes.contains(_))


    if (computeEntropy) {
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
          val tokensCount = tokens.groupBy(x => x).map(x => x._2.size)
          val tokensP = tokensCount.map {
            tokenCount =>
              val p_i: Double = tokenCount / numberOfTokens
              p_i * (Math.log10(p_i) / Math.log10(2.0d))
          }

          val entropy = {
            if (normalizeEntropy) {
              -tokensP.sum / (Math.log10(numberOfTokens) / Math.log10(2.0d))
            }
            else {
              -tokensP.sum
            }
          }
          (attribute, entropy)
      }

      attributesToken.unpersist()

      val entropies = entropyPerAttribute.map(x => (x._1.sourceName+separator+x._1.attribute, x._2)).collect()

      /** Assign the tokens to each cluster */
      val entropyPerCluster = entropyPerAttribute.map {
        case (attribute, entropy) =>
          val clusterID = keyClusterMap.get(attribute) //Obain the cluster ID
          if (clusterID.isDefined) {
            //If is defined assigns the tokens to this cluster
            (clusterID.get, entropy)
          }
          else {
            //Otherwise the tokens will be assigned to the default cluster
            (defaultClusterID, entropy)
          }
      }.groupByKey().map(x => (x._1, x._2.sum / x._2.size))

      /** A map that contains the cluster entropy for each cluster id */
      val entropyMap = entropyPerCluster.collectAsMap()

      /** Entropy of the default cluster */
      val defaultEntropy = {
        val e = entropyMap.get(defaultClusterID)
        if (e.isDefined) {
          e.get
        }
        else {
          0.0
        }
      }

      /* Compose everything together */
      val clustersF = clusters.map {
        case (keys, clusterID) =>
          val entropy = {
            val e = entropyMap.get(clusterID)
            if (e.isDefined) {
              e.get
            }
            else {
              1
            }
          }
          KeysCluster(clusterID, keys.map(k => k.sourceName + separator + k.attribute).toList, entropy)
      }.toList ::: KeysCluster(defaultClusterID, nonClusteredAttributes.map(k => k.sourceName + separator + k.attribute).toList, defaultEntropy) :: Nil

      (clustersF, entropies)
    }
    else {
      val clustersF = clusters.map {
        case (keys, clusterID) =>
          KeysCluster(clusterID, keys.map(k => k.sourceName + separator + k.attribute).toList)
      }.toList ::: KeysCluster(defaultClusterID, nonClusteredAttributes.map(k => k.sourceName + separator + k.attribute).toList) :: Nil

      (clustersF, Array.empty[(String, Double)])
    }
  }

}
