package BlockBuildingMethods

import java.util
import java.util.Calendar

import BlockBuildingMethods.LSHTwitter.Settings
import DataStructures._
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.jgrapht.alg.ConnectivityInspector
import org.jgrapht.graph.{DefaultEdge, SimpleGraph}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.types._

/**
  * Created by Luca on 10/01/2017.
  */
object LSHSpark {

  def createBlocks(profiles: RDD[Profile], numHashes: Int, threshold: Double, separatorID: Long = -1, keysToExclude: Iterable[String] = Nil): Set[Long] = {
    val sqlContext = SparkSession.builder.getOrCreate()
    import sqlContext.implicits._


    val profileTokens = profiles.map{profile =>
      val attributes = profile.attributes.filter(kv => !keysToExclude.exists(_.equals(kv.key)))
      val tokens = attributes.flatMap(_.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING)).filter(_.trim.size > 0).distinct
      (profile.id, tokens)
    }

    val df = profileTokens.map(p => (p._1, p._2.toArray)).toDF("id","tokens")
    val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("tokens").setOutputCol("features").fit(df)
    val isNoneZeroVector = udf({v: Vector => v.numNonzeros > 0}, DataTypes.BooleanType)
    val vectorizedDf = cvModel.transform(df).filter(isNoneZeroVector(col("features"))).select(col("id"), col("features"))

    //val mh = new MinHashLSH().setNumHashTables(numHashes).setInputCol("features").setOutputCol("hashValues")
    val mh = new BucketedRandomProjectionLSH().setBucketLength(2.0).setInputCol("features").setOutputCol("hashValues")

    val model = mh.fit(vectorizedDf)


    val a = model.approxSimilarityJoin(vectorizedDf, vectorizedDf, threshold).filter("distCol != 0").selectExpr("datasetA.id", "datasetB.id AS id2").rdd.map(row => (row.getAs[Long](0), row.getAs[Long](1)))

    a.filter(x => ((x._1 > separatorID && x._2 <= separatorID) || (x._1 <= separatorID && x._2 > separatorID))).flatMap(x => List(x._1, x._2)).collect().toSet
  }

  def clusterSimilarAttributes(profiles: RDD[Profile], numHashes : Int, threshold : Double, separatorID: Long = -1): List[KeysCluster] = {
    val sqlContext = SparkSession.builder.getOrCreate()
    import sqlContext.implicits._


    val attributesToken: RDD[(String, String)] = profiles.flatMap {
      profile =>
        val dataset = BlockingUtils.getPrefix(profile.id, separatorID)
        val attributes = profile.attributes
        /* Tokenize the values of the keeped attributes, then for each token emits (dataset + key, token) */
        attributes.flatMap {
          kv =>
            kv.value.split(BlockingUtils.TokenizerPattern.DEFAULT_SPLITTING).filter(_.trim.size > 0).map((dataset + kv.key, _))
        }
    }

    val df = attributesToken.groupByKey().map(p => (p._1, p._2.toArray)).toDF("id","words")
    val cvModel: CountVectorizerModel = new CountVectorizer().setInputCol("words").setOutputCol("features").fit(df)
    val isNoneZeroVector = udf({v: Vector => v.numNonzeros > 0}, DataTypes.BooleanType)
    val vectorizedDf = cvModel.transform(df).filter(isNoneZeroVector(col("features"))).select(col("id"), col("features"))

    val mh = new MinHashLSH().setNumHashTables(numHashes).setInputCol("features").setOutputCol("hashValues")
    val model = mh.fit(vectorizedDf)

    val edges = {
      //val e = model.approxSimilarityJoin(vectorizedDf, vectorizedDf, threshold).filter("distCol != 0").selectExpr("datasetA.id", "datasetB.id AS id2", "distCol AS w").rdd.map(row => (row.getAs[String](0), (row.getAs[String](1), row.getAs[Double](2))))
      val e = model.approxSimilarityJoin(vectorizedDf, vectorizedDf, threshold).filter("distCol != 0").selectExpr("datasetA.id", "datasetB.id AS id2", "distCol AS w").rdd.map(row => (row.getAs[String](0), (row.getAs[String](1), row.getAs[Double](2))))
      if(separatorID < 0){
        e
      }
      else{
        e.filter{ case (attr1, (attr2, sim)) =>
          (attr1.startsWith(Settings.FIRST_DATASET_PREFIX) && attr2.startsWith(Settings.SECOND_DATASET_PREFIX)) ||
            (attr2.startsWith(Settings.FIRST_DATASET_PREFIX) && attr1.startsWith(Settings.SECOND_DATASET_PREFIX))
        }
      }
    }

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

    val graph = new SimpleGraph[String, DefaultEdge](classOf[DefaultEdge]);

    val vertices = topEdges.map(_._1).union(topEdges.flatMap(_._2)).distinct().collect()

    vertices.foreach{ v =>
      graph.addVertex(v)
    }
    topEdges.collect().foreach{ case(from, to) =>
      to.foreach{ t =>
        graph.addEdge(from, t)
      }
    }

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

    topEdges.count()
    val t4 = Calendar.getInstance()

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
