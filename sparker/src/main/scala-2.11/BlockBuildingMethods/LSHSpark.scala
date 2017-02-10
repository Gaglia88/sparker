package BlockBuildingMethods

import DataStructures.{BlockAbstract, BlockClean, BlockDirty, Profile}
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

/**
  * Created by Luca on 10/01/2017.
  */
object LSHSpark {
  /**
    * Crea i blocchi
    * */
  def createBlocks(profiles: RDD[Profile], numHashes : Int, numBands : Int, separatorID: Long = -1): RDD[BlockAbstract] = {
    val sqlContext = SparkSession.builder.getOrCreate()
    import sqlContext.implicits._

    //Numero elementi per banda
    val numElementsPerBand = Math.ceil(numHashes/numBands).toInt

    val sparseVectorData = LSHSpark.createSparseVector(profiles)

    val df = sparseVectorData.toDF("id", "keys")

    //Inizializzo lsh
    val lsh = new MinHashLSH()
      .setNumHashTables(numHashes)
      .setInputCol("keys")
      .setOutputCol("hashes")

    //Genero il modello
    val LSHmodel = lsh.fit(df)

    //Aggiunge al DF una colonna con gli hash, alla fine ho un DF con due colonne: id profilo, hashes
    val profileWithHashes = LSHmodel.transform(df).select("id", "hashes")

    //Trasformo il dataframe
    val profileWithBuckets = profileWithHashes.map{
      case Row(id : Long, hash : mutable.WrappedArray[org.apache.spark.ml.linalg.Vector]) =>
        /*
        * Il primo map mette tutti gli elementi in un'unica lista, perché prima era un array di array.
        * La funzione grouped trasforma la lista in un elenco di liste in modo da avere il numero di
        * bande richieste.
        * Es se ho 4 hash e vengono richieste 2 bande: [h1, h2, h3, h4] => [[h1, h2], [h3, h4]]
        * Infine faccio un map in modo da ottenere l'hashcode di una lista, due liste uguali
        * avranno lo stesso hash.
        * */
        val buckets = hash.map(x => x(0)).grouped(numElementsPerBand).toList.map(_.hashCode())
        (id, buckets)
    }.toDF("id", "buckets")

    //Converto il dataframe in rdd
    val profileWithBucketsRDD = profileWithBuckets.rdd.map(r => (r.getLong(0), r.getAs[Iterable[Int]](1)))

    //Raggruppo i profili per il bucket, rimuovo i bucket con 1 solo profilo e poi genero i blocchi
    profileWithBucketsRDD.flatMap(x => x._2.map((_, x._1))).groupByKey().filter(_._2.size > 1).map{
      x =>
        if(separatorID < 0){
         BlockDirty(x._1, (x._2.toList, Nil))
        }
        else {
          BlockClean(x._1, x._2.toList.partition(_ <= separatorID))
        }
    }.filter(_.getComparisonSize() > 0).map(x => x)
  }

  /**
    * Dato un RDD di profili lo trasforma in uno sparse vector
    * */
  def createSparseVector(profiles : RDD[Profile]) : RDD[(Long, SparseVector)] = {
    /** Genero tutti i token e gli assegno un id univoco */
    val tokens = profiles.flatMap {
      profile =>
        profile.attributes.flatMap {
          kv =>
            kv.value.split("\\W+").map {
              token =>
                token
            }
        }
    }.distinct().filter(_.trim.length > 0).zipWithIndex()

    /** Estraggo ID token massimo */
    val maxTokenID = (tokens.map(_._2).max() + 1).toInt

    /** Emetto una coppia (token, id profilo) per ogni token ed ogni profilo */
    val profileWithTokens = profiles.flatMap {
      profile =>
        profile.attributes.flatMap {
          kv =>
            kv.value.split("\\W+").map {
              token =>
                (token, profile.id)
            }
        }
    }.distinct()

    /**
      * Genera le feature
      * (id profilo, [(id token, 1.0), (id token, 1.0), ...])
      **/
    val features = profileWithTokens.join(tokens).map {
      x =>
        val tokenID = x._2._2.toInt
        val profileId = x._2._1

        (profileId, tokenID)
    }.distinct().map(x => (x._1, (x._2, 1.toDouble))).groupByKey()

    /**
      * Converto le feature in uno sparsevector
      **/
    val sparseVectorData = features.map {
      x =>
        (x._1, Vectors.sparse(maxTokenID, x._2.toSeq).asInstanceOf[SparseVector])
    }

    sparseVectorData
  }
}
