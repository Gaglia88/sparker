package Experiments

/**
  * Created by Luca on 03/03/2017.
  */
package Experiments.NonCondivisi

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Luca on 03/02/2017.
  * Prova graphX con pregel
  */
object TestPregel {
  val initialMsg = -1.0

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]").set("spark.executor.memory", "10g")
    val sc = new SparkContext(conf)

    //Profili con i loro valori
    val profiles = (1L, 1.0) :: (2L, 2.0) :: (3L, 3.0) :: (4L, 4.0) :: (5L, 5.0) :: Nil
    //Vertici che collegano i profili
    val edges = Edge(1, 2, 1.0) :: Edge(1, 3, 1.0) :: Edge(1, 4, 1.0) :: Edge(4, 5, 1.0) :: Nil

    //Mi creo una struttura dove ho: (id vertice, (valore del vertice, messaggio ricevo nello step precedente))
    val v : RDD[(Long, (Double, Double))] = sc.parallelize(profiles).map(x => (x._1, (x._2, x._2)))
    val e = sc.parallelize(edges)

    val graph = Graph(v, e)

    /*
      Sommo ad un edge i valori di tutti i precedenti a lui collegati, in pratica alla fine ad esempio il vertice 6 dovrà
      contenere 60+30+10 = 100
    */
    val minGraph = graph.pregel(
      initialMsg,
      Int.MaxValue,
      EdgeDirection.Out)(
      receiveMsg,
      sendMsg,
      mergeMsg)


    //Stampo il risultato
    minGraph.vertices.map(x => (x._1, x._2._1)).collect().foreach(println)
  }

  /**
    * Riceve un messaggio in un vertice
    *
    * @param vertexId id del vertice
    * @param value valore attuale contenuto vertice, il primo campo è la somma di tutti i valori ricevuti, il secondo è l'ultimo valore ricevuto
    * @param message valore che sta ricevendo
    * @return (nuovo valore del vertice, ultimo valore ricevuto)
    * */
  def receiveMsg(vertexId: VertexId, value: (Double, Double), message: Double): (Double, Double) = {
    @transient lazy val log = org.apache.log4j.LogManager.getLogger("")
    log.info("Il nodo "+vertexId+" riceve il valore "+message)
    if (message == initialMsg)
      value
    else
      (message, message)
  }

  /**
    * Invia un messaggio
    * @param triplet tripla che contiene (id vertice sorgente, valore interno sorgente, id vertice destinazione, valore interno destinazione)
    * @return elenco di messaggi da mandare (id vertice destinatario, valore da inviare)
    * */
  def sendMsg(triplet: EdgeTriplet[(Double, Double), Double]): Iterator[(VertexId, Double)] = {
    @transient lazy val log = org.apache.log4j.LogManager.getLogger("")
    log.info("Dal nodo "+triplet.srcId+" invio il valore "+triplet.srcAttr+" al nodo "+triplet.dstId)
    Iterator((triplet.dstId, triplet.srcAttr._2))
  }

  /**
    * Unisce più messaggi, viene usato nel caso in cui ad un certo step
    * due vertici mandano un messaggio allo stesso destinatario.
    * */
  def mergeMsg(msg1: Double, msg2: Double): Double = {
    @transient lazy val log = org.apache.log4j.LogManager.getLogger("")
    log.info("Unisco i messaggi "+msg1+" "+msg2)
    msg1
  }
}

