package Experiments

import java.util.Calendar

import BlockBuildingMethods.LSHTwitter3
import DataStructures.MatchingEntities
import Wrappers.CSVWrapper
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.control.Breaks._

/**
  * Created by Luca on 24/02/2017.
  */
object Test2 {
  def main(args: Array[String]) {
    val purgingRatio = 1.000
    val filteringRatio = 0.6
    val ngramSize = 4

    /*val memoryHeap = args(0)
    val memoryStack = args(1)
    val pathDataset1 = args(2)
    val pathDataset2 = args(3)
    val pathGt = args(4)*/



    val memoryHeap = 15
    val memoryStack = 5
    val pathDataset1 = "C:\\Users\\Luca\\Downloads\\citations\\citations\\citeseer.csv"
    val pathDataset2 = "C:\\Users\\Luca\\Downloads\\citations\\citations\\dblp.csv"
    val pathGt = "C:\\Users\\Luca\\Downloads\\citations\\citations"


    println("Heap " + memoryHeap + "g")
    println("Stack " + memoryStack + "g")
    println("First dataset path " + pathDataset1)
    println("Second dataset path " + pathDataset2)
    println("Groundtruth path " + pathGt)
    println()


    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.executor.memory", memoryHeap + "g")
      .set("spark.network.timeout", "10000s")
      .set("spark.executor.heartbeatInterval", "40s")
      .set("spark.default.parallelism", "32")
      .set("spark.driver.maxResultSize", "10g")
      .set("spark.executor.extraJavaOptions", "-Xss" + memoryStack + "g")
      .set("spark.local.dir", "/data2/sparkTmp/")

    val sc = new SparkContext(conf)

    //val baseDir = "C:\\Users\\Luca\\Desktop\\Nuova cartella (2)\\spark_sbt_project\\logs\\songs\\"
    val baseDir = "C:\\Users\\Luca\\Desktop\\"
    val a = sc.textFile(baseDir+"WNP_CBS_AVG_NO-SCHEMA1.txt")
    val b = a.filter(_.startsWith("INFO - SPARKER"))
    b.repartition(1).saveAsTextFile(baseDir+"ok")

    /*
    println("Start to loading profiles")

    val dbpedia = sc.textFile("C:/Users/Luca/Desktop/clusters_dbpedia.txt")
    val clusters = dbpedia.map{c =>
      val a = c.replace("List", "").replace("(", "").replace(")", "").split(",").toList.map(_.trim)
      (a(0), a(1))
    }

    val c1 = clusters.map(_.swap).groupByKey()//.map(x => x._1 :: x._2.toList)

    LSHTwitter3.clustersTransitiveClosure(c1).foreach(println)*/


    /*

    def cluster1 = "a" :: "b" :: Nil
    def cluster2 = "c" :: "b" :: Nil
    def cluster3 = "d" :: "e" :: Nil
    def cluster4 = "e" :: "f" :: Nil
    def cluster5 = "g" :: "h" :: Nil

    val lst = cluster1 :: cluster2 :: cluster3 :: cluster4 :: cluster5 :: Nil

    val test = sc.parallelize(lst)


    val result = c1.aggregate(new mutable.MutableList[mutable.HashSet[String]])(seqOp, combOp)


    result.foreach(println)

    /*
    val startTime = Calendar.getInstance();
    val dataset1 = CSVWrapper.loadProfiles(filePath = pathDataset1, header = true, realIDField = "id") //SerializedObjectLoader.loadProfiles(filePath = pathDataset1, realFieldID = "id")
    val separatorID = dataset1.map(_.id).max()
    val dataset2 = CSVWrapper.loadProfiles(filePath = pathDataset2, startIDFrom = separatorID + 1, header = true, realIDField = "id") //SerializedObjectLoader.loadProfiles(filePath = pathDataset2, realFieldID = "id", startIDFrom = separatorID+1)
    val maxProfileID = dataset2.map(_.id).max()

    val profiles = dataset1.union(dataset2)
    profiles.cache()
    val numProfiles = profiles.count()

    val a = sc.broadcast(profiles.map(p => (p.id, p.originalID)).collectAsMap())

    val matchFound = loadGT("C:/Users/Luca/Desktop/part-00000")
    matchFound.map(m => (a.value(m.firstEntityID.toLong), a.value(m.secondEntityID.toLong))).repartition(1).saveAsTextFile("C:/Users/Luca/Desktop/gta")*/*/
  }

  def seqOp(results : mutable.MutableList[mutable.HashSet[String]], in : List[String]) : mutable.MutableList[mutable.HashSet[String]] = {
    val cluster = mutable.HashSet(in: _*)
    results += cluster
    results
  }

  def combOp(clusters : mutable.MutableList[mutable.HashSet[String]], results : mutable.MutableList[mutable.HashSet[String]]) : mutable.MutableList[mutable.HashSet[String]] = {
    var inserted : Boolean = false

    for(i <- 0 to clusters.size-1){//Scorro tutti i cluster
      inserted = false//Resetto inserted
      val cluster = clusters(i)
      breakable{
        for(j <- 0 to results.size-1){//Scorro tutti i cluster dei risultati
          if(results(j).exists(e => cluster.contains(e))){//Se il cluster i-mo e il cluster j-mo del risultato hanno qualcosa in comune
            results(j) ++= cluster//Inserisco nel cluster j-mo dei risultati gli elementi del cluster i-mo
            inserted = true//Segnalo che ho inserito gli elementi del cluster i-mo in un cluster già esistente
            break//Mi fermo perché ho trovato quello in cui va
          }
        }
      }
      if(!inserted){//Se il cluster i-mo non è stato inserito tra quelli del risultato, allora è un nuovo cluster univoco e lo inserisco tra i risultati
        results += cluster
      }
    }

    results
  }

  def loadGT(path : String) : RDD[MatchingEntities] = {
    val sc = SparkContext.getOrCreate()
    val gt = sc.textFile(path)
    gt.map{
      row =>
        val keys = row.split(",")
        MatchingEntities(keys(0), keys(1))
    }
  }
}
