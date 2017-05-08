package Experiments

import BlockBuildingMethods.LSHTwitter3
import BlockRefinementMethods.PruningMethods.CNPFor.NeighbourWithWeight
import DataStructures.KeysCluster
import Utilities.BoundedPriorityQueue
import Wrappers.{CSVWrapper, JsonRDDWrapper}
import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by Luca on 16/02/2017.
  */
object Test {
  def main(args: Array[String]) {
    /*
    val c = List(KeysCluster(0,List("d_2_starring", "d_1_actor name"),5.025564082512673E-6), KeysCluster(1,List("d_2_title", "d_1_title"),5.361338295373993E-5),
      KeysCluster(2,List("d_1_director name", "d_2_writer"),5.3570230016185796E-5), KeysCluster(3,List("tuttiTokenNonNeiCluster"),2.5389929205080563E-5))

    c.foreach{e =>
      println(e.entropy)
      println(Math.pow(1000000000, e.entropy*10000))
      println()
    }*/


    /*
    val t = NeighbourWithWeight(10, 0.4.toFloat) :: NeighbourWithWeight(10, 0.1.toFloat) :: NeighbourWithWeight(10, 0.2.toFloat) :: Nil


    val q = new BoundedPriorityQueue[NeighbourWithWeight](5)

    q ++= t

    println(t.sorted)
    println(q.toArray.size)
    */
    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.executor.memory", "15g")
      .set("spark.network.timeout", "10000s")
      .set("spark.executor.heartbeatInterval", "40s")
      .set("spark.default.parallelism", "32")
      .set("spark.executor.extraJavaOptions", "-Xss5g")
      .set("spark.local.dir", "/data2/sparkTmp/")
      .set("spark.driver.maxResultSize", "10g")

    val sc = new SparkContext(conf)


    val a = sc.textFile("C:/Users/Luca/Desktop/WNP_CBS_AVG_SCHEMA_ENTROPY.txt")

    /*val soglie = a.filter(_.startsWith("INFO - soglia - ")).map{x =>
      x.replace("INFO - soglia - ", "").toDouble
    }.filter(x => !x.isNaN)


    val valori = a.filter(_.startsWith("INFO - w - ")).map(_.replace("INFO - w - ", "").toDouble).filter(!_.isNaN)

    val avg = soglie.mean()
    valori.map{v =>
      /*val normalizedValue = (v/avg)*10
      (Math.round(normalizedValue)/10, 1)*/
      (v, 1)
    }.groupByKey().map(x => (x._1, x._2.sum)).sortBy(_._1).collect().foreach(println)

    println("Soglia "+avg)*/

    val clustersMatch = a.filter(_.startsWith("INFO - cm - ")).flatMap(_.replace("INFO - cm - ", "").split(",").map(_.trim).filter(_.size > 0).map(_.toDouble).filter(!_.isNaN))
    val countMatch = clustersMatch.map(x => (x, 1)).groupByKey().map(x => (x._1, x._2.sum))
    countMatch.sortByKey().collect().foreach(println)

    println()

    val clustersNonMatch = a.filter(_.startsWith("INFO - cn - ")).flatMap(_.replace("INFO - cn - ", "").split(",").map(_.trim).filter(_.size > 0).map(_.toDouble).filter(!_.isNaN))
    val countNonMatch = clustersNonMatch.map(x => (x, 1)).groupByKey().map(x => (x._1, x._2.sum))
    countNonMatch.sortByKey().collect().foreach(println)

    /*
    val a = sc.textFile("C:/Users/Luca/Desktop/err.txt")
    val b = a.map{x =>
      var str = x
      while(str.contains("\\\\\"")){
        str = str.replace("\\\\\"", "\\\"")
      }
      str
    }
    b.repartition(1).saveAsTextFile("C:/Users/Luca/Desktop/dbpediamin")

    val p = JsonRDDWrapper.loadProfiles("C:/Users/Luca/Desktop/part-00000", 0, "realId")
    p.count()
*/


    /*
    val a = sc.textFile("/marconi_scratch/userexternal/lgaglia1/ER/datasets/dbpedia_freebase/DBpediaProfilesGT.json")
    val b = a.map{x =>
      x.replace("\\\\\"", "\\\"")
    }
    b.repartition(1).saveAsTextFile("/marconi_scratch/userexternal/lgaglia1/ER/datasets/dbpedia_freebase/DBpediaProfilesGTOK.json")

    val c = sc.textFile("/marconi_scratch/userexternal/lgaglia1/ER/datasets/dbpedia_freebase/FreebaseProfilesGT.json")
    val d = c.map{x =>
      x.replace("\\\\\"", "\\\"")
    }
    d.repartition(1).saveAsTextFile("/marconi_scratch/userexternal/lgaglia1/ER/datasets/dbpedia_freebase/FreebaseProfilesGTOK.json")
*/
    /*
    val gt = CSVWrapper.loadGroundtruth("C:/Users/Luca/Desktop/groundtruth_dbpedia_freebase.csv")
    val idDbpedia = sc.broadcast(sc.textFile("C:/Users/Luca/Desktop/idDBpdia/part-00000/").collect().toSet)
    val idFreebase = sc.broadcast(sc.textFile("C:/Users/Luca/Desktop/idFreebase/part-00000/").collect().toSet)

    val gtFiltered = gt.filter{m =>
      idDbpedia.value.contains(m.firstEntityID) && idFreebase.value.contains(m.secondEntityID)
    }

    gtFiltered.map{x =>
      val dbpedia = "\""+x.firstEntityID.replace("\"", "\\\"")+"\""
      val freebase = "\""+x.secondEntityID.replace("\"", "\\\"")+"\""
      dbpedia+","+freebase
    }.repartition(1).saveAsTextFile("C:/Users/Luca/Desktop/newGT")
    */

    /*val b = a.map{str =>
      val split = str.split(",")
      val dbpedia = "\""+split(0).trim+"\""
      val freebase = split(1).trim
      dbpedia+",\""+freebase.patch(freebase.lastIndexOf("/"), ".", 1)+"\""
    }
    b.repartition(1).saveAsTextFile("C:/Users/Luca/Desktop/gtfix")*/




    /*val a = sc.textFile("C:/Users/Luca/Desktop/logDbpedia6.txt")
    val pattern = """KeysCluster\((.+?),List\((.+?)\),(.+?)\)""".r
    val entropies = a.map{str =>
      str match{
        case pattern(id, elements, entropy) =>
          entropy.toDouble
        case _ =>
          -10.0
      }
    }.filter(_ > -9).map(Math.pow(10000000, _))

    println(entropies.stats())*/

    /*

    a.filter(_.startsWith("INFO - SPARKER")).repartition(1).saveAsTextFile("C:/Users/Luca/Desktop/Nuova cartella (2)/spark_sbt_project/logs/citations/OK")
*/


    /*
    val dati = a.filter(x => x.startsWith("List")).take(1)(0)

    val pattern = """List\((.+?)\)""".r
    val keys = pattern.findAllIn(dati).flatMap { lst =>
      val a : Iterable[String] = {
        if(lst.startsWith("List((List")){
          Nil
        }
        else{
          lst.replace("List", "").replace("(", "").replace(")", "").split(",").map(_.trim)
        }
      }
      a
    }.filter(_.size > 0)


    val keys = a.flatMap { lst =>
      val a : Iterable[String] = lst.replace("List", "").replace("(", "").replace(")", "").split(",").map(_.trim)
      a.slice(0, a.size-1)
    }.filter(_.size > 0)

    println(a.map((_, 1)).reduceByKey((x, y) => x+y).filter(_._2 > 1).count())

    val p1 : (Long, List[Long]) = (1, (Long.MaxValue-1 :: Long.MaxValue-2 :: 1.toLong :: 2.toLong :: 5.toLong :: 4.toLong :: 14.toLong :: Nil))
    val p2 : (Long, List[Long]) = (2, (Long.MaxValue-1 :: 1.toLong :: 2.toLong :: 3.toLong :: 4.toLong :: Nil))

    val l = p1 :: p2 :: Nil

    val m = l.map{
      case(profileID, blocks) =>
        var bitMap : Long = 0
        blocks.foreach{
          blockID =>
            bitMap |= (1 << blockID)
        }
        (profileID, bitMap)
    }

    m.foreach(println)

    val t1 = System.nanoTime()
    val commons = contTrueBits(m(0)._2 & m(1)._2).toFloat
    val js = commons / (contTrueBits(m(0)._2) + contTrueBits(m(1)._2) - commons)
    val t2 = System.nanoTime()

    val t3 = System.nanoTime()
    val commons2 = (p1._2.intersect(p2._2)).size.toFloat
    val js2 = commons2 / (p1._2.size + p2._2.size - commons2)
    val t4 = System.nanoTime()

    println("Dimensione elemento massimo "+(Long.MaxValue-1))
    println("Dimensione in memoria delle liste "+SizeEstimator.estimate(l) +" byte")
    println("Dimensione in memoria delle bitmask "+SizeEstimator.estimate(m)+" byte")
    println("JS Calcolata con bitmask "+js)
    println("Tempo di calcolo "+(t2 - t1)+ " ns")
    println("JS calcolata con liste "+js2)
    println("Tempo di calcolo "+(t4 - t3)+ " ns")*/
  }


  /**
    * Data una bitmask conta i bit posti ad 1
    * */
  def contTrueBits(n : Long) : Int = {
    var i = 0
    var tmp = n >>> i
    var cont = 0
    while(tmp > 0){
      cont += (tmp & 1).toInt
      i += 1
      tmp = n >>> i
    }
    cont
  }
}
