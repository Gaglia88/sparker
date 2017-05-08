package Experiments

import java.util.Calendar

import DataStructures.{KeyValue, MatchingEntities, Profile}
import org.apache.log4j.LogManager
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}



/**
  * Created by Luca on 01/03/2017.
  */
object TestRDF2 {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.executor.memory", "15g")
      .set("spark.network.timeout", "10000s")
      .set("spark.executor.heartbeatInterval", "40s")
      .set("spark.default.parallelism", "4")
      .set("spark.executor.extraJavaOptions", "-Xss5g")
      .set("spark.local.dir", "/data2/sparkTmp")
      .set("spark.driver.maxResultSize", "100g")

    val sc = new SparkContext(conf)

    val log = LogManager.getRootLogger

    val startTime = Calendar.getInstance();
    println("INIZIO CARICAMENTO DELLE TRIPLE")
    val data = sc.textFile("C:/Users/Luca/Desktop/dbpedia.txt")
    val trueTriples = data.filter(x => x.startsWith("<"))

    val pattern = """<(.+?)>\s<(.+?)>\s(.+?)\s<(.+?)>\s.""".r
    val empty = ("", "", "")

    val lines = trueTriples.map(string =>
      string match {
        case pattern(s, p, o, c) =>
          if(o.startsWith("_:")){
            empty
          }
          else{
            var obj = o.trim
            if(obj.startsWith("<")){
              obj = obj.drop(1)
            }
            if(obj.endsWith(">")){
              obj = obj.dropRight(1)
            }
            (s, p, obj)
          }
        case _ =>
          empty
      }
    ).filter(_ != empty)

    lines.cache()
    val num = lines.count()
    val loadTime = Calendar.getInstance();
    println("CARICAMENTO TERMINATO")
    println("NUMERO LINEE "+num)
    println("TEMPO CARICAMENTO "+(loadTime.getTimeInMillis-startTime.getTimeInMillis)+" ms")


    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val linesDF = lines.toDF("id", "p", "o")

    val t = linesDF.groupBy("id").pivot("p").agg(org.apache.spark.sql.functions.collect_list("o"))
    val a = t.filter{r =>
      r.getAs[scala.collection.mutable.WrappedArray[String]]("http://www.w3.org/2002/07/owl#sameAs").exists(_.startsWith("http://rdf.freebase.com"))
    }

    val c = a.rdd.map   {r =>
      val a = for(i <- 0 to r.size-1) yield {
        if(i > 0){
          r.getAs[scala.collection.mutable.WrappedArray[String]](i).mkString("||")
        }
        else{
          r.getAs[String](0)
        }
      }
      a.map{str =>
        "\""+str.replace("\"", " ").trim+"\""
      }.mkString(",")
    }

    val h : List[String] = a.columns.map(_.replace(",", "")).mkString(",") :: Nil
    val head = sc.parallelize(h)

    head.union(c).repartition(1).saveAsTextFile("C:/Users/Luca/Desktop/outcsv")

    //c.collect().foreach(println)

    //c.toDF().show(false)

    //a.write.format("com.databricks.spark.csv").save("C:/Users/Luca/Desktop/datiOut.csv")

    /*def toTuple[A <: Object](as:List[A]):Product = {
      val tupleClass = Class.forName("scala.Tuple" + as.size)
      tupleClass.getConstructors.apply(0).newInstance(as:_*).asInstanceOf[Product]
    }*/
  }
}
