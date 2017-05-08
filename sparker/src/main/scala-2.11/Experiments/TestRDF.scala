package Experiments

import java.util.Calendar

import DataStructures.{KeyValue, MatchingEntities, Profile}
import Wrappers.{CSVWrapper, JsonRDDWrapper}
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Luca on 01/03/2017.
  */
object TestRDF {
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


    val basePath = "C:/Users/Luca/Desktop/"
    val log = LogManager.getRootLogger
    val pattern = """<(.+?)>\s<(.+?)>\s(.+?)\s<(.+?)>\s.""".r
    val empty = ("", ("", ""))

    val startTime = Calendar.getInstance();
    println("SPARKER - INIZIO CARICAMENTO DELLE TRIPLE")
    val data = sc.textFile(basePath+"freebase.txt")
    val trueTriples = data.filter(x => x.startsWith("<"))
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
            (s, (p, obj))
          }
        case _ =>
          empty
      }
    ).filter(_ != empty)
    lines.cache()
    val num = lines.count()
    val loadTime = Calendar.getInstance();
    println("SPARKER - CARICAMENTO TERMINATO")
    println("SPARKER - NUMERO LINEE "+num)
    println("SPARKER - TEMPO CARICAMENTO "+(loadTime.getTimeInMillis-startTime.getTimeInMillis)+" ms")


    println("SPARKER - INIZIO GROUP BY")
    val linesByID = lines.groupByKey()
    linesByID.cache()
    val linesGroupedNum = linesByID.count()
    lines.unpersist()
    val groupTime = Calendar.getInstance()
    println("SPARKER - TERMINATO GROUP BY "+linesGroupedNum)
    println("SPARKER - TEMPO GROUP BY "+(groupTime.getTimeInMillis-loadTime.getTimeInMillis)+" ms")


    /*
    println("SPARKER - ELIMINO PROFILI CHE NON CONTENGONO RIFERIMENTI A FREEBASE")
    val conWithFreebase = linesByID.filter(x => !x._2.filter(av => av._2.contains("freebase") && av._1.contains("sameAs")).isEmpty)
    conWithFreebase.cache()
    val numLinesAfterFilter = conWithFreebase.count()
    linesByID.unpersist()
    val filterTime = Calendar.getInstance()
    println("SPARKER - TERMINATA PULIZIA "+numLinesAfterFilter)
    println("SPARKER - TEMPO FILTERING "+(filterTime.getTimeInMillis-groupTime.getTimeInMillis)+" ms")


    println("SPARKER - GENERO GROUNDTRUTH")
    val groundtruth = conWithFreebase.map{x =>
      val a = x._2.filter(av => av._2.contains("freebase") && av._1.contains("sameAs"))
      x._1+","+a.head._2
    }
    groundtruth.repartition(1).saveAsTextFile(basePath+"groundtruth.csv")
    groundtruth.unpersist()
    val gtTime = Calendar.getInstance()
    println("SPARKER - TERMINATA GENERAZIONE")
    println("SPARKER - TEMPO GENERAZIONE GT "+(gtTime.getTimeInMillis-filterTime.getTimeInMillis)+" ms")
    */

    log.info("SPARKER - MANTENGO SOLO I PROFILI CONTENUTI IN FREEBASE CHE SONO ANCHE NEL GROUNDTRUTH")
    val gt = sc.textFile(basePath+"groundtruth.csv").map{str =>
      str.split(",")(1).trim.toLowerCase
    }
    gt.collect().foreach(log.info)
    val freebaseIDs = sc.broadcast(gt.collect().toSet)
    val conWithFreebase = linesByID.filter(x => freebaseIDs.value.contains(x._1.trim.toLowerCase))
    conWithFreebase.cache()
    val numLinesAfterFilter = conWithFreebase.count()
    linesByID.unpersist()
    val filterTime = Calendar.getInstance()
    log.info("SPARKER - TERMINATA PULIZIA "+numLinesAfterFilter)
    log.info("SPARKER - TEMPO FILTERING "+(filterTime.getTimeInMillis-groupTime.getTimeInMillis)+" ms")

    val profilesJSON = conWithFreebase.map(profileToJSON2)
    profilesJSON.repartition(1).saveAsTextFile(basePath+"DBpediaProfiles.json")
    val profilesTime = Calendar.getInstance()
    println("SPARKER - TERMINATA GENERAZIONE PROFILI")
    println("SPARKER - TEMPO GENERAZIONE PROFILI "+(profilesTime.getTimeInMillis-filterTime.getTimeInMillis)+" ms")

    /*
    println("SPARKER - GENERO I PROFILI")
    val profiles = conWithFreebase.zipWithIndex().map{
      case ((realID, attributes), id) =>
        val profile = Profile(id = id, originalID = realID)
        attributes.foreach{
          case(attribute, value) =>
            profile.attributes += KeyValue(attribute, value)
        }
        profile
    }
    profiles.cache()
    val numProfiles = profiles.count()
    val maxProfileID = profiles.map(_.id).max()
    val a = profiles.collect()
    val profilesTime = Calendar.getInstance()
    println("SPARKER - TERMINATA GENERAZIONE PROFILI")
    println("SPARKER - MAX PROFILE ID "+maxProfileID)
    println("SPARKER - TEMPO GENERAZIONE GT "+(profilesTime.getTimeInMillis-gtTime.getTimeInMillis)+" ms")


    val profiles2 = CSVWrapper.loadProfiles(filePath = "C:/Users/Luca/Desktop/outcsv/part-00000", header = true, explodeInnerFields = true, innerSeparator = "\\|\\|", realIDField = "id")
    val b = profiles2.collect()


    a.foreach(println)
    b.foreach(println)
    */

    val profiles = JsonRDDWrapper.loadProfiles(basePath+"DBpediaProfiles.json/part-00000", realIDField = "realId").collect()
    profiles.foreach(println)
  }

  def profileToJSON2(profile : (String, Iterable[(String, String)])) : String = {
    val profileID = "\""+profile._1.replace("\"", "\\\"")+"\""
    val keys = profile._2
    val a = keys.map{case(attribute, value) =>
      val fixedAttribute = "\""+attribute.replace("\"", "\\\"")+"\""
      val fixedValue = "\""+value.replace("\"", "\\\"")+"\""
      (fixedAttribute, fixedValue)
    }
    val b = a.groupBy(kw => kw._1).map{kw =>
      kw._1 + ": ["+kw._2.map(_._2).mkString(",")+"]"
    }
    "{\"realId\": "+profileID+", "+b.mkString(",")+"}"
  }

  def profileToJSON(profile : Profile) : String = {
    val a = profile.attributes.map{kw =>
      KeyValue(kw.key, "\""+kw.value.replace("\"", "\\\"")+"\"")
    }
    val b = a.groupBy(kw => kw.key).map{kw =>
      kw._1 + ": ["+kw._2.map(_.value).mkString(",")+"]"
    }
    "{realId: \""+profile.originalID.replace("\"", "\\\"")+"\", "+b.mkString(",")+"}"
  }
}
