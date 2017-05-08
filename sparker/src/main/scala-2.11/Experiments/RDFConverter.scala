import java.util.Calendar

import Wrappers.CSVWrapper

//import Wrappers.JsonRDDWrapper
import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}
//import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
/*

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


val basePath = "/marconi_scratch/userexternal/lgaglia1/ER/"
val log = LogManager.getRootLogger
log.setLevel(Level.INFO)
val layout = new SimpleLayout();
val appender = new FileAppender(layout,"/marconi_scratch/userexternal/lgaglia1/log/logConverter.txt",false);
log.addAppender(appender);
val pattern = """<(.+?)>\s<(.+?)>\s(.+?)\s<(.+?)>\s.""".r
val empty = ("", ("", ""))

val startTime = Calendar.getInstance();
log.info("SPARKER - INIZIO CARICAMENTO DELLE TRIPLE")
val data = sc.textFile(basePath+"freebase.nq")
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
log.info("SPARKER - CARICAMENTO TERMINATO")
log.info("SPARKER - NUMERO LINEE "+num)
log.info("SPARKER - TEMPO CARICAMENTO "+(loadTime.getTimeInMillis-startTime.getTimeInMillis)+" ms")


log.info("SPARKER - INIZIO GROUP BY")
val linesByID = lines.groupByKey()
linesByID.cache()
val linesGroupedNum = linesByID.count()
lines.unpersist()
val groupTime = Calendar.getInstance()
log.info("SPARKER - TERMINATO GROUP BY "+linesGroupedNum)
log.info("SPARKER - TEMPO GROUP BY "+(groupTime.getTimeInMillis-loadTime.getTimeInMillis)+" ms")


/*
log.info("SPARKER - ELIMINO PROFILI CHE NON CONTENGONO RIFERIMENTI A DBPEDIA")
val linesInTheGroundtruth = linesByID.filter(x => !x._2.filter(av => av._2.contains("dbpedia") && av._1.contains("sameAs")).isEmpty)
linesInTheGroundtruth.cache()
val numLinesAfterFilter = linesInTheGroundtruth.count()
linesByID.unpersist()
val filterTime = Calendar.getInstance()
log.info("SPARKER - TERMINATA PULIZIA "+numLinesAfterFilter)
log.info("SPARKER - TEMPO FILTERING "+(filterTime.getTimeInMillis-groupTime.getTimeInMillis)+" ms")

log.info("SPARKER - GENERO GROUNDTRUTH")
val groundtruth = linesInTheGroundtruth.map{x =>
  val a = x._2.filter(av => av._2.contains("dbpedia") && av._1.contains("sameAs"))
  val freebase = "\""+x._1.replace("\"", "\\\"")+"\""
  val dbpedia = "\""+a.head._2.replace("\"", "\\\"")+"\""
  dbpedia+","+freebase
}
groundtruth.repartition(1).saveAsTextFile(basePath+"gtFromFreebase")
groundtruth.unpersist()
val gtTime = Calendar.getInstance()
log.info("SPARKER - TERMINATA GENERAZIONE")
log.info("SPARKER - TEMPO GENERAZIONE GT "+(gtTime.getTimeInMillis-filterTime.getTimeInMillis)+" ms")
*/

log.info("SPARKER - MANTENGO SOLO I PROFILI CHE SONO ANCHE NEL GROUNDTRUTH")
val gt = CSVWrapper.loadGroundtruth(basePath+"groundtruth.csv/groundtruth.csv")
val freebaseIDs = sc.broadcast(gt.map(_.secondEntityID).collect().toSet)
val linesInTheGroundtruth = linesByID.filter(x => freebaseIDs.value.contains(x._1))
linesInTheGroundtruth.cache()
val numLinesAfterFilter = linesInTheGroundtruth.count()
val linesNotInTheGroundtruth =  linesByID.subtract(linesInTheGroundtruth)
linesNotInTheGroundtruth.cache()
linesNotInTheGroundtruth.count()
linesByID.unpersist()
val filterTime = Calendar.getInstance()
log.info("SPARKER - TERMINATA PULIZIA "+numLinesAfterFilter)
log.info("SPARKER - TEMPO FILTERING "+(filterTime.getTimeInMillis-groupTime.getTimeInMillis)+" ms")


val profilesGtJSON = linesInTheGroundtruth.map{profile =>
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
profilesGtJSON.repartition(1).saveAsTextFile(basePath+"FreebaseProfilesGT.json")
linesInTheGroundtruth.map(_._1).repartition(1).saveAsTextFile(basePath+"FreebaseGtProfilesIDs")


val profilesNotGtJSON = linesNotInTheGroundtruth.map{profile =>
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
profilesGtJSON.union(profilesNotGtJSON).repartition(1).saveAsTextFile(basePath+"AllFreebaseProfilesIDs")
val profilesTime = Calendar.getInstance()
log.info("SPARKER - TERMINATA GENERAZIONE PROFILI")
log.info("SPARKER - TEMPO GENERAZIONE PROFILI "+(profilesTime.getTimeInMillis-filterTime.getTimeInMillis)+" ms")
log.info("SPARKER - TEMPO ESECUZIONE TOTALE "+(profilesTime.getTimeInMillis-startTime.getTimeInMillis)+" ms")

/*
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
log.setLevel(Level.INFO)
val layout = new SimpleLayout();
val appender = new FileAppender(layout,"/marconi_scratch/userexternal/lgaglia1/log/logConverter.txt",false);
log.addAppender(appender);

val startTime = Calendar.getInstance();
log.info("INIZIO CARICAMENTO DELLE TRIPLE")
val data = sc.textFile("/marconi_scratch/userexternal/lgaglia1/ER/dbpedia.nq")
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
log.info("CARICAMENTO TERMINATO")
log.info("NUMERO LINEE "+num)
log.info("TEMPO CARICAMENTO "+(loadTime.getTimeInMillis-startTime.getTimeInMillis)+" ms")


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

head.union(c).repartition(1).saveAsTextFile("/marconi_scratch/userexternal/lgaglia1/ER/dbpedia.csv")*/
*/