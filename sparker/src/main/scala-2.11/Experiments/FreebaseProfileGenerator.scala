import java.util.Calendar
import Wrappers.CSVWrapper
import org.apache.log4j.{FileAppender, Level, LogManager, SimpleLayout}
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
val appender = new FileAppender(layout,"/marconi_scratch/userexternal/lgaglia1/log/freebaseProfileGenerator.txt",false);
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


log.info("SPARKER - MANTENGO SOLO I PROFILI CHE SONO ANCHE NEL GROUNDTRUTH")
val gt = CSVWrapper.loadGroundtruth(basePath+"groundtruth_dbpedia_freebase.csv")
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
profilesGtJSON.repartition(1).saveAsTextFile(basePath+"FreebaseProfilesGT")
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
profilesGtJSON.union(profilesNotGtJSON).repartition(1).saveAsTextFile(basePath+"AllFreebaseProfiles")
val profilesTime = Calendar.getInstance()
log.info("SPARKER - TERMINATA GENERAZIONE PROFILI")
log.info("SPARKER - TEMPO GENERAZIONE PROFILI "+(profilesTime.getTimeInMillis-filterTime.getTimeInMillis)+" ms")
log.info("SPARKER - TEMPO ESECUZIONE TOTALE "+(profilesTime.getTimeInMillis-startTime.getTimeInMillis)+" ms")
*/