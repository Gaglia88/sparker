import java.util.Calendar

import DataStructures.{KeyValue, Profile}
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
/*

val conf = new SparkConf()
  .setAppName("Main")
  .setMaster("local[*]")
  .set("spark.executor.memory", "118g")
  .set("spark.network.timeout", "10000s")
  .set("spark.executor.heartbeatInterval", "40s")
  .set("spark.default.parallelism", "64")
  .set("spark.executor.extraJavaOptions", "-Xss15g")
  .set("spark.local.dir", "/marconi_scratch/userexternal/lgaglia1/sparkTmp")
  .set("spark.driver.maxResultSize", "100g")

val sc = new SparkContext(conf)

val log = LogManager.getRootLogger

val startTime = Calendar.getInstance();
log.warn("INIZIO CARICAMENTO DELLE TRIPLE")
val data = sc.textFile("/marconi_scratch/userexternal/lgaglia1/ER/dbpedia.nq")
val trueTriples = data.filter(x => x.startsWith("<"))

val pattern = """<(.+?)>\s<(.+?)>\s(.+?)\s<(.+?)>\s.""".r
val empty = ("", ("", ""))

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
log.warn("CARICAMENTO TERMINATO")
log.warn("NUMERO LINEE "+num)
log.warn("TEMPO CARICAMENTO "+(loadTime.getTimeInMillis-startTime.getTimeInMillis)+" ms")

log.warn("INIZIO GROUP BY")
val linesByID = lines.groupByKey()
linesByID.cache()
val linesGroupedNum = linesByID.count()
lines.unpersist()
val groupTime = Calendar.getInstance()
log.warn("TERMINATO GROUP BY "+linesGroupedNum)
log.warn("TEMPO GROUP BY "+(groupTime.getTimeInMillis-loadTime.getTimeInMillis)+" ms")


log.warn("ELIMINO PROFILI CHE NON CONTENGONO RIFERIMENTI A FREEBASE")
val conWithFreebase = linesByID.filter(x => !x._2.filter(av => av._2.contains("freebase") && av._1.contains("sameAs")).isEmpty)
conWithFreebase.cache()
val numLinesAfterFilter = conWithFreebase.count()
linesByID.unpersist()
val filterTime = Calendar.getInstance()
log.warn("TERMINATA PULIZIA "+numLinesAfterFilter)
log.warn("TEMPO FILTERING "+(filterTime.getTimeInMillis-groupTime.getTimeInMillis)+" ms")

log.warn("GENERO GROUNDTRUTH")
val groundtruth = conWithFreebase.map{x =>
  val a = x._2.filter(av => av._2.contains("freebase") && av._1.contains("sameAs"))
  (x._1, a.head._2)
}
groundtruth.repartition(1).saveAsTextFile("/marconi_scratch/userexternal/lgaglia1/ER/groundtruth")
val gtTime = Calendar.getInstance()
log.warn("TERMINATA GENERAZIONE")
log.warn("TEMPO GENERAZIONE GT "+(gtTime.getTimeInMillis-filterTime.getTimeInMillis)+" ms")

log.warn("GENERO I PROFILI")
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
profiles.saveAsObjectFile("/marconi_scratch/userexternal/lgaglia1/ER/dbpediaProfiles")
val profilesTime = Calendar.getInstance()
log.warn("TERMINATA GENERAZIONE PROFILI")
log.warn("MAX PROFILE ID "+maxProfileID)
log.warn("TEMPO GENERAZIONE GT "+(profilesTime.getTimeInMillis-gtTime.getTimeInMillis)+" ms")*/