import java.util.Calendar

import Wrappers.{CSVWrapper, JsonRDDWrapper}
import org.apache.spark.{SparkConf, SparkContext}

/*
val pathDataset1 = "/marconi_scratch/userexternal/lgaglia1/ER/datasets/dbpedia_freebase/DBpediaProfilesGT.json"
val pathDataset2 = "/marconi_scratch/userexternal/lgaglia1/ER/datasets/dbpedia_freebase/FreebaseProfilesGT.json"
val pathGt = "/marconi_scratch/userexternal/lgaglia1/ER/datasets/dbpedia_freebase/groundtruth_dbpedia_freebase.csv"




val conf = new SparkConf()
  .setAppName("Main")
  .setMaster("local[*]")
  .set("spark.executor.memory", 10+"g")
  .set("spark.network.timeout", "10000s")
  .set("spark.executor.heartbeatInterval", "40s")
  .set("spark.default.parallelism", "32")
  .set("spark.executor.extraJavaOptions", "-Xss"+10+"g")
  .set("spark.local.dir", "/data2/sparkTmp/")
  .set("spark.driver.maxResultSize", "10g")
val sc = new SparkContext(conf)

val startTime = Calendar.getInstance();
val dataset1 = JsonRDDWrapper.loadProfiles(pathDataset1, 0, "realId")
val separatorID = dataset1.map(_.id).max()
val profilesDataset1 = dataset1.count()
val dataset2 = JsonRDDWrapper.loadProfiles(pathDataset2, separatorID + 1, "realId")
val maxProfileID = dataset2.map(_.id).max()
val profilesDataset2 = dataset2.count()

val groundtruth = CSVWrapper.loadGroundtruth(filePath = pathGt, header = true)
val gtNum = groundtruth.count()

val realIdId1 = sc.broadcast(dataset1.map{p =>
  (p.originalID, p.id)
}.collectAsMap())

val realIdId2 = sc.broadcast(dataset2.map{p =>
  (p.originalID, p.id)
}.collectAsMap())

/*
val newGT = groundtruth.map{g =>
  val first = realIdId1.value.get(g.firstEntityID)
  val second = realIdId2.value.get(g.secondEntityID)
  if(first.isDefined && second.isDefined){
    1
  }
  else{
    0
  }
}.sum()

println("SPARKER - NUMERO ELEMENTI NEL GT "+newGT)
*/

val realIdId1 = sc.broadcast(dataset1.map{p =>
  val tmp = p.originalID.replaceFirst("\"", "")
  (tmp.patch(tmp.lastIndexOf("\""), "", 1), p.id)
}.collectAsMap())

val realIdId2 = sc.broadcast(dataset2.map{p =>
  val tmp = p.originalID.replaceFirst("\"", "")
  (tmp.patch(tmp.lastIndexOf("\""), "", 1), p.id)
}.collectAsMap())


val newGT = groundtruth.map{g =>
  val first = realIdId1.value.get(g.firstEntityID)
  val second = realIdId2.value.get(g.secondEntityID)
  if(first.isDefined && second.isDefined){
    val a = "\""+"\\\""+g.firstEntityID.replace("\"", "\\\"")+"\\\""+"\""
    val b = "\""+"\\\""+g.secondEntityID.replace("\"", "\\\"")+"\\\""+"\""

    a+","+b
  }
  else{
    ""
  }
}.filter(_ != "").repartition(1).saveAsTextFile("/marconi_scratch/userexternal/lgaglia1/ER/datasets/dbpedia_freebase/groundtruth_dbpedia_freebase_ok.csv")
*/