package Experiments

import BlockBuildingMethods.TokenBlocking
import BlockRefinementMethods.{BlockFiltering, BlockPurging}
import DataStructures._
import Utilities.Converters
import Wrappers.CSVWrapper
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * Created by Luca on 20/02/2017.
  */
object Main3 {
  val fieldsToUseJS = "Physician_First_Name" :: "Physician_Last_Name" :: "Recipient_Primary_Business_Street_Address_Line1" :: Nil


  def main(args: Array[String]): Unit = {
    val baseDatasetDir = "C:\\Users\\Luca\\Desktop\\UNI\\blockingFrameworkOK\\blockingframework-spark\\spark_sbt_project\\datasets\\"

    val memoryHeap = 15
    val memoryStack = 5

    val conf = new SparkConf()
      .setAppName("Main")
      .setMaster("local[*]")
      .set("spark.executor.memory", memoryHeap+"g")
      .set("spark.network.timeout", "10000s")
      .set("spark.executor.heartbeatInterval", "40s")
      .set("spark.default.parallelism", "32")
      .set("spark.executor.extraJavaOptions", "-Xss"+memoryStack+"g")

    val sc = new SparkContext(conf)

    val profiles = CSVWrapper.loadProfiles(filePath = baseDatasetDir + "mediciU.csv", header = true, explodeInnerFields = true, separator = ";")

    val maxID = profiles.map(_.id).max().toInt

    val separatorID = -1

    val keysToUse = "Physician_First_Name" :: "Physician_Last_Name" :: "Recipient_Primary_Business_Street_Address_Line1" :: "Recipient_City" :: "Recipient_State" :: "Recipient_Zip_Code" :: "Recipient_Country" :: Nil;

    val blocks = TokenBlocking.createBlocksUsingSchemaEntropy(profiles, separatorID, keysToUse)

    val blocksPurged = BlockPurging.blockPurging(blocks, 1.005)

    val profileBlocks = Converters.blocksToProfileBlocks(blocksPurged)

    val profileBlocksFiltered = BlockFiltering.blockFiltering(profileBlocks, 0.8)

    val blocksAfterFiltering = Converters.profilesBlockToBlocks(profileBlocksFiltered)

    val entropies = sc.broadcast(blocks.map(b => (b.blockID, b.entropy)).collectAsMap())


    val blockIndexMap = sc.broadcast(blocksAfterFiltering.map(b => (b.blockID, b.profiles)).collectAsMap())
    val profilesMap = sc.broadcast(profiles.map(p =>(p.id, p)).collectAsMap())

    val profileBlocksAfterFiltering = Converters.blocksToProfileBlocks(blocksAfterFiltering)
    val profileBlockIndex = profileBlocksAfterFiltering.map(p => (p.profileID, p.blocks.map(_.blockID))).collectAsMap()
    val profileBlockIndexB = sc.broadcast(profileBlockIndex)

    //val res = WNPJS(profileBlocksAfterFiltering, blockIndexMap, profileBlockIndexB, maxID, separatorID, profilesMap, getValueFromProfile)
    val res = WNPJSE(profileBlocksAfterFiltering, blockIndexMap, entropies, profileBlockIndexB, maxID, separatorID, profilesMap, getValueFromProfile)

    val sortedCandidates = res.sortBy(x => x._2, ascending = false)

    val results = calculateTopProfiles(sortedCandidates.map(_._1).collect(), 20, getValueFromProfile, profileBlockIndex, blockIndexMap, separatorID, profilesMap, profileSimilarity)


    /*sortedCandidates.map {
      case(profileID, value, soglia) =>
        val p = profilesMap.value(profileID)
        (profileID, p.getAttributeValues("Physician_First_Name"), p.getAttributeValues("Physician_Last_Name"), value, soglia)
    }.repartition(1).saveAsTextFile("C:/Users/Luca/Desktop/sortMeds2")*/

    /*results.map{case(value, profileIDs) =>
      val p = profilesMap.value(profileIDs.head)
      (p.getAttributeValues("Physician_First_Name"), p.getAttributeValues("Physician_Last_Name"), value, profileIDs)
    }.foreach(println)*/



    val r = results.map{case(value, profileIDs) =>
      val p = profilesMap.value(profileIDs.head)
      (p.getAttributeValues("Physician_First_Name") + " "+ p.getAttributeValues("Physician_Last_Name"), value)
    }
    r.toList.sortBy(-_._2).foreach(println)


    /*

    val sameEntity : mutable.HashSet[Long]  = new mutable.HashSet[Long]()
    val toExplore = new mutable.Queue[Long]()

    toExplore += resSorted.head._1

    while(!toExplore.isEmpty){
      val profileID = toExplore.dequeue()
      val currentProfile = profilesMap.value.get(profileID).get
      sameEntity += profileID
      val blocks = profileBlockIndex.get(profileID).get
      blocks.foreach{
        blockID =>
          val blockProfiles = blockIndexMap.value.get(blockID)
          if(blockProfiles.isDefined){
            val neightbours = {
              if(separatorID >= 0 && profileID <= separatorID){
                blockProfiles.get._2
              }
              else{
                blockProfiles.get._1
              }
            }

            neightbours.foreach{
              neightbourID =>
                if(!sameEntity.contains(neightbourID)){
                  val neightbourProfile =  profilesMap.value.get(neightbourID).get
                  if(profileSimilarity(currentProfile, neightbourProfile) >= 0.5){
                    sameEntity += neightbourID
                    toExplore += neightbourID
                  }
                }
            }
          }
      }
    }

    sameEntity.map {
      case(profileID) =>
        val p = profilesMap.value(profileID)
        (profileID, p.getAttributeValues("Physician_First_Name"), p.getAttributeValues("Physician_Last_Name"))
    }.foreach(println)*/
  }

  def calculateTopProfiles(sortedCandidates : Array[Long], maxNumberOfResults : Int, getProfileValue : Profile => Double,
                           profileBlockIndex : scala.collection.Map[Long, List[Long]],
                           blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]],
                           separatorID : Long, profileIndex : Broadcast[scala.collection.Map[Long, Profile]],
                           profileSimilarity : (Profile, Profile) => Double) : Iterable[(Double, Iterable[Long])] = {

    val alreadyUsed = new mutable.HashSet[Long]()
    val results = new mutable.ListBuffer[(Double, Iterable[Long])]()
    var count = 0
    for(i<-0 to sortedCandidates.size-1){
      if(!alreadyUsed.contains(sortedCandidates(i))){
        val cluster = calculateRealNeightbours(sortedCandidates(i), profileBlockIndex, blockIndex, separatorID, profileIndex, profileSimilarity)
        val measure = cluster.map(profileIndex.value.get(_).get).map(getProfileValue).sum
        results += ((measure, cluster))
        count += 1
        if(count >= maxNumberOfResults){
          return results
        }
        alreadyUsed ++= cluster
      }
    }
    results
  }

  def calculateRealNeightbours(startProfileID : Long, profileBlockIndex : scala.collection.Map[Long, List[Long]],
                               blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]],
                               separatorID : Long, profileIndex : Broadcast[scala.collection.Map[Long, Profile]],
                               profileSimilarity : (Profile, Profile) => Double) : Iterable[Long] = {

    val sameEntity : mutable.HashSet[Long]  = new mutable.HashSet[Long]()
    val toExplore = new mutable.Queue[Long]()

    toExplore += startProfileID

    while(!toExplore.isEmpty){
      val profileID = toExplore.dequeue()
      val currentProfile = profileIndex.value.get(profileID).get
      sameEntity += profileID
      val blocks = profileBlockIndex.get(profileID).get
      blocks.foreach{
        blockID =>
          val blockProfiles = blockIndex.value.get(blockID)
          if(blockProfiles.isDefined){
            val neightbours = {
              if(separatorID >= 0 && profileID <= separatorID){
                blockProfiles.get._2
              }
              else{
                blockProfiles.get._1
              }
            }

            neightbours.foreach{
              neightbourID =>
                if(!sameEntity.contains(neightbourID)){
                  val neightbourProfile =  profileIndex.value.get(neightbourID).get
                  if(profileSimilarity(currentProfile, neightbourProfile) >= 0.5){
                    sameEntity += neightbourID
                    toExplore += neightbourID
                  }
                }
            }
          }
      }
    }

    sameEntity
  }


  def WNPJSE(profileBlocksFiltered : RDD[ProfileBlocks], blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]],
             entropies : Broadcast[scala.collection.Map[Long, Double]],
            profileBlockIndex : Broadcast[scala.collection.Map[Long, (List[Long])]],
            maxID : Int, separatorID : Long, profileIndex : Broadcast[scala.collection.Map[Long, Profile]],
            getProfileValue : Profile => Double) : RDD[(Long, Double, Double)] = {
    profileBlocksFiltered mapPartitions{
      partition =>

        val arrayPesi = Array.fill[Double](maxID+1){0}
        val arrayVicini = Array.ofDim[Int](maxID+1)
        var numeroVicini = 0
        var pesoTotale : Double = 0

        partition map{
          pb =>
            val profileID = pb.profileID
            val blocchiProfilo = profileBlockIndex.value.get(profileID).get
            val blocchiInCuiCompare = pb.blocks

            blocchiInCuiCompare foreach {
              block =>
                val idBlocco = block.blockID
                val blockEntropy = entropies.value(idBlocco)
                val profiliNelBlocco = blockIndex.value.get(idBlocco)
                if(profiliNelBlocco.isDefined){
                  val profiliCheContiene = {
                    if(separatorID >= 0 && profileID <= separatorID){
                      profiliNelBlocco.get._2
                    }
                    else{
                      profiliNelBlocco.get._1
                    }
                  }

                  profiliCheContiene foreach {
                    secondProfileID =>
                      val vicino = secondProfileID.toInt
                      val pesoAttuale = arrayPesi(vicino)
                      if(pesoAttuale == 0){
                        arrayVicini.update(numeroVicini, vicino)
                        val blocchiVicino = profileBlockIndex.value.get(vicino).get
                        val commonBlocksNumber = (blocchiProfilo.intersect(blocchiVicino).size).toDouble
                        val js = (commonBlocksNumber / (blocchiProfilo.size + blocchiVicino.size - commonBlocksNumber))*blockEntropy
                        pesoTotale += js
                        arrayPesi.update(vicino, js)
                        numeroVicini = numeroVicini+1
                      }
                  }
                }
            }


            val soglia = pesoTotale/numeroVicini

            val vicini = for(i <- 0 to numeroVicini-1  if arrayPesi(arrayVicini(i)) >= soglia && profileID < arrayVicini(i)) yield {
              arrayVicini(i)
            }

            var totale = getProfileValue(profileIndex.value.get(profileID).get)
            for(i <- 0 to vicini.size-1){
              totale = totale + getProfileValue(profileIndex.value.get(vicini(i)).get)*arrayPesi(arrayVicini(i))
            }

            for(i <-0 to numeroVicini-1){
              arrayPesi.update(arrayVicini(i), 0)
            }

            numeroVicini = 0
            pesoTotale = 0

            (profileID, totale, soglia)
        }
    }
  }

  def WNPJS(profileBlocksFiltered : RDD[ProfileBlocks], blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]],
            profileBlockIndex : Broadcast[scala.collection.Map[Long, (List[Long])]],
            maxID : Int, separatorID : Long, profileIndex : Broadcast[scala.collection.Map[Long, Profile]],
            getProfileValue : Profile => Double) : RDD[(Long, Double, Double)] = {
    profileBlocksFiltered mapPartitions{
      partition =>

        val arrayPesi = Array.fill[Double](maxID+1){0}
        val arrayVicini = Array.ofDim[Int](maxID+1)
        var numeroVicini = 0
        var pesoTotale : Double = 0

        partition map{
          pb =>
            val profileID = pb.profileID
            val blocchiProfilo = profileBlockIndex.value.get(profileID).get
            val blocchiInCuiCompare = pb.blocks

            blocchiInCuiCompare foreach {
              block =>
                val idBlocco = block.blockID
                val profiliNelBlocco = blockIndex.value.get(idBlocco)
                if(profiliNelBlocco.isDefined){
                  val profiliCheContiene = {
                    if(separatorID >= 0 && profileID <= separatorID){
                      profiliNelBlocco.get._2
                    }
                    else{
                      profiliNelBlocco.get._1
                    }
                  }

                  profiliCheContiene foreach {
                    secondProfileID =>
                      val vicino = secondProfileID.toInt
                      val pesoAttuale = arrayPesi(vicino)
                      if(pesoAttuale == 0){
                        arrayVicini.update(numeroVicini, vicino)
                        val blocchiVicino = profileBlockIndex.value.get(vicino).get
                        val commonBlocksNumber = (blocchiProfilo.intersect(blocchiVicino).size).toDouble
                        val js = commonBlocksNumber / (blocchiProfilo.size + blocchiVicino.size - commonBlocksNumber)
                        pesoTotale += js
                        arrayPesi.update(vicino, js)
                        numeroVicini = numeroVicini+1
                      }
                  }
                }
            }


            val soglia = pesoTotale/numeroVicini

            val vicini = for(i <- 0 to numeroVicini-1  if arrayPesi(arrayVicini(i)) >= soglia && profileID < arrayVicini(i)) yield {
              arrayVicini(i)
            }

            var totale = getProfileValue(profileIndex.value.get(profileID).get)
            for(i <- 0 to vicini.size-1){
              totale = totale + getProfileValue(profileIndex.value.get(vicini(i)).get)*arrayPesi(arrayVicini(i))
            }

            for(i <-0 to numeroVicini-1){
              arrayPesi.update(arrayVicini(i), 0)
            }

            numeroVicini = 0
            pesoTotale = 0

            (profileID, totale, soglia)
        }
    }
  }

  def WNPCBS(profileBlocksFiltered : RDD[ProfileBlocks], blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]],
             maxID : Int, separatorID : Long, profileIndex : Broadcast[scala.collection.Map[Long, Profile]],
             getProfileValue : Profile => Double) : RDD[(Long, Double, Double)] = {
    profileBlocksFiltered mapPartitions{
      partition =>

        val arrayPesi = Array.fill[Int](maxID+1){0}
        val arrayVicini = Array.ofDim[Int](maxID+1)
        var numeroVicini = 0
        var localMax = 0

        partition map{
          pb =>
            val profileID = pb.profileID
            val blocchiInCuiCompare = pb.blocks

            blocchiInCuiCompare foreach {
              block =>
                val idBlocco = block.blockID
                val profiliNelBlocco = blockIndex.value.get(idBlocco)
                if(profiliNelBlocco.isDefined){
                  val profiliCheContiene = {
                    if(separatorID >= 0 && profileID <= separatorID){
                      profiliNelBlocco.get._2
                    }
                    else{
                      profiliNelBlocco.get._1
                    }
                  }

                  profiliCheContiene foreach {
                    secondProfileID =>
                      val vicino = secondProfileID.toInt
                      val pesoAttuale = arrayPesi(vicino)
                      if(pesoAttuale == 0){
                        arrayVicini.update(numeroVicini, vicino)
                        arrayPesi.update(vicino, 1)
                        numeroVicini = numeroVicini+1
                      }
                      else{
                        arrayPesi.update(vicino, pesoAttuale+1)
                      }

                      if(pesoAttuale+1 > localMax){
                        localMax = pesoAttuale+1
                      }
                  }
                }
            }


            val soglia = localMax.toFloat/2.0

            val vicini = for(i <- 0 to numeroVicini-1  if arrayPesi(arrayVicini(i)) >= soglia && profileID < arrayVicini(i)) yield {
              arrayVicini(i)
            }

            var totale = getProfileValue(profileIndex.value.get(profileID).get)
            for(i <- 0 to vicini.size-1){
              totale = totale + getProfileValue(profileIndex.value.get(vicini(i)).get)*arrayPesi(arrayVicini(i))
            }

            for(i <-0 to numeroVicini-1){
              arrayPesi.update(arrayVicini(i), 0)
            }

            numeroVicini = 0
            localMax = 0

            (profileID, totale, soglia)
        }
    }
  }

  def getValueFromProfile(p : Profile) : Double = {
    try{
      p.getAttributeValues("Total_Amount_of_Payment_USDollars").toDouble
    }
    catch {
      case _ : Throwable => 0.0
    }
  }

  def profileSimilarity(p1 : Profile, p2 : Profile) : Double = {
    val js =
      for(i <- 0 to Main3.fieldsToUseJS.size-1) yield {
        getJS(p1.getAttributeValues(Main3.fieldsToUseJS(i)), p2.getAttributeValues(Main3.fieldsToUseJS(i)))
      }
    js.sum/js.size.toDouble
  }

  def getJS(word1 : String, word2 : String) : Double = {
    val t1 = getNgrams(word1, 2)
    val t2 = getNgrams(word2, 2)

    val commonBlocksNumber = (t1.intersect(t2).size).toDouble
    val jaccardSimilarity = commonBlocksNumber / (t1.size + t2.size - commonBlocksNumber)
    jaccardSimilarity
  }

  def getNgrams(word : String, n : Int) : List[String] = {
    ("$"+word+"$").toLowerCase.sliding(n).map(_.mkString).toList
  }
}
