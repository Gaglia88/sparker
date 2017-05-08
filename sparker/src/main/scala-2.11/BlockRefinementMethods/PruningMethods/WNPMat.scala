package BlockRefinementMethods.PruningMethods

import DataStructures.{ProfileBlocks, UnweightedEdge}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by Luca on 27/03/2017.
  */
object WNPMat {
  def WNP5(profileBlocksFiltered : RDD[ProfileBlocks], blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]], maxID : Int, separatorID : Long) : Dataset[(Long, Long, Int)] = {


    val sc = SparkContext.getOrCreate()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "3000")
    import sqlContext.implicits._

    val test = profileBlocksFiltered.map { pb =>
      val blocks = pb.blocks.map(x => x.blockID).toArray
      (pb.profileID, blocks)
    }.toDS()

    test.mapPartitions { partition =>
      val arrayPesi = Array.fill[Int](maxID + 1){0} //Usato per memorizzare i pesi di ogni vicino
      val arrayVicini = Array.ofDim[Int](maxID + 1) //Usato per tenere gli ID dei miei vicini
      var numeroVicini = 0 //Memorizza il numero di vicini che ho
      var pesoTotale = 0 //Memorizza il complessivo locale

      partition flatMap {
        //Mappo gli elementi contenuti nella partizione sono: [id profilo, blocchi]
        pb =>
          val profileID = pb._1 //ID PROFILO
          val blocchiInCuiCompare = pb._2 //Blocchi in cui compare questo profilo

          blocchiInCuiCompare foreach {
            //Per ognuno dei blocchi in cui compare
            idBlocco =>
              val profiliNelBlocco = blockIndex.value.get(idBlocco) //Leggo gli ID di tutti gli altri profili che sono in quel blocco
              if (profiliNelBlocco.isDefined) {
                val profiliCheContiene = {
                  if (separatorID >= 0 && profileID <= separatorID) {
                    //Se siamo in un contesto clean e l'id del profilo appartiene al dataset1, i suoi vicini sono nel dataset2
                    profiliNelBlocco.get._2
                  }
                  else {
                    profiliNelBlocco.get._1 //Altrimenti sono nel dataset1
                  }
                }

                profiliCheContiene foreach {
                  //Per ognuno dei suoi vicini in questo blocco
                  secondProfileID =>
                    val vicino = secondProfileID.toInt //ID del vicino
                  val pesoAttuale = arrayPesi(vicino) //Leggo il peso attuale che ha questo vicino
                    pesoTotale += 1 //Incremento il peso totale
                    if (pesoAttuale == 0) {
                      //Se è 0 vuol dire che non l'avevo mai trovato prima
                      arrayVicini.update(numeroVicini, vicino) //Aggiungo all'elenco dei vicini questo nuovo vicino
                      arrayPesi.update(vicino, 1) //Aggiorno il suo peso ad 1
                      numeroVicini = numeroVicini + 1 //Incremento il numero di vicini
                    }
                    else {
                      arrayPesi.update(vicino, pesoAttuale + 1) //Altrimenti lo avevo già trovato, allora incremento di 1 il numero di volte in cui l'ho trovato (il suo peso)
                    }
                }
              }
          }

          val soglia = pesoTotale.toFloat / numeroVicini.toFloat //Soglia di pruning, media

          val results: Iterable[(Long, Long, Int)] = for (
            i <- 0 to numeroVicini - 1
            if (arrayPesi(arrayVicini(i)) >= soglia)
          ) yield {
            if (profileID < arrayVicini(i)) {
              (profileID.toLong, arrayVicini(i).toLong, arrayPesi(arrayVicini(i)))
            }
            else {
              (arrayVicini(i).toLong, profileID.toLong, arrayPesi(arrayVicini(i)))
            }
          }

          for (i <- 0 to numeroVicini - 1) {
            //Scorro i vicini che ho trovato
            arrayPesi.update(arrayVicini(i), 0) //Il peso di questo vicino non mi serve più, lo resetto per il prossimo giro
          }
          numeroVicini = 0 //Resetto numero di vicini
          pesoTotale = 0 //Resetto il peso totale

          results
      }
    }
  }
}
