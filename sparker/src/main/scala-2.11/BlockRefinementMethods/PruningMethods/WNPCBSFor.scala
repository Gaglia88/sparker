package BlockRefinementMethods.PruningMethods

import DataStructures.{ProfileBlocks, UnweightedEdge, WeightedEdge}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by Luca on 24/01/2017.
  * Weight Node Pruning with CBS
  * Experimental, we have to finish this class
  */
object WNPCBSFor {

  def WNPEntropy(profileBlocksFiltered : RDD[ProfileBlocks],  blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]], maxID : Int, separatorID : Long,
                 blockEntropies : Broadcast[scala.collection.Map[Long, Double]]) : RDD[WeightedEdge] = {
    profileBlocksFiltered mapPartitions{
      partition =>

        val arrayPesi = Array.fill[Double](maxID+1){0}
        val arrayVicini = Array.ofDim[Int](maxID+1)
        var numeroVicini = 0
        var pesoTotale = 0

        partition flatMap{
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

                  val entropia = blockEntropies.value.get(idBlocco).get

                  profiliCheContiene foreach {
                    secondProfileID =>
                      val vicino = secondProfileID.toInt
                      val pesoAttuale = arrayPesi(vicino)
                      pesoTotale += 1
                      if(pesoAttuale == 0){
                        arrayVicini.update(numeroVicini, vicino)
                        arrayPesi.update(vicino, entropia)
                        numeroVicini = numeroVicini+1
                      }
                      else{
                        arrayPesi.update(vicino, pesoAttuale+entropia)
                      }
                  }
                }
            }



            val avg = pesoTotale.toFloat/numeroVicini.toFloat

            val a = for(i <- 0 to numeroVicini-1 if(arrayPesi(arrayVicini(i))) >= avg) yield {
              if(profileID < arrayVicini(i)) {
                WeightedEdge(profileID, arrayVicini(i), arrayPesi(arrayVicini(i)))
              }
              else{
                WeightedEdge(arrayVicini(i), profileID, arrayPesi(arrayVicini(i)))
              }
            }

            for(i <- 0 to numeroVicini-1) {
              arrayPesi.update(arrayVicini(i), 0)
            }
            numeroVicini = 0
            pesoTotale = 0

            a
        }
    }
  }

  def WNP2(profileBlocksFiltered : RDD[ProfileBlocks],  blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]], maxID : Int, separatorID : Long) : RDD[Long] = {
    profileBlocksFiltered mapPartitions{
      partition =>

        val arrayPesi = Array.fill[Int](maxID+1){0}
        val arrayVicini = Array.ofDim[Int](maxID+1)
        var numeroVicini = 0
        var pesoTotale = 0

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
                      pesoTotale += 1
                      if(pesoAttuale == 0){
                        arrayVicini.update(numeroVicini, vicino)
                        arrayPesi.update(vicino, 1)
                        numeroVicini = numeroVicini+1
                      }
                      else{
                        arrayPesi.update(vicino, pesoAttuale+1)
                      }
                  }
                }
            }



            val avg = pesoTotale.toFloat/numeroVicini.toFloat

            var tot = 0

            for(i <- 0 to numeroVicini-1) {
              if(arrayPesi(arrayVicini(i)) >= avg){
                tot +=1
              }
              arrayPesi.update(arrayVicini(i), 0)
            }

            numeroVicini = 0
            pesoTotale = 0

            tot
        }
    }
  }

  def WNP3(profileBlocksFiltered : RDD[ProfileBlocks],  blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]], maxID : Int, separatorID : Long) : RDD[Long] = {
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


            var cont = 0
            val soglia = localMax.toFloat/2.0

            for(i <- 0 to numeroVicini-1) {
              if(arrayPesi(arrayVicini(i)) >= soglia){
                cont += 1
              }
              arrayPesi.update(arrayVicini(i), 0)
            }

            numeroVicini = 0
            localMax = 0
            cont

          /*if(numeroVicini > 0){
            val pesi = for(i <- 0 to numeroVicini-1) yield {
              val p = arrayPesi(arrayVicini(i))
              arrayPesi.update(arrayVicini(i), 0)
              p
            }

            numeroVicini = 0

            pesi.filter(_ >= pesi.max.toFloat/2.0).size
          }
          else{
            0
          }*/
        }
    }
  }

  def WNP4(profileBlocksFiltered : RDD[ProfileBlocks],  blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]], maxID : Int, separatorID : Long, groundtruth : Broadcast[scala.collection.Map[Long, Long]]) : RDD[(Double, UnweightedEdge)] = {
    profileBlocksFiltered mapPartitions{
      partition =>

        val arrayPesi = Array.fill[Int](maxID+1){0}             //Usato per memorizzare i pesi di ogni vicino
      val arrayFound = Array.fill[Boolean](maxID+1){false}    //Usato per dire se ho già trovato il match di un certo elemento
      val arrayVicini = Array.ofDim[Int](maxID+1)             //Usato per tenere gli ID dei miei vicini
      var numeroVicini = 0                                    //Memorizza il numero di vicini che ho
      var localMax = 0                                        //Memorizza il massimo locale

        partition map{                                                    //Mappo gli elementi contenuti nella partizione sono: [id profilo, blocchi]
          pb =>
            val profileID = pb.profileID                                  //ID PROFILO
          val blocchiInCuiCompare = pb.blocks                           //Blocchi in cui compare questo profilo

            blocchiInCuiCompare foreach {                                 //Per ognuno dei blocchi in cui compare
              block =>
                val idBlocco = block.blockID                              //ID BLOCCO
              val profiliNelBlocco = blockIndex.value.get(idBlocco)     //Leggo gli ID di tutti gli altri profili che sono in quel blocco
                if(profiliNelBlocco.isDefined){
                  val profiliCheContiene = {
                    if(separatorID >= 0 && profileID <= separatorID){     //Se siamo in un contesto clean e l'id del profilo appartiene al dataset1, i suoi vicini sono nel dataset2
                      profiliNelBlocco.get._2
                    }
                    else{
                      profiliNelBlocco.get._1                             //Altrimenti sono nel dataset1
                    }
                  }

                  profiliCheContiene foreach {                            //Per ognuno dei suoi vicini in questo blocco
                    secondProfileID =>
                      val vicino = secondProfileID.toInt                  //ID del vicino
                    val pesoAttuale = arrayPesi(vicino)                 //Leggo il peso attuale che ha questo vicino
                      if(pesoAttuale == 0){                               //Se è 0 vuol dire che non l'avevo mai trovato prima
                        arrayVicini.update(numeroVicini, vicino)          //Aggiungo all'elenco dei vicini questo nuovo vicino
                        arrayPesi.update(vicino, 1)                       //Aggiorno il suo peso ad 1
                        numeroVicini = numeroVicini+1                     //Incremento il numero di vicini
                      }
                      else{
                        arrayPesi.update(vicino, pesoAttuale+1)           //Altrimenti lo avevo già trovato, allora incremento di 1 il numero di volte in cui l'ho trovato (il suo peso)
                      }

                      if(pesoAttuale+1 > localMax){                       //Se il peso del vicino attuale è maggiore del massimo locale, allora lo aggiorno
                        localMax = pesoAttuale+1
                      }
                  }
                }
            }


            var cont = 0                                                //Contatore che legge quanti vicini mantengo
          val soglia = localMax.toFloat/2.0                           //Soglia di pruning, max/2

            var perfectMatchEdge : UnweightedEdge = null                  //Edge che verrà dato in uscita per questo profilo che corrisponde ad un match nel dataset 2 (solo se lo trova), va bene solo se clean sto metodo!

            for(i <- 0 to numeroVicini-1) {                             //Scorro i vicini che ho trovato
              if(arrayPesi(arrayVicini(i)) >= soglia){                  //Questo vicino ha un peso superiore della soglia
                cont += 1                                               //Incremento il contatore che mi dice quanti vicini ho mantenuto
                if(!arrayFound(profileID.toInt)){                       //Guardo se ho già trovato un match per questo profilo, se non l'ho trovato proseguo
                  if(profileID < arrayVicini(i)) {                      //Il groundtruth è organizzato come (ID dataset1, ID dataset2), quindi devo cercare il profilo con ID minore
                  val m = groundtruth.value.get(profileID)            //Guardo se ho questo ID
                    if(m.isDefined && m.get == arrayVicini(i)){         //Se l'id c'è e il suo profilo che matcha è uguale a questo, allora ho trovato il match
                      perfectMatchEdge = UnweightedEdge(profileID, arrayVicini(i)) //Genero l'edge che voglio tenere
                      arrayFound.update(profileID.toInt, true)          //Setto nell'elenco che ho trovato il match per il profilo
                      arrayFound.update(arrayVicini(i), true)           //Stessa cosa per il suo compagno
                    }
                  }
                  else{                                                 //Stessa cosa di sopra ma girata
                  val m = groundtruth.value.get(arrayVicini(i))
                    if(m.isDefined && m.get == profileID) {
                      perfectMatchEdge = UnweightedEdge(arrayVicini(i), profileID)
                      arrayFound.update(profileID.toInt, true)
                      arrayFound.update(arrayVicini(i), true)
                    }
                  }
                }
              }

              arrayPesi.update(arrayVicini(i), 0)                       //Il peso di questo vicino non mi serve più, lo resetto per il prossimo giro
            }

            numeroVicini = 0                                            //Resetto numero di vicini
            localMax = 0                                                //Resetto massimo locale

            (cont.toDouble, perfectMatchEdge)                           //Fornisco in output il numero di vicini mantenuto e il match vero
        }
    }
  }

  def WNP5(profileBlocksFiltered : RDD[ProfileBlocks],  blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]], maxID : Int, separatorID : Long, groundtruth : Broadcast[scala.collection.Map[Long, Long]]) : RDD[(Double, UnweightedEdge)] = {
    profileBlocksFiltered mapPartitions{
      partition =>

        val arrayPesi = Array.fill[Int](maxID+1){0}             //Usato per memorizzare i pesi di ogni vicino
      val arrayFound = Array.fill[Boolean](maxID+1){false}    //Usato per dire se ho già trovato il match di un certo elemento
      val arrayVicini = Array.ofDim[Int](maxID+1)             //Usato per tenere gli ID dei miei vicini
      var numeroVicini = 0                                    //Memorizza il numero di vicini che ho
      var pesoTotale = 0                                      //Memorizza il complessivo locale

        partition map{                                                    //Mappo gli elementi contenuti nella partizione sono: [id profilo, blocchi]
          pb =>
            val profileID = pb.profileID                                  //ID PROFILO
          val blocchiInCuiCompare = pb.blocks                           //Blocchi in cui compare questo profilo

            blocchiInCuiCompare foreach {                                 //Per ognuno dei blocchi in cui compare
              block =>
                val idBlocco = block.blockID                              //ID BLOCCO
              val profiliNelBlocco = blockIndex.value.get(idBlocco)     //Leggo gli ID di tutti gli altri profili che sono in quel blocco
                if(profiliNelBlocco.isDefined){
                  val profiliCheContiene = {
                    if(separatorID >= 0 && profileID <= separatorID){     //Se siamo in un contesto clean e l'id del profilo appartiene al dataset1, i suoi vicini sono nel dataset2
                      profiliNelBlocco.get._2
                    }
                    else{
                      profiliNelBlocco.get._1                             //Altrimenti sono nel dataset1
                    }
                  }

                  profiliCheContiene foreach {                            //Per ognuno dei suoi vicini in questo blocco
                    secondProfileID =>
                      val vicino = secondProfileID.toInt                  //ID del vicino
                    val pesoAttuale = arrayPesi(vicino)                 //Leggo il peso attuale che ha questo vicino
                      pesoTotale +=1                                      //Incremento il peso totale
                      if(pesoAttuale == 0){                               //Se è 0 vuol dire che non l'avevo mai trovato prima
                        arrayVicini.update(numeroVicini, vicino)          //Aggiungo all'elenco dei vicini questo nuovo vicino
                        arrayPesi.update(vicino, 1)                       //Aggiorno il suo peso ad 1
                        numeroVicini = numeroVicini+1                     //Incremento il numero di vicini
                      }
                      else{
                        arrayPesi.update(vicino, pesoAttuale+1)           //Altrimenti lo avevo già trovato, allora incremento di 1 il numero di volte in cui l'ho trovato (il suo peso)
                      }
                  }
                }
            }


            var cont = 0                                                //Contatore che legge quanti vicini mantengo
          val soglia = pesoTotale.toFloat/numeroVicini.toFloat          //Soglia di pruning, media

            var perfectMatchEdge : UnweightedEdge = null                  //Edge che verrà dato in uscita per questo profilo che corrisponde ad un match nel dataset 2 (solo se lo trova), va bene solo se clean sto metodo!

            for(i <- 0 to numeroVicini-1) {                             //Scorro i vicini che ho trovato
              if(arrayPesi(arrayVicini(i)) >= soglia){                  //Questo vicino ha un peso superiore della soglia
                cont += 1                                               //Incremento il contatore che mi dice quanti vicini ho mantenuto
                if(!arrayFound(profileID.toInt)){                       //Guardo se ho già trovato un match per questo profilo, se non l'ho trovato proseguo
                  if(profileID < arrayVicini(i)) {                      //Il groundtruth è organizzato come (ID dataset1, ID dataset2), quindi devo cercare il profilo con ID minore
                  val m = groundtruth.value.get(profileID)            //Guardo se ho questo ID
                    if(m.isDefined && m.get == arrayVicini(i)){         //Se l'id c'è e il suo profilo che matcha è uguale a questo, allora ho trovato il match
                      perfectMatchEdge = UnweightedEdge(profileID, arrayVicini(i)) //Genero l'edge che voglio tenere
                      arrayFound.update(profileID.toInt, true)          //Setto nell'elenco che ho trovato il match per il profilo
                      arrayFound.update(arrayVicini(i), true)           //Stessa cosa per il suo compagno
                    }
                  }
                  else{                                                 //Stessa cosa di sopra ma girata
                  val m = groundtruth.value.get(arrayVicini(i))
                    if(m.isDefined && m.get == profileID) {
                      perfectMatchEdge = UnweightedEdge(arrayVicini(i), profileID)
                      arrayFound.update(profileID.toInt, true)
                      arrayFound.update(arrayVicini(i), true)
                    }
                  }
                }
              }

              arrayPesi.update(arrayVicini(i), 0)                       //Il peso di questo vicino non mi serve più, lo resetto per il prossimo giro
            }

            numeroVicini = 0                                            //Resetto numero di vicini
            pesoTotale = 0                                              //Resetto il peso totale

            (cont.toDouble, perfectMatchEdge)                           //Fornisco in output il numero di vicini mantenuto e il match vero
        }
    }
  }

  def WNP(profileBlocksFiltered : RDD[ProfileBlocks],  blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]], maxID : Int, separatorID : Long) : RDD[WeightedEdge] = {
    profileBlocksFiltered mapPartitions{
      partition =>

        val arrayPesi = Array.fill[Int](maxID+1){0}
        val arrayVicini = Array.ofDim[Int](maxID+1)
        var numeroVicini = 0
        var pesoTotale = 0

        partition flatMap{
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
                      pesoTotale += 1
                      if(pesoAttuale == 0){
                        arrayVicini.update(numeroVicini, vicino)
                        arrayPesi.update(vicino, 1)
                        numeroVicini = numeroVicini+1
                      }
                      else{
                        arrayPesi.update(vicino, pesoAttuale+1)
                      }
                  }
                }
            }



            val avg = pesoTotale.toFloat/numeroVicini.toFloat

            val a = for(i <- 0 to numeroVicini-1 if(arrayPesi(arrayVicini(i))) >= avg) yield {
              if(profileID < arrayVicini(i)) {
                WeightedEdge(profileID, arrayVicini(i), arrayPesi(arrayVicini(i)))
              }
              else{
                WeightedEdge(arrayVicini(i), profileID, arrayPesi(arrayVicini(i)))
              }
            }

            for(i <- 0 to numeroVicini-1) {
              arrayPesi.update(arrayVicini(i), 0)
            }
            numeroVicini = 0
            pesoTotale = 0

            a
        }
    }
  }
}
