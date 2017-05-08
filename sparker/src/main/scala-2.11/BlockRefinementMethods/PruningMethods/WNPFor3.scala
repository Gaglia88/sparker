package BlockRefinementMethods.PruningMethods

import BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
import BlockRefinementMethods.PruningMethods.WNPFor.ThresholdTypes
import DataStructures.{ProfileBlocks, UnweightedEdge}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by Luca on 03/05/2017.
  */
object WNPFor3 {

  def WNP(profileBlocksFiltered : RDD[ProfileBlocks],//RDD(profile, [blocks ids])
          blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]],//(block id, ([dataset 1 blocks], [dataset 2 blocks])
          maxID : Int,//MaxProfileID
          separatorID : Long,//Separator ID
          groundtruth : Broadcast[scala.collection.immutable.Set[(Long, Long)]],//Groundtruth
          thresholdType : String = ThresholdTypes.AVG,//Threshold type
          weightType : String = WeightTypes.CBS,//Weight type
          profileBlocksSizeIndex : Broadcast[scala.collection.Map[Long, Int]] = null,//(block id, block size)
          useEntropy : Boolean = false,
          blocksEntropies : Broadcast[scala.collection.Map[Long, Double]] = null,//(block id, entropy)
          blockCluster : Broadcast[scala.collection.Map[Long, Double]] = null
         )
  : RDD[(Double, Iterable[UnweightedEdge])] = {
    
    if(useEntropy == true && blocksEntropies == null){
      throw new Exception("blocksEntropies must be defined")
    }
    else if(weightType == WeightTypes.chiSquare){
      if(profileBlocksSizeIndex == null){
        throw new Exception("profileBlocksSizeIndex must be defined")
      }
      else{
        WNPChiSquare(profileBlocksFiltered, blockIndex, maxID, separatorID, groundtruth, thresholdType, profileBlocksSizeIndex, useEntropy, blocksEntropies)
      }
    }
    else if(weightType == WeightTypes.ARCS){
      WNPArcs(profileBlocksFiltered, blockIndex, maxID, separatorID, groundtruth, thresholdType, useEntropy, blocksEntropies)
    }
    else if(weightType == WeightTypes.JS){
      if(profileBlocksSizeIndex == null){
        throw new Exception("profileBlocksSizeIndex must be defined")
      }
      else{
        WNPJS(profileBlocksFiltered, blockIndex, maxID, separatorID, groundtruth, thresholdType, profileBlocksSizeIndex, useEntropy, blocksEntropies)
      }
    }
    else{
      WNPCBS(profileBlocksFiltered, blockIndex, maxID, separatorID, groundtruth, thresholdType, useEntropy, blocksEntropies, blockCluster)
    }
  }

  def WNPArcs(profileBlocksFiltered : RDD[ProfileBlocks],
              blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]],
              maxID : Int,
              separatorID : Long,
              groundtruth : Broadcast[scala.collection.immutable.Set[(Long, Long)]],
              thresholdType : String = ThresholdTypes.AVG,
              useEntropy : Boolean = false,
              blocksEntropies : Broadcast[scala.collection.Map[Long, Double]] = null
             )
  : RDD[(Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions {partition => //For each partition
      val localWeights = Array.fill[Double](maxID+1){0}//Contains the neighbours weights
      val neighbours = Array.ofDim[Int](maxID+1)//Contains the neighbours IDs
      var neighboursNumber = 0//Keeps the number of neighbours
      var acc : Double = 0//Accumulator, used to compute the AVG or to keep the local maximum

      val entropies = {
        if(useEntropy){
          Array.fill[Double](maxID + 1) {0.0}
        }
        else{
          Array.fill[Double](0) {0.0}
        }
      }//Contains the sums of entropies

      val cbs = {
        if(useEntropy){
          Array.fill[Double](maxID + 1) {0.0}
        }
        else{
          Array.fill[Double](0) {0.0}
        }
      }

      partition.map{pb => //Maps the elements contained in the partition, they are a list of (profile ID, [blocks which contains the profile])
        val profileID = pb.profileID//ProfileID
        val profileBlocks = pb.blocks//Blocks in which it appears

        profileBlocks.foreach {block => //Foreach block
          val blockID = block.blockID//Block ID
          val blockProfiles = blockIndex.value.get(blockID)//Other profiles in the same block
          if(blockProfiles.isDefined){//Check if exists
            var comparisons : Double = 0
            val profilesIDs = {//Gets the others profiles IDs
              if(separatorID >= 0 && profileID <= separatorID){//If we are in a Clean-Clean context and the profile ID is in the first dataset, then his neighbours are in the second dataset
                comparisons = blockProfiles.get._1.size*blockProfiles.get._2.size
                blockProfiles.get._2
              }
              else{//Otherwise they are in the first dataset
                if(separatorID > 0){
                  comparisons = blockProfiles.get._1.size*blockProfiles.get._2.size
                }
                else{
                  comparisons = blockProfiles.get._1.size*blockProfiles.get._2.size
                }
                blockProfiles.get._1
              }
            }

            val blockEntropy = {
              if(useEntropy){
                val e = blocksEntropies.value.get(blockID)
                if(e.isDefined){
                  e.get
                }
                else{
                  0.0
                }
              }
              else{
                0.0
              }
            }

            profilesIDs.foreach {secondProfileID => //For each neighbour computes the weight
              val neighbourID = secondProfileID.toInt//neighbour ID
              val neighbourWeight = localWeights(neighbourID)//New neighbour weight
              localWeights.update(neighbourID, neighbourWeight+(1/comparisons))//Update the neighbour weight
              if(neighbourWeight == 0){//If its equal to 0 mean that this neighbour it is the first time that this neighbour appears
                neighbours.update(neighboursNumber, neighbourID)//Add the neighbour to the neighbours list
                neighboursNumber += 1//Increment the number of neighbour
              }

              if(useEntropy){
                val neighbourEntropy = entropies(neighbourID)+blockEntropy//New neighbour entropy
                entropies.update(neighbourID, neighbourEntropy)//Update the neighbour entropy
                val commonBlocks = cbs(neighbourID)+1
                cbs.update(neighbourID, commonBlocks)
              }
            }
          }
        }

        var cont = 0//Counts the number of keeped neighbour

        for(i <- 0 to neighboursNumber-1){//Computes the average weight
          val neighbourID = neighbours(i)//Neighbour ID
          val weight : Double = {
            if(useEntropy){
              localWeights(neighbourID)*(entropies(neighbourID)/cbs(neighbourID))
            }
            else{
              localWeights(neighbourID)
            }
          }//Value of ARCS for this neighbour

          if(useEntropy){
            localWeights.update(neighbourID, weight)
          }
          if(thresholdType == ThresholdTypes.AVG){//If the threshold it is the AVG sums all the weights
            acc += weight
          }
          else if(weight > acc){//Else if max/2 compute the local maximum
            acc = weight
          }
        }

        val threshold = {//Pruning threshold
          if(thresholdType == ThresholdTypes.AVG){
            acc/neighboursNumber.toFloat
          }
          else{
            acc/2.0
          }
        }

        var edges : List[UnweightedEdge] = Nil

        for(i <- 0 to neighboursNumber-1) {//For each neighbour
          if(localWeights(neighbours(i)) >= threshold){//If the  neighbour has a weight greater than the threshold
            cont += 1//Increments the counter that keep the number of keeped neighbours
            if(profileID < neighbours(i)) {//The groundtruth contains (ID dataset 1, ID dataset2), I have to look the profile with lower ID first
              if(groundtruth.value.contains((profileID, neighbours(i)))){//If this elements is in the groundtruth
                edges = UnweightedEdge(profileID, neighbours(i)) :: edges //Generates the edge to keep
              }
            }
            else{//Same operation
              if(groundtruth.value.contains((neighbours(i), profileID))) {
                edges = UnweightedEdge(neighbours(i), profileID) :: edges
              }
            }
          }
          localWeights.update(neighbours(i), 0)//Resets the neighbour weight for the next iteration
          if(useEntropy){
            cbs.update(neighbours(i), 0)
            entropies.update(neighbours(i), 0)
          }
        }

        neighboursNumber = 0//Resets neighbours number for the next iteration
        acc = 0//Reset global weight for the next iteration/

        (cont.toDouble, edges)//Returns number of keeped neighbours and the matches
      }
    }
  }

  def WNPJS(profileBlocksFiltered : RDD[ProfileBlocks],//RDD(profile, [blocks ids])
             blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]],//(block id, ([dataset 1 blocks], [dataset 2 blocks])
             maxID : Int,//MaxProfileID
             separatorID : Long,//Separator ID
             groundtruth : Broadcast[scala.collection.immutable.Set[(Long, Long)]],//Groundtruth
             thresholdType : String = ThresholdTypes.AVG,//Threshold type
             profileBlocksSizeIndex : Broadcast[scala.collection.Map[Long, Int]],
             useEntropy : Boolean = false,
             blocksEntropies : Broadcast[scala.collection.Map[Long, Double]] = null//(block id, entropy)
            )
  : RDD[(Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions {partition => //For each partition
      val localWeights = Array.fill[Double](maxID+1){0}//Contains the neighbours weights
      val neighbours = Array.ofDim[Int](maxID+1)//Contains the neighbours IDs
      var neighboursNumber = 0//Keeps the number of neighbours
      var acc : Double = 0//Accumulator, used to compute the AVG or to keep the local maximum
      val entropies = {
        if(useEntropy){
          Array.fill[Double](maxID + 1) {0.0}
        }
        else{
          Array.fill[Double](0) {0.0}
        }
      }//Contains the sums of entropies

      partition.map{pb => //Maps the elements contained in the partition, they are a list of (profilee ID, [blocks which contains the profile])
        val profileID = pb.profileID//ProfileID
        val profileBlocks = pb.blocks//Blocks in which it appears
        val numberOfProfileBlocks = profileBlocks.size////Number of blocks in which it appears

        profileBlocks.foreach {block => //Foreach block
          val blockID = block.blockID//Block ID
          val blockProfiles = blockIndex.value.get(blockID)//Other profiles in the same block
          if(blockProfiles.isDefined){//Check if exists
            val profilesIDs = {//Gets the others profiles IDs
              if(separatorID >= 0 && profileID <= separatorID){//If we are in a Clean-Clean context and the profile ID is in the first dataset, then his neighbours are in the second dataset
                blockProfiles.get._2
              }
              else{//Otherwise they are in the first dataset
                blockProfiles.get._1
              }
            }

            val blockEntropy = {
              if(useEntropy){
                val e = blocksEntropies.value.get(blockID)
                if(e.isDefined){
                  e.get
                }
                else{
                  0.0
                }
              }
              else{
                0.0
              }
            }

            profilesIDs.foreach {secondProfileID => //For each neighbour computes the weight
              val neighbourID = secondProfileID.toInt//neighbour ID
              val neighbourWeight = localWeights(neighbourID)+1//New neighbour weight
              localWeights.update(neighbourID, neighbourWeight)//Update the neighbour weight
              if(useEntropy){
                val neighbourEntropy = entropies(neighbourID)+blockEntropy//New neighbour entropy
                entropies.update(neighbourID, neighbourEntropy)//Update the neighbour entropy
              }
              if(neighbourWeight == 1){//If its equal to 1 mean that this neighbour it is the first time that this neighbour appears
                neighbours.update(neighboursNumber, neighbourID)//Add the neighbour to the neighbours list
                neighboursNumber += 1//Increment the number of neighbour
              }
            }
          }
        }

        for(i <- 0 to neighboursNumber-1){//Computes the Jaccard Similarity
          val neighbourID = neighbours(i)//Neighbour ID
          val commonBlocks = localWeights(neighbourID)//Number of common blocks between this neighbour
          val JS = {
            if(useEntropy){
              (commonBlocks / (numberOfProfileBlocks + profileBlocksSizeIndex.value(neighbourID) - commonBlocks))*(entropies(neighbourID)/commonBlocks)
            }
            else{
              commonBlocks / (numberOfProfileBlocks + profileBlocksSizeIndex.value(neighbourID) - commonBlocks)//Jaccard Similarity
            }
          }
          localWeights.update(neighbourID, JS)//Update the local neighbour weight
          if(thresholdType == ThresholdTypes.AVG){//If the threshold it is the AVG sums all the weights
            acc += JS
          }
          else if(JS > acc){//Else if max/2 compute the local maximum
            acc = JS
          }
        }

        var cont = 0//Counts the number of keeped neighbour
        val threshold : Double = {//Pruning threshold
          if(thresholdType == ThresholdTypes.AVG){
            acc/neighboursNumber.toDouble
          }
          else{
            acc/2.0
          }
        }

        var edges : List[UnweightedEdge] = Nil

        for(i <- 0 to neighboursNumber-1) {//For each neighbour
          if(localWeights(neighbours(i)) >= threshold){//If the  neighbour has a weight greater than the threshold
            cont += 1//Increments the counter that keep the number of keeped neighbours
            if(profileID < neighbours(i)) {//The groundtruth contains (ID dataset 1, ID dataset2), I have to look the profile with lower ID first
              if(groundtruth.value.contains((profileID, neighbours(i)))){//If this elements is in the groundtruth
                edges = UnweightedEdge(profileID, neighbours(i)) :: edges //Generates the edge to keep
              }
            }
            else{//Same operation
              if(groundtruth.value.contains((neighbours(i), profileID))) {
                edges = UnweightedEdge(neighbours(i), profileID) :: edges
              }
            }
          }
          localWeights.update(neighbours(i), 0)//Resets the neighbour weight for the next iteration
          if(useEntropy){
            entropies.update(neighbours(i), 0)//Resets the entropy weight
          }
        }

        neighboursNumber = 0//Resets neighbours number for the next iteration
        acc = 0//Reset global weight for the next iteration/

        (cont.toDouble, edges)//Returns number of keeped neighbours and the matches
      }
    }
  }

  def WNPCBS(profileBlocksFiltered : RDD[ProfileBlocks],//RDD(profile, [blocks ids])
             blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]],//(block id, ([dataset 1 blocks], [dataset 2 blocks])
             maxID : Int,//MaxProfileID
             separatorID : Long,//Separator ID
             groundtruth : Broadcast[scala.collection.immutable.Set[(Long, Long)]],//Groundtruth
             thresholdType : String = ThresholdTypes.AVG,//Threshold type
             useEntropy : Boolean = false,
             blocksEntropies : Broadcast[scala.collection.Map[Long, Double]] = null,//(block id, entropy)
             blockCluster : Broadcast[scala.collection.Map[Long, Double]] = null
            ) 
  : RDD[(Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions {partition => //For each partition
      val localWeights = Array.fill[Double](maxID+1){0}//Contains the neighbours weights
      val neighbours = Array.ofDim[Int](maxID+1)//Contains the neighbours IDs
      var neighboursNumber = 0//Keeps the number of neighbours
      var acc : Double = 0//Accumulator, used to compute the AVG or to keep the local maximum
      val entropies = {
        if(useEntropy){
          Array.fill[Double](maxID + 1) {0.0}
        }
        else{
          Array.fill[Double](0) {0.0}
        }
      }//Contains the sums of entropies
      val clustersMatch = Array.fill[String](maxID+1){""}

      @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")

      partition.map{pb => //Maps the elements contained in the partition, they are a list of (profilee ID, [blocks which contains the profile])
        val profileID = pb.profileID//ProfileID
        val profileBlocks = pb.blocks//Blocks in which it appears

        profileBlocks.foreach {block => //Foreach block
          val blockID = block.blockID//Block ID
          val clusterID = blockCluster.value.get(blockID).get
          val blockProfiles = blockIndex.value.get(blockID)//Other profiles in the same block
          if(blockProfiles.isDefined){//Check if exists
            val profilesIDs = {//Gets the others profiles IDs
              if(separatorID >= 0 && profileID <= separatorID){//If we are in a Clean-Clean context and the profile ID is in the first dataset, then his neighbours are in the second dataset
                blockProfiles.get._2
              }
              else{//Otherwise they are in the first dataset
                blockProfiles.get._1
              }
            }

            val blockEntropy = {
              if(useEntropy){
                val e = blocksEntropies.value.get(blockID)
                if(e.isDefined){
                  e.get
                }
                else{
                  0.0
                }
              }
              else{
                0.0
              }
            }
            
            profilesIDs.foreach {secondProfileID => //For each neighbour computes the weight
              val neighbourID = secondProfileID.toInt//neighbour ID
              val weight = {
                if(useEntropy){
                  blockEntropy
                }
                else{
                  1
                }
              }
              val neighbourWeight = localWeights(neighbourID)+weight//New neighbour weight
              localWeights.update(neighbourID, neighbourWeight)//Update the neighbour weight
              clustersMatch.update(neighbourID, clustersMatch(neighbourID)+","+clusterID)
              if(neighbourWeight == weight){//If its equal to weight means that this neighbour it is the first time that this neighbour appears
                neighbours.update(neighboursNumber, neighbourID)//Add the neighbour to the neighbours list
                neighboursNumber += 1//Increment the number of neighbour
              }
              if(thresholdType == ThresholdTypes.AVG){//If the threshold it is the AVG sums all the weights
                acc += weight
              }
              else if(neighbourWeight > acc){//Else if max/2 compute the local maximum
                acc = neighbourWeight
              }
            }
          }
        }

        var cont = 0//Counts the number of keeped neighbour
        val threshold : Double = {//Pruning threshold
          if(thresholdType == ThresholdTypes.AVG){
            acc/neighboursNumber.toDouble
          }
          else{
            acc/2.0
          }
        }

        //log.info("soglia - "+threshold)
        var edges : List[UnweightedEdge] = Nil

        for(i <- 0 to neighboursNumber-1) {//For each neighbour
          //log.info("w - "+localWeights(neighbours(i)))
          if(localWeights(neighbours(i)) >= threshold){//If the  neighbour has a weight greater than the threshold
            cont += 1//Increments the counter that keep the number of keeped neighbours
            if(profileID < neighbours(i)) {//The groundtruth contains (ID dataset 1, ID dataset2), I have to look the profile with lower ID first
              if(groundtruth.value.contains((profileID, neighbours(i)))){//If this elements is in the groundtruth
                edges = UnweightedEdge(profileID, neighbours(i)) :: edges //Generates the edge to keep
                log.info("cm - "+clustersMatch(neighbours(i)))
              }
              else{
                log.info("cn - "+clustersMatch(neighbours(i)))
              }
            }
            else{//Same operation
              if(groundtruth.value.contains((neighbours(i), profileID))) {
                edges = UnweightedEdge(neighbours(i), profileID) :: edges
                log.info("cm - "+clustersMatch(neighbours(i)))
              }
              else{
                log.info("cn - "+clustersMatch(neighbours(i)))
              }
            }
          }
          localWeights.update(neighbours(i), 0)//Resets the neighbour weight for the next iteration
          clustersMatch.update(neighbours(i), "")
          if(useEntropy){
            entropies.update(neighbours(i), 0)//Resets the entropy weight
          }
        }

        neighboursNumber = 0//Resets neighbours number for the next iteration
        acc = 0//Reset global weight for the next iteration/

        (cont.toDouble, edges)//Returns number of keeped neighbours and the matches
      }
    }
  }
  
  
  def WNPChiSquare(profileBlocksFiltered : RDD[ProfileBlocks],
                   blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]],
                   maxID : Int,
                   separatorID : Long,
                   groundtruth : Broadcast[scala.collection.immutable.Set[(Long, Long)]],
                   thresholdType : String = ThresholdTypes.AVG,
                   profileBlocksSizeIndex : Broadcast[scala.collection.Map[Long, Int]],
                   useEntropy : Boolean = false,
                   blocksEntropies : Broadcast[scala.collection.Map[Long, Double]] = null//(block id, entropy)
                  )
  : RDD[(Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions {partition => //For each partition
      val totalNumberOfBlocks : Double = blockIndex.value.size.toDouble
      val localWeights = Array.fill[Double](maxID+1){0}//Contains the neighbours weights
      val neighbours = Array.ofDim[Int](maxID+1)//Contains the neighbours IDs
      val entropies = {
        if(useEntropy){
          Array.fill[Double](maxID + 1) {0.0}
        }
        else{
          Array.fill[Double](0) {0.0}
        }
      }//Contains the sums of entropies
      var neighboursNumber = 0//Keeps the number of neighbours
      var acc : Double = 0//Accumulator, used to compute the AVG or to keep the local maximum

      partition.map{pb => //Maps the elements contained in the partition, they are a list of (profile ID, [blocks which contains the profile])
        val profileID = pb.profileID//ProfileID
        val profileBlocks = pb.blocks//Blocks in which it appears

        profileBlocks.foreach {block => //Foreach block
          val blockID = block.blockID//Block ID
          val blockProfiles = blockIndex.value.get(blockID)//Other profiles in the same block
          if(blockProfiles.isDefined){//Check if exists
            val profilesIDs = {//Gets the others profiles IDs
              if(separatorID >= 0 && profileID <= separatorID){//If we are in a Clean-Clean context and the profile ID is in the first dataset, then his neighbours are in the second dataset
                blockProfiles.get._2
              }
              else{//Otherwise they are in the first dataset
                blockProfiles.get._1
              }
            }
            val blockEntropy = {
              if(useEntropy){
                val e = blocksEntropies.value.get(blockID)
                if(e.isDefined){
                  e.get
                }
                else{
                  0.0
                }
              }
              else{
                0.0
              }
            }

            profilesIDs.foreach {secondProfileID => //For each neighbour computes the weight
              val neighbourID = secondProfileID.toInt//neighbour ID
              val neighbourWeight = localWeights(neighbourID)+1//New neighbour weight
              localWeights.update(neighbourID, neighbourWeight)//Update the neighbour weight
              if(useEntropy){
                val neighbourEntropy = entropies(neighbourID)+blockEntropy//New neighbour entropy
                entropies.update(neighbourID, neighbourEntropy)//Update the neighbour entropy
              }
              if(neighbourWeight == 1){//If its equal to 1 mean that this neighbour it is the first time that this neighbour appears
                neighbours.update(neighboursNumber, neighbourID)//Add the neighbour to the neighbours list
                neighboursNumber += 1//Increment the number of neighbour
              }
            }
          }
        }

        var cont = 0//Counts the number of keeped neighbour

        for(i <- 0 to neighboursNumber-1){//Computes the Jaccard Similarity
          val neighbourID = neighbours(i)//Neighvbour ID
          val CBS : Double = localWeights(neighbourID)//Number of common blocks between this neighbour
          val neighbourNumBlocks : Double = profileBlocksSizeIndex.value(neighbourID)
          val currentProfileNumBlocks : Double = profileBlocks.size

          val CMat = Array.ofDim[Double](3,3)
          CMat(0)(0) = CBS
          CMat(0)(1) = neighbourNumBlocks-CBS
          CMat(0)(2) = neighbourNumBlocks

          CMat(1)(0) = currentProfileNumBlocks-CBS
          CMat(1)(1) = totalNumberOfBlocks-(neighbourNumBlocks+currentProfileNumBlocks-CBS)
          CMat(1)(2) = totalNumberOfBlocks-neighbourNumBlocks

          CMat(2)(0) = currentProfileNumBlocks
          CMat(2)(1) = totalNumberOfBlocks - currentProfileNumBlocks

          var weight : Double = 0
          var expectedValue : Double = 0
          for(i <- 0 to 1){
            for(j <- 0 to 1){
              expectedValue = (CMat(i)(2)*CMat(2)(j))/totalNumberOfBlocks
              weight += Math.pow((CMat(i)(j)-expectedValue),2)/expectedValue
            }
          }

          if(useEntropy){
            val averageEntropy : Double = entropies(neighbourID)/CBS
            weight = weight * averageEntropy
          }

          localWeights.update(neighbourID, weight)//Update the local neighbour weight

          if(thresholdType == ThresholdTypes.AVG){//If the threshold it is the AVG sums all the weights
            acc += weight
          }
          else if(weight > acc){//Else if max/2 compute the local maximum
            acc = weight
          }
        }

        val threshold : Double = {//Pruning threshold
          if(thresholdType == ThresholdTypes.AVG){
            acc/neighboursNumber.toDouble
          }
          else{
            acc/2.0
          }
        }

        var edges : List[UnweightedEdge] = Nil

        for(i <- 0 to neighboursNumber-1) {//For each neighbour
          if(localWeights(neighbours(i)) >= threshold){//If the  neighbour has a weight greater than the threshold
            cont += 1//Increments the counter that keep the number of keeped neighbours
            if(profileID < neighbours(i)) {//The groundtruth contains (ID dataset 1, ID dataset2), I have to look the profile with lower ID first
              if(groundtruth.value.contains((profileID, neighbours(i)))){//If this elements is in the groundtruth
                edges = UnweightedEdge(profileID, neighbours(i)) :: edges //Generates the edge to keep
              }
            }
            else{//Same operation
              if(groundtruth.value.contains((neighbours(i), profileID))) {
                edges = UnweightedEdge(neighbours(i), profileID) :: edges
              }
            }
          }
          localWeights.update(neighbours(i), 0)//Resets the neighbour weight for the next iteration
          if(useEntropy){
            entropies.update(neighbours(i), 0)//Resets the entropy weight
          }
        }

        neighboursNumber = 0//Resets neighbours number for the next iteration
        acc = 0//Reset global weight for the next iteration/

        (cont.toDouble, edges)//Returns number of keeped neighbours and the matches
      }
    }
  }
}
