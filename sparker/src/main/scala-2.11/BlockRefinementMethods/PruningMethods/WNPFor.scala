package BlockRefinementMethods.PruningMethods

import BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
import DataStructures.{ProfileBlocks, UnweightedEdge, WeightedEdge}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

/**
  * Weight Node Pruning
  * Experimental, we have to finish this class
  *
  * @author Luca Gagliardelli
  * @since 2017/01/24
  */
object WNPFor {

  /**
    * Types of threeshold
    * */
  object ThresholdTypes {
    /** Local maximum divided by 2 */
    val MAX_FRACT_2 = "maxdiv2"
    /** Average of all local weights */
    val AVG = "avg"
  }

  def WNPEntropyChiSquare(profileBlocksFiltered : RDD[ProfileBlocks], blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]], maxID : Int, separatorID : Long, groundtruth : Broadcast[scala.collection.immutable.Set[(Long, Long)]], blocksEntropies : Broadcast[scala.collection.Map[Long, Double]], thresholdType : String = ThresholdTypes.AVG, profileBlocksSizeIndex : Broadcast[scala.collection.Map[Long, Int]] = null) : RDD[(Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions {partition => //For each partition
      val totalNumberOfBlocks : Double = blockIndex.value.size.toDouble
      val localWeights = Array.fill[Double](maxID+1){0}//Contains the neighbours weights
      val neighbours = Array.ofDim[Int](maxID+1)//Contains the neighbours IDs
      val entropies = Array.fill[Double](maxID+1){0.0}//Contains the sums of entropies
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
              val e = blocksEntropies.value.get(blockID)
              if(e.isDefined){
                e.get
              }
              else{
                0.0
              }
            }

            profilesIDs.foreach {secondProfileID => //For each neighbour computes the weight
              val neighbourID = secondProfileID.toInt//neighbour ID
              val neighbourWeight = localWeights(neighbourID)+1//New neighbour weight
              val neighbourEntropy = entropies(neighbourID)+blockEntropy//New neighbour entropy
              localWeights.update(neighbourID, neighbourWeight)//Update the neighbour weight
              entropies.update(neighbourID, neighbourEntropy)//Update the neighbour entropy
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
          val averageEntropy : Double = entropies(neighbourID)/CBS
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
              //println("EXPECTED VALUE "+expectedValue)
              weight += Math.pow((CMat(i)(j)-expectedValue),2)/expectedValue
            }
          }


          /*println()
          println("Number of blocks "+totalNumberOfBlocks)
          println("Neighbour ID "+neighbourID)
          println("CBS "+CBS)
          println("AverageEntropy "+averageEntropy)
          println("neighbourNumBlocks "+neighbourNumBlocks)
          println("profileNumBlocks "+currentProfileNumBlocks)
          println("MATRICE ")
          println(CMat(0)(0)+" "+CMat(0)(1)+" "+CMat(0)(2))
          println(CMat(1)(0)+" "+CMat(1)(1)+" "+CMat(2)(1))
          println(CMat(2)(0)+" "+CMat(2)(1)+" "+CMat(2)(2))
          println("PESO "+weight)

          println()
          println()*/


          weight = weight * averageEntropy

          localWeights.update(neighbourID, weight)//Update the local neighbour weight
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
          entropies.update(neighbours(i), 0)//Resets the entropy weight
        }

        neighboursNumber = 0//Resets neighbours number for the next iteration
        acc = 0//Reset global weight for the next iteration/

        (cont.toDouble, edges)//Returns number of keeped neighbours and the matches
      }
    }
  }

  def WNPArcs(profileBlocksFiltered : RDD[ProfileBlocks], blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]], maxID : Int, separatorID : Long, groundtruth : Broadcast[scala.collection.immutable.Set[(Long, Long)]], blocksEntropies : Broadcast[scala.collection.Map[Long, Double]], thresholdType : String = ThresholdTypes.AVG, profileBlocksSizeIndex : Broadcast[scala.collection.Map[Long, Int]] = null) : RDD[(Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions {partition => //For each partition
      val localWeights = Array.fill[Double](maxID+1){0}//Contains the neighbours weights
      val neighbours = Array.ofDim[Int](maxID+1)//Contains the neighbours IDs
      var neighboursNumber = 0//Keeps the number of neighbours
      var acc : Double = 0//Accumulator, used to compute the AVG or to keep the local maximum

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

            profilesIDs.foreach {secondProfileID => //For each neighbour computes the weight
              val neighbourID = secondProfileID.toInt//neighbour ID
              val neighbourWeight = localWeights(neighbourID)//New neighbour weight
              localWeights.update(neighbourID, neighbourWeight+(1/comparisons))//Update the neighbour weight
              if(neighbourWeight == 0){//If its equal to 0 mean that this neighbour it is the first time that this neighbour appears
                neighbours.update(neighboursNumber, neighbourID)//Add the neighbour to the neighbours list
                neighboursNumber += 1//Increment the number of neighbour
              }
            }
          }
        }

        var cont = 0//Counts the number of keeped neighbour

        for(i <- 0 to neighboursNumber-1){//Computes the average weight
          val neighbourID = neighbours(i)//Neighbour ID
          val weight : Double = localWeights(neighbourID)//Value of ARCS for this neighbour
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
        }

        neighboursNumber = 0//Resets neighbours number for the next iteration
        acc = 0//Reset global weight for the next iteration/

        (cont.toDouble, edges)//Returns number of keeped neighbours and the matches
      }
    }
  }

  def WNPEntropy(profileBlocksFiltered : RDD[ProfileBlocks], blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]], maxID : Int, separatorID : Long, groundtruth : Broadcast[scala.collection.immutable.Set[(Long, Long)]], blocksEntropies : Broadcast[scala.collection.Map[Long, Double]], thresholdType : String = ThresholdTypes.AVG, weightType: String = WeightTypes.CBS, profileBlocksSizeIndex : Broadcast[scala.collection.Map[Long, Int]] = null) : RDD[(Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions {partition => //For each partition
      val localWeights = Array.fill[Float](maxID+1){0}//Contains the neighbours weights
      val neighbours = Array.ofDim[Int](maxID+1)//Contains the neighbours IDs
      val entropies = Array.fill[Double](maxID+1){0.0}//Contains the sums of entropies
      var neighboursNumber = 0//Keeps the number of neighbours
      var acc : Float = 0//Accumulator, used to compute the AVG or to keep the local maximum

      partition.map{pb => //Maps the elements contained in the partition, they are a list of (profilee ID, [blocks which contains the profile])
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
              val e = blocksEntropies.value.get(blockID)
              if(e.isDefined){
                e.get
              }
              else{
                0.0
              }
            }

            profilesIDs.foreach {secondProfileID => //For each neighbour computes the weight
              val neighbourID = secondProfileID.toInt//neighbour ID
              val neighbourWeight = localWeights(neighbourID)+1//New neighbour weight
              val neighbourEntropy = entropies(neighbourID)+blockEntropy//New neighbour entropy
              localWeights.update(neighbourID, neighbourWeight)//Update the neighbour weight
              entropies.update(neighbourID, neighbourEntropy)//Update the neighbour entropy
              if(neighbourWeight == 1){//If its equal to 1 mean that this neighbour it is the first time that this neighbour appears
                neighbours.update(neighboursNumber, neighbourID)//Add the neighbour to the neighbours list
                neighboursNumber += 1//Increment the number of neighbour
              }
            }
          }
        }

        var cont = 0//Counts the number of keeped neighbour

        if(weightType == WeightTypes.JS){
          for(i <- 0 to neighboursNumber-1){//Computes the Jaccard Similarity
            val neighbourID = neighbours(i)//Neighbour ID
            val commonBlocks = localWeights(neighbourID)//Number of common blocks between this neighbour
            val averageEntropy = entropies(neighbourID)/commonBlocks
            val JS = (commonBlocks / (profileBlocks.size + profileBlocksSizeIndex.value(neighbourID) - commonBlocks))*averageEntropy.toFloat//Jaccard Similarity
            localWeights.update(neighbourID, JS)//Update the local neighbour weight
            if(thresholdType == ThresholdTypes.AVG){//If the threshold it is the AVG sums all the weights
              acc += JS
            }
            else if(JS > acc){//Else if max/2 compute the local maximum
              acc = JS
            }
          }
        }
        else{
          for(i <- 0 to neighboursNumber-1){//Computes the CBS*entropy
            val neighbourID = neighbours(i)//Neighbour ID
            val JS = entropies(neighbourID).toFloat
            localWeights.update(neighbourID, JS)//Update the local neighbour weight
            if(thresholdType == ThresholdTypes.AVG){//If the threshold it is the AVG sums all the weights
              acc += JS
            }
            else if(JS > acc){//Else if max/2 compute the local maximum
              acc = JS
            }
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
          entropies.update(neighbours(i), 0)//Resets the entropy weight
        }

        neighboursNumber = 0//Resets neighbours number for the next iteration
        acc = 0//Reset global weight for the next iteration/

        (cont.toDouble, edges)//Returns number of keeped neighbours and the matches
      }
    }
  }

  def WNP_dataset(profileBlocksFiltered : RDD[ProfileBlocks], blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]], maxID : Int, separatorID : Long, groundtruth : Broadcast[scala.collection.immutable.Set[(Long, Long)]], thresholdType : String = ThresholdTypes.AVG, weightType: String = WeightTypes.CBS, profileBlocksSizeIndex : Broadcast[scala.collection.Map[Long, Int]] = null) : Dataset[(Double, List[(Long, Long, Float)])] = {

    val sc = SparkContext.getOrCreate()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("spark.sql.shuffle.partitions", "3000")
    import sqlContext.implicits._

    val test = profileBlocksFiltered.map { pb =>
      val blocks = pb.blocks.map(x => x.blockID).toArray
      (pb.profileID, blocks)
    }.toDS()

    test.mapPartitions {partition => //For each partition
      val localWeights = Array.fill[Float](maxID+1){0}//Contains the neighbours weights
      val neighbours = Array.ofDim[Int](maxID+1)//Contains the neighbours IDs
      var neighboursNumber = 0//Keeps the number of neighbours
      var acc : Float = 0//Accumulator, used to compute the AVG or to keep the local maximum

      partition.map{pb => //Maps the elements contained in the partition, they are a list of (profilee ID, [blocks which contains the profile])
        val profileID = pb._1//ProfileID
        val profileBlocks = pb._2//Blocks in which it appears

        profileBlocks.foreach {blockID => //Foreach block
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
            profilesIDs.foreach {secondProfileID => //For each neighbour computes the weight
              val neighbourID = secondProfileID.toInt//neighbour ID
              val neighbourWeight = localWeights(neighbourID)+1//New neighbour weight
              localWeights.update(neighbourID, neighbourWeight)//Update the neighbour weight
              if(neighbourWeight == 1){//If its equal to 1 mean that this neighbour it is the first time that this neighbour appears
                neighbours.update(neighboursNumber, neighbourID)//Add the neighbour to the neighbours list
                neighboursNumber += 1//Increment the number of neighbour
              }
            }
          }
        }

        var cont = 0//Counts the number of keeped neighbour

        if(weightType == WeightTypes.JS){
          for(i <- 0 to neighboursNumber-1){//Computes the Jaccard Similarity
            val neighbourID = neighbours(i)//Neighbour ID
            val commonBlocks = localWeights(neighbourID)//Number of common blocks between this neighbour
            val JS = (commonBlocks / (profileBlocks.size + profileBlocksSizeIndex.value(neighbourID) - commonBlocks))//Jaccard Similarity
            localWeights.update(neighbourID, JS)//Update the local neighbour weight
            if(thresholdType == ThresholdTypes.AVG){//If the threshold it is the AVG sums all the weights
              acc += JS
            }
            else if(JS > acc){//Else if max/2 compute the local maximum
              acc = JS
            }
          }
        }
        else{
          for(i <- 0 to neighboursNumber-1){//Computes the CBS*entropy
            val neighbourID = neighbours(i)//Neighbour ID
            val commonBlocks = localWeights(neighbourID)//Number of common blocks between this neighbour
            if(thresholdType == ThresholdTypes.AVG){//If the threshold it is the AVG sums all the weights
              acc += commonBlocks
            }
            else if(commonBlocks > acc){//Else if max/2 compute the local maximum
              acc = commonBlocks
            }
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

        var edges : List[(Long, Long, Float)] = Nil

        for(i <- 0 to neighboursNumber-1) {//For each neighbour
          if(localWeights(neighbours(i)) >= threshold){//If the  neighbour has a weight greater than the threshold
            cont += 1//Increments the counter that keep the number of keeped neighbours
            if(profileID < neighbours(i)) {//The groundtruth contains (ID dataset 1, ID dataset2), I have to look the profile with lower ID first
              if(groundtruth.value.contains((profileID, neighbours(i)))){//If this elements is in the groundtruth
                edges = (profileID, neighbours(i).toLong, localWeights(neighbours(i))) :: edges //Generates the edge to keep
              }
            }
            else{//Same operation
              if(groundtruth.value.contains((neighbours(i), profileID))) {
                edges = (neighbours(i).toLong, profileID, localWeights(neighbours(i))) :: edges
              }
            }
          }
          localWeights.update(neighbours(i), 0)//Resets the neighbour weight for the next iteration
        }

        neighboursNumber = 0//Resets neighbours number for the next iteration
        acc = 0//Reset global weight for the next iteration/

        (cont.toDouble, edges)//Returns number of keeped neighbours and the matches
      }
    }
  }

  def WNP(profileBlocksFiltered : RDD[ProfileBlocks], blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]], maxID : Int, separatorID : Long, groundtruth : Broadcast[scala.collection.immutable.Set[(Long, Long)]], thresholdType : String = ThresholdTypes.AVG, weightType: String = WeightTypes.CBS, profileBlocksSizeIndex : Broadcast[scala.collection.Map[Long, Int]] = null) : RDD[(Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions {partition => //For each partition
      val localWeights = Array.fill[Float](maxID+1){0}//Contains the neighbours weights
      val neighbours = Array.ofDim[Int](maxID+1)//Contains the neighbours IDs
      var neighboursNumber = 0//Keeps the number of neighbours
      var acc : Float = 0//Accumulator, used to compute the AVG or to keep the local maximum

      partition.map{pb => //Maps the elements contained in the partition, they are a list of (profilee ID, [blocks which contains the profile])
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
            profilesIDs.foreach {secondProfileID => //For each neighbour computes the weight
              val neighbourID = secondProfileID.toInt//neighbour ID
              val neighbourWeight = localWeights(neighbourID)+1//New neighbour weight
              localWeights.update(neighbourID, neighbourWeight)//Update the neighbour weight
              if(neighbourWeight == 1){//If its equal to 1 mean that this neighbour it is the first time that this neighbour appears
                neighbours.update(neighboursNumber, neighbourID)//Add the neighbour to the neighbours list
                neighboursNumber += 1//Increment the number of neighbour
              }
            }
          }
        }

        var cont = 0//Counts the number of keeped neighbour

        if(weightType == WeightTypes.JS){
          for(i <- 0 to neighboursNumber-1){//Computes the Jaccard Similarity
            val neighbourID = neighbours(i)//Neighbour ID
            val commonBlocks = localWeights(neighbourID)//Number of common blocks between this neighbour
            val JS = (commonBlocks / (profileBlocks.size + profileBlocksSizeIndex.value(neighbourID) - commonBlocks))//Jaccard Similarity
            localWeights.update(neighbourID, JS)//Update the local neighbour weight
            if(thresholdType == ThresholdTypes.AVG){//If the threshold it is the AVG sums all the weights
              acc += JS
            }
            else if(JS > acc){//Else if max/2 compute the local maximum
              acc = JS
            }
          }
        }
        else{
          for(i <- 0 to neighboursNumber-1){//Computes the CBS*entropy
            val neighbourID = neighbours(i)//Neighbour ID
            val commonBlocks = localWeights(neighbourID)//Number of common blocks between this neighbour
            if(thresholdType == ThresholdTypes.AVG){//If the threshold it is the AVG sums all the weights
              acc += commonBlocks
            }
            else if(commonBlocks > acc){//Else if max/2 compute the local maximum
              acc = commonBlocks
            }
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
        }

        neighboursNumber = 0//Resets neighbours number for the next iteration
        acc = 0//Reset global weight for the next iteration/

        (cont.toDouble, edges)//Returns number of keeped neighbours and the matches
      }
    }
  }

  def WNPCBS(profileBlocksFiltered : RDD[ProfileBlocks], blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]], maxID : Int, separatorID : Long, groundtruth : Broadcast[scala.collection.immutable.Set[(Long, Long)]], thresholdType : String = ThresholdTypes.AVG) : RDD[(Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions {partition => //For each partition
      val localWeights = Array.fill[Float](maxID+1){0}//Contains the neighbours weights
      val neighbours = Array.ofDim[Int](maxID+1)//Contains the neighbours IDs
      var neighboursNumber = 0//Keeps the number of neighbours
      var acc : Float = 0//Accumulator, used to compute the AVG or to keep the local maximum

      partition.map{pb => //Maps the elements contained in the partition, they are a list of (profilee ID, [blocks which contains the profile])
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
            profilesIDs.foreach {secondProfileID => //For each neighbour computes the weight
              val neighbourID = secondProfileID.toInt//neighbour ID
              val neighbourWeight = localWeights(neighbourID)+1//New neighbour weight
              localWeights.update(neighbourID, neighbourWeight)//Update the neighbour weight
              if(neighbourWeight == 1){//If its equal to 1 mean that this neighbour it is the first time that this neighbour appears
                neighbours.update(neighboursNumber, neighbourID)//Add the neighbour to the neighbours list
                neighboursNumber += 1//Increment the number of neighbour
              }
              if(thresholdType == ThresholdTypes.AVG){//If the threshold it is the AVG sums all the weights
                acc += 1
              }
              else if(neighbourWeight > acc){//Else if max/2 compute the local maximum
                acc = neighbourWeight
              }
            }
          }
        }

        var cont = 0//Counts the number of keeped neighbour
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
        }

        neighboursNumber = 0//Resets neighbours number for the next iteration
        acc = 0//Reset global weight for the next iteration/

        (cont.toDouble, edges)//Returns number of keeped neighbours and the matches
      }
    }
  }

  def WNPJS(profileBlocksFiltered : RDD[ProfileBlocks], blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]], profileBlocksSizeIndex : Broadcast[scala.collection.Map[Long, Int]], maxID : Int, separatorID : Long, groundtruth : Broadcast[scala.collection.immutable.Set[(Long, Long)]], thresholdType : String = ThresholdTypes.AVG) : RDD[(Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions {partition => //For each partition
      val localWeights = Array.fill[Float](maxID+1){0}//Contains the neighbours weights
      val neighbours = Array.ofDim[Int](maxID+1)//Contains the neighbours IDs
      var neighboursNumber = 0//Keeps the number of neighbours
      var acc : Float = 0//Accumulator, used to compute the AVG or to keep the local maximum

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
            profilesIDs.foreach {secondProfileID => //For each neighbour computes the weight
              val neighbourID = secondProfileID.toInt//neighbour ID
              val neighbourWeight = localWeights(neighbourID)+1//New neighbour weight
              localWeights.update(neighbourID, neighbourWeight)//Update the neighbour weight
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
          val JS = commonBlocks / (numberOfProfileBlocks + profileBlocksSizeIndex.value(neighbourID) - commonBlocks)//Jaccard Similarity
          localWeights.update(neighbourID, JS)//Update the local neighbour weight
          if(thresholdType == ThresholdTypes.AVG){//If the threshold it is the AVG sums all the weights
            acc += JS
          }
          else if(JS > acc){//Else if max/2 compute the local maximum
            acc = JS
          }
        }

        var cont = 0//Counts the number of keeped neighbour
        var edges : List[UnweightedEdge] = Nil
        val threshold = {//Pruning threshold
          if(thresholdType == ThresholdTypes.AVG){
            acc/neighboursNumber.toFloat
          }
          else{
            acc/2.0
          }
        }

        for(i <- 0 to neighboursNumber-1) yield {//For each neighbour
          if(localWeights(neighbours(i)) >= threshold){//If the  neighbour has a weight greater than the threshold
            cont += 1//Increments the counter that keep the number of keeped neighbours
            if(profileID < neighbours(i)) {//The groundtruth contains (ID dataset 1, ID dataset2), I have to look the profile with lower ID first
              if(groundtruth.value.contains((profileID, neighbours(i)))){//If this elements is in the groundtruth
                edges = UnweightedEdge(profileID, neighbours(i)) :: edges//Generates the edge to keep
              }
            }
            else{//Same operation
              if(groundtruth.value.contains((neighbours(i), profileID))) {
                edges = UnweightedEdge(neighbours(i), profileID) :: edges
              }
            }
          }
          localWeights.update(neighbours(i), 0)//Resets the neighbour weight for the next iteration
        }

        neighboursNumber = 0//Resets neighbours number for the next iteration
        acc = 0//Reset global weight for the next iteration/

        (cont.toDouble, edges)//Returns number of keeped neighbours and the matches
      }
    }
  }

  def CalcPCPQ(profileBlocksFiltered : RDD[ProfileBlocks], blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]],
               maxID : Int, separatorID : Long, groundtruth : Broadcast[scala.collection.immutable.Set[(Long, Long)]]) : RDD[(Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered mapPartitions{
      partition =>

        val arrayPesi = Array.fill[Int](maxID+1){0}             //Usato per memorizzare i pesi di ogni vicino
        val arrayVicini = Array.ofDim[Int](maxID+1)             //Usato per tenere gli ID dei miei vicini
        var numeroVicini = 0                                    //Memorizza il numero di vicini che ho

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
                  }
                }
            }


            var cont = 0                                     //Contatore che legge quanti vicini mantengo

            var edges : List[UnweightedEdge] = Nil               //Edge che verrà dato in uscita per questo profilo che corrisponde ad un match nel dataset 2 (solo se lo trova), va bene solo se clean sto metodo!

            for(i <- 0 to numeroVicini-1) {                           //Scorro i vicini che ho trovato
              if(profileID < arrayVicini(i)){                         //Aumento il contatore solo se id profiloattuale < id vicino, così li conta una volta sola
                cont += 1
              }
              if(groundtruth.value.contains((profileID, arrayVicini(i)))) {  //Il groundtruth è organizzato come (ID dataset1, ID dataset2), quindi devo cercare il profilo con ID minore
                edges = UnweightedEdge(profileID, arrayVicini(i)) :: edges //Genero l'edge che voglio tenere
              }
              else if(groundtruth.value.contains((arrayVicini(i), profileID))) {
                edges = UnweightedEdge(arrayVicini(i), profileID) :: edges
              }

              arrayPesi.update(arrayVicini(i), 0)                       //Il peso di questo vicino non mi serve più, lo resetto per il prossimo giro
            }

            numeroVicini = 0                                            //Resetto numero di vicini

            (cont.toDouble, edges)                           //Fornisco in output il numero di vicini mantenuto e il match vero
        }
    }
  }
}
