package BlockRefinementMethods.PruningMethods

import BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
import DataStructures.{BlockAbstract, ProfileBlocks, UnweightedEdge}
import Utilities.BoundedPriorityQueue
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Implements the CNP.
  * @author Luca Gagliardelli
  * @since 2017/03/06
  */
object CNPFor {

  /**
    * Computes the CNP threshold.
    * Number of neighbours to keep for each record.
    *
    * @param blocks current blocks
    * @param numberOfProfiles total number of profiles
    * */
  def computeThreshold(blocks : RDD[BlockAbstract], numberOfProfiles : Long) : Int = {
    val numElements = blocks.map(_.getAllProfiles.size).sum()
    Math.floor((numElements/numberOfProfiles)-1).toInt
  }

  def CNP(profileBlocksFiltered : RDD[ProfileBlocks], blockIndex : Broadcast[scala.collection.Map[Long, (List[Long], List[Long])]], maxID : Int, separatorID : Long, groundtruth : Broadcast[scala.collection.immutable.Set[(Long, Long)]], CNPThreshold : Int, weightType : String = WeightTypes.CBS, profileBlocksSizeIndex : Broadcast[scala.collection.Map[Long, Int]] = null) : RDD[(Double, Iterable[UnweightedEdge])] = {
    profileBlocksFiltered.mapPartitions {partition => //For each partition
      val localWeights = Array.fill[Float](maxID+1){0}//Contains the neighbours weights
      val neighbours = Array.ofDim[Int](maxID+1)//Contains the neighbours IDs
      var neighboursNumber = 0//Keeps the number of neighbours
      val neighboursToKeep = new BoundedPriorityQueue[NeighbourWithWeight](CNPThreshold)

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

        if(weightType == WeightTypes.JS){
          for(i <- 0 to neighboursNumber-1){//Computes the Jaccard Similarity
            val neighbourID = neighbours(i)//Neighbour ID
            val commonBlocks = localWeights(neighbourID)//Number of common blocks between this neighbour
            val JS = (commonBlocks / (profileBlocks.size + profileBlocksSizeIndex.value(neighbourID) - commonBlocks))//Jaccard Similarity
            neighboursToKeep += NeighbourWithWeight(neighbourID, JS)
            localWeights.update(neighbours(i), 0)//Resets the neighbour weight for the next iteration
          }
        }
        else{
          for(i <- 0 to neighboursNumber-1){//Computes the CBS*entropy
            val neighbourID = neighbours(i)//Neighbour ID
            val CBS = localWeights(neighbourID)
            neighboursToKeep += NeighbourWithWeight(neighbourID, CBS)
            localWeights.update(neighbours(i), 0)//Resets the neighbour weight for the next iteration
          }
        }

        var edges : List[UnweightedEdge] = Nil
        val candidates = neighboursToKeep.toArray

        for(i <- 0 to candidates.size-1) yield {//For each neighbour
          val neighbourID = candidates(i).neighbourID

          if(profileID < neighbourID) {//The groundtruth contains (ID dataset 1, ID dataset2), I have to look the profile with lower ID first
            if(groundtruth.value.contains((profileID, neighbourID))){//If this elements is in the groundtruth
              edges = UnweightedEdge(profileID, neighbourID) :: edges //Generates the edge to keep
            }
          }
          else{//Same operation
            if(groundtruth.value.contains((neighbourID, profileID))) {
              edges = UnweightedEdge(neighbourID, profileID) :: edges
            }
          }
        }

        neighboursNumber = 0//Resets neighbours number for the next iteration
        neighboursToKeep.clear()//Removes all the elements from the list

        (candidates.size.toDouble, edges)//Returns number of keeped neighbours and the matches
      }
    }
  }

  /**
    * Represents a neighbour with his weight
    * */
  case class NeighbourWithWeight(neighbourID : Long, weight : Float) extends Ordered[NeighbourWithWeight] {
    /** Default method to sorts iterable items that contains NeighbourWithWeight  */
    def compare(that : NeighbourWithWeight) : Int = {
      this.weight compare that.weight
    }
  }
}
