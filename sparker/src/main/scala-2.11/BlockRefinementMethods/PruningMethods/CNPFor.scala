package BlockRefinementMethods.PruningMethods

import BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
import DataStructures.{BlockAbstract, ProfileBlocks, UnweightedEdge}
import Utilities.BoundedPriorityQueue
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by Luca on 04/08/2017.
  */
object CNPFor {

  /**
    * Computes the Weight Node Pruning
    * @param blocks current blocks
    * @param numberOfProfiles total number of profiles
    * @param profileBlocksFiltered  profileBlocks after block filtering
    * @param blockIndex             a map that given a block ID returns the ID of the contained profiles
    * @param maxID                  maximum profile ID
    * @param separatorID            maximum profile ID of the first dataset (-1 if there is only one dataset)
    * @param groundtruth            set of true matches
    * @param thresholdType          type of threshold to use
    * @param weightType             type of weight to use @see BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
    * @param profileBlocksSizeIndex a map that contains for each profile the number of its blocks
    * @param useEntropy             if true use the provided entropies to improve the edge weighting
    * @param blocksEntropies        a map that contains for each block its entropy
    * @param comparisonType         type of comparison to perform @see ComparisonTypes
    * @return an RDD that contains for each partition the number of existing edges and the retained edges
    **/
  def CNP(blocks : RDD[BlockAbstract],
          numberOfProfiles : Long,
          profileBlocksFiltered: RDD[ProfileBlocks],
          blockIndex: Broadcast[scala.collection.Map[Long, (Set[Long], Set[Long])]],
          maxID: Int,
          separatorID: Long,
          groundtruth: Broadcast[scala.collection.immutable.Set[(Long, Long)]],
          thresholdType: String = PruningUtils.ThresholdTypes.AVG,
          weightType: String = WeightTypes.CBS,
          profileBlocksSizeIndex: Broadcast[scala.collection.Map[Long, Int]] = null,
          useEntropy: Boolean = false,
          blocksEntropies: Broadcast[scala.collection.Map[Long, Double]] = null,
          comparisonType: String = PruningUtils.ComparisonTypes.OR
         )
  : RDD[(Double, Iterable[UnweightedEdge])] = {
    if (useEntropy == true && blocksEntropies == null) {
      throw new Exception("blocksEntropies must be defined")
    }

    if ((weightType == WeightTypes.ECBS || weightType == WeightTypes.EJS || weightType == WeightTypes.JS || weightType == WeightTypes.chiSquare) && profileBlocksSizeIndex == null) {
      throw new Exception("profileBlocksSizeIndex must be defined")
    }

    val sc = SparkContext.getOrCreate()
    val CNPThreshold = computeCNPThreshold(blocks, numberOfProfiles)
    var numberOfEdges: Double = 0
    var edgesPerProfile: Broadcast[scala.collection.Map[Long, Double]] = null

    if (weightType == WeightTypes.EJS) {
      val stats = CommonNodePruning.computeStatistics(profileBlocksFiltered, blockIndex, maxID, separatorID)
      numberOfEdges = stats.map(_._1.toDouble).sum()
      edgesPerProfile = sc.broadcast(stats.map(x => (x._2._1, x._2._2.toDouble)).groupByKey().map(x => (x._1, x._2.sum)).collectAsMap())
    }

    val retainedNeighbours = sc.broadcast(calcRetainedNeighbours(profileBlocksFiltered, blockIndex, maxID, separatorID, thresholdType, weightType, profileBlocksSizeIndex, useEntropy, blocksEntropies, numberOfEdges, edgesPerProfile, CNPThreshold).collectAsMap())
    val edges = pruning(profileBlocksFiltered, blockIndex, separatorID, groundtruth, retainedNeighbours, comparisonType)

    retainedNeighbours.unpersist()
    if (edgesPerProfile != null) {
      edgesPerProfile.unpersist()
    }

    edges
  }


  /**
    * Computes the CNP threshold.
    * Number of neighbours to keep for each record.
    *
    * @param blocks current blocks
    * @param numberOfProfiles total number of profiles
    * */
  def computeCNPThreshold(blocks : RDD[BlockAbstract], numberOfProfiles : Long) : Int = {
    val numElements = blocks.map(_.getAllProfiles.size).sum()
    Math.floor((numElements/numberOfProfiles)-1).toInt
  }

  /**
    * For each profile computes the retained neighbours
    *
    * @param profileBlocksFiltered  profileBlocks after block filtering
    * @param blockIndex             a map that given a block ID returns the ID of the contained profiles
    * @param maxID                  maximum profile ID
    * @param separatorID            maximum profile ID of the first dataset (-1 if there is only one dataset)
    * @param thresholdType          type of threshold to use
    * @param weightType             type of weight to use @see BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
    * @param profileBlocksSizeIndex a map that contains for each profile the number of its blocks
    * @param useEntropy             if true use the provided entropies to improve the edge weighting
    * @param blocksEntropies        a map that contains for each block its entropy
    * @param numberOfEdges          global number of existings edges
    * @param edgesPerProfile        a maps that contains for each profile the number of edges
    * @return an RDD which contains for each profileID the IDs of the retained neighbours
    **/
  def calcRetainedNeighbours(profileBlocksFiltered: RDD[ProfileBlocks],
                     blockIndex: Broadcast[scala.collection.Map[Long, (Set[Long], Set[Long])]],
                     maxID: Int,
                     separatorID: Long,
                     thresholdType: String,
                     weightType: String,
                     profileBlocksSizeIndex: Broadcast[scala.collection.Map[Long, Int]],
                     useEntropy: Boolean,
                     blocksEntropies: Broadcast[scala.collection.Map[Long, Double]],
                     numberOfEdges: Double,
                     edgesPerProfile: Broadcast[scala.collection.Map[Long, Double]],
                     CNPThreshold : Int
                    ): RDD[(Long, Set[Long])] = {

    profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) {
        0
      }
      val neighbours = Array.ofDim[Int](maxID + 1)
      var neighboursNumber = 0
      val neighboursToKeep = new BoundedPriorityQueue[NeighbourWithWeight](CNPThreshold)

      val entropies: Array[Double] = {
        if (useEntropy) {
          Array.fill[Double](maxID + 1) {
            0.0
          }
        }
        else {
          null
        }
      }

      partition.map { pb =>
        neighboursNumber = CommonNodePruning.calcCBS(pb, blockIndex, separatorID, useEntropy, blocksEntropies, localWeights, entropies, neighbours, true)
        CommonNodePruning.calcWeights(pb, localWeights, neighbours, entropies, neighboursNumber, blockIndex, separatorID, weightType, profileBlocksSizeIndex, useEntropy, numberOfEdges, edgesPerProfile)

        for(i <- 0 to neighboursNumber-1){
          val neighbourID = neighbours(i)
          neighboursToKeep += NeighbourWithWeight(neighbourID, localWeights(neighbourID))
        }
        val sortedNeighbours = neighboursToKeep.toArray.map(_.neighbourID).toSet
        CommonNodePruning.doReset(localWeights, neighbours, entropies, useEntropy, neighboursNumber)
        neighboursToKeep.clear()
        (pb.profileID, sortedNeighbours)
      }
    }
  }

  /**
    * Performs the pruning
    *
    * @param profileBlocksFiltered  profileBlocks after block filtering
    * @param blockIndex             a map that given a block ID returns the ID of the contained profiles
    * @param separatorID            maximum profile ID of the first dataset (-1 if there is only one dataset)
    * @param groundtruth            set of true matches
    * @param retainedNeighbours     retained neighbours for each profile
    * @param comparisonType         type of comparison to perform @see ComparisonTypes
    * @return an RDD that for each partition contains the number of retained edges and the list of the elements that appears in the groundtruth
    **/
  def pruning(profileBlocksFiltered: RDD[ProfileBlocks],
              blockIndex: Broadcast[scala.collection.Map[Long, (Set[Long], Set[Long])]],
              separatorID: Long,
              groundtruth: Broadcast[scala.collection.immutable.Set[(Long, Long)]],
              retainedNeighbours: Broadcast[scala.collection.Map[Long, Set[Long]]],
              comparisonType: String
             ): RDD[(Double, Iterable[UnweightedEdge])] = {

    profileBlocksFiltered.mapPartitions { partition =>
      partition.map { pb =>
        val profileID = pb.profileID
        val profileBlocks = pb.blocks
        val profileRetainedNeighbours = retainedNeighbours.value(profileID)

        var cont : Double = 0
        var edges: List[UnweightedEdge] = Nil

        profileBlocks.foreach { block =>
          val blockID = block.blockID
          val blockProfiles = blockIndex.value.get(blockID)

          if (blockProfiles.isDefined) {
            val profilesIDs = {
              if (separatorID >= 0 && profileID <= separatorID) {
                blockProfiles.get._2
              }
              else {
                blockProfiles.get._1
              }
            }

            profilesIDs.foreach { secondProfileID =>
              val neighbourID = secondProfileID
              val neighbourRetainedNeighbours = retainedNeighbours.value(neighbourID)

              if(comparisonType == PruningUtils.ComparisonTypes.OR
                && (
                    (profileRetainedNeighbours.contains(neighbourID) && !neighbourRetainedNeighbours.contains(profileID))
                    || (neighbourRetainedNeighbours.contains(profileID) && profileRetainedNeighbours.contains(neighbourID) && profileID < neighbourID)
                )){
                cont += 1
                if(profileID < neighbourID && groundtruth.value.contains((profileID, neighbourID))) {
                    edges = UnweightedEdge(profileID, neighbourID) :: edges
                }
                else if(groundtruth.value.contains((neighbourID, profileID))) {
                    edges = UnweightedEdge(neighbourID, profileID) :: edges
                }
              }
              else if(comparisonType == PruningUtils.ComparisonTypes.AND && neighbourRetainedNeighbours.contains(profileID) && profileRetainedNeighbours.contains(neighbourID) && profileID < neighbourID){
                cont += 1
                if(profileID < neighbourID && groundtruth.value.contains((profileID, neighbourID))) {
                  edges = UnweightedEdge(profileID, neighbourID) :: edges
                }
                else if(groundtruth.value.contains((neighbourID, profileID))) {
                  edges = UnweightedEdge(neighbourID, profileID) :: edges
                }
              }
            }
          }
        }

        (cont, edges)
      }
    }
  }

  /**
    * Represents a neighbour with its weight
    * */
  case class NeighbourWithWeight(neighbourID : Long, weight : Double) extends Ordered[NeighbourWithWeight] {
    /** Default method to sorts iterable items that contains NeighbourWithWeight  */
    def compare(that : NeighbourWithWeight) : Int = {
      this.weight compare that.weight
    }
  }
}
