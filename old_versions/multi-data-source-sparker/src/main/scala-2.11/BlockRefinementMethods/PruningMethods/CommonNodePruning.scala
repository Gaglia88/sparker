package BlockRefinementMethods.PruningMethods

import BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
import DataStructures.ProfileBlocks
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by Luca on 04/08/2017.
  */
object CommonNodePruning {
  /**
    * Compute the statistics needed to perform the EJS
    *
    * @param profileBlocksFiltered profileBlocks after block filtering
    * @param blockIndex            a map that given a block ID returns the ID of the contained profiles
    * @param maxID                 maximum profile ID
    * @param separatorID           maximum profile ID of the first dataset (-1 if there is only one dataset)
    * @return an RDD which contains (number of distinct edges, (profileID, number of neighbours))
    **/
  def computeStatistics(profileBlocksFiltered: RDD[ProfileBlocks],
                        blockIndex: Broadcast[scala.collection.Map[Long, Array[Set[Long]]]],
                        maxID: Int,
                        separatorID: Array[Long]
                       ): RDD[(Int, (Long, Int))] = {

    profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) {
        0
      }
      val neighbours = Array.ofDim[Int](maxID + 1)

      partition.map { pb =>
        var neighboursNumber = 0
        var distinctEdges = 0
        val profileID = pb.profileID
        val profileBlocks = pb.blocks

        profileBlocks.foreach { block =>
          val blockID = block.blockID
          val blockProfiles = blockIndex.value.get(blockID)
          if (blockProfiles.isDefined) {
            val profilesIDs = {
              if (separatorID.isEmpty) {
                blockProfiles.get.head
              }
              else {
                PruningUtils.getAllNeighbors(profileID, blockProfiles.get, separatorID)
              }
            }

            profilesIDs.foreach { secondProfileID =>
              val neighbourID = secondProfileID.toInt
              val neighbourWeight = localWeights(neighbourID)
              localWeights.update(neighbourID, neighbourWeight + 1)
              if (neighbourWeight == 1) {
                neighbours.update(neighboursNumber, neighbourID)
                neighboursNumber += 1
                if (profileID < neighbourID) {
                  distinctEdges += 1
                }
              }
            }
          }
        }

        for (i <- 0 until neighboursNumber) {
          localWeights.update(neighbours(i), 0)
        }

        (distinctEdges, (profileID, neighboursNumber))
      }
    }
  }

  /**
    * Computes the chi square.
    *
    * @param CBS                     common blocks
    * @param neighbourNumBlocks      number of blocks that the neighbour owns
    * @param currentProfileNumBlocks number of blocks that the current profile owns
    * @param totalNumberOfBlocks     number of existing blocks
    **/
  def calcChiSquare(CBS: Double, neighbourNumBlocks: Double, currentProfileNumBlocks: Double, totalNumberOfBlocks: Double): Double = {
    val CMat = Array.ofDim[Double](3, 3)
    var weight: Double = 0
    var expectedValue: Double = 0

    CMat(0)(0) = CBS
    CMat(0)(1) = neighbourNumBlocks - CBS
    CMat(0)(2) = neighbourNumBlocks

    CMat(1)(0) = currentProfileNumBlocks - CBS
    CMat(1)(1) = totalNumberOfBlocks - (neighbourNumBlocks + currentProfileNumBlocks - CBS)
    CMat(1)(2) = totalNumberOfBlocks - neighbourNumBlocks

    CMat(2)(0) = currentProfileNumBlocks
    CMat(2)(1) = totalNumberOfBlocks - currentProfileNumBlocks

    for (i <- 0 to 1) {
      for (j <- 0 to 1) {
        expectedValue = (CMat(i)(2) * CMat(2)(j)) / totalNumberOfBlocks
        weight += Math.pow((CMat(i)(j) - expectedValue), 2) / expectedValue
      }
    }

    return weight
  }


  /**
    * Compute the CBS for a single profile
    *
    * @param pb              the profile
    * @param blockIndex      a map that given a block ID returns the ID of the contained profiles
    * @param separatorID     maximum profile ID of the first dataset (-1 if there is only one dataset)
    * @param useEntropy      if true use the provided entropies to improve the edge weighting
    * @param blocksEntropies a map that contains for each block its entropy
    * @param weights         an array which the CBS values will be stored in
    * @param entropies       an array which the entropy of each neighbour will be stored in
    * @param neighbours      an array which the ID of the neighbours will be stored in
    * @param firstStep       if it is set to true all the edges will be computed, otherwise only the edges that have an ID
    *                        greater than current profile ID.
    * @return number of neighbours
    **/
  def calcCBS(pb: ProfileBlocks,
              blockIndex: Broadcast[scala.collection.Map[Long, Array[Set[Long]]]],
              separatorID: Array[Long],
              useEntropy: Boolean,
              blocksEntropies: Broadcast[scala.collection.Map[Long, Double]],
              weights: Array[Double],
              entropies: Array[Double],
              neighbours: Array[Int],
              firstStep: Boolean
             ): Int = {
    var neighboursNumber = 0
    val profileID = pb.profileID
    val profileBlocks = pb.blocks

    profileBlocks.foreach { block =>
      val blockID = block.blockID
      val blockProfiles = blockIndex.value.get(blockID)

      if (blockProfiles.isDefined) {
        val profilesIDs = {
          if (separatorID.isEmpty) {
            blockProfiles.get.head
          }
          else {
            PruningUtils.getAllNeighbors(profileID, blockProfiles.get, separatorID)
          }
        }

        val blockEntropy = {
          if (useEntropy) {
            val e = blocksEntropies.value.get(blockID)
            if (e.isDefined) {
              e.get
            }
            else {
              0.0
            }
          }
          else {
            0.0
          }
        }

        profilesIDs.foreach { secondProfileID =>
          val neighbourID = secondProfileID.toInt
          if ((profileID < neighbourID) || firstStep) {
            weights.update(neighbourID, weights(neighbourID) + 1)

            if (useEntropy) {
              entropies.update(neighbourID, entropies(neighbourID) + blockEntropy)
            }

            if (weights(neighbourID) == 1) {
              neighbours.update(neighboursNumber, neighbourID)
              neighboursNumber += 1
            }
          }

        }
      }
    }

    neighboursNumber
  }


  /**
    * Compute the weights for each neighbour
    *
    * @param pb                     the profile
    * @param weights                an array which contains the CBS values
    * @param neighbours             an array which contains the IDs of the neighbours
    * @param entropies              an array which contains the entropy of each neighbour
    * @param neighboursNumber       number of neighbours
    * @param blockIndex             a map that given a block ID returns the ID of the contained profiles
    * @param separatorID            maximum profile ID of the first dataset (-1 if there is only one dataset)
    * @param weightType             type of weight to use @see BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
    * @param profileBlocksSizeIndex a map that contains for each profile the number of its blocks
    * @param useEntropy             if true use the provided entropies to improve the edge weighting
    * @param numberOfEdges          global number of existings edges
    * @param edgesPerProfile        a maps that contains for each profile the number of edges
    * @return number of neighbours
    **/
  def calcWeights(pb: ProfileBlocks,
                  weights: Array[Double],
                  neighbours: Array[Int],
                  entropies: Array[Double],
                  neighboursNumber: Int,
                  blockIndex: Broadcast[scala.collection.Map[Long, Array[Set[Long]]]],
                  separatorID: Array[Long],
                  weightType: String,
                  profileBlocksSizeIndex: Broadcast[scala.collection.Map[Long, Int]],
                  useEntropy: Boolean,
                  numberOfEdges: Double,
                  edgesPerProfile: Broadcast[scala.collection.Map[Long, Double]]
                 ): Unit = {

    if (weightType == WeightTypes.chiSquare) {
      val numberOfProfileBlocks = pb.blocks.size
      val totalNumberOfBlocks: Double = blockIndex.value.size.toDouble
      for (i <- 0 until neighboursNumber) {
        val neighbourID = neighbours(i)
        if (useEntropy) {
          weights.update(neighbourID, calcChiSquare(weights(neighbourID), profileBlocksSizeIndex.value(neighbourID), numberOfProfileBlocks, totalNumberOfBlocks) * entropies(neighbourID))
        }
        else {
          weights.update(neighbourID, calcChiSquare(weights(neighbourID), profileBlocksSizeIndex.value(neighbourID), numberOfProfileBlocks, totalNumberOfBlocks))
        }
      }
    }
    else if (weightType == WeightTypes.ARCS) {
      val profileID = pb.profileID
      val profileBlocks = pb.blocks
      profileBlocks.foreach { block =>
        val blockID = block.blockID
        val blockProfiles = blockIndex.value.get(blockID)

        if (blockProfiles.isDefined) {
          val comparisons = {
            if (separatorID.isEmpty) {
              val s = blockProfiles.get.head.size.toDouble
              s*(s-1)
            }
            else {
              blockProfiles.get.map(_.size.toDouble).product
            }
          }

          for (i <- 0 until neighboursNumber) {
            val neighbourID = neighbours(i)
            weights.update(neighbourID, weights(neighbourID) / comparisons)
            if (useEntropy) {
              weights.update(neighbourID, weights(neighbourID) * entropies(neighbourID))
            }
          }
        }
      }
    }
    else if (weightType == WeightTypes.JS) {
      val numberOfProfileBlocks = pb.blocks.size
      for (i <- 0 until neighboursNumber) {
        val neighbourID = neighbours(i)
        val commonBlocks = weights(neighbourID)
        val JS = {
          if (useEntropy) {
            (commonBlocks / (numberOfProfileBlocks + profileBlocksSizeIndex.value(neighbourID) - commonBlocks)) * entropies(neighbourID)
          }
          else {
            commonBlocks / (numberOfProfileBlocks + profileBlocksSizeIndex.value(neighbourID) - commonBlocks)
          }
        }
        weights.update(neighbourID, JS)
      }
    }
    else if (weightType == WeightTypes.EJS) {
      val profileNumberOfNeighbours = edgesPerProfile.value.getOrElse(pb.profileID, 0.00000000001)
      val numberOfProfileBlocks = pb.blocks.size
      for (i <- 0 until neighboursNumber) {
        val neighbourID = neighbours(i)
        val commonBlocks = weights(neighbourID)
        val EJS = {
          if (useEntropy) {
            ((commonBlocks / (numberOfProfileBlocks + profileBlocksSizeIndex.value(neighbourID) - commonBlocks)) * entropies(neighbourID)) * Math.log10(numberOfEdges / edgesPerProfile.value.getOrElse(neighbourID, 0.00000000001) * Math.log10(numberOfEdges / profileNumberOfNeighbours))
          }
          else {
            (commonBlocks / (numberOfProfileBlocks + profileBlocksSizeIndex.value(neighbourID) - commonBlocks)) * Math.log10(numberOfEdges / edgesPerProfile.value.getOrElse(neighbourID, 0.00000000001) * Math.log10(numberOfEdges / profileNumberOfNeighbours))
          }
        }
        weights.update(neighbourID, EJS)
      }
    }
    else if (weightType == WeightTypes.ECBS) {
      val blocksNumber = blockIndex.value.size
      val numberOfProfileBlocks = pb.blocks.size
      for (i <- 0 until neighboursNumber) {
        val neighbourID = neighbours(i)
        val commonBlocks = weights(neighbourID)
        val ECBS = {
          if (useEntropy) {
            commonBlocks * Math.log10(blocksNumber / numberOfProfileBlocks) * Math.log10(blocksNumber / profileBlocksSizeIndex.value(neighbourID)) * entropies(neighbourID)
          }
          else {
            commonBlocks * Math.log10(blocksNumber / numberOfProfileBlocks) * Math.log10(blocksNumber / profileBlocksSizeIndex.value(neighbourID))
          }
        }
        weights.update(neighbourID, ECBS)
      }
    }
  }

  /**
    * Resets the arrays
    *
    * @param weights          an array which contains the weight of each neighbour
    * @param neighbours       an array which contains the IDs of the neighbours
    * @param entropies        an array which contains the entropy of each neighbour
    * @param useEntropy       if true use the provided entropies to improve the edge weighting
    * @param neighboursNumber number of neighbours
    **/
  def doReset(weights: Array[Double],
              neighbours: Array[Int],
              entropies: Array[Double],
              useEntropy: Boolean,
              neighboursNumber: Int): Unit = {
    for (i <- 0 until neighboursNumber) {
      val neighbourID = neighbours(i)
      weights.update(neighbourID, 0)
      if (useEntropy) {
        entropies.update(neighbourID, 0)
      }
    }
  }
}
