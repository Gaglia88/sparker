package BlockRefinementMethods.PruningMethods

import BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
import DataStructures.{ProfileBlocks, UnweightedEdge}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by Luca on 03/05/2017.
  */
object WNPFor {
  /**
    * Computes the Weight Node Pruning
    *
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
    * @param chi2divider            used only in the chiSquare weight method to compute the threshold
    * @param comparisonType         type of comparison to perform @see PruningUtils.ComparisonTypes
    * @return an RDD that contains for each partition the number of existing edges and the retained edges
    **/
  def WNP(profileBlocksFiltered: RDD[ProfileBlocks],
          blockIndex: Broadcast[scala.collection.Map[Long, (Set[Long], Set[Long])]],
          maxID: Int,
          separatorID: Long,
          groundtruth: Broadcast[scala.collection.immutable.Set[(Long, Long)]],
          thresholdType: String = PruningUtils.ThresholdTypes.AVG,
          weightType: String = WeightTypes.CBS,
          profileBlocksSizeIndex: Broadcast[scala.collection.Map[Long, Int]] = null,
          useEntropy: Boolean = false,
          blocksEntropies: Broadcast[scala.collection.Map[Long, Double]] = null,
          chi2divider: Double = 2.0,
          comparisonType: String = PruningUtils.ComparisonTypes.OR
         )
  : RDD[(Double, Iterable[UnweightedEdge])] = {
    if (useEntropy && blocksEntropies == null) {
      throw new Exception("blocksEntropies must be defined")
    }

    if ((weightType == WeightTypes.ECBS || weightType == WeightTypes.EJS || weightType == WeightTypes.JS || weightType == WeightTypes.chiSquare) && profileBlocksSizeIndex == null) {
      throw new Exception("profileBlocksSizeIndex must be defined")
    }

    val sc = SparkContext.getOrCreate()
    var numberOfEdges: Double = 0
    var edgesPerProfile: Broadcast[scala.collection.Map[Long, Double]] = null

    if (weightType == WeightTypes.EJS) {
      val stats = CommonNodePruning.computeStatistics(profileBlocksFiltered, blockIndex, maxID, separatorID)
      numberOfEdges = stats.map(_._1.toDouble).sum()
      edgesPerProfile = sc.broadcast(stats.map(x => (x._2._1, x._2._2.toDouble)).groupByKey().map(x => (x._1, x._2.sum)).collectAsMap())
    }

    val thresholds = sc.broadcast(calcThresholds(profileBlocksFiltered, blockIndex, maxID, separatorID, thresholdType, weightType, profileBlocksSizeIndex, useEntropy, blocksEntropies, numberOfEdges, edgesPerProfile).collectAsMap())
    val edges = pruning(profileBlocksFiltered, blockIndex, maxID, separatorID, groundtruth, thresholdType, weightType, profileBlocksSizeIndex, useEntropy, blocksEntropies, chi2divider, comparisonType, thresholds, numberOfEdges, edgesPerProfile)

    thresholds.unpersist()
    if (edgesPerProfile != null) {
      edgesPerProfile.unpersist()
    }

    edges
  }


  /**
    * Compute the threshold for a profile
    *
    * @param weights          an array which contains the weight of each neighbour
    * @param neighbours       an array which contains the IDs of the neighbours
    * @param neighboursNumber number of neighbours
    * @param thresholdType    type of threshold to use
    * @return the profile threshold
    **/
  def calcThreshold(weights: Array[Double],
                    neighbours: Array[Int],
                    neighboursNumber: Int,
                    thresholdType: String): Double = {
    var acc: Double = 0
    for (i <- 0 to neighboursNumber - 1) {
      val neighbourID = neighbours(i)
      if (thresholdType == PruningUtils.ThresholdTypes.AVG) {
        acc += weights(neighbourID)
      }
      else if (thresholdType == PruningUtils.ThresholdTypes.MAX_FRACT_2 && weights(neighbourID) > acc) {
        acc = weights(neighbourID)
      }
    }

    if (thresholdType == PruningUtils.ThresholdTypes.AVG) {
      acc /= neighboursNumber
    }
    else if (thresholdType == PruningUtils.ThresholdTypes.MAX_FRACT_2) {
      acc /= 2.0
    }
    acc
  }

  /**
    * Performs the pruning
    *
    * @param weights          an array which contains the weight of each neighbour
    * @param neighbours       an array which contains the IDs of the neighbours
    * @param neighboursNumber number of neighbours
    * @param groundtruth      set of true matches
    * @param weightType       type of weight to use @see BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
    * @param comparisonType   type of comparison to perform @see ComparisonTypes
    * @param thresholds       local profile threshold
    * @param chi2divider      used only in the chiSquare weight method to compute the local threshold
    * @return a tuple that contains the number of retained edges and the edges that exists in the groundtruth
    **/
  def doPruning(profileID: Long,
                weights: Array[Double],
                neighbours: Array[Int],
                neighboursNumber: Int,
                groundtruth: Broadcast[scala.collection.immutable.Set[(Long, Long)]],
                weightType: String,
                comparisonType: String,
                thresholds: Broadcast[scala.collection.Map[Long, Double]],
                chi2divider: Double
               ): (Double, Iterable[UnweightedEdge]) = {

    var cont: Double = 0
    var edges: List[UnweightedEdge] = Nil
    val profileThreshold = thresholds.value(profileID)

    if (weightType == WeightTypes.chiSquare) {
      for (i <- 0 to neighboursNumber - 1) {
        val neighbourID = neighbours(i)
        val neighbourThreshold = thresholds.value(neighbourID)
        val neighbourWeight = weights(neighbourID)
        val threshold = Math.sqrt(Math.pow(neighbourThreshold, 2) + Math.pow(profileThreshold, 2)) / chi2divider

        if (neighbourWeight >= threshold) {
          cont += 1
          if (groundtruth.value.contains((profileID, neighbourID))) {
            edges = UnweightedEdge(profileID, neighbours(i)) :: edges
          }
        }
      }
    }
    else {
      for (i <- 0 to neighboursNumber - 1) {
        val neighbourID = neighbours(i)
        val neighbourThreshold = thresholds.value(neighbourID)
        val neighbourWeight = weights(neighbourID)

        if (
          (comparisonType == PruningUtils.ComparisonTypes.AND && neighbourWeight >= neighbourThreshold && neighbourWeight >= profileThreshold)
            || (comparisonType == PruningUtils.ComparisonTypes.OR && (neighbourWeight >= neighbourThreshold || neighbourWeight >= profileThreshold))
        ) {
          cont += 1
          if (groundtruth.value.contains((profileID, neighbourID))) {
            edges = UnweightedEdge(profileID, neighbours(i)) :: edges
          }
        }
      }
    }

    (cont, edges)
  }


  /**
    * For each profile computes the threshold
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
    * @return an RDD which contains for each profileID the threshold
    **/
  def calcThresholds(profileBlocksFiltered: RDD[ProfileBlocks],
                     blockIndex: Broadcast[scala.collection.Map[Long, (Set[Long], Set[Long])]],
                     maxID: Int,
                     separatorID: Long,
                     thresholdType: String,
                     weightType: String,
                     profileBlocksSizeIndex: Broadcast[scala.collection.Map[Long, Int]],
                     useEntropy: Boolean,
                     blocksEntropies: Broadcast[scala.collection.Map[Long, Double]],
                     numberOfEdges: Double,
                     edgesPerProfile: Broadcast[scala.collection.Map[Long, Double]]
                    ): RDD[(Long, Double)] = {

    profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) {
        0
      }
      val neighbours = Array.ofDim[Int](maxID + 1)
      var neighboursNumber = 0

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
        val threshold = calcThreshold(localWeights, neighbours, neighboursNumber, thresholdType)
        CommonNodePruning.doReset(localWeights, neighbours, entropies, useEntropy, neighboursNumber)
        (pb.profileID, threshold)
      }
    }
  }

  /**
    * Performs the pruning
    *
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
    * @param chi2divider            used only in the chiSquare weight method to compute the threshold
    * @param comparisonType         type of comparison to perform @see ComparisonTypes
    * @param numberOfEdges          global number of existings edges
    * @param edgesPerProfile        a maps that contains for each profile the number of edges
    * @return an RDD that for each partition contains the number of retained edges and the list of the elements that appears in the groundtruth
    **/
  def pruning(profileBlocksFiltered: RDD[ProfileBlocks],
              blockIndex: Broadcast[scala.collection.Map[Long, (Set[Long], Set[Long])]],
              maxID: Int,
              separatorID: Long,
              groundtruth: Broadcast[scala.collection.immutable.Set[(Long, Long)]],
              thresholdType: String,
              weightType: String,
              profileBlocksSizeIndex: Broadcast[scala.collection.Map[Long, Int]],
              useEntropy: Boolean,
              blocksEntropies: Broadcast[scala.collection.Map[Long, Double]],
              chi2divider: Double,
              comparisonType: String,
              thresholds: Broadcast[scala.collection.Map[Long, Double]],
              numberOfEdges: Double,
              edgesPerProfile: Broadcast[scala.collection.Map[Long, Double]]
             ): RDD[(Double, Iterable[UnweightedEdge])] = {

    profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) {
        0
      }
      val neighbours = Array.ofDim[Int](maxID + 1)
      var neighboursNumber = 0

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
        neighboursNumber = CommonNodePruning.calcCBS(pb, blockIndex, separatorID, useEntropy, blocksEntropies, localWeights, entropies, neighbours, false)
        CommonNodePruning.calcWeights(pb, localWeights, neighbours, entropies, neighboursNumber, blockIndex, separatorID, weightType, profileBlocksSizeIndex, useEntropy, numberOfEdges, edgesPerProfile)
        val result = doPruning(pb.profileID, localWeights, neighbours, neighboursNumber, groundtruth, weightType, comparisonType, thresholds, chi2divider)
        CommonNodePruning.doReset(localWeights, neighbours, entropies, useEntropy, neighboursNumber)
        result
      }
    }
  }
}
