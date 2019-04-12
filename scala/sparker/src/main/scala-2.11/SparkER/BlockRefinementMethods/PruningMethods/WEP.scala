package SparkER.BlockRefinementMethods.PruningMethods

import SparkER.BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
import SparkER.DataStructures.{ProfileBlocks, UnweightedEdge}
//import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by Luca on 10/04/2018.
  */
object WEP {
  /**
    * Computes the Weight Edge Pruning
    *
    * @param profileBlocksFiltered  profileBlocks after block filtering
    * @param blockIndex             a map that given a block ID returns the ID of the contained profiles
    * @param maxID                  maximum profile ID
    * @param separatorIDs           maximum profile ID of the first dataset (-1 if there is only one dataset)
    * @param groundtruth            set of true matches
    * @param weightType             type of weight to use @see BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
    * @param profileBlocksSizeIndex a map that contains for each profile the number of its blocks
    * @param useEntropy             if true use the provided entropies to improve the edge weighting
    * @param blocksEntropies        a map that contains for each block its entropy
    * @return an RDD that contains for each partition the number of existing edges, number of gt edges, and the retained edges
    **/
  def WEP(profileBlocksFiltered: RDD[ProfileBlocks],
          blockIndex: Broadcast[scala.collection.Map[Long, Array[Set[Long]]]],
          maxID: Int,
          separatorIDs: Array[Long],
          groundtruth: Broadcast[scala.collection.immutable.Set[(Long, Long)]],
          weightType: String = WeightTypes.CBS,
          profileBlocksSizeIndex: Broadcast[scala.collection.Map[Long, Int]] = null,
          useEntropy: Boolean = false,
          blocksEntropies: Broadcast[scala.collection.Map[Long, Double]] = null
         )
  : RDD[(Double, Double, Iterable[UnweightedEdge])] = {
    if (useEntropy && blocksEntropies == null) {
      throw new Exception("blocksEntropies must be defined")
    }

    if ((weightType == WeightTypes.ECBS || weightType == WeightTypes.EJS || weightType == WeightTypes.JS || weightType == WeightTypes.chiSquare) && profileBlocksSizeIndex == null) {
      throw new Exception("profileBlocksSizeIndex must be defined")
    }

    if (!List(WeightTypes.CBS, WeightTypes.JS, WeightTypes.chiSquare, WeightTypes.ARCS, WeightTypes.ECBS, WeightTypes.EJS).contains(weightType)) {
      throw new Exception("Please provide a valid WeightType, " + weightType + " is not an acceptable value!")
    }

    val sc = SparkContext.getOrCreate()
    var numberOfEdges: Double = 0
    var edgesPerProfile: Broadcast[scala.collection.Map[Long, Double]] = null

    if (weightType == WeightTypes.EJS) {
      val stats = CommonNodePruning.computeStatistics(profileBlocksFiltered, blockIndex, maxID, separatorIDs)
      numberOfEdges = stats.map(_._1.toDouble).sum()
      edgesPerProfile = sc.broadcast(stats.map(x => (x._2._1, x._2._2.toDouble)).groupByKey().map(x => (x._1, x._2.sum)).collectAsMap())
    }

    val threshold = calcThreshold(profileBlocksFiltered, blockIndex, maxID, separatorIDs, weightType, profileBlocksSizeIndex, useEntropy, blocksEntropies, numberOfEdges, edgesPerProfile)

    val edges = pruning(profileBlocksFiltered, blockIndex, maxID, separatorIDs, groundtruth, weightType, profileBlocksSizeIndex, useEntropy, blocksEntropies, threshold, numberOfEdges, edgesPerProfile)

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
    * @return the sum of the edges values
    **/
  def calcSum(weights: Array[Double],
              neighbours: Array[Int],
              neighboursNumber: Int): Double = {
    var acc: Double = 0
    for (i <- 0 until neighboursNumber) {
      val neighbourID = neighbours(i)
      acc += weights(neighbourID)
    }

    acc
  }

  /**
    * Performs the pruning
    *
    * @param weights          an array which contains the weight of each neighbour
    * @param neighbours       an array which  contains the IDs of the neighbours
    * @param neighboursNumber number of neighbours
    * @param groundtruth      set of true matches
    * @param weightType       type of weight to use @see BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
    * @param globalThreshold  global threshold
    * @return a tuple that contains the number of retained edges and the edges that exists in the groundtruth
    **/
  def doPruning(profileID: Long,
                weights: Array[Double],
                neighbours: Array[Int],
                neighboursNumber: Int,
                groundtruth: Broadcast[scala.collection.immutable.Set[(Long, Long)]],
                weightType: String,
                globalThreshold: Double
               ): (Double, Double, Iterable[UnweightedEdge]) = {

    var cont: Double = 0
    var gtFound: Double = 0
    var edges: List[UnweightedEdge] = Nil

    //@transient lazy val log = LogManager.getRootLogger

    for (i <- 0 until neighboursNumber) {
      val neighbourID = neighbours(i)
      val neighbourWeight = weights(neighbourID)

      if (neighbourWeight >= globalThreshold) {
        //log.info("SPARKER - Id mio "+profileID+", ID vicino "+neighbourID+", soglia vicino "+neighbourThreshold+", peso vicino "+neighbourWeight+", soglia mia "+profileThreshold+", comparison type "+comparisonType+" ----> lo tengo")
        cont += 1
        if (groundtruth != null && groundtruth.value.contains((profileID, neighbourID))) {
          gtFound += 1
        }

        edges = UnweightedEdge(profileID, neighbours(i)) :: edges
      } /*
        else{
          log.info("SPARKER - Id mio "+profileID+", ID vicino "+neighbourID+", soglia vicino "+neighbourThreshold+", peso vicino "+neighbourWeight+", soglia mia "+profileThreshold+", comparison type "+comparisonType+" ----> Non lo tengo")
        }*/
    }

    (cont, gtFound, edges)
  }


  /**
    * Computes the global threshold
    *
    * @param profileBlocksFiltered  profileBlocks after block filtering
    * @param blockIndex             a map that given a block ID returns the ID of the contained profiles
    * @param maxID                  maximum profile ID
    * @param separatorID            maximum profile ID of the first dataset (-1 if there is only one dataset)
    * @param weightType             type of weight to use @see BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
    * @param profileBlocksSizeIndex a map that contains for each profile the number of its blocks
    * @param useEntropy             if true use the provided entropies to improve the edge weighting
    * @param blocksEntropies        a map that contains for each block its entropy
    * @param numberOfEdges          global number of existings edges
    * @param edgesPerProfile        a map that contains for each profile the number of edges
    * @return the global threshold that will be used to prune the graph
    **/
  def calcThreshold(profileBlocksFiltered: RDD[ProfileBlocks],
                    blockIndex: Broadcast[scala.collection.Map[Long, Array[Set[Long]]]],
                    maxID: Int,
                    separatorID: Array[Long],
                    weightType: String,
                    profileBlocksSizeIndex: Broadcast[scala.collection.Map[Long, Int]],
                    useEntropy: Boolean,
                    blocksEntropies: Broadcast[scala.collection.Map[Long, Double]],
                    numberOfEdges: Double,
                    edgesPerProfile: Broadcast[scala.collection.Map[Long, Double]]
                   ): Double = {

    val partialSums = profileBlocksFiltered.mapPartitions { partition =>
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
        val localSum = calcSum(localWeights, neighbours, neighboursNumber)
        CommonNodePruning.doReset(localWeights, neighbours, entropies, useEntropy, neighboursNumber)
        (localSum, neighboursNumber.toDouble)
      }
    }

    val sums = partialSums.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    sums._1 / sums._2
  }

  /**
    * Performs the pruning
    *
    * @param profileBlocksFiltered  profileBlocks after block filtering
    * @param blockIndex             a map that given a block ID returns the ID of the contained profiles
    * @param maxID                  maximum profile ID
    * @param separatorID            maximum profile ID of the first dataset (-1 if there is only one dataset)
    * @param groundtruth            set of true matches
    * @param weightType             type of weight to use @see BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
    * @param profileBlocksSizeIndex a map that contains for each profile the number of its blocks
    * @param useEntropy             if true use the provided entropies to improve the edge weighting
    * @param blocksEntropies        a map that contains for each block its entropy
    * @param numberOfEdges          global number of existings edges
    * @param edgesPerProfile        a map that contains for each profile the number of edges
    * @return an RDD that for each partition contains the number of retained edges and the list of the elements that appears in the groundtruth
    **/
  def pruning(profileBlocksFiltered: RDD[ProfileBlocks],
              blockIndex: Broadcast[scala.collection.Map[Long, Array[Set[Long]]]],
              maxID: Int,
              separatorID: Array[Long],
              groundtruth: Broadcast[scala.collection.immutable.Set[(Long, Long)]],
              weightType: String,
              profileBlocksSizeIndex: Broadcast[scala.collection.Map[Long, Int]],
              useEntropy: Boolean,
              blocksEntropies: Broadcast[scala.collection.Map[Long, Double]],
              threshold: Double,
              numberOfEdges: Double,
              edgesPerProfile: Broadcast[scala.collection.Map[Long, Double]]
             ): RDD[(Double, Double, Iterable[UnweightedEdge])] = {

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
        val result = doPruning(pb.profileID, localWeights, neighbours, neighboursNumber, groundtruth, weightType, threshold)
        CommonNodePruning.doReset(localWeights, neighbours, entropies, useEntropy, neighboursNumber)
        result
      }
    }
  }
}
