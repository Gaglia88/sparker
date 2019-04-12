package SparkER.BlockRefinementMethods.PruningMethods

import SparkER.BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
import SparkER.DataStructures.{ProfileBlocks, UnweightedEdge}

import scala.collection.mutable
//import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

/**
  * Created by Luca on 10/04/2018.
  */
object CEP {
  /**
    * Computes the Cardinality Edge Pruning
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
  def CEP(profileBlocksFiltered: RDD[ProfileBlocks],
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

    val edgesToKeep = math.floor(blockIndex.value.values.flatMap(_.map(_.size.toDouble)).sum / 2.0)

    println("SPARKER - Number of edges to keep " + edgesToKeep)

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

    val threshold = calcThreshold(profileBlocksFiltered, blockIndex, maxID, separatorIDs, weightType, profileBlocksSizeIndex, useEntropy, blocksEntropies, numberOfEdges, edgesPerProfile, edgesToKeep)
    println("SPARKER - Computed threshold " + threshold)


    val toKeep = sc.broadcast(threshold._2)

    val edges = pruning(profileBlocksFiltered, blockIndex, maxID, separatorIDs, groundtruth, weightType, profileBlocksSizeIndex, useEntropy, blocksEntropies, threshold._1, toKeep, numberOfEdges, edgesPerProfile)


    toKeep.unpersist()
    if (edgesPerProfile != null) {
      edgesPerProfile.unpersist()
    }

    edges
  }


  /**
    * Compute the threshold for a profile
    *
    * @param weights         an array which contains the weight of each neighbor
    * @param neighbors       an array which contains the IDs of the neighbors
    * @param neighborsNumber number of neighbors
    * @return the frequencies for each weight
    **/
  def calcFreq(weights: Array[Double],
               neighbors: Array[Int],
               neighborsNumber: Int): Array[(Double, Double)] = {
    var acc: Double = 0
    val out = mutable.Map[Double, Double]()
    for (i <- 0 until neighborsNumber) {
      val weight = weights(neighbors(i))
      val w = out.getOrElse(weight, 0.0) + 1
      out.put(weight, w)
    }
    out.toArray
  }

  /**
    * Performs the pruning
    *
    * @param weights         an array which contains the weight of each neighbor
    * @param neighbors       an array which  contains the IDs of the neighbors
    * @param neighborsNumber number of neighbors
    * @param groundtruth     set of true matches
    * @param weightType      type of weight to use @see BlockRefinementMethods.PruningMethods.PruningUtils.WeightTypes
    * @param globalThreshold global threshold
    * @return a tuple that contains the number of retained edges and the edges that exists in the groundtruth
    **/
  def doPruning(profileID: Long,
                weights: Array[Double],
                neighbors: Array[Int],
                neighborsNumber: Int,
                groundtruth: Broadcast[scala.collection.immutable.Set[(Long, Long)]],
                weightType: String,
                globalThreshold: Double,
                toKeep: Broadcast[scala.collection.Map[Long, Double]]
               ): (Double, Double, Iterable[UnweightedEdge]) = {

    var cont: Double = 0
    var gtFound: Double = 0
    var edges: List[UnweightedEdge] = Nil

    var neighborToKeep = toKeep.value.getOrElse(profileID, 0.0).toInt

    //@transient lazy val log = LogManager.getRootLogger

    for (i <- 0 until neighborsNumber) {
      val neighborID = neighbors(i)
      val neighborWeight = weights(neighborID)

      if ((neighborWeight > globalThreshold) || (neighborWeight == globalThreshold && neighborToKeep > 0)) {
        //log.info("SPARKER - Id mio "+profileID+", ID vicino "+neighborID+", soglia vicino "+neighborThreshold+", peso vicino "+neighborWeight+", soglia mia "+profileThreshold+", comparison type "+comparisonType+" ----> lo tengo")
        cont += 1
        if (groundtruth != null && groundtruth.value.contains((profileID, neighborID))) {
          gtFound += 1
        }

        edges = UnweightedEdge(profileID, neighbors(i)) :: edges

        if (neighborWeight == globalThreshold) {
          neighborToKeep -= 1
        }
      } /*
        else{
          log.info("SPARKER - Id mio "+profileID+", ID vicino "+neighborID+", soglia vicino "+neighborThreshold+", peso vicino "+neighborWeight+", soglia mia "+profileThreshold+", comparison type "+comparisonType+" ----> Non lo tengo")
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
                    edgesPerProfile: Broadcast[scala.collection.Map[Long, Double]],
                    edgesToKeep: Double
                   ): (Double, scala.collection.Map[Long, Double]) = {

    val partialFreqs = profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) {
        0
      }
      val neighbors = Array.ofDim[Int](maxID + 1)
      var neighborsNumber = 0

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
        neighborsNumber = CommonNodePruning.calcCBS(pb, blockIndex, separatorID, useEntropy, blocksEntropies, localWeights, entropies, neighbors, false)
        CommonNodePruning.calcWeights(pb, localWeights, neighbors, entropies, neighborsNumber, blockIndex, separatorID, weightType, profileBlocksSizeIndex, useEntropy, numberOfEdges, edgesPerProfile)
        val localFreq = calcFreq(localWeights, neighbors, neighborsNumber)
        CommonNodePruning.doReset(localWeights, neighbors, entropies, useEntropy, neighborsNumber)
        (pb.profileID, localFreq.toList)
      }
    }

    val frequencies = partialFreqs.flatMap(x => x._2).groupByKey().map(x => (x._1, x._2.sum)).collect().sortBy(-_._1)

    var sum = 0.0
    var cont = 0
    while (sum < edgesToKeep && cont < frequencies.length) {
      sum += frequencies(cont)._2
      cont += 1
    }

    val threshold = frequencies(cont - 1)._1
    var lastToKeep = frequencies(cont - 1)._2 - (sum - edgesToKeep)

    println("SPARKER - Di quelli con peso uguale a  " + threshold + " devo tenerne " + lastToKeep)


    /** Per ogni profilo conto quanti elementi ha dell'ultima soglia che devo tenere */
    val stats = partialFreqs.map { p =>
      (p._1, p._2.filter(_._1 == threshold).map(_._2))
    }.filter(_._2.nonEmpty).map(x => (x._1, x._2.head)).collect()


    /** Scelgo a caso per ogni profilo quanti tenerne in modo da tenere esattamente il numero che mi serve */
    val toKeep = for (i <- stats.indices; if lastToKeep > 0) yield {
      if (stats(i)._2 > lastToKeep) {
        val a = lastToKeep
        lastToKeep -= stats(i)._2
        (stats(i)._1, a)
      }
      else {
        lastToKeep -= stats(i)._2
        stats(i)
      }
    }

    (threshold, toKeep.filter(_._2 > 0).toMap)
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
              toKeep: Broadcast[scala.collection.Map[Long, Double]],
              numberOfEdges: Double,
              edgesPerProfile: Broadcast[scala.collection.Map[Long, Double]]
             ): RDD[(Double, Double, Iterable[UnweightedEdge])] = {

    profileBlocksFiltered.mapPartitions { partition =>
      val localWeights = Array.fill[Double](maxID + 1) {
        0
      }
      val neighbors = Array.ofDim[Int](maxID + 1)
      var neighborsNumber = 0

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
        neighborsNumber = CommonNodePruning.calcCBS(pb, blockIndex, separatorID, useEntropy, blocksEntropies, localWeights, entropies, neighbors, false)
        CommonNodePruning.calcWeights(pb, localWeights, neighbors, entropies, neighborsNumber, blockIndex, separatorID, weightType, profileBlocksSizeIndex, useEntropy, numberOfEdges, edgesPerProfile)
        val result = doPruning(pb.profileID, localWeights, neighbors, neighborsNumber, groundtruth, weightType, threshold, toKeep)
        CommonNodePruning.doReset(localWeights, neighbors, entropies, useEntropy, neighborsNumber)
        result
      }
    }
  }
}
