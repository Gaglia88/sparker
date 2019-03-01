from py_sparker.pruning_utils import WeightTypes, ThresholdTypes, ComparisonTypes
from py_sparker.common_node_pruning import computeStatistics, calcCBS, calcChiSquare, calcWeights, doReset
import math


class WNP(object):

    @staticmethod
    def doPruning(profileID, weights, neighbours, neighboursNumber, groundtruth, weightType, comparisonType, thresholds,
                  chi2divider):
        """
        Performs the pruning
        :param profileID: id of the profile
        :param weights: an array which contains the weight of each neighbour
        :param neighbours: an array which  contains the IDs of the neighbours
        :param neighboursNumber: number of neighbours
        :param groundtruth: set of true matches (optional)
        :param weightType: type of weight to us
        :param comparisonType: type of comparison to perform @see ComparisonTypes
        :param thresholds: local profile threshold
        :param chi2divider: used only in the chiSquare weight method to compute the local threshold
        :return: a triplet that contains the number of retained edges, the edges that exists in the groundtruth, and the retained edges
        """
        cont = 0
        gtFound = 0
        edges = []
        profileThreshold = thresholds.value[profileID]
        if weightType == WeightTypes.chiSquare:
            for i in range(0, neighboursNumber):
                neighbourID = neighbours[i]
                neighbourThreshold = thresholds.value[neighbourID]
                neighbourWeight = weights[neighbourID]
                threshold = math.sqrt(math.pow(neighbourThreshold, 2) + math.pow(profileThreshold, 2)) / chi2divider

                if neighbourWeight >= threshold:
                    cont += 1
                    if groundtruth is not None:
                        if (profileID, neighbourID) in groundtruth.value:
                            gtFound += 1

                    edges.append((profileID, neighbourID))
        else:
            for i in range(0, neighboursNumber):
                neighbourID = neighbours[i]
                neighbourThreshold = thresholds.value[neighbourID]
                neighbourWeight = weights[neighbourID]

                if (
                        comparisonType == ComparisonTypes.AND and neighbourWeight >= neighbourThreshold and neighbourWeight >= profileThreshold) or (
                        comparisonType == ComparisonTypes.OR and (
                        neighbourWeight >= neighbourThreshold or neighbourWeight >= profileThreshold)):
                    cont += 1
                    if groundtruth is not None:
                        if (profileID, neighbourID) in groundtruth.value:
                            gtFound += 1

                    edges.append((profileID, neighbourID))

        return cont, gtFound, edges

    @staticmethod
    def pruning(profileBlocksFiltered, blockIndex, maxID, separatorID, groundtruth, thresholdType, weightType,
                profileBlocksSizeIndex, useEntropy, blocksEntropies, chi2divider, comparisonType, thresholds,
                numberOfEdges, edgesPerProfile):
        """
        Performs the pruning
        :param profileBlocksFiltered: profileBlocks after block filtering
        :param blockIndex: a map that given a block ID returns the ID of the contained profiles
        :param maxID: maximum profile ID
        :param separatorID: maximum profile ID of each dataset (-1 if there is only one dataset)
        :param groundtruth: set of true matches
        :param thresholdType: type of threshold to use
        :param weightType: type of weight to use see pruning_utils.WeightTypes
        :param profileBlocksSizeIndex: a map that contains for each profile the number of its blocks
        :param useEntropy: if true use the provided entropies to improve the edge weighting
        :param blocksEntropies: a map that contains for each block its entropy
        :param chi2divider: used only in the chiSquare weight method to compute the threshold
        :param comparisonType: type of comparison to perform @see ComparisonTypes
        :param thresholds: a map that contains the threshold of each profile
        :param numberOfEdges: global number of existings edges
        :param edgesPerProfile: a map that contains for each profile the number of edges
        :return: an RDD that for each partition contains a triplet that contains the number of retained edges, the edges that exists in the groundtruth, and the retained edges
        """

        def computePartiton(partition):
            localWeights = [0 for i in range(0, maxID + 1)]
            neighbours = [0 for i in range(0, maxID + 1)]
            entropies = None
            if useEntropy:
                entropies = [0.0 for i in range(0, maxID + 1)]

            def insideMap(pb):
                neighboursNumber = calcCBS(pb, blockIndex, separatorID, useEntropy, blocksEntropies, localWeights,
                                           entropies, neighbours, False)
                calcWeights(pb, localWeights, neighbours, entropies, neighboursNumber, blockIndex, separatorID,
                            weightType, profileBlocksSizeIndex, useEntropy, numberOfEdges, edgesPerProfile)
                result = WNP.doPruning(pb.profileID, localWeights, neighbours, neighboursNumber, groundtruth,
                                       weightType, comparisonType, thresholds, chi2divider)
                doReset(localWeights, neighbours, entropies, useEntropy, neighboursNumber)
                return result

            return map(insideMap, partition)

        return profileBlocksFiltered.mapPartitions(computePartiton)

    @staticmethod
    def calcThreshold(weights, neighbours, neighboursNumber, thresholdType):
        """
        Computes the threshold for a profile
        :param weights: an array which contains the weight of each neighbour
        :param neighbours: an array which contains the IDs of the neighbours
        :param neighboursNumber: number of neighbours
        :param thresholdType: type of threshold to use
        :return: the profile' threshold
        """
        acc = 0
        for i in range(0, neighboursNumber):
            neighbourID = neighbours[i]
            if thresholdType == ThresholdTypes.AVG:
                acc += weights[neighbourID]
            elif thresholdType == ThresholdTypes.MAX_FRACT_2 and weights[neighbourID] > acc:
                acc = weights[neighbourID]
            else:
                pass

        if thresholdType == ThresholdTypes.AVG:
            if neighboursNumber > 0:
                acc /= float(neighboursNumber)
        else:
            acc /= 2.0

        return acc

    @staticmethod
    def calcThresholds(profileBlocksFiltered, blockIndex, maxID, separatorID, thresholdType, weightType,
                       profileBlocksSizeIndex, useEntropy, blocksEntropies, numberOfEdges, edgesPerProfile):
        """
        For each profile computes the threshold
        :param profileBlocksFiltered: profileBlocks after block filtering
        :param blockIndex: a map that given a block ID returns the ID of the contained profiles
        :param maxID: maximum profile ID
        :param separatorID: maximum profile ID of each datasets (empty if its dirty)
        :param thresholdType: type of threshold to use
        :param weightType: type of weight to use
        :param profileBlocksSizeIndex: a map that contains for each profile the number of its blocks
        :param useEntropy: if true use the provided entropies to improve the edge weighting
        :param blocksEntropies: a map that contains for each block its entropy
        :param numberOfEdges: global number of existings edges
        :param edgesPerProfile: a map that contains for each profile the number of edges
        :return: an RDD which contains for each profileID the threshold
        """

        def computePartiton(partition):
            localWeights = [0 for i in range(0, maxID + 1)]
            neighbours = [0 for i in range(0, maxID + 1)]
            entropies = None
            if useEntropy:
                entropies = [0.0 for i in range(0, maxID + 1)]

            def insideMap(pb):
                neighboursNumber = calcCBS(pb, blockIndex, separatorID, useEntropy, blocksEntropies, localWeights,
                                           entropies, neighbours, True)
                calcWeights(pb, localWeights, neighbours, entropies, neighboursNumber, blockIndex, separatorID,
                            weightType, profileBlocksSizeIndex, useEntropy, numberOfEdges, edgesPerProfile)
                threshold = WNP.calcThreshold(localWeights, neighbours, neighboursNumber, thresholdType)
                doReset(localWeights, neighbours, entropies, useEntropy, neighboursNumber)
                return pb.profileID, threshold

            return map(insideMap, partition)

        return profileBlocksFiltered.mapPartitions(computePartiton)

    @staticmethod
    def wnp(profileBlocksFiltered, blockIndex, maxID, separatorIDs=[], groundtruth=0, thresholdType=ThresholdTypes.AVG,
            weightType=WeightTypes.CBS, profileBlocksSizeIndex=None, useEntropy=False, blocksEntropies=None,
            chi2divider=2.0, comparisonType=ComparisonTypes.OR):
        """
        Performs the Weight Node Pruning. Returns an RDD that contains for each partition (number of edges after the pruning, number of true matches found in the groundtruth, list of edges)
        :param profileBlocksFiltered: profiles after filtering
        :param blockIndex: broadcasted blocking index
        :param maxID: highest profile ID
        :param separatorIDs: id of the separators that identifies the different data sources
        :param groundtruth: groundtruth (optional)
        :param thresholdType: type of threshold from ThresholdTypes, default AVG
        :param weightType: weight method from WeightTypes, default CBS
        :param profileBlocksSizeIndex: broadcast map that given a block ID returns its size, needed for ECBS, EJS, JS and chiSquare weights. (optional)
        :param useEntropy: boolean, use entropy to weight the edges. Default false.
        :param blocksEntropies: broadcasted entropies, a map that given a block return its entropy. (optional)
        :param chi2divider: parameter d of blast, see the paper for more info. Default 2.0
        :param comparisonType: pruning strategy from ComparisonTypes, default OR
        :return: Returns an RDD that contains for each partition (number of edges after the pruning, number of true matches found in the groundtruth, list of edges)
        """
        validWeights = [WeightTypes.CBS, WeightTypes.JS, WeightTypes.chiSquare, WeightTypes.ARCS, WeightTypes.ECBS,
                        WeightTypes.EJS]
        validCompTypes = [ComparisonTypes.OR, ComparisonTypes.AND]

        if weightType not in validWeights:
            raise ValueError("Please provide a valid WeightType, " + str(weightType) + " is not an acceptable value!")

        if comparisonType not in validCompTypes:
            raise ValueError(
                "Please provide a valid ComparisonType, " + str(comparisonType) + " is not an acceptable value!")

        if useEntropy and blocksEntropies is None:
            raise ValueError('blocksEntropies must be defined')

        if (
                weightType == WeightTypes.ECBS or weightType == WeightTypes.EJS or weightType == WeightTypes.JS or weightType == WeightTypes.chiSquare) and profileBlocksSizeIndex is None:
            raise ValueError('profileBlocksSizeIndex must be defined')

        sc = profileBlocksFiltered.context
        numberOfEdges = 0
        edgesPerProfile = None

        if weightType == WeightTypes.EJS:
            stats = computeStatistics(profileBlocksFiltered, blockIndex, maxID, separatorIDs)
            numberOfEdges = stats.map(lambda x: x[0]).sum()
            edgesPerProfile = sc.broadcast(
                stats.map(lambda x: (x[1][0], x[1][1])).groupByKey().map(lambda x: (x[0], sum(x[1]))).collectAsMap())

        thresholds = sc.broadcast(
            WNP.calcThresholds(profileBlocksFiltered, blockIndex, maxID, separatorIDs, thresholdType, weightType,
                               profileBlocksSizeIndex, useEntropy, blocksEntropies, numberOfEdges,
                               edgesPerProfile).collectAsMap())

        edges = WNP.pruning(profileBlocksFiltered, blockIndex, maxID, separatorIDs, groundtruth, thresholdType,
                            weightType, profileBlocksSizeIndex, useEntropy, blocksEntropies, chi2divider,
                            comparisonType, thresholds, numberOfEdges, edgesPerProfile)

        thresholds.unpersist()
        if edgesPerProfile is not None:
            edgesPerProfile.unpersist()

        return edges
