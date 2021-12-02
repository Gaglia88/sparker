import numpy as np
import math
from py_sparker.pruning_utils import WeightTypes, PruningUtils


def calcChiSquare(CBS, neighbourNumBlocks, currentProfileNumBlocks, totalNumberOfBlocks):
    """
    Computes the chi-square.
    :param CBS: Double - common blocks
    :param neighbourNumBlocks: Double - number of blocks that the neighbour owns
    :param currentProfileNumBlocks: Double - number of blocks that the current profile owns
    :param totalNumberOfBlocks: Double - number of existing blocks
    :return: chi-square
    """
    CMat = np.eye(3, 3)
    weight = 0
    expectedValue = 0

    CMat[0][0] = CBS
    CMat[0][1] = neighbourNumBlocks - CBS
    CMat[0][2] = neighbourNumBlocks

    CMat[1][0] = currentProfileNumBlocks - CBS
    CMat[1][1] = totalNumberOfBlocks - (neighbourNumBlocks + currentProfileNumBlocks - CBS)
    CMat[1][2] = totalNumberOfBlocks - neighbourNumBlocks

    CMat[2][0] = currentProfileNumBlocks
    CMat[2][1] = totalNumberOfBlocks - currentProfileNumBlocks

    for i in range(0, 2):
        for j in range(0, 2):
            expectedValue = (CMat[i][2] * CMat[2][j]) / totalNumberOfBlocks
            weight += math.pow((CMat[i][j] - expectedValue), 2) / expectedValue

    return weight


def doReset(weights, neighbors, entropies, useEntropy, neighborsNumber):
    """
    Resets the arrays
    :param weights: Array[Double]: an array which contains the weight of each neighbour
    :param neighbors: Array[Int] : an array which contains the IDs of the neighbors
    :param entropies: Array[Double] : an array which contains the entropy of each neighbour
    :param useEntropy: boolean: if true use the provided entropies to improve the edge weighting
    :param neighborsNumber: int : number of neighbors
    :return:
    """
    for i in range(0, neighborsNumber):
        neighbourID = neighbors[i]
        weights[neighbourID] = 0
        if useEntropy:
            entropies[neighbourID] = 0
    pass


def computeStatistics(profileBlocksFiltered, blockIndex, maxID, separatorID):
    """
    Computes the statistics needed to perform the EJS
    :param profileBlocksFiltered: RDD[ProfileBlocks] : profileBlocks after block filtering
    :param blockIndex: Broadcast[Map[Long, Array[Set[Long]]]] : a map that given a block ID returns the IDs of the contained profiles
    :param maxID: Long : maximum profile ID
    :param separatorID: Array[Long] : maximum profile ID of each dataset
    :return: an RDD which contains (number of distinct edges, (profileID, number of neighbors))
    """

    def computePartition(partition):
        localWeights = [0 for i in range(0, maxID + 1)]
        neighbors = [0 for i in range(0, maxID + 1)]

        def compInside(pb):
            neighborsNumber = 0
            distinctEdges = 0
            profileID = pb.profileID
            profileBlocks = pb.blocks
            for block in profileBlocks:
                blockID = block.blockID
                if blockID in blockIndex.value:
                    blockProfiles = blockIndex.value[blockID]
                    profilesIDs = []
                    if len(separatorID) == 0:
                        profilesIDs = blockProfiles[0]
                    else:
                        profilesIDs = PruningUtils.getAllNeighbors(profileID, blockProfiles, separatorID)

                    for neighbourID in profilesIDs:
                        neighbourWeight = localWeights[neighbourID]
                        if neighbourWeight == 0:
                            localWeights[neighbourID] = 1
                            neighbors[neighborsNumber] = neighbourID
                            neighborsNumber += 1
                            if profileID < neighbourID:
                                distinctEdges += 1

            for i in range(0, neighborsNumber):
                localWeights[i] = 0

            return distinctEdges, (profileID, neighborsNumber)

        return map(compInside, partition)

    return profileBlocksFiltered.mapPartitions(computePartition)


def calcCBS(pb, blockIndex, separatorID, useEntropy, blocksEntropies, weights, entropies, neighbors, firstStep):
    """
    Computes the CBS for a single profile
    :param pb: ProfileBlocks: the profile
    :param blockIndex: Broadcast[scala.collection.Map[Long, Array[Set[Long]]]] : a map that given a block ID returns the ID of the contained profiles
    :param separatorID: Array[Long] : List of the ids that separate the profiles coming from the different datasets
    :param useEntropy: boolean : if true use the provided entropies to improve the edge weighting
    :param blocksEntropies: Broadcast[scala.collection.Map[Long, Double]]: a map that contains for each block its entropy
    :param weights: Array[Double] : an array which the CBS values will be stored in
    :param entropies: Array[Double] : an array which the entropy of each neighbour will be stored in
    :param neighbors: Array[Int] : an array which the ID of the neighbors will be stored in
    :param firstStep: Boolean: if it is set to true all the edges will be computed, otherwise only the edges that have an ID greater than current profile ID.
    :return: number of neighbors
    """
    neighborsNumber = 0
    profileID = pb.profileID
    profileBlocks = pb.blocks
    for block in profileBlocks:
        blockID = block.blockID
        if blockID in blockIndex.value:
            blockProfiles = blockIndex.value[blockID]
            profilesIDs = []
            if len(separatorID) == 0:
                profilesIDs = blockProfiles[0]
            else:
                profilesIDs = PruningUtils.getAllNeighbors(profileID, blockProfiles, separatorID)

            blockEntropy = 0
            if useEntropy:
                if blockID in blocksEntropies.value:
                    blockEntropy = blocksEntropies.value[blockID]

            for neighborID in profilesIDs:
                if (profileID < neighborID) or firstStep:
                    weights[neighborID] += 1
                    if useEntropy:
                        entropies[neighborID] += blockEntropy
                    if weights[neighborID] == 1:
                        neighbors[neighborsNumber] = neighborID
                        neighborsNumber += 1
    return neighborsNumber


def calcWeights(pb, weights, neighbors, entropies, neighborsNumber, blockIndex, separatorID, weightType,
                profileBlocksSizeIndex, useEntropy, numberOfEdges, edgesPerProfile):
    """
    Computes the weights for each neighbour
    :param pb: ProfileBlocks : the profile
    :param weights: Array[Double] : an array which contains the CBS values
    :param neighbors: Array[Int] : an array which contains the IDs of the neighbors
    :param entropies: Array[Double] : an array which contains the entropy of each neighbour
    :param neighborsNumber: Int : number of neighbors
    :param blockIndex: Broadcast[scala.collection.Map[Long, Array[Set[Long]]]] : a map that given a block ID returns the ID of the contained profiles
    :param separatorID: Array[Long] : maximum profile ID of the first dataset (-1 if there is only one dataset)
    :param weightType: String :  type of weight to use see pruning_utils.WeightTypes
    :param profileBlocksSizeIndex: Broadcast[scala.collection.Map[Long, Int]] : a map that contains for each profile the number of its blocks
    :param useEntropy: Boolean : if true use the provided entropies to improve the edge weighting
    :param numberOfEdges: Double : global number of existings edges
    :param edgesPerProfile: Broadcast[scala.collection.Map[Long, Double]] : a maps that contains for each profile the number of edges
    :return: number of neighbors
    """
    if weightType == WeightTypes.chiSquare:
        numberOfProfileBlocks = len(pb.blocks)
        totalNumberOfBlocks = float(len(blockIndex.value))
        for i in range(0, neighborsNumber):
            neighbourID = neighbors[i]
            if useEntropy:
                weights[neighbourID] = calcChiSquare(weights[neighbourID], profileBlocksSizeIndex.value[neighbourID],
                                                     numberOfProfileBlocks, totalNumberOfBlocks) * entropies[
                                           neighbourID]
            else:
                weights[neighbourID] = calcChiSquare(weights[neighbourID], profileBlocksSizeIndex.value[neighbourID],
                                                     numberOfProfileBlocks, totalNumberOfBlocks)

    elif weightType == WeightTypes.ARCS:
        profileID = pb.profileID
        profileBlocks = pb.blocks
        for block in profileBlocks:
            blockID = block.blockID
            if blockID in blockIndex.value:
                blockProfiles = blockIndex.value[blockID]
                comparisons = 1
                if (len(separatorID) == 0):
                    l = len(blockProfiles[0])
                    comparisons = float(l * (l - 1))
                else:
                    comparisons = float(np.prod(map(lambda l: len(l), blockProfiles)))

                for i in range(0, neighborsNumber):
                    neighbourID = neighbors[i]
                    weights[neighbourID] = weights[neighbourID] / comparisons
                    if useEntropy:
                        weights[neighbourID] = weights[neighbourID] * entropies[neighbourID]

    elif weightType == WeightTypes.JS:
        numberOfProfileBlocks = len(pb.blocks)
        for i in range(0, neighborsNumber):
            neighbourID = neighbors[i]
            commonBlocks = weights[neighbourID]
            js = 0
            if useEntropy:
                js = (commonBlocks / (
                        numberOfProfileBlocks + profileBlocksSizeIndex.value[neighbourID] - commonBlocks)) * \
                     entropies[neighbourID]
            else:
                js = commonBlocks / (numberOfProfileBlocks + profileBlocksSizeIndex.value[neighbourID] - commonBlocks)
            weights[neighbourID] = js

    elif weightType == WeightTypes.EJS:
        profileNumberOfneighbors = 0.00000000001
        if pb.profileID in edgesPerProfile.value:
            profileNumberOfneighbors = edgesPerProfile.value[pb.profileID] + 0.00000000001

        numberOfProfileBlocks = len(pb.blocks)

        for i in range(0, neighborsNumber):
            neighbourID = neighbors[i]
            commonBlocks = weights[neighbourID]
            ejs = 0
            en = 0.00000000001
            if neighbourID in edgesPerProfile.value:
                en = edgesPerProfile.value[neighbourID]

            if useEntropy:
                try:
                    ejs = ((commonBlocks / (
                            numberOfProfileBlocks + profileBlocksSizeIndex.value[neighbourID] - commonBlocks)) *
                           entropies[neighbourID]) * math.log10(
                        numberOfEdges / en * math.log10(numberOfEdges / profileNumberOfneighbors))
                except Exception:
                    pass
            else:
                try:
                    ejs = (commonBlocks / (numberOfProfileBlocks + profileBlocksSizeIndex.value[
                        neighbourID] - commonBlocks)) * math.log10(
                        numberOfEdges / (en * math.log10(numberOfEdges / profileNumberOfneighbors)))
                except Exception:
                    pass

            weights[neighbourID] = ejs
    elif weightType == WeightTypes.ECBS:
        blocksNumber = len(blockIndex.value)
        numberOfProfileBlocks = len(pb.blocks)
        for i in range(0, neighborsNumber):
            neighbourID = neighbors[i]
            commonBlocks = weights[neighbourID]
            ecbs = 0
            if useEntropy:
                ecbs = commonBlocks * math.log10(blocksNumber / numberOfProfileBlocks) * math.log10(
                    blocksNumber / profileBlocksSizeIndex.value[neighbourID]) * entropies[neighbourID]
            else:
                ecbs = commonBlocks * math.log10(blocksNumber / numberOfProfileBlocks) * math.log10(
                    blocksNumber / profileBlocksSizeIndex.value[neighbourID])

            weights[neighbourID] = ecbs
    pass
