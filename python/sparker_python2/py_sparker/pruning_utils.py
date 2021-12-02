class WeightTypes(object):
    """
    Contains the supported weight types to weight the edges of the meta-blocking graph
    """
    CBS = "cbs"
    JS = "js"
    chiSquare = "chiSquare"
    ARCS = "arcs"
    ECBS = "ecbs"
    EJS = "ejs"


class ThresholdTypes(object):
    """
    Contains the supported thresholds types to prune the edges
    """
    MAX_FRACT_2 = "maxdiv2"
    AVG = "avg"


class ComparisonTypes(object):
    """
    Contains the supported pruning strategies
    """
    AND = "and"
    OR = "or"


class PruningUtils(object):
    @staticmethod
    def getAllNeighbors(profileId, block, separators):
        """
        Given a block and a profile ID returns all its neighbors
        :param profileId: profile id
        :param block: profile in which its contained
        :param separators: id of the separators that identifies the different datasources
        :return: all neighbors of the profile
        """
        output = set()
        i = 0
        while i < len(separators) and profileId > separators[i]:
            output.update(block[i])
            i += 1
        i += 1

        while i < len(separators):
            output.update(block[i])
            i += 1

        if profileId <= separators[-1]:
            output.update(block[-1])

        return output
