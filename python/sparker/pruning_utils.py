class WeightTypes(object):
    """
    Contains the supported weight types to weight the edges of the meta-blocking graph
    """
    CBS = "cbs"
    JS = "js"
    CHI_SQUARE = "chi_square"
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
    def get_all_neighbors(profile_id, block, separators):
        """
        Given a block and a profile ID returns all its neighbors
        :param profile_id: profile id
        :param block: profile in which its contained
        :param separators: id of the separators that identifies the different data sources
        :return: all neighbors of the profile
        """
        output = set()
        i = 0
        while i < len(separators) and profile_id > separators[i]:
            output.update(block[i])
            i += 1
        i += 1

        while i < len(separators):
            output.update(block[i])
            i += 1

        if profile_id <= separators[-1]:
            output.update(block[-1])

        return output
