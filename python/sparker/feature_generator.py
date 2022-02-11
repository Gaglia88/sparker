from .converters import Converters
from .common_node_pruning import PruningUtils
import math
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, FloatType, IntegerType, LongType, StructType, StructField


class FeatureGenerator(object):
    """
    Generates features for generalized supervised meta-blocking
    """

    @staticmethod
    def calc_profiles_stats(profile_blocks, max_id, block_index, separators):
        """
        Compute the statistics for each profile
        :param profile_blocks: blocks ids for every profile
        :param max_id: max profile id
        :param block_index: broadcasted blocks
        :param separators: separators ids
        :return: An RDD that contains for every profile the number of redundant comparisons
                 and the number of non-redundant comparisons (profile_id, num_redundant_comp, num_non_redundant_comp)
        """

        def compute_partition(partition):
            seen = [False] * (max_id + 1)
            neighbors = [0] * (max_id + 1)

            def inside_map(pb):
                redundant_comparisons = 0
                non_redundant_comparisons = 0
                neighbors_number = 0

                for block in pb.blocks:
                    block_id = block.block_id
                    if block_id in block_index.value:
                        block_profiles = block_index.value[block_id]
                        if len(separators) == 0:
                            neighbors_ids = block_profiles[0]
                        else:
                            neighbors_ids = PruningUtils.get_all_neighbors(pb.profile_id, block_profiles, separators)

                        for neighbor_id in neighbors_ids:
                            if not seen[neighbor_id]:
                                non_redundant_comparisons += 1
                                seen[neighbor_id] = True
                                neighbors[neighbors_number] = neighbor_id
                                neighbors_number += 1
                            redundant_comparisons += 1

                for i in range(0, neighbors_number):
                    seen[neighbors[i]] = False

                return pb.profile_id, redundant_comparisons, non_redundant_comparisons

            return map(inside_map, partition)

        return profile_blocks.mapPartitions(compute_partition)

    @staticmethod
    def generate_features(profiles,
                          blocks,
                          separators=None,
                          groundtruth=None,
                          convert_ids=True
                          ):
        """
        Generate the features for Generalized Supervised Meta-Blocking.
        For every pair of profiles that co-occurs in the blocks, returns a set of features.
        The features are returned as a DataFrame.

        :param profiles: RDD of profiles
        :param blocks: RDD of blocks
        :param separators: (optional) separators ids, None if the dataset is dirty
        :param groundtruth: (optional) set of pairs that represents the groundtruth, same ids of the profiles
        :param convert_ids: (default True) converts the profiles id to their original identifiers
        :return: a DataFrame with the features for every pair of profiles.
        """
        if separators is None:
            separators = []
        # Max profile id
        max_id = profiles.map(lambda p: p.profile_id).max()
        # Converts the blocks into profile blocks
        profile_blocks = Converters.blocks_to_profile_blocks(blocks)

        sc = profiles.context

        if groundtruth is not None:
            groundtruth_broadcast = sc.broadcast(groundtruth)

        if convert_ids:
            profiles_ids = sc.broadcast(profiles.map(lambda p: (p.profile_id, p.original_id)).collectAsMap())
        else:
            profiles_ids = None

        blocks_num = blocks.count()
        block_index = sc.broadcast(blocks.map(lambda b: (b.block_id, b.profiles)).collectAsMap())
        profile_blocks_num_index = sc.broadcast(profile_blocks.map(lambda pb: (pb.profile_id, len(pb.blocks)))
                                                .collectAsMap())
        total_comparisons = blocks.map(lambda b: b.get_comparison_size()).sum()

        inv_profile_block_size_index = sc.broadcast(blocks
                                                    .flatMap(lambda b: map(lambda p: (p, 1.0 / b.get_size()),
                                                                           b.get_all_profiles()))
                                                    .groupByKey()
                                                    .map(lambda x: (x[0], sum(x[1]))).collectAsMap())
        comparison_per_profile_index = sc.broadcast(blocks
                                                    .flatMap(lambda b: map(lambda p: (p, b.get_comparison_size()),
                                                                           b.get_all_profiles()))
                                                    .groupByKey()
                                                    .map(lambda x: (x[0], sum(x[1]))).collectAsMap())

        inv_profile_block_comp_size_index = sc.broadcast(blocks
                                                         .flatMap(
            lambda b: map(lambda p: (p, 1.0 / b.get_comparison_size())
                          , b.get_all_profiles()))
                                                         .groupByKey()
                                                         .map(lambda x: (x[0], sum(x[1]))).collectAsMap())

        stats = FeatureGenerator.calc_profiles_stats(profile_blocks, max_id, block_index, separators)

        profiles_stats = sc.broadcast(stats.map(lambda x: (x[0], (x[1], x[2]))).collectAsMap())

        def compute_partition(partition):
            neighbors = [0] * (max_id + 1)
            cbs = [0] * (max_id + 1)
            raccb = [0.0] * (max_id + 1)
            rs = [0.0] * (max_id + 1)

            def inside_map(pb):
                features = []
                neighbors_number = 0
                profile_id = pb.profile_id

                for block in pb.blocks:
                    block_id = block.block_id
                    if block_id in block_index.value:
                        block_profiles = block_index.value[block_id]
                        if len(separators) == 0:
                            neighbors_ids = block_profiles[0]
                        else:
                            neighbors_ids = PruningUtils.get_all_neighbors(pb.profile_id, block_profiles, separators)

                        for neighbor_id in neighbors_ids:
                            if profile_id < neighbor_id:
                                cbs[neighbor_id] += 1
                                if cbs[neighbor_id] == 1:
                                    raccb[neighbor_id] += 1.0 / block.comparisons
                                    rs[neighbor_id] += 1.0 / sum(map(lambda x: len(x), block_profiles))
                                    neighbors[neighbors_number] = neighbor_id
                                    neighbors_number += 1

                ibf1 = math.log(blocks_num / profile_blocks_num_index.value[profile_id])
                compP1 = profiles_stats.value[profile_id]

                for i in range(0, neighbors_number):
                    neighbor_id = neighbors[i]
                    compP2 = profiles_stats.value[neighbor_id]
                    ibf2 = math.log(blocks_num / profile_blocks_num_index.value[neighbor_id])
                    CFIBF = cbs[neighbor_id] * ibf1 * ibf2
                    RACCB = raccb[neighbor_id]
                    if RACCB < 1.0E-6:
                        RACCB = 1.0E-6
                    JS = cbs[neighbor_id] / (compP1[0] + compP2[0] - cbs[neighbor_id])
                    RS = rs[neighbor_id]
                    NRS = RS / (inv_profile_block_size_index.value[profile_id] +
                                inv_profile_block_size_index.value[neighbor_id] - RS)
                    WJS = RACCB / (inv_profile_block_comp_size_index.value[profile_id] +
                                   inv_profile_block_comp_size_index.value[neighbor_id] - RACCB)
                    JS_1 = cbs[neighbor_id] / (profile_blocks_num_index.value[profile_id] +
                                               profile_blocks_num_index.value[neighbor_id] -
                                               cbs[neighbor_id])
                    AEJS = JS_1 * math.log(total_comparisons / comparison_per_profile_index.value[profile_id]) * \
                           math.log(total_comparisons / comparison_per_profile_index.value[neighbor_id])

                    if groundtruth is not None:
                        if (profile_id, neighbor_id) in groundtruth_broadcast.value:
                            IS_MATCH = 1
                        else:
                            IS_MATCH = 0
                    else:
                        IS_MATCH = None

                    raccb[neighbor_id] = 0
                    rs[neighbor_id] = 0
                    cbs[neighbor_id] = 0

                    if convert_ids:
                        features.append(
                            (profiles_ids.value[profile_id],
                             profiles_ids.value[neighbor_id],
                             CFIBF,
                             RACCB,
                             JS,
                             compP1[1],
                             compP2[1],
                             RS,
                             AEJS,
                             NRS,
                             WJS,
                             IS_MATCH
                             )
                        )
                    else:
                        features.append(
                            (profile_id,
                             neighbor_id,
                             CFIBF,
                             RACCB,
                             JS,
                             compP1[1],
                             compP2[1],
                             RS,
                             AEJS,
                             NRS,
                             WJS,
                             IS_MATCH
                             )
                        )
                return features

            return map(inside_map, partition)

        features = profile_blocks.mapPartitions(compute_partition).flatMap(lambda x: x)

        spark = SparkSession.builder.getOrCreate()

        if convert_ids:
            id_type = StringType()
        else:
            id_type = IntegerType()

        schema = StructType() \
            .add(StructField("p1", id_type, False)) \
            .add(StructField("p2", id_type, False)) \
            .add(StructField("cfibf", FloatType(), False)) \
            .add(StructField("raccb", FloatType(), False)) \
            .add(StructField("js", FloatType(), False)) \
            .add(StructField("numCompP1", LongType(), False)) \
            .add(StructField("numCompP2", LongType(), False)) \
            .add(StructField("rs", FloatType(), False)) \
            .add(StructField("aejs", FloatType(), False)) \
            .add(StructField("nrs", FloatType(), False)) \
            .add(StructField("wjs", FloatType(), False)) \
            .add(StructField("is_match", IntegerType(), True))

        df = spark.createDataFrame(features, schema)

        return df
