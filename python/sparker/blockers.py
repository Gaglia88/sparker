import re
from .objects import BlockClean, BlockDirty
from .blocking_strategies import BlockingKeysStrategies
from .blocking_utils import BlockingUtils


class Blocking(object):
    @staticmethod
    def create_blocks_clusters(profiles, clusters, separator_ids=None, keys_to_exclude=None,
                               attributes_to_exclude=None, cluster_name_separator="_"):
        """
        Create the blocks by using the provided clusters of attributes
        :param profiles: RDD of profiles
        :param clusters: clusters of attributes
        :param separator_ids: separator for clean datasets
        :param keys_to_exclude: tokens to exclude from the process (e.g. stopwords)
        :param attributes_to_exclude: attributes to exclude from the process
        :param cluster_name_separator: separator between cluster id and token
        :return: RDD of blocks
        """
        if keys_to_exclude is None:
            keys_to_exclude = []
        if separator_ids is None:
            separator_ids = []
        if attributes_to_exclude is None:
            attributes_to_exclude = []
        default_cluster_id = max(map(lambda c: c.cluster_id, clusters))
        entropies = dict(map(lambda cluster: (cluster.cluster_id, cluster.entropy), clusters))
        cluster_map = dict([(k, c.cluster_id) for c in clusters for k in c.keys])

        def create_keys(profile):
            try:
                dataset = str(profile.source_id) + str(cluster_name_separator)
                non_empty_attributes = list(filter(lambda a: not a.is_empty(), profile.attributes))
                valid_keys = list(filter(lambda x: len(x[1]) > 0,
                                         map(lambda x: ("", "") if x.key in attributes_to_exclude else (x.key, x.value),
                                             non_empty_attributes)))
                split = list(map(lambda x: (x[0], re.split('\W+', x[1].lower())), valid_keys))

                def associate_cluster(data):
                    key = dataset + str(data[0])
                    cluster_id = str(cluster_map.get(key, default_cluster_id))
                    tokens_ok = list(
                        filter(lambda x: len(x) > 0 and x not in keys_to_exclude, map(lambda x: x.strip(), data[1])))
                    return list(map(lambda x: x + "_" + cluster_id, tokens_ok))

                tokens = [t for s in split for t in associate_cluster(s)]
                return profile.profile_id, list(set(tokens))
            except:
                return profile.profile_id, []

        tokens_per_profile = profiles.map(create_keys)

        token_profile = tokens_per_profile.flatMap(lambda x: [(y, x[0]) for y in x[1]])

        profile_per_key = token_profile.groupByKey().filter(lambda x: len(x[1]) > 1)

        def associate_entropy(data):
            key = data[0]
            profiles_int = data[1]
            entropy = 1.0
            try:
                cluster_id = int(key.split("_")[-1])
                entropy = entropies.get(cluster_id, 1.0)
            except:
                pass

            return key, profiles_int, entropy

        profiles_grouped = profile_per_key \
            .map(associate_entropy) \
            .map(lambda c: ([set(c[1])], c[2]) if len(separator_ids) == 0
        else (BlockingUtils.separate_profiles(c[1], separator_ids), c[2]))
        profiles_grouped_with_ids = profiles_grouped \
            .filter(lambda block: len(block[0][0]) > 1 if len(separator_ids) == 0
        else len(list(filter(lambda b: len(b) > 0, block[0]))) > 1).zipWithIndex()
        return profiles_grouped_with_ids.map(
            lambda b: BlockDirty(b[1], b[0][0], entropy=b[0][1]) if len(separator_ids) == 0 else
            BlockClean(b[1], b[0][0], entropy=b[0][1]))

    @staticmethod
    def create_blocks(profiles, separator_ids=None, keys_to_exclude=None,
                      attributes_to_exclude=None,
                      blocking_method=BlockingKeysStrategies.token_blocking,
                      **kwargs):
        """
        Indexes the provided profiles into blocks according to the blocking_method
        :param profiles: RDD of profiles
        :param separator_ids: separators
        :param keys_to_exclude: attributes to exclude from the blocking process
        :param attributes_to_exclude: attributes to exclude from the process
        :param blocking_method: blocking method to use, see BlockingKeysStrategies
        :param kwargs: extra arguments for the blocking method
        :return: RDD of blocks
        """
        if attributes_to_exclude is None:
            attributes_to_exclude = []
        if keys_to_exclude is None:
            keys_to_exclude = []
        if separator_ids is None:
            separator_ids = []
        tokens_per_profile = profiles.map(lambda profile: (
            profile.profile_id, set(blocking_method(profile.attributes, keys_to_exclude=keys_to_exclude,
                                                    attributes_to_exclude=attributes_to_exclude, **kwargs))))
        token_profile = tokens_per_profile.flatMap(lambda x: [(y, x[0]) for y in x[1]])
        profile_per_key = token_profile.groupByKey().filter(lambda x: len(x[1]) > 1)
        profiles_grouped = profile_per_key.map(lambda c: [set(c[1])] if len(separator_ids) == 0
        else BlockingUtils.separate_profiles(c[1], separator_ids))
        profiles_grouped_with_ids = profiles_grouped.filter(lambda block: len(block[0]) > 1 if len(separator_ids) == 0
        else len(list(filter(lambda b: len(b) > 0, block))) > 1) \
            .zipWithIndex()
        return profiles_grouped_with_ids.map(lambda b: BlockDirty(b[1], b[0]) if len(separator_ids) == 0
        else BlockClean(b[1], b[0]))
