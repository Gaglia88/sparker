import re
import sys
import random
import networkx as nx
from collections import Counter
import math
from .objects import KeysCluster, Attr


class AttributeClustering(object):
    @staticmethod
    def get_hashes(str_hash, num_hashes, seed=1234):
        """
        Given an integer, generates num_hashes hashes.

        Parameters
        ----------
        str_hash : int
            integer number to hash
        num_hashes : int
            number of hashes to generate
        seed : int
            seed for random number generation
        """
        random.seed(seed)

        def get_part():
            a = int(random.randint(0, sys.maxsize - 1))
            b = int(random.randint(0, sys.maxsize - 1))
            return int(((a * int(str_hash) + b) % 2147495899) % sys.maxsize)

        return [get_part() for _ in range(0, num_hashes)]

    @staticmethod
    def get_num_bands(target_threshold, sig_num):
        b = sig_num
        while (pow(1.0 / b, 1.0 / (float(sig_num) / float(b)))) < target_threshold and b > 1:
            b -= 1

        return b + 1

    @staticmethod
    def get_num_rows(target_threshold, sig_num):
        """
        Given the target_threshold and the number of hashes, returns the number of rows for LSH

        Parameters
        ----------
        target_threshold : float
            Similarity threshold ]0,1]
        sig_num : int
            Number of hashes
        """
        bands = AttributeClustering.get_num_bands(target_threshold, sig_num)
        n_rows = int(sig_num / bands)
        if n_rows < 1:
            return 1
        else:
            return n_rows

    @staticmethod
    def sliding(lst, w):
        """
        Given a list l, split the list in chunks of w size.
        E.g. l=[1,2,3,4,5] w=2, returns [[1,2],[3,4], [5]]

        Parameters
        ----------
        lst : []
            A list
        w : int
            Chunk size
        """
        for i in range(0, len(lst), w):
            yield lst[i:i + w]

    @staticmethod
    def calc_similarity(sig1, sig2):
        """
        Given two signatures of the same length compute the Jaccard similarity.

        Parameters
        ----------
        sig1 : [int]
            First signature
        sig2 : [int]
            Second signature
        """
        common = 0
        for i in range(0, len(sig1)):
            if sig1[i] == sig2[i]:
                common += 1
        return float(common) / float(len(sig1))

    @staticmethod
    def cluster_similar_attributes(profiles, num_hashes, target_threshold, max_factor=1.0, num_bands=-1,
                                   keys_to_exclude=None, compute_entropy=False, separator="_", normalize_entropy=False):
        """
            Clusters together similar attributes.
            Returns a list of Keys_cluster objects.

            Parameters
            ----------
            profiles : RDD[Profile]
                RDD of profiles
            num_hashes : int
                Number of hashesh to use for LSH
            target_threshold : float
                Similarity threshold for LSH (]0,1])
            max_factor : float, optional
                max_factor
            num_bands : int, optional
                Number of bands for LSH, if it is not set, it is computed automatically
            keys_to_exclude : [string], optional
                List of attributes that are excluded from the clustering process
            compute_entropy : boolean, optional
                If true, compute the entropy for each cluster
            separator : str, optional
                The id of the source is associated to each attribute. The id is separated from the attribute name
                with this separator.
                E.g. if there is an attribute "name" coming from source 1, and the separator is "_", the attribute is
                inserted in a cluster as "1_name"
            normalize_entropy : boolean, optional
                if True the entropy is normalized

        """

        if keys_to_exclude is None:
            keys_to_exclude = []

        def get_tokens(profile):
            attributes = filter(lambda x: x.key not in keys_to_exclude, profile.attributes)
            split = map(lambda a: (a.key, filter(lambda t: len(t) > 0,
                                                 map(lambda t: t.strip(),
                                                     re.split('\W+', a.value.lower())))), attributes)
            d = map(lambda t: [(Attr(profile.source_id, t[0]), token) for token in t[1]], split)
            return [y for x in d for y in x]

        # For each profile produces an RDD of (attribute, token)
        attributes_token = profiles.flatMap(get_tokens)
        # For each attribute produces an RDD of (token, [attributes]), to each token is assigned an unique id,
        # so the final RDD is (token_id, [attribute])
        attributes_per_token = attributes_token.map(lambda a: (a[1], a[0])).groupByKey().zipWithIndex().map(
            lambda x: (x[1], set(x[0][1])))

        sc = profiles.context

        # Hashes each token, producing a map (token, hashes)
        hashes = sc.broadcast(attributes_per_token
                              .map(lambda a: (a[0], AttributeClustering.get_hashes(a[0], num_hashes))).collectAsMap())

        # Generates an RDD of (attribute, [token_ids])
        tokens_per_attribute = attributes_per_token.flatMap(lambda a: [(p, a[0]) for p in a[1]]).groupByKey().map(
            lambda x: (x[0], set(x[1])))

        # Extract all the attributes
        all_attributes = tokens_per_attribute.map(lambda x: x[0])

        def make_signature(data):
            attribute = data[0]
            tokens = data[1]
            signature = [sys.maxsize] * num_hashes
            for t in tokens:
                h = hashes.value[t]
                for i in range(0, num_hashes):
                    if h[i] < signature[i]:
                        signature[i] = h[i]
            return attribute, signature

        # Generates the signature for each attribute, producing an RDD of (attribute, signature)
        attribute_with_signature = tokens_per_attribute.map(make_signature)

        # Computes the number of rows for each band
        if num_bands > 0:
            num_rows = num_bands
        else:
            num_rows = AttributeClustering.get_num_rows(target_threshold, num_hashes)

        def get_buckets(data):
            attribute = data[0]
            signature = data[1]
            buckets_int = AttributeClustering.sliding(signature, num_rows)
            ids = map(lambda b: hash(tuple(b)), buckets_int)
            return attribute, ids

        # Generate the bucket from each signature, the result is (attribute, [buckets])
        buckets = attribute_with_signature.map(get_buckets)

        # Produce an RDD of (bucket, [attributes])
        attributes_per_bucket = buckets.flatMap(lambda b: [(x, b[0]) for x in b[1]]).groupByKey() \
            .map(lambda b: set(b[1])) \
            .filter(lambda b: 1 < len(b) < 101) \
            .map(lambda b: tuple(b)) \
            .distinct()

        # Collects and sends in broadcast the signatures
        attribute_signatures = attribute_with_signature.collectAsMap()

        hashes.unpersist()
        attribute_signatures_broadcast = sc.broadcast(attribute_signatures)

        def get_edges(cluster):
            pairs = [(cluster[i], cluster[j]) for i in range(0, len(cluster)) for j in range(i + 1, len(cluster))]
            gpairs = filter(lambda p: p[0].source_name != p[1].source_name, pairs)
            return map(lambda p: (p[0], (p[1],
                                         AttributeClustering.calc_similarity(attribute_signatures_broadcast.value[p[0]],
                                                                             attribute_signatures_broadcast.value[p[1]])
                                         )), gpairs)

        # Generate all the possible pairs (attribute1, attribute2, sim(attribute1, attribute2)) from the buckets
        edges = attributes_per_bucket.flatMap(get_edges)
        # Now the pairs are grouped by the first attribute, obtaining an RDD of
        # (attribute1, [(attribute1, attribute2, sim(a1,a2)), ..., (attribute1, attribute_n, sim(a1, a_n))])
        edges_per_key = edges.union(edges.map(lambda e: (e[1][0], (e[0], e[1][1])))).groupByKey() \
            .map(lambda x: (x[0], set(x[1])))

        def get_top_edges(data):
            key1 = data[0]
            keys2 = data[1]
            max_v = max(list(map(lambda x: x[1], keys2))) * max_factor
            top_keys2 = list(map(lambda x: x[0], filter(lambda k: k[1] >= max_v, keys2)))
            return key1, top_keys2

        # For each attribute keeps only the top N*max_factor most similar attributes
        top_edges = edges_per_key.map(get_top_edges)

        # Attributes kept after the top edges selection
        graph_vertices = top_edges.map(lambda x: x[0]).union(top_edges.flatMap(lambda x: x[1])).distinct().collect()
        graph_edges = top_edges.collect()

        # Generates a graph with the attributes as vertex, with the edges the retained top edges
        g = nx.Graph()
        for v in graph_vertices:
            g.add_node(v)

        for from_v in graph_edges:
            for to_v in from_v[1]:
                g.add_edge(from_v[0], to_v)

        # Applies the connected components on the graph, obtaining the cluster of attributes.
        clusters = list(filter(lambda c: len(c) > 1, list(nx.connected_components(g))))
        clustered_attributes = [y for x in clusters for y in x]
        # Lists of attributes that do not appears in any cluster
        non_clustered_attributes = list(map(lambda k: str(k.source_name) + str(separator) + str(k.attribute),
                                            filter(lambda a: a not in clustered_attributes, all_attributes.collect())))
        clusters = [(clusters[i], i) for i in range(0, len(clusters))]
        # Id of the default cluster (contains the non_clustered_attributes)
        default_cluster_id = len(clusters)
        attribute_signatures_broadcast.unpersist()

        # Compute the entropy
        if compute_entropy:
            # Map that associate each attribute to the cluster id in which is contained
            key_cluster_map = dict([(attribute, cluster[1]) for cluster in clusters for attribute in cluster[0]])

            def get_attribute_entropy(data):
                attribute = data[0]
                tokens = data[1]
                number_of_tokens = float(len(tokens))  # type: float
                c = Counter(tokens)
                tokens_count = [c[t] for t in set(tokens)]
                tokens_p1 = list(map(lambda token_count: float(token_count) / number_of_tokens, tokens_count))
                tokens_p = list(map(lambda p_i: p_i * (math.log10(p_i) / math.log10(2.0)), tokens_p1))
                if normalize_entropy:
                    entropy = - sum(tokens_p) / (math.log10(number_of_tokens) / math.log10(2.0))
                else:
                    entropy = - sum(tokens_p)

                return attribute, entropy

            # Computes the entropy of each attribute, producing an RDD of (attribute, entropy)
            entropy_per_attribute = attributes_token.groupByKey().map(get_attribute_entropy)
            attributes_token.unpersist()

            def associate_entropy_to_cluster(data):
                attribute = data[0]
                entropy = data[1]
                cluster_id = key_cluster_map.get(attribute, default_cluster_id)
                return cluster_id, entropy

            # Associate each attribute to its cluster, producing an RDD of (cluster_id, entropy)
            # then groups together the entropies of the same cluster, and computes the average.
            # This is the cluster' entropy.
            entropy_per_cluster = entropy_per_attribute.map(associate_entropy_to_cluster).groupByKey().map(
                lambda x: (x[0], sum(x[1]) / len(x)))
            entropy_map = entropy_per_cluster.collectAsMap()

            def create_cluster_entropy(cluster):
                keys = cluster[0]
                cluster_id = cluster[1]
                entropy = entropy_map.get(cluster_id, 1.0)
                attributes = list(map(lambda k: str(k.source_name) + separator + str(k.attribute), keys))
                return KeysCluster(cluster_id, attributes, entropy)

            # Generates the default cluster
            default_cluster = KeysCluster(default_cluster_id, non_clustered_attributes,
                                          entropy_map.get(default_cluster_id, 1.0))

            # Put all together, and generates the final clusters.
            final_clusters = list(map(create_cluster_entropy, clusters))
            final_clusters.append(default_cluster)
        else:
            def create_cluster(cluster):
                keys = cluster[0]
                cluster_id = cluster[1]
                attributes = list(map(lambda k: str(k.source_name) + separator + str(k.attribute), keys))
                return KeysCluster(cluster_id, attributes)

            # Generates the default cluster
            default_cluster = KeysCluster(default_cluster_id, non_clustered_attributes)

            # Put all together, and generates the final clusters.
            final_clusters = list(map(create_cluster, clusters))
            final_clusters.append(default_cluster)

        return final_clusters
