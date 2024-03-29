import pyspark.sql.functions as f
from pyspark.sql.types import BooleanType


class SupervisedMB(object):

    @staticmethod
    def bcl(edges):
        """
        Performs the pruning by using the binary classifier outcome.
        :param edges: DataFrame of edges with the probability column (p_match)
        """
        return edges.filter("is_match == 1")

    @staticmethod
    def cep(edges, blocks):
        """
        Performs the pruning by using the cardinality edge pruning method.
        :param edges: DataFrame of edges with the probability column (p_match)
        :param blocks: original block collection
        """
        over_t = edges.filter("p_match >= 0.5")
        number_of_edges_to_keep = int(blocks.map(lambda b: b.get_size()).sum()/2)
        pruned_edges = over_t.sort('p_match', ascending=False).limit(number_of_edges_to_keep)
        return pruned_edges

    @staticmethod
    def wep(edges):
        """
        Performs the pruning by using the Weight Edge Pruning method
        :param edges: DataFrame of edges with the probability column (p_match)
        :return: dataframe with pruned edges
        """
        over_t = edges.filter("p_match >= 0.5")
        threshold = over_t.rdd.map(lambda x: x.asDict()['p_match']).mean()
        return over_t.filter(over_t.p_match >= threshold)

    @staticmethod
    def blast(edges):
        """
        Performs the pruning by using the BLAST method
        :param edges: DataFrame of edges with the probability column (p_match)
        :return: dataframe with pruned edges
        """
        sc = edges.rdd.context

        over_t = edges.filter("p_match >= 0.5")
        over_t.cache()
        over_t.count()

        profiles1_max_proba = sc.broadcast(over_t.groupby('p1').max('p_match').rdd.collectAsMap())
        profiles2_max_proba = sc.broadcast(over_t.groupby('p2').max('p_match').rdd.collectAsMap())

        def do_pruning(p1, p2, p_match):
            threshold = 0.35 * (profiles1_max_proba.value[p1] + profiles2_max_proba.value[p2])
            return p_match >= threshold

        pruning_udf = f.udf(do_pruning, BooleanType())

        res = over_t \
            .select("p1", "p2", "p_match", "is_match", pruning_udf("p1", "p2", "p_match").alias("keep")) \
            .where("keep") \
            .select("p1", "p2", "p_match", "is_match")
        res.count()
        over_t.unpersist()
        profiles1_max_proba.unpersist()
        profiles2_max_proba.unpersist()
        return res

    @staticmethod
    def rcnp(edges, profiles, blocks):
        """
        Performs the pruning by using the Reciprocal Cardinality Node Pruning method
        :param edges: DataFrame of edges with the probability column (p_match)
        :param profiles: RDD of profiles
        :param blocks: RDD of blocks
        :return: dataframe with pruned edges
        """
        sc = edges.rdd.context

        over_t = edges.filter("p_match >= 0.5")
        over_t.cache()
        over_t.count()

        n_entities = profiles.count()
        block_size = blocks.map(lambda b: b.get_size()).sum()
        k = int((2 * max(1.0, block_size / n_entities)))

        over_t_rdd = over_t.rdd.map(lambda x: x.asDict())

        def get_top_k(x):
            profile_id = x[0]
            neighbors_ids = sorted(x[1], key=lambda y: -y[1])

            if len(neighbors_ids) > k:
                neighbors_ids = neighbors_ids[:k]

            neighbors_ids = set(map(lambda y: y[0], neighbors_ids))

            return profile_id, neighbors_ids

        top_p1 = over_t_rdd.map(lambda x: (x['p1'], (x['p2'], x['p_match']))).groupByKey().map(get_top_k)
        top_p2 = over_t_rdd.map(lambda x: (x['p2'], (x['p1'], x['p_match']))).groupByKey().map(get_top_k)

        top_p1_brd = sc.broadcast(top_p1.collectAsMap())
        top_p2_brd = sc.broadcast(top_p2.collectAsMap())

        def do_pruning(p1, p2):
            return (p1 in top_p2_brd.value[p2]) and (p2 in top_p1_brd.value[p1])

        pruning_udf = f.udf(do_pruning, BooleanType())

        res = over_t \
            .select("p1", "p2", "p_match", "is_match", pruning_udf("p1", "p2").alias("keep")) \
            .where("keep") \
            .select("p1", "p2", "p_match", "is_match")
        res.count()

        over_t.unpersist()
        top_p1_brd.unpersist()
        top_p2_brd.unpersist()

        return res

    @staticmethod
    def cnp(edges, profiles, blocks):
        """
        Performs the pruning by using the Cardinality Node Pruning method
        :param edges: DataFrame of edges with the probability column (p_match)
        :param profiles: RDD of profiles
        :param blocks: RDD of blocks
        :return: dataframe with pruned edges
        """
        sc = edges.rdd.context

        over_t = edges.filter("p_match >= 0.5")
        over_t.cache()
        over_t.count()

        n_entities = profiles.count()
        block_size = blocks.map(lambda b: b.get_size()).sum()
        k = int((2 * max(1.0, block_size / n_entities)))

        over_t_rdd = over_t.rdd.map(lambda x: x.asDict())

        def get_top_k(x):
            profile_id = x[0]
            neighbors_ids = sorted(x[1], key=lambda y: -y[1])

            if len(neighbors_ids) > k:
                neighbors_ids = neighbors_ids[:k]

            neighbors_ids = set(map(lambda y: y[0], neighbors_ids))

            return profile_id, neighbors_ids

        top_p1 = over_t_rdd.map(lambda x: (x['p1'], (x['p2'], x['p_match']))).groupByKey().map(get_top_k)

        top_p2 = over_t_rdd.map(lambda x: (x['p2'], (x['p1'], x['p_match']))).groupByKey().map(get_top_k)

        top_p1_brd = sc.broadcast(top_p1.collectAsMap())
        top_p2_brd = sc.broadcast(top_p2.collectAsMap())

        def do_pruning(p1, p2):
            return (p1 in top_p2_brd.value[p2]) or (p2 in top_p1_brd.value[p1])

        pruning_udf = f.udf(do_pruning, BooleanType())

        res = over_t \
            .select("p1", "p2", "p_match", "is_match", pruning_udf("p1", "p2").alias("keep")) \
            .where("keep") \
            .select("p1", "p2", "p_match", "is_match")

        res.count()

        over_t.unpersist()
        top_p1_brd.unpersist()
        top_p2_brd.unpersist()

        return res

    @staticmethod
    def get_stats(edges, groundtruth):
        """
        Given the dataframe of edges with the is_match column
        computes precision, recall and F1-score
        :param edges: dataframe of edges
        :param groundtruth: converted groundtruth
        :return: precision, recall, F1-score
        """
        gt_size = len(groundtruth)

        num_matches = edges.where("is_match == 1").count()
        num_edges = edges.count()

        pc = num_matches / gt_size
        pq = num_matches / num_edges

        f1 = 0.0
        if pc > 0 and pq > 0:
            f1 = 2 * pc * pq / (pc + pq)

        return pc, pq, f1
