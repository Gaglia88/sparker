import re
from .utils import Utils


class BlockingKeysStrategies(object):
    @staticmethod
    def token_blocking_w_attr(attributes, attributes_to_exclude=None, keys_to_exclude=None):
        """
        Performs the token blocking by adding the attribute name as prefix of the token.
        E.g. KeyValue(key=nominative, value=John Smith) produces [nominative_john, nominative_smith]
        It tokenize text by punctuation, white spaces, etc.
        :param attributes: KeyValue attributes
        :param attributes_to_exclude: attributes to exclude from the blocking process
        :param keys_to_exclude: keys (e.g. stopwords) to exclude from the blocking process
        :return: list of tokens extracted from the attributes
        """
        if attributes_to_exclude is None:
            attributes_to_exclude = []
        if keys_to_exclude is None:
            keys_to_exclude = []
        try:
            non_empty_attributes = filter(lambda a: not a.is_empty(), attributes)
            values = filter(lambda x: len(x[0]) > 0,
                            map(lambda x: ("", "") if x.key in attributes_to_exclude else (x.key, x.value.lower()),
                                non_empty_attributes))
            split = map(lambda x: (x[0], re.split('\W+', x[1])), values)
            return set(map(lambda x: x[0] + "_" + x[1], filter(lambda x: len(x[1]) > 0,
                                                               [(x[0], y.strip()) for x in split for y in x[1]
                                                                if y not in keys_to_exclude])))
        except:
            return []

    @staticmethod
    def token_blocking(attributes, attributes_to_exclude=None, keys_to_exclude=None):
        """
        Performs the token blocking. It tokenize text by punctuation, white spaces, etc.
        :param attributes: KeyValue attributes
        :param attributes_to_exclude: attributes to exclude from the blocking process
        :param keys_to_exclude: keys (e.g. stopwords) to exclude from the blocking process
        :return: list of tokens extracted from the attributes
        """
        if attributes_to_exclude is None:
            attributes_to_exclude = []
        if keys_to_exclude is None:
            keys_to_exclude = []
        try:
            non_empty_attributes = filter(lambda a: not a.is_empty(), attributes)
            values = filter(lambda x: len(x) > 0,
                            map(lambda x: "" if x.key in attributes_to_exclude else x.value.lower(),
                                non_empty_attributes))
            split = map(lambda x: re.split('\W+', x), values)
            return list(set(filter(lambda x: len(x) > 0, [y.strip() for x in split for y in x if y.strip()
                                                          not in keys_to_exclude])))
        except:
            return []

    @staticmethod
    def ngrams_blocking(attributes, attributes_to_exclude=None, ngram_size=3, keys_to_exclude=None):
        """
        Performs blocking by using ngrams as blocking keys.
        :param attributes: KeyValue attributes
        :param attributes_to_exclude: attributes to exclude from the blocking process
        :param ngram_size: size of the ngrams, default 3
        :param keys_to_exclude: keys (e.g. stopwords) to exclude from the blocking process
        :return: list of ngrams extracted from the attributes
        """
        if attributes_to_exclude is None:
            attributes_to_exclude = []
        if keys_to_exclude is None:
            keys_to_exclude = []
        try:
            # First, extracts all the tokens
            tokens = BlockingKeysStrategies.token_blocking(attributes, attributes_to_exclude)
            # Then generates n-grams from each token
            ngrams = [Utils.get_ngrams(token, ngram_size) for token in tokens]
            # Extracts all the ngrams
            return list(set([n for g in ngrams for n in g if n not in keys_to_exclude]))

        except:
            return []
