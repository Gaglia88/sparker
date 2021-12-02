import re
from .utils import Utils


class BlockingKeysStrategies(object):
    @staticmethod
    def token_blocking(attributes, keys_to_exclude=None):
        """
        Performs the token blocking. It tokenize text by punctuation, white spaces, etc.
        :param attributes: KeyValue attributes
        :param keys_to_exclude: keys to exclude from the blocking process
        :return: list of tokens extracted from the attributes
        """
        if keys_to_exclude is None:
            keys_to_exclude = []
        try:
            non_empty_attributes = filter(lambda a: not a.is_empty(), attributes)
            values = filter(lambda x: len(x) > 0,
                            map(lambda x: "" if x.key in keys_to_exclude else x.value.lower(), non_empty_attributes))
            split = map(lambda x: re.split('\W+', x), values)
            return list(set(filter(lambda x: len(x) > 0, [y.strip() for x in split for y in x])))
        except:
            return []

    @staticmethod
    def ngrams_blocking(attributes, keys_to_exclude=None, ngram_size=3):
        """
        Performs blocking by using ngrams as blocking keys.
        :param attributes: KeyValue attributes
        :param keys_to_exclude: keys to exclude from the blocking process
        :param ngram_size: size of the ngrams, default 3
        :return: list of ngrams extracted from the attributes
        """
        if keys_to_exclude is None:
            keys_to_exclude = []
        try:
            # First, extracts all the tokens
            tokens = BlockingKeysStrategies.token_blocking(attributes, keys_to_exclude)
            # Then generates n-grams from each token
            ngrams = [Utils.get_ngrams(token, ngram_size) for token in tokens]
            # Extracts all the ngrams
            return list(set([n for g in ngrams for n in g]))

        except:
            return []
