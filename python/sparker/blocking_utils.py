class BlockingUtils(object):
    @staticmethod
    def separate_profiles(elements, separators):
        input_e = elements
        output = []
        for sep in separators:
            a = [x for x in input_e if x <= sep]
            input_e = [x for x in input_e if x > sep]
            output.append(a)
        output.append(input_e)
        return list(map(lambda x: set(x), output))