from pyspark.sql import SparkSession
from pyspark import SparkContext
from .objects import Profile, MatchingEntities, KeyValue
import json


class JSONWrapper(object):
    """
    Wrapper to load the profiles from a JSON
    """

    @staticmethod
    def load_profiles(file_path, start_id_from=0, real_id_field="", source_id=0):
        """
            Load the profiles from a JSON file.
            Returns an RDD of profiles.

            Parameters
            ----------

            file_path : str
                path of the CSV file
            start_id_from : int, optional
                value from which start the profile identifiers
            real_id_field : str, optional
                name of the attribute that contain the profile identifier in the CSV
            source_id : int, optional
                source identifier
            """
        def parse_profile_json(row):
            real_id = ""
            p_data = json.loads(row[0])
            p_attributes = []

            for key in p_data:
                if real_id_field == key:
                    real_id = str(p_data[key])
                elif type(p_data[key]) == list:
                    for value in p_data[key]:
                        try:
                            p_attributes.append(KeyValue(key, str(value)))
                        except Exception:
                            pass
                else:
                    try:
                        p_attributes.append(KeyValue(key, str(p_data[key])))
                    except Exception:
                        pass

            return Profile(row[1] + start_id_from, attributes=p_attributes, original_id=real_id, source_id=source_id)

        sc = SparkContext.getOrCreate()
        raw = sc.textFile(file_path)
        return raw.zipWithIndex().map(lambda row: parse_profile_json(row))

    @staticmethod
    def load_groundtruth(file_path, first_dataset_attribute, second_dataset_attribute):
        def parse_match_json(row):
            j_data = json.loads(row)
            return MatchingEntities(str(j_data[first_dataset_attribute]), str(j_data[second_dataset_attribute]))

        sc = SparkContext.getOrCreate()
        raw = sc.textFile(file_path)
        return raw.map(parse_match_json)


class CSVWrapper(object):
    """
    Wrapper to load the profiles from a CSV
    """

    @staticmethod
    def load_profiles(file_path, start_id_from=0, separator=",", header=True, real_id_field="", source_id=0):
        """
        Load the profiles from a CSV file.
        Returns an RDD of profiles.

        Parameters
        ----------

        file_path : str
            path of the CSV file
        start_id_from : int, optional
            value from which start the profile identifiers
        separator : string, optional
            CSV column separator
        header : boolean, optional
            if true means that the CSV has an header row
        real_id_field : str, optional
            name of the attribute that contain the profile identifier in the CSV
        source_id : int, optional
            source identifier
        """
        sql_context = SparkSession.builder.getOrCreate()
        df = sql_context.read.option("header", header).option("sep", separator).option("delimiter", "\"").csv(file_path)
        column_names = df.columns

        def row_to_attributes(row):
            return list(filter(lambda k: not k.is_empty(), [KeyValue(col, row[col]) for col in column_names]))

        def get_profile(data):
            profile_id = data[1] + start_id_from
            attributes = data[0]
            real_id = ""
            if len(real_id_field) > 0:
                real_id = "".join(map(lambda a: a.value, filter(lambda a: a.key == real_id_field, attributes))).strip()

            return Profile(profile_id, list(filter(lambda a: a.key != real_id_field, attributes)), real_id, source_id)

        return df.rdd.map(lambda row: row_to_attributes(row)).zipWithIndex().map(lambda x: get_profile(x))

    @staticmethod
    def load_groundtruth(file_path, id1='id1', id2='id2', separator=",", header=True):
        """
        Load the groundtruth from a csv file
        Return an RDD of Matching_entities

        Parameters
        ----------

        file_path : str
            path of the CSV file
        id1 : str, optional
            name of the column that contains the first profile identifier
        id2 : str, optional
            name of the column that contains the second profile identifier
        separator : string, optional
            CSV column separator
        header : boolean, optional
            if true means that the CSV has an header row
        """
        sql_context = SparkSession.builder.getOrCreate()
        df = sql_context.read.option("header", header).option("sep", separator).csv(file_path)
        return df.rdd.map(lambda row: MatchingEntities(row[id1], row[id2]))
