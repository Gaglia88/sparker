from pyspark.sql import SparkSession
from pyspark import SparkContext
from py_sparker.objects import Profile, MatchingEntities, KeyValue
import json


class JSONWrapper(object):
    """
    Wrapper to load the profiles from a JSON
    """

    @staticmethod
    def loadProfiles(filePath, startIDFrom=0, realIDField="", sourceId=0):
        def parseProfileJson(row):
            realID = ""
            pData = json.loads(row[0])
            pAttributes = []

            for key in pData:
                if realIDField == key:
                    realID = str(pData[key])
                elif type(pData[key]) == list:
                    for value in pData[key]:
                        try:
                            pAttributes.append(KeyValue(key, unicode(str(value), errors="ignore")))
                        except Exception:
                            pass
                else:
                    try:
                        pAttributes.append(KeyValue(key, unicode(str(pData[key]), errors="ignore")))
                    except Exception:
                        pass

            return Profile(row[1] + startIDFrom, attributes=pAttributes, originalID=realID, sourceId=sourceId)

        sc = SparkContext.getOrCreate()
        raw = sc.textFile(filePath)
        return raw.zipWithIndex().map(lambda row: parseProfileJson(row))

    @staticmethod
    def loadGroundtruth(filePath, firstDatasetAttribute, secondDatasetAttribute):
        def parseMatchJson(row):
            jData = json.loads(row)
            return MatchingEntities(str(jData[firstDatasetAttribute]), str(jData[secondDatasetAttribute]))

        sc = SparkContext.getOrCreate()
        raw = sc.textFile(filePath)
        return raw.map(parseMatchJson)


class CSVWrapper(object):
    """
    Wrapper to load the profiles from a CSV
    """

    @staticmethod
    def loadProfiles(filePath, startIDFrom=0, separator=",", header=True, realIDField="", sourceId=0):
        """
        Load the profiles from a CSV file.
        Returns an RDD of profiles.

        Parameters
        ----------

        filePath : str
            path of the CSV file
        startIDFrom : int, optional
            value from which start the profile identifiers
        separator : string, optional
            CSV column separator
        header : boolean, optional
            if true means that the CSV has an header row
        realIDField : str, optional
            name of the attribute that contain the profile identifier in the CSV
        sourceId : int, optional
            source identifier
        """
        sqlContext = SparkSession.builder.getOrCreate()
        df = sqlContext.read.option("header", header).option("sep", separator).option("delimiter", "\"").csv(filePath)
        columnNames = df.columns

        def rowToAttributes(row):
            return filter(lambda k: not k.isEmpty(), [KeyValue(col, row[col]) for col in columnNames])

        def getProfile(data):
            profileID = data[1] + startIDFrom
            attributes = data[0]
            realID = ""
            if len(realIDField) > 0:
                realID = "".join(map(lambda a: a.value, filter(lambda a: a.key == realIDField, attributes))).strip()

            return Profile(profileID, list(filter(lambda a: a.key != realIDField, attributes)), realID, sourceId)

        return df.rdd.map(lambda row: rowToAttributes(row)).zipWithIndex().map(lambda x: getProfile(x))

    @staticmethod
    def loadGroundtruth(filePath, id1='id1', id2='id2', separator=",", header=True):
        """
        Load the groundtruth from a csv file
        Return an RDD of MatchingEntities

        Parameters
        ----------

        filePath : str
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
        sqlContext = SparkSession.builder.getOrCreate()
        df = sqlContext.read.option("header", header).option("sep", separator).csv(filePath)
        return df.rdd.map(lambda row: MatchingEntities(row[id1], row[id2]))
